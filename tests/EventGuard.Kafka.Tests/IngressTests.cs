using System.Data.Common;
using System.Text;
using Confluent.Kafka;

namespace EventGuard.Kafka.Tests;

public sealed class IngressTests(BrokerFixture broker) : IClassFixture<BrokerFixture>
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Conflict_is_durable_and_failed_mapping_does_not_skip_the_next_record(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group(); var id = Guid.NewGuid();
        await broker.RawAsync(topic, id, [1]);
        await broker.RawAsync(topic, id, [2]);
        await broker.RawAsync(topic); // invalid stable identity
        await broker.RawAsync(topic, Guid.NewGuid());
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var store = new ObservedStore(db.Store);
        await Assert.ThrowsAsync<IngressMessageRejectedException>(() => Ingress(db, store, topic, group).RunAsync(stop.Token));
        Assert.Equal([AcceptanceStatus.Accepted, AcceptanceStatus.Conflict], store.Results.Select(r => r.Status));
        Assert.Equal(2, BrokerFixture.Committed(group, topic));
        Assert.Equal(1, await db.CountAsync("works"));
        Assert.Equal(1, await db.CountAsync("conflicts"));
        var claim = (await db.Store.ClaimNextAsync(db.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)))!;
        Assert.Equal(new byte[] { 1 }, claim.Payload);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Real_database_failure_leaves_source_offset_uncommitted(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group();
        await broker.RawAsync(topic, Guid.NewGuid());
        await db.BreakAcceptanceAsync();
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await Assert.ThrowsAnyAsync<DbException>(() => Ingress(db, db.Store, topic, group).RunAsync(stop.Token));
        Assert.Equal(Offset.Unset.Value, BrokerFixture.Committed(group, topic));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Host_cancellation_after_acceptance_recovers_as_duplicate(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group();
        await broker.RawAsync(topic, Guid.NewGuid());
        var accepted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var paused = new ObservedStore(db.Store, after: async (_, ct) => { accepted.SetResult(); await Task.Delay(Timeout.InfiniteTimeSpan, ct); });
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var running = Ingress(db, paused, topic, group).RunAsync(stop.Token);
        await accepted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        stop.Cancel(); await running;
        Assert.Equal(Offset.Unset.Value, BrokerFixture.Committed(group, topic));
        var recovered = new ObservedStore(db.Store);
        using var again = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        running = Ingress(db, recovered, topic, group).RunAsync(again.Token);
        try { await BrokerFixture.WaitCommitAsync(group, topic, 1); }
        finally { again.Cancel(); await running; }
        Assert.Equal(AcceptanceStatus.Duplicate, Assert.Single(recovered.Results).Status);
        Assert.Equal(1, await db.CountAsync("works"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Mapper_cannot_change_scope_policy_or_settlement_cursor(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group(); var id = Guid.NewGuid();
        await broker.RawAsync(topic, id);
        await broker.RawAsync(topic);
        WorkSubmission Map(ConsumeResult<byte[], byte[]> record)
        {
            if (record.Offset.Value != 0) throw new InvalidOperationException("sentinel");
            var result = new WorkSubmission(new(db.Scope, StoreHarness.Policy, id.ToString("D")), id, record.Message.Value, ContentFingerprint.FromBytes(record.Message.Value));
            record.Offset = 999;
            return result;
        }
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await Assert.ThrowsAsync<InvalidOperationException>(() => Ingress(db, db.Store, topic, group, Map).RunAsync(stop.Token));
        Assert.Equal(1, BrokerFixture.Committed(group, topic));
        var wrongGroup = broker.Group();
        await Assert.ThrowsAsync<InvalidOperationException>(() => Ingress(db, db.Store, topic, wrongGroup, record =>
            new(new(db.Scope, new("different", "body/v1"), "key"), id, record.Message.Value, ContentFingerprint.FromBytes(record.Message.Value))).RunAsync(stop.Token));
        Assert.Equal(Offset.Unset.Value, BrokerFixture.Committed(wrongGroup, topic));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Partition_offsets_only_follow_their_own_durable_acceptance(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(2); var group = broker.Group();
        await broker.RawAsync(topic, Guid.NewGuid(), partition: 0);
        await broker.RawAsync(topic, Guid.NewGuid(), partition: 1);
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var running = Ingress(db, db.Store, topic, group).RunAsync(stop.Token);
        try { await BrokerFixture.WaitCommitAsync(group, topic, 1, 0); await BrokerFixture.WaitCommitAsync(group, topic, 1, 1); }
        finally { stop.Cancel(); await running; }
        Assert.Equal(2, await db.CountAsync("works"));
    }

    [Theory]
    [InlineData("duplicate-header")]
    [InlineData("tombstone")]
    [InlineData("oversized")]
    public async Task Invalid_native_input_is_not_acknowledged(string kind)
    {
        await using var db = new StoreHarness(false); await db.InitializeAsync();
        var topic = await broker.TopicAsync(config: new() { ["max.message.bytes"] = "2097152" }); var group = broker.Group();
        var config = BrokerFixture.ProducerConfig(); config.MessageMaxBytes = 2097152;
        using var producer = new ProducerBuilder<byte[], byte[]>(config).Build();
        var headers = new Headers { { KafkaPublisher.MessageIdHeader, Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("D")) } };
        if (kind == "duplicate-header") headers.Add(KafkaPublisher.MessageIdHeader, Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("D")));
        await producer.ProduceAsync(topic, new Message<byte[], byte[]> { Value = kind == "tombstone" ? null! : new byte[kind == "oversized" ? 1048577 : 1], Headers = headers });
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await Assert.ThrowsAnyAsync<InvalidOperationException>(() => Ingress(db, db.Store, topic, group).RunAsync(stop.Token));
        Assert.Equal(Offset.Unset.Value, BrokerFixture.Committed(group, topic));
        Assert.Equal(0, await db.CountAsync("works"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Expired_acceptance_after_rebalance_stops_without_regressing_offset_or_hanging_shutdown(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group();
        await broker.RawAsync(topic, Guid.NewGuid()); await broker.RawAsync(topic, Guid.NewGuid());
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var config = BrokerFixture.ConsumerConfig(group); config.MaxPollIntervalMs = 6000;
        // Real acceptance commits before this test gate. Ignore cancellation deliberately to
        // model a slow completion callback without blocking the timer/thread pool.
        var paused = new ObservedStore(db.Store, after: async (_, _) => {
            entered.TrySetResult();
            await release.Task.WaitAsync(TimeSpan.FromSeconds(25));
        });
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var old = new KafkaIngress(config, paused, db.Scope, StoreHarness.Policy, [topic], acceptanceTimeout: TimeSpan.FromSeconds(1)).RunAsync(stop.Token);
        await Task.WhenAny(entered.Task, old).WaitAsync(TimeSpan.FromSeconds(10));
        if (old.IsCompleted) await old;
        var replacement = Ingress(db, db.Store, topic, group).RunAsync(stop.Token);
        try
        {
            await BrokerFixture.WaitCommitAsync(group, topic, 2);
            release.TrySetResult();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => old.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal(2, BrokerFixture.Committed(group, topic));
            Assert.Equal(2, await db.CountAsync("works"));
        }
        finally
        {
            release.TrySetResult(); stop.Cancel();
            try { await old.WaitAsync(TimeSpan.FromSeconds(10)); }
            catch (Exception error) when (error is KafkaException or OperationCanceledException) { }
            await replacement.WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    [Fact]
    public async Task Native_generation_fence_rejects_commit_after_another_member_has_advanced_offset()
    {
        await using var db = new StoreHarness(false); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group();
        await broker.RawAsync(topic, Guid.NewGuid()); await broker.RawAsync(topic, Guid.NewGuid());
        var config = BrokerFixture.ConsumerConfig(group); config.MaxPollIntervalMs = 6000;
        using var old = new ConsumerBuilder<byte[], byte[]>(config).SetLogHandler((_, _) => { }).Build();
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        old.Subscribe(topic);
        var first = old.Consume(stop.Token);
        var staleOffset = new TopicPartitionOffset(first.TopicPartition, first.Offset + 1);
        var replacement = Ingress(db, db.Store, topic, group).RunAsync(stop.Token);
        try
        {
            await BrokerFixture.WaitCommitAsync(group, topic, 2);
            Assert.ThrowsAny<KafkaException>(() => old.Commit([staleOffset]));
            Assert.Equal(2, BrokerFixture.Committed(group, topic));
            Assert.Equal(2, await db.CountAsync("works"));
        }
        finally
        {
            old.Unsubscribe(); stop.Cancel();
            await replacement.WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    internal static KafkaIngress Ingress(StoreHarness db, IWorkStore store, string topic, string group,
        Func<ConsumeResult<byte[], byte[]>, WorkSubmission>? map = null) =>
        new(BrokerFixture.ConsumerConfig(group), store, db.Scope, StoreHarness.Policy, [topic], map, TimeSpan.FromSeconds(5));
}
