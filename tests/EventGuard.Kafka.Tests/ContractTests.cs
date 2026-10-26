using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using EventGuard.Kafka.Sample.Contracts;

namespace EventGuard.Kafka.Tests;

public sealed class ContractTests(BrokerFixture broker) : IClassFixture<BrokerFixture>
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Unsupported_application_contract_version_does_not_advance_offset(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group();
        var model = new FileUpdated(Guid.NewGuid(), 3, "file", 1, "file.txt");
        await broker.RawAsync(topic, model.EventId, JsonSerializer.SerializeToUtf8Bytes(model));
        WorkSubmission Map(ConsumeResult<byte[], byte[]> record)
        {
            var value = FileUpdatedJson.Decode(record.Message.Value);
            return new(new(db.Scope, StoreHarness.Policy, value.EventId.ToString("D")), value.EventId, record.Message.Value, ContentFingerprint.FromBytes(record.Message.Value));
        }
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var error = await Assert.ThrowsAsync<IngressMessageRejectedException>(() => IngressTests.Ingress(db, db.Store, topic, group, Map).RunAsync(stop.Token));
        Assert.Equal("unsupported-contract-version", error.ReasonCode);
        Assert.Equal(Offset.Unset.Value, BrokerFixture.Committed(group, topic));
        Assert.Equal(0, await db.CountAsync("works"));
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Policy_binding_change_cannot_be_hidden_by_a_new_consumer_group(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group(); var firstId = Guid.NewGuid();
        await db.Store.AcceptAsync(new(new(db.Scope, StoreHarness.Policy, firstId.ToString("D")), firstId, [1], ContentFingerprint.FromBytes([1])));
        await broker.RawAsync(topic, Guid.NewGuid());
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await Assert.ThrowsAsync<PolicyMismatchException>(() => new KafkaIngress(BrokerFixture.ConsumerConfig(group), db.Store, db.Scope,
            new("new-key/v2", "body/v1"), [topic], acceptanceTimeout: TimeSpan.FromSeconds(5)).RunAsync(stop.Token));
        Assert.Equal(Offset.Unset.Value, BrokerFixture.Committed(group, topic));
        Assert.Equal(1, await db.CountAsync("works"));
    }
    [Fact]
    public async Task Aborted_Kafka_transaction_is_not_admitted_and_committed_input_is()
    {
        await using var db = new StoreHarness(false); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group();
        var config = BrokerFixture.ProducerConfig(); config.TransactionalId = "eg_kafka_test_" + Guid.NewGuid().ToString("N");
        using var producer = new ProducerBuilder<byte[], byte[]>(config).Build();
        producer.InitTransactions(TimeSpan.FromSeconds(10));
        Message<byte[], byte[]> Message(Guid id) => new() { Value = [1], Headers = new Headers { { KafkaPublisher.MessageIdHeader, Encoding.UTF8.GetBytes(id.ToString("D")) } } };
        producer.BeginTransaction();
        await producer.ProduceAsync(topic, Message(Guid.NewGuid()));
        producer.AbortTransaction(TimeSpan.FromSeconds(10));
        var committedId = Guid.NewGuid();
        producer.BeginTransaction();
        var accepted = await producer.ProduceAsync(topic, Message(committedId));
        producer.CommitTransaction(TimeSpan.FromSeconds(10));
        var store = new ObservedStore(db.Store);
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var running = IngressTests.Ingress(db, store, topic, group).RunAsync(stop.Token);
        try { await BrokerFixture.WaitCommitAsync(group, topic, accepted.Offset.Value + 1); }
        finally { stop.Cancel(); await running; }
        Assert.Equal(committedId, Assert.Single(store.Results).Work.MessageId);
        Assert.Equal(1, await db.CountAsync("works"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Explicit_old_and_current_versions_are_accepted_and_unknown_version_is_quarantined(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group();
        var scope = new WorkScope(db.Scope.Tenant, db.Scope.Consumer, db.Scope.Source, "file.updated");
        var first = new FileUpdated(Guid.NewGuid(), 1, "resource", 18, "current.txt");
        var second = new FileUpdatedV2(Guid.NewGuid(), 2, "resource", 17, "old.txt");
        var firstBytes = JsonSerializer.SerializeToUtf8Bytes(first); var secondBytes = JsonSerializer.SerializeToUtf8Bytes(second);
        await broker.RawAsync(topic, first.EventId, firstBytes); await broker.RawAsync(topic, second.EventId, secondBytes);
        await broker.RawAsync(topic, Guid.NewGuid(), JsonSerializer.SerializeToUtf8Bytes(first with { ContractVersion = 3 }));
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var receiving = new KafkaIngress(BrokerFixture.ConsumerConfig(group), db.Store, scope, StoreHarness.Policy, [topic],
            delivery => FileUpdatedJson.Contract.Map(scope, StoreHarness.Policy, delivery.Message.Value, value => (value.EventId.ToString("D"), value.EventId)),
            TimeSpan.FromSeconds(5), new("test-cluster")).RunAsync(stop.Token);
        try { await BrokerFixture.WaitCommitAsync(group, topic, 3); }
        finally { stop.Cancel(); await receiving; }
        Assert.Equal(2, await db.CountAsync("works")); Assert.Equal(1, await db.CountAsync("ingress_quarantine"));
        var observed = new HashSet<Guid>();
        for (var i = 0; i < 2; i++)
        {
            var claim = (await db.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)))!;
            var value = FileUpdatedJson.Decode(claim.Payload); Assert.Equal(2, value.ContractVersion); Assert.True(observed.Add(value.EventId));
            Assert.Equal(value.EventId == first.EventId ? firstBytes : secondBytes, claim.Payload);
            Assert.Equal(ExecutionStatus.Completed, (await db.ExecuteAsync(claim)).Status); Assert.Equal(1, await db.EffectCount(claim.Work.Id));
        }
        Assert.Contains(first.EventId, observed); Assert.Contains(second.EventId, observed);
        Assert.Equal("unsupported-contract-version", (await ((IIngressQuarantineStore)db.Store).FindQuarantineAsync(scope, await db.QuarantineIdAsync()))!.ReasonCode);
    }
}
