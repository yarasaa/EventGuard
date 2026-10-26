using System.Text;
using Confluent.Kafka;

namespace EventGuard.Kafka.Tests;

public sealed class IngressQuarantineTests(BrokerFixture broker) : IClassFixture<BrokerFixture>
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Permanent_input_is_preserved_before_offset_advances_and_following_work_continues(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group();
        var headers = new Headers { { "binary", new byte[] { 0, 255 } }, { "binary", null } };
        await broker.RawAsync(topic, body: [4, 5], headers: headers);
        await broker.RawAsync(topic, Guid.NewGuid());
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var ingress = new KafkaIngress(BrokerFixture.ConsumerConfig(group), db.Store, db.Scope, StoreHarness.Policy, [topic],
            acceptanceTimeout: TimeSpan.FromSeconds(5), quarantine: new("test-cluster-incarnation-1"));
        var running = ingress.RunAsync(stop.Token);
        try { await BrokerFixture.WaitCommitAsync(group, topic, 2); }
        finally { stop.Cancel(); await running; }
        Assert.Equal(1, await db.CountAsync("works")); Assert.Equal(1, await db.CountAsync("ingress_quarantine"));
        var recovery = (IIngressQuarantineStore)db.Store;
        var id = await db.QuarantineIdAsync();
        Assert.Equal("invalid-message-id", (await recovery.FindQuarantineAsync(db.Scope, id))!.ReasonCode);
        var raw = (await recovery.ReadQuarantineDataAsync(db.Scope, id))!;
        Assert.Equal(new byte[] { 4, 5 }, raw.CopyBody()); Assert.Equal(topic + ":0:0", raw.SourcePosition);
        using var reader = new BinaryReader(new MemoryStream(raw.CopyMetadata()), Encoding.UTF8);
        Assert.Equal(-1, reader.ReadInt32()); reader.ReadInt32(); reader.ReadInt64();
        Assert.Equal(2, reader.ReadInt32()); Assert.Equal("binary", reader.ReadString());
        Assert.Equal(2, reader.ReadInt32()); Assert.Equal(new byte[] { 0, 255 }, reader.ReadBytes(2));
        Assert.Equal("binary", reader.ReadString()); Assert.Equal(-1, reader.ReadInt32());
        // Re-reading the same native partition/offset under another group uses the same raw identity.
        var again = broker.Group(); using var second = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        running = new KafkaIngress(BrokerFixture.ConsumerConfig(again), db.Store, db.Scope, StoreHarness.Policy, [topic],
            acceptanceTimeout: TimeSpan.FromSeconds(5), quarantine: new("test-cluster-incarnation-1")).RunAsync(second.Token);
        try { await BrokerFixture.WaitCommitAsync(again, topic, 2); }
        finally { second.Cancel(); await running; }
        Assert.Equal(1, await db.CountAsync("ingress_quarantine")); Assert.Equal(id, await db.QuarantineIdAsync());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Mapper_mutation_does_not_change_raw_snapshot_or_settlement_cursor(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group();
        await broker.RawAsync(topic, body: [6]); await broker.RawAsync(topic, body: [7]);
        WorkSubmission Map(ConsumeResult<byte[], byte[]> message)
        {
            if (message.Offset.Value == 1) throw new InvalidOperationException("unexpected mapper error");
            message.Offset = 999; message.Message.Value[0] = 99;
            throw new IngressMessageRejectedException("invalid-contract");
        }
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        await Assert.ThrowsAsync<InvalidOperationException>(() => new KafkaIngress(BrokerFixture.ConsumerConfig(group), db.Store,
            db.Scope, StoreHarness.Policy, [topic], Map, TimeSpan.FromSeconds(5), new("test-cluster")).RunAsync(stop.Token));
        Assert.Equal(1, BrokerFixture.Committed(group, topic)); Assert.Equal(1, await db.CountAsync("ingress_quarantine"));
        var raw = await ((IIngressQuarantineStore)db.Store).ReadQuarantineDataAsync(db.Scope, await db.QuarantineIdAsync());
        Assert.Equal(new byte[] { 6 }, raw!.CopyBody()); Assert.Equal(topic + ":0:0", raw.SourcePosition);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Real_database_failure_during_quarantine_never_commits_offset(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group(); await broker.RawAsync(topic);
        // Deliberately use an uninitialized local schema; native storage fails, not a fake store.
        await using var postgres = sqlServer ? null : Npgsql.NpgsqlDataSource.Create(db.ConnectionString);
        IWorkStore broken = sqlServer
            ? new EventGuard.SqlServer.SqlServerWorkStore(new(db.ConnectionString), "eg_missing_" + Guid.NewGuid().ToString("N"))
            : new EventGuard.PostgreSql.PostgreSqlWorkStore(postgres!, "eg_missing_" + Guid.NewGuid().ToString("N"));
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(() => new KafkaIngress(BrokerFixture.ConsumerConfig(group), broken,
            db.Scope, StoreHarness.Policy, [topic], acceptanceTimeout: TimeSpan.FromSeconds(5), quarantine: new("test-cluster")).RunAsync(stop.Token));
        Assert.Equal(Offset.Unset.Value, BrokerFixture.Committed(group, topic));
    }
}
