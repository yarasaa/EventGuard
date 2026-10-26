using Confluent.Kafka;
namespace EventGuard.Kafka.Tests;

// Same test class as the other frame-loss cases: the fixed native proxy cannot overlap itself.
public sealed partial class FaultTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Quarantine_survives_lost_native_offset_commit_request_or_reply(bool sqlServer, bool dropRequest)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group(); await broker.RawAsync(topic);
        await using (var proxy = new KafkaReplyDropProxy(8, dropRequest))
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var running = new KafkaIngress(BrokerFixture.ConsumerConfig(group, KafkaReplyDropProxy.Address), db.Store,
                db.Scope, StoreHarness.Policy, [topic], acceptanceTimeout: TimeSpan.FromSeconds(5), quarantine: new("test-cluster")).RunAsync(stop.Token);
            await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.ThrowsAnyAsync<KafkaException>(() => running.WaitAsync(TimeSpan.FromSeconds(15)));
            Assert.Equal(1, await db.CountAsync("ingress_quarantine"));
            Assert.Equal(dropRequest ? Offset.Unset.Value : 1, BrokerFixture.Committed(group, topic));
        }
        var original = await db.QuarantineIdAsync();
        await broker.RawAsync(topic, Guid.NewGuid());
        using var again = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var receiving = new KafkaIngress(BrokerFixture.ConsumerConfig(group), db.Store, db.Scope, StoreHarness.Policy,
            [topic], acceptanceTimeout: TimeSpan.FromSeconds(5), quarantine: new("test-cluster")).RunAsync(again.Token);
        try { await BrokerFixture.WaitCommitAsync(group, topic, 2); }
        finally { again.Cancel(); await receiving; }
        Assert.Equal(1, await db.CountAsync("ingress_quarantine")); Assert.Equal(original, await db.QuarantineIdAsync());
        Assert.Equal(1, await db.CountAsync("works"));
    }
}
