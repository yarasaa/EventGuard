using Confluent.Kafka;
using EventGuard.Runtime;

namespace EventGuard.Kafka.Tests;

// This class alone owns the fixed proxy listener; verify.sh also runs framework targets sequentially.
public sealed partial class FaultTests(BrokerFixture broker) : IClassFixture<BrokerFixture>
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Lost_publish_reply_is_uncertain_and_a_retry_can_create_a_physical_duplicate(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var message = BrokerFixture.Message(topic);
        await db.EnqueueAsync(message);
        await using (var proxy = new KafkaReplyDropProxy(0))
        await using (var publisher = new KafkaPublisher(BrokerFixture.ProducerConfig(KafkaReplyDropProxy.Address), "local", "v1", 1))
        {
            var dispatcher = new OutboxDispatcher(db.Outbox, message.Scope, [publisher], new RetryPolicy(maximumAttempts: 3, initialDelay: TimeSpan.FromMilliseconds(1), jitterRatio: 0));
            var sending = dispatcher.DispatchNextAsync();
            await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var result = await sending.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(OutboxState.RetryScheduled, result!.Record.State);
            Assert.True(result.Record.DeliveryUncertain);
            Assert.Equal(message.CopyPayload(), BrokerFixture.Read(topic).Message.Value);
        }
        await Task.Delay(5);
        await using var recovered = new KafkaPublisher(BrokerFixture.ProducerConfig(), "local", "v1");
        var published = await new OutboxDispatcher(db.Outbox, message.Scope, [recovered]).DispatchNextAsync();
        Assert.Equal(OutboxState.Published, published!.Record.State);
        Assert.True(published.Record.DeliveryUncertain);
        Assert.Equal(message.CopyPayload(), BrokerFixture.Read(topic, 1).Message.Value);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Lost_offset_commit_request_or_response_preserves_durable_acceptance(bool dropRequest)
    {
        await using var db = new StoreHarness(false); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group();
        await broker.RawAsync(topic, Guid.NewGuid());
        await using (var proxy = new KafkaReplyDropProxy(8, dropRequest))
        {
            var config = BrokerFixture.ConsumerConfig(group, KafkaReplyDropProxy.Address);
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var running = new KafkaIngress(config, db.Store, db.Scope, StoreHarness.Policy, [topic], acceptanceTimeout: TimeSpan.FromSeconds(5)).RunAsync(stop.Token);
            await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.ThrowsAnyAsync<KafkaException>(() => running.WaitAsync(TimeSpan.FromSeconds(15)));
            Assert.Equal(1, await db.CountAsync("works"));
            Assert.Equal(dropRequest ? Offset.Unset.Value : 1, BrokerFixture.Committed(group, topic));
        }
        var recovered = new ObservedStore(db.Store);
        await broker.RawAsync(topic); // sentinel after the accepted record
        using var again = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await Assert.ThrowsAsync<IngressMessageRejectedException>(() => IngressTests.Ingress(db, recovered, topic, group).RunAsync(again.Token));
        Assert.Equal(dropRequest ? 1 : 0, recovered.Results.Count);
        if (dropRequest) Assert.Equal(AcceptanceStatus.Duplicate, recovered.Results.Single().Status);
        Assert.Equal(1, BrokerFixture.Committed(group, topic));
    }

    [Fact]
    public async Task Cancellation_keeps_the_native_pending_slot_until_delivery_finishes()
    {
        var topic = await broker.TopicAsync();
        await using var proxy = new KafkaReplyDropProxy(0);
        var publisher = new KafkaPublisher(BrokerFixture.ProducerConfig(KafkaReplyDropProxy.Address), "local", "v1", 1);
        using var firstStop = new CancellationTokenSource();
        var first = publisher.PublishAsync(BrokerFixture.Message(topic), firstStop.Token);
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(10));
        firstStop.Cancel();
        Assert.Equal(PublishStatus.Unknown, (await first).Status);
        using var secondStop = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        Assert.Equal(PublishStatus.NotSentTransient, (await publisher.PublishAsync(BrokerFixture.Message(topic), secondStop.Token)).Status);
        await publisher.DisposeAsync();
        Assert.Equal(0, BrokerFixture.Read(topic).Offset.Value);
        using var observer = BrokerFixture.Reader(broker.Group());
        Assert.Equal(1, observer.QueryWatermarkOffsets(new(topic, 0), TimeSpan.FromSeconds(5)).High.Value);
    }
}
