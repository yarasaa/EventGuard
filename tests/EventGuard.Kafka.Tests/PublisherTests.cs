using System.Text;
using Confluent.Kafka;
using EventGuard.Runtime;

namespace EventGuard.Kafka.Tests;

public sealed class PublisherTests(BrokerFixture broker) : IClassFixture<BrokerFixture>
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Outbox_to_Kafka_to_consumer_commits_once(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer);
        await db.InitializeAsync();
        var topic = await broker.TopicAsync();
        var group = broker.Group();
        var message = BrokerFixture.Message(topic, headers: new Dictionary<string, string> { ["application"] = "files" }, key: "file-42");
        await db.EnqueueAsync(message);
        await using var publisher = new KafkaPublisher(BrokerFixture.ProducerConfig(), "local", "v1");
        var published = await new OutboxDispatcher(db.Outbox, message.Scope, [publisher]).DispatchNextAsync();
        Assert.Equal(OutboxState.Published, published!.Record.State);
        Assert.Equal("kafka-persisted", published.Record.LastReason);
        Assert.Equal(PublishStatus.Accepted, (await publisher.PublishAsync(message)).Status); // physical duplicate on a distinct application send
        var native = BrokerFixture.Read(topic);
        Assert.Equal("file-42", Encoding.UTF8.GetString(native.Message.Key));
        Assert.Equal(message.CopyPayload(), native.Message.Value);
        Assert.Equal("files", Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes("application")));
        Assert.Equal(message.MessageId.ToString("D"), Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes(KafkaPublisher.MessageIdHeader)));
        var observed = new ObservedStore(db.Store);
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var accepting = new KafkaIngress(BrokerFixture.ConsumerConfig(group), observed, db.Scope, StoreHarness.Policy, [topic], acceptanceTimeout: TimeSpan.FromSeconds(5)).RunAsync(stop.Token);
        try { await BrokerFixture.WaitCommitAsync(group, topic, 2); }
        finally { stop.Cancel(); await accepting; }
        Assert.Equal([AcceptanceStatus.Accepted, AcceptanceStatus.Duplicate], observed.Results.Select(r => r.Status));
        var claim = (await db.Store.ClaimNextAsync(db.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)))!;
        Assert.Equal(ExecutionStatus.Completed, (await db.ExecuteAsync(claim)).Status);
        Assert.Equal(1, await db.EffectCount(claim.Work.Id));
        Assert.Equal(1, await db.CountAsync("works"));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(32)]
    public async Task Parallel_publications_preserve_every_message_identity(int maximumInFlight)
    {
        var topic = await broker.TopicAsync(3);
        await using var publisher = new KafkaPublisher(BrokerFixture.ProducerConfig(), "local", "v1", maximumInFlight);
        var messages = Enumerable.Range(0, 48).Select(_ => BrokerFixture.Message(topic, key: "same-key")).ToArray();
        Assert.All(await Task.WhenAll(messages.Select(m => publisher.PublishAsync(m))), r => Assert.Equal(PublishStatus.Accepted, r.Status));
        using var reader = BrokerFixture.Reader(broker.Group());
        reader.Assign(Enumerable.Range(0, 3).Select(p => new TopicPartitionOffset(topic, p, Offset.Beginning)));
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var ids = new HashSet<Guid>();
        var partitions = new HashSet<int>();
        for (var i = 0; i < messages.Length; i++)
        {
            var record = reader.Consume(stop.Token);
            Assert.True(ids.Add(Guid.Parse(Encoding.UTF8.GetString(record.Message.Headers.GetLastBytes(KafkaPublisher.MessageIdHeader)))));
            partitions.Add(record.Partition.Value);
        }
        Assert.Equal(messages.Select(m => m.MessageId).Order(), ids.Order());
        Assert.Single(partitions);
        Assert.Equal(messages.Length, Enumerable.Range(0, 3).Sum(p =>
            reader.QueryWatermarkOffsets(new(topic, p), TimeSpan.FromSeconds(5)).High.Value));
    }

    [Theory]
    [InlineData("invalid/topic", null)]
    [InlineData("valid-topic", "eventguard-message-id")]
    [InlineData("valid-topic", "Content-Type")]
    public async Task Invalid_envelope_is_rejected_before_enqueue(string topic, string? header)
    {
        await using var publisher = new KafkaPublisher(BrokerFixture.ProducerConfig(), "local", "v1");
        var message = BrokerFixture.Message(topic, headers: header is null ? null : new Dictionary<string, string> { [header] = "override" });
        Assert.Equal(PublishStatus.NotSentPermanent, (await publisher.PublishAsync(message)).Status);
    }

    [Fact]
    public async Task Broker_size_rejection_is_known_not_persisted_and_next_message_progresses()
    {
        var topic = await broker.TopicAsync(config: new() { ["max.message.bytes"] = "1024" });
        await using var publisher = new KafkaPublisher(BrokerFixture.ProducerConfig(), "local", "v1");
        var result = await publisher.PublishAsync(BrokerFixture.Message(topic, body: new byte[8192]));
        Assert.Equal(PublishStatus.NotSentPermanent, result.Status);
        Assert.Equal(PublishStatus.Accepted, (await publisher.PublishAsync(BrokerFixture.Message(topic))).Status);
        Assert.Equal(0, BrokerFixture.Read(topic).Offset.Value);
    }

    [Fact]
    public async Task Cancelled_and_disposed_publisher_does_not_enqueue()
    {
        var topic = await broker.TopicAsync();
        var publisher = new KafkaPublisher(BrokerFixture.ProducerConfig(), "local", "v1");
        using var stop = new CancellationTokenSource(); stop.Cancel();
        Assert.Equal(PublishStatus.NotSentTransient, (await publisher.PublishAsync(BrokerFixture.Message(topic), stop.Token)).Status);
        await Task.WhenAll(publisher.DisposeAsync().AsTask(), publisher.DisposeAsync().AsTask());
        Assert.Equal(PublishStatus.NotSentTransient, (await publisher.PublishAsync(BrokerFixture.Message(topic))).Status);
    }
}
