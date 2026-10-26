using System.Text;
using RabbitMQ.Client;
using EventGuard.Runtime;

namespace EventGuard.RabbitMq.Tests;

public sealed class PublisherTests(BrokerFixture broker) : IClassFixture<BrokerFixture>
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Confirmed_message_preserves_persistent_identity_body_and_application_headers(bool namedExchange)
    {
        var queue = await broker.QueueAsync();
        var address = namedExchange ? await broker.ExchangeAsync() : RabbitMqPublisher.DefaultExchange;
        if (namedExchange) await broker.Admin.QueueBindAsync(queue, address, "file");
        var message = BrokerFixture.Message(queue, route: new("rabbitmq", "local", "v1", address, namedExchange ? "file" : queue),
            headers: new Dictionary<string, string> { ["kind"] = "dosya-ş" });
        var fingerprint = message.Fingerprint;
        await using var publisher = broker.Publisher();
        Assert.Equal(PublishStatus.Accepted, (await publisher.PublishAsync(message)).Status);
        var delivered = await broker.Admin.BasicGetAsync(queue, true);
        Assert.NotNull(delivered);
        Assert.True(delivered.BasicProperties.Persistent);
        Assert.Equal(message.MessageId.ToString("D"), delivered.BasicProperties.MessageId);
        Assert.Equal(message.ContentType, delivered.BasicProperties.ContentType);
        Assert.Equal(message.CopyPayload(), delivered.Body.ToArray());
        Assert.Equal("dosya-ş", Encoding.UTF8.GetString((byte[])delivered.BasicProperties.Headers!["kind"]!));
        Assert.Single(message.Headers);
        Assert.Equal(fingerprint, message.Fingerprint);
    }

    [Fact]
    public async Task Persistent_publish_is_confirmed_by_a_single_node_quorum_queue()
    {
        var queue = await broker.QueueAsync(new Dictionary<string, object?> { ["x-queue-type"] = "quorum", ["x-delivery-limit"] = -1 });
        await using var publisher = broker.Publisher();
        var message = BrokerFixture.Message(queue);
        Assert.Equal(PublishStatus.Accepted, (await publisher.PublishAsync(message)).Status);
        Assert.Equal(message.MessageId.ToString("D"), (await broker.Admin.BasicGetAsync(queue, true))!.BasicProperties.MessageId);
    }

    [Fact]
    public async Task Mandatory_unroutable_publication_is_dead_lettered_and_channel_remains_usable()
    {
        var message = BrokerFixture.Message("eg_absent_" + Guid.NewGuid().ToString("N"));
        await broker.EnqueueAsync(message);
        await using var publisher = broker.Publisher(channels: 1);
        var result = await new OutboxDispatcher(broker.Outbox, message.Scope, [publisher]).DispatchNextAsync();
        Assert.Equal(OutboxState.DeadLetter, result!.Record.State);
        Assert.False(result.Record.DeliveryUncertain);
        Assert.Equal("rabbitmq-unroutable", result.Record.LastReason);
        var queue = await broker.QueueAsync();
        Assert.Equal(PublishStatus.Accepted, (await publisher.PublishAsync(BrokerFixture.Message(queue))).Status);
        Assert.NotNull(await broker.Admin.BasicGetAsync(queue, true));
    }

    [Theory]
    [InlineData("exchange")]
    [InlineData("routing-key")]
    [InlineData("content-type")]
    [InlineData("header-key")]
    [InlineData("traceparent")]
    [InlineData("confirm-header")]
    [InlineData("binding")]
    public async Task Unsupported_envelope_is_rejected_before_send(string field)
    {
        var queue = await broker.QueueAsync();
        var longValue = new string('ş', 128); // 256 bytes, not 256 characters.
        var route = new PublishRoute("rabbitmq", field == "binding" ? "other" : "local", "v1",
            field == "exchange" ? longValue : RabbitMqPublisher.DefaultExchange, field == "routing-key" ? longValue : queue);
        var headers = field switch
        {
            "header-key" => new Dictionary<string, string> { [longValue] = "v" },
            "traceparent" => new Dictionary<string, string> { ["traceparent"] = "v" },
            "confirm-header" => new Dictionary<string, string> { [Constants.PublishSequenceNumberHeader] = "v" },
            _ => null
        };
        await using var publisher = broker.Publisher();
        var result = await publisher.PublishAsync(BrokerFixture.Message(queue, route: route, headers: headers,
            contentType: field == "content-type" ? longValue : "application/json"));
        Assert.Equal(PublishStatus.NotSentPermanent, result.Status);
        Assert.Null(await broker.Admin.BasicGetAsync(queue, true));
    }

    [Fact]
    public async Task Concurrent_publications_use_bounded_exclusive_channels()
    {
        var queue = await broker.QueueAsync();
        await using var publisher = broker.Publisher(channels: 3);
        var messages = Enumerable.Range(0, 80).Select(_ => BrokerFixture.Message(queue)).ToArray();
        var results = await Task.WhenAll(messages.Select(m => publisher.PublishAsync(m)));
        Assert.All(results, result => Assert.Equal(PublishStatus.Accepted, result.Status));
        var ids = new HashSet<Guid>();
        for (var i = 0; i < messages.Length; i++)
        {
            var received = await broker.Admin.BasicGetAsync(queue, true);
            Assert.NotNull(received);
            Assert.True(ids.Add(Guid.Parse(received.BasicProperties.MessageId!)));
        }
        Assert.Equal(messages.Select(m => m.MessageId).Order(), ids.Order());
        Assert.Null(await broker.Admin.BasicGetAsync(queue, true));
    }

    [Fact]
    public async Task Partial_routing_nack_is_unknown_because_another_queue_received_the_message()
    {
        var exchange = await broker.ExchangeAsync("fanout");
        var healthy = await broker.QueueAsync();
        var full = await broker.QueueAsync(new Dictionary<string, object?> { ["x-max-length"] = 0, ["x-overflow"] = "reject-publish" });
        await broker.Admin.QueueBindAsync(healthy, exchange, "");
        await broker.Admin.QueueBindAsync(full, exchange, "");
        await using var publisher = broker.Publisher();
        var outcome = await publisher.PublishAsync(BrokerFixture.Message(healthy, route: new("rabbitmq", "local", "v1", exchange)));
        Assert.Equal(PublishStatus.Unknown, outcome.Status);
        Assert.NotNull(await broker.Admin.BasicGetAsync(healthy, true));
        Assert.Null(await broker.Admin.BasicGetAsync(full, true));
    }

    [Fact]
    public async Task Lost_publisher_confirm_retries_same_message_and_consumer_effect_occurs_once()
    {
        var queue = await broker.QueueAsync();
        var message = BrokerFixture.Message(queue);
        await broker.EnqueueAsync(message);
        await using var proxy = new AmqpAckDropProxy(publisherConfirm: true);
        await using var connection = await RabbitMqConnection.OpenAsync(BrokerFixture.Factory(proxy.Port));
        await using var publisher = broker.Publisher(connection, channels: 1);
        var retry = new RetryPolicy(initialDelay: TimeSpan.FromMilliseconds(1), maximumDelay: TimeSpan.FromMilliseconds(1), jitterRatio: 0);
        var dispatcher = new OutboxDispatcher(broker.Outbox, message.Scope, [publisher], retry);
        var unknown = await dispatcher.DispatchNextAsync();
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(OutboxState.RetryScheduled, unknown!.Record.State);
        Assert.True(unknown.Record.DeliveryUncertain);
        Assert.Equal(1u, (await broker.Admin.QueueDeclarePassiveAsync(queue)).MessageCount);
        var accepted = await dispatcher.DispatchNextAsync();
        Assert.Equal(OutboxState.Published, accepted!.Record.State);
        Assert.True(accepted.Record.DeliveryUncertain);
        Assert.Equal(2u, (await broker.Admin.QueueDeclarePassiveAsync(queue)).MessageCount);
        var scope = BrokerFixture.Scope();
        await broker.DrainAsync(queue, scope);
        var work = await broker.FindByMessageAsync(scope, message.MessageId);
        Assert.NotNull(work);
        var claim = await broker.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10));
        Assert.NotNull(claim);
        Assert.Equal(message.CopyPayload(), claim.Payload);
        Assert.Equal(ExecutionStatus.Completed, (await broker.Store.ExecuteAsync(claim, broker.Effect, new())).Status);
        Assert.Null(await broker.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
        await using var count = broker.DataSource.CreateCommand($"SELECT count(*) FROM {broker.Schema}.effects WHERE work_id=@id");
        count.Parameters.AddWithValue("id", work.Id);
        Assert.Equal(1L, (long)(await count.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task Cancelled_before_send_and_disposed_publisher_do_not_publish()
    {
        var queue = await broker.QueueAsync();
        var publisher = broker.Publisher();
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        Assert.Equal(PublishStatus.NotSentTransient, (await publisher.PublishAsync(BrokerFixture.Message(queue), cancelled.Token)).Status);
        await publisher.DisposeAsync();
        Assert.Equal(PublishStatus.NotSentTransient, (await publisher.PublishAsync(BrokerFixture.Message(queue))).Status);
        Assert.Null(await broker.Admin.BasicGetAsync(queue, true));
    }

    [Fact]
    public async Task Confirm_timeout_is_unknown_and_a_fresh_channel_can_publish_again()
    {
        var queue = await broker.QueueAsync();
        await using var proxy = new AmqpAckDropProxy(publisherConfirm: true, disconnect: false,
            forwardedConfirmDelay: TimeSpan.FromMilliseconds(400));
        await using var connection = await RabbitMqConnection.OpenAsync(BrokerFixture.Factory(proxy.Port));
        // Exercise the real deadline without requiring healthy channel setup and confirms
        // to finish inside a 250 ms performance budget on a shared CI runner.
        await using var publisher = new RabbitMqPublisher(connection, "local", "v1", channelCount: 1, publishTimeout: TimeSpan.FromSeconds(5));
        var message = BrokerFixture.Message(queue);
        var first = publisher.PublishAsync(message);
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var unknown = await first.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(PublishStatus.Unknown, unknown.Status);
        Assert.Equal("rabbitmq-confirm-unknown", unknown.ReasonCode);
        var recovered = await publisher.PublishAsync(message).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(PublishStatus.Accepted, recovered.Status);
        Assert.Equal("rabbitmq-confirmed", recovered.ReasonCode);
        Assert.Equal(2u, (await broker.Admin.QueueDeclarePassiveAsync(queue)).MessageCount);
        for (var i = 0; i < 2; i++)
        {
            var received = await broker.Admin.BasicGetAsync(queue, true);
            Assert.NotNull(received);
            Assert.Equal(message.MessageId.ToString("D"), received.BasicProperties.MessageId);
            Assert.Equal(message.CopyPayload(), received.Body.ToArray());
        }
    }

    [Fact]
    public async Task Disposal_cancels_waiters_and_drains_the_single_inflight_publication()
    {
        var queue = await broker.QueueAsync();
        await using var proxy = new AmqpAckDropProxy(publisherConfirm: true, disconnect: false);
        await using var connection = await RabbitMqConnection.OpenAsync(BrokerFixture.Factory(proxy.Port));
        await using var publisher = broker.Publisher(connection, channels: 1);
        var sending = Enumerable.Range(0, 8).Select(_ => publisher.PublishAsync(BrokerFixture.Message(queue))).ToArray();
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await publisher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        var results = await Task.WhenAll(sending).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Single(results, r => r.Status == PublishStatus.Unknown);
        Assert.Equal(7, results.Count(r => r.Status == PublishStatus.NotSentTransient));
        Assert.Equal(1u, (await broker.Admin.QueueDeclarePassiveAsync(queue)).MessageCount);
    }

    [Fact]
    public async Task Automatic_client_recovery_is_rejected_explicitly()
    {
        var factory = BrokerFixture.Factory();
        factory.AutomaticRecoveryEnabled = true;
        await Assert.ThrowsAsync<ArgumentException>(() => RabbitMqConnection.OpenAsync(factory));
    }
}
