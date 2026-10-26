using System.Reflection;
using Confluent.Kafka;

namespace EventGuard.Kafka.Tests;

public sealed class PublisherQueueTests
{
    [Fact]
    public async Task Completed_delivery_still_in_native_queue_does_not_reject_the_next_send()
    {
        var native = DispatchProxy.Create<IProducer<byte[], byte[]>, QueueProducer>();
        var queue = (QueueProducer)(object)native;
        await using var publisher = new KafkaPublisher(BrokerFixture.ProducerConfig(), "local", "v1", 1,
            config => { queue.Capacity = config.QueueBufferingMaxMessages!.Value; return native; });
        try
        {
            var first = publisher.PublishAsync(BrokerFixture.Message("topic"));
            queue.Deliver(); // Callback ran, but librdkafka has not destroyed the native message yet.
            Assert.Equal(PublishStatus.Accepted, (await first).Status);
            var next = publisher.PublishAsync(BrokerFixture.Message("topic"));
            queue.ReleaseDelivered();
            queue.DeliverAll();
            Assert.Equal(PublishStatus.Accepted, (await next).Status);
            Assert.Equal(0, queue.QueueFullCount);
            Assert.Equal(2, queue.DeliveredIds.Distinct().Count());
        }
        finally { queue.DeliverAll(); }
    }

    [Fact]
    public async Task Cancellation_does_not_release_the_application_slot_before_delivery()
    {
        var native = DispatchProxy.Create<IProducer<byte[], byte[]>, QueueProducer>();
        var queue = (QueueProducer)(object)native;
        await using var publisher = new KafkaPublisher(BrokerFixture.ProducerConfig(), "local", "v1", 1,
            config => { queue.Capacity = config.QueueBufferingMaxMessages!.Value; return native; });
        try
        {
            using var firstStop = new CancellationTokenSource();
            var first = publisher.PublishAsync(BrokerFixture.Message("topic"), firstStop.Token);
            firstStop.Cancel();
            Assert.Equal(PublishStatus.Unknown, (await first).Status);
            using var nextStop = new CancellationTokenSource();
            var next = publisher.PublishAsync(BrokerFixture.Message("topic"), nextStop.Token);
            Assert.Equal(1, queue.ProduceCount);
            nextStop.Cancel();
            Assert.Equal(PublishStatus.NotSentTransient, (await next).Status);
            queue.DeliverAll();
        }
        finally { queue.DeliverAll(); }
    }

    [Fact]
    public async Task Explicit_native_queue_limit_still_returns_a_known_unsent_result()
    {
        var native = DispatchProxy.Create<IProducer<byte[], byte[]>, QueueProducer>();
        var queue = (QueueProducer)(object)native;
        var config = BrokerFixture.ProducerConfig();
        config.QueueBufferingMaxMessages = 1;
        await using var publisher = new KafkaPublisher(config, "local", "v1", 2,
            effective => { queue.Capacity = effective.QueueBufferingMaxMessages!.Value; return native; });
        try
        {
            var first = publisher.PublishAsync(BrokerFixture.Message("topic"));
            var next = await publisher.PublishAsync(BrokerFixture.Message("topic"));
            queue.DeliverAll();
            Assert.Equal(PublishStatus.NotSentTransient, next.Status);
            Assert.Equal(1, queue.QueueFullCount);
            Assert.Equal(PublishStatus.Accepted, (await first).Status);
        }
        finally { queue.DeliverAll(); }
    }

    // Models the native queue's callback-before-message-destruction order. No broker is used.
    public class QueueProducer : DispatchProxy
    {
        private readonly Queue<(Message<byte[], byte[]> Message, Action<DeliveryReport<byte[], byte[]>> Report)> _pending = new();
        private int _deliveredNotReleased;
        public int Capacity { get; set; }
        public int ProduceCount { get; private set; }
        public int QueueFullCount { get; private set; }
        public List<string> DeliveredIds { get; } = [];

        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method?.Name == nameof(IDisposable.Dispose)) return null;
            if (method?.Name != "Produce" || args is not [string, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> callback])
                throw new NotSupportedException(method?.Name);
            ProduceCount++;
            if (_pending.Count + _deliveredNotReleased >= Capacity)
            {
                QueueFullCount++;
                throw new ProduceException<byte[], byte[]>(new Error(ErrorCode.Local_QueueFull),
                    new DeliveryReport<byte[], byte[]> { Message = message, Status = PersistenceStatus.NotPersisted });
            }
            _pending.Enqueue((message, callback));
            return null;
        }

        public void Deliver()
        {
            var (message, report) = _pending.Dequeue();
            _deliveredNotReleased++;
            DeliveredIds.Add(System.Text.Encoding.UTF8.GetString(message.Headers.GetLastBytes(KafkaPublisher.MessageIdHeader)));
            report(new DeliveryReport<byte[], byte[]> { Message = message, Error = new Error(ErrorCode.NoError), Status = PersistenceStatus.Persisted });
        }

        public void ReleaseDelivered() => _deliveredNotReleased = 0;
        public void DeliverAll()
        {
            while (_pending.Count > 0) Deliver();
            ReleaseDelivered();
        }
    }
}
