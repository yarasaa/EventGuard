using Confluent.Kafka;

namespace EventGuard.Kafka;

/// <summary>Bounded idempotent Kafka publishing; delivery reports govern persistence classification.</summary>
public sealed class KafkaPublisher : IOutboundTransport, IAsyncDisposable
{
    public const string TransportName = "kafka";
    public const string MessageIdHeader = "eventguard-message-id";
    public const string ContentTypeHeader = "content-type";
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly SemaphoreSlim _slots;
    private readonly int _capacity;
    private readonly CancellationTokenSource _stopping = new();
    private readonly object _disposeLock = new();
    private Task? _disposal;
    public string Transport => TransportName;
    public string Connection { get; }
    public string ConfigurationVersion { get; }

    public KafkaPublisher(ProducerConfig configuration, string bindingName, string configurationVersion, int maximumInFlight = 32)
        : this(configuration, bindingName, configurationVersion, maximumInFlight,
            config => new ProducerBuilder<byte[], byte[]>(config).SetLogHandler((_, _) => { }).Build())
    {
    }

    internal KafkaPublisher(ProducerConfig configuration, string bindingName, string configurationVersion, int maximumInFlight,
        Func<ProducerConfig, IProducer<byte[], byte[]>> createProducer)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        if (maximumInFlight is < 1 or > 1024) throw new ArgumentOutOfRangeException(nameof(maximumInFlight));
        var binding = new PublishRoute(TransportName, bindingName, configurationVersion, "binding");
        Connection = binding.Connection;
        ConfigurationVersion = binding.ConfigurationVersion;
        var config = new ProducerConfig(configuration.ToDictionary(p => p.Key, p => p.Value));
        ArgumentException.ThrowIfNullOrWhiteSpace(config.BootstrapServers);
        if (config.EnableIdempotence == false || config.Acks is not (null or Acks.All) || config.TransactionalId is not null)
            throw new ArgumentException("Kafka publishing requires idempotence, acks=all and no transactional.id.", nameof(configuration));
        if (config.EnableDeliveryReports == false || config.EnableBackgroundPoll == false || config.DeliveryReportFields is not null
            || string.Equals(config.Get("delivery.report.only.error"), "true", StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("Complete delivery reports are required.", nameof(configuration));
        if (config.AllowAutoCreateTopics == true) throw new ArgumentException("Provision topics explicitly.", nameof(configuration));
        config.EnableIdempotence = true;
        config.Acks = Acks.All;
        config.AllowAutoCreateTopics = false;
        config.MessageTimeoutMs ??= 10000;
        if (config.MessageTimeoutMs is <= 0 or > 3600000) throw new ArgumentException("Use a finite delivery timeout of at most one hour.", nameof(configuration));
        // librdkafka decrements native queue accounting after its delivery callback returns.
        // Our continuation can release an application slot before that native cleanup runs.
        // Reserve one native slot for the completing report; _slots still bounds active sends.
        config.QueueBufferingMaxMessages ??= maximumInFlight + 1;
        config.QueueBufferingMaxKbytes ??= 8192;
        _slots = new(maximumInFlight, maximumInFlight);
        _capacity = maximumInFlight;
        // Native logs are not a safe application logging contract. Errors are returned through delivery reports.
        _producer = createProducer(config);
    }

    public async Task<PublishOutcome> PublishAsync(OutgoingMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (message.Route.Transport != Transport || message.Route.Connection != Connection || message.Route.ConfigurationVersion != ConfigurationVersion)
            return new(PublishStatus.NotSentPermanent, "kafka-binding-mismatch");
        if (!KafkaEnvelope.ValidTopic(message.Route.Address) || message.Headers.Keys.Any(KafkaEnvelope.ReservedHeader))
            return new(PublishStatus.NotSentPermanent, "kafka-envelope-unsupported");
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stopping.Token);
        try { await _slots.WaitAsync(stopping.Token).ConfigureAwait(false); }
        catch (OperationCanceledException) { return new(PublishStatus.NotSentTransient, "kafka-before-publish-cancelled"); }
        // The slot belongs to the native delivery task, including after the caller stops waiting.
        // Cancellation cannot remove an enqueued record and must not permit unbounded native outstanding sends.
        var sending = SendAsync(message, stopping.Token);
        try { return await sending.WaitAsync(stopping.Token).ConfigureAwait(false); }
        catch (OperationCanceledException) { return new(PublishStatus.Unknown, "kafka-delivery-unknown"); }
    }

    private async Task<PublishOutcome> SendAsync(OutgoingMessage message, CancellationToken beforeEnqueue)
    {
        var invoked = false;
        try
        {
            beforeEnqueue.ThrowIfCancellationRequested();
            var headers = new Headers();
            foreach (var (key, value) in message.Headers) headers.Add(key, KafkaEnvelope.Utf8.GetBytes(value));
            headers.Add(MessageIdHeader, KafkaEnvelope.Utf8.GetBytes(message.MessageId.ToString("D")));
            headers.Add(ContentTypeHeader, KafkaEnvelope.Utf8.GetBytes(message.ContentType));
            var native = new Message<byte[], byte[]>
            {
                Key = message.Route.RoutingKey is { } routingKey ? KafkaEnvelope.Utf8.GetBytes(routingKey) : null!,
                Value = message.CopyPayload(),
                Headers = headers
            };
            beforeEnqueue.ThrowIfCancellationRequested();
            invoked = true;
            // Native timeout completes this task even when the caller has cancelled. No Kafka transaction is used.
            // v2.15.1 ProduceAsync rewraps KafkaException and loses the delivery persistence status.
            // The callback retains the actual native status, including PossiblyPersisted on a lost reply.
            var receipt = new TaskCompletionSource<DeliveryReport<byte[], byte[]>>(TaskCreationOptions.RunContinuationsAsynchronously);
            _producer.Produce(message.Route.Address, native, report => receipt.TrySetResult(report));
            var delivery = await receipt.Task.ConfigureAwait(false);
            return Outcome(delivery.Error, delivery.Status);
        }
        catch (ProduceException<byte[], byte[]> error)
        {
            return Outcome(error.Error, error.DeliveryResult.Status);
        }
        catch (Exception)
        {
            return invoked ? new(PublishStatus.Unknown, "kafka-delivery-unknown")
                : new(PublishStatus.NotSentTransient, "kafka-before-publish-failed");
        }
        finally { _slots.Release(); }
    }

    private static PublishOutcome Outcome(Error error, PersistenceStatus status)
    {
        if (!error.IsError && status == PersistenceStatus.Persisted) return new(PublishStatus.Accepted, "kafka-persisted");
        if (status != PersistenceStatus.NotPersisted) return new(PublishStatus.Unknown, "kafka-delivery-unknown");
        var permanent = error.Code is ErrorCode.MsgSizeTooLarge or ErrorCode.TopicAuthorizationFailed
            or ErrorCode.ClusterAuthorizationFailed or ErrorCode.InvalidMsg or ErrorCode.InvalidMsgSize;
        return new(permanent ? PublishStatus.NotSentPermanent : PublishStatus.NotSentTransient, "kafka-not-persisted");
    }

    public ValueTask DisposeAsync()
    {
        lock (_disposeLock) return new(_disposal ??= DisposeCoreAsync());
    }
    private async Task DisposeCoreAsync()
    {
        await _stopping.CancelAsync().ConfigureAwait(false);
        for (var i = 0; i < _capacity; i++) await _slots.WaitAsync().ConfigureAwait(false);
        _producer.Dispose();
        // Keep the cancelled token and semaphore readable by late callers.
    }
}
