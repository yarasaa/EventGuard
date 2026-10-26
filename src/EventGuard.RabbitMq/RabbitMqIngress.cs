using System.Runtime.ExceptionServices;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace EventGuard.RabbitMq;

/// <summary>Copies deliveries into a durable work store before acknowledging them individually.</summary>
public sealed class RabbitMqIngress
{
    private readonly RabbitMqConnection _connection;
    private readonly IWorkStore _store;
    private readonly WorkScope _scope;
    private readonly IdentityPolicy _policy;
    private readonly string _queue;
    private readonly ushort _prefetch;
    private readonly Func<BasicDeliverEventArgs, WorkSubmission> _map;
    private readonly IngressQuarantineOptions? _quarantine;
    private readonly IIngressQuarantineStore? _quarantineStore;
    private int _running;

    // A mapper runs synchronously. RabbitMQ's delivery body/properties must not escape its callback.
    public RabbitMqIngress(RabbitMqConnection connection, IWorkStore store, WorkScope scope, IdentityPolicy policy,
        string queue, Func<BasicDeliverEventArgs, WorkSubmission>? map = null, ushort prefetch = 16,
        IngressQuarantineOptions? quarantine = null)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _quarantine = quarantine;
        _quarantineStore = quarantine is null ? null : store as IIngressQuarantineStore
            ?? throw new ArgumentException("Opt-in quarantine requires a durable ingress quarantine store.", nameof(store));
        _scope = scope ?? throw new ArgumentNullException(nameof(scope));
        _policy = policy ?? throw new ArgumentNullException(nameof(policy));
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        if (new UTF8Encoding(false, true).GetByteCount(queue) > 255) throw new ArgumentException("Queue name exceeds AMQP short-string limit.", nameof(queue));
        if (prefetch is < 1 or > 1024) throw new ArgumentOutOfRangeException(nameof(prefetch));
        _queue = queue;
        _prefetch = prefetch;
        _map = map ?? MapByMessageId;
    }

    private WorkSubmission MapByMessageId(BasicDeliverEventArgs delivery)
    {
        if (!Guid.TryParse(delivery.BasicProperties.MessageId, out var id) || id == Guid.Empty)
            throw new IngressMessageRejectedException("invalid-message-id");
        if (delivery.BasicProperties.CorrelationId?.Length > 256) throw new IngressMessageRejectedException("invalid-correlation-id");
        return new(new(_scope, _policy, id.ToString("D")), id, delivery.Body.Span,
            ContentFingerprint.FromBytes(delivery.Body.Span), correlationId: delivery.BasicProperties.CorrelationId);
    }

    // One channel, sequential callbacks, bounded prefetch. Run separate instances for parallel ingestion.
    // On failure the host must observe the exception and choose a restart/backoff policy.
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref _running, 1, 0) != 0)
            throw new InvalidOperationException("This ingress is already running.");
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        // A server-closed channel's DisposeAsync need not await an in-flight application callback.
        var callbackGate = new SemaphoreSlim(1, 1);
        var failure = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        IChannel? channel = null;
        try
        {
            var connection = await _connection.GetAsync(stopping.Token).ConfigureAwait(false);
            channel = await connection.CreateChannelAsync(new CreateChannelOptions(false, false, consumerDispatchConcurrency: 1), stopping.Token).ConfigureAwait(false);
            channel.ChannelShutdownAsync += (_, _) =>
            {
                failure.TrySetResult(new IOException("RabbitMQ ingress channel closed; unacknowledged deliveries require recovery."));
                return Task.CompletedTask;
            };
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.UnregisteredAsync += (_, _) =>
            {
                failure.TrySetResult(new IOException("RabbitMQ cancelled the ingress subscription."));
                return Task.CompletedTask;
            };
            consumer.ReceivedAsync += async (_, delivery) =>
            {
                await callbackGate.WaitAsync().ConfigureAwait(false);
                try
                {
                    if (stopping.IsCancellationRequested || failure.Task.IsCompleted) return;
                    if (delivery.Body.Length > 1024 * 1024) throw new InvalidOperationException("Ingress inline payload exceeds 1 MiB.");
                    var deliveryTag = delivery.DeliveryTag;
                    var snapshot = _quarantine is null ? null : RabbitMqIngressSnapshot.Capture(delivery, _queue, _quarantine);
                    WorkSubmission? submission = null;
                    IngressMessageRejectedException? rejected = null;
                    try { submission = _map(delivery) ?? throw new InvalidOperationException("Ingress mapper returned no submission."); }
                    catch (IngressMessageRejectedException error) when (snapshot is not null) { rejected = error; }
                    if (rejected is not null)
                    {
                        var quarantined = await _quarantineStore!.QuarantineAsync(_scope, _policy, snapshot!, rejected.ReasonCode, stopping.Token).ConfigureAwait(false);
                        if (quarantined is null || quarantined.Status is not (IngressQuarantineStatus.Recorded or IngressQuarantineStatus.Duplicate))
                            throw new InvalidOperationException("Ingress quarantine did not durably preserve this input.");
                    }
                    else
                    {
                        if (submission!.Identity.Scope != _scope || submission.Identity.Policy != _policy)
                            throw new InvalidOperationException("Ingress mapping changed its configured scope or identity policy.");
                        var result = await _store.AcceptAsync(submission, stopping.Token).ConfigureAwait(false);
                        if (result is null || !Enum.IsDefined(result.Status)) throw new InvalidOperationException("Store returned no durable acceptance outcome.");
                    }
                    stopping.Token.ThrowIfCancellationRequested();
                    await channel.BasicAckAsync(deliveryTag, multiple: false, stopping.Token).ConfigureAwait(false);
                }
                catch (Exception exception) { failure.TrySetResult(exception); }
                finally { callbackGate.Release(); }
            };
            await channel.BasicQosAsync(0, _prefetch, global: false, stopping.Token).ConfigureAwait(false);
            await channel.BasicConsumeAsync(_queue, autoAck: false, consumer, stopping.Token).ConfigureAwait(false);
            var exception = await failure.Task.WaitAsync(stopping.Token).ConfigureAwait(false);
            ExceptionDispatchInfo.Capture(exception).Throw();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        finally
        {
            await stopping.CancelAsync().ConfigureAwait(false);
            // Close outside callbacks: shutdown waits for consumer dispatch to finish.
            try
            {
                if (channel is not null) await channel.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                await callbackGate.WaitAsync().ConfigureAwait(false);
                callbackGate.Release();
                Volatile.Write(ref _running, 0);
            }
        }
    }
}
