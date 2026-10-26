using System.Text;
using System.Threading.Channels;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace EventGuard.RabbitMq;

/// <summary>Persistent, mandatory AMQP publications with tracked publisher confirms.</summary>
public sealed class RabbitMqPublisher : IOutboundTransport, IAsyncDisposable
{
    public const string TransportName = "rabbitmq";
    public const string DefaultExchange = "amq.default";
    private readonly RabbitMqConnection _connection;
    private readonly Channel<Slot> _slots;
    private readonly int _capacity;
    private readonly TimeSpan _publishTimeout;
    private readonly CancellationTokenSource _stopping = new();
    private readonly object _disposeLock = new();
    private Task? _disposal;
    public string Transport => TransportName;
    public string Connection { get; }
    public string ConfigurationVersion { get; }

    public RabbitMqPublisher(RabbitMqConnection connection, string bindingName, string configurationVersion, int channelCount = 4,
        TimeSpan? publishTimeout = null)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        // Apply the same identity rules as durable route bindings.
        var binding = new PublishRoute(TransportName, bindingName, configurationVersion, DefaultExchange);
        Connection = binding.Connection;
        ConfigurationVersion = binding.ConfigurationVersion;
        if (channelCount is < 1 or > 256) throw new ArgumentOutOfRangeException(nameof(channelCount));
        _capacity = channelCount;
        _publishTimeout = publishTimeout ?? TimeSpan.FromSeconds(10);
        if (_publishTimeout <= TimeSpan.Zero || _publishTimeout > TimeSpan.FromHours(1)) throw new ArgumentOutOfRangeException(nameof(publishTimeout));
        _slots = Channel.CreateBounded<Slot>(new BoundedChannelOptions(channelCount) { FullMode = BoundedChannelFullMode.Wait });
        for (var i = 0; i < channelCount; i++) _slots.Writer.TryWrite(new());
    }

    public async Task<PublishOutcome> PublishAsync(OutgoingMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (message.Route.Transport != Transport || message.Route.Connection != Connection || message.Route.ConfigurationVersion != ConfigurationVersion)
            return new(PublishStatus.NotSentPermanent, "rabbitmq-binding-mismatch");
        if (!ShortString(message.Route.Address) || !ShortString(message.Route.RoutingKey ?? "") || !ShortString(message.ContentType)
            || message.Headers.Keys.Any(k => !ShortString(k) || ReservedHeader(k)))
            return new(PublishStatus.NotSentPermanent, "rabbitmq-envelope-unsupported");

        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stopping.Token);
        deadline.CancelAfter(_publishTimeout);
        Slot? slot = null;
        var invoked = false;
        var discard = false;
        try
        {
            slot = await _slots.Reader.ReadAsync(deadline.Token).ConfigureAwait(false);
            var connection = await _connection.GetAsync(deadline.Token).ConfigureAwait(false);
            if (slot.Connection != connection || slot.Channel is not { IsOpen: true })
            {
                await slot.ResetAsync().ConfigureAwait(false);
                slot.Channel = await connection.CreateChannelAsync(new CreateChannelOptions(true, true), deadline.Token).ConfigureAwait(false);
                slot.Connection = connection;
            }
            var properties = new BasicProperties
            {
                MessageId = message.MessageId.ToString("D"),
                ContentType = message.ContentType,
                Persistent = true,
                Headers = message.Headers.ToDictionary(p => p.Key, p => (object?)Encoding.UTF8.GetBytes(p.Value), StringComparer.Ordinal)
            };
            var body = message.CopyPayload();
            deadline.Token.ThrowIfCancellationRequested();
            invoked = true;
            await slot.Channel.BasicPublishAsync(message.Route.Address == DefaultExchange ? "" : message.Route.Address,
                message.Route.RoutingKey ?? "", mandatory: true, properties, body, deadline.Token).ConfigureAwait(false);
            return new(PublishStatus.Accepted, "rabbitmq-confirmed");
        }
        catch (PublishException exception) when (exception.IsReturn)
        {
            return new(PublishStatus.NotSentPermanent, "rabbitmq-unroutable");
        }
        catch (Exception)
        {
            // A nack may follow partial routing. Timeout/connection loss may follow broker acceptance.
            discard = true;
            return invoked ? new(PublishStatus.Unknown, "rabbitmq-confirm-unknown")
                : new(PublishStatus.NotSentTransient, "rabbitmq-before-publish-failed");
        }
        finally
        {
            if (slot is not null)
            {
                if (discard) await slot.ResetAsync().ConfigureAwait(false);
                _slots.Writer.TryWrite(slot);
            }
        }
    }

    private static bool ShortString(string value) => Encoding.UTF8.GetByteCount(value) <= 255;
    private static bool ReservedHeader(string key) => key == Constants.PublishSequenceNumberHeader
        || key.Equals("traceparent", StringComparison.OrdinalIgnoreCase)
        || key.Equals("tracestate", StringComparison.OrdinalIgnoreCase)
        || key.Equals("baggage", StringComparison.OrdinalIgnoreCase);

    public ValueTask DisposeAsync()
    {
        lock (_disposeLock) return new(_disposal ??= DisposeCoreAsync());
    }

    private async Task DisposeCoreAsync()
    {
        await _stopping.CancelAsync().ConfigureAwait(false);
        // Cancellation releases all active publishers; waiters cannot take another slot.
        for (var i = 0; i < _capacity; i++)
        {
            var slot = await _slots.Reader.ReadAsync().ConfigureAwait(false);
            await slot.ResetAsync().ConfigureAwait(false);
        }
        // Keep the cancellation source readable so a late call returns NotSentTransient.
    }

    private sealed class Slot
    {
        public IConnection? Connection { get; set; }
        public IChannel? Channel { get; set; }
        public async ValueTask ResetAsync()
        {
            var channel = Channel;
            Channel = null;
            Connection = null;
            if (channel is null) return;
            try { await channel.DisposeAsync().ConfigureAwait(false); }
            catch (Exception) { /* Cleanup cannot turn an unknown send into a known failure. */ }
        }
    }
}
