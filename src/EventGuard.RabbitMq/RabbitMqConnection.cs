using RabbitMQ.Client;

namespace EventGuard.RabbitMq;

/// <summary>Owns a connection. Reconnects between operations; never resends a publication.</summary>
public sealed class RabbitMqConnection : IAsyncDisposable
{
    private readonly ConnectionFactory _factory;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IConnection? _connection;
    private bool _disposed;

    private RabbitMqConnection(ConnectionFactory factory) => _factory = factory;

    // The factory must remain unchanged for this binding's lifetime. Stop users before disposal.
    public static async Task<RabbitMqConnection> OpenAsync(ConnectionFactory factory, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        if (factory.AutomaticRecoveryEnabled)
            throw new ArgumentException("Disable automatic recovery; EventGuard creates fresh channels between attempts.", nameof(factory));
        var owner = new RabbitMqConnection(factory);
        await owner.GetAsync(cancellationToken).ConfigureAwait(false);
        return owner;
    }

    internal async Task<IConnection> GetAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_connection is { IsOpen: true }) return _connection;
            if (_connection is not null) await _connection.DisposeAsync().ConfigureAwait(false);
            _connection = null;
            if (_factory.AutomaticRecoveryEnabled)
                throw new InvalidOperationException("The connection factory was changed after binding.");
            _connection = await _factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
            return _connection;
        }
        finally { _gate.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed) return;
            _disposed = true;
            if (_connection is not null) await _connection.DisposeAsync().ConfigureAwait(false);
        }
        finally { _gate.Release(); }
    }
}
