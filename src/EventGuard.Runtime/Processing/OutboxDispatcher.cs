namespace EventGuard.Runtime;

// The transport must be safe for concurrent calls and honor cancellation.
public sealed class OutboxDispatcher
{
    private readonly IOutboxStore _store;
    private readonly ProducerScope _scope;
    private readonly Dictionary<(string, string, string), IOutboundTransport> _transports = new();
    private readonly RetryPolicy _retry;
    private readonly WorkPumpOptions _options;
    private readonly RuntimeTelemetry? _telemetry;

    public OutboxDispatcher(IOutboxStore store, ProducerScope scope, IEnumerable<IOutboundTransport> transports,
        RetryPolicy? retry = null, WorkPumpOptions? options = null, RuntimeTelemetry? telemetry = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _scope = scope ?? throw new ArgumentNullException(nameof(scope));
        ArgumentNullException.ThrowIfNull(transports);
        _retry = retry ?? new();
        _options = options ?? new();
        _telemetry = telemetry;
        foreach (var transport in transports)
        {
            ArgumentNullException.ThrowIfNull(transport);
            var key = (transport.Transport, transport.Connection, transport.ConfigurationVersion);
            if (!_transports.TryAdd(key, transport)) throw new ArgumentException("Duplicate transport binding.", nameof(transports));
        }
    }

    public async Task<OutboxSettlement?> DispatchNextAsync(CancellationToken cancellationToken = default)
    {
        var started = _telemetry?.Begin("outbox") ?? 0;
        var result = "unconfirmed";
        try
        {
            var settlement = await DispatchCoreAsync(cancellationToken);
            result = settlement is null ? "idle" : RuntimeTelemetry.OutboxOutcome(settlement);
            return settlement;
        }
        finally { _telemetry?.End("outbox", result, started); }
    }

    private async Task<OutboxSettlement?> DispatchCoreAsync(CancellationToken cancellationToken)
    {
        var claim = await _store.ClaimNextAsync(_scope, Guid.NewGuid(), _options.LeaseDuration, cancellationToken);
        if (claim is null) return null;
        var message = claim.Record.Message;
        var route = message.Route;
        PublishOutcome outcome;
        if (!_transports.TryGetValue((route.Transport, route.Connection, route.ConfigurationVersion), out var transport))
            outcome = new(PublishStatus.NotSentPermanent, "transport-binding-missing");
        else if (cancellationToken.IsCancellationRequested)
            outcome = new(PublishStatus.NotSentTransient, "cancelled-before-send");
        else
        {
            // No DB transaction is held while waiting on the external transport.
            using var send = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            send.CancelAfter(_options.LeaseDuration);
            try
            {
                outcome = await transport.PublishAsync(message, send.Token)
                    ?? new(PublishStatus.Unknown, "publish-outcome-missing");
            }
            catch (Exception)
            {
                // An exception/cancellation alone cannot prove that the broker did not accept the message.
                outcome = new(PublishStatus.Unknown, "publish-outcome-unknown");
            }
        }
        // A stopped host still gets a bounded opportunity to persist the observed result.
        using var settlement = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        return await _store.SettleAsync(claim, outcome, _retry, settlement.Token);
    }

    public async Task RunAsync(CancellationToken stoppingToken)
    {
        await StoreStartup.ValidateAsync(_store, _scope.Tenant, StoreRole.Outbox, _options.Startup, stoppingToken);
        using var shutdown = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        await Task.WhenAll(Enumerable.Range(0, _options.MaximumConcurrency).Select(_ => WorkerAsync()));

        async Task WorkerAsync()
        {
            _telemetry?.StartLoop("outbox");
            try
            {
                while (!shutdown.IsCancellationRequested)
                    if (await DispatchNextAsync(shutdown.Token) is null)
                        await Task.Delay(_options.IdleDelay, shutdown.Token);
            }
            catch (OperationCanceledException) when (shutdown.IsCancellationRequested) { }
            catch
            {
                // Infrastructure failure is visible to the host; do not leave sibling workers orphaned.
                shutdown.Cancel();
                throw;
            }
            finally { _telemetry?.StopLoop("outbox"); }
        }
    }
}
