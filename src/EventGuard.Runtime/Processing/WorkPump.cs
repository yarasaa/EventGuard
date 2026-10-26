namespace EventGuard.Runtime;

// Host-agnostic: await RunAsync(stoppingToken) from a Worker Service, console, or BackgroundService.
// This is bounded local concurrency, not a distributed rate limiter.
public sealed class WorkPump<TSession>
{
    private readonly ITransactionalWorkStore<TSession> _store;
    private readonly WorkScope _scope;
    private readonly Func<WorkItem, ReadOnlyMemory<byte>, TSession, CancellationToken, ValueTask> _handler;
    private readonly RetryPolicy _retry;
    private readonly WorkPumpOptions _options;
    private readonly Action<Exception>? _onFault;
    private readonly RuntimeTelemetry? _telemetry;

    public WorkPump(ITransactionalWorkStore<TSession> store, WorkScope scope,
        Func<WorkItem, ReadOnlyMemory<byte>, TSession, CancellationToken, ValueTask> handler,
        RetryPolicy? retry = null, WorkPumpOptions? options = null, Action<Exception>? onFault = null, RuntimeTelemetry? telemetry = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _scope = scope ?? throw new ArgumentNullException(nameof(scope));
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        _retry = retry ?? new();
        _options = options ?? new();
        _onFault = onFault;
        _telemetry = telemetry;
    }

    public async Task RunAsync(CancellationToken stoppingToken)
    {
        await StoreStartup.ValidateAsync(_store, _scope.Tenant, StoreRole.Consumer, _options.Startup, stoppingToken);
        using var shutdown = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var workers = Enumerable.Range(0, _options.MaximumConcurrency).Select(_ => SuperviseAsync()).ToArray();
        await Task.WhenAll(workers);

        async Task SuperviseAsync()
        {
            _telemetry?.StartLoop("consumer");
            try { await RunWorkerAsync(shutdown.Token); }
            catch (OperationCanceledException) when (shutdown.IsCancellationRequested) { }
            catch
            {
                // A fault must not leave sibling loops running after the caller observes failure.
                shutdown.Cancel();
                throw;
            }
            finally { _telemetry?.StopLoop("consumer"); }
        }
    }

    private async Task RunWorkerAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            WorkClaim? claim;
            try
            {
                var started = _telemetry?.Begin("consumer") ?? 0;
                var outcome = "unconfirmed";
                try
                {
                    claim = await _store.ClaimNextAsync(_scope, Guid.NewGuid(), _options.LeaseDuration, ct);
                    outcome = claim is null ? "idle" : RuntimeTelemetry.WorkOutcome((await _store.ExecuteAsync(claim, _handler, _retry, ct)).Status);
                }
                finally { _telemetry?.End("consumer", outcome, started); }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
            catch (Exception error) when (_onFault is not null)
            {
                // Failed claims/executions remain durable; recovery still follows store locking rules.
                _onFault(error);
                await Task.Delay(_options.FaultDelay, ct);
                continue;
            }
            if (claim is null) await Task.Delay(_options.IdleDelay, ct);
        }
    }
}
