namespace EventGuard.Runtime;

/// <summary>Applies bounded pages of versioned results with one atomic SQL-only application callback.</summary>
public sealed class ExternalResultBatchWorker<TSession>
{
    private readonly IExternalResultBatchStore<TSession> _store;
    private readonly ExternalResultTarget _target;
    private readonly Func<IReadOnlyList<ExternalActionUpdate>, TSession, CancellationToken, ValueTask> _handler;
    private readonly ExternalResultBatchOptions _batch;
    private readonly WorkPumpOptions _pump;
    private readonly RetryPolicy _retry;

    public ExternalResultBatchWorker(IExternalResultBatchStore<TSession> store, ExternalResultTarget target,
        Func<IReadOnlyList<ExternalActionUpdate>, TSession, CancellationToken, ValueTask> handler,
        ExternalResultBatchOptions? batch = null, RetryPolicy? retry = null, WorkPumpOptions? pump = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _target = target ?? throw new ArgumentNullException(nameof(target));
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        _batch = batch ?? new(); _retry = retry ?? new(); _pump = pump ?? new();
    }

    public Task<ExternalResultBatchExecution> RunOnceAsync(CancellationToken cancellationToken = default) =>
        _store.ExecuteResultBatchAsync(_target, _batch, _handler, _retry, cancellationToken);

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        using (var startup = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
        {
            startup.CancelAfter(_pump.StartupTimeout);
            if (_store is ITransactionalWorkStore<TSession> transactional)
                await StoreStartup.ValidateAsync(transactional, _target.Scope.Tenant, StoreRole.Consumer,
                    _pump.Startup, startup.Token, _pump.StartupTimeout);
            await _store.ValidateExternalResultBatchAsync(startup.Token);
        }
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await Task.WhenAll(Enumerable.Range(0, _pump.MaximumConcurrency).Select(_ => Loop()));
        async Task Loop()
        {
            try
            {
                while (!stop.IsCancellationRequested)
                    if ((await RunOnceAsync(stop.Token)).Works.Count == 0)
                        await Task.Delay(_pump.IdleDelay, stop.Token);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
            catch { stop.Cancel(); throw; }
        }
    }
}
