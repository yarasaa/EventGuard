namespace EventGuard.Runtime;

/// <summary>Durably retries application SQL/status updates without calling the remote service again.</summary>
public sealed class ExternalResultWorker<TSession> where TSession : IExternalResultSession
{
    private readonly WorkPump<TSession> _pump;
    public ExternalResultWorker(ITransactionalWorkStore<TSession> store, ExternalResultTarget target,
        Func<ExternalActionUpdate, TSession, CancellationToken, ValueTask> handler,
        RetryPolicy? retryPolicy = null, WorkPumpOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(store); ArgumentNullException.ThrowIfNull(target); ArgumentNullException.ThrowIfNull(handler);
        _pump = new(store, target.WorkScope, async (_, _, session, ct) =>
        {
            await session.ExternalResults.ApplyAsync(target, (update, token) => handler(update, session, token), ct);
        }, retryPolicy, options);
    }
    public Task RunAsync(CancellationToken cancellationToken = default) => _pump.RunAsync(cancellationToken);
}
