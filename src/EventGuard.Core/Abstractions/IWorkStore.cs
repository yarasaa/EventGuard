namespace EventGuard;

public interface IWorkStore
{
    Task<AcceptanceResult> AcceptAsync(WorkSubmission submission, CancellationToken cancellationToken = default);
    Task<WorkItem?> FindAsync(WorkScope scope, Guid workId, CancellationToken cancellationToken = default);
    Task<WorkClaim?> ClaimNextAsync(WorkScope scope, Guid claimRequestId, TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);
    Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken cancellationToken = default);
}

// TSession is supplied by the provider; Core never requires DbConnection, DbContext or broker types.
public interface ITransactionalWorkStore<TSession> : IWorkStore
{
    Task<ExecutionResult> ExecuteAsync(WorkClaim claim,
        Func<WorkItem, ReadOnlyMemory<byte>, TSession, CancellationToken, ValueTask> handler,
        RetryPolicy retryPolicy, CancellationToken cancellationToken = default);
}
