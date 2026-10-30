namespace EventGuard;

public sealed class ExternalResultBatchOptions
{
    public int MaximumItems { get; }
    public int MaximumBytes { get; }
    public TimeSpan ExecutionTimeout { get; }
    public TimeSpan FailureRecordingTimeout { get; }
    public ExternalResultBatchOptions(int maximumItems = 100, int maximumBytes = 8 * 1024 * 1024,
        TimeSpan? executionTimeout = null, TimeSpan? failureRecordingTimeout = null)
    {
        if (maximumItems is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(maximumItems));
        if (maximumBytes < 1024 * 1024 + 128 * 1024 || maximumBytes > 64 * 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(maximumBytes), "Use 1.125–64 MiB including action and result bytes.");
        MaximumItems = maximumItems; MaximumBytes = maximumBytes;
        ExecutionTimeout = executionTimeout ?? TimeSpan.FromSeconds(30);
        FailureRecordingTimeout = failureRecordingTimeout ?? TimeSpan.FromSeconds(10);
        if (ExecutionTimeout <= TimeSpan.Zero || ExecutionTimeout > TimeSpan.FromHours(1))
            throw new ArgumentOutOfRangeException(nameof(executionTimeout));
        if (FailureRecordingTimeout <= TimeSpan.Zero || FailureRecordingTimeout > TimeSpan.FromHours(1))
            throw new ArgumentOutOfRangeException(nameof(failureRecordingTimeout));
    }
}

public sealed record ExternalResultBatchExecution(IReadOnlyList<ExecutionResult> Works, int Applied, int Superseded);

public interface IExternalResultBatchStore<TSession>
{
    Task ValidateExternalResultBatchAsync(CancellationToken cancellationToken = default);
    /// <summary>One atomic SQL-only application callback for the newest unapplied version of each action in a bounded page.
    /// All work outcomes, position advances and application SQL share one transaction. No external I/O in the handler.</summary>
    Task<ExternalResultBatchExecution> ExecuteResultBatchAsync(ExternalResultTarget target, ExternalResultBatchOptions options,
        Func<IReadOnlyList<ExternalActionUpdate>, TSession, CancellationToken, ValueTask> handler, RetryPolicy retry,
        CancellationToken cancellationToken = default);
}
