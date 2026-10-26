using System.Buffers.Binary;
using System.Diagnostics;
namespace EventGuard.LoadTests;

internal sealed class MeasuredStore<TSession>(ITransactionalWorkStore<TSession> inner, int count, CancellationToken operationDeadline,
    Func<int,long>? plannedTimestamp = null) : ITransactionalWorkStore<TSession>
{
    private int _expectedCompletions = count;
    public LatencyHistogram Admission { get; } = new();
    public LatencyHistogram Claim { get; } = new();
    public LatencyHistogram EmptyClaim { get; } = new();
    public LatencyHistogram Execution { get; } = new();
    public LatencyHistogram SubmissionToCompletion { get; } = new();
    public LatencyHistogram DrainToCompletion { get; } = new();
    public LatencyHistogram ScheduledToCompletion { get; } = new();
    public long[] SubmissionStarted { get; } = new long[count];
    public long[] Acceptance { get; } = new long[3];
    public long[] ExecutionOutcomes { get; } = new long[5];
    public long UnconfirmedAdmission, UnconfirmedClaim, UnconfirmedExecution, CancelledClaims, DrainStarted, LastCompletion;
    public TaskCompletionSource Finished { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    // Only change after arrival admission has stopped; declared generator rejections never entered the store.
    public void ExpectCompletions(int expected)
    {
        if (expected < 0 || expected > SubmissionStarted.Length) throw new ArgumentOutOfRangeException(nameof(expected));
        Volatile.Write(ref _expectedCompletions,expected);
        if (Interlocked.Read(ref ExecutionOutcomes[0]) == expected) Finished.TrySetResult();
    }
    public async Task<AcceptanceResult> AcceptAsync(WorkSubmission input, CancellationToken ct = default)
    {
        var started = Stopwatch.GetTimestamp();
        try { var result = await inner.AcceptAsync(input, ct); Interlocked.Increment(ref Acceptance[(int)result.Status]); return result; }
        catch { Interlocked.Increment(ref UnconfirmedAdmission); throw; }
        finally { Admission.Record(started); }
    }
    public async Task<WorkClaim?> ClaimNextAsync(WorkScope scope, Guid request, TimeSpan lease, CancellationToken ct = default)
    {
        // Normal finite-run shutdown stops between operations. An already started native
        // operation retains the case deadline, so completion does not inject a new fault.
        ct.ThrowIfCancellationRequested();
        var started = Stopwatch.GetTimestamp();
        try { var result = await inner.ClaimNextAsync(scope, request, lease, operationDeadline); (result is null ? EmptyClaim : Claim).Record(started); return result; }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { Interlocked.Increment(ref CancelledClaims); throw; }
        catch { Interlocked.Increment(ref UnconfirmedClaim); throw; }
    }
    public async Task<ExecutionResult> ExecuteAsync(WorkClaim claim,
        Func<WorkItem,ReadOnlyMemory<byte>,TSession,CancellationToken,ValueTask> handler, RetryPolicy retry, CancellationToken ct = default)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            var result = await inner.ExecuteAsync(claim, handler, retry, operationDeadline);
            if (result.Status == ExecutionStatus.Completed)
            {
                var index = BinaryPrimitives.ReadInt32LittleEndian(claim.Payload);
                SubmissionToCompletion.Record(SubmissionStarted[index]); DrainToCompletion.Record(DrainStarted);
                if (plannedTimestamp?.Invoke(index) is > 0 and var scheduled) ScheduledToCompletion.Record(scheduled);
                var ended = Stopwatch.GetTimestamp(); long previous;
                do { previous = Interlocked.Read(ref LastCompletion); if (previous >= ended) break; }
                while (Interlocked.CompareExchange(ref LastCompletion, ended, previous) != previous);
            }
            var finished = Interlocked.Increment(ref ExecutionOutcomes[(int)result.Status]);
            if (result.Status == ExecutionStatus.Completed && finished == Volatile.Read(ref _expectedCompletions)) Finished.TrySetResult();
            return result;
        }
        catch { Interlocked.Increment(ref UnconfirmedExecution); throw; }
        finally { Execution.Record(started); }
    }
    public Task<WorkItem?> FindAsync(WorkScope scope, Guid id, CancellationToken ct = default) => inner.FindAsync(scope,id,ct);
    public Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken ct = default) => inner.ReplayAsync(request,ct);
}
