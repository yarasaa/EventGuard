namespace EventGuard;

public sealed class ExternalActionPolicy
{
    public int MaximumSubmissionAttempts { get; }
    public TimeSpan RetryDelay { get; }
    public TimeSpan ObservationInterval { get; }
    public ExternalObservationSchedule? ObservationSchedule { get; }
    public ExternalActionPolicy(int maximumSubmissionAttempts = 5, TimeSpan? retryDelay = null, TimeSpan? observationInterval = null)
    {
        if (maximumSubmissionAttempts is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(maximumSubmissionAttempts));
        MaximumSubmissionAttempts = maximumSubmissionAttempts;
        RetryDelay = retryDelay ?? TimeSpan.FromSeconds(30); ObservationInterval = observationInterval ?? TimeSpan.FromMinutes(1);
        if (RetryDelay < TimeSpan.FromMilliseconds(10) || RetryDelay > TimeSpan.FromDays(1)
            || ObservationInterval < TimeSpan.FromMilliseconds(10) || ObservationInterval > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(retryDelay));
    }

    public ExternalActionPolicy(ExternalObservationSchedule observationSchedule,
        int maximumSubmissionAttempts = 5, TimeSpan? retryDelay = null)
        : this(maximumSubmissionAttempts, retryDelay,
            (observationSchedule ?? throw new ArgumentNullException(nameof(observationSchedule))).Steps[0].Interval)
        => ObservationSchedule = observationSchedule;

    internal DateTimeOffset NextObservationAt(ExternalActionRecord record, DateTimeOffset now)
    {
        var elapsed = record.ObservationDeadline is { } deadline && record.Action.ObservationTimeout is { } duration
            ? now - (deadline - duration) : TimeSpan.Zero;
        var next = now + (ObservationSchedule?.IntervalAt(elapsed) ?? ObservationInterval);
        // Wake at the deadline even when the selected interval extends beyond it.
        return record.ObservationDeadline is { } end && next > end ? end : next;
    }
}

public static class ExternalActionLifecycle
{
    public static ExternalActionRecord Settle(ExternalActionRecord before, ExternalPhase phase, ExternalResult result,
        ExternalActionPolicy policy, DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(before); ArgumentNullException.ThrowIfNull(result); ArgumentNullException.ThrowIfNull(policy);
        if (!Enum.IsDefined(phase) || before.Version < 0) throw new ArgumentException("Invalid action transition.");
        var after = before with { Version = checked(before.Version + 1), LastReason = result.Reason };
        if (phase == ExternalPhase.Observe)
        {
            if (before.State != ExternalActionState.Accepted || before.Observation != ExternalObservationState.Pending)
                throw new InvalidOperationException("Observation requires a pending accepted action.");
            if (result.Kind is not (ExternalResultKind.Pending or ExternalResultKind.Unknown or ExternalResultKind.Succeeded or ExternalResultKind.Failed))
                throw new InvalidOperationException("An observation cannot change submission acceptance.");
            var observation = result.Kind switch
            {
                ExternalResultKind.Succeeded => ExternalObservationState.Succeeded,
                ExternalResultKind.Failed => ExternalObservationState.Failed,
                _ when before.ObservationDeadline <= now => ExternalObservationState.TimedOut,
                _ => ExternalObservationState.Pending
            };
            return after with { Observation = observation, NextAttemptAt = policy.NextObservationAt(before, now),
                ResultData = result.CopyData() };
        }
        if (phase == ExternalPhase.Submit && before.State != ExternalActionState.Sending
            || phase == ExternalPhase.Reconcile && before.State != ExternalActionState.Unknown)
            throw new InvalidOperationException("Action is not owned for this phase.");
        if (result.Kind is ExternalResultKind.Pending or ExternalResultKind.Succeeded or ExternalResultKind.Failed)
            throw new InvalidOperationException("Submission/reconciliation needs explicit acceptance evidence.");
        return result.Kind switch
        {
            ExternalResultKind.Accepted => after with
            {
                State = ExternalActionState.Accepted, ProviderReference = result.Reference, ResultData = result.CopyData(),
                Observation = before.Action.ObservationTimeout.HasValue ? ExternalObservationState.Pending : ExternalObservationState.None,
                ObservationDeadline = before.Action.ObservationTimeout is { } timeout ? now + timeout : null,
                NextAttemptAt = before.Action.ObservationTimeout is { } window && window < policy.ObservationInterval
                    ? now + window : now + policy.ObservationInterval
            },
            ExternalResultKind.Rejected => after with { State = ExternalActionState.Rejected, ResultData = result.CopyData() },
            ExternalResultKind.NotAcceptedRetryable => after with
            {
                State = before.SubmissionAttempts < policy.MaximumSubmissionAttempts ? ExternalActionState.Ready : ExternalActionState.Rejected,
                NextAttemptAt = now + policy.RetryDelay,
                LastReason = before.SubmissionAttempts < policy.MaximumSubmissionAttempts ? result.Reason : "submission-attempts-exhausted"
            },
            _ => after with { State = ExternalActionState.Unknown, NextAttemptAt = now + policy.RetryDelay }
        };
    }
}
