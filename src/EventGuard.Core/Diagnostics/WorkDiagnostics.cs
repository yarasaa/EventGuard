namespace EventGuard;

public sealed record WorkExplanation(string Code, bool ReplayEligible, DateTimeOffset? RetryAt);

public static class WorkDiagnostics
{
    public static WorkExplanation Explain(WorkItem work, DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(work);
        if (work.State == WorkState.Completed) return new("already-completed", false, null);
        if (work.State == WorkState.Expired || work.ExpiresAt <= now) return new("expired", false, null);
        return work.State switch
        {
            WorkState.Processing => new("processing-or-awaiting-recovery", false, null),
            WorkState.RetryScheduled => new("retry-scheduled", false, work.NextAttemptAt),
            WorkState.DeadLetter => new("failed-replay-eligible", true, null),
            WorkState.Pending => new(work.NextAttemptAt > now ? "scheduled" : "pending", false, work.NextAttemptAt),
            _ => throw new ArgumentOutOfRangeException(nameof(work))
        };
    }
}
