namespace EventGuard.Operations;

/// <summary>Explains persisted metadata without inferring worker health or exposing application errors.</summary>
public sealed record WorkGuidance(string Code, string Explanation, string NextStep)
{
    public static WorkGuidance Explain(WorkSummary work, DateTimeOffset observedAt)
    {
        ArgumentNullException.ThrowIfNull(work);
        return work.State switch
        {
            WorkState.Completed => new("completed", "Processing committed successfully.",
                "No consumer action is required. Check the outbox separately for external delivery."),
            WorkState.Expired => Expired(),
            WorkState.DeadLetter when work.ReasonCode == "attempt-budget-exhausted" => new("attempt-budget-exhausted",
                "Automatic processing stopped after the attempt budget was exhausted.",
                "Review attempts and worker logs, fix the cause, then review replay eligibility."),
            WorkState.DeadLetter => new("dead-letter", "Automatic processing stopped after a non-retryable failure.",
                "Check contract validation and handler logs. Correct the cause before reviewing replay eligibility."),
            WorkState.Processing => new("processing", "A worker has claimed this item; completion has not yet been recorded.",
                "Check worker health and attempt history. This view does not include the lease deadline, so it cannot determine whether the worker is stuck."),
            WorkState.Pending or WorkState.RetryScheduled when work.ExpiresAt <= observedAt => new("expiry-awaiting-sweep",
                "The execution deadline has passed, but expiry has not yet been recorded.",
                "Check that the worker and expiry sweep are running. The original deadline is not extended by replay."),
            WorkState.Pending or WorkState.RetryScheduled when work.NextAttemptAt > observedAt => new("scheduled",
                work.State == WorkState.RetryScheduled ? "A retry is scheduled for the next attempt time." : "The item is waiting for its scheduled start time.",
                "Wait until the next attempt time. If it remains waiting afterwards, check worker health and backlog."),
            WorkState.Pending or WorkState.RetryScheduled => new("ready-to-process", "The item is eligible for processing.",
                "If waiting persists, check worker health, scope configuration, database connectivity and backlog. This record alone does not identify the cause."),
            _ => new("unknown-state", "This state is not recognized by this dashboard version.",
                "Check package and schema compatibility before taking action.")
        };
    }

    public static string? AttemptExplanation(string? reasonCode) => reasonCode switch
    {
        "lease-recovered" => "The previous lease expired and another worker recovered the item. This does not prove that an external side effect was rolled back.",
        "cancelled" => "Processing was cancelled. Check shutdown or cancellation in the worker logs.",
        "transient-failure" => "Processing failed with a retryable error. Check worker logs for the application error.",
        "permanent-failure" => "Processing failed with a non-retryable error. Check the contract and handler logs.",
        "attempt-budget-exhausted" => "The configured automatic attempt budget was exhausted.",
        "expired" => "The execution deadline passed.",
        _ => null
    };

    private static WorkGuidance Expired() => new("expired", "The execution deadline passed and automatic processing stopped.",
        "Review the original deadline and business requirements. Replay cannot revive expired payloads.");
}
