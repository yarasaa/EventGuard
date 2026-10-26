namespace EventGuard.Dashboard;

public static class OutboxDisplay
{
    public static string State(OutboxState state) => state switch
    {
        OutboxState.Pending => "Pending", OutboxState.Publishing => "Publishing", OutboxState.RetryScheduled => "Retry scheduled",
        OutboxState.Published => "Published", OutboxState.DeadLetter => "Dead letter", OutboxState.Quarantined => "Quarantined", _ => "Unknown"
    };
    public static string Explanation(OutboxState state) => state switch
    {
        OutboxState.Pending => "Waiting for a publisher or its scheduled sending time.",
        OutboxState.Publishing => "A publish attempt is active. Its delivery result may not yet be recorded.",
        OutboxState.RetryScheduled => "Waiting for the next scheduled publish attempt.",
        OutboxState.Published => "The configured transport reported acceptance. This does not confirm that a consumer processed the event.",
        OutboxState.DeadLetter => "Automatic publish retries have stopped. Review the attempt history and any delivery uncertainty.",
        OutboxState.Quarantined => "The stored envelope failed validation and was isolated. Publication is blocked; the original snapshot is retained in the database.",
        _ => "The persisted status is not recognized."
    };
}
