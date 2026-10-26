namespace EventGuard.Dashboard;

public static class OutboxReplayDisplay
{
    public static string Title(OutboxReplayStatus status) => status switch
    {
        OutboxReplayStatus.Accepted => "Replay accepted",
        OutboxReplayStatus.NotFound => "Outgoing event not found",
        OutboxReplayStatus.NotFailed => "Publication is not eligible",
        OutboxReplayStatus.VersionConflict => "Publication changed",
        OutboxReplayStatus.DeliveryUncertain => "Duplicate risk requires acknowledgement",
        OutboxReplayStatus.Quarantined => "Publication is quarantined",
        OutboxReplayStatus.RequestConflict => "Request identity conflict",
        OutboxReplayStatus.RateLimited => "Tenant recovery limit reached",
        _ => "Unknown request status"
    };

    public static string Explanation(OutboxReplayStatus status) => status switch
    {
        OutboxReplayStatus.Accepted => "The request was saved and the outgoing event was returned to the queue. This is not confirmation that publication has completed.",
        OutboxReplayStatus.NotFound => "The outgoing event was not found within this producer scope when the request was evaluated.",
        OutboxReplayStatus.NotFailed => "The publication was no longer in dead-letter state. This request did not reopen it.",
        OutboxReplayStatus.VersionConflict => "The publication version changed after the preview. This request did not reopen it. Inspect its current state before preparing a new request.",
        OutboxReplayStatus.DeliveryUncertain => "An earlier attempt may have delivered the event. This request did not acknowledge possible duplication and did not reopen it. Review the delivery risk before preparing a new request.",
        OutboxReplayStatus.Quarantined => "The event is quarantined and was not queued for publication. Inspect its validation metadata. Replay cannot release quarantine or repair stored content.",
        OutboxReplayStatus.RequestConflict => "This request ID was already used with different content. The original request remains authoritative.",
        OutboxReplayStatus.RateLimited => "The shared tenant recovery budget was exhausted. This saved refusal did not reopen the record. Review eligibility and prepare a new request after capacity is available.",
        _ => "The request status is not recognized."
    };
}
