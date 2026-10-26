namespace EventGuard.Dashboard;

public static class ReplayDisplay
{
    public static string Title(ReplayStatus status) => status switch
    {
        ReplayStatus.Accepted => "Replay accepted",
        ReplayStatus.NotFound => "Work not found",
        ReplayStatus.NotFailed => "Work is not eligible",
        ReplayStatus.VersionConflict => "Work changed",
        ReplayStatus.PayloadExpired => "Processing window expired",
        ReplayStatus.RequestConflict => "Request identity conflict",
        ReplayStatus.RateLimited => "Tenant recovery limit reached",
        _ => "Unknown request status"
    };

    public static string Explanation(ReplayStatus status) => status switch
    {
        ReplayStatus.Accepted => "The request was saved and the work was returned to the queue. This is not confirmation that processing has completed.",
        ReplayStatus.NotFound => "The work was not found within this scope when the request was evaluated.",
        ReplayStatus.NotFailed => "The work was no longer in dead-letter state. This request did not reopen it.",
        ReplayStatus.VersionConflict => "The work version changed after the preview. This request did not reopen it. Inspect its current state before preparing a new request.",
        ReplayStatus.PayloadExpired => "The event expired before the request was evaluated. Its processing window was not extended.",
        ReplayStatus.RequestConflict => "This request ID was already used with different content. The original request remains authoritative.",
        ReplayStatus.RateLimited => "The shared tenant recovery budget was exhausted. This saved refusal did not reopen the record. Review eligibility and prepare a new request after capacity is available.",
        _ => "The request status is not recognized."
    };
}
