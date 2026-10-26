using EventGuard.Operations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
namespace EventGuard.Dashboard.Pages;
[RequestSizeLimit(16_384)]
[RequestFormLimits(ValueLengthLimit = 12_288, ValueCountLimit = 8)]
public sealed class BulkReplayModel(DashboardBulkReplays bulk) : PageModel
{
    public string Kind { get; private set; } = "";
    public string ScopeKey { get; private set; } = "";
    public string Label { get; private set; } = "";
    public BulkReplayReview? Review { get; private set; }
    public BulkReplayProgress? Progress { get; private set; }
    public Guid? LookupRequestId { get; private set; }
    public string? Error { get; private set; }
    public bool Conflict { get; private set; }
    private void Authorize(string kind, string scope) { Label = bulk.Authorize(User, kind, scope); Kind = kind; ScopeKey = scope; }
    public async Task<IActionResult> OnGetAsync(string kind, string scope, Guid? requestId)
    {
        Authorize(kind, scope);
        if (!ModelState.IsValid) return BadRequest();
        try
        {
            if (requestId is { } id)
            {
                LookupRequestId = id; Progress = await bulk.FindAsync(User, kind, scope, id, HttpContext.RequestAborted);
                if (Progress is null) Response.StatusCode = 404;
                else if (!Progress.Complete) Review = bulk.Resume(User, kind, scope, Progress);
            }
        }
        catch (OperationsUnavailableException) { Error = "Batch status is temporarily unavailable. Please retry."; Unavailable(); }
        return Page();
    }
    public async Task<IActionResult> OnPostReviewAsync(string kind, string scope, string ids, string reason, bool acknowledgePossibleDuplicate)
    {
        Authorize(kind, scope);
        if (!ModelState.IsValid) return BadRequest();
        try { Review = await bulk.ReviewAsync(User, kind, scope, ids, reason, acknowledgePossibleDuplicate, HttpContext.RequestAborted); }
        catch (InvalidReplayException error) { Error = error.Message; Response.StatusCode = 400; }
        catch (OperationsUnavailableException) { Error = "Preview information is temporarily unavailable. No batch was submitted."; Unavailable(); }
        return Page();
    }
    public async Task<IActionResult> OnPostSubmitAsync(string kind, string scope, string ticket)
    {
        Authorize(kind, scope);
        if (!ModelState.IsValid) return BadRequest();
        try
        {
            Review = bulk.ReadCommand(User, kind, scope, ticket);
            var result = await bulk.SubmitAsync(User, kind, scope, ticket, HttpContext.RequestAborted);
            if (result.Status == BulkReplayStatus.RequestConflict)
            { Conflict = true; Error = "This batch ID already has a different saved plan. Check its original status."; Response.StatusCode = 409; return Page(); }
            return RedirectToPage("/BulkReplay", new { kind, scope, requestId = Review.Request.RequestId });
        }
        catch (InvalidReplayException error) { Error = error.Message; Response.StatusCode = 400; }
        catch (ReplayUnavailableException error)
        {
            Error = error.OutcomeUnconfirmed ? "The batch may have been partially applied. Check saved progress or resume this exact plan. Completed members will not be reapplied."
                : "Command capacity is busy. This attempt was not started. Retry this same plan.";
            Unavailable();
        }
        return Page();
    }
    private void Unavailable() { Response.StatusCode = 503; Response.Headers.RetryAfter = "5"; }
}
