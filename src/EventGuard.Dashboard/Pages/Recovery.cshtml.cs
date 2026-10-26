using EventGuard.Operations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
namespace EventGuard.Dashboard.Pages;

[RequestSizeLimit(16_384)]
[RequestFormLimits(ValueLengthLimit = 12_288, ValueCountLimit = 8)]
public sealed class RecoveryModel(DashboardRecovery recovery) : PageModel
{
    public string Kind { get; private set; } = "";
    public string ScopeKey { get; private set; } = "";
    public string Label { get; private set; } = "";
    public Guid Id { get; private set; }
    public RecoveryPreview? Preview { get; private set; }
    public RecoveryReview? Review { get; private set; }
    public RecoveryReceipt? Receipt { get; private set; }
    public Guid? LookupRequestId { get; private set; }
    public string? Error { get; private set; }
    public bool RequestConflict { get; private set; }
    private void Authorize(string kind, string scope, Guid id)
    { Label = recovery.Authorize(User, kind, scope); Kind = kind; ScopeKey = scope; Id = id; }
    public async Task<IActionResult> OnGetAsync(string kind, string scope, Guid id, Guid? requestId)
    {
        Authorize(kind, scope, id);
        if (!ModelState.IsValid) return BadRequest();
        try
        {
            if (requestId is { } request)
            { LookupRequestId = request; Receipt = await recovery.FindReceiptAsync(User, kind, scope, id, request, HttpContext.RequestAborted); if (Receipt is null) Response.StatusCode = 404; }
            else { Preview = await recovery.PreviewAsync(User, kind, scope, id, HttpContext.RequestAborted); if (Preview is null) return NotFound(); }
        }
        catch (OperationsUnavailableException) { Error = "Recovery information is temporarily unavailable. Please retry."; Unavailable(); }
        return Page();
    }
    public IActionResult OnPostReview(string kind, string scope, Guid id, string draft, string manifest, string reason, bool acknowledgePossibleDuplicate)
    {
        Authorize(kind, scope, id);
        if (!ModelState.IsValid) return BadRequest();
        try { Review = recovery.Review(User, kind, scope, id, draft, manifest, reason, acknowledgePossibleDuplicate); }
        catch (InvalidReplayException error) { Error = error.Message; Response.StatusCode = 400; }
        return Page();
    }
    public async Task<IActionResult> OnPostSubmitAsync(string kind, string scope, Guid id, string ticket)
    {
        Authorize(kind, scope, id);
        if (!ModelState.IsValid) return BadRequest();
        try
        {
            Review = recovery.ReadCommand(User, kind, scope, id, ticket);
            var status = await recovery.SubmitAsync(User, kind, scope, id, ticket, HttpContext.RequestAborted);
            if (status == "RequestConflict")
            { RequestConflict = true; Error = "This request ID was already used with different content. Check its saved result."; Response.StatusCode = 409; return Page(); }
            return RedirectToPage("/Recovery", new { kind, scope, id, requestId = Review.RequestId });
        }
        catch (InvalidReplayException error) { Error = error.Message; Response.StatusCode = 400; }
        catch (ReplayUnavailableException error)
        {
            Error = error.OutcomeUnconfirmed ? "The result is unconfirmed and may already have committed. Check its saved status or retry this exact request."
                : "Recovery capacity is busy. This attempt was not started. Retry this same request.";
            Unavailable();
        }
        return Page();
    }
    private void Unavailable() { Response.StatusCode = 503; Response.Headers.RetryAfter = "5"; }
}
