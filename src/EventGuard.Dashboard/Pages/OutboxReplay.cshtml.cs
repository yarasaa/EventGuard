using EventGuard.Operations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace EventGuard.Dashboard.Pages;

[RequestSizeLimit(16_384)]
[RequestFormLimits(ValueLengthLimit = 12_288, ValueCountLimit = 8)]
public sealed class OutboxReplayModel(DashboardOutboxReplays replays) : PageModel
{
    public OutboxScopeRegistration Scope { get; private set; } = null!;
    public Guid IntentId { get; private set; }
    public OutboxReplayPreview? Preview { get; private set; }
    public OutboxReplayReview? Review { get; private set; }
    public OutboxReplayReceipt? Receipt { get; private set; }
    public Guid? LookupRequestId { get; private set; }
    public string? Error { get; private set; }
    public bool OutcomeUnconfirmed { get; private set; }
    public bool RequestConflict { get; private set; }

    public async Task<IActionResult> OnGetAsync(string scope, Guid id, Guid? requestId)
    {
        Scope = replays.Authorize(User, scope); IntentId = id;
        if (!ModelState.IsValid) return BadRequest();
        try
        {
            if (requestId is { } request)
            {
                LookupRequestId = request;
                Receipt = await replays.FindReceiptAsync(User, scope, id, request, HttpContext.RequestAborted);
                if (Receipt is null) Response.StatusCode = 404;
            }
            else
            {
                Preview = await replays.PreviewAsync(User, scope, id, HttpContext.RequestAborted);
                if (Preview is null) return NotFound();
            }
        }
        catch (OperationsUnavailableException) { Error = "Replay information is temporarily unavailable. Please try again."; Unavailable(); }
        return Page();
    }

    public IActionResult OnPostReview(string scope, Guid id, string draft, string reason, bool acknowledgePossibleDuplicate)
    {
        Scope = replays.Authorize(User, scope); IntentId = id;
        if (!ModelState.IsValid) return BadRequest();
        try { Review = replays.Review(User, scope, id, draft, reason, acknowledgePossibleDuplicate); }
        catch (InvalidReplayException error) { Error = error.Message; Response.StatusCode = 400; }
        return Page();
    }

    public async Task<IActionResult> OnPostSubmitAsync(string scope, Guid id, string ticket)
    {
        Scope = replays.Authorize(User, scope); IntentId = id;
        if (!ModelState.IsValid) return BadRequest();
        try
        {
            Review = replays.ReadCommand(User, scope, id, ticket);
            var result = await replays.SubmitAsync(User, scope, id, ticket, HttpContext.RequestAborted);
            if (result.Status == OutboxReplayStatus.RequestConflict)
            {
                RequestConflict = true;
                Error = "This request ID was already used with different content. Check its original saved status.";
                Response.StatusCode = 409;
                return Page();
            }
            return RedirectToPage("/OutboxReplay", new { scope, id, requestId = Review.RequestId });
        }
        catch (InvalidReplayException error) { Error = error.Message; Response.StatusCode = 400; }
        catch (ReplayUnavailableException error)
        {
            OutcomeUnconfirmed = error.OutcomeUnconfirmed;
            Error = error.OutcomeUnconfirmed
                ? "The result could not be confirmed. The request may already have committed. Check its saved status or retry this exact request."
                : "Replay capacity is busy. This attempt was not started. Retry this same request.";
            Unavailable();
        }
        return Page();
    }

    private void Unavailable() { Response.StatusCode = 503; Response.Headers.RetryAfter = "5"; }
}
