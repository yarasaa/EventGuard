using EventGuard.Operations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace EventGuard.Dashboard.Pages;

public sealed class OutboxDetailModel(DashboardOutboxQueries queries, DashboardOutboxReplays replays) : PageModel
{
    public ReadSnapshot<OutboxDetail?>? Data { get; private set; }
    public OutboxScopeRegistration Scope { get; private set; } = null!;
    public bool CanReplay => replays.CanReplay(User, Scope);
    public bool Unavailable { get; private set; }
    public string ReturnUrl { get; private set; } = "/outbox";
    public async Task<IActionResult> OnGetAsync(string scope, Guid id, string? returnTo)
    {
        Scope = queries.Resolve(User, scope);
        ReturnUrl = queries.ListLocation(User, scope, returnTo);
        if (!ModelState.IsValid) return BadRequest();
        try
        {
            Data = await queries.FindAsync(User, scope, id, HttpContext.RequestAborted);
            if (Data.Value is null) return NotFound();
        }
        catch (OperationsUnavailableException) { Unavailable = true; Response.StatusCode = 503; Response.Headers.RetryAfter = "5"; }
        return Page();
    }
}
