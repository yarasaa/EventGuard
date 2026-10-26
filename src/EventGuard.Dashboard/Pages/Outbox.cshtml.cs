using EventGuard.Operations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace EventGuard.Dashboard.Pages;

public sealed class OutboxModel(DashboardOutboxQueries queries) : PageModel
{
    public DashboardOutboxPage? Data { get; private set; }
    public IReadOnlyList<OutboxScopeRegistration> Scopes { get; private set; } = [];
    public bool Unavailable { get; private set; }
    public bool Quarantine { get; private set; }
    public async Task<IActionResult> OnGetAsync(string? scope, string? state, string? cursor, string? from, string? until,
        string? intentId, string? minimumAttempts, string? replayed, string? uncertain)
    {
        Quarantine = Request.Path.Equals(new PathString("/quarantine"));
        Scopes = queries.AllowedScopes(User);
        try { Data = await queries.ListAsync(User, scope, state, cursor, HttpContext.RequestAborted,
            OutboxFilterInput.Parse(from, until, intentId, minimumAttempts, replayed, uncertain), Quarantine); }
        catch (OperationsUnavailableException) { Unavailable = true; Response.StatusCode = 503; Response.Headers.RetryAfter = "5"; }
        return Page();
    }
}
