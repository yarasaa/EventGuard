using EventGuard.Operations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace EventGuard.Dashboard.Pages;

public sealed class IndexModel(DashboardQueries queries, DashboardOutboxQueries outbox) : PageModel
{
    public DashboardPage? Data { get; private set; }
    public IReadOnlyList<ScopeRegistration> Scopes { get; private set; } = [];
    public bool Unavailable { get; private set; }
    public async Task<IActionResult> OnGetAsync(string? scope, string? state, string? cursor, string? from, string? until, string? workId, string? minimumAttempts, string? replayed)
    {
        Scopes = queries.AllowedScopes(User);
        if (scope is null && Scopes.Count == 0 && outbox.AllowedScopes(User).Count > 0) return LocalRedirect("/outbox");
        try { Data = await queries.ListAsync(User, scope, state, cursor, HttpContext.RequestAborted, FilterInput.Parse(from, until, workId, minimumAttempts, replayed)); }
        catch (OperationsUnavailableException) { Unavailable = true; Response.StatusCode = 503; Response.Headers.RetryAfter = "5"; }
        return Page();
    }
}
