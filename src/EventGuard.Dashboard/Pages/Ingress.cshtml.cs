using EventGuard.Operations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
namespace EventGuard.Dashboard.Pages;
public sealed class IngressModel(DashboardIngressQueries queries, DashboardQueries consumers) : PageModel
{
    public DashboardIngressPage? Data { get; private set; }
    public IReadOnlyList<ScopeRegistration> Scopes => consumers.AllowedScopes(User);
    public bool Unavailable { get; private set; }
    public async Task<IActionResult> OnGetAsync(string? scope, string? state, string? id, string? cursor)
    {
        try { Data = await queries.ListAsync(User, scope, state, id, cursor, HttpContext.RequestAborted); }
        catch (OperationsUnavailableException) { Unavailable = true; Response.StatusCode = 503; Response.Headers.RetryAfter = "5"; }
        return Page();
    }
}
