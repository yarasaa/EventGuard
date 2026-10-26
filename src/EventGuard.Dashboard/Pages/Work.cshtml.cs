using EventGuard.Operations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace EventGuard.Dashboard.Pages;

public sealed class WorkModel(DashboardQueries queries, DashboardReplays replays) : PageModel
{
    public ReadSnapshot<WorkDetail?>? Data { get; private set; }
    public ScopeRegistration Scope { get; private set; } = null!;
    public bool Unavailable { get; private set; }
    public bool DataUnavailable { get; private set; }
    public bool CanReadData { get; private set; }
    public ReadSnapshot<WorkData?>? EventData { get; private set; }
    public string ReturnUrl { get; private set; } = "/";
    public bool CanReplay { get; private set; }
    public async Task<IActionResult> OnGetAsync(string scope, Guid id, string? returnTo)
    {
        Scope = queries.Resolve(User, scope);
        CanReplay = replays.CanReplay(User, Scope);
        ReturnUrl = queries.ListLocation(User, scope, returnTo);
        if (!ModelState.IsValid) return BadRequest();
        try
        {
            Data = await queries.FindAsync(User, scope, id, HttpContext.RequestAborted);
            if (Data.Value is null) return NotFound();
            CanReadData = queries.CanReadData(User, Scope);
            if (CanReadData)
            {
                try { EventData = await queries.ReadDataAsync(User, scope, id, HttpContext.RequestAborted); }
                catch (OperationsUnavailableException) { DataUnavailable = true; }
            }
        }
        catch (OperationsUnavailableException) { Unavailable = true; Response.StatusCode = 503; Response.Headers.RetryAfter = "5"; }
        return Page();
    }
}
