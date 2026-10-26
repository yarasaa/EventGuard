using EventGuard.Operations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
namespace EventGuard.Dashboard.Pages;

public sealed class BacklogModel(DashboardBacklog backlog) : PageModel
{
    public BacklogView View { get; private set; } = null!;
    public IReadOnlyList<BacklogTarget> Targets => backlog.Allowed(User);
    public string Count(BacklogBucket row) => (row.IsLowerBound ? "At least " : "") + row.Count.ToString("N0", System.Globalization.CultureInfo.InvariantCulture);
    public string ListUrl(BacklogBucket row) => (View.Target.Kind == BacklogKind.Consumer ? "/" : row.State == "Quarantined" ? "/quarantine" : "/outbox")
        + QueryString.Create(new Dictionary<string,string?> { ["scope"] = View.Target.ScopeKey, ["state"] = row.State });
    public async Task<IActionResult> OnGetAsync(string? target)
    {
        View = await backlog.ReadAsync(User, target, HttpContext.RequestAborted);
        if (!View.Available) Response.StatusCode = 503;
        return Page();
    }
}
