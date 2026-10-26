using EventGuard.Operations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
namespace EventGuard.Dashboard.Pages;
public sealed class WorkerHistoryModel(DashboardOptions options, DashboardWorkerHistory history) : PageModel
{
    public WorkerHistoryView View { get; private set; } = null!;
    public EventGuard.Operations.Prometheus.WorkerGroup[] Groups => options.Workers.Groups;
    public async Task<IActionResult> OnGetAsync(string? group, string? window)
    {
        View = await history.ReadAsync(User, group, window, HttpContext.RequestAborted);
        if (View.Snapshot.Availability == FleetAvailability.Unavailable) Response.StatusCode = 503;
        return Page();
    }
}
