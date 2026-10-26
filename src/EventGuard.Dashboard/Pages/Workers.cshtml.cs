using EventGuard.Operations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace EventGuard.Dashboard.Pages;

public sealed class WorkersModel(DashboardOptions settings, DashboardWorkers workers) : PageModel
{
    public WorkerView View { get; private set; } = null!;
    public string? Group { get; private set; }
    public string? State { get; private set; }
    public EventGuard.Operations.Prometheus.WorkerGroup[] Groups => settings.Workers.Groups;
    public string GroupLabel(string key) => Groups.FirstOrDefault(g => g.Key == key)?.Label ?? key;
    public string GroupComponent(string key) => Groups.FirstOrDefault(g => g.Key == key)?.Component ?? "";
    public int StaleSeconds => settings.Workers.StaleAfterSeconds;
    public int ProgressSeconds => settings.Workers.ProgressAfterSeconds;
    public async Task<IActionResult> OnGetAsync(string? group, string? state, int p = 1)
    {
        View = await workers.ReadAsync(User, group, state, p, HttpContext.RequestAborted);
        Group = group; State = state;
        if (View.Snapshot.Availability == FleetAvailability.Unavailable) Response.StatusCode = 503;
        return Page();
    }
}
