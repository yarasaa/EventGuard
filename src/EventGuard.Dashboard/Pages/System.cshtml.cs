using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
namespace EventGuard.Dashboard.Pages;

public sealed class SystemModel(DashboardOptions settings, DashboardHealth health, DashboardMetrics metrics, IHostApplicationLifetime lifetime) : PageModel
{
    public DashboardHealthSnapshot Health { get; private set; } = null!;
    public DateTimeOffset StartedAt => metrics.StartedAt;
    public long Requests => metrics.Requests;
    public bool Stopping => lifetime.ApplicationStopping.IsCancellationRequested;
    public bool MetricsEnabled => settings.MetricsToken.Length > 0;
    public bool WorkersConfigured => settings.Workers.Endpoint.Length > 0;
    public async Task<IActionResult> OnGetAsync()
    {
        if (!MonitoringEndpoints.CanMonitor(settings, User)) return NotFound();
        Health = await health.ReadAsync(HttpContext.RequestAborted);
        return Page();
    }
}
