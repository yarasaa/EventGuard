namespace EventGuard.Dashboard;

public static class MonitoringEndpoints
{
    public static bool CanMonitor(DashboardOptions options, System.Security.Claims.ClaimsPrincipal user) => options.MonitoringEnabled
        && user.Identity?.IsAuthenticated == true && user.HasClaim("eventguard_monitor", options.StoreId);

    public static void MapMonitoring(this WebApplication app, DashboardOptions settings)
    {
        if (!settings.MonitoringEnabled) return;
        app.MapGet("/health/live", () => Results.Text("Live")).AllowAnonymous();
        app.MapGet("/health/ready", async (HttpContext context, DashboardHealth health) =>
        {
            var ready = health.IsReady(await health.ReadAsync(context.RequestAborted));
            return Results.Text(ready ? "Ready" : "Not ready", statusCode: ready ? 200 : 503);
        }).AllowAnonymous();
        app.MapGet("/api/system", async (HttpContext context, DashboardHealth health, DashboardMetrics metrics, IHostApplicationLifetime lifetime) =>
        {
            if (!CanMonitor(settings, context.User)) return Results.NotFound();
            var result = await health.ReadAsync(context.RequestAborted);
            return Results.Ok(new { Health = result, Stopping = lifetime.ApplicationStopping.IsCancellationRequested, metrics.StartedAt, metrics.Requests,
                WorkerMonitoringConfigured = settings.Workers.Endpoint.Length > 0, WorkerStatusUrl = "/api/workers" });
        });
        app.MapGet("/api/workers", async (HttpContext context, DashboardWorkers workers, string? group, string? state, int? page) =>
        {
            var view = await workers.ReadAsync(context.User, group, state, page ?? 1, context.RequestAborted);
            return Results.Json(new { Availability = view.Snapshot.Availability.ToString(), view.Snapshot.CheckedAt, view.Total, view.Page,
                Workers = view.Rows.Select(r => new { r.Group, r.Instance, State = r.Condition.ToString(), r.SampledAt, r.RunningLoops, r.ActiveCycles, r.LastCycleAt, r.Rates }) },
                statusCode: view.Snapshot.Availability == EventGuard.Operations.FleetAvailability.Unavailable ? 503 : 200);
        });
        if (settings.MetricsToken.Length == 0) return;
        app.MapGet("/metrics", async (HttpContext context, DashboardMetrics metrics, DashboardHealth health) =>
        {
            if (context.Request.Headers.Authorization.Count != 1 || !metrics.Authorized(context.Request.Headers.Authorization[0]))
            { context.Response.StatusCode = 401; return; }
            metrics.Update(await health.ReadAsync(context.RequestAborted));
            context.Response.ContentType = "text/plain; version=0.0.4; charset=utf-8";
            await metrics.Registry.CollectAndExportAsTextAsync(context.Response.Body, context.RequestAborted);
        }).AllowAnonymous();
    }
}
