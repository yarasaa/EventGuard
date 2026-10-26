using System.Net;
using EventGuard.Operations;
namespace EventGuard.Dashboard.Tests;
public sealed class WorkerHistoryFlowTests
{
    private sealed class Monitor : IWorkerMonitor, IWorkerHistoryMonitor
    {
        public int HistoryCalls; public FleetAvailability Availability = FleetAvailability.Available;
        public Task<WorkerFleetSnapshot> ReadAsync(CancellationToken cancellationToken = default) => Task.FromResult(new WorkerFleetSnapshot(Availability, DateTimeOffset.UtcNow, []));
        public Task<WorkerHistorySnapshot> ReadHistoryAsync(string group, WorkerHistoryWindow window, CancellationToken ct = default)
        {
            HistoryCalls++; var now = DateTimeOffset.UtcNow;
            return Task.FromResult(new WorkerHistorySnapshot(Availability, group, now.AddHours(-1), now, 15, Availability == FleetAvailability.Available
                ? [new("completed", [new(now.AddSeconds(-45), 2), new(now.AddSeconds(-30), null), new(now.AddSeconds(-15), 0), new(now, 1)])] : []));
        }
    }
    [Fact]
    public async Task History_and_inventory_are_authorized_escape_labels_and_keep_missing_data_visible()
    {
        var monitor = new Monitor(); await using var app = new DashboardFactory(workerMonitor: monitor, overrides: new()
        {
            ["Dashboard:MonitoringEnabled"] = "true", ["Dashboard:Workers:Endpoint"] = "https://private-metrics.test", ["Dashboard:Workers:Groups:0:Key"] = "workers",
            ["Dashboard:Workers:Groups:0:Label"] = "Workers <script>", ["Dashboard:Workers:Groups:0:Job"] = "indexer", ["Dashboard:Workers:Groups:0:ExpectedInstances"] = "2"
        }); using var client = app.Client();
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/workers/history?window=invalid")).StatusCode); Assert.Equal(0, monitor.HistoryCalls);
        client.DefaultRequestHeaders.Add("Test-Monitor-Permission", "test-store");
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/workers/history?group=unknown")).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/workers/history?window=2h")).StatusCode); Assert.Equal(0, monitor.HistoryCalls);
        var html = await client.GetStringAsync("/workers/history?group=workers&window=6h");
        Assert.Contains("<svg", html); Assert.Contains("3 / 4 samples", html); Assert.DoesNotContain("<script>", html); Assert.Contains("&lt;script&gt;", html); Assert.DoesNotContain("private-metrics", html);
        var inventory = await client.GetStringAsync("/workers"); Assert.Contains("Expected inventory", inventory); Assert.Contains("<td>2</td><td>0</td><td>0</td><td>2</td>", inventory);
        monitor.Availability = FleetAvailability.Unavailable;
        var unavailable = await client.GetAsync("/workers/history"); Assert.Equal(HttpStatusCode.ServiceUnavailable, unavailable.StatusCode); Assert.DoesNotContain("<svg", await unavailable.Content.ReadAsStringAsync());
        var missing = await (await client.GetAsync("/workers")).Content.ReadAsStringAsync(); Assert.Contains("<td>2</td><td>Unknown</td><td>Unknown</td><td>Unknown</td>", missing);
        Assert.Equal("Ready", await client.GetStringAsync("/health/ready"));
    }
}
