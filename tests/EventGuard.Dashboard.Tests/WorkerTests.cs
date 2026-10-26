using System.Net;
using EventGuard.Operations;
namespace EventGuard.Dashboard.Tests;

public sealed class WorkerTests
{
    private static Dictionary<string,string?> Config()=>new(){["Dashboard:MonitoringEnabled"]="true",["Dashboard:Workers:Endpoint"]="https://private-metrics.test",
        ["Dashboard:Workers:BearerToken"]="private-query-token",["Dashboard:Workers:Groups:0:Key"]="indexer",["Dashboard:Workers:Groups:0:Label"]="Document workers",
        ["Dashboard:Workers:Groups:0:Job"]="indexer",["Dashboard:Workers:Groups:0:Component"]="consumer"};
    [Fact]
    public async Task Only_store_monitor_can_read_workers_and_filter_validation_precedes_source_access()
    {
        var monitor=new Monitor(); await using var app=new DashboardFactory(overrides:Config(),workerMonitor:monitor);using var client=app.Client();
        Assert.Equal(HttpStatusCode.NotFound,(await client.GetAsync("/workers")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound,(await client.GetAsync("/api/workers")).StatusCode);Assert.Equal(0,monitor.Calls);
        client.DefaultRequestHeaders.Add("Test-Monitor-Permission","another-store");
        Assert.Equal(HttpStatusCode.NotFound,(await client.GetAsync("/api/workers")).StatusCode);Assert.Equal(0,monitor.Calls);
        client.DefaultRequestHeaders.Remove("Test-Monitor-Permission"); client.DefaultRequestHeaders.Add("Test-Monitor-Permission","test-store");
        foreach(var query in new[]{"group=foreign","state=0","state=unknown","page=0","page=100"})
            Assert.Equal(HttpStatusCode.BadRequest,(await client.GetAsync("/api/workers?"+query)).StatusCode);
        Assert.Equal(0,monitor.Calls); Assert.Equal(0,app.Store.Calls);
    }
    [Fact]
    public async Task Worker_page_escapes_labels_and_paginates_both_directions_with_filters()
    {
        var monitor=new Monitor(); await using var app=new DashboardFactory(overrides:Config(),workerMonitor:monitor);using var client=app.Client();
        client.DefaultRequestHeaders.Add("Test-Monitor-Permission","test-store");
        var first=await client.GetStringAsync("/workers?group=indexer&state=Running");
        Assert.Contains("Next",first);Assert.Contains("p=2",first);Assert.DoesNotContain("<script>",first);Assert.Contains("&lt;script&gt;",first);
        var second=await client.GetStringAsync("/workers?group=indexer&state=Running&p=2");
        Assert.Contains("Previous",second);Assert.Contains("p=1",second);Assert.Contains("group=indexer",second);Assert.Contains("state=Running",second);
        Assert.DoesNotContain("private-query-token",second);Assert.DoesNotContain("private-metrics",second);
        var filtered=await client.GetStringAsync("/api/workers?state=ScrapeFailed");Assert.Contains("\"total\":0",filtered);
        Assert.Equal(0,app.Store.HealthCalls);Assert.Equal(0,app.Store.Calls);
    }
    [Fact]
    public async Task Source_outage_is_explicit_and_does_not_make_dashboard_readiness_fail()
    {
        var monitor=new Monitor {Availability=FleetAvailability.Unavailable};await using var app=new DashboardFactory(overrides:Config(),workerMonitor:monitor);using var client=app.Client();
        client.DefaultRequestHeaders.Add("Test-Monitor-Permission","test-store");
        var page=await client.GetAsync("/workers");Assert.Equal(HttpStatusCode.ServiceUnavailable,page.StatusCode);
        Assert.Contains("Monitoring source unavailable",await page.Content.ReadAsStringAsync());
        Assert.Equal(HttpStatusCode.ServiceUnavailable,(await client.GetAsync("/api/workers")).StatusCode);
        Assert.Equal("Ready",await client.GetStringAsync("/health/ready"));
    }
    [Fact]
    public async Task Unconfigured_workers_show_setup_instead_of_fake_live_data()
    {
        await using var app=new DashboardFactory(overrides:new(){["Dashboard:MonitoringEnabled"]="true"});using var client=app.Client();
        client.DefaultRequestHeaders.Add("Test-Monitor-Permission","test-store");
        Assert.Contains("Connect your monitoring source",await client.GetStringAsync("/workers"));
        Assert.Contains("Disabled",await client.GetStringAsync("/api/workers"));
    }
    private sealed class Monitor : IWorkerMonitor
    {
        public int Calls;public FleetAvailability Availability=FleetAvailability.Available;
        public Task<WorkerFleetSnapshot> ReadAsync(CancellationToken cancellationToken=default)
        {
            Calls++;
            return Task.FromResult(new WorkerFleetSnapshot(Availability,DateTimeOffset.UtcNow,Availability==FleetAvailability.Available
                ? Enumerable.Range(0,30).Select(i=>new WorkerObservation("indexer","<script>pod-"+i,WorkerCondition.Running,DateTimeOffset.UtcNow,2,1,DateTimeOffset.UtcNow)).ToArray() : []));
        }
    }
}
