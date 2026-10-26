using System.Net;
using System.Text;
using System.Text.Json;
using System.Globalization;
namespace EventGuard.Operations.Prometheus.Tests;

public sealed class WorkerMonitorTests
{
    private static PrometheusWorkerOptions Options() => new() { Endpoint = "https://metrics.test/prometheus", BearerToken = "private-test-token", ProgressAfterSeconds = 30,
        Groups = [new() { Key = "documents", Label = "Document indexer", Job = "indexer", Component = "consumer" }] };
    private static PrometheusWorkerMonitor Monitor(Handler handler, Time time, PrometheusWorkerOptions? options = null) =>
        new(options ?? Options(), "test-store", new HttpClient(handler), time);
    private static Dictionary<string, object> Point(Time time, string name, double value, string instance = "pod-a:9400", string store = "test-store") => new()
    {
        ["metric"] = new Dictionary<string,string> { ["__name__"] = name, ["job"] = "indexer", ["instance"] = instance,
            ["eventguard_store"] = store, ["component"] = "consumer" },
        ["value"] = new object[] { time.Seconds, value.ToString("R", CultureInfo.InvariantCulture) }
    };
    private static List<Dictionary<string, object>> Points(Time time, double loops = 2, double up = 1, double? scraped = null, double? observed = null, double? last = null) =>
    [Point(time,"up",up), Point(time,"eventguard_scrape_timestamp_seconds",scraped ?? time.Seconds),
     Point(time,"eventguard_worker_running_loops",loops), Point(time,"eventguard_worker_active_cycles",1),
     Point(time,"eventguard_worker_observed_timestamp_seconds",observed ?? time.Seconds),
     Point(time,"eventguard_worker_last_cycle_timestamp_seconds",last ?? time.Seconds),
     Point(time,"eventguard_worker_started_timestamp_seconds",time.Seconds-100)];
    private static string Payload(IEnumerable<Dictionary<string, object>> points, string[]? warnings = null) =>
        JsonSerializer.Serialize(new { status = "success", warnings = warnings ?? [], data = new { resultType = "vector", result = points } });

    [Theory]
    [InlineData(2,1,0,0,0,WorkerCondition.Running)]
    [InlineData(0,1,0,0,0,WorkerCondition.NoRunningLoops)]
    [InlineData(2,0,0,0,0,WorkerCondition.ScrapeFailed)]
    [InlineData(2,1,61,0,0,WorkerCondition.Stale)]
    [InlineData(2,1,0,61,0,WorkerCondition.Stale)]
    [InlineData(2,1,-6,0,0,WorkerCondition.Stale)]
    [InlineData(2,1,0,0,40,WorkerCondition.ProgressOverdue)]
    public async Task Observations_distinguish_source_freshness_and_runtime_progress(double loops, double up, double scrapeAge, double observationAge, double cycleAge, WorkerCondition expected)
    {
        var time = new Time(); var handler = new Handler { Body = () => Payload(Points(time,loops,up,time.Seconds-scrapeAge,time.Seconds-observationAge,time.Seconds-cycleAge)) };
        using var monitor = Monitor(handler,time);
        var snapshot = await monitor.ReadAsync(); Assert.Equal(FleetAvailability.Available,snapshot.Availability);
        Assert.Equal(expected,Assert.Single(snapshot.Workers).Condition);
        Assert.Contains("timestamp(up",handler.Query); Assert.Contains("or on(__name__, job, instance, component)",handler.Query); Assert.Contains("eventguard_store=\"test-store\"",handler.Query);
        Assert.Equal("https://metrics.test/prometheus/api/v1/query",handler.Url); Assert.Equal("Bearer private-test-token",handler.Authorization);
    }
    [Fact]
    public async Task Missing_instrumentation_is_not_reported_as_zero_or_stopped()
    {
        var time=new Time(); var handler=new Handler { Body=()=>Payload(Points(time).Take(2)) }; using var monitor=Monitor(handler,time);
        var row=Assert.Single((await monitor.ReadAsync()).Workers); Assert.Equal(WorkerCondition.NotInstrumented,row.Condition); Assert.Null(row.RunningLoops);
    }
    [Theory]
    [InlineData("duplicate")][InlineData("foreign-store")][InlineData("partial")][InlineData("nan")][InlineData("too-many")][InlineData("oversize")][InlineData("negative")]
    public async Task Ambiguous_partial_or_invalid_source_data_is_unavailable(string fault)
    {
        var time=new Time(); var points=Points(time);
        if(fault=="duplicate") points.Add(points[0]);
        if(fault=="foreign-store") points[0]=Point(time,"up",1,store:"other-store");
        if(fault=="nan") points[0]["value"]=new object[]{time.Seconds,"NaN"};
        if(fault=="negative") points[2]=Point(time,"eventguard_worker_running_loops",-1);
        if(fault=="too-many") points=Enumerable.Range(0,4097).Select(i=>Point(time,"up",1,"pod-"+i)).ToList();
        var handler=new Handler { Body=()=>fault=="oversize" ? new string('x',2*1024*1024+1) : Payload(points,fault=="partial" ? ["secret backend detail"] : null) };
        using var monitor=Monitor(handler,time); var result=await monitor.ReadAsync();
        Assert.Equal(FleetAvailability.Unavailable,result.Availability); Assert.Empty(result.Workers); Assert.DoesNotContain("private-test-token",JsonSerializer.Serialize(result));
    }
    [Fact]
    public async Task Missing_instances_are_retained_temporarily_then_expire_and_source_outage_never_reuses_healthy_rows()
    {
        var time=new Time(); var points=Points(time); var handler=new Handler { Body=()=>Payload(points) }; using var monitor=Monitor(handler,time);
        Assert.Equal(WorkerCondition.Running,Assert.Single((await monitor.ReadAsync()).Workers).Condition);
        time.Advance(6); points=[];
        var missing=Assert.Single((await monitor.ReadAsync()).Workers); Assert.Equal("pod-a:9400",missing.Instance); Assert.Equal(WorkerCondition.Missing,missing.Condition);
        handler.Status=HttpStatusCode.ServiceUnavailable; time.Advance(6);
        var unavailable=await monitor.ReadAsync(); Assert.Equal(FleetAvailability.Unavailable,unavailable.Availability); Assert.Empty(unavailable.Workers);
        time.Advance(601); handler.Status=HttpStatusCode.OK;
        var expired=Assert.Single((await monitor.ReadAsync()).Workers); Assert.Equal("",expired.Instance); Assert.Equal(WorkerCondition.Missing,expired.Condition);
    }
    [Fact]
    public async Task Cached_singleflight_keeps_running_after_one_waiter_cancels()
    {
        var time=new Time(); var handler=new Handler { Body=()=>Payload(Points(time)), Release=new(TaskCreationOptions.RunContinuationsAsynchronously) }; using var monitor=Monitor(handler,time);
        using var cancelled=new CancellationTokenSource(); var first=monitor.ReadAsync(cancelled.Token); var second=monitor.ReadAsync();
        cancelled.Cancel(); await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>first); Assert.Equal(1,handler.Calls);
        handler.Release.SetResult(); Assert.Equal(FleetAvailability.Available,(await second).Availability);
        await monitor.ReadAsync(); Assert.Equal(1,handler.Calls); time.Advance(6); await monitor.ReadAsync(); Assert.Equal(2,handler.Calls);
    }
    [Fact]
    public async Task Unavailable_probe_has_a_deadline_and_does_not_spawn_extra_requests()
    {
        var time=new Time(); var handler=new Handler { Release=new(TaskCreationOptions.RunContinuationsAsynchronously) }; using var monitor=Monitor(handler,time);
        var reads=Enumerable.Range(0,8).Select(_=>monitor.ReadAsync()).ToArray();
        await Task.WhenAll(reads).WaitAsync(TimeSpan.FromSeconds(6)); Assert.Equal(1,handler.Calls);
        Assert.All(reads,r=>Assert.Equal(FleetAvailability.Unavailable,r.Result.Availability));
    }
    [Fact]
    public void Configuration_rejects_untrusted_query_fragments_and_insecure_remote_endpoints()
    {
        foreach(var endpoint in new[]{"http://remote.test", "https://user:password@metrics.test", "https://metrics.test?token=x", "https://metrics.test/#x"})
        { var options=Options();options.Endpoint=endpoint;Assert.Throws<ArgumentException>(()=>options.Validate("test-store",true)); }
        var injection=Options(); injection.Groups[0].Job="indexer\"} or up";Assert.Throws<ArgumentException>(()=>injection.Validate("test-store"));
        var local=Options();local.Endpoint="http://127.0.0.1:9090";Assert.Throws<ArgumentException>(()=>local.Validate("test-store"));local.Validate("test-store",true);
    }
    [Fact]
    public async Task More_than_two_hundred_worker_rows_is_an_error_not_silent_truncation()
    {
        var time=new Time();var handler=new Handler { Body=()=>Payload(Enumerable.Range(0,201).Select(i=>Point(time,"up",1,"pod-"+i))) };
        using var monitor=Monitor(handler,time);Assert.Equal(FleetAvailability.Unavailable,(await monitor.ReadAsync()).Availability);
    }
    [Theory]
    [InlineData("ready")][InlineData("zero")][InlineData("warmup")][InlineData("reset")][InlineData("missing")][InlineData("stale")][InlineData("negative")]
    public async Task Rates_need_fresh_complete_history_after_the_current_telemetry_warmup(string mode)
    {
        var time = new Time(); var points = Points(time, observed: mode == "stale" ? time.Seconds - 61 : null);
        var created = mode is "warmup" or "reset" ? time.Seconds - 20 : time.Seconds - 120;
        points.Add(Point(time, "eventguard_worker_telemetry_created_timestamp_seconds", created));
        foreach (var name in new[] { "completed", "published", "retry_scheduled", "dead_letter", "unconfirmed" })
        {
            points.Add(Point(time, "eventguard_worker_" + name + "_total", mode == "reset" ? 0 : 100));
            if (mode == "missing" && name == "unconfirmed") continue;
            points.Add(Point(time, "eventguard_worker_" + name + "_per_second", mode == "negative" ? -1 : mode == "zero" ? 0 : 2.5));
        }
        var handler = new Handler { Body = () => Payload(points) }; using var monitor = Monitor(handler, time);
        var snapshot = await monitor.ReadAsync();
        Assert.Contains("rate(eventguard_worker_completed_total{", handler.Query); Assert.Contains("[60s]", handler.Query);
        Assert.DoesNotContain("_per_second|", handler.Query);
        if (mode == "negative") { Assert.Equal(FleetAvailability.Unavailable, snapshot.Availability); return; }
        Assert.Equal(FleetAvailability.Available, snapshot.Availability);
        var row = Assert.Single(snapshot.Workers);
        if (mode is "ready" or "zero")
        {
            Assert.NotNull(row.Rates); Assert.Equal(mode == "zero" ? 0 : 2.5, row.Rates.CompletedPerSecond);
            Assert.Equal(mode == "zero" ? 0 : 2.5, row.Rates.RetryPerSecond); Assert.Equal(60, row.Rates.WindowSeconds);
        }
        else Assert.Null(row.Rates);
    }

    private sealed class Handler : HttpMessageHandler
    {
        public Func<string> Body {get;set;}=()=>"";public HttpStatusCode Status {get;set;}=HttpStatusCode.OK;
        public TaskCompletionSource? Release {get;set;} public int Calls; public string Query="",Url="",Authorization="";
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken ct)
        {
            Interlocked.Increment(ref Calls); Url=request.RequestUri!.ToString(); Authorization=request.Headers.Authorization!.ToString();
            var body=await request.Content!.ReadAsStringAsync(ct);
            Query=Uri.UnescapeDataString(body.Split('&').Single(v=>v.StartsWith("query=",StringComparison.Ordinal))[6..].Replace('+',' '));
            if(Release is not null) await Release.Task.WaitAsync(ct);
            return new(Status){Content=new StringContent(Body(),Encoding.UTF8,"application/json")};
        }
    }
    private sealed class Time : TimeProvider
    {
        private long _seconds;
        public double Seconds=>GetUtcNow().ToUnixTimeMilliseconds()/1000d;
        public override long TimestampFrequency=>1;
        public override long GetTimestamp()=>_seconds;
        public override DateTimeOffset GetUtcNow()=>DateTimeOffset.Parse("2026-09-15T00:00:00Z").AddSeconds(_seconds);
        public void Advance(int seconds)=>_seconds+=seconds;
    }
}
