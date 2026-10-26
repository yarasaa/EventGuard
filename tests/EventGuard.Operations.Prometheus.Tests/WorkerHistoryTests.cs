using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
namespace EventGuard.Operations.Prometheus.Tests;
public sealed class WorkerHistoryTests
{
    private static PrometheusWorkerOptions Options() => new() { Endpoint = "https://metrics.test/prometheus", BearerToken = "private-token",
        Groups = [new() { Key = "workers", Label = "Workers", Job = "indexer", ExpectedInstances = 2 }] };
    private sealed class Time : TimeProvider
    {
        public long Seconds; public override long TimestampFrequency => 1; public override long GetTimestamp() => Seconds;
        public override DateTimeOffset GetUtcNow() => DateTimeOffset.FromUnixTimeSeconds(1_800_000_000 + Seconds);
    }
    private sealed class Handler : HttpMessageHandler
    {
        public int Calls; public string? Fault; public TaskCompletionSource? Release; public Dictionary<string,string> Form = new();
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Calls++; Assert.Equal("https://metrics.test/prometheus/api/v1/query_range", request.RequestUri!.ToString()); Assert.Equal(HttpMethod.Post, request.Method);
            Assert.Equal("Bearer private-token", request.Headers.Authorization!.ToString());
            Form = (await request.Content!.ReadAsStringAsync(ct)).Split('&').Select(s => s.Split('=', 2)).ToDictionary(p => p[0], p => Uri.UnescapeDataString(p[1].Replace('+', ' ')));
            if (Release is not null) await Release.Task.WaitAsync(ct);
            var start = double.Parse(Form["start"], CultureInfo.InvariantCulture); var step = double.Parse(Form["step"], CultureInfo.InvariantCulture);
            object Point(double at, string value) => new object[] { at, value };
            var values = new List<object> { Point(start, "2"), Point(start + 2 * step, "NaN"), Point(start + 3 * step, "0") };
            if (Fault == "duplicate-point") values.Insert(1, Point(start, "1"));
            if (Fault == "off-grid") values[0] = Point(start + 0.5, "1");
            if (Fault == "outside") values[0] = Point(start - step, "1");
            if (Fault == "infinity") values[0] = Point(start, "+Inf");
            if (Fault == "negative") values[0] = Point(start, "-1");
            if (Fault == "too-many-points") values = Enumerable.Range(0, 302).Select(i => Point(start + i * step, "1")).ToList();
            var metric = new Dictionary<string,string> { ["outcome"] = Fault == "unknown" ? "foreign" : "completed" };
            if (Fault == "labels") metric["instance"] = "secret-pod";
            var series = new List<object> { new { metric, values } };
            if (Fault == "duplicate-series") series.Add(series[0]); if (Fault == "too-many-series") series = Enumerable.Repeat(series[0], 6).ToList();
            if (Fault == "empty") series.Clear();
            var body = JsonSerializer.Serialize(new { status = "success", warnings = Fault == "partial" ? new[] { "private backend error" } : [], data = new { resultType = "matrix", result = series } });
            return new(HttpStatusCode.OK) { Content = new StringContent(Fault == "oversize" ? new string('x', 256*1024+1) : body, Encoding.UTF8, "application/json") };
        }
    }
    [Theory] [InlineData(WorkerHistoryWindow.Hour,15,241)] [InlineData(WorkerHistoryWindow.SixHours,90,241)] [InlineData(WorkerHistoryWindow.Day,300,289)]
    public async Task History_is_fixed_query_bounded_and_distinguishes_zero_from_missing(WorkerHistoryWindow window, int step, int count)
    {
        var handler = new Handler(); using var monitor = new PrometheusWorkerMonitor(Options(), "test-store", new HttpClient(handler), new Time());
        var result = await monitor.ReadHistoryAsync("workers", window); Assert.Equal(FleetAvailability.Available, result.Availability); Assert.Equal(step, result.StepSeconds);
        var points = result.Series.Single(s => s.Outcome == "completed").Points; Assert.Equal(count, points.Count);
        Assert.Equal(2, points[0].PerSecond); Assert.Null(points[1].PerSecond); Assert.Null(points[2].PerSecond); Assert.Equal(0, points[3].PerSecond);
        Assert.All(result.Series.Where(s => s.Outcome != "completed").SelectMany(s => s.Points), p => Assert.Null(p.PerSecond));
        Assert.Equal("6", handler.Form["limit"]); Assert.Equal("2s", handler.Form["timeout"]);
        Assert.Contains("job=\"indexer\",eventguard_store=\"test-store\"", handler.Form["query"]); Assert.Contains("timestamp(up", handler.Form["query"]);
        Assert.DoesNotContain("vector(0)", handler.Form["query"]);
        Assert.DoesNotContain("private", JsonSerializer.Serialize(result));
    }
    [Theory] [InlineData("duplicate-point")] [InlineData("off-grid")] [InlineData("outside")] [InlineData("infinity")] [InlineData("negative")]
    [InlineData("too-many-points")] [InlineData("labels")] [InlineData("unknown")] [InlineData("duplicate-series")] [InlineData("too-many-series")] [InlineData("partial")] [InlineData("oversize")]
    public async Task Invalid_or_partial_history_is_unavailable(string fault)
    {
        var handler = new Handler { Fault = fault }; using var monitor = new PrometheusWorkerMonitor(Options(), "test-store", new HttpClient(handler), new Time());
        var result = await monitor.ReadHistoryAsync("workers", WorkerHistoryWindow.Hour); Assert.Equal(FleetAvailability.Unavailable, result.Availability); Assert.Empty(result.Series);
    }
    [Fact]
    public async Task Singleflight_cache_cancellation_and_admission_bound_backend_work()
    {
        var handler = new Handler { Release = new(TaskCreationOptions.RunContinuationsAsynchronously) }; var time = new Time();
        using var monitor = new PrometheusWorkerMonitor(Options(), "test-store", new HttpClient(handler), time);
        using var cancel = new CancellationTokenSource(); var first = monitor.ReadHistoryAsync("workers", WorkerHistoryWindow.Hour, cancel.Token);
        var second = monitor.ReadHistoryAsync("workers", WorkerHistoryWindow.Hour);
        Assert.Equal(FleetAvailability.Unavailable, (await monitor.ReadHistoryAsync("workers", WorkerHistoryWindow.Day)).Availability);
        cancel.Cancel(); await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        handler.Release.SetResult(); Assert.Equal(FleetAvailability.Available, (await second).Availability);
        await monitor.ReadHistoryAsync("workers", WorkerHistoryWindow.Hour); Assert.Equal(1, handler.Calls);
        time.Seconds += 16; handler.Fault = "partial"; Assert.Equal(FleetAvailability.Unavailable, (await monitor.ReadHistoryAsync("workers", WorkerHistoryWindow.Hour)).Availability);
        Assert.Equal(2, handler.Calls);
        await Assert.ThrowsAsync<ArgumentException>(() => monitor.ReadHistoryAsync("unknown", WorkerHistoryWindow.Hour));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => monitor.ReadHistoryAsync("workers", (WorkerHistoryWindow)2));
    }
    [Fact]
    public async Task Deadline_is_total_and_empty_history_is_not_zero()
    {
        var handler = new Handler { Release = new(TaskCreationOptions.RunContinuationsAsynchronously) }; var time = new Time();
        using var monitor = new PrometheusWorkerMonitor(Options(), "test-store", new HttpClient(handler), time);
        var result = await monitor.ReadHistoryAsync("workers", WorkerHistoryWindow.Hour).WaitAsync(TimeSpan.FromSeconds(6)); Assert.Equal(FleetAvailability.Unavailable, result.Availability);
        handler.Release.SetResult(); handler.Fault = "empty"; time.Seconds += 16;
        var empty = await monitor.ReadHistoryAsync("workers", WorkerHistoryWindow.Hour); Assert.Equal(FleetAvailability.Available, empty.Availability);
        Assert.All(empty.Series.SelectMany(s => s.Points), p => Assert.Null(p.PerSecond));
    }
    [Fact]
    public void Expected_inventory_survives_empty_observations_and_outage_is_unknown()
    {
        var empty = new WorkerFleetSnapshot(FleetAvailability.Available, DateTimeOffset.UtcNow, []);
        Assert.Equal(2, WorkerInventory.From(empty, "workers", 2).Missing); Assert.Equal(0, WorkerInventory.From(empty, "workers", 0).Missing);
        Assert.Null(WorkerInventory.From(empty, "workers", null).Missing);
        var outage = WorkerInventory.From(empty with { Availability = FleetAvailability.Unavailable }, "workers", 2);
        Assert.Equal(2, outage.Expected); Assert.Null(outage.Observed); Assert.Null(outage.Running); Assert.Null(outage.Missing);
        var options = Options(); options.Groups[0].ExpectedInstances = -1; Assert.Throws<ArgumentException>(() => options.Validate("test-store"));
    }
}
