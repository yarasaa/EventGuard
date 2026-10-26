using System.Net;
using Microsoft.Extensions.DependencyInjection;

namespace EventGuard.Dashboard.Tests;

public sealed class MonitoringTests
{
    private const string Token = "monitoring-test-token-never-include-in-output";
    private static Dictionary<string, string?> Config() => new() { ["Dashboard:MonitoringEnabled"] = "true", ["Dashboard:MetricsToken"] = Token };
    [Fact]
    public async Task Liveness_does_not_touch_dependencies_and_readiness_recovers_after_cached_failure()
    {
        var time = new TestTime();
        await using var app = new DashboardFactory(overrides: Config(), time: time);
        app.Store.Unavailable = true;
        using var client = app.Client(null);
        Assert.Equal("Live", await client.GetStringAsync("/health/live")); Assert.Equal(0, app.Store.HealthCalls);
        var down = await client.GetAsync("/health/ready"); Assert.Equal(HttpStatusCode.ServiceUnavailable, down.StatusCode);
        Assert.Equal("Not ready", await down.Content.ReadAsStringAsync()); Assert.Equal(1, app.Store.HealthCalls);
        app.Store.Unavailable = false;
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await client.GetAsync("/health/ready")).StatusCode);
        time.Advance();
        Assert.Equal("Ready", await client.GetStringAsync("/health/ready")); Assert.Equal(2, app.Store.HealthCalls);
        var health = app.Services.GetRequiredService<DashboardHealth>();
        var cached = await health.ReadAsync();
        app.Services.GetRequiredService<Microsoft.Extensions.Hosting.IHostApplicationLifetime>().StopApplication();
        Assert.False(health.IsReady(cached));
    }
    [Fact]
    public async Task Concurrent_probes_share_one_inflight_query_and_cancelling_one_waiter_does_not_cancel_the_probe()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = app.Client();
        app.Store.HealthRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var health = app.Services.GetRequiredService<DashboardHealth>(); using var stop = new CancellationTokenSource();
        var first = health.ReadAsync(stop.Token); var second = health.ReadAsync(); stop.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        Assert.Equal(1, app.Store.HealthCalls); app.Store.HealthRelease.SetResult();
        Assert.True((await second).Ready); Assert.Equal(1, app.Store.HealthCalls);
    }
    [Fact]
    public async Task Command_connection_is_checked_only_when_replay_is_enabled()
    {
        var config = Config(); config["Dashboard:OutboxReplayEnabled"] = "true"; config["Dashboard:ReplayConnectionString"] = "test-double";
        await using var app = new DashboardFactory(overrides: config); app.ReplayStore.HealthUnavailable = true;
        using var client = app.Client(null);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await client.GetAsync("/health/ready")).StatusCode);
        Assert.Equal(1, app.ReplayStore.HealthCalls); Assert.Equal(0, app.ReplayStore.OutboxCalls); Assert.Equal(0, app.ReplayStore.Calls);
    }
    [Fact]
    public async Task Monitoring_claim_is_separate_and_denial_does_not_query_the_database()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = app.Client();
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/api/system")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/system")).StatusCode); Assert.Equal(0, app.Store.HealthCalls);
        client.DefaultRequestHeaders.Add("Test-Monitor-Permission", "test-store");
        var html = await client.GetStringAsync("/system");
        Assert.Contains("Worker monitoring is not connected", html); Assert.Contains("Metadata access", html); Assert.DoesNotContain(Token, html);
    }
    [Fact]
    public async Task Scrapes_require_the_exact_secret_and_export_bounded_labels_without_sensitive_data()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = app.Client(null);
        Assert.Equal(HttpStatusCode.Unauthorized, (await client.GetAsync("/metrics")).StatusCode); Assert.Equal(0, app.Store.HealthCalls);
        client.DefaultRequestHeaders.Add("Authorization", "Bearer " + Token + "wrong");
        Assert.Equal(HttpStatusCode.Unauthorized, (await client.GetAsync("/metrics")).StatusCode);
        client.DefaultRequestHeaders.Remove("Authorization"); client.DefaultRequestHeaders.Add("Authorization", "Bearer " + Token);
        await client.GetAsync("/private-business-id-123?payload=sensitive");
        var text = await client.GetStringAsync("/metrics");
        Assert.Contains("eventguard_dashboard_metadata_ready 1", text);
        Assert.Contains("eventguard_dashboard_replay_ready -1", text);
        Assert.Contains("status_class=\"4xx\"", text);
        foreach (var secret in new[] { Token, "private-business-id", "sensitive", "scope", "tenant", "trace_id", "Password" }) Assert.DoesNotContain(secret, text);
        Assert.Equal(0, app.Store.Calls); Assert.Equal(0, app.ReplayStore.HealthCalls);
        Assert.Equal("no-store", (await client.GetAsync("/metrics")).Headers.CacheControl!.ToString());
    }
    [Fact]
    public async Task Registries_are_per_host_and_monitoring_is_disabled_by_default()
    {
        await using var a = new DashboardFactory(overrides: Config()); await using var b = new DashboardFactory(overrides: Config());
        using var first = a.Client(); using var second = b.Client();
        await first.GetAsync("/missing");
        Assert.NotSame(a.Services.GetRequiredService<DashboardMetrics>().Registry, b.Services.GetRequiredService<DashboardMetrics>().Registry);
        Assert.Equal(0, b.Services.GetRequiredService<DashboardMetrics>().Requests);
        await using var disabled = new DashboardFactory(); using var client = disabled.Client();
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/health/live")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/metrics")).StatusCode);
    }
    [Fact]
    public async Task Pod_address_scrapes_require_valid_token_and_do_not_bypass_host_or_https_rules_for_pages()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = app.Client(null);
        client.DefaultRequestHeaders.Host = "10.0.0.42";
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/metrics")).StatusCode);
        client.DefaultRequestHeaders.Add("Authorization", "Bearer " + Token);
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/metrics")).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/")).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("http://dashboard.test/metrics")).StatusCode);
    }
    private sealed class TestTime : TimeProvider
    {
        private long _ticks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => _ticks;
        public override DateTimeOffset GetUtcNow() => DateTimeOffset.Parse("2026-09-15T00:00:00Z").AddTicks(_ticks);
        public void Advance() => _ticks += TimeSpan.FromSeconds(6).Ticks;
    }
}
