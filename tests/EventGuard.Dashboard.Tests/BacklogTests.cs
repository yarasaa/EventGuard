using System.Net;
using EventGuard.Operations;
namespace EventGuard.Dashboard.Tests;

public sealed class BacklogTests
{
    private static Dictionary<string, string?> Config() => new()
    {
        ["Dashboard:BacklogEnabled"] = "true", ["Dashboard:OutboxScopes:0:Key"] = "a",
        ["Dashboard:OutboxScopes:0:Label"] = "Outgoing events", ["Dashboard:OutboxScopes:0:Tenant"] = "a",
        ["Dashboard:OutboxScopes:0:Producer"] = "publisher"
    };
    [Fact]
    public async Task Consumer_and_producer_grants_are_independent_and_checked_before_cache_access()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = app.Client();
        Assert.Contains("Pending", await client.GetStringAsync("/api/backlog?target=consumer:a"));
        Assert.Equal(1, app.Store.BacklogCalls);
        using var other = app.Client("test-store/b"); other.DefaultRequestHeaders.Add("Test-Monitor-Permission", "test-store");
        foreach (var target in new[] { "consumer:a", "outbox:a", "consumer:foreign", "malicious" })
            Assert.Equal(HttpStatusCode.NotFound, (await other.GetAsync("/api/backlog?target=" + target)).StatusCode);
        Assert.Equal(1, app.Store.BacklogCalls);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/backlog?target=outbox:a")).StatusCode);
        client.DefaultRequestHeaders.Add("Test-Outbox-Permission", "test-store/a");
        Assert.Contains("Pending", await client.GetStringAsync("/api/backlog?target=outbox:a"));
        Assert.Equal(BacklogKind.Outbox, app.Store.LastBacklogQuery!.Kind);
        using var anonymous = app.Client(null);
        Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.GetAsync("/api/backlog")).StatusCode);
    }
    [Fact]
    public async Task Disabled_backlog_is_not_accessible_and_does_not_query_the_store()
    {
        await using var app = new DashboardFactory(); using var client = app.Client();
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/api/backlog")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/backlog")).StatusCode);
        Assert.Equal(0, app.Store.BacklogCalls);
    }
    [Fact]
    public async Task Page_preserves_scope_links_and_explicit_lower_bound_without_exposing_unavailable_counts()
    {
        var time = new Time(); await using var app = new DashboardFactory(overrides: Config(), time: time); using var client = app.Client();
        var page = await client.GetStringAsync("/backlog?target=consumer:a");
        Assert.Contains("At least 10,000", page); Assert.Contains("scope=a&amp;state=Pending", page);
        Assert.DoesNotContain("<script>", page); Assert.Contains("&lt;script&gt;", page);
        app.Store.Unavailable = true; time.Seconds += 16;
        var unavailable = await client.GetAsync("/backlog?target=consumer:a");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, unavailable.StatusCode);
        var body = await unavailable.Content.ReadAsStringAsync();
        Assert.Contains("Backlog data unavailable", body); Assert.DoesNotContain("At least 10,000", body); Assert.DoesNotContain("Password", body);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await client.GetAsync("/api/backlog?target=consumer:a")).StatusCode);
    }
    [Fact]
    public async Task Singleflight_survives_waiter_cancellation_and_cache_expires()
    {
        var time = new Time(); await using var app = new DashboardFactory(overrides: Config(), time: time); using var client = app.Client();
        app.Store.BacklogRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        var first = client.GetAsync("/api/backlog?target=consumer:a", cancellation.Token);
        await app.Store.BacklogEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var second = client.GetAsync("/api/backlog?target=consumer:a");
        cancellation.Cancel(); await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        app.Store.BacklogRelease.SetResult(); Assert.Equal(HttpStatusCode.OK, (await second).StatusCode);
        await client.GetAsync("/api/backlog?target=consumer:a"); Assert.Equal(1, app.Store.BacklogCalls);
        time.Seconds += 16; await client.GetAsync("/api/backlog?target=consumer:a"); Assert.Equal(2, app.Store.BacklogCalls);
    }
    private sealed class Time : TimeProvider
    {
        public long Seconds; public override long TimestampFrequency => 1; public override long GetTimestamp() => Seconds;
    }
}
public sealed partial class MetadataStore : IBacklogReadStore
{
    public int BacklogCalls; public TaskCompletionSource? BacklogRelease;
    public TaskCompletionSource BacklogEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public BacklogQuery? LastBacklogQuery;
    public async Task<BacklogSummary> ReadBacklogAsync(BacklogQuery query, CancellationToken ct = default)
    {
        Interlocked.Increment(ref BacklogCalls); LastBacklogQuery = query; BacklogEntered.TrySetResult();
        if (BacklogRelease is not null) await BacklogRelease.Task.WaitAsync(ct);
        if (Unavailable) throw new InvalidOperationException("Password=never-display");
        return new(query.CountLimit, query.States.Select(s => new BacklogBucket(s.Name, s.Name == "Pending" ? 10000 : 0,
            s.Name == "Pending", s.Name == "Pending" ? DateTimeOffset.Parse("2026-01-01T00:00:00Z") : null)).ToArray());
    }
}
