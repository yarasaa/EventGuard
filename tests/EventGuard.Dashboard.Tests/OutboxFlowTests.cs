using System.Net;
using System.Text.Json;

namespace EventGuard.Dashboard.Tests;

public sealed class OutboxFlowTests
{
    private static Dictionary<string, string?> Config()
    {
        var config = new Dictionary<string, string?>();
        foreach (var (key, index) in new[] { ("a", 0), ("b", 1) })
        {
            var prefix = "Dashboard:OutboxScopes:" + index + ":";
            config[prefix + "Key"] = key; config[prefix + "Tenant"] = key; config[prefix + "Producer"] = "publisher";
            config[prefix + "Label"] = key == "a" ? "Document events" : "Private other producer";
            config[prefix + "Operation"] = "Publish document updates";
        }
        return config;
    }
    private static HttpClient Client(DashboardFactory app, string? permission = "test-store/a")
    {
        var client = app.Client();
        if (permission is not null) client.DefaultRequestHeaders.Add("Test-Outbox-Permission", permission);
        return client;
    }
    [Theory]
    [InlineData("/outbox")] [InlineData("/quarantine")] [InlineData("/api/outbox")] [InlineData("/api/quarantine")]
    [InlineData("/outbox/a/1a752644-297b-4f6e-8ec9-e8c7d5d413f7")]
    public async Task Consumer_permission_alone_never_grants_outbox_access(string path)
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Client(app, null);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync(path)).StatusCode);
        Assert.Equal(0, app.Store.OutboxCalls);
    }
    [Fact]
    public async Task Producer_only_user_lands_on_outbox_without_consumer_access()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = app.Client("unassigned");
        client.DefaultRequestHeaders.Add("Test-Outbox-Permission", "test-store/a");
        var response = await client.GetAsync("/");
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal("/outbox", response.Headers.Location!.ToString());
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/outbox")).StatusCode);
        Assert.Equal(0, app.Store.Calls);
    }
    [Fact]
    public async Task Anonymous_api_and_unknown_scopes_do_not_reach_the_store()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var anonymous = app.Client(null);
        foreach (var path in new[] { "/api/outbox", "/api/quarantine", "/api/outbox-scopes" })
            Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.GetAsync(path)).StatusCode);
        using var client = Client(app);
        foreach (var path in new[] { "/api/outbox?scope=b", "/api/outbox?scope=missing", "/api/outbox/b/" + MetadataStore.WorkId })
            Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync(path)).StatusCode);
        Assert.Equal(0, app.Store.OutboxCalls);
        using var other = Client(app, "test-store/b");
        Assert.Equal(HttpStatusCode.NotFound, (await other.GetAsync("/api/outbox/b/" + MetadataStore.WorkId)).StatusCode);
        Assert.Equal(new ProducerScope("b", "publisher").Id, app.Store.LastOutboxScope);
    }
    [Fact]
    public async Task Quarantine_is_a_forced_state_and_cannot_be_overridden_with_query_input()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Client(app);
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/quarantine?scope=a")).StatusCode);
        Assert.Equal(OutboxState.Quarantined, app.Store.LastOutboxQuery!.State);
        var calls = app.Store.OutboxCalls;
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/api/quarantine?scope=a&state=Published")).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/quarantine?state=Pending")).StatusCode);
        Assert.Equal(calls, app.Store.OutboxCalls);
    }
    [Fact]
    public async Task Combined_filters_reverse_cursor_and_detail_return_preserve_context()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Client(app); app.Store.NextPage = true;
        var query = "?scope=a&from=2020-01-01T00:00&until=2027-01-01T00:00&minimumAttempts=1&uncertain=yes&replayed=no&intentId=" + MetadataStore.WorkId;
        var first = JsonDocument.Parse(await client.GetStringAsync("/api/quarantine" + query));
        var next = first.RootElement.GetProperty("nextCursor").GetString()!;
        var second = JsonDocument.Parse(await client.GetStringAsync("/api/quarantine" + query + "&cursor=" + Uri.EscapeDataString(next)));
        Assert.NotNull(app.Store.LastOutboxQuery!.Before);
        Assert.True(app.Store.LastOutboxQuery.Filters.DeliveryUncertain);
        Assert.False(app.Store.LastOutboxQuery.Filters.Replayed);
        Assert.Equal(MetadataStore.WorkId, app.Store.LastOutboxQuery.Filters.IntentId);
        var back = second.RootElement.GetProperty("previousCursor").GetString()!;
        await client.GetStringAsync("/api/quarantine" + query + "&cursor=" + Uri.EscapeDataString(back));
        Assert.NotNull(app.Store.LastOutboxQuery.After);
        var saved = second.RootElement.GetProperty("currentCursor").GetString()!;
        var detail = await client.GetStringAsync("/outbox/a/" + MetadataStore.WorkId + "?returnTo=" + Uri.EscapeDataString(saved));
        Assert.Contains("href=\"/quarantine?", detail); Assert.Contains("uncertain=yes", detail); Assert.Contains(WebUtility.HtmlEncode(saved), detail);
        var calls = app.Store.OutboxCalls;
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/api/quarantine" + query.Replace("uncertain=yes", "uncertain=no") + "&cursor=" + Uri.EscapeDataString(next))).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/api/outbox" + query + "&state=Quarantined&cursor=" + Uri.EscapeDataString(next))).StatusCode);
        Assert.Equal(calls, app.Store.OutboxCalls);
    }
    [Fact]
    public async Task Cursors_cannot_cross_producer_consumer_or_be_tampered_with()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Client(app); app.Store.NextPage = true;
        var page = JsonDocument.Parse(await client.GetStringAsync("/api/outbox?scope=a"));
        var cursor = Uri.EscapeDataString(page.RootElement.GetProperty("currentCursor").GetString()!);
        using var other = Client(app, "test-store/b"); var calls = app.Store.OutboxCalls;
        foreach (var path in new[] { "/api/outbox?scope=b&cursor=" + cursor, "/outbox/b/" + MetadataStore.WorkId + "?returnTo=" + cursor })
            Assert.Equal(HttpStatusCode.BadRequest, (await other.GetAsync(path)).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/api/works?scope=a&cursor=" + cursor)).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/api/outbox?scope=a&cursor=broken" + cursor)).StatusCode);
        Assert.Equal(calls, app.Store.OutboxCalls);
    }
    [Theory]
    [InlineData("state=999")] [InlineData("intentId=invalid")] [InlineData("minimumAttempts=-1")]
    [InlineData("uncertain=maybe")] [InlineData("from=2026-02-01T00:00&until=2026-01-01T00:00")]
    public async Task Invalid_filters_are_rejected_before_database_access(string filter)
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Client(app);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/api/outbox?scope=a&" + filter)).StatusCode);
        Assert.Equal(0, app.Store.OutboxCalls);
    }
    [Fact]
    public async Task Pages_escape_metadata_show_uncertainty_and_never_expose_mutation_controls()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Client(app);
        foreach (var path in new[] { "/outbox", "/quarantine", "/outbox/a/" + MetadataStore.WorkId })
        {
            var response = await client.GetAsync(path); Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var html = await response.Content.ReadAsStringAsync();
            Assert.Contains("&lt;script&gt;binding&lt;/script&gt;", html); Assert.DoesNotContain("<script>", html);
            Assert.DoesNotContain("Private other producer", html); Assert.DoesNotContain("Submit replay", html);
            Assert.Contains("no-store", response.Headers.CacheControl!.ToString());
        }
        var detail = await client.GetStringAsync("/outbox/a/" + MetadataStore.WorkId);
        Assert.Contains("Possible earlier delivery", detail); Assert.Contains("outbox-envelope-invalid", detail);
        Assert.Contains("original snapshot stays in the database", detail);
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync("/api/outbox/a/" + MetadataStore.WorkId + "/replay", null)).StatusCode);
    }
    [Fact]
    public async Task Unavailable_reads_are_not_rendered_as_empty_success()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Client(app); app.Store.Unavailable = true;
        foreach (var path in new[] { "/outbox", "/quarantine", "/api/outbox", "/api/quarantine", "/outbox/a/" + MetadataStore.WorkId })
        {
            var response = await client.GetAsync(path); Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
            var text = await response.Content.ReadAsStringAsync(); Assert.DoesNotContain("never-display", text); Assert.DoesNotContain("No records match", text);
        }
    }
}
