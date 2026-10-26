using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace EventGuard.Dashboard.Tests;

public sealed class DashboardEnhancementTests
{
    [Fact]
    public async Task Work_details_explain_waiting_without_claiming_to_know_worker_health()
    {
        await using var app = new DashboardFactory();
        app.Store.State = WorkState.Pending;
        using var client = app.Client();
        var html = await client.GetStringAsync("/works/a/" + MetadataStore.WorkId);
        Assert.Contains("Processing guidance", html);
        Assert.Contains("ready-to-process", html);
        Assert.Contains("check worker health", html);
        Assert.Contains("does not identify the cause", html);
    }

    [Fact]
    public async Task Advanced_filters_are_parsed_bound_to_cursors_and_retained_in_return_links()
    {
        await using var app = new DashboardFactory();
        app.Store.NextPage = true;
        using var client = app.Client();
        var query = "scope=a&from=2025-01-01T00:00&until=2027-01-01T00:00&minimumAttempts=2&replayed=yes&workId=" + MetadataStore.WorkId;
        using var first = JsonDocument.Parse(await client.GetStringAsync("/api/works?" + query));
        var filter = app.Store.LastQuery!.Filters;
        Assert.Equal(2, filter.MinimumAttempts);
        Assert.True(filter.Replayed);
        Assert.Equal(MetadataStore.WorkId, filter.WorkId);
        Assert.Equal(DateTimeOffset.Parse("2025-01-01T00:00:00Z"), filter.CreatedFrom);
        var cursor = Uri.EscapeDataString(first.RootElement.GetProperty("nextCursor").GetString()!);
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/api/works?" + query + "&cursor=" + cursor)).StatusCode);
        var calls = app.Store.Calls;
        foreach (var changed in new[] { "minimumAttempts=3", "replayed=no", "from=2024-01-01T00:00", "until=2028-01-01T00:00", "workId=" + Guid.NewGuid() })
        {
            var name = changed.Split('=')[0];
            var altered = Regex.Replace(query, name + "=[^&]*", changed);
            Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/api/works?" + altered + "&cursor=" + cursor)).StatusCode);
        }
        Assert.Equal(calls, app.Store.Calls);
        var html = await client.GetStringAsync("/works/a/" + MetadataStore.WorkId + "?returnTo=" + cursor);
        var back = WebUtility.HtmlDecode(Regex.Match(html, "class=\"back\" href=\"([^\"]+)\"").Groups[1].Value);
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync(back)).StatusCode);
        Assert.Equal(filter, app.Store.LastQuery!.Filters);
        Assert.NotNull(app.Store.LastQuery.Before);
    }

    [Theory]
    [InlineData("from=2025-02-30T00:00")]
    [InlineData("from=2026-01-02T00:00&until=2026-01-01T00:00")]
    [InlineData("minimumAttempts=-1")]
    [InlineData("minimumAttempts=2147483648")]
    [InlineData("replayed=unknown")]
    [InlineData("workId=not-a-guid")]
    public async Task Invalid_advanced_filters_fail_before_querying(string query)
    {
        await using var app = new DashboardFactory();
        using var client = app.Client();
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/api/works?scope=a&" + query)).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/?scope=a&" + query)).StatusCode);
        Assert.Equal(0, app.Store.Calls);
    }
    private static Dictionary<string, string?> DataConfiguration() => new()
    {
        ["Dashboard:Scopes:0:Operation"] = "Update the search index",
        ["Dashboard:Scopes:0:Description"] = "Apply the incoming document revision.",
        ["Dashboard:Scopes:0:DataFields:0:Label"] = "Document title",
        ["Dashboard:Scopes:0:DataFields:0:Pointer"] = "/document/title",
        ["Dashboard:Scopes:0:DataFields:1:Label"] = "Revision",
        ["Dashboard:Scopes:0:DataFields:1:Pointer"] = "/document/revision"
    };

    [Fact]
    public async Task Next_previous_and_detail_return_keep_filter_and_page_context()
    {
        await using var app = new DashboardFactory();
        app.Store.NextPage = true;
        using var client = app.Client();
        using var first = JsonDocument.Parse(await client.GetStringAsync("/api/works?scope=a&state=Completed"));
        Assert.Equal(JsonValueKind.Null, first.RootElement.GetProperty("previousCursor").ValueKind);
        var next = first.RootElement.GetProperty("nextCursor").GetString()!;
        using var second = JsonDocument.Parse(await client.GetStringAsync("/api/works?scope=a&state=Completed&cursor=" + Uri.EscapeDataString(next)));
        Assert.NotNull(app.Store.LastQuery!.Before);
        Assert.Null(app.Store.LastQuery.After);
        var cutoff = app.Store.LastQuery.CreatedBefore;
        var previous = second.RootElement.GetProperty("previousCursor").GetString()!;
        var page = await client.GetStringAsync("/?scope=a&state=Completed&cursor=" + Uri.EscapeDataString(previous));
        Assert.NotNull(app.Store.LastQuery!.After);
        Assert.Null(app.Store.LastQuery.Before);
        Assert.Equal(cutoff, app.Store.LastQuery.CreatedBefore);
        Assert.Contains("lang=\"en\"", page);
        Assert.Contains("Next page", page);
        var current = second.RootElement.GetProperty("currentCursor").GetString()!;
        var detail = await client.GetStringAsync("/works/a/" + MetadataStore.WorkId + "?returnTo=" + Uri.EscapeDataString(current));
        var returnUrl = WebUtility.HtmlDecode(Regex.Match(detail, "class=\"back\" href=\"([^\"]+)\"").Groups[1].Value);
        Assert.StartsWith("/?scope=a&state=Completed&", returnUrl);
        Assert.Contains("&cursor=", returnUrl);
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync(returnUrl)).StatusCode);
        Assert.NotNull(app.Store.LastQuery!.Before);
        Assert.Equal(cutoff, app.Store.LastQuery.CreatedBefore);
        var calls = app.Store.Calls;
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/works/a/" + MetadataStore.WorkId + "?returnTo=https://attacker.test")).StatusCode);
        using var other = app.Client("test-store/b");
        Assert.Equal(HttpStatusCode.BadRequest, (await other.GetAsync("/works/b/" + MetadataStore.WorkId + "?returnTo=" + Uri.EscapeDataString(current))).StatusCode);
        Assert.Equal(calls, app.Store.Calls);
    }

    [Theory]
    [InlineData("test-store/a", null, true)]
    [InlineData("test-store/a", "test-store/b", true)]
    [InlineData("test-store/a", "test-store/a", false)]
    [InlineData("test-store/b", "test-store/a", true)]
    [InlineData(null, "test-store/a", true)]
    public async Task Data_requires_configuration_and_both_matching_permissions(string? scopePermission, string? dataPermission, bool configured)
    {
        await using var app = new DashboardFactory(overrides: configured ? DataConfiguration() : null);
        using var client = app.Client(scopePermission);
        if (dataPermission is not null) client.DefaultRequestHeaders.Add("Test-Data-Permission", dataPermission);
        var response = await client.GetAsync("/api/works/a/" + MetadataStore.WorkId + "/data");
        Assert.Equal(scopePermission is null ? HttpStatusCode.Unauthorized : HttpStatusCode.NotFound, response.StatusCode);
        Assert.Equal(0, app.Store.DataCalls);
        Assert.Equal(0, app.Store.Calls);
    }

    [Fact]
    public async Task Authorized_data_is_allowlisted_and_html_escaped_while_metadata_stays_payload_free()
    {
        await using var app = new DashboardFactory(overrides: DataConfiguration());
        using var client = app.Client();
        var withoutData = await client.GetStringAsync("/works/a/" + MetadataStore.WorkId);
        Assert.Contains("Update the search index", withoutData);
        Assert.Contains("not enabled for this scope or your account", withoutData);
        Assert.Equal(0, app.Store.DataCalls);
        client.DefaultRequestHeaders.Add("Test-Data-Permission", "test-store/a");
        var metadata = await client.GetStringAsync("/api/works/a/" + MetadataStore.WorkId);
        Assert.DoesNotContain("document", metadata);
        Assert.Equal(0, app.Store.DataCalls);
        var html = await client.GetStringAsync("/works/a/" + MetadataStore.WorkId);
        Assert.Contains("Document title", html);
        Assert.Contains("&lt;script&gt;", html);
        Assert.DoesNotContain("<script>", html);
        Assert.DoesNotContain("never-display", html);
        var data = await client.GetAsync("/api/works/a/" + MetadataStore.WorkId + "/data");
        Assert.Equal(HttpStatusCode.OK, data.StatusCode);
        Assert.Contains("no-store", data.Headers.CacheControl!.ToString());
        Assert.DoesNotContain("never-display", await data.Content.ReadAsStringAsync());
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/api/works/a/" + Guid.NewGuid() + "/data")).StatusCode);
    }

    [Fact]
    public async Task Data_read_failure_does_not_hide_metadata_or_expose_database_error()
    {
        await using var app = new DashboardFactory(overrides: DataConfiguration());
        app.Store.DataUnavailable = true;
        using var client = app.Client();
        client.DefaultRequestHeaders.Add("Test-Data-Permission", "test-store/a");
        var response = await client.GetAsync("/works/a/" + MetadataStore.WorkId);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        Assert.Contains("Event data could not be read", html);
        Assert.Contains("Attempt history", html);
        Assert.DoesNotContain("do-not-render-this", html);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await client.GetAsync("/api/works/a/" + MetadataStore.WorkId + "/data")).StatusCode);
    }
}
