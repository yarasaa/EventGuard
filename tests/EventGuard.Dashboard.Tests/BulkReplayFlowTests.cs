using System.Net;
using System.Text.RegularExpressions;
namespace EventGuard.Dashboard.Tests;
public sealed class BulkReplayFlowTests
{
    private static string Path(string kind) => $"/bulk-replay/{kind}/a";
    private static Dictionary<string, string?> Config() => new()
    {
        ["Dashboard:BulkReplayEnabled"] = "true", ["Dashboard:ReplayEnabled"] = "true", ["Dashboard:OutboxReplayEnabled"] = "true",
        ["Dashboard:ReplayConnectionString"] = "test-double", ["Dashboard:Scopes:0:AllowReplay"] = "true",
        ["Dashboard:Scopes:1:AllowReplay"] = "true", ["Dashboard:OutboxScopes:0:Key"] = "a", ["Dashboard:OutboxScopes:0:Label"] = "Events",
        ["Dashboard:OutboxScopes:0:Tenant"] = "a", ["Dashboard:OutboxScopes:0:Producer"] = "publisher", ["Dashboard:OutboxScopes:0:AllowReplay"] = "true"
    };
    private static HttpClient Operator(DashboardFactory app, string subject = "operator-a")
    {
        app.Store.State = WorkState.DeadLetter; app.Store.OutboxState = OutboxState.DeadLetter;
        var client = app.Client(); client.DefaultRequestHeaders.Add("Test-Subject", subject);
        foreach (var permission in new[] { "Test-Replay-Permission", "Test-Outbox-Permission", "Test-Outbox-Replay-Permission" }) client.DefaultRequestHeaders.Add(permission, "test-store/a");
        return client;
    }
    private static string Field(string html, string name)
    {
        var match = Regex.Match(html, "<input[^>]*name=\"" + name + "\"[^>]*value=\"([^\"]*)\"");
        Assert.True(match.Success, "Missing field " + name); return WebUtility.HtmlDecode(match.Groups[1].Value);
    }
    private static async Task<HttpResponseMessage> Review(HttpClient client, string kind, string ids, bool acknowledge = true)
    {
        var form = await client.GetStringAsync(Path(kind));
        return await client.PostAsync(Path(kind) + "?handler=Review", new FormUrlEncodedContent(new Dictionary<string, string>
        { ["ids"] = ids, ["reason"] = "Fixed <script>dependency</script>", ["acknowledgePossibleDuplicate"] = acknowledge.ToString(), ["__RequestVerificationToken"] = Field(form, "__RequestVerificationToken") }));
    }
    private static FormUrlEncodedContent Submit(string html, string? ticket = null, string? csrf = null) => new(new Dictionary<string, string>
    { ["ticket"] = ticket ?? Field(html, "ticket"), ["__RequestVerificationToken"] = csrf ?? Field(html, "__RequestVerificationToken") });
    [Theory] [InlineData("consumer")] [InlineData("outbox")]
    public async Task Review_is_read_only_and_partial_submission_can_resume_from_its_saved_plan(string kind)
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Operator(app);
        var second = Guid.NewGuid(); app.Store.AdditionalWorkIds.Add(second);
        var preview = await Review(client, kind, MetadataStore.WorkId + "\n" + second); Assert.Equal(HttpStatusCode.OK, preview.StatusCode);
        var review = await preview.Content.ReadAsStringAsync(); Assert.Equal(0, app.ReplayStore.BulkPlans); Assert.DoesNotContain("<script>", review);
        if (kind == "consumer") app.ReplayStore.LoseNextReply = true; else app.ReplayStore.LoseNextOutboxReply = true;
        var partial = await client.PostAsync(Path(kind) + "?handler=Submit", Submit(review)); Assert.Equal(HttpStatusCode.ServiceUnavailable, partial.StatusCode);
        var uncertain = await partial.Content.ReadAsStringAsync(); Assert.Equal(Field(review, "ticket"), Field(uncertain, "ticket"));
        var statusUrl = WebUtility.HtmlDecode(Regex.Match(uncertain, "href=\"([^\"]+requestId=[^\"]+)\"").Groups[1].Value); Assert.NotEmpty(statusUrl);
        app.Store.Unavailable = true; // Saved progress/resume must not silently replace the plan with a fresh preview.
        var saved = await client.GetStringAsync(statusUrl); Assert.Contains("1 / 2", saved); Assert.Contains("Resume same batch", saved);
        var resumed = await client.PostAsync(Path(kind) + "?handler=Submit", Submit(saved)); Assert.Equal(HttpStatusCode.Redirect, resumed.StatusCode);
        var complete = await client.GetStringAsync(resumed.Headers.Location); Assert.Contains("All request outcomes recorded", complete);
        Assert.Equal(2, kind == "consumer" ? app.ReplayStore.Generations : app.ReplayStore.OutboxGenerations);
        Assert.DoesNotContain("Resume same batch", complete);
        using var other = Operator(app, "operator-b"); Assert.Equal(HttpStatusCode.NotFound, (await other.GetAsync(statusUrl)).StatusCode);
    }
    [Theory] [InlineData("consumer")] [InlineData("outbox")]
    public async Task Signed_plan_cannot_change_actor_scope_members_or_bypass_csrf(string kind)
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Operator(app);
        var review = await (await Review(client, kind, MetadataStore.WorkId.ToString())).Content.ReadAsStringAsync();
        Assert.Equal(HttpStatusCode.BadRequest, (await client.PostAsync(Path(kind) + "?handler=Submit", Submit(review, csrf: "bad"))).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.PostAsync(Path(kind) + "?handler=Submit", Submit(review, Field(review, "ticket") + "bad"))).StatusCode);
        using var other = Operator(app, "operator-b"); var otherForm = await other.GetStringAsync(Path(kind));
        Assert.Equal(HttpStatusCode.BadRequest, (await other.PostAsync(Path(kind) + "?handler=Submit", Submit(review, csrf: Field(otherForm, "__RequestVerificationToken")))).StatusCode);
        client.DefaultRequestHeaders.Remove(kind == "consumer" ? "Test-Replay-Permission" : "Test-Outbox-Replay-Permission");
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync(Path(kind) + "?handler=Submit", Submit(review))).StatusCode);
        Assert.Equal(0, app.ReplayStore.BulkPlans);
    }
    [Fact]
    public async Task Selection_is_bounded_and_unknown_duplicate_ineligible_or_unacknowledged_items_do_not_prepare_a_batch()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Operator(app);
        foreach (var ids in new[] { "bad", MetadataStore.WorkId + "," + MetadataStore.WorkId, Guid.NewGuid().ToString(), string.Join(',', Enumerable.Range(0, 26).Select(_ => Guid.NewGuid())) })
            Assert.Equal(HttpStatusCode.BadRequest, (await Review(client, "consumer", ids)).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await Review(client, "outbox", MetadataStore.WorkId.ToString(), false)).StatusCode);
        app.Store.State = WorkState.Completed;
        Assert.Equal(HttpStatusCode.BadRequest, (await Review(client, "consumer", MetadataStore.WorkId.ToString())).StatusCode);
        Assert.Equal(0, app.ReplayStore.BulkPlans);
    }
    [Fact]
    public async Task Global_switch_defaults_to_no_bulk_access_even_for_replay_operators()
    {
        var config = Config(); config["Dashboard:BulkReplayEnabled"] = "false";
        await using var app = new DashboardFactory(overrides: config); using var client = Operator(app);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync(Path("consumer"))).StatusCode);
        Assert.Equal(0, app.Store.Calls); Assert.Equal(0, app.ReplayStore.BulkPlans);
    }
}
