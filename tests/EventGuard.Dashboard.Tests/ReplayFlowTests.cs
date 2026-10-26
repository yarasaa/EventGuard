using System.Net;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.DataProtection;

namespace EventGuard.Dashboard.Tests;

public sealed class ReplayFlowTests
{
    [Fact]
    public async Task Valid_maximum_reason_survives_ticket_encoding_and_form_values_cannot_replace_the_signed_actor_or_reason()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.State = WorkState.DeadLetter;
        using var client = Operator(app);
        var reason = new string('<', 1024);
        var review = await Review(client, reason);
        var response = await client.PostAsync(Api, new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["ticket"] = Field(review, "ticket"), ["__RequestVerificationToken"] = Field(review, "__RequestVerificationToken"),
            ["actor"] = "victim", ["reason"] = "forged"
        }));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(reason, app.ReplayStore.LastRequest!.Reason);
        Assert.NotEqual("victim", app.ReplayStore.LastRequest.Actor);
    }

    [Fact]
    public async Task A_changed_reason_with_the_same_draft_identity_reports_conflict_instead_of_success()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.State = WorkState.DeadLetter;
        using var client = Operator(app);
        var preview = await client.GetStringAsync(Path);
        async Task<string> WithReason(string reason)
        {
            var response = await client.PostAsync(Path + "?handler=Review", new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["draft"] = Field(preview, "draft"), ["reason"] = reason,
                ["__RequestVerificationToken"] = Field(preview, "__RequestVerificationToken")
            }));
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            return await response.Content.ReadAsStringAsync();
        }
        var first = await WithReason("first reason");
        var second = await WithReason("different reason");
        Assert.Equal(HttpStatusCode.Redirect, (await client.PostAsync(Path + "?handler=Submit", Submit(first))).StatusCode);
        var conflict = await client.PostAsync(Path + "?handler=Submit", Submit(second));
        Assert.Equal(HttpStatusCode.Conflict, conflict.StatusCode);
        Assert.Contains("already used with different content", await conflict.Content.ReadAsStringAsync());
        Assert.Equal(1, app.ReplayStore.Generations);
    }

    [Fact]
    public async Task Tampering_and_removed_permission_block_an_already_reviewed_request()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.State = WorkState.DeadLetter;
        using var client = Operator(app);
        var review = await Review(client);
        var corrupt = review.Replace(Field(review, "ticket"), Field(review, "ticket") + "corrupt", StringComparison.Ordinal);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.PostAsync(Api, Submit(corrupt))).StatusCode);
        client.DefaultRequestHeaders.Remove("Test-Replay-Permission");
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync(Api, Submit(review))).StatusCode);
        Assert.Equal(0, app.ReplayStore.Calls);
    }
    private static readonly string Path = "/works/a/" + MetadataStore.WorkId + "/replay";
    private static readonly string Api = "/api/works/a/" + MetadataStore.WorkId + "/replay";
    private static Dictionary<string, string?> Config() => new()
    {
        ["Dashboard:ReplayEnabled"] = "true", ["Dashboard:ReplayConnectionString"] = "test-double",
        ["Dashboard:Scopes:0:AllowReplay"] = "true", ["Dashboard:Scopes:1:AllowReplay"] = "true"
    };
    private static HttpClient Operator(DashboardFactory app, string subject = "operator-a")
    {
        var client = app.Client();
        client.DefaultRequestHeaders.Add("Test-Replay-Permission", "test-store/a");
        client.DefaultRequestHeaders.Add("Test-Subject", subject);
        return client;
    }
    private static string Field(string html, string name)
    {
        var match = Regex.Match(html, "<input[^>]*name=\"" + name + "\"[^>]*value=\"([^\"]*)\"");
        Assert.True(match.Success, "Missing form field: " + name);
        return WebUtility.HtmlDecode(match.Groups[1].Value);
    }
    private static async Task<string> Review(HttpClient client, string reason = "Dependency repaired <script>alert(1)</script>")
    {
        var preview = await client.GetStringAsync(Path);
        var response = await client.PostAsync(Path + "?handler=Review", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["draft"] = Field(preview, "draft"), ["reason"] = reason,
            ["__RequestVerificationToken"] = Field(preview, "__RequestVerificationToken")
        }));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return await response.Content.ReadAsStringAsync();
    }
    private static FormUrlEncodedContent Submit(string review, string? csrf = null) => new(new Dictionary<string, string>
    {
        ["ticket"] = Field(review, "ticket"), ["__RequestVerificationToken"] = csrf ?? Field(review, "__RequestVerificationToken")
    });

    [Fact]
    public async Task Review_is_read_only_submit_records_identity_reason_and_repeat_reuses_the_receipt()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.State = WorkState.DeadLetter;
        using var client = Operator(app);
        var review = await Review(client);
        Assert.Equal(0, app.ReplayStore.Calls);
        Assert.Contains("&lt;script&gt;", review);
        Assert.DoesNotContain("<script>", review);
        var submitted = await client.PostAsync(Path + "?handler=Submit", Submit(review));
        Assert.Equal(HttpStatusCode.Redirect, submitted.StatusCode);
        var expectedActor = StableHash.Of("eventguard-operator/v1", "https://identity.test", "operator-a");
        Assert.Equal(expectedActor, app.ReplayStore.LastRequest!.Actor);
        var repeated = await client.PostAsync(Path + "?handler=Submit", Submit(review));
        Assert.Equal(submitted.Headers.Location, repeated.Headers.Location);
        Assert.Equal(1, app.ReplayStore.Generations);
        var result = await client.GetStringAsync(submitted.Headers.Location);
        Assert.Contains("Replay accepted", result);
        Assert.Contains("not confirmation that processing has completed", result);
        Assert.Contains("Dependency repaired &lt;script&gt;", result);
        Assert.Contains("no-store", (await client.GetAsync(submitted.Headers.Location)).Headers.CacheControl!.ToString());
    }

    [Theory]
    [InlineData(false, "test-store/a", "operator-a")]
    [InlineData(true, "test-store/b", "operator-a")]
    [InlineData(true, "test-store/a", "none")]
    public async Task Viewer_wrong_scope_and_missing_subject_cannot_read_preview_or_send_command(bool replayClaim, string scopeClaim, string subject)
    {
        await using var app = new DashboardFactory(overrides: Config());
        using var client = app.Client(scopeClaim);
        if (replayClaim) client.DefaultRequestHeaders.Add("Test-Replay-Permission", "test-store/a");
        client.DefaultRequestHeaders.Add("Test-Subject", subject);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync(Path)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync(Api, new FormUrlEncodedContent(new Dictionary<string, string> { ["ticket"] = "bogus" }))).StatusCode);
        Assert.Equal(0, app.Store.Calls);
        Assert.Equal(0, app.ReplayStore.Calls);
    }

    [Fact]
    public async Task Api_requires_authentication_antiforgery_and_an_actor_bound_review_ticket()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.State = WorkState.DeadLetter;
        using var anonymous = app.Client(null);
        Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.PostAsync(Api, null)).StatusCode);
        using var client = Operator(app);
        var review = await Review(client);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.PostAsync(Api, Submit(review, "invalid"))).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.PostAsync(Path + "?handler=Submit", Submit(review, "invalid"))).StatusCode);
        using var other = Operator(app, "operator-b");
        var otherReview = await Review(other);
        Assert.Equal(HttpStatusCode.BadRequest, (await other.PostAsync(Api, Submit(review, Field(otherReview, "__RequestVerificationToken")))).StatusCode);
        Assert.Equal(0, app.ReplayStore.Calls);
        Assert.Equal(HttpStatusCode.OK, (await client.PostAsync(Api, Submit(review))).StatusCode);
        var id = app.ReplayStore.LastRequest!.RequestId;
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync(Api + "/" + id)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await other.GetAsync(Api + "/" + id)).StatusCode);
    }

    [Theory]
    [InlineData(WorkState.Completed)] [InlineData(WorkState.Processing)] [InlineData(WorkState.Pending)] [InlineData(WorkState.Expired)]
    public async Task Ineligible_work_has_no_command_form(WorkState state)
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.State = state;
        using var client = Operator(app);
        var html = await client.GetStringAsync(Path);
        Assert.DoesNotContain("name=\"draft\"", html);
        Assert.Equal(0, app.ReplayStore.Calls);
    }

    [Fact]
    public async Task Expired_dead_letter_has_no_command_form()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.State = WorkState.DeadLetter; app.Store.ExpiresAt = DateTimeOffset.UtcNow.AddDays(-1);
        using var client = Operator(app);
        Assert.DoesNotContain("name=\"draft\"", await client.GetStringAsync(Path));
    }

    [Fact]
    public async Task Lost_response_keeps_the_exact_ticket_and_checking_status_does_not_issue_a_command()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.State = WorkState.DeadLetter; app.ReplayStore.LoseNextReply = true;
        using var client = Operator(app);
        var review = await Review(client);
        var response = await client.PostAsync(Path + "?handler=Submit", Submit(review));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        var uncertain = await response.Content.ReadAsStringAsync();
        Assert.Contains("Outcome not confirmed", uncertain);
        Assert.DoesNotContain("never-render", uncertain);
        Assert.Equal(Field(review, "ticket"), Field(uncertain, "ticket"));
        var receiptUrl = Api + "/" + app.ReplayStore.LastRequest!.RequestId;
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync(receiptUrl)).StatusCode);
        Assert.Equal(1, app.ReplayStore.Calls);
        Assert.Equal(HttpStatusCode.Redirect, (await client.PostAsync(Path + "?handler=Submit", Submit(uncertain))).StatusCode);
        Assert.Equal(1, app.ReplayStore.Generations);
    }

    [Fact]
    public async Task A_review_ticket_can_move_between_hosts_with_shared_protection_and_one_durable_request_identity()
    {
        var store = new ReplayTestStore(); var protection = new EphemeralDataProtectionProvider();
        await using var a = new DashboardFactory(overrides: Config(), replayStore: store, sharedProtection: protection);
        await using var b = new DashboardFactory(overrides: Config(), replayStore: store, sharedProtection: protection);
        a.Store.State = b.Store.State = WorkState.DeadLetter;
        using var first = Operator(a); using var second = Operator(b);
        var review = await Review(first);
        var secondReview = await Review(second);
        var replies = await Task.WhenAll(first.PostAsync(Api, Submit(review)),
            second.PostAsync(Api, Submit(review, Field(secondReview, "__RequestVerificationToken"))));
        Assert.All(replies, r => Assert.Equal(HttpStatusCode.OK, r.StatusCode));
        Assert.Equal(1, store.Generations);
    }

    [Fact]
    public async Task A_stale_review_does_not_claim_success_when_store_rejects_expected_version()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.State = WorkState.DeadLetter; app.ReplayStore.NextStatus = ReplayStatus.VersionConflict;
        using var client = Operator(app);
        var review = await Review(client);
        var result = await client.PostAsync(Path + "?handler=Submit", Submit(review));
        Assert.Equal(HttpStatusCode.Redirect, result.StatusCode);
        Assert.Contains("Work changed", await client.GetStringAsync(result.Headers.Location));
        Assert.Equal(0, app.ReplayStore.Generations);
    }
}
