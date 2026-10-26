using System.Net;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.DataProtection;

namespace EventGuard.Dashboard.Tests;

public sealed class OutboxReplayFlowTests
{
    [Fact]
    public async Task Valid_maximum_reason_survives_ticket_encoding_and_form_values_cannot_replace_the_signed_actor_or_reason()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.OutboxState = OutboxState.DeadLetter;
        using var client = Operator(app);
        var reason = new string('<', 1024);
        var review = await Review(client, reason);
        var response = await client.PostAsync(Api, new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["ticket"] = Field(review, "ticket"), ["__RequestVerificationToken"] = Field(review, "__RequestVerificationToken"),
            ["actor"] = "victim", ["reason"] = "forged", ["acknowledgePossibleDuplicate"] = "false"
        }));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(app.ReplayStore.LastOutboxRequest!.AcknowledgePossibleDuplicate);
        Assert.Equal(reason, app.ReplayStore.LastOutboxRequest.Reason);
        Assert.NotEqual("victim", app.ReplayStore.LastOutboxRequest.Actor);
    }

    [Fact]
    public async Task A_changed_reason_with_the_same_draft_identity_reports_conflict_instead_of_success()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.OutboxState = OutboxState.DeadLetter;
        using var client = Operator(app);
        var preview = await client.GetStringAsync(Path);
        async Task<string> WithReason(string reason)
        {
            var response = await client.PostAsync(Path + "?handler=Review", new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["draft"] = Field(preview, "draft"), ["reason"] = reason, ["acknowledgePossibleDuplicate"] = "true",
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
        Assert.Equal(1, app.ReplayStore.OutboxGenerations);
    }

    [Fact]
    public async Task Tampering_and_removed_permission_block_an_already_reviewed_request()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.OutboxState = OutboxState.DeadLetter;
        using var client = Operator(app);
        var review = await Review(client);
        var corrupt = review.Replace(Field(review, "ticket"), Field(review, "ticket") + "corrupt", StringComparison.Ordinal);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.PostAsync(Api, Submit(corrupt))).StatusCode);
        client.DefaultRequestHeaders.Remove("Test-Outbox-Replay-Permission");
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync(Api, Submit(review))).StatusCode);
        Assert.Equal(0, app.ReplayStore.OutboxCalls);
    }
    private static readonly string Path = "/outbox/a/" + MetadataStore.WorkId + "/replay";
    private static readonly string Api = "/api/outbox/a/" + MetadataStore.WorkId + "/replay";
    private static Dictionary<string, string?> Config() => new()
    {
        ["Dashboard:OutboxReplayEnabled"] = "true", ["Dashboard:ReplayConnectionString"] = "test-double",
        ["Dashboard:OutboxScopes:0:Key"] = "a", ["Dashboard:OutboxScopes:0:Label"] = "Outgoing documents",
        ["Dashboard:OutboxScopes:0:Tenant"] = "a", ["Dashboard:OutboxScopes:0:Producer"] = "publisher",
        ["Dashboard:OutboxScopes:0:AllowReplay"] = "true",
        ["Dashboard:OutboxScopes:1:Key"] = "b", ["Dashboard:OutboxScopes:1:Label"] = "Other producer",
        ["Dashboard:OutboxScopes:1:Tenant"] = "b", ["Dashboard:OutboxScopes:1:Producer"] = "publisher",
        ["Dashboard:OutboxScopes:1:AllowReplay"] = "true"
    };
    private static HttpClient Operator(DashboardFactory app, string subject = "operator-a")
    {
        var client = app.Client("unassigned"); // Producer-only operators do not need consumer permissions.
        client.DefaultRequestHeaders.Add("Test-Outbox-Permission", "test-store/a");
        client.DefaultRequestHeaders.Add("Test-Outbox-Replay-Permission", "test-store/a");
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
            ["draft"] = Field(preview, "draft"), ["reason"] = reason, ["acknowledgePossibleDuplicate"] = "true",
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
        app.Store.OutboxState = OutboxState.DeadLetter;
        using var client = Operator(app);
        var review = await Review(client);
        Assert.Equal(0, app.ReplayStore.OutboxCalls);
        Assert.Contains("&lt;script&gt;", review);
        Assert.DoesNotContain("<script>", review);
        var submitted = await client.PostAsync(Path + "?handler=Submit", Submit(review));
        Assert.Equal(HttpStatusCode.Redirect, submitted.StatusCode);
        var expectedActor = StableHash.Of("eventguard-operator/v1", "https://identity.test", "operator-a");
        Assert.Equal(expectedActor, app.ReplayStore.LastOutboxRequest!.Actor);
        var repeated = await client.PostAsync(Path + "?handler=Submit", Submit(review));
        Assert.Equal(submitted.Headers.Location, repeated.Headers.Location);
        Assert.Equal(1, app.ReplayStore.OutboxGenerations);
        var result = await client.GetStringAsync(submitted.Headers.Location);
        Assert.Contains("Replay accepted", result);
        Assert.Contains("not confirmation that publication has completed", result);
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
        using var client = app.Client();
        client.DefaultRequestHeaders.Add("Test-Outbox-Permission", scopeClaim);
        if (replayClaim) client.DefaultRequestHeaders.Add("Test-Outbox-Replay-Permission", "test-store/a");
        client.DefaultRequestHeaders.Add("Test-Subject", subject);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync(Path)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync(Api, new FormUrlEncodedContent(new Dictionary<string, string> { ["ticket"] = "bogus" }))).StatusCode);
        Assert.Equal(0, app.Store.OutboxCalls);
        Assert.Equal(0, app.ReplayStore.OutboxCalls);
    }

    [Fact]
    public async Task Api_requires_authentication_antiforgery_and_an_actor_bound_review_ticket()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.OutboxState = OutboxState.DeadLetter;
        using var anonymous = app.Client(null);
        Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.PostAsync(Api, null)).StatusCode);
        using var client = Operator(app);
        var review = await Review(client);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.PostAsync(Api, Submit(review, "invalid"))).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.PostAsync(Path + "?handler=Submit", Submit(review, "invalid"))).StatusCode);
        using var other = Operator(app, "operator-b");
        var otherReview = await Review(other);
        Assert.Equal(HttpStatusCode.BadRequest, (await other.PostAsync(Api, Submit(review, Field(otherReview, "__RequestVerificationToken")))).StatusCode);
        Assert.Equal(0, app.ReplayStore.OutboxCalls);
        Assert.Equal(HttpStatusCode.OK, (await client.PostAsync(Api, Submit(review))).StatusCode);
        var id = app.ReplayStore.LastOutboxRequest!.RequestId;
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync(Api + "/" + id)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await other.GetAsync(Api + "/" + id)).StatusCode);
    }

    [Theory]
    [InlineData(OutboxState.Published)] [InlineData(OutboxState.Publishing)] [InlineData(OutboxState.Pending)] [InlineData(OutboxState.Quarantined)]
    public async Task Ineligible_work_has_no_command_form(OutboxState state)
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.OutboxState = state;
        using var client = Operator(app);
        var html = await client.GetStringAsync(Path);
        Assert.DoesNotContain("name=\"draft\"", html);
        Assert.Equal(0, app.ReplayStore.OutboxCalls);
    }

    [Fact]
    public async Task Lost_response_keeps_the_exact_ticket_and_checking_status_does_not_issue_a_command()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.OutboxState = OutboxState.DeadLetter; app.ReplayStore.LoseNextOutboxReply = true;
        using var client = Operator(app);
        var review = await Review(client);
        var response = await client.PostAsync(Path + "?handler=Submit", Submit(review));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        var uncertain = await response.Content.ReadAsStringAsync();
        Assert.Contains("Outcome not confirmed", uncertain);
        Assert.DoesNotContain("never-render", uncertain);
        Assert.Equal(Field(review, "ticket"), Field(uncertain, "ticket"));
        var receiptUrl = Api + "/" + app.ReplayStore.LastOutboxRequest!.RequestId;
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync(receiptUrl)).StatusCode);
        Assert.Equal(1, app.ReplayStore.OutboxCalls);
        Assert.Equal(HttpStatusCode.Redirect, (await client.PostAsync(Path + "?handler=Submit", Submit(uncertain))).StatusCode);
        Assert.Equal(1, app.ReplayStore.OutboxGenerations);
    }

    [Fact]
    public async Task A_review_ticket_can_move_between_hosts_with_shared_protection_and_one_durable_request_identity()
    {
        var store = new ReplayTestStore(); var protection = new EphemeralDataProtectionProvider();
        await using var a = new DashboardFactory(overrides: Config(), replayStore: store, sharedProtection: protection);
        await using var b = new DashboardFactory(overrides: Config(), replayStore: store, sharedProtection: protection);
        a.Store.OutboxState = b.Store.OutboxState = OutboxState.DeadLetter;
        using var first = Operator(a); using var second = Operator(b);
        var review = await Review(first);
        var secondReview = await Review(second);
        var replies = await Task.WhenAll(first.PostAsync(Api, Submit(review)),
            second.PostAsync(Api, Submit(review, Field(secondReview, "__RequestVerificationToken"))));
        Assert.All(replies, r => Assert.Equal(HttpStatusCode.OK, r.StatusCode));
        Assert.Equal(1, store.OutboxGenerations);
    }

    [Fact]
    public async Task A_stale_review_does_not_claim_success_when_store_rejects_expected_version()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.OutboxState = OutboxState.DeadLetter; app.ReplayStore.NextOutboxStatus = OutboxReplayStatus.VersionConflict;
        using var client = Operator(app);
        var review = await Review(client);
        var result = await client.PostAsync(Path + "?handler=Submit", Submit(review));
        Assert.Equal(HttpStatusCode.Redirect, result.StatusCode);
        Assert.Contains("Publication changed", await client.GetStringAsync(result.Headers.Location));
        Assert.Equal(0, app.ReplayStore.OutboxGenerations);
    }

    [Fact]
    public async Task Uncertain_delivery_requires_explicit_acknowledgement_and_draft_is_not_a_command()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.OutboxState = OutboxState.DeadLetter;
        using var client = Operator(app);
        var preview = await client.GetStringAsync(Path);
        Assert.Contains("type=\"checkbox\"", preview);
        Assert.DoesNotContain("checked", preview);
        foreach (var ack in new string?[] { null, "false" })
        {
            var fields = new Dictionary<string, string> { ["draft"] = Field(preview, "draft"), ["reason"] = "fixed",
                ["__RequestVerificationToken"] = Field(preview, "__RequestVerificationToken") };
            if (ack is not null) fields["acknowledgePossibleDuplicate"] = ack;
            var result = await client.PostAsync(Path + "?handler=Review", new FormUrlEncodedContent(fields));
            Assert.Equal(HttpStatusCode.BadRequest, result.StatusCode);
            Assert.Contains("Explicitly acknowledge", await result.Content.ReadAsStringAsync());
        }
        var submitDraft = new FormUrlEncodedContent(new Dictionary<string, string> { ["ticket"] = Field(preview, "draft"),
            ["__RequestVerificationToken"] = Field(preview, "__RequestVerificationToken") });
        Assert.Equal(HttpStatusCode.BadRequest, (await client.PostAsync(Api, submitDraft)).StatusCode);
        Assert.Equal(0, app.ReplayStore.OutboxCalls);
        Assert.Equal(0, app.ReplayStore.Calls);
    }

    [Fact]
    public async Task Changed_acknowledgement_conflicts_and_false_is_never_upgraded_on_submit()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.OutboxState = OutboxState.DeadLetter; app.Store.DeliveryUncertain = false;
        using var client = Operator(app);
        var preview = await client.GetStringAsync(Path);
        Assert.DoesNotContain("type=\"checkbox\"", preview);
        async Task<string> WithAck(bool ack)
        {
            var result = await client.PostAsync(Path + "?handler=Review", new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["draft"] = Field(preview, "draft"), ["reason"] = "fixed", ["acknowledgePossibleDuplicate"] = ack.ToString(),
                ["__RequestVerificationToken"] = Field(preview, "__RequestVerificationToken")
            }));
            Assert.Equal(HttpStatusCode.OK, result.StatusCode);
            return await result.Content.ReadAsStringAsync();
        }
        var first = await WithAck(false); var changed = await WithAck(true);
        Assert.Equal(HttpStatusCode.OK, (await client.PostAsync(Api, Submit(first))).StatusCode);
        Assert.False(app.ReplayStore.LastOutboxRequest!.AcknowledgePossibleDuplicate);
        Assert.Equal(HttpStatusCode.Conflict, (await client.PostAsync(Api, Submit(changed))).StatusCode);
        Assert.Equal(1, app.ReplayStore.OutboxGenerations);
    }

    [Theory]
    [InlineData("Dashboard:OutboxReplayEnabled")]
    [InlineData("Dashboard:OutboxScopes:0:AllowReplay")]
    public async Task Global_and_per_producer_switches_default_to_no_command_access(string setting)
    {
        var config = Config(); config[setting] = "false";
        await using var app = new DashboardFactory(overrides: config);
        using var client = Operator(app);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync(Path)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync(Api, null)).StatusCode);
        Assert.Equal(0, app.ReplayStore.OutboxCalls);
    }

    [Theory]
    [InlineData(OutboxReplayStatus.DeliveryUncertain, "Duplicate risk requires acknowledgement")]
    [InlineData(OutboxReplayStatus.Quarantined, "Publication is quarantined")]
    [InlineData(OutboxReplayStatus.NotFailed, "Publication is not eligible")]
    public async Task Database_rejections_remain_saved_rejections(OutboxReplayStatus status, string title)
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.OutboxState = OutboxState.DeadLetter; app.ReplayStore.NextOutboxStatus = status;
        using var client = Operator(app);
        var review = await Review(client);
        var result = await client.PostAsync(Path + "?handler=Submit", Submit(review));
        Assert.Equal(HttpStatusCode.Redirect, result.StatusCode);
        Assert.Contains(title, await client.GetStringAsync(result.Headers.Location));
        Assert.Equal(0, app.ReplayStore.OutboxGenerations);
    }
    [Fact]
    public async Task Consumer_replay_permission_does_not_authorize_outbox_and_receipts_do_not_require_a_fresh_preview()
    {
        await using var app = new DashboardFactory(overrides: Config());
        app.Store.OutboxState = OutboxState.DeadLetter;
        using var client = Operator(app);
        var review = await Review(client);
        client.DefaultRequestHeaders.Remove("Test-Outbox-Replay-Permission");
        client.DefaultRequestHeaders.Add("Test-Replay-Permission", "test-store/a");
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync(Api, Submit(review))).StatusCode);
        Assert.Equal(0, app.ReplayStore.OutboxCalls);
        client.DefaultRequestHeaders.Add("Test-Outbox-Replay-Permission", "test-store/a");
        Assert.Equal(HttpStatusCode.OK, (await client.PostAsync(Api, Submit(review))).StatusCode);
        app.Store.OutboxState = OutboxState.Published; app.Store.Unavailable = true;
        Assert.Equal(HttpStatusCode.OK, (await client.PostAsync(Api, Submit(review))).StatusCode);
        Assert.Equal(1, app.ReplayStore.OutboxGenerations);
        Assert.Equal(0, app.ReplayStore.Calls);
    }
}
