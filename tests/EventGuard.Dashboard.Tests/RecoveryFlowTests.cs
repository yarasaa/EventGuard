using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
namespace EventGuard.Dashboard.Tests;

public sealed class RecoveryFlowTests
{
    private static string Path(string kind) => $"/recovery/{kind}/a/{MetadataStore.WorkId}";
    private static Dictionary<string, string?> Config() => new()
    {
        ["Dashboard:IngressMonitoringEnabled"] = "true", ["Dashboard:RecoveryEnabled"] = "true",
        ["Dashboard:ReplayConnectionString"] = "test-double", ["Dashboard:Scopes:0:AllowRecovery"] = "true",
        ["Dashboard:Scopes:1:AllowRecovery"] = "true",
        ["Dashboard:OutboxScopes:0:Key"] = "a", ["Dashboard:OutboxScopes:0:Label"] = "Outgoing documents",
        ["Dashboard:OutboxScopes:0:Tenant"] = "a", ["Dashboard:OutboxScopes:0:Producer"] = "publisher",
        ["Dashboard:OutboxScopes:0:AllowRecovery"] = "true"
    };
    private static HttpClient Operator(DashboardFactory app, string subject = "operator-a")
    {
        var client = app.Client();
        foreach (var name in new[] { "Test-Recovery-Permission", "Test-Outbox-Permission", "Test-Outbox-Recovery-Permission" })
            client.DefaultRequestHeaders.Add(name, "test-store/a");
        client.DefaultRequestHeaders.Add("Test-Subject", subject); return client;
    }
    private static string Manifest(string kind) => kind == "ingress"
        ? JsonSerializer.Serialize(new { messageId = Guid.NewGuid(), payloadBase64 = "e30=", businessKey = "item<script>" })
        : JsonSerializer.Serialize(new { messageId = Guid.NewGuid(), payloadBase64 = "e30=", transport = "kafka", connection = "events", configurationVersion = "v1", address = "topic" });
    private static string Field(string html, string name)
    {
        var match = Regex.Match(html, "<input[^>]*name=\"" + name + "\"[^>]*value=\"([^\"]*)\"");
        Assert.True(match.Success, "Missing form field: " + name); return WebUtility.HtmlDecode(match.Groups[1].Value);
    }
    private static FormUrlEncodedContent Submit(string review, string? ticket = null, string? csrf = null) => new(new Dictionary<string, string>
    { ["ticket"] = ticket ?? Field(review, "ticket"), ["__RequestVerificationToken"] = csrf ?? Field(review, "__RequestVerificationToken") });
    private static async Task<string> Review(HttpClient client, string kind, string? manifest = null, string? preview = null, string reason = "Corrected <script>input</script>")
    {
        preview ??= await client.GetStringAsync(Path(kind));
        var response = await client.PostAsync(Path(kind) + "?handler=Review", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["draft"] = Field(preview, "draft"), ["manifest"] = manifest ?? Manifest(kind), ["reason"] = reason,
            ["acknowledgePossibleDuplicate"] = "true", ["__RequestVerificationToken"] = Field(preview, "__RequestVerificationToken")
        }));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode); return await response.Content.ReadAsStringAsync();
    }
    [Theory] [InlineData("ingress")] [InlineData("outbox")]
    public async Task Reviewed_request_is_actor_bound_escaped_idempotent_and_survives_lost_reply(string kind)
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Operator(app);
        var review = await Review(client, kind);
        Assert.Equal(0, app.ReplayStore.RecoveryCalls); Assert.DoesNotContain("<script>", review); Assert.Contains("&lt;script&gt;", review);
        app.ReplayStore.LoseNextRecoveryReply = true;
        var lost = await client.PostAsync(Path(kind) + "?handler=Submit", Submit(review));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, lost.StatusCode);
        var retry = await lost.Content.ReadAsStringAsync(); Assert.DoesNotContain("never-render", retry); Assert.Equal(Field(review, "ticket"), Field(retry, "ticket"));
        app.Store.Unavailable = true; // Receipt and repeat must not depend on fresh preview reads.
        var repeated = await client.PostAsync(Path(kind) + "?handler=Submit", Submit(retry));
        Assert.Equal(HttpStatusCode.Redirect, repeated.StatusCode); Assert.Equal(1, app.ReplayStore.Recoveries);
        var receipt = await client.GetStringAsync(repeated.Headers.Location); Assert.Contains("Accepted", receipt); Assert.Contains("&lt;script&gt;", receipt);
        using var other = Operator(app, "operator-b"); Assert.Equal(HttpStatusCode.NotFound, (await other.GetAsync(repeated.Headers.Location)).StatusCode);
        var expectedActor = StableHash.Of("eventguard-operator/v1", "https://identity.test", "operator-a");
        Assert.Equal(expectedActor, kind == "ingress" ? app.ReplayStore.LastRelease!.Actor : app.ReplayStore.LastRepair!.Actor);
    }
    [Theory] [InlineData("ingress")] [InlineData("outbox")]
    public async Task Csrf_draft_tampering_different_actor_and_revoked_permission_block_commands(string kind)
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Operator(app);
        var preview = await client.GetStringAsync(Path(kind)); var review = await Review(client, kind, preview: preview);
        foreach (var ticket in new[] { Field(preview, "draft"), Field(review, "ticket") + "bad" })
            Assert.Equal(HttpStatusCode.BadRequest, (await client.PostAsync(Path(kind) + "?handler=Submit", Submit(review, ticket))).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.PostAsync(Path(kind) + "?handler=Submit", Submit(review, csrf: "bad"))).StatusCode);
        using var other = Operator(app, "operator-b"); var otherReview = await Review(other, kind);
        Assert.Equal(HttpStatusCode.BadRequest, (await other.PostAsync(Path(kind) + "?handler=Submit", Submit(review, csrf: Field(otherReview, "__RequestVerificationToken")))).StatusCode);
        client.DefaultRequestHeaders.Remove(kind == "ingress" ? "Test-Recovery-Permission" : "Test-Outbox-Recovery-Permission");
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync(Path(kind) + "?handler=Submit", Submit(review))).StatusCode);
        Assert.Equal(0, app.ReplayStore.RecoveryCalls);
    }
    [Theory] [InlineData("ingress")] [InlineData("outbox")]
    public async Task Changing_same_draft_content_is_a_durable_request_conflict(string kind)
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Operator(app);
        var preview = await client.GetStringAsync(Path(kind)); var manifest = Manifest(kind);
        var first = await Review(client, kind, manifest, preview); var changed = await Review(client, kind, manifest, preview, "different");
        Assert.Equal(HttpStatusCode.Redirect, (await client.PostAsync(Path(kind) + "?handler=Submit", Submit(first))).StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, (await client.PostAsync(Path(kind) + "?handler=Submit", Submit(changed))).StatusCode);
        Assert.Equal(1, app.ReplayStore.Recoveries);
    }
    [Fact]
    public async Task Viewer_has_metadata_but_replay_permission_does_not_grant_recovery()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = app.Client();
        client.DefaultRequestHeaders.Add("Test-Replay-Permission", "test-store/a");
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/ingress?scope=a")).StatusCode);
        var reads = app.Store.IngressCalls;
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync(Path("ingress"))).StatusCode);
        Assert.Equal(reads, app.Store.IngressCalls); Assert.Equal(0, app.ReplayStore.RecoveryCalls);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/ingress?scope=b&id=bad&cursor=bad")).StatusCode);
        Assert.Equal(reads, app.Store.IngressCalls);
    }
    [Fact]
    public async Task Pagination_preserves_filters_and_previous_direction_and_refuses_cross_scope_cursor()
    {
        await using var app = new DashboardFactory(overrides: Config()); app.Store.NextPage = true; using var client = app.Client();
        var first = await client.GetStringAsync("/ingress?scope=a&state=Held");
        var next = WebUtility.HtmlDecode(Regex.Match(first, "<a[^>]*rel=\"next\"[^>]*href=\"([^\"]+)\"").Groups[1].Value);
        Assert.NotEmpty(next); var second = await client.GetStringAsync(next); Assert.NotNull(app.Store.LastIngressQuery!.Window.Before);
        var previous = WebUtility.HtmlDecode(Regex.Match(second, "<a[^>]*rel=\"prev\"[^>]*href=\"([^\"]+)\"").Groups[1].Value);
        Assert.NotEmpty(previous); Assert.Equal(HttpStatusCode.OK, (await client.GetAsync(previous)).StatusCode); Assert.NotNull(app.Store.LastIngressQuery!.Window.After);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync(next.Replace("Held", "Released"))).StatusCode);
        using var other = app.Client("test-store/b"); Assert.Equal(HttpStatusCode.BadRequest, (await other.GetAsync(next.Replace("scope=a", "scope=b"))).StatusCode);
    }
    [Fact]
    public async Task Unknown_delivery_requires_acknowledgement_and_ineligible_records_have_no_draft()
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = Operator(app);
        var preview = await client.GetStringAsync(Path("outbox"));
        var response = await client.PostAsync(Path("outbox") + "?handler=Review", new FormUrlEncodedContent(new Dictionary<string, string>
        { ["draft"] = Field(preview, "draft"), ["manifest"] = Manifest("outbox"), ["reason"] = "fixed", ["__RequestVerificationToken"] = Field(preview, "__RequestVerificationToken") }));
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode); Assert.Equal(0, app.ReplayStore.RecoveryCalls);
        app.Store.OutboxState = OutboxState.Published; app.Store.IngressReleased = true;
        Assert.DoesNotContain("name=\"draft\"", await client.GetStringAsync(Path("outbox")));
        Assert.DoesNotContain("name=\"draft\"", await client.GetStringAsync(Path("ingress")));
    }
    [Fact]
    public void Manifest_rejects_ambiguous_unknown_oversize_and_mismatched_fingerprints()
    {
        Assert.Throws<ArgumentException>(() => RecoveryManifest.Parse("{\"messageId\":1,\"MessageId\":2}"));
        Assert.Throws<JsonException>(() => RecoveryManifest.Parse("{\"unexpected\":1}"));
        Assert.Throws<ArgumentException>(() => RecoveryManifest.Parse(new string('a', 4097)));
        var manifest = new RecoveryManifest(Guid.NewGuid(), "e30=", "key", new string('0', 64));
        Assert.Throws<ArgumentException>(() => manifest.Submission(new("t", "c", "s", "e"), "v1", "sha256-bytes/v1"));
        Assert.Throws<ArgumentException>(() => (manifest with { PayloadBase64 = Convert.ToBase64String(new byte[2049]) }).Submission(new("t", "c", "s", "e"), "v1", "sha256-bytes/v1"));
    }
}
