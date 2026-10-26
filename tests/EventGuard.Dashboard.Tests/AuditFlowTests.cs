using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using EventGuard.Operations;
namespace EventGuard.Dashboard.Tests;
// HTTP/auth/cursor tests use an explicit read double; native ledger reads are tested separately.
public sealed partial class MetadataStore : IAuditReadStore
{
    public int AuditCalls { get; private set; }
    public Task ValidateAuditAsync(bool consumer, bool outbox, CancellationToken ct = default) => Task.CompletedTask;
    public Task<AuditPage> ListAuditAsync(AuditQuery query, CancellationToken ct = default)
    {
        AuditCalls++; if (Unavailable) throw new InvalidOperationException("secret-connection");
        var time = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        var items = Enumerable.Range(1, 120).Select(i => new AuditEntry(new Guid(i, 0, 0, new byte[8]), AuditKind.ConsumerReplay, WorkId,
            "private-actor", "=FORMULA <script>alert(1)</script>", "Accepted", false, time.AddSeconds(-i))).Where(e => e.CreatedAt >= query.From && e.CreatedAt < query.Until);
        if (query.Before is { } before) items = items.Where(e => e.CreatedAt < before.CreatedAt);
        if (query.After is { } after) items = items.Where(e => e.CreatedAt > after.CreatedAt).Reverse();
        return Task.FromResult(AuditPage.FromWindow(items.Take(query.PageSize + 1).ToList(), query));
    }
}
public sealed class AuditFlowTests
{
    private const string Dates = "from=2026-01-01T00:00:00&until=2026-01-02T00:00:00";
    private static Dictionary<string,string?> Config() => new()
    {
        ["Dashboard:AuditEnabled"] = "true", ["Dashboard:Scopes:0:AllowAudit"] = "true", ["Dashboard:Scopes:1:AllowAudit"] = "true",
        ["Dashboard:OutboxScopes:0:Key"] = "a", ["Dashboard:OutboxScopes:0:Label"] = "Events", ["Dashboard:OutboxScopes:0:Tenant"] = "a",
        ["Dashboard:OutboxScopes:0:Producer"] = "publisher", ["Dashboard:OutboxScopes:0:AllowAudit"] = "true"
    };
    private static string Link(string html, string marker) => WebUtility.HtmlDecode(Regex.Match(html, "<a[^>]*" + marker + "[^>]*href=\"([^\"]+)\"").Groups[1].Value);
    [Theory] [InlineData("consumer")] [InlineData("outbox")]
    public async Task Audit_requires_separate_permission_before_parsing_and_exports_only_one_bounded_page(string kind)
    {
        await using var app = new DashboardFactory(overrides: Config()); using var client = app.Client();
        client.DefaultRequestHeaders.Add("Test-Outbox-Permission", "test-store/a");
        var path = $"/audit/{kind}/a";
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync(path + "?cursor=bad")).StatusCode); Assert.Equal(0, app.Store.AuditCalls);
        client.DefaultRequestHeaders.Add(kind == "consumer" ? "Test-Audit-Permission" : "Test-Outbox-Audit-Permission", "test-store/a");
        var html = await client.GetStringAsync(path + "?" + Dates); Assert.DoesNotContain("<script>", html); Assert.Contains("&lt;script&gt;", html);
        var exported = await client.GetAsync(path + "?handler=Export&" + Dates);
        Assert.Equal("application/x-ndjson", exported.Content.Headers.ContentType!.MediaType);
        Assert.Contains("no-store", exported.Headers.CacheControl!.ToString()); Assert.Equal("attachment", exported.Content.Headers.ContentDisposition!.DispositionType);
        var rows = (await exported.Content.ReadAsStringAsync()).Split('\n', StringSplitOptions.RemoveEmptyEntries); Assert.Equal(100, rows.Length);
        using var first = JsonDocument.Parse(rows[0]); Assert.Equal("=FORMULA <script>alert(1)</script>", first.RootElement.GetProperty("Reason").GetString());
        var next = Link(html, "rel=\"next\""); Assert.NotEmpty(next);
        var second = await client.GetStringAsync(next); var back = Link(second, "rel=\"prev\""); Assert.NotEmpty(back);
        Assert.Contains(MetadataStore.WorkId.ToString(), await client.GetStringAsync(back));
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync(next.Replace("2026-01-01", "2025-12-31"))).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync(next.Replace("/a?", "/b?"))).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync(path + "?from=2025-01-01T00:00&until=2026-01-01T00:00")).StatusCode);
        app.Store.Unavailable = true; var failed = await client.GetAsync(path + "?handler=Export&" + Dates); Assert.Equal(HttpStatusCode.ServiceUnavailable, failed.StatusCode);
        Assert.DoesNotContain("secret-", await failed.Content.ReadAsStringAsync());
    }
    [Fact]
    public async Task Audit_is_off_by_default()
    {
        await using var app = new DashboardFactory(); using var client = app.Client(); client.DefaultRequestHeaders.Add("Test-Audit-Permission", "test-store/a");
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/audit/consumer/a")).StatusCode); Assert.Equal(0, app.Store.AuditCalls);
    }
}
