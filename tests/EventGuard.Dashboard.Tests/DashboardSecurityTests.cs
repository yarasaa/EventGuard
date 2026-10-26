using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace EventGuard.Dashboard.Tests;

public sealed class DashboardSecurityTests
{
    [Theory]
    [InlineData("/api/works")]
    [InlineData("/api/scopes")]
    [InlineData("/api/works/a/1a752644-297b-4f6e-8ec9-e8c7d5d413f7")]
    public async Task Anonymous_api_requests_get_401_without_querying_the_database(string path)
    {
        await using var app = new DashboardFactory(false);
        using var client = app.Client(null);
        Assert.Equal(HttpStatusCode.Unauthorized, (await client.GetAsync(path)).StatusCode);
        Assert.Equal(0, app.Store.Calls);
    }

    [Theory]
    [InlineData("/api/works?scope=b")]
    [InlineData("/api/works?scope=missing")]
    [InlineData("/api/works/b/1a752644-297b-4f6e-8ec9-e8c7d5d413f7")]
    [InlineData("/works/b/1a752644-297b-4f6e-8ec9-e8c7d5d413f7")]
    public async Task Unknown_and_unauthorized_scopes_have_same_404_and_never_reach_store(string path)
    {
        await using var app = new DashboardFactory();
        using var client = app.Client();
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync(path)).StatusCode);
        Assert.Equal(0, app.Store.Calls);
    }

    [Fact]
    public async Task Detail_checks_work_id_within_the_authorized_scope()
    {
        await using var app = new DashboardFactory();
        using var client = app.Client("test-store/b");
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/api/works/b/" + MetadataStore.WorkId)).StatusCode);
        Assert.Equal(new WorkScope("b", "indexer", "documents", "updated").Id, app.Store.LastScope);
    }

    [Fact]
    public async Task Page_escapes_labels_only_offers_authorized_scopes_and_sets_privacy_headers()
    {
        await using var app = new DashboardFactory();
        using var client = app.Client();
        var response = await client.GetAsync("/");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        Assert.Contains("&lt;script&gt;", html);
        Assert.DoesNotContain("<script>", html);
        Assert.DoesNotContain("Other tenant", html);
        Assert.Contains("no-store", response.Headers.CacheControl!.ToString());
        Assert.Contains("frame-ancestors 'none'", response.Headers.GetValues("Content-Security-Policy").Single());
    }

    [Fact]
    public async Task Database_failure_is_503_and_never_presented_as_empty_or_secret_details()
    {
        await using var app = new DashboardFactory();
        app.Store.Unavailable = true;
        using var client = app.Client();
        foreach (var path in new[] { "/", "/api/works", "/works/a/" + MetadataStore.WorkId })
        {
            var response = await client.GetAsync(path);
            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
            var text = await response.Content.ReadAsStringAsync();
            Assert.DoesNotContain("do-not-render-this", text);
            Assert.DoesNotContain("Password", text);
            Assert.DoesNotContain("class=\"empty\"", text);
        }
    }

    [Fact]
    public async Task Cursor_is_signed_and_bound_to_scope_and_filter()
    {
        await using var app = new DashboardFactory();
        app.Store.NextPage = true;
        using var client = app.Client();
        var first = JsonDocument.Parse(await client.GetStringAsync("/api/works?scope=a"));
        var cursor = first.RootElement.GetProperty("nextCursor").GetString()!;
        var escaped = Uri.EscapeDataString(cursor);
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/api/works?scope=a&cursor=" + escaped)).StatusCode);
        var calls = app.Store.Calls;
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/api/works?scope=a&state=Completed&cursor=" + escaped)).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("/api/works?scope=a&cursor=corrupt" + escaped)).StatusCode);
        using var other = app.Client("test-store/b");
        Assert.Equal(HttpStatusCode.BadRequest, (await other.GetAsync("/api/works?scope=b&cursor=" + escaped)).StatusCode);
        Assert.Equal(calls, app.Store.Calls);
    }

    [Theory]
    [InlineData("/api/works?state=999")]
    [InlineData("/api/works?state=broken")]
    [InlineData("/api/works?scope=a&cursor=invalid")]
    public async Task Invalid_queries_fail_before_database_access(string path)
    {
        await using var app = new DashboardFactory();
        using var client = app.Client();
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync(path)).StatusCode);
        Assert.Equal(0, app.Store.Calls);
    }

    [Fact]
    public async Task Http_and_untrusted_hosts_are_rejected_and_forwarded_headers_do_not_grant_trust()
    {
        await using var app = new DashboardFactory();
        using var client = app.Client();
        client.DefaultRequestHeaders.Add("X-Forwarded-Proto", "https");
        client.DefaultRequestHeaders.Add("X-Forwarded-Host", "dashboard.test");
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("http://dashboard.test/api/works")).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.GetAsync("https://attacker.test/api/works")).StatusCode);
        Assert.Equal(0, app.Store.Calls);
    }

    [Fact]
    public async Task Production_oidc_is_code_pkce_with_secure_short_lived_cookie_and_no_saved_tokens()
    {
        await using var app = new DashboardFactory(false);
        using var client = app.Client(null);
        var oidc = app.Services.GetRequiredService<IOptionsMonitor<OpenIdConnectOptions>>().Get("OpenIdConnect");
        Assert.Equal("code", oidc.ResponseType);
        Assert.True(oidc.UsePkce);
        Assert.True(oidc.RequireHttpsMetadata);
        Assert.False(oidc.MapInboundClaims);
        Assert.False(oidc.SaveTokens);
        var cookie = app.Services.GetRequiredService<IOptionsMonitor<CookieAuthenticationOptions>>().Get("Cookies");
        Assert.Equal(Microsoft.AspNetCore.Http.CookieSecurePolicy.Always, cookie.Cookie.SecurePolicy);
        Assert.False(cookie.SlidingExpiration);
        Assert.Equal(TimeSpan.FromMinutes(30), cookie.ExpireTimeSpan);
    }

    [Fact]
    public async Task No_replay_write_endpoint_is_available()
    {
        await using var app = new DashboardFactory();
        using var client = app.Client();
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync("/api/works/a/" + MetadataStore.WorkId + "/replay", null)).StatusCode);
        Assert.Equal(0, app.Store.Calls);
    }

    [Theory]
    [InlineData("Dashboard:DevelopmentAccess", "true")]
    [InlineData("Dashboard:Authority", "http://identity.test")]
    [InlineData("Dashboard:ClientSecret", "")]
    [InlineData("Dashboard:AllowedHosts:0", "*")]
    public async Task Unsafe_production_configuration_cannot_start(string key, string value)
    {
        await using var app = new DashboardFactory(overrides: new() { [key] = value });
        Assert.ThrowsAny<InvalidOperationException>(() => app.Client());
    }
}
