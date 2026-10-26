using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace EventGuard.Dashboard.Tests;

public sealed class OidcFlowTests
{
    [Theory]
    [InlineData("valid", HttpStatusCode.OK)]
    [InlineData("wrong-audience", HttpStatusCode.Unauthorized)]
    [InlineData("wrong-issuer", HttpStatusCode.Unauthorized)]
    [InlineData("wrong-nonce", HttpStatusCode.Unauthorized)]
    [InlineData("wrong-signature", HttpStatusCode.Unauthorized)]
    public async Task Real_oidc_handler_validates_signed_id_token_before_issuing_scope_cookie(string scenario, HttpStatusCode expected)
    {
        using var idp = new OidcTestProvider { Scenario = scenario };
        await using var app = new DashboardFactory(false, idp: idp);
        using var client = app.Client(null);
        var challenge = await client.GetAsync("/sign-in");
        Assert.Equal(HttpStatusCode.Redirect, challenge.StatusCode);
        var parameters = QueryHelpers.ParseQuery(challenge.Headers.Location!.Query);
        Assert.Equal("code", parameters["response_type"]);
        Assert.Equal("S256", parameters["code_challenge_method"]);
        idp.Nonce = parameters["nonce"].ToString();
        idp.CodeChallenge = parameters["code_challenge"].ToString();
        var callback = await client.GetAsync("/signin-oidc?code=test-code&state=" + Uri.EscapeDataString(parameters["state"].ToString()));
        Assert.Equal(HttpStatusCode.Redirect, callback.StatusCode);
        if (scenario == "valid")
        {
            Assert.Equal("/", callback.Headers.Location!.OriginalString);
            Assert.Contains(callback.Headers.GetValues("Set-Cookie"), c => c.StartsWith("__Host-EventGuard=", StringComparison.Ordinal) && c.Contains("secure", StringComparison.OrdinalIgnoreCase));
        }
        else Assert.Equal("/auth-error", callback.Headers.Location!.OriginalString);
        Assert.True(idp.ValidPkceExchange);
        var result = await client.GetAsync("/api/works?scope=a");
        Assert.Equal(expected, result.StatusCode);
        Assert.Equal(scenario == "valid" ? 1 : 0, app.Store.Calls);
        Assert.Equal(scenario == "valid" ? HttpStatusCode.NotFound : HttpStatusCode.Unauthorized,
            (await client.GetAsync("/api/works?scope=b")).StatusCode);
        if (scenario == "valid")
        {
            Assert.Equal(HttpStatusCode.BadRequest, (await client.PostAsync("/Logout", null)).StatusCode);
            var html = await client.GetStringAsync("/Logout");
            var match = System.Text.RegularExpressions.Regex.Match(html, "name=\"__RequestVerificationToken\" type=\"hidden\" value=\"([^\"]+)\"");
            Assert.True(match.Success);
            var logout = await client.PostAsync("/Logout", new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["__RequestVerificationToken"] = WebUtility.HtmlDecode(match.Groups[1].Value)
            }));
            Assert.Equal(HttpStatusCode.Redirect, logout.StatusCode);
            Assert.Equal("/signed-out", logout.Headers.Location!.OriginalString);
            Assert.Equal(HttpStatusCode.Unauthorized, (await client.GetAsync("/api/works")).StatusCode);
        }
    }
}

// Test-only identity provider: real token signatures/correlation/nonce/cookie flow, no external IdP claim.
public sealed class OidcTestProvider : HttpMessageHandler
{
    private readonly RSA _rsa = RSA.Create(2048);
    public string Scenario { get; set; } = "valid";
    public string Nonce { get; set; } = "";
    public string CodeChallenge { get; set; } = "";
    public bool ValidPkceExchange { get; private set; }
    public OpenIdConnectConfiguration Configuration
    {
        get
        {
            var configuration = new OpenIdConnectConfiguration
            {
                Issuer = "https://identity.test", AuthorizationEndpoint = "https://identity.test/authorize",
                TokenEndpoint = "https://identity.test/token"
            };
            configuration.SigningKeys.Add(new RsaSecurityKey(_rsa) { KeyId = "test-key" });
            return configuration;
        }
    }
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (request.RequestUri!.AbsoluteUri != "https://identity.test/token") throw new InvalidOperationException("Unexpected test IdP request.");
        var form = QueryHelpers.ParseQuery(await request.Content!.ReadAsStringAsync(cancellationToken));
        ValidPkceExchange = form["code"] == "test-code" && form["grant_type"] == "authorization_code"
            && Base64UrlEncoder.Encode(SHA256.HashData(Encoding.ASCII.GetBytes(form["code_verifier"].ToString()))) == CodeChallenge;
        using var other = RSA.Create(2048);
        var key = new RsaSecurityKey(Scenario == "wrong-signature" ? other : _rsa) { KeyId = "test-key" };
        var token = new JwtSecurityToken(Scenario == "wrong-issuer" ? "https://attacker.test" : "https://identity.test",
            Scenario == "wrong-audience" ? "different-client" : "eventguard-tests",
            [new Claim("sub", "viewer-1"), new Claim("nonce", Scenario == "wrong-nonce" ? "invalid" : Nonce),
                new Claim("eventguard_scope", "test-store/a"), new Claim("name", "Test Viewer"),
                new Claim("iat", DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString(System.Globalization.CultureInfo.InvariantCulture), ClaimValueTypes.Integer64)],
            DateTime.UtcNow.AddMinutes(-1), DateTime.UtcNow.AddMinutes(5), new SigningCredentials(key, SecurityAlgorithms.RsaSha256));
        var json = JsonSerializer.Serialize(new
        {
            access_token = "test-only-opaque-access-token", token_type = "Bearer", expires_in = 300,
            id_token = new JwtSecurityTokenHandler().WriteToken(token)
        });
        return new(HttpStatusCode.OK) { Content = new StringContent(json, Encoding.UTF8, "application/json") };
    }
    protected override void Dispose(bool disposing) { if (disposing) _rsa.Dispose(); base.Dispose(disposing); }
}
