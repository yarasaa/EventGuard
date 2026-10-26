using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

namespace EventGuard.Dashboard;

// Registered only after startup has validated the explicit Development-only loopback profile.
internal sealed class LocalDevelopmentAuthentication(IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger, UrlEncoder encoder, DashboardOptions dashboard) : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var claims = dashboard.Scopes.Select(s => new Claim("eventguard_scope", dashboard.StoreId + "/" + s.Key)).ToList();
        claims.AddRange(dashboard.Scopes.Where(s => s.DataFields.Length > 0).Select(s => new Claim("eventguard_data", dashboard.StoreId + "/" + s.Key)));
        claims.AddRange(dashboard.Modules.Select(s => new Claim("eventguard_module", dashboard.StoreId + "/" + s.Key)));
        claims.AddRange(dashboard.OutboxScopes.Select(s => new Claim("eventguard_outbox", dashboard.StoreId + "/" + s.Key)));
        if (dashboard.MonitoringEnabled) claims.Add(new("eventguard_monitor", dashboard.StoreId));
        claims.Add(new("name", "Local developer"));
        claims.Add(new("iss", "eventguard:local:" + dashboard.StoreId));
        claims.Add(new("sub", "local-developer"));
        if (dashboard.ReplayEnabled) claims.AddRange(dashboard.Scopes.Where(s => s.AllowReplay)
            .Select(s => new Claim("eventguard_replay", dashboard.StoreId + "/" + s.Key)));
        if (dashboard.OutboxReplayEnabled) claims.AddRange(dashboard.OutboxScopes.Where(s => s.AllowReplay)
            .Select(s => new Claim("eventguard_outbox_replay", dashboard.StoreId + "/" + s.Key)));
        if (dashboard.RecoveryEnabled)
        {
            claims.AddRange(dashboard.Scopes.Where(s => s.AllowRecovery).Select(s => new Claim("eventguard_recovery", dashboard.StoreId + "/" + s.Key)));
            claims.AddRange(dashboard.OutboxScopes.Where(s => s.AllowRecovery).Select(s => new Claim("eventguard_outbox_recovery", dashboard.StoreId + "/" + s.Key)));
        }
        if (dashboard.AuditEnabled)
        {
            claims.AddRange(dashboard.Scopes.Where(s => s.AllowAudit).Select(s => new Claim("eventguard_audit", dashboard.StoreId + "/" + s.Key)));
            claims.AddRange(dashboard.OutboxScopes.Where(s => s.AllowAudit).Select(s => new Claim("eventguard_outbox_audit", dashboard.StoreId + "/" + s.Key)));
        }
        var identity = new ClaimsIdentity(claims, Scheme.Name, "name", "role");
        return Task.FromResult(AuthenticateResult.Success(new(new ClaimsPrincipal(identity), Scheme.Name)));
    }
}
