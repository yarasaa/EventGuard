using System.Net;
using System.Security.Claims;
using System.Text.Encodings.Web;
using EventGuard.Operations;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventGuard.Dashboard.Tests;

public sealed class DashboardFactory(bool testAuthentication = true, Dictionary<string, string?>? overrides = null,
    OidcTestProvider? idp = null, ReplayTestStore? replayStore = null, IDataProtectionProvider? sharedProtection = null, TimeProvider? time = null, bool persistedProtection = false, IWorkerMonitor? workerMonitor = null) : WebApplicationFactory<Program>
{
    public MetadataStore Store { get; } = new();
    public ReplayTestStore ReplayStore { get; } = replayStore ?? new();
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Production");
        var config = new Dictionary<string, string?>
        {
            ["Dashboard:Provider"] = "PostgreSql", ["Dashboard:ConnectionString"] = "test-double-not-a-connection",
            ["Dashboard:StoreId"] = "test-store", ["Dashboard:EnvironmentLabel"] = "test",
            ["Dashboard:ApplicationLabel"] = "Documents", ["Dashboard:AllowedHosts:0"] = "dashboard.test",
            ["Dashboard:Authority"] = "https://identity.test", ["Dashboard:ClientId"] = "eventguard-tests",
            ["Dashboard:ClientSecret"] = "test-client-secret", ["Dashboard:DevelopmentAccess"] = "false"
        };
        foreach (var key in new[] { "a", "b" })
        {
            var prefix = "Dashboard:Scopes:" + (key == "a" ? "0" : "1") + ":";
            foreach (var (name, value) in new Dictionary<string, string>
            {
                ["Key"] = key, ["Label"] = key == "a" ? "Files <script>alert(1)</script>" : "Other tenant",
                ["Tenant"] = key, ["Consumer"] = "indexer", ["Source"] = "documents", ["EventType"] = "updated"
            }) config[prefix + name] = value;
        }
        if (overrides is not null) foreach (var pair in overrides) config[pair.Key] = pair.Value;
        // WebApplication reads host configuration before service registration.
        foreach (var pair in config) builder.UseSetting(pair.Key, pair.Value);
        builder.ConfigureAppConfiguration((_, configuration) => configuration.AddInMemoryCollection(config));
        builder.ConfigureTestServices(services =>
        {
            if (workerMonitor is not null) { services.RemoveAll<IWorkerMonitor>(); services.AddSingleton(workerMonitor); }
            if (time is not null) { services.RemoveAll<TimeProvider>(); services.AddSingleton(time); }
            services.RemoveAll<IWorkReadStore>();
            services.AddSingleton<IWorkReadStore>(Store);
            services.RemoveAll<IWorkReplayStore>();
            services.AddSingleton<IWorkReplayStore>(ReplayStore);
            if (!persistedProtection) services.AddDataProtection().UseEphemeralDataProtectionProvider();
            if (sharedProtection is not null) { services.RemoveAll<IDataProtectionProvider>(); services.AddSingleton(sharedProtection); }
            if (idp is not null) services.Configure<OpenIdConnectOptions>("OpenIdConnect", options =>
            {
                options.Configuration = idp.Configuration;
                options.BackchannelHttpHandler = idp;
            });
            if (testAuthentication)
                services.AddAuthentication(options =>
                {
                    options.DefaultAuthenticateScheme = "test";
                    options.DefaultChallengeScheme = "test";
                }).AddScheme<AuthenticationSchemeOptions, TestAuthentication>("test", _ => { });
        });
    }
    public HttpClient Client(string? permission = "test-store/a")
    {
        var client = CreateClient(new() { BaseAddress = new("https://dashboard.test"), AllowAutoRedirect = false });
        if (permission is not null) client.DefaultRequestHeaders.Add("Test-Permission", permission);
        return client;
    }

    private sealed class TestAuthentication(IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger, UrlEncoder encoder)
        : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
    {
        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            if (!Request.Headers.TryGetValue("Test-Permission", out var value)) return Task.FromResult(AuthenticateResult.NoResult());
            var identity = new ClaimsIdentity([new Claim("eventguard_scope", value.ToString()), new Claim("iss", "https://identity.test")], Scheme.Name);
            var subject = Request.Headers["Test-Subject"].FirstOrDefault() ?? "operator-a";
            if (subject != "none") identity.AddClaim(new("sub", subject));
            if (Request.Headers.TryGetValue("Test-Module-Permission", out var module)) identity.AddClaim(new("eventguard_module", module.ToString()));
            if (Request.Headers.TryGetValue("Test-Monitor-Permission", out var monitor)) identity.AddClaim(new("eventguard_monitor", monitor.ToString()));
            if (Request.Headers.TryGetValue("Test-Replay-Permission", out var replay)) identity.AddClaim(new("eventguard_replay", replay.ToString()));
            if (Request.Headers.TryGetValue("Test-Outbox-Replay-Permission", out var outboxReplay)) identity.AddClaim(new("eventguard_outbox_replay", outboxReplay.ToString()));
            if (Request.Headers.TryGetValue("Test-Outbox-Permission", out var outbox)) identity.AddClaim(new("eventguard_outbox", outbox.ToString()));
            if (Request.Headers.TryGetValue("Test-Recovery-Permission", out var recovery)) identity.AddClaim(new("eventguard_recovery", recovery.ToString()));
            if (Request.Headers.TryGetValue("Test-Outbox-Recovery-Permission", out var repair)) identity.AddClaim(new("eventguard_outbox_recovery", repair.ToString()));
            if (Request.Headers.TryGetValue("Test-Audit-Permission", out var audit)) identity.AddClaim(new("eventguard_audit", audit.ToString()));
            if (Request.Headers.TryGetValue("Test-Outbox-Audit-Permission", out var outboxAudit)) identity.AddClaim(new("eventguard_outbox_audit", outboxAudit.ToString()));
            if (Request.Headers.TryGetValue("Test-Data-Permission", out var data)) identity.AddClaim(new("eventguard_data", data.ToString()));
            return Task.FromResult(AuthenticateResult.Success(new(new ClaimsPrincipal(identity), Scheme.Name)));
        }
    }
}

public sealed partial class MetadataStore : IWorkReadStore, IWorkDataReadStore, IStoreHealthCheck
{
    public int HealthCalls { get; private set; }
    public TaskCompletionSource? HealthRelease { get; set; }
    public async Task CheckHealthAsync(bool consumer, bool outbox, CancellationToken ct = default)
    {
        HealthCalls++;
        if (HealthRelease is not null) await HealthRelease.Task.WaitAsync(ct);
        if (Unavailable) throw new InvalidOperationException("Password=never-display");
    }
    public static readonly Guid WorkId = Guid.Parse("1a752644-297b-4f6e-8ec9-e8c7d5d413f7");
    public int Calls { get; private set; }
    public bool Unavailable { get; set; }
    public bool NextPage { get; set; }
    public string? LastScope { get; private set; }
    public WorkQuery? LastQuery { get; private set; }
    public int DataCalls { get; private set; }
    public bool DataUnavailable { get; set; }
    public Task<WorkPage> ListAsync(WorkQuery query, CancellationToken cancellationToken = default)
    {
        Calls++; LastScope = query.Scope.Id; LastQuery = query;
        if (Unavailable) throw new InvalidOperationException("Password=do-not-render-this");
        var work = Work();
        return Task.FromResult(new WorkPage([work], NextPage ? new(work.CreatedAt, work.Id) : null,
            query.Before is not null ? new(work.CreatedAt, work.Id) : null));
    }
    public Task<WorkDetail?> FindAsync(WorkScope scope, Guid id, CancellationToken cancellationToken = default)
    {
        Calls++; LastScope = scope.Id;
        if (Unavailable) throw new InvalidOperationException("Password=do-not-render-this");
        return Task.FromResult<WorkDetail?>(scope.Tenant == "a" && (id == WorkId || AdditionalWorkIds.Contains(id)) ? new(Work() with { Id = id }, [], false, [], false) : null);
    }
    public HashSet<Guid> AdditionalWorkIds { get; } = [];
    public WorkState State { get; set; } = WorkState.Completed;
    public DateTimeOffset? ExpiresAt { get; set; }
    private WorkSummary Work() => new(WorkId, State, 2, 1, 0,
        DateTimeOffset.Parse("2026-01-01T00:00:00Z"), DateTimeOffset.Parse("2026-01-01T00:00:00Z"), ExpiresAt,
        DateTimeOffset.Parse("2026-01-01T00:00:01Z"), null);

    public Task<WorkData?> ReadDataAsync(WorkScope scope, Guid id, WorkDataSelection selection, CancellationToken ct = default)
    {
        DataCalls++;
        if (DataUnavailable) throw new InvalidOperationException("Password=do-not-render-this");
        return Task.FromResult<WorkData?>(scope.Tenant == "a" && id == WorkId ? WorkDataProjection.FromJson(
            "{\"document\":{\"title\":\"<script>alert(1)</script>\",\"revision\":18},\"secret\":\"never-display\"}"u8.ToArray(), selection) : null);
    }
}
