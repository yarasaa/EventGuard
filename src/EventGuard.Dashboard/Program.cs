using EventGuard;
using System.Net;
using EventGuard.Dashboard;
using EventGuard.Operations;
using EventGuard.Operations.PostgreSql;
using EventGuard.Operations.SqlServer;
using EventGuard.Operations.Replay.PostgreSql;
using EventGuard.Operations.Replay.SqlServer;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Http.Features;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.ConfigureKestrel(server => server.Limits.MaxRequestBodySize = 16_384);
builder.Services.Configure<FormOptions>(options => { options.ValueLengthLimit = 12_288; options.ValueCountLimit = 8; options.KeyLengthLimit = 64; });
builder.Services.AddAntiforgery(options => options.HeaderName = "X-CSRF-TOKEN");
var settings = builder.Configuration.GetSection("Dashboard").Get<DashboardOptions>() ?? new();
settings.Validate(builder.Environment.IsDevelopment(), builder.Configuration.GetSection("Kestrel:Endpoints").GetChildren().Any());
if (settings.DevelopmentAccess)
    builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, settings.DevelopmentPort));

builder.Services.AddSingleton(settings);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<DashboardHealth>();
builder.Services.AddSingleton<DashboardMetrics>();
builder.Services.AddSingleton<IWorkerMonitor>(_ => settings.Workers.Endpoint.Length == 0 ? new DisabledWorkerMonitor() :
    new EventGuard.Operations.Prometheus.PrometheusWorkerMonitor(settings.Workers, settings.StoreId, settings.DevelopmentAccess));
builder.Services.AddSingleton<DashboardWorkers>();
builder.Services.AddSingleton<DashboardWorkerHistory>();
builder.Services.AddSingleton<DashboardBacklog>();
builder.Services.AddSingleton<DashboardModules>();
var keys = builder.Services.AddDataProtection().SetApplicationName("EventGuard.Dashboard." + settings.StoreId);
if (settings.DevelopmentAccess) keys.UseEphemeralDataProtectionProvider();
else DashboardKeyRing.Configure(keys, settings);
builder.Services.AddRazorPages(options => options.Conventions.AddPageRoute("/Outbox", "/quarantine"));
builder.Services.AddSingleton<DashboardQueries>();
builder.Services.AddSingleton<DashboardOutboxQueries>();
builder.Services.AddSingleton<IWorkReadStore>(_ => settings.Provider switch
{
    "PostgreSql" => new PostgreSqlWorkReader(settings.ConnectionString, settings.Schema),
    _ => new SqlServerWorkReader(settings.ConnectionString, settings.Schema)
});
builder.Services.AddSingleton<OperationsReader>();
builder.Services.AddSingleton<IWorkReplayStore>(_ => !(settings.ReplayEnabled || settings.OutboxReplayEnabled || settings.RecoveryEnabled) ? new DisabledReplayStore() : settings.Provider switch
{
    "PostgreSql" => new PostgreSqlReplayStore(settings.ConnectionString, settings.ReplayConnectionString, settings.Schema),
    _ => new SqlServerReplayStore(settings.ConnectionString, settings.ReplayConnectionString, settings.Schema)
});
builder.Services.AddSingleton<OperationsReplayer>();
builder.Services.AddSingleton<DashboardReplays>();
builder.Services.AddSingleton<DashboardIngressQueries>();
builder.Services.AddSingleton<DashboardRecovery>();
builder.Services.AddSingleton<DashboardBulkReplays>();
builder.Services.AddSingleton<DashboardAudit>();
builder.Services.AddSingleton<DashboardOutboxReplays>();
builder.Services.AddAuthorization(options => options.FallbackPolicy = new AuthorizationPolicyBuilder().RequireAuthenticatedUser().Build());
if (settings.DevelopmentAccess)
    builder.Services.AddAuthentication("local").AddScheme<AuthenticationSchemeOptions, LocalDevelopmentAuthentication>("local", _ => { });
else
    builder.Services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
        .AddCookie(options =>
        {
            options.LoginPath = "/sign-in";
            options.Cookie.Name = "__Host-EventGuard";
            options.Cookie.SecurePolicy = CookieSecurePolicy.Always;
            options.Cookie.HttpOnly = true;
            options.Cookie.SameSite = SameSiteMode.Lax;
            options.ExpireTimeSpan = TimeSpan.FromMinutes(30);
            options.SlidingExpiration = false;
            options.Events.OnRedirectToLogin = context =>
            {
                if (context.Request.Path.StartsWithSegments("/api")) context.Response.StatusCode = 401;
                else context.Response.Redirect("/sign-in");
                return Task.CompletedTask;
            };
            options.Events.OnRedirectToAccessDenied = context => { context.Response.StatusCode = 403; return Task.CompletedTask; };
        })
        .AddOpenIdConnect(options =>
        {
            options.Authority = settings.Authority;
            options.ClientId = settings.ClientId;
            options.ClientSecret = settings.ClientSecret;
            options.ResponseType = "code";
            options.UsePkce = true;
            options.RequireHttpsMetadata = true;
            options.MapInboundClaims = false;
            options.SaveTokens = false;
            options.Scope.Clear();
            options.Scope.Add("openid");
            options.Scope.Add("profile");
            options.TokenValidationParameters.NameClaimType = "name";
            options.Events.OnRemoteFailure = context =>
            {
                context.HandleResponse();
                context.Response.Redirect("/auth-error");
                return Task.CompletedTask;
            };
        });

var app = builder.Build();
if (settings.KeyRingPath.Length > 0) DashboardKeyRing.Activate(app.Services);
// This check is read-only. Index installation and worker migrations never run in the web process.
using (var budget = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
{
    var source = app.Services.GetRequiredService<IWorkReadStore>();
    if (source is PostgreSqlWorkReader pg)
    {
        if (settings.Scopes.Length > 0) await pg.ValidateAsync(budget.Token);
        if (settings.OutboxScopes.Length > 0) await pg.ValidateOutboxAsync(budget.Token);
    }
    if (source is SqlServerWorkReader sql)
    {
        if (settings.Scopes.Length > 0) await sql.ValidateAsync(budget.Token);
        if (settings.OutboxScopes.Length > 0) await sql.ValidateOutboxAsync(budget.Token);
    }
    if (settings.IngressMonitoringEnabled) await ((IRecoveryHealthCheck)source).ValidateRecoveryAsync(budget.Token);
    if (settings.AuditEnabled) await ((IAuditReadStore)source).ValidateAuditAsync(settings.Scopes.Any(s => s.AllowAudit), settings.OutboxScopes.Any(s => s.AllowAudit), budget.Token);
    var replay = app.Services.GetRequiredService<IWorkReplayStore>();
    if (replay is PostgreSqlReplayStore pgReplay) await pgReplay.ValidateAsync(budget.Token);
    if (replay is SqlServerReplayStore sqlReplay) await sqlReplay.ValidateAsync(budget.Token);
    if (settings.RecoveryEnabled) await ((IRecoveryHealthCheck)replay).ValidateRecoveryAsync(budget.Token);
}

app.Use(async (context, next) =>
{
    if (!settings.MonitoringEnabled || context.Request.Path.StartsWithSegments("/health") || context.Request.Path == "/metrics") { await next(); return; }
    var start = System.Diagnostics.Stopwatch.GetTimestamp();
    var failureStatus = 0;
    try { await next(); }
    catch { failureStatus = 500; throw; }
    finally { context.RequestServices.GetRequiredService<DashboardMetrics>().Record(failureStatus == 0 ? context.Response.StatusCode : failureStatus, start); }
});
app.Use(async (context, next) =>
{
    context.Response.Headers.CacheControl = "no-store";
    context.Response.Headers.ContentSecurityPolicy = "default-src 'self'; style-src 'self'; script-src 'none'; img-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'";
    context.Response.Headers.XContentTypeOptions = "nosniff";
    context.Response.Headers["Referrer-Policy"] = "no-referrer";
    if (settings.DevelopmentAccess)
    {
        if (context.Connection.RemoteIpAddress is not { } remote || !IPAddress.IsLoopback(remote)
            || context.Request.Host.Host is not ("localhost" or "127.0.0.1")
            || context.Request.Headers["Sec-Fetch-Site"] == "cross-site")
        { context.Response.StatusCode = 403; return; }
    }
    else if (!context.Request.IsHttps || (!settings.AllowedHosts.Contains(context.Request.Host.Host, StringComparer.OrdinalIgnoreCase)
        && !(settings.MonitoringEnabled && settings.MetricsToken.Length > 0 && context.Request.Path == "/metrics"
            && context.Request.Headers.Authorization.Count == 1 && context.RequestServices.GetRequiredService<DashboardMetrics>().Authorized(context.Request.Headers.Authorization[0]))))
    { context.Response.StatusCode = 400; return; }
    try { await next(); }
    catch (ScopeNotAvailableException) { context.Response.StatusCode = 404; await context.Response.WriteAsync("Scope or record not found."); }
    catch (InvalidQueryException) { context.Response.StatusCode = 400; await context.Response.WriteAsync("Invalid filter or expired page. Reopen the relevant list."); }
    catch (InvalidReplayException error) { context.Response.StatusCode = 400; await context.Response.WriteAsync(error.Message); }
    catch (OperationsUnavailableException)
    {
        context.Response.StatusCode = 503;
        context.Response.Headers.RetryAfter = "5";
        await context.Response.WriteAsync("Data is temporarily unavailable. This does not mean there are no records. Please try again.");
    }
});
app.UseStaticFiles();
app.UseRouting();
app.UseAuthentication();
app.UseAuthorization();
app.MapGet("/sign-in", () => settings.DevelopmentAccess ? Results.LocalRedirect("/") :
    Results.Challenge(new AuthenticationProperties { RedirectUri = "/" }, [OpenIdConnectDefaults.AuthenticationScheme])).AllowAnonymous();
app.MapGet("/auth-error", () => Results.Problem("Sign-in failed. Check the identity provider configuration.", statusCode: 401)).AllowAnonymous();
app.MapGet("/api/modules", async (HttpContext context, DashboardModules modules, string? target, int? state, string? id, string? cursor) =>
    Results.Ok(await modules.ReadAsync(context.User, target, state, id, cursor, context.RequestAborted)));
app.MapGet("/api/scopes", (HttpContext context, DashboardQueries queries) => queries.AllowedScopes(context.User)
    .Select(s => new { s.Key, s.Label }));
app.MapGet("/api/works", async (HttpContext context, DashboardQueries queries, string? scope, string? state, string? cursor, string? from, string? until, string? workId, string? minimumAttempts, string? replayed) =>
{
    var result = await queries.ListAsync(context.User, scope, state, cursor, context.RequestAborted, FilterInput.Parse(from, until, workId, minimumAttempts, replayed));
    return Results.Ok(new { result.Snapshot, result.NextCursor, result.PreviousCursor, result.CurrentCursor });
});
app.MapGet("/api/works/{scope}/{id:guid}", async (HttpContext context, DashboardQueries queries, string scope, Guid id) =>
{
    var result = await queries.FindAsync(context.User, scope, id, context.RequestAborted);
    return result.Value is null ? Results.NotFound() : Results.Ok(result);
});
app.MapGet("/api/works/{scope}/{id:guid}/data", async (HttpContext context, DashboardQueries queries, string scope, Guid id) =>
{
    var result = await queries.ReadDataAsync(context.User, scope, id, context.RequestAborted);
    return result.Value is null ? Results.NotFound() : Results.Ok(result);
});
app.MapGet("/api/outbox-scopes", (HttpContext context, DashboardOutboxQueries queries) => queries.AllowedScopes(context.User)
    .Select(s => new { s.Key, s.Label }));
foreach (var path in new[] { "/api/outbox", "/api/quarantine" })
{
    var quarantine = path == "/api/quarantine";
    app.MapGet(path, async (HttpContext context, DashboardOutboxQueries queries, string? scope, string? state, string? cursor,
        string? from, string? until, string? intentId, string? minimumAttempts, string? replayed, string? uncertain) =>
    {
        var result = await queries.ListAsync(context.User, scope, state, cursor, context.RequestAborted,
            OutboxFilterInput.Parse(from, until, intentId, minimumAttempts, replayed, uncertain), quarantine);
        return Results.Ok(new { result.Snapshot, result.NextCursor, result.PreviousCursor, result.CurrentCursor });
    });
}
app.MapGet("/api/outbox/{scope}/{id:guid}", async (HttpContext context, DashboardOutboxQueries queries, string scope, Guid id) =>
{
    var result = await queries.FindAsync(context.User, scope, id, context.RequestAborted);
    return result.Value is null ? Results.NotFound() : Results.Ok(result);
});
if (settings.ReplayEnabled)
{
    app.MapGet("/api/works/{scope}/{id:guid}/replay/{requestId:guid}", async (HttpContext context, DashboardReplays replays, string scope, Guid id, Guid requestId) =>
    {
        var receipt = await replays.FindReceiptAsync(context.User, scope, id, requestId, context.RequestAborted);
        return receipt is null ? Results.NotFound() : Results.Ok(receipt);
    });
    app.MapPost("/api/works/{scope}/{id:guid}/replay", async (HttpContext context, DashboardReplays replays, IAntiforgery antiforgery, string scope, Guid id) =>
    {
        replays.Authorize(context.User, scope);
        if (context.Request.ContentLength > 16_384) return Results.StatusCode(413);
        if (!string.Equals(context.Request.ContentType?.Split(';', 2)[0].Trim(), "application/x-www-form-urlencoded", StringComparison.OrdinalIgnoreCase))
            return Results.BadRequest();
        try
        {
            await antiforgery.ValidateRequestAsync(context);
            var form = await context.Request.ReadFormAsync(context.RequestAborted);
            if (form["ticket"].Count != 1) return Results.BadRequest();
            var review = replays.ReadCommand(context.User, scope, id, form["ticket"]!);
            var result = await replays.SubmitAsync(context.User, scope, id, review.Ticket, context.RequestAborted);
            return Results.Json(new { review.RequestId, Result = result }, statusCode: result.Status == ReplayStatus.Accepted ? 200 : result.Status == ReplayStatus.NotFound ? 404 : 409);
        }
        catch (AntiforgeryValidationException) { return Results.BadRequest(); }
        catch (InvalidDataException) { return Results.BadRequest(); }
        catch (ReplayUnavailableException error)
        {
            context.Response.Headers.RetryAfter = "5";
            return Results.Json(new { error.RequestId, error.OutcomeUnconfirmed,
                Message = "Check the saved receipt or retry the same request." }, statusCode: 503);
        }
    });
}
if (settings.OutboxReplayEnabled)
{
    app.MapGet("/api/outbox/{scope}/{id:guid}/replay/{requestId:guid}", async (HttpContext context, DashboardOutboxReplays replays, string scope, Guid id, Guid requestId) =>
    {
        var receipt = await replays.FindReceiptAsync(context.User, scope, id, requestId, context.RequestAborted);
        return receipt is null ? Results.NotFound() : Results.Ok(receipt);
    });
    app.MapPost("/api/outbox/{scope}/{id:guid}/replay", async (HttpContext context, DashboardOutboxReplays replays, IAntiforgery antiforgery, string scope, Guid id) =>
    {
        replays.Authorize(context.User, scope);
        if (context.Request.ContentLength > 16_384) return Results.StatusCode(413);
        if (!string.Equals(context.Request.ContentType?.Split(';', 2)[0].Trim(), "application/x-www-form-urlencoded", StringComparison.OrdinalIgnoreCase))
            return Results.BadRequest();
        try
        {
            await antiforgery.ValidateRequestAsync(context);
            var form = await context.Request.ReadFormAsync(context.RequestAborted);
            if (form["ticket"].Count != 1) return Results.BadRequest();
            var review = replays.ReadCommand(context.User, scope, id, form["ticket"]!);
            var result = await replays.SubmitAsync(context.User, scope, id, review.Ticket, context.RequestAborted);
            return Results.Json(new { review.RequestId, Result = result }, statusCode: result.Status == OutboxReplayStatus.Accepted ? 200 : result.Status == OutboxReplayStatus.NotFound ? 404 : 409);
        }
        catch (AntiforgeryValidationException) { return Results.BadRequest(); }
        catch (InvalidDataException) { return Results.BadRequest(); }
        catch (ReplayUnavailableException error)
        {
            context.Response.Headers.RetryAfter = "5";
            return Results.Json(new { error.RequestId, error.OutcomeUnconfirmed,
                Message = "Check the saved receipt or retry the same request." }, statusCode: 503);
        }
    });
}
app.MapGet("/api/backlog", async (HttpContext context, DashboardBacklog backlog, string? target) =>
{
    var view = await backlog.ReadAsync(context.User, target, context.RequestAborted);
    return Results.Json(new { view.Target, view.Snapshot, view.Available }, statusCode: view.Available ? 200 : 503);
});
app.MapMonitoring(settings);
app.MapRazorPages();
await app.RunAsync();

public partial class Program;
