using EventGuard;
using EventGuard.Runtime;
using EventGuard.PostgreSql;
using Npgsql;
using Prometheus;
using System.Net;
using System.Security.Cryptography;
using System.Text;

var builder = WebApplication.CreateBuilder(args);
var port = builder.Configuration.GetValue("port", 5190);
if (port is < 1024 or > 65535) throw new ArgumentException("Use an unprivileged local port.");
builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, port));
var connection = builder.Configuration["connection"] ?? "Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only;Maximum Pool Size=4";
var parsed = new NpgsqlConnectionStringBuilder(connection);
if (parsed.Host is not ("127.0.0.1" or "localhost") || parsed.Database != "eventguard_test")
    throw new InvalidOperationException("This sample only uses the local eventguard_test database.");
const string schema = "eventguard_monitored_sample";
await using var source = NpgsqlDataSource.Create(connection);
var store = new PostgreSqlWorkStore(source, schema);
await store.InitializeAsync();
await using (var ddl = source.CreateCommand($"CREATE TABLE IF NOT EXISTS {schema}.effects(work_id uuid PRIMARY KEY)")) await ddl.ExecuteNonQueryAsync();
var scope = new WorkScope("demo", "monitored-indexer", "local", "sample.created");
if (builder.Configuration.GetValue("seed", false))
{
    for (var i = 0; i < 20; i++)
    {
        var id = Guid.NewGuid(); byte[] payload = "{\"action\":\"record_sample_effect\"}"u8.ToArray();
        await store.AcceptAsync(new(new(scope, new("sample-id/v1", "sha256/v1"), id.ToString("N")), id, payload, ContentFingerprint.FromBytes(payload)));
    }
}
// Add a bounded batch to the isolated sample without starting another worker or listener.
if (builder.Configuration.GetValue("seed-only", false)) return;
using var telemetry = new RuntimeTelemetry();
var registry = Metrics.NewCustomRegistry();
using var bridge = MeterAdapter.StartListening(new MeterAdapterOptions
{
    Registry = registry,
    MetricFactory = Metrics.WithCustomRegistry(registry),
    InstrumentFilterPredicate = instrument => ReferenceEquals(instrument.Meter.Scope, telemetry)
        && instrument.Name.StartsWith("eventguard_worker_", StringComparison.Ordinal)
});
var expectedToken = SHA256.HashData(Encoding.UTF8.GetBytes("local-worker-metrics-token-not-for-production"));
var app = builder.Build();
app.MapGet("/metrics", async (HttpContext context) =>
{
    var header = context.Request.Headers.Authorization;
    if (header.Count != 1 || header[0] is not { } authorization || !authorization.StartsWith("Bearer ", StringComparison.Ordinal)
        || authorization.Length > 256 || !CryptographicOperations.FixedTimeEquals(expectedToken, SHA256.HashData(Encoding.UTF8.GetBytes(authorization[7..]))))
    { context.Response.StatusCode = 401; return; }
    context.Response.Headers.CacheControl = "no-store";
    context.Response.ContentType = "text/plain; version=0.0.4; charset=utf-8";
    await registry.CollectAndExportAsTextAsync(context.Response.Body, context.RequestAborted);
});
using var stop = CancellationTokenSource.CreateLinkedTokenSource(app.Lifetime.ApplicationStopping);
var stopAfter = builder.Configuration.GetValue("stop-loops-after", 0);
if (stopAfter > 0) stop.CancelAfter(TimeSpan.FromSeconds(stopAfter));
var pump = new WorkPump<PostgreSqlSession>(store, scope, async (work, _, session, ct) =>
{
    await Task.Delay(750, ct);
    await using var command = session.CreateCommand($"INSERT INTO {schema}.effects(work_id) VALUES(@id)");
    command.Parameters.AddWithValue("id", work.Id);
    await command.ExecuteNonQueryAsync(ct);
}, options: new(maximumConcurrency: 2), telemetry: telemetry);
var run = pump.RunAsync(stop.Token);
var host = app.RunAsync();
try
{
    var completed = await Task.WhenAny(run, host);
    if (completed == run && run.IsFaulted) { app.Lifetime.StopApplication(); await run; }
    await host;
}
finally { stop.Cancel(); await run; }
