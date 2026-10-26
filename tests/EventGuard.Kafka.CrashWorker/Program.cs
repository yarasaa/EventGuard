using System.Text.Json;
using Confluent.Kafka;
using EventGuard;
using EventGuard.Kafka;
using EventGuard.PostgreSql;
using EventGuard.SqlServer;
using Microsoft.Data.SqlClient;
using Npgsql;

string Required(string name) => Environment.GetEnvironmentVariable(name) ?? throw new InvalidOperationException(name);
var schema = Required("EVENTGUARD_TEST_SCHEMA");
var topic = Required("EVENTGUARD_TEST_TOPIC");
var group = Required("EVENTGUARD_TEST_GROUP");
if (!(schema.StartsWith("eg_test_", StringComparison.Ordinal) || schema.StartsWith("eg_sql_test_", StringComparison.Ordinal))
    || !topic.StartsWith("eg_kafka_test_", StringComparison.Ordinal) || !group.StartsWith("eg_kafka_test_", StringComparison.Ordinal))
    throw new InvalidOperationException("Crash worker only runs against local test resources.");
var connection = Required("EVENTGUARD_TEST_CONNECTION");
IWorkStore store;
NpgsqlDataSource? pg = null;
if (Required("EVENTGUARD_TEST_PROVIDER") == "sqlserver")
{
    var config = new SqlConnectionStringBuilder(connection);
    if (config.DataSource is not ("localhost,51433" or "127.0.0.1,51433") || config.InitialCatalog != "eventguard_test") throw new InvalidOperationException("Local SQL test DB required.");
    store = new SqlServerWorkStore(new(connection), schema);
}
else
{
    var config = new NpgsqlConnectionStringBuilder(connection);
    if (config.Host is not ("localhost" or "127.0.0.1") || !config.Database!.StartsWith("eventguard_test", StringComparison.Ordinal)) throw new InvalidOperationException("Local PostgreSQL test DB required.");
    pg = NpgsqlDataSource.Create(connection);
    store = new PostgreSqlWorkStore(pg, schema);
}
try
{
    var scope = new WorkScope("tenant", Required("EVENTGUARD_TEST_CONSUMER"), "kafka/local/v1", "files.updated");
    await new KafkaIngress(new ConsumerConfig
    {
        BootstrapServers = "127.0.0.1:19092",
        GroupId = group,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        SessionTimeoutMs = 6000,
        HeartbeatIntervalMs = 1000,
        SocketTimeoutMs = 1000
    }, new PauseAfterAcceptance(store), scope, new("message-id/v1", "body/v1"), [topic]).RunAsync(CancellationToken.None);
}
finally { if (pg is not null) await pg.DisposeAsync(); }

sealed class PauseAfterAcceptance(IWorkStore inner) : IWorkStore
{
    public async Task<AcceptanceResult> AcceptAsync(WorkSubmission submission, CancellationToken cancellationToken = default)
    {
        var result = await inner.AcceptAsync(submission, cancellationToken);
        Console.WriteLine(JsonSerializer.Serialize(new { stage = "durable-before-offset", workId = result.Work.Id }));
        await Console.Out.FlushAsync(cancellationToken);
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        return result;
    }
    public Task<WorkItem?> FindAsync(WorkScope scope, Guid workId, CancellationToken cancellationToken = default) => inner.FindAsync(scope, workId, cancellationToken);
    public Task<WorkClaim?> ClaimNextAsync(WorkScope scope, Guid requestId, TimeSpan leaseDuration, CancellationToken cancellationToken = default) => inner.ClaimNextAsync(scope, requestId, leaseDuration, cancellationToken);
    public Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken cancellationToken = default) => inner.ReplayAsync(request, cancellationToken);
}
