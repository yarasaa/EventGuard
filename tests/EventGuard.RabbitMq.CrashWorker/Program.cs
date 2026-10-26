using System.Text.Json;
using EventGuard;
using EventGuard.PostgreSql;
using EventGuard.RabbitMq;
using Npgsql;
using RabbitMQ.Client;

string Required(string name) => Environment.GetEnvironmentVariable(name) ?? throw new InvalidOperationException(name);
var dbOptions = new NpgsqlConnectionStringBuilder(Required("EVENTGUARD_TEST_CONNECTION"));
var schema = Required("EVENTGUARD_TEST_SCHEMA");
var queue = Required("EVENTGUARD_TEST_QUEUE");
if (dbOptions.Host is not ("localhost" or "127.0.0.1") || !dbOptions.Database!.StartsWith("eventguard_test", StringComparison.Ordinal)
    || !schema.StartsWith("eg_rmq_test_", StringComparison.Ordinal) || !queue.StartsWith("eg_rmq_test_", StringComparison.Ordinal))
    throw new InvalidOperationException("Crash worker only runs against local test resources.");
await using var data = NpgsqlDataSource.Create(dbOptions.ConnectionString);
await using var broker = await RabbitMqConnection.OpenAsync(new ConnectionFactory
{
    HostName = "127.0.0.1",
    Port = 5673,
    VirtualHost = "eventguard_test",
    UserName = "eventguard",
    Password = "eventguard-local-only",
    AutomaticRecoveryEnabled = false,
    TopologyRecoveryEnabled = false
});
var scope = new WorkScope("tenant", Required("EVENTGUARD_TEST_CONSUMER"), "rabbitmq/local/v1", "files.updated");
await new RabbitMqIngress(broker, new PauseAfterAcceptance(new PostgreSqlWorkStore(data, schema)), scope,
    new("message-id/v1", "body/v1"), queue, prefetch: 1).RunAsync(CancellationToken.None);

sealed class PauseAfterAcceptance(IWorkStore inner) : IWorkStore
{
    public async Task<AcceptanceResult> AcceptAsync(WorkSubmission submission, CancellationToken cancellationToken = default)
    {
        var accepted = await inner.AcceptAsync(submission, cancellationToken);
        Console.WriteLine(JsonSerializer.Serialize(new { stage = "durable-before-ack", workId = accepted.Work.Id }));
        await Console.Out.FlushAsync(cancellationToken);
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        return accepted;
    }
    public Task<WorkItem?> FindAsync(WorkScope scope, Guid workId, CancellationToken cancellationToken = default) => inner.FindAsync(scope, workId, cancellationToken);
    public Task<WorkClaim?> ClaimNextAsync(WorkScope scope, Guid requestId, TimeSpan duration, CancellationToken cancellationToken = default) => inner.ClaimNextAsync(scope, requestId, duration, cancellationToken);
    public Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken cancellationToken = default) => inner.ReplayAsync(request, cancellationToken);
}
