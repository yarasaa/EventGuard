using EventGuard.PostgreSql;
using Npgsql;
using RabbitMQ.Client;

namespace EventGuard.RabbitMq.Tests;

public sealed class BrokerFixture : IAsyncLifetime
{
    public string Schema { get; } = "eg_rmq_test_" + Guid.NewGuid().ToString("N");
    public string DbConnectionString { get; } = Environment.GetEnvironmentVariable("EVENTGUARD_TEST_CONNECTION")
        ?? "Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only;Maximum Pool Size=8;Timeout=5;Command Timeout=10";
    public NpgsqlDataSource DataSource { get; private set; } = null!;
    public PostgreSqlWorkStore Store { get; private set; } = null!;
    public PostgreSqlOutboxStore Outbox { get; private set; } = null!;
    public IConnection Native { get; private set; } = null!;
    public IChannel Admin { get; private set; } = null!;
    public RabbitMqConnection Connection { get; private set; } = null!;
    private readonly List<string> _queues = [];
    private readonly List<string> _exchanges = [];
    public static ConnectionFactory Factory(int port = 5673) => new()
    {
        HostName = "127.0.0.1",
        Port = port,
        VirtualHost = "eventguard_test",
        UserName = "eventguard",
        Password = "eventguard-local-only",
        AutomaticRecoveryEnabled = false,
        TopologyRecoveryEnabled = false,
        RequestedConnectionTimeout = TimeSpan.FromSeconds(5),
        ContinuationTimeout = TimeSpan.FromSeconds(5)
    };
    public async Task InitializeAsync()
    {
        var db = new NpgsqlConnectionStringBuilder(DbConnectionString);
        if (db.Host is not ("localhost" or "127.0.0.1") || !db.Database!.StartsWith("eventguard_test", StringComparison.Ordinal))
            throw new InvalidOperationException("Tests require a local eventguard_test database.");
        db.MaxPoolSize = 8;
        DataSource = NpgsqlDataSource.Create(db.ConnectionString);
        Store = new(DataSource, Schema);
        await Store.InitializeAsync();
        Outbox = new(DataSource, Schema);
        await using var command = DataSource.CreateCommand($"CREATE TABLE {Schema}.effects(work_id uuid NOT NULL)");
        await command.ExecuteNonQueryAsync();
        Native = await Factory().CreateConnectionAsync();
        Admin = await Native.CreateChannelAsync(new CreateChannelOptions(true, true));
        Connection = await RabbitMqConnection.OpenAsync(Factory());
    }
    public async Task<string> QueueAsync(IDictionary<string, object?>? arguments = null)
    {
        var name = "eg_rmq_test_" + Guid.NewGuid().ToString("N");
        await Admin.QueueDeclareAsync(name, durable: true, exclusive: false, autoDelete: false, arguments);
        _queues.Add(name);
        return name;
    }
    public async Task<string> ExchangeAsync(string type = "direct")
    {
        var name = "eg_rmq_test_" + Guid.NewGuid().ToString("N");
        await Admin.ExchangeDeclareAsync(name, type, durable: true);
        _exchanges.Add(name);
        return name;
    }
    public async Task DeleteQueueAsync(string queue)
    {
        await Admin.QueueDeleteAsync(queue);
        _queues.Remove(queue);
    }
    public static WorkScope Scope() => new("tenant", Guid.NewGuid().ToString("N"), "rabbitmq/local/v1", "files.updated");
    public static IdentityPolicy Policy { get; } = new("message-id/v1", "body/v1");
    public static OutgoingMessage Message(string queue, Guid? id = null, byte[]? body = null, PublishRoute? route = null,
        IReadOnlyDictionary<string, string>? headers = null, string contentType = "application/json") =>
        new(new("tenant", Guid.NewGuid().ToString("N")), Guid.NewGuid(), id ?? Guid.NewGuid(),
            route ?? new("rabbitmq", "local", "v1", RabbitMqPublisher.DefaultExchange, queue), body ?? [1, 2, 3], contentType, headers);
    public RabbitMqPublisher Publisher(RabbitMqConnection? connection = null, int channels = 4) => new(connection ?? Connection, "local", "v1", channels);
    public async Task EnqueueAsync(OutgoingMessage message) => await Outbox.ProduceAsync(
        new(message.Scope, message.IntentId, ContentFingerprint.FromBytes(message.CopyPayload())),
        async (session, ct) => { await session.Outbox.EnqueueAsync(message, ct); });
    public async Task RawAsync(string queue, string? messageId, byte[] body) => await Admin.BasicPublishAsync("", queue, true,
        new BasicProperties { MessageId = messageId, Persistent = true }, body);
    public async Task<long> CountAsync(string table)
    {
        if (table is not ("works" or "effects" or "conflicts")) throw new ArgumentException("Unknown test table.");
        await using var command = DataSource.CreateCommand($"SELECT count(*) FROM {Schema}.{table}");
        return (long)(await command.ExecuteScalarAsync())!;
    }
    public async ValueTask Effect(WorkItem work, ReadOnlyMemory<byte> _, PostgreSqlSession session, CancellationToken ct)
    {
        await using var command = session.CreateCommand($"INSERT INTO {Schema}.effects VALUES(@id)");
        command.Parameters.AddWithValue("id", work.Id);
        await command.ExecuteNonQueryAsync(ct);
    }
    public async Task<WorkItem?> FindByMessageAsync(WorkScope scope, Guid messageId)
    {
        await using var command = DataSource.CreateCommand($"SELECT id FROM {Schema}.works WHERE scope_id=@scope AND message_id=@message");
        command.Parameters.AddWithValue("scope", scope.Id);
        command.Parameters.AddWithValue("message", messageId);
        var id = await command.ExecuteScalarAsync();
        return id is Guid workId ? await Store.FindAsync(scope, workId) : null;
    }
    // Sequential callbacks reach this invalid sentinel only after previous individual acks were sent.
    public async Task DrainAsync(string queue, WorkScope scope, IWorkStore? store = null)
    {
        await RawAsync(queue, null, [0]);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await Assert.ThrowsAsync<IngressMessageRejectedException>(() => new RabbitMqIngress(Connection, store ?? Store, scope, Policy, queue).RunAsync(timeout.Token));
        var sentinel = await Admin.BasicGetAsync(queue, autoAck: true);
        Assert.NotNull(sentinel);
        Assert.Null(sentinel.BasicProperties.MessageId);
        Assert.Null(await Admin.BasicGetAsync(queue, autoAck: true));
    }
    public async Task DisposeAsync()
    {
        if (Admin is not null)
        {
            foreach (var queue in _queues) await Admin.QueueDeleteAsync(queue);
            foreach (var exchange in _exchanges) await Admin.ExchangeDeleteAsync(exchange);
            await Admin.DisposeAsync();
        }
        if (Native is not null) await Native.DisposeAsync();
        if (Connection is not null) await Connection.DisposeAsync();
        if (DataSource is not null)
        {
            await using var command = DataSource.CreateCommand($"DROP SCHEMA IF EXISTS {Schema} CASCADE");
            await command.ExecuteNonQueryAsync();
            await DataSource.DisposeAsync();
        }
    }
}
