using System.Collections.Concurrent;
using System.Data.Common;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using EventGuard.PostgreSql;
using EventGuard.SqlServer;

[assembly: Xunit.CollectionBehavior(MaxParallelThreads = 2)]

namespace EventGuard.Kafka.Tests;

public sealed class BrokerFixture : IAsyncLifetime
{
    public const string Address = "127.0.0.1:19092";
    private readonly ConcurrentBag<string> _topics = [];
    private readonly ConcurrentBag<string> _groups = [];
    public IAdminClient Admin { get; private set; } = null!;
    public IProducer<byte[], byte[]> Producer { get; private set; } = null!;
    public static ProducerConfig ProducerConfig(string address = Address) => new()
    {
        BootstrapServers = address,
        EnableIdempotence = true,
        Acks = Acks.All,
        MessageTimeoutMs = 3000,
        SocketTimeoutMs = 2000,
        AllowAutoCreateTopics = false
    };
    public static ConsumerConfig ConsumerConfig(string group, string address = Address) => new()
    {
        BootstrapServers = address,
        GroupId = group,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false,
        EnableAutoOffsetStore = false,
        GroupProtocol = GroupProtocol.Classic,
        SessionTimeoutMs = 6000,
        HeartbeatIntervalMs = 1000,
        MaxPollIntervalMs = 10000,
        SocketTimeoutMs = 1000,
        AllowAutoCreateTopics = false
    };
    public Task InitializeAsync()
    {
        Admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Address, SocketTimeoutMs = 3000 }).Build();
        Assert.NotEmpty(Admin.GetMetadata(TimeSpan.FromSeconds(10)).Brokers);
        Producer = new ProducerBuilder<byte[], byte[]>(ProducerConfig()).SetLogHandler((_, _) => { }).Build();
        return Task.CompletedTask;
    }
    public async Task<string> TopicAsync(int partitions = 1, Dictionary<string, string>? config = null)
    {
        var name = "eg_kafka_test_" + Guid.NewGuid().ToString("N");
        await Admin.CreateTopicsAsync([new TopicSpecification { Name = name, NumPartitions = partitions, ReplicationFactor = 1, Configs = config }]);
        _topics.Add(name);
        return name;
    }
    public string Group()
    {
        var name = "eg_kafka_test_" + Guid.NewGuid().ToString("N");
        _groups.Add(name);
        return name;
    }
    public Task<DeliveryResult<byte[], byte[]>> RawAsync(string topic, Guid? id = null, byte[]? body = null, int partition = 0, Headers? headers = null)
    {
        headers ??= [];
        if (id.HasValue) headers.Add(KafkaPublisher.MessageIdHeader, Encoding.UTF8.GetBytes(id.Value.ToString("D")));
        return Producer.ProduceAsync(new TopicPartition(topic, partition), new Message<byte[], byte[]> { Value = body ?? [1, 2, 3], Headers = headers });
    }
    public static IConsumer<byte[], byte[]> Reader(string group, string address = Address) =>
        new ConsumerBuilder<byte[], byte[]>(ConsumerConfig(group, address)).SetLogHandler((_, _) => { }).Build();
    public static long Committed(string group, string topic, int partition = 0)
    {
        using var consumer = Reader(group);
        return consumer.Committed([new TopicPartition(topic, partition)], TimeSpan.FromSeconds(5)).Single().Offset.Value;
    }
    public static async Task WaitCommitAsync(string group, string topic, long offset, int partition = 0)
    {
        var until = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < until)
        {
            if (Committed(group, topic, partition) == offset) return;
            await Task.Delay(50);
        }
        Assert.Equal(offset, Committed(group, topic, partition));
    }
    public static ConsumeResult<byte[], byte[]> Read(string topic, long offset = 0, int partition = 0)
    {
        using var reader = Reader("observer-" + Guid.NewGuid().ToString("N"));
        reader.Assign(new TopicPartitionOffset(topic, partition, offset));
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        return reader.Consume(stop.Token);
    }
    public static OutgoingMessage Message(string topic, ProducerScope? scope = null, Guid? id = null,
        IReadOnlyDictionary<string, string>? headers = null, string? key = null, byte[]? body = null) =>
        new(scope ?? new("tenant", Guid.NewGuid().ToString("N")), Guid.NewGuid(), id ?? Guid.NewGuid(),
            new("kafka", "local", "v1", topic, key), body ?? [1, 2, 3], headers: headers);
    public async Task DisposeAsync()
    {
        Producer?.Dispose();
        if (Admin is null) return;
        foreach (var group in _groups)
        {
            // Unsubscribe + native Dispose does not promise immediate coordinator removal.
            // Wait beyond this fixture's six-second session before treating cleanup as failed.
            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (true)
            {
                try { await Admin.DeleteGroupsAsync([group]); break; }
                catch (DeleteGroupsException e) when (e.Results.All(r => r.Error.Code == ErrorCode.GroupIdNotFound)) { break; }
                catch (DeleteGroupsException e) when (DateTime.UtcNow < deadline && e.Results.All(r => r.Error.Code == ErrorCode.NonEmptyGroup))
                { await Task.Delay(200); }
            }
        }
        if (!_topics.IsEmpty) await Admin.DeleteTopicsAsync(_topics);
        Admin.Dispose();
    }
}

internal sealed class StoreHarness(bool sqlServer) : IAsyncDisposable
{
    private readonly PostgreSql.Tests.DatabaseFixture? _pg = sqlServer ? null : new();
    private readonly SqlServer.Tests.DatabaseFixture? _sql = sqlServer ? new() : null;
    public WorkScope Scope { get; } = new("tenant", Guid.NewGuid().ToString("N"), "kafka/local/v1", "files.updated");
    public static IdentityPolicy Policy { get; } = new("message-id/v1", "body/v1");
    public IWorkStore Store => sqlServer ? _sql!.Store : _pg!.Store;
    public IOutboxStore Outbox => sqlServer ? _sql!.Outbox : _pg!.Outbox;
    public string Schema => sqlServer ? _sql!.Schema : _pg!.Schema;
    public string ConnectionString => sqlServer ? _sql!.ConnectionString : _pg!.ConnectionString;
    public Task InitializeAsync() => sqlServer ? _sql!.InitializeAsync() : _pg!.InitializeAsync();
    public Task EnqueueAsync(OutgoingMessage message) => sqlServer ? _sql!.EnqueueAsync(message) : _pg!.EnqueueAsync(message);
    public Task<ExecutionResult> ExecuteAsync(WorkClaim claim) => sqlServer ? _sql!.Store.ExecuteAsync(claim, _sql.Effect, new()) : _pg!.Store.ExecuteAsync(claim, _pg.Effect, new());
    public Task<long> EffectCount(Guid id) => sqlServer ? _sql!.EffectCount(id) : _pg!.EffectCount(id);
    public async Task<long> CountAsync(string table)
    {
        if (table is not ("works" or "business_effects" or "conflicts" or "ingress_quarantine" or "ingress_release_requests")) throw new ArgumentException(nameof(table));
        await using DbConnection connection = sqlServer ? await _sql!.DataSource.OpenConnectionAsync() : await _pg!.DataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {(sqlServer ? "COUNT_BIG(*)" : "count(*)")} FROM {Schema}.{table}";
        return (long)(await command.ExecuteScalarAsync())!;
    }
    public async Task<Guid> QuarantineIdAsync()
    {
        await using DbConnection connection = sqlServer ? await _sql!.DataSource.OpenConnectionAsync() : await _pg!.DataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT id FROM {Schema}.ingress_quarantine";
        return (Guid)(await command.ExecuteScalarAsync())!;
    }
    public async Task BreakAcceptanceAsync()
    {
        await using DbConnection connection = sqlServer ? await _sql!.DataSource.OpenConnectionAsync() : await _pg!.DataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sqlServer ? $"DROP TABLE {Schema}.conflicts; DROP TABLE {Schema}.attempts; DROP TABLE {Schema}.works"
            : $"DROP TABLE {Schema}.works CASCADE";
        // SQL Server's outbox FK also refers to works; use a denied read instead of destructive DDL there.
        if (sqlServer) command.CommandText = $"EXEC sp_rename '{Schema}.works', 'unavailable_works'";
        await command.ExecuteNonQueryAsync();
    }
    public async ValueTask DisposeAsync()
    {
        if (sqlServer) await _sql!.DisposeAsync(); else await _pg!.DisposeAsync();
    }
}

internal sealed class ObservedStore(IWorkStore inner, Func<WorkSubmission, CancellationToken, Task>? before = null,
    Func<AcceptanceResult, CancellationToken, Task>? after = null) : IWorkStore
{
    public ConcurrentQueue<AcceptanceResult> Results { get; } = [];
    public async Task<AcceptanceResult> AcceptAsync(WorkSubmission submission, CancellationToken cancellationToken = default)
    {
        if (before is not null) await before(submission, cancellationToken);
        var result = await inner.AcceptAsync(submission, cancellationToken);
        Results.Enqueue(result);
        if (after is not null) await after(result, cancellationToken);
        return result;
    }
    public Task<WorkItem?> FindAsync(WorkScope scope, Guid workId, CancellationToken cancellationToken = default) => inner.FindAsync(scope, workId, cancellationToken);
    public Task<WorkClaim?> ClaimNextAsync(WorkScope scope, Guid requestId, TimeSpan leaseDuration, CancellationToken cancellationToken = default) => inner.ClaimNextAsync(scope, requestId, leaseDuration, cancellationToken);
    public Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken cancellationToken = default) => inner.ReplayAsync(request, cancellationToken);
}
