using Npgsql;
using EventGuard.Store.Conformance;

namespace EventGuard.PostgreSql.Tests;

public sealed class DatabaseFixture : IAsyncLifetime, IWorkStoreFixture<PostgreSqlSession>, IOutboxStoreFixture, IJobStoreFixture<PostgreSqlSession>
{
    public string ConnectionString { get; } = Environment.GetEnvironmentVariable("EVENTGUARD_TEST_CONNECTION")
        ?? "Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only;Maximum Pool Size=40;Timeout=5;Command Timeout=10";
    public string Schema { get; } = "eg_test_" + Guid.NewGuid().ToString("N");
    public NpgsqlDataSource DataSource { get; private set; } = null!;
    public PostgreSqlWorkStore Store { get; private set; } = null!;
    public PostgreSqlOutboxStore Outbox { get; private set; } = null!;
    public IOutboxRecoveryStore Recovery { get; private set; } = null!;
    IOutboxStore IOutboxStoreFixture.Outbox => Outbox;
    public IOutboxRecoveryStore RestartRecovery() => new PostgreSqlOutboxRecoveryStore(DataSource, Schema);
    public async Task EnqueueAsync(OutgoingMessage message) => await Outbox.ProduceAsync(
        new(message.Scope, message.IntentId, ContentFingerprint.FromBytes(message.CopyPayload())),
        async (session, ct) => { await session.Outbox.EnqueueAsync(message, ct); });
    ITransactionalWorkStore<PostgreSqlSession> IWorkStoreFixture<PostgreSqlSession>.Store => Store;
    public ITransactionalWorkStore<PostgreSqlSession> RestartStore() => new PostgreSqlWorkStore(DataSource, Schema);

    public ITransactionalJobStore<PostgreSqlSession> Jobs => Store;
    public Task InitializeJobsAsync() => Store.InitializeJobsAsync();

    public async Task InitializeAsync()
    {
        var options = new NpgsqlConnectionStringBuilder(ConnectionString);
        if (options.Host is not ("127.0.0.1" or "localhost") || !options.Database!.StartsWith("eventguard_test", StringComparison.Ordinal))
            throw new InvalidOperationException("Integration tests require an explicit local eventguard_test database.");
        // Test classes and both TFMs run together against the small local database.
        options.MaxPoolSize = Math.Min(options.MaxPoolSize, 8);
        DataSource = NpgsqlDataSource.Create(options.ConnectionString);
        Store = new(DataSource, Schema);
        await Store.InitializeAsync();
        Outbox = new(DataSource, Schema);
        Recovery = RestartRecovery();
        await using var command = DataSource.CreateCommand($"CREATE TABLE {Schema}.business_effects(work_id uuid NOT NULL, note text NOT NULL)");
        await command.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        if (DataSource is null) return;
        await using (var command = DataSource.CreateCommand($"DROP SCHEMA IF EXISTS {Schema} CASCADE"))
            await command.ExecuteNonQueryAsync();
        await DataSource.DisposeAsync();
    }

    public async ValueTask Effect(WorkItem work, ReadOnlyMemory<byte> payload, PostgreSqlSession session, CancellationToken cancellationToken)
    {
        await using var command = session.CreateCommand($"INSERT INTO {Schema}.business_effects VALUES (@id,'processed')");
        command.Parameters.AddWithValue("id", work.Id);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<long> EffectCount(Guid workId)
    {
        await using var command = DataSource.CreateCommand($"SELECT count(*) FROM {Schema}.business_effects WHERE work_id=@id");
        command.Parameters.AddWithValue("id", workId);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    public static WorkScope Scope() => StoreContractData.Scope();
    public static WorkSubmission Submission(WorkScope scope, string key = "file-1", byte[]? payload = null,
        string version = "v1", DateTimeOffset? notBefore = null, DateTimeOffset? expiresAt = null)
        => StoreContractData.Submission(scope, key, payload, version, notBefore, expiresAt);

    public Task<WorkClaim?> Claim(WorkScope scope, TimeSpan? lease = null) =>
        Store.ClaimNextAsync(scope, Guid.NewGuid(), lease ?? TimeSpan.FromSeconds(10));
}
