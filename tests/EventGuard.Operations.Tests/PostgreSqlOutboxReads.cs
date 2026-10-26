using EventGuard.Operations.PostgreSql;
using Npgsql;
using Fixture = EventGuard.PostgreSql.Tests.DatabaseFixture;

namespace EventGuard.Operations.Tests;

public sealed class PostgreSqlOutboxReads(Fixture db) : OutboxReadStoreContract(db), IClassFixture<Fixture>
{
    protected override async Task<IWorkReadStore> OpenAsync()
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var prepare = new NpgsqlCommand(OperationsIndexes.CreateScript(db.Schema) + OutboxReadIndexes.CreateScript(db.Schema), connection);
        await prepare.ExecuteNonQueryAsync();
        var source = new PostgreSqlWorkReader(db.ConnectionString, db.Schema);
        await source.ValidateAsync(); await source.ValidateOutboxAsync();
        return source;
    }
    protected override async Task SetCreatedAt(ProducerScope scope, DateTimeOffset time)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"UPDATE {db.Schema}.outbox_messages SET created_at=@time WHERE scope_id=@scope", connection);
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("time", time);
        await command.ExecuteNonQueryAsync();
    }
    protected override async Task CorruptHeaders(ProducerScope scope, Guid id)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"UPDATE {db.Schema}.outbox_messages SET headers='null' WHERE scope_id=@scope AND id=@id", connection);
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", id);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Outbox_read_cancels_on_real_schema_lock_and_pool_recovers()
    {
        var source = await OpenAsync(); await using var cleanup = (IAsyncDisposable)source;
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var command = new NpgsqlCommand($"LOCK TABLE {db.Schema}.outbox_messages IN ACCESS EXCLUSIVE MODE", connection, transaction);
        await command.ExecuteNonQueryAsync();
        var reader = new OperationsReader(source, timeout: TimeSpan.FromMilliseconds(200));
        var watch = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAsync<OperationsUnavailableException>(() => reader.ListOutboxAsync(new(Scope(), DateTimeOffset.UtcNow)));
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(4));
        await transaction.RollbackAsync();
        Assert.Empty((await reader.ListOutboxAsync(new(Scope(), DateTimeOffset.UtcNow))).Value.Items);
    }
}
