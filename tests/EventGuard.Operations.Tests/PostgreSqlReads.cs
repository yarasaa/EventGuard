using EventGuard.Operations.PostgreSql;
using EventGuard.PostgreSql;
using Npgsql;
using Fixture = EventGuard.PostgreSql.Tests.DatabaseFixture;

namespace EventGuard.Operations.Tests;

public sealed class PostgreSqlReads(Fixture db) : ReadStoreContract<PostgreSqlSession>(db), IClassFixture<Fixture>
{
    protected override async Task<IWorkReadStore> OpenAsync()
    {
        await using var prepare = db.DataSource.CreateCommand(OperationsIndexes.CreateScript(db.Schema));
        await prepare.ExecuteNonQueryAsync();
        var source = new PostgreSqlWorkReader(db.ConnectionString, db.Schema);
        await source.ValidateAsync();
        return source;
    }
    protected override async Task SetCreatedAt(WorkScope scope, DateTimeOffset time)
    {
        await using var command = db.DataSource.CreateCommand($"UPDATE {db.Schema}.works SET created_at=@time WHERE scope_id=@scope");
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("time", time);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Read_budget_cancels_a_real_schema_lock_and_worker_pool_still_works()
    {
        var source = await OpenAsync();
        await using var cleanup = (IAsyncDisposable)source;
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var command = new NpgsqlCommand($"LOCK TABLE {db.Schema}.works IN ACCESS EXCLUSIVE MODE", connection, transaction);
        await command.ExecuteNonQueryAsync();
        var reader = new OperationsReader(source, timeout: TimeSpan.FromMilliseconds(200));
        var watch = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAsync<OperationsUnavailableException>(() => reader.ListAsync(new(Fixture.Scope(), DateTimeOffset.UtcNow)));
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(4));
        await Assert.ThrowsAsync<OperationsUnavailableException>(() => reader.ReadBacklogAsync(BacklogQuery.Consumer(Fixture.Scope())));
        await transaction.RollbackAsync();
        Assert.All((await reader.ReadBacklogAsync(BacklogQuery.Consumer(Fixture.Scope()))).Value.Buckets, b => Assert.Equal(0, b.Count));
        Assert.Empty((await reader.ListAsync(new(Fixture.Scope(), DateTimeOffset.UtcNow))).Value.Items);
    }
}
