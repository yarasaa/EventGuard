using EventGuard.Operations.SqlServer;
using EventGuard.SqlServer;
using Microsoft.Data.SqlClient;
using Fixture = EventGuard.SqlServer.Tests.DatabaseFixture;

namespace EventGuard.Operations.Tests;

public sealed class SqlServerReads(Fixture db) : ReadStoreContract<SqlServerSession>(db), IClassFixture<Fixture>
{
    protected override async Task<IWorkReadStore> OpenAsync()
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var prepare = new SqlCommand(OperationsIndexes.CreateScript(db.Schema), connection);
        await prepare.ExecuteNonQueryAsync();
        var source = new SqlServerWorkReader(db.ConnectionString, db.Schema);
        await source.ValidateAsync();
        return source;
    }
    protected override async Task SetCreatedAt(WorkScope scope, DateTimeOffset time)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new SqlCommand($"UPDATE {db.Schema}.works SET created_at=@time WHERE scope_id=@scope", connection);
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("time", time);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Read_budget_cancels_a_real_schema_lock_and_pool_recovers()
    {
        var source = await OpenAsync();
        await using var cleanup = (IAsyncDisposable)source;
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
        await using var command = new SqlCommand($"ALTER TABLE {db.Schema}.works ADD ops_lock_probe int NULL", connection, transaction);
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
