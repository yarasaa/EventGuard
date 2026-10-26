using System.Data;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class ProfileTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Concurrent_initializers_preserve_existing_data_and_reject_unknown_versions()
    {
        var scope = DatabaseFixture.Scope();
        var accepted = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope));
        await Task.WhenAll(Enumerable.Range(0, 6).Select(_ => db.Store.InitializeAsync()));
        Assert.Equal(accepted.Work.Id, (await db.Store.AcceptAsync(DatabaseFixture.Submission(scope))).Work.Id);
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var change = new SqlCommand($"UPDATE {db.Schema}.schema_version SET version=99", connection);
        await change.ExecuteNonQueryAsync();
        try { await Assert.ThrowsAsync<InvalidOperationException>(() => db.Store.InitializeAsync()); }
        finally { change.CommandText = $"UPDATE {db.Schema}.schema_version SET version=6"; await change.ExecuteNonQueryAsync(); }
    }
    [Fact]
    public async Task Pooled_connection_is_reset_to_read_committed_after_repeatable_read()
    {
        var options = new SqlConnectionStringBuilder(db.ConnectionString) { MaxPoolSize = 1, ApplicationName = Guid.NewGuid().ToString("N") };
        var source = new SqlServerDataSource(options.ConnectionString);
        await using (var connection = await source.OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead))
            await transaction.CommitAsync();
        await using var reused = await source.OpenConnectionAsync();
        await using var query = new SqlCommand("SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id=@@SPID", reused);
        Assert.Equal((short)2, await query.ExecuteScalarAsync());
    }
    [Fact]
    public async Task Unsupported_database_profile_is_rejected_without_creating_schema()
    {
        var options = new SqlConnectionStringBuilder(db.ConnectionString) { InitialCatalog = "master" };
        var source = new SqlServerDataSource(options.ConnectionString);
        var schema = "eg_rejected_" + Guid.NewGuid().ToString("N");
        await Assert.ThrowsAsync<NotSupportedException>(() => new SqlServerWorkStore(source, schema).InitializeAsync());
        await using var connection = await source.OpenConnectionAsync();
        await using var query = new SqlCommand("SELECT SCHEMA_ID(@schema)", connection);
        query.Parameters.AddWithValue("schema", schema);
        Assert.Equal(DBNull.Value, await query.ExecuteScalarAsync());
    }
    [Fact]
    public void Mars_and_unsafe_schema_names_are_rejected()
    {
        var options = new SqlConnectionStringBuilder(db.ConnectionString) { MultipleActiveResultSets = true };
        Assert.Throws<NotSupportedException>(() => new SqlServerDataSource(options.ConnectionString));
        Assert.Throws<ArgumentException>(() => new SqlServerWorkStore(db.DataSource, "dbo]; DROP TABLE works;--"));
    }
    [Fact]
    public async Task Identity_is_ordinal_even_under_a_case_insensitive_database_collation()
    {
        var scope = DatabaseFixture.Scope();
        var one = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope, "file"));
        var two = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope, "FILE"));
        var three = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope, "file "));
        Assert.Equal(3, new[] { one.Work.Id, two.Work.Id, three.Work.Id }.Distinct().Count());
    }
}
