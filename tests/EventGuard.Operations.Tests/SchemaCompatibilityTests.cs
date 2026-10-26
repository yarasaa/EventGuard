using Microsoft.Data.SqlClient;

namespace EventGuard.Operations.Tests;

public sealed class SchemaCompatibilityTests
{
    [Fact]
    public async Task PostgreSql_health_accepts_old_and_new_layout_but_rejects_unknown_versions()
    {
        var db = new EventGuard.PostgreSql.Tests.DatabaseFixture(); await db.InitializeAsync();
        try
        {
            await using var command = db.DataSource.CreateCommand(PostgreSql.OperationsIndexes.CreateScript(db.Schema) + PostgreSql.OutboxReadIndexes.CreateScript(db.Schema));
            await command.ExecuteNonQueryAsync();
            await using var read = new PostgreSql.PostgreSqlWorkReader(db.ConnectionString,db.Schema);
            await using var replay = new Replay.PostgreSql.PostgreSqlReplayStore(db.ConnectionString,db.ConnectionString,db.Schema);
            await read.CheckHealthAsync(true,true); await replay.CheckHealthAsync(true,true);
            command.CommandText=$"DROP INDEX {db.Schema}.ix_claim_expiry; UPDATE {db.Schema}.schema_version SET version=4";
            await command.ExecuteNonQueryAsync();
            await read.CheckHealthAsync(true,true); await Assert.ThrowsAsync<InvalidOperationException>(() => replay.CheckHealthAsync(true,true));
            command.CommandText=$"DROP INDEX IF EXISTS {db.Schema}.ix_claim_active; UPDATE {db.Schema}.schema_version SET version=3";
            await command.ExecuteNonQueryAsync();
            await read.CheckHealthAsync(true,true); await Assert.ThrowsAsync<InvalidOperationException>(() => replay.CheckHealthAsync(true,true));
            command.CommandText=$"UPDATE {db.Schema}.schema_version SET version=999"; await command.ExecuteNonQueryAsync();
            await Assert.ThrowsAsync<InvalidOperationException>(()=>read.ValidateAsync());
            await Assert.ThrowsAsync<InvalidOperationException>(()=>read.ValidateOutboxAsync());
            await Assert.ThrowsAsync<InvalidOperationException>(()=>replay.ValidateAsync());
        }
        finally { await db.DisposeAsync(); }
    }

    [Fact]
    public async Task SqlServer_health_accepts_old_and_new_layout_but_rejects_unknown_versions()
    {
        var db = new EventGuard.SqlServer.Tests.DatabaseFixture(); await db.InitializeAsync();
        try
        {
            await using var connection = await db.DataSource.OpenConnectionAsync();
            await using var command = new SqlCommand(SqlServer.OperationsIndexes.CreateScript(db.Schema) + SqlServer.OutboxReadIndexes.CreateScript(db.Schema),connection);
            await command.ExecuteNonQueryAsync();
            await using var read = new SqlServer.SqlServerWorkReader(db.ConnectionString,db.Schema);
            await using var replay = new Replay.SqlServer.SqlServerReplayStore(db.ConnectionString,db.ConnectionString,db.Schema);
            await read.CheckHealthAsync(true,true); await replay.CheckHealthAsync(true,true);
            command.CommandText=$"DROP INDEX ix_claim_expiry ON {db.Schema}.works; UPDATE {db.Schema}.schema_version SET version=2";
            await command.ExecuteNonQueryAsync();
            await read.CheckHealthAsync(true,true); await Assert.ThrowsAsync<InvalidOperationException>(() => replay.CheckHealthAsync(true,true));
            command.CommandText=$"DROP INDEX ix_claim_active ON {db.Schema}.works; UPDATE {db.Schema}.schema_version SET version=1";
            await command.ExecuteNonQueryAsync();
            await read.CheckHealthAsync(true,true); await Assert.ThrowsAsync<InvalidOperationException>(() => replay.CheckHealthAsync(true,true));
            command.CommandText=$"UPDATE {db.Schema}.schema_version SET version=999"; await command.ExecuteNonQueryAsync();
            await Assert.ThrowsAsync<InvalidOperationException>(()=>read.ValidateAsync());
            await Assert.ThrowsAsync<InvalidOperationException>(()=>read.ValidateOutboxAsync());
            await Assert.ThrowsAsync<InvalidOperationException>(()=>replay.ValidateAsync());
        }
        finally { await db.DisposeAsync(); }
    }
}
