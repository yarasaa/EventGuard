using EventGuard.Operations.PostgreSql;
using EventGuard.Operations.Replay.PostgreSql;
using EventGuard.PostgreSql;
using Npgsql;
namespace EventGuard.Operations.Tests;
public sealed class PostgreSqlAuditTests(EventGuard.PostgreSql.Tests.DatabaseFixture db)
    : AuditStoreContract<PostgreSqlSession>(db, db), IClassFixture<EventGuard.PostgreSql.Tests.DatabaseFixture>
{
    protected override async Task<IWorkReadStore> OpenReadsAsync()
    {
        
        await using var setup = db.DataSource.CreateCommand(AuditIndexes.CreateScript(db.Schema));
        await setup.ExecuteNonQueryAsync();
        var reader = new PostgreSqlWorkReader(db.ConnectionString, db.Schema);
        await reader.ValidateAuditAsync(true, true); return reader;
    }
    protected override Task<IWorkReplayStore> OpenCommandsAsync() => Task.FromResult<IWorkReplayStore>(new PostgreSqlReplayStore(db.ConnectionString, db.ConnectionString, db.Schema));
    protected override async Task SetTimesAsync(string scopeId, bool outgoing, DateTimeOffset timestamp)
    {
        
        var table = outgoing ? "outbox_replay_requests" : "replay_requests";
        await using var update = db.DataSource.CreateCommand($"UPDATE {db.Schema}.{table} SET created_at=@at WHERE scope_id=@scope; UPDATE {db.Schema}.bulk_replay_requests SET created_at=@at WHERE scope_id=@scope");
        update.Parameters.AddWithValue("scope", scopeId); update.Parameters.AddWithValue("at", timestamp); await update.ExecuteNonQueryAsync();
    }
}
