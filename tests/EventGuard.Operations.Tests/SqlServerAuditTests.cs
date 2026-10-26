using EventGuard.Operations.SqlServer;
using EventGuard.Operations.Replay.SqlServer;
using EventGuard.SqlServer;
using Microsoft.Data.SqlClient;
namespace EventGuard.Operations.Tests;
public sealed class SqlServerAuditTests(EventGuard.SqlServer.Tests.DatabaseFixture db)
    : AuditStoreContract<SqlServerSession>(db, db), IClassFixture<EventGuard.SqlServer.Tests.DatabaseFixture>
{
    protected override async Task<IWorkReadStore> OpenReadsAsync()
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var setup = new SqlCommand(AuditIndexes.CreateScript(db.Schema), connection);
        await setup.ExecuteNonQueryAsync();
        var reader = new SqlServerWorkReader(db.ConnectionString, db.Schema);
        await reader.ValidateAuditAsync(true, true); return reader;
    }
    protected override Task<IWorkReplayStore> OpenCommandsAsync() => Task.FromResult<IWorkReplayStore>(new SqlServerReplayStore(db.ConnectionString, db.ConnectionString, db.Schema));
    protected override async Task SetTimesAsync(string scopeId, bool outgoing, DateTimeOffset timestamp)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        var table = outgoing ? "outbox_replay_requests" : "replay_requests";
        await using var update = new SqlCommand($"UPDATE {db.Schema}.{table} SET created_at=@at WHERE scope_id=@scope; UPDATE {db.Schema}.bulk_replay_requests SET created_at=@at WHERE scope_id=@scope", connection);
        update.Parameters.AddWithValue("scope", scopeId); update.Parameters.AddWithValue("at", timestamp); await update.ExecuteNonQueryAsync();
    }
}
