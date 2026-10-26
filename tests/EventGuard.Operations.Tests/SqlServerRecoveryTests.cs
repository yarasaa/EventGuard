using EventGuard.Operations.SqlServer;
using EventGuard.Operations.Replay.SqlServer;
using EventGuard.SqlServer;
using Microsoft.Data.SqlClient;
namespace EventGuard.Operations.Tests;
public sealed class SqlServerRecoveryTests(EventGuard.SqlServer.Tests.DatabaseFixture db)
    : RecoveryStoreContract<SqlServerSession>(db, db), IClassFixture<EventGuard.SqlServer.Tests.DatabaseFixture>
{
    protected override async Task<IWorkReplayStore> OpenCommandsAsync()
    {
        var store = new SqlServerReplayStore(db.ConnectionString, db.ConnectionString, db.Schema);
        await store.ValidateRecoveryAsync(); return store;
    }
    protected override async Task<IWorkReadStore> OpenReadsAsync()
    {
        var store = new SqlServerWorkReader(db.ConnectionString, db.Schema);
        await store.ValidateRecoveryAsync(); return store;
    }
    protected override async Task CorruptAsync(OutgoingMessage message)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new SqlCommand($"UPDATE {db.Schema}.outbox_messages SET headers='[]' WHERE scope_id=@scope AND id=@id", connection);
        command.Parameters.AddWithValue("scope", message.Scope.Id); command.Parameters.AddWithValue("id", message.IntentId);
        await command.ExecuteNonQueryAsync();
    }
    [Fact]
    public async Task Release_commit_reply_loss_is_resolved_with_the_original_request()
    {
        var request = await ReleaseRequest();
        await using var proxy = new EventGuard.SqlServer.Tests.CommitReplyDropProxy();
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false };
        await using var interrupted = new SqlServerReplayStore(settings.ConnectionString, settings.ConnectionString, db.Schema);
        await ResolveLostRelease(interrupted, proxy.Dropped.Task, request);
    }
    [Fact]
    public async Task Repair_commit_reply_loss_is_resolved_with_the_original_request()
    {
        var request = await RepairRequest();
        await using var proxy = new EventGuard.SqlServer.Tests.CommitReplyDropProxy();
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false };
        await using var interrupted = new SqlServerReplayStore(settings.ConnectionString, settings.ConnectionString, db.Schema);
        await ResolveLostRepair(interrupted, proxy.Dropped.Task, request);
    }
}
