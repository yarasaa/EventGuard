using EventGuard.Operations.PostgreSql;
using EventGuard.Operations.Replay.PostgreSql;
using EventGuard.PostgreSql;
using Npgsql;
namespace EventGuard.Operations.Tests;
public sealed class PostgreSqlRecoveryTests(EventGuard.PostgreSql.Tests.DatabaseFixture db)
    : RecoveryStoreContract<PostgreSqlSession>(db, db), IClassFixture<EventGuard.PostgreSql.Tests.DatabaseFixture>
{
    protected override async Task<IWorkReplayStore> OpenCommandsAsync()
    {
        var store = new PostgreSqlReplayStore(db.ConnectionString, db.ConnectionString, db.Schema);
        await store.ValidateRecoveryAsync(); return store;
    }
    protected override async Task<IWorkReadStore> OpenReadsAsync()
    {
        var store = new PostgreSqlWorkReader(db.ConnectionString, db.Schema);
        await store.ValidateRecoveryAsync(); return store;
    }
    protected override async Task CorruptAsync(OutgoingMessage message)
    {
        
        await using var command = db.DataSource.CreateCommand($"UPDATE {db.Schema}.outbox_messages SET headers='[]' WHERE scope_id=@scope AND id=@id");
        command.Parameters.AddWithValue("scope", message.Scope.Id); command.Parameters.AddWithValue("id", message.IntentId);
        await command.ExecuteNonQueryAsync();
    }
    [Fact]
    public async Task Release_commit_reply_loss_is_resolved_with_the_original_request()
    {
        var request = await ReleaseRequest();
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new EventGuard.PostgreSql.Tests.CommitReplyDropProxy(settings.Port);
        settings.Port = proxy.Port; settings.SslMode = SslMode.Disable; settings.DisableGssEncryptionForFaultProxy();
        await using var interrupted = new PostgreSqlReplayStore(settings.ConnectionString, settings.ConnectionString, db.Schema);
        await ResolveLostRelease(interrupted, proxy.Dropped.Task, request);
    }
    [Fact]
    public async Task Repair_commit_reply_loss_is_resolved_with_the_original_request()
    {
        var request = await RepairRequest();
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new EventGuard.PostgreSql.Tests.CommitReplyDropProxy(settings.Port);
        settings.Port = proxy.Port; settings.SslMode = SslMode.Disable; settings.DisableGssEncryptionForFaultProxy();
        await using var interrupted = new PostgreSqlReplayStore(settings.ConnectionString, settings.ConnectionString, db.Schema);
        await ResolveLostRepair(interrupted, proxy.Dropped.Task, request);
    }
}
