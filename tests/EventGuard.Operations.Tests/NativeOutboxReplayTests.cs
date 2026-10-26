using EventGuard.Operations.Replay.PostgreSql;
using EventGuard.Operations.Replay.SqlServer;
using EventGuard.PostgreSql;
using EventGuard.SqlServer;
using Npgsql;
using Microsoft.Data.SqlClient;

namespace EventGuard.Operations.Tests;

public sealed class PostgreSqlOutboxReplayTests(EventGuard.PostgreSql.Tests.DatabaseFixture db)
    : OutboxReplayStoreContract(db), IClassFixture<EventGuard.PostgreSql.Tests.DatabaseFixture>
{
    protected override async Task<IWorkReplayStore> OpenAsync()
    {
        var store = new PostgreSqlReplayStore(db.ConnectionString, db.ConnectionString, db.Schema);
        await store.ValidateAsync();
        return store;
    }
    [Fact]
    public async Task Real_commit_reply_loss_is_resolved_without_reopening_a_later_failed_attempt()
    {
        var request = await FailedRequest(true);
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new EventGuard.PostgreSql.Tests.CommitReplyDropProxy(settings.Port);
        settings.Port = proxy.Port; settings.SslMode = SslMode.Disable; settings.DisableGssEncryptionForFaultProxy();
        await using var interrupted = new PostgreSqlReplayStore(settings.ConnectionString, settings.ConnectionString, db.Schema);
        await ResolveAfterLostReply(interrupted, proxy.Dropped.Task, request);
    }
    protected override async Task CorruptHeaders(ProducerScope scope, Guid id)
    {
        await using var connection = new NpgsqlConnection(db.ConnectionString); await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"UPDATE {db.Schema}.outbox_messages SET headers='null'::jsonb WHERE scope_id=@scope AND id=@id", connection);
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", id);
        Assert.Equal(1, await command.ExecuteNonQueryAsync());
    }

}

public sealed class SqlServerOutboxReplayTests(EventGuard.SqlServer.Tests.DatabaseFixture db)
    : OutboxReplayStoreContract(db), IClassFixture<EventGuard.SqlServer.Tests.DatabaseFixture>
{
    protected override async Task<IWorkReplayStore> OpenAsync()
    {
        var store = new SqlServerReplayStore(db.ConnectionString, db.ConnectionString, db.Schema);
        await store.ValidateAsync();
        return store;
    }
    [Fact]
    public async Task Real_commit_reply_loss_is_resolved_without_reopening_a_later_failed_attempt()
    {
        var request = await FailedRequest(true);
        await using var proxy = new EventGuard.SqlServer.Tests.CommitReplyDropProxy();
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false };
        await using var interrupted = new SqlServerReplayStore(settings.ConnectionString, settings.ConnectionString, db.Schema);
        await ResolveAfterLostReply(interrupted, proxy.Dropped.Task, request);
    }
    protected override async Task CorruptHeaders(ProducerScope scope, Guid id)
    {
        await using var connection = new SqlConnection(db.ConnectionString); await connection.OpenAsync();
        await using var command = new SqlCommand($"UPDATE {db.Schema}.outbox_messages SET headers='null' WHERE scope_id=@scope AND id=@id", connection);
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", id);
        Assert.Equal(1, await command.ExecuteNonQueryAsync());
    }

}
