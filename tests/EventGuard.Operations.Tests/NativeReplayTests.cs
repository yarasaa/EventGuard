using EventGuard.Operations.Replay.PostgreSql;
using EventGuard.Operations.Replay.SqlServer;
using EventGuard.PostgreSql;
using EventGuard.SqlServer;
using Npgsql;
using Microsoft.Data.SqlClient;

namespace EventGuard.Operations.Tests;

public sealed class PostgreSqlReplayTests(EventGuard.PostgreSql.Tests.DatabaseFixture db)
    : ReplayStoreContract<PostgreSqlSession>(db), IClassFixture<EventGuard.PostgreSql.Tests.DatabaseFixture>
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
        var (scope, request) = await FailedRequest();
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new EventGuard.PostgreSql.Tests.CommitReplyDropProxy(settings.Port);
        settings.Port = proxy.Port; settings.SslMode = SslMode.Disable; settings.DisableGssEncryptionForFaultProxy();
        await using var interrupted = new PostgreSqlReplayStore(settings.ConnectionString, settings.ConnectionString, db.Schema);
        await ResolveAfterLostReply(interrupted, proxy.Dropped.Task, scope, request);
    }
    [Fact]
    public void Command_connection_cannot_silently_target_a_different_database() =>
        Assert.Throws<ArgumentException>(() => new PostgreSqlReplayStore(db.ConnectionString,
            new NpgsqlConnectionStringBuilder(db.ConnectionString) { Database = "other" }.ConnectionString, db.Schema));
}

public sealed class SqlServerReplayTests(EventGuard.SqlServer.Tests.DatabaseFixture db)
    : ReplayStoreContract<SqlServerSession>(db), IClassFixture<EventGuard.SqlServer.Tests.DatabaseFixture>
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
        var (scope, request) = await FailedRequest();
        await using var proxy = new EventGuard.SqlServer.Tests.CommitReplyDropProxy();
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false };
        await using var interrupted = new SqlServerReplayStore(settings.ConnectionString, settings.ConnectionString, db.Schema);
        await ResolveAfterLostReply(interrupted, proxy.Dropped.Task, scope, request);
    }
    [Fact]
    public void Command_connection_cannot_silently_target_a_different_database() =>
        Assert.Throws<ArgumentException>(() => new SqlServerReplayStore(db.ConnectionString,
            new SqlConnectionStringBuilder(db.ConnectionString) { InitialCatalog = "other" }.ConnectionString, db.Schema));
}
