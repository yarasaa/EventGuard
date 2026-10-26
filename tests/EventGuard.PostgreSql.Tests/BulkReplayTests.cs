using EventGuard.Store.Conformance;
using Npgsql;
namespace EventGuard.PostgreSql.Tests;
public sealed class BulkReplayTests(DatabaseFixture db) : BulkReplayContract<PostgreSqlSession>(db, db), IClassFixture<DatabaseFixture>
{
    protected override IBulkReplayStore OpenBulk() => new PostgreSqlBulkReplayStore(db.DataSource, db.Schema);
    [Theory] [InlineData(false)] [InlineData(true)]
    public async Task Member_commit_reply_loss_preserves_partial_progress_and_resume_identity(bool outgoing)
    {
        var request = await PrepareRequest(outgoing);
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new CommitReplyDropProxy(settings.Port);
        settings.Port = proxy.Port; settings.Pooling = true; settings.MaxPoolSize = 1; settings.SslMode = SslMode.Disable; settings.DisableGssEncryptionForFaultProxy();
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        // Reuse one native socket across plan lookup and member transaction: this frame proxy accepts one socket.
        await ResolveMemberReplyLoss(new PostgreSqlBulkReplayStore(source, db.Schema), proxy.Dropped.Task, request);
    }
}
