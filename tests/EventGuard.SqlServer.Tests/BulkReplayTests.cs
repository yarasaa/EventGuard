using EventGuard.Store.Conformance;
using Microsoft.Data.SqlClient;
namespace EventGuard.SqlServer.Tests;
public sealed class BulkReplayTests(DatabaseFixture db) : BulkReplayContract<SqlServerSession>(db, db), IClassFixture<DatabaseFixture>
{
    protected override IBulkReplayStore OpenBulk() => new SqlServerBulkReplayStore(db.DataSource, db.Schema);
    [Theory] [InlineData(false)] [InlineData(true)]
    public async Task Member_commit_reply_loss_preserves_partial_progress_and_resume_identity(bool outgoing)
    {
        var request = await PrepareRequest(outgoing);
        await using var proxy = new CommitReplyDropProxy();
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Pooling = true, MaxPoolSize = 1, Encrypt = false };
        var source = new SqlServerDataSource(settings.ConnectionString);
        // Reuse one native socket across plan lookup and member transaction: this frame proxy accepts one socket.
        await ResolveMemberReplyLoss(new SqlServerBulkReplayStore(source, db.Schema), proxy.Dropped.Task, request);
    }
}
