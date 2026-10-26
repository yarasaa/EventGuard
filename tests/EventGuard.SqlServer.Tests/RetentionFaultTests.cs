using Microsoft.Data.SqlClient;
namespace EventGuard.SqlServer.Tests;

public sealed class RetentionFaultTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private async Task<(RetentionRequest Request, Guid WorkId)> Prepare()
    {
        var scope = DatabaseFixture.Scope(); var accepted = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope, payload: [1, 2]));
        var claim = (await db.Claim(scope))!; await db.Store.ExecuteAsync(claim, db.Effect, new());
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var age = new SqlCommand($"UPDATE {db.Schema}.works SET created_at=@old,completed_at=@old WHERE id=@id", connection);
        age.Parameters.AddWithValue("id", accepted.Work.Id); age.Parameters.AddWithValue("old", new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await age.ExecuteNonQueryAsync();
        return (new(scope, Guid.NewGuid(), RetentionTarget.ConsumerPayload, new(2021, 1, 1, 0, 0, 0, TimeSpan.Zero), "operator", "configured retention"), accepted.Work.Id);
    }

    [Fact]
    public async Task Native_commit_reply_loss_resolves_to_the_original_batch_receipt()
    {
        var (request, id) = await Prepare();
        await using var proxy = new CommitReplyDropProxy();
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false, Pooling = false };
        var source = new SqlServerDataSource(settings.ConnectionString);
        var interrupted = new SqlServerWorkStore(source, db.Schema);
        var error = await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.RetainAsync(request));
        Assert.Equal(request.RequestId, error.OperationId); await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var repeated = await db.Store.RetainAsync(request);
        Assert.True(repeated.RepeatedRequest); Assert.Equal(new[] { id }, repeated.PurgedIds);
    }

    [Fact]
    public async Task Native_audit_failure_rolls_back_removed_data_and_version()
    {
        var (request, id) = await Prepare();
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new SqlCommand($"ALTER TABLE {db.Schema}.retention_requests ADD CONSTRAINT test_reject_retention CHECK(id <> '{request.RequestId:D}')", connection);
        await command.ExecuteNonQueryAsync();
        try { await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(() => db.Store.RetainAsync(request)); }
        finally { command.CommandText = $"ALTER TABLE {db.Schema}.retention_requests DROP CONSTRAINT test_reject_retention"; await command.ExecuteNonQueryAsync(); }
        command.CommandText = $"SELECT payload,payload_purged_at FROM {db.Schema}.works WHERE id=@id"; command.Parameters.AddWithValue("id", id);
        await using (var reader = await command.ExecuteReaderAsync()) { Assert.True(await reader.ReadAsync()); Assert.Equal(new byte[] { 1, 2 }, (byte[])reader.GetValue(0)); Assert.True(reader.IsDBNull(1)); }
        var repeated = await db.Store.RetainAsync(request); Assert.False(repeated.RepeatedRequest); Assert.Equal(new[] { id }, repeated.PurgedIds);
    }
}
