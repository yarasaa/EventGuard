using Npgsql;

namespace EventGuard.PostgreSql.Tests;

public sealed class IngressQuarantineFaultTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Lost_native_commit_reply_resolves_from_durable_snapshot_or_release_receipt(bool release)
    {
        var scope = DatabaseFixture.Scope(); var policy = new IdentityPolicy("v1", "sha256-bytes/v1");
        var raw = new IngressSnapshot("broker", "topic:0:1", "test/v1", [1], [2]);
        var original = release ? await db.Store.QuarantineAsync(scope, policy, raw, "invalid-contract") : null;
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new CommitReplyDropProxy(settings.Port);
        settings.Port = proxy.Port; settings.Pooling = false; settings.SslMode = SslMode.Disable; settings.DisableGssEncryptionForFaultProxy();
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        var interrupted = new PostgreSqlWorkStore(source, db.Schema);
        if (!release)
        {
            await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.QuarantineAsync(scope, policy, raw, "invalid-contract"));
            await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var repeated = await db.Store.QuarantineAsync(scope, policy, raw, "invalid-contract");
            Assert.Equal(IngressQuarantineStatus.Duplicate, repeated.Status);
            Assert.Equal(raw.Fingerprint, (await db.Store.ReadQuarantineDataAsync(scope, repeated.Item.Id))!.Fingerprint);
        }
        else
        {
            var request = new IngressReleaseRequest(Guid.NewGuid(), original!.Item.Id, 0, DatabaseFixture.Submission(scope), "operator", "fixed");
            var error = await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.ReleaseAsync(request));
            Assert.Equal(request.RequestId, error.OperationId);
            await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var repeated = await db.Store.ReleaseAsync(request);
            Assert.True(repeated.RepeatedRequest); Assert.Equal(IngressReleaseStatus.Accepted, repeated.Status);
            Assert.Equal(repeated.WorkId, (await db.Store.FindQuarantineAsync(scope, original.Item.Id))!.ReleasedWorkId);
            var claim = (await db.Claim(scope))!;
            await db.Store.ExecuteAsync(claim, db.Effect, new());
            Assert.Equal(1, await db.EffectCount(claim.Work.Id)); Assert.Null(await db.Claim(scope));
        }
    }

    [Fact]
    public async Task Native_receipt_failure_rolls_back_work_acceptance_and_release_together()
    {
        var scope = DatabaseFixture.Scope();
        var record = await db.Store.QuarantineAsync(scope, new("v1", "sha256-bytes/v1"),
            new("broker", "position", "codec/v1", [1], []), "invalid-contract");
        var request = new IngressReleaseRequest(Guid.NewGuid(), record.Item.Id, 0, DatabaseFixture.Submission(scope), "operator", "fixed");
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"ALTER TABLE {db.Schema}.ingress_release_requests ADD CONSTRAINT test_reject_receipt CHECK(id <> '{request.RequestId:D}')", connection);
        await command.ExecuteNonQueryAsync();
        try { await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(() => db.Store.ReleaseAsync(request)); }
        finally { command.CommandText = $"ALTER TABLE {db.Schema}.ingress_release_requests DROP CONSTRAINT test_reject_receipt"; await command.ExecuteNonQueryAsync(); }
        Assert.Null((await db.Store.FindQuarantineAsync(scope, record.Item.Id))!.ReleasedWorkId);
        Assert.Null(await db.Claim(scope));
        var result = await db.Store.ReleaseAsync(request);
        Assert.Equal(IngressReleaseStatus.Accepted, result.Status); Assert.False(result.RepeatedRequest);
    }
}
