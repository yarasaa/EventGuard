using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class CommitTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private SqlServerDataSource Through(CommitReplyDropProxy proxy)
    {
        var options = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false, Pooling = false };
        return new(options.ConnectionString);
    }
    [Fact]
    public async Task Lost_commit_reply_preserves_completed_and_prevents_a_second_business_effect()
    {
        var scope = DatabaseFixture.Scope();
        var accepted = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope));
        var claim = (await db.Claim(scope))!;
        await using var proxy = new CommitReplyDropProxy();
        var interrupted = new SqlServerWorkStore(Through(proxy), db.Schema);
        var error = await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.ExecuteAsync(claim, db.Effect, new()));
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(claim.Token, error.OperationId);
        Assert.Equal(WorkState.Completed, (await db.Store.FindAsync(scope, accepted.Work.Id))!.State);
        Assert.Equal(ExecutionStatus.LeaseLost, (await db.Store.ExecuteAsync(claim, db.Effect, new())).Status);
        Assert.Equal(1, await db.EffectCount(accepted.Work.Id));
    }
    [Fact]
    public async Task Lost_producer_commit_reply_is_resolved_using_the_durable_receipt()
    {
        var message = Message();
        var request = new ProducerRequest(message.Scope, Guid.NewGuid(), ContentFingerprint.FromBytes([1]));
        await using var proxy = new CommitReplyDropProxy();
        var interrupted = new SqlServerOutboxStore(Through(proxy), db.Schema);
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.ProduceAsync(request, async (session, ct) =>
        {
            await using var command = session.CreateCommand($"INSERT INTO {db.Schema}.business_effects VALUES(@id,'producer')");
            command.Parameters.AddWithValue("id", request.RequestId);
            await command.ExecuteNonQueryAsync(ct);
            await session.Outbox.EnqueueAsync(message, ct);
        }));
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(ProducerStatus.AlreadyCommitted, (await db.Outbox.ProduceAsync(request, (_, _) => throw new InvalidOperationException("Must not repeat."))).Status);
        Assert.Equal(1, await db.EffectCount(request.RequestId));
        Assert.NotNull(await db.Outbox.FindAsync(message.Scope, message.IntentId));
    }
    [Fact]
    public async Task Lost_settlement_commit_reply_does_not_reopen_a_confirmed_publication()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        var claim = (await db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)))!;
        await using var proxy = new CommitReplyDropProxy();
        var interrupted = new SqlServerOutboxStore(Through(proxy), db.Schema);
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.SettleAsync(claim, new(PublishStatus.Accepted, "accepted"), new()));
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(OutboxState.Published, (await db.Outbox.FindAsync(message.Scope, message.IntentId))!.State);
        Assert.Null(await db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
    }
    [Fact]
    public async Task Lost_replay_commit_reply_never_opens_another_generation()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        var claim = (await db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)))!;
        var failed = (await db.Outbox.SettleAsync(claim, new(PublishStatus.NotSentPermanent, "failed"), new())).Record;
        var request = new OutboxReplayRequest(message.Scope, Guid.NewGuid(), message.IntentId, failed.Version, "operator", "fixed");
        await using var proxy = new CommitReplyDropProxy();
        var recovery = new SqlServerOutboxRecoveryStore(Through(proxy), db.Schema);
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => recovery.ReplayAsync(request));
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var next = (await db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)))!;
        await db.Outbox.SettleAsync(next, new(PublishStatus.NotSentPermanent, "failed-again"), new());
        var repeat = await db.Recovery.ReplayAsync(request);
        Assert.True(repeat.RepeatedRequest);
        Assert.Equal(1, repeat.Generation);
        Assert.Equal(OutboxState.DeadLetter, (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!.State);
    }
    internal static OutgoingMessage Message(ProducerScope? scope = null) => new(scope ?? new("tenant", Guid.NewGuid().ToString("N")),
        Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "files.created"), [1, 2]);
}
