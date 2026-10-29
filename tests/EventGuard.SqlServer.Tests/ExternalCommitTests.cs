using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class ExternalCommitTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Lost_claim_or_settlement_commit_reply_is_resolvable(bool settle)
    {
        await db.Store.InitializeExternalActionsAsync();
        var action = new ExternalAction(new("tenant", Guid.NewGuid().ToString("N")), Guid.NewGuid(), "service", "v1", [1]);
        await db.Outbox.ProduceAsync(new(action.Scope, action.Id, ContentFingerprint.FromBytes([1])),
            async (session, ct) => { await session.ExternalActions.EnqueueAsync(action, ct); });
        var request = Guid.NewGuid();
        var claim = settle ? (await db.Store.ClaimActionAsync(action.Scope, "service", "v1", ExternalPhase.Submit, request, TimeSpan.FromMinutes(1)))! : null;
        await using var proxy = new CommitReplyDropProxy();
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false, Pooling = false };
        var source = new SqlServerDataSource(settings.ConnectionString);
        var interrupted = new SqlServerWorkStore(source, db.Schema);
        var result = new ExternalResult(ExternalResultKind.Accepted, "accepted", "reference");
        var error = await Assert.ThrowsAsync<CommitOutcomeUnknownException>(async () =>
        {
            if (settle) await interrupted.SettleActionAsync(claim!, result, new());
            else await interrupted.ClaimActionAsync(action.Scope, "service", "v1", ExternalPhase.Submit, request, TimeSpan.FromMinutes(1));
        });
        Assert.Equal(request, error.OperationId);
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        if (settle)
        {
            var repeated = await db.Store.SettleActionAsync(claim!, result, new());
            Assert.True(repeated.Recorded); Assert.Equal(ExternalActionState.Accepted, repeated.Record.State);
        }
        else
        {
            var recovered = await db.Store.ClaimActionAsync(action.Scope, "service", "v1", ExternalPhase.Submit, request, TimeSpan.FromMinutes(1));
            Assert.NotNull(recovered); Assert.Equal(1, recovered.Record.SubmissionAttempts);
            await db.Store.SettleActionAsync(recovered, result, new());
        }
        Assert.Single(await db.Store.ReadActionEvidenceAsync(action.Scope, action.Id));
        Assert.Null(await db.Store.ClaimActionAsync(action.Scope, "service", "v1", ExternalPhase.Submit, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Lost_native_batch_commit_reply_preserves_membership_and_per_item_evidence(bool settle)
    {
        await db.Store.InitializeExternalActionsAsync();
        var scope = new ProducerScope("batch-commit", Guid.NewGuid().ToString("N"));
        var actions = Enumerable.Range(0, 10).Select(_ => new ExternalAction(scope, Guid.NewGuid(), "service", "v1", [1])).ToArray();
        await db.Outbox.ProduceAsync(new(scope, Guid.NewGuid(), ContentFingerprint.FromBytes([1])), async (session, ct) =>
        { foreach (var action in actions) await session.ExternalActions.EnqueueAsync(action, ct); });
        var request = new ExternalBatchClaimRequest(scope, "service", "v1", ExternalPhase.Submit, Guid.NewGuid(), TimeSpan.FromMinutes(2), 6, 1024);
        IReadOnlyList<ExternalActionClaim>? claims = settle ? await db.Store.ClaimActionsAsync(request) : null;
        var result = new ExternalResult(ExternalResultKind.Accepted, "accepted", "reference");
        ExternalBatchSettlementRequest Results() => new(claims!.Select(c => new ExternalBatchSettlementItem(c, result)), new());
        await using var proxy = new CommitReplyDropProxy();
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false, Pooling = false };
        var source = new SqlServerDataSource(settings.ConnectionString);
        var interrupted = new SqlServerWorkStore(source, db.Schema);
        var error = await Assert.ThrowsAsync<CommitOutcomeUnknownException>(async () =>
        {
            if (settle) await interrupted.SettleActionsAsync(Results());
            else await interrupted.ClaimActionsAsync(request);
        });
        Assert.Equal(settle ? claims![0].Token : request.RequestId, error.OperationId);
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        if (settle) Assert.All(await db.Store.SettleActionsAsync(Results()), r => Assert.True(r.Recorded));
        else
        {
            claims = await db.Store.ClaimActionsAsync(request); Assert.Equal(6, claims.Count);
            Assert.All(claims, c => Assert.Equal(1, c.Record.SubmissionAttempts));
            await db.Store.SettleActionsAsync(Results());
        }
        Assert.Empty(await db.Store.ClaimActionsAsync(request));
        foreach (var claim in claims!) Assert.True(Assert.Single(await db.Store.ReadActionEvidenceAsync(scope, claim.Record.Action.Id)).Applied);
        Assert.Equal(4, (await db.Store.ClaimActionsAsync(new(scope, "service", "v1", ExternalPhase.Submit, Guid.NewGuid(), TimeSpan.FromMinutes(2), 10, 1024))).Count);
    }

}
