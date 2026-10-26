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
}
