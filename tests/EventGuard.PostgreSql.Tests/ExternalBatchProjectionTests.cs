using Npgsql;

namespace EventGuard.PostgreSql.Tests;

public sealed class ExternalBatchProjectionTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private async Task<(ExternalAction[] Actions, ExternalResultTarget[] Targets)> Seed()
    {
        await db.Store.InitializeExternalActionsAsync();
        var scope = new ProducerScope("bulk-results", Guid.NewGuid().ToString("N"));
        var targets = new[] { new ExternalResultTarget(scope, "application-status", "v1"), new ExternalResultTarget(scope, "audit-status", "v1") };
        var actions = Enumerable.Range(0, 12).Select(i => new ExternalAction(scope, Guid.NewGuid(), "service", "v1", [(byte)i])).ToArray();
        await db.Outbox.ProduceAsync(new(scope, Guid.NewGuid(), ContentFingerprint.FromBytes([1])), async (session, ct) =>
        { for (var i = 0; i < actions.Length; i++) await session.ExternalActions.EnqueueAsync(actions[i], targets[i % 2], ct); });
        return (actions, targets);
    }
    private Task<IReadOnlyList<ExternalActionClaim>> Claim(ExternalAction action) => db.Store.ClaimActionsAsync(
        new(action.Scope, action.Binding, action.BindingVersion, ExternalPhase.Submit, Guid.NewGuid(), TimeSpan.FromMinutes(2), 100, 1024 * 1024));
    private static ExternalBatchSettlementRequest Results(IReadOnlyList<ExternalActionClaim> claims) => new(
        claims.Select(c => new ExternalBatchSettlementItem(c, new(ExternalResultKind.Accepted, "accepted", "référence-" + c.Record.Action.Id, [7,8,9]))), new());

    [Fact]
    public async Task Native_settlement_atomically_projects_each_application_handler_once()
    {
        var (actions, targets) = await Seed(); var claims = await Claim(actions[0]);
        await db.Store.SettleActionsAsync(Results(claims));
        await db.Store.SettleActionsAsync(Results(claims));
        var applied = new HashSet<Guid>();
        foreach (var target in targets)
        {
            while (await db.Claim(target.WorkScope) is { } work)
            {
                var execution = await db.Store.ExecuteAsync(work, async (_, _, session, ct) =>
                {
                    await session.ExternalResults.ApplyAsync(target, (update, token) =>
                    {
                        Assert.Equal(ExternalActionState.Accepted, update.State);
                        Assert.Equal(new byte[] { 7,8,9 }, update.CopyStateData());
                        Assert.Equal("référence-" + update.Action.Id, update.ProviderReference);
                        Assert.True(applied.Add(update.Action.Id)); return ValueTask.CompletedTask;
                    }, ct);
                }, new RetryPolicy());
                Assert.Equal(ExecutionStatus.Completed, execution.Status);
            }
        }
        Assert.Equal(actions.Length, applied.Count);
    }

    [Fact]
    public async Task Policy_mismatch_rolls_back_all_item_evidence_and_state()
    {
        var (actions, targets) = await Seed(); var target = targets[0];
        await db.Store.AcceptAsync(new(new(target.WorkScope, new("incompatible", "v1"), "unrelated"), Guid.NewGuid(), [1], ContentFingerprint.FromBytes([1])));
        var claims = await Claim(actions[0]);
        await Assert.ThrowsAsync<PolicyMismatchException>(() => db.Store.SettleActionsAsync(Results(claims)));
        foreach (var a in actions)
        {
            Assert.Equal(ExternalActionState.Sending, (await db.Store.FindActionAsync(a.Scope, a.Id))!.State);
            Assert.Null(Assert.Single(await db.Store.ReadActionEvidenceAsync(a.Scope, a.Id)).Result);
        }
    }

    [Fact]
    public async Task Batch_lease_recovery_projects_unknown_and_late_evidence_does_not_add_a_second_update()
    {
        var (actions, _) = await Seed(); var claims = await Claim(actions[0]);
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"UPDATE {db.Schema}.external_actions SET lease_until=@past,next_at=@past WHERE scope_id=@scope", connection);
        command.Parameters.AddWithValue("scope", actions[0].Scope.Id);
        command.Parameters.AddWithValue("past", DateTimeOffset.UtcNow.AddMinutes(-1));
        await command.ExecuteNonQueryAsync();
        Assert.Empty(await Claim(actions[0]));
        Assert.All(await db.Store.SettleActionsAsync(Results(claims)), r => Assert.False(r.Recorded));
        command.CommandText = $"SELECT COUNT(*) FROM {db.Schema}.external_result_updates WHERE scope_id=@scope AND state=4";
        Assert.Equal(actions.Length, Convert.ToInt32(await command.ExecuteScalarAsync()));
    }

    [Fact]
    public async Task Empty_polls_do_not_grow_the_receipt_ledger()
    {
        await db.Store.InitializeExternalActionsAsync();
        var scope = new ProducerScope("empty", Guid.NewGuid().ToString("N"));
        for (var i = 0; i < 5; i++) Assert.Empty(await db.Store.ClaimActionsAsync(
            new(scope, "service", "v1", ExternalPhase.Submit, Guid.NewGuid(), TimeSpan.FromMinutes(1), 10, 1024)));
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"SELECT COUNT(*) FROM {db.Schema}.external_batch_requests WHERE scope_id=@scope", connection);
        command.Parameters.AddWithValue("scope", scope.Id);
        Assert.Equal(0, Convert.ToInt32(await command.ExecuteScalarAsync()));
    }
}
