using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class ExternalBatchConcurrencyTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private async Task<ExternalAction[]> Seed(int count)
    {
        await db.Store.InitializeExternalActionsAsync();
        var scope = new ProducerScope("batch-locks", Guid.NewGuid().ToString("N"));
        var actions = Enumerable.Range(0, count).Select(_ => new ExternalAction(scope, Guid.NewGuid(), "service", "v1", [1])).ToArray();
        await db.Outbox.ProduceAsync(new(scope, Guid.NewGuid(), ContentFingerprint.FromBytes([1])), async (session, ct) =>
        { foreach (var a in actions) await session.ExternalActions.EnqueueAsync(a, ct); });
        return actions;
    }
    private static ExternalBatchClaimRequest Request(ProducerScope scope, int count) =>
        new(scope, "service", "v1", ExternalPhase.Submit, Guid.NewGuid(), TimeSpan.FromMinutes(2), count, 1024 * 1024);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Claim_and_settlement_do_not_read_lock_unselected_actions(bool settle)
    {
        var actions = await Seed(20); var scope = actions[0].Scope;
        var claims = settle ? await db.Store.ClaimActionsAsync(Request(scope, 5)) : [];
        var locked = actions.First(a => claims.All(c => c.Record.Action.Id != a.Id));
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
        await using var hold = new SqlCommand($"UPDATE {db.Schema}.external_actions SET last_reason='test-lock' WHERE scope_id=@scope AND id=@id", connection, transaction);
        hold.Parameters.AddWithValue("scope", scope.Id); hold.Parameters.AddWithValue("id", locked.Id);
        await hold.ExecuteNonQueryAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        if (settle)
        {
            var results = await db.Store.SettleActionsAsync(new(claims.Select(c => new ExternalBatchSettlementItem(c,
                new(ExternalResultKind.Accepted, "accepted"))), new()), timeout.Token);
            Assert.Equal(5, results.Count); Assert.All(results, r => Assert.True(r.Recorded));
        }
        else
        {
            var available = await db.Store.ClaimActionsAsync(Request(scope, 20), timeout.Token);
            Assert.Equal(19, available.Count); Assert.DoesNotContain(available, c => c.Record.Action.Id == locked.Id);
        }
        await transaction.RollbackAsync();
    }

    [Fact]
    public async Task Concurrent_claims_with_overlapping_transaction_lifetimes_do_not_convert_foreign_shared_locks()
    {
        await db.Store.InitializeExternalActionsAsync();
        await using var connection = await db.DataSource.OpenConnectionAsync();
        // Keep every claim transaction alive after candidate hydration to expose cross-batch shared lock conversions.
        await using var trigger = new SqlCommand($"CREATE TRIGGER {db.Schema}.pause_batch_receipt ON {db.Schema}.external_batch_requests AFTER INSERT AS WAITFOR DELAY '00:00:00.100'", connection);
        await trigger.ExecuteNonQueryAsync();
        try
        {
            for (var round = 0; round < 5; round++)
            {
                var actions = await Seed(120); var scope = actions[0].Scope;
                var pages = await Task.WhenAll(Enumerable.Range(0, 6).Select(_ => db.Store.ClaimActionsAsync(Request(scope, 20))));
                var allPages = pages.Where(p => p.Count > 0).ToList();
                var claims = pages.SelectMany(p => p).ToList();
                while (claims.Count < actions.Length)
                {
                    var remaining = await db.Store.ClaimActionsAsync(Request(scope, 20));
                    Assert.NotEmpty(remaining); allPages.Add(remaining); claims.AddRange(remaining);
                }
                Assert.Equal(120, claims.Count); Assert.Equal(120, claims.Select(c => c.Record.Action.Id).Distinct().Count());
                var settled = await Task.WhenAll(allPages.Select(page => db.Store.SettleActionsAsync(new(page.Select(c =>
                    new ExternalBatchSettlementItem(c, new(ExternalResultKind.Accepted, "accepted"))), new()))));
                Assert.All(settled.SelectMany(p => p), r => Assert.True(r.Recorded));
            }
        }
        finally
        {
            trigger.CommandText = $"DROP TRIGGER {db.Schema}.pause_batch_receipt";
            await trigger.ExecuteNonQueryAsync();
        }
    }
}
