using Npgsql;

namespace EventGuard.PostgreSql.Tests;

public sealed class ExternalBatchMigrationTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Schema_four_upgrade_preserves_inflight_single_claim_and_enables_native_batches()
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        foreach (var migration in new[] { "001_external.sql", "002_result_handlers.sql", "003_call_limits.sql", "004_observation_notices.sql" })
        {
            await using var stream = typeof(PostgreSqlWorkStore).Assembly.GetManifestResourceStream("EventGuard.PostgreSql.External.Migrations." + migration)!;
            using var reader = new StreamReader(stream);
            await using var command = new NpgsqlCommand((await reader.ReadToEndAsync()).Replace("__SCHEMA__", db.Schema, StringComparison.Ordinal), connection);
            await command.ExecuteNonQueryAsync();
        }
        var scope = new ProducerScope("upgrade", Guid.NewGuid().ToString("N"));
        var actions = Enumerable.Range(0, 3).Select(_ => new ExternalAction(scope, Guid.NewGuid(), "service", "v1", [1])).ToArray();
        await db.Outbox.ProduceAsync(new(scope, Guid.NewGuid(), ContentFingerprint.FromBytes([1])), async (session, ct) =>
        { foreach (var a in actions) await session.ExternalActions.EnqueueAsync(a, ct); });
        var inflight = (await db.Store.ClaimActionAsync(scope, "service", "v1", ExternalPhase.Submit, Guid.NewGuid(), TimeSpan.FromMinutes(2)))!;
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Store.ValidateExternalActionsAsync());
        await db.Store.InitializeExternalActionsAsync(); await db.Store.InitializeExternalActionsAsync();
        await db.Store.ValidateExternalActionsAsync();
        Assert.True((await db.Store.SettleActionAsync(inflight, new(ExternalResultKind.Accepted, "accepted"), new())).Recorded);
        var claims = await db.Store.ClaimActionsAsync(new(scope, "service", "v1", ExternalPhase.Submit, Guid.NewGuid(), TimeSpan.FromMinutes(2), 10, 1024));
        Assert.Equal(2, claims.Count);
        Assert.All(await db.Store.SettleActionsAsync(new(claims.Select(c => new ExternalBatchSettlementItem(c, new(ExternalResultKind.Accepted, "accepted"))), new())), r => Assert.True(r.Recorded));
    }
}
