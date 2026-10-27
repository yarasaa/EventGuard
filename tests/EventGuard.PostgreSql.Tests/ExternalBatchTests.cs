using EventGuard.TestSupport;

namespace EventGuard.PostgreSql.Tests;

public sealed class ExternalBatchTests(DatabaseFixture db) : ExternalBatchStoreContract, IClassFixture<DatabaseFixture>
{
    protected override IExternalActionStore Store => db.Store;
    protected override IExternalActionStore Restart() => new PostgreSqlWorkStore(db.DataSource, db.Schema);
    protected override Task Initialize() => db.Store.InitializeExternalActionsAsync();
    protected override async Task Save(ExternalAction[] actions) => await db.Outbox.ProduceAsync(
        new(actions[0].Scope, Guid.NewGuid(), ContentFingerprint.FromBytes([1])), async (session, ct) =>
        { foreach (var action in actions) await session.ExternalActions.EnqueueAsync(action, ct); });
    protected override async Task Due(ProducerScope scope, bool expireLeases = false)
    {
        await using var command = db.DataSource.CreateCommand($"UPDATE {db.Schema}.external_actions SET next_at=@past" +
            (expireLeases ? ",lease_until=@past" : "") + " WHERE scope_id=@scope");
        command.Parameters.AddWithValue("past", DateTimeOffset.UtcNow.AddMinutes(-1));
        command.Parameters.AddWithValue("scope", scope.Id); await command.ExecuteNonQueryAsync();
    }
}
