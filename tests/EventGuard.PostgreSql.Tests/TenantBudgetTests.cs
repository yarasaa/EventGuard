using EventGuard.Store.Conformance;
using Npgsql;
namespace EventGuard.PostgreSql.Tests;
public sealed class TenantBudgetTests(DatabaseFixture db) : TenantBudgetContract<PostgreSqlSession>(db, db), IClassFixture<DatabaseFixture>
{
    protected override async Task AgeWindowAsync(string tenant)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"UPDATE {db.Schema}.tenant_budgets SET window_started_at=@old WHERE tenant_id=@id", connection);
        command.Parameters.AddWithValue("old", new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero));
        command.Parameters.AddWithValue("id", new TenantBudgetPolicy(tenant).Id); await command.ExecuteNonQueryAsync();
    }
    protected override async Task ExpireAsync(Guid workId)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"UPDATE {db.Schema}.works SET expires_at=@old WHERE id=@id", connection);
        command.Parameters.AddWithValue("old", new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero)); command.Parameters.AddWithValue("id", workId);
        await command.ExecuteNonQueryAsync();
    }
    protected override async Task CorruptAsync(OutgoingMessage message)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"UPDATE {db.Schema}.outbox_messages SET headers='[]' WHERE scope_id=@scope AND id=@id", connection);
        command.Parameters.AddWithValue("scope", message.Scope.Id); command.Parameters.AddWithValue("id", message.IntentId); await command.ExecuteNonQueryAsync();
    }
}
