using EventGuard.Store.Conformance;
namespace EventGuard.SqlServer.Tests;

public sealed class OutboxRepairTests(DatabaseFixture db) : OutboxRepairContract(db), IClassFixture<DatabaseFixture>
{
    protected override async Task CorruptAsync(OutgoingMessage message)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new Microsoft.Data.SqlClient.SqlCommand($"UPDATE {db.Schema}.outbox_messages SET headers='[]' WHERE scope_id=@scope AND id=@id", connection);
        command.Parameters.AddWithValue("scope", message.Scope.Id); command.Parameters.AddWithValue("id", message.IntentId);
        Assert.Equal(1, await command.ExecuteNonQueryAsync());
    }
    protected override async Task<string> SnapshotAsync(OutgoingMessage message)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new Microsoft.Data.SqlClient.SqlCommand($"SELECT original_record FROM {db.Schema}.outbox_quarantine WHERE scope_id=@scope AND intent_id=@id", connection);
        command.Parameters.AddWithValue("scope", message.Scope.Id); command.Parameters.AddWithValue("id", message.IntentId);
        return (string)(await command.ExecuteScalarAsync())!;
    }
}
