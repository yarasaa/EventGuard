using EventGuard.Store.Conformance;
namespace EventGuard.PostgreSql.Tests;

public sealed class RetentionTests(DatabaseFixture db) : RetentionContract<PostgreSqlSession>(db, db), IClassFixture<DatabaseFixture>
{
    protected override async Task AgeAsync(string scopeId)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new Npgsql.NpgsqlCommand($"""
            UPDATE {db.Schema}.works SET created_at=@old,completed_at=CASE WHEN completed_at IS NULL THEN NULL ELSE @old END,
                expires_at=CASE WHEN expires_at IS NULL THEN NULL ELSE @old END WHERE scope_id=@scope;
            UPDATE {db.Schema}.outbox_messages SET created_at=@old,published_at=CASE WHEN published_at IS NULL THEN NULL ELSE @old END WHERE scope_id=@scope;
            UPDATE {db.Schema}.ingress_quarantine SET created_at=@old,released_at=CASE WHEN released_at IS NULL THEN NULL ELSE @old END WHERE scope_id=@scope;
            UPDATE {db.Schema}.outbox_quarantine SET detected_at=@old WHERE scope_id=@scope;
            UPDATE {db.Schema}.conflicts SET received_at=@old WHERE work_id IN (SELECT id FROM {db.Schema}.works WHERE scope_id=@scope);
            """, connection);
        command.Parameters.AddWithValue("scope", scopeId); command.Parameters.AddWithValue("old", new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await command.ExecuteNonQueryAsync();
    }
    protected override async Task<byte[]> DataAsync(Guid id, RetentionTarget target)
    {
        var table = target switch { RetentionTarget.ConsumerPayload => "works", RetentionTarget.ConflictPayload => "conflicts", RetentionTarget.PublishedOutboxPayload => "outbox_messages", _ => throw new ArgumentException(nameof(target)) };
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new Npgsql.NpgsqlCommand($"SELECT payload FROM {db.Schema}.{table} WHERE id=@id", connection);
        command.Parameters.AddWithValue("id", id); return (byte[])(await command.ExecuteScalarAsync())!;
    }
    protected override async Task<Guid[]> ConflictIdsAsync(string scopeId)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new Npgsql.NpgsqlCommand($"SELECT c.id FROM {db.Schema}.conflicts c JOIN {db.Schema}.works w ON w.id=c.work_id WHERE w.scope_id=@scope", connection);
        command.Parameters.AddWithValue("scope", scopeId); var ids = new List<Guid>();
        await using var reader = await command.ExecuteReaderAsync(); while (await reader.ReadAsync()) ids.Add(reader.GetGuid(0)); return ids.ToArray();
    }
    protected override async Task CorruptAsync(OutgoingMessage message)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new Npgsql.NpgsqlCommand($"UPDATE {db.Schema}.outbox_messages SET headers='[]' WHERE scope_id=@scope AND id=@id", connection);
        command.Parameters.AddWithValue("scope", message.Scope.Id); command.Parameters.AddWithValue("id", message.IntentId); await command.ExecuteNonQueryAsync();
    }
}
