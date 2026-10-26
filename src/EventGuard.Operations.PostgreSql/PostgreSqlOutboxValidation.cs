using Npgsql;

namespace EventGuard.Operations.PostgreSql;

public sealed partial class PostgreSqlWorkReader
{
    public async Task ValidateOutboxAsync(CancellationToken ct = default)
    {
        await using var connection = await _data.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand($"""
            SELECT count(*)::int FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_class t ON t.oid=i.indrelid
            WHERE n.nspname=@schema AND i.indisvalid AND i.indisready
            AND EXISTS (SELECT 1 FROM {_schema}.schema_version WHERE singleton AND version IN (3,4,5,6,7,8,9,10,11)) AND
            ((t.relname='outbox_messages' AND c.relname IN ('ix_ops_outbox_created','ix_ops_outbox_state_created'))
             OR (t.relname='outbox_replay_requests' AND c.relname='ix_ops_outbox_replays'))
            """, connection);
        command.Parameters.AddWithValue("schema", _schema.Trim('"'));
        if (await command.ExecuteScalarAsync(ct).ConfigureAwait(false) is not 3)
            throw new InvalidOperationException("Outbox reads require three additional indexes. Apply OutboxReadIndexes.CreateScript before enabling outbox scopes.");
        await ValidateMetadataAccessAsync(true, ct);
    }
}
