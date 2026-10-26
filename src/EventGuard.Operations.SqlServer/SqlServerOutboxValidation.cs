using Microsoft.Data.SqlClient;

namespace EventGuard.Operations.SqlServer;

public sealed partial class SqlServerWorkReader
{
    public async Task ValidateOutboxAsync(CancellationToken ct = default)
    {
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var command = new SqlCommand($"""
            SELECT count(*) FROM sys.indexes WHERE is_disabled=0
            AND EXISTS (SELECT 1 FROM {_schema}.schema_version WHERE singleton=1 AND version IN (1,2,3,4,5,6,7,8))
            AND (SELECT is_read_committed_snapshot_on FROM sys.databases WHERE database_id=DB_ID())=1 AND
            ((object_id=OBJECT_ID(@messages) AND name IN ('ix_ops_outbox_created','ix_ops_outbox_state_created'))
             OR (object_id=OBJECT_ID(@replays) AND name='ix_ops_outbox_replays'))
            """, connection) { CommandTimeout = 2 };
        command.Parameters.AddWithValue("messages", _schema + ".outbox_messages");
        command.Parameters.AddWithValue("replays", _schema + ".outbox_replay_requests");
        if (await command.ExecuteScalarAsync(ct).ConfigureAwait(false) is not 3)
            throw new InvalidOperationException("Outbox reads require three additional indexes. Apply OutboxReadIndexes.CreateScript before enabling outbox scopes.");
        await ValidateMetadataAccessAsync(true, ct);
    }
}
