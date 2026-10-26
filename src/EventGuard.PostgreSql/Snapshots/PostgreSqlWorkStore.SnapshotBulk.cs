using Npgsql;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore
{
    private async Task<long> InsertSnapshotItemsAsync(NpgsqlConnection c, NpgsqlTransaction t, SnapshotProgress row,
        SnapshotItem[] items, CancellationToken ct)
    {
        if (items.Length == 0) return 0;
        await using var command = StoreCommand(c, t, $"""
            WITH incoming AS (
                SELECT * FROM unnest(@keys,@hashes,@fingerprints,@payloads) WITH ORDINALITY AS i(item_key,key_hash,fingerprint,payload,position)
            ), conflicts AS (
                SELECT 1 FROM incoming i JOIN {_schema}.snapshot_items s
                ON s.scope_id=@scope AND s.snapshot_id=@id AND s.generation=@generation AND s.key_hash=i.key_hash
                WHERE s.fingerprint<>i.fingerprint OR s.item_key<>i.item_key
            ), fresh AS (
                SELECT i.* FROM incoming i WHERE NOT EXISTS(SELECT 1 FROM {_schema}.snapshot_items s
                    WHERE s.scope_id=@scope AND s.snapshot_id=@id AND s.generation=@generation AND s.key_hash=i.key_hash)
            ), inserted AS (
                INSERT INTO {_schema}.snapshot_items(scope_id,snapshot_id,generation,ordinal,key_hash,item_key,fingerprint,payload)
                SELECT @scope,@id,@generation,@base+row_number() OVER(ORDER BY position),key_hash,item_key,fingerprint,payload
                FROM fresh WHERE NOT EXISTS(SELECT 1 FROM conflicts) RETURNING 1
            ) SELECT (SELECT count(*) FROM conflicts),(SELECT count(*) FROM inserted)
            """, ("scope", row.Request.Definition.Target.Scope.Id), ("id", row.Request.Id), ("generation", row.Generation), ("base", row.ItemCount),
            ("keys", items.Select(i => i.Key).ToArray()), ("hashes", items.Select(i => i.KeyHash).ToArray()),
            ("fingerprints", items.Select(i => i.Fingerprint).ToArray()), ("payloads", items.Select(i => i.CopyPayload()).ToArray()));
        await using var reader = await command.ExecuteReaderAsync(ct); await reader.ReadAsync(ct);
        if (reader.GetInt64(0) != 0) throw new SnapshotConflictException(); return reader.GetInt64(1);
    }
}
