using Microsoft.Data.SqlClient;
using System.Text.Json;

namespace EventGuard.SqlServer;

public sealed partial class SqlServerWorkStore
{
    private async Task<long> InsertSnapshotItemsAsync(SqlConnection c, SqlTransaction t, SnapshotProgress row,
        SnapshotItem[] items, CancellationToken ct)
    {
        if (items.Length == 0) return 0;
        var json = JsonSerializer.Serialize(items.Select((item, index) => new
        { position = index, key = item.Key, hash = item.KeyHash, fingerprint = item.Fingerprint, payload = Convert.ToHexString(item.CopyPayload()) }));
        await using var command = StoreCommand(c, t, $"""
            DECLARE @incoming TABLE(position int NOT NULL,item_key nvarchar(1024),key_hash nvarchar(64) COLLATE Latin1_General_100_BIN2,
                fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2,payload varbinary(max));
            INSERT INTO @incoming SELECT position,item_key,key_hash,fingerprint,CONVERT(varbinary(max),payload,2)
            FROM OPENJSON(@items) WITH(position int '$.position', item_key nvarchar(1024) '$.key',key_hash nvarchar(64) '$.hash',
                fingerprint nvarchar(64) '$.fingerprint',payload varchar(max) '$.payload');
            IF EXISTS(SELECT 1 FROM @incoming i JOIN {_schema}.snapshot_items s
                ON s.scope_id=@scope AND s.snapshot_id=@id AND s.generation=@generation AND s.key_hash=i.key_hash
                WHERE s.fingerprint<>i.fingerprint OR s.item_key COLLATE Latin1_General_100_BIN2<>i.item_key COLLATE Latin1_General_100_BIN2)
                THROW 51002,'Snapshot item identity conflict.',1;
            INSERT INTO {_schema}.snapshot_items(scope_id,snapshot_id,generation,ordinal,key_hash,item_key,fingerprint,payload)
            SELECT @scope,@id,@generation,@base+row_number() OVER(ORDER BY position),key_hash,item_key,fingerprint,payload
            FROM @incoming i WHERE NOT EXISTS(SELECT 1 FROM {_schema}.snapshot_items s
                WHERE s.scope_id=@scope AND s.snapshot_id=@id AND s.generation=@generation AND s.key_hash=i.key_hash);
            SELECT CAST(@@ROWCOUNT AS bigint);
            """, ("scope", row.Request.Definition.Target.Scope.Id), ("id", row.Request.Id), ("generation", row.Generation), ("base", row.ItemCount), ("items", json));
        try { return (long)(await command.ExecuteScalarCheckedAsync(ct))!; }
        catch (SqlException error) when (error.Number == 51002) { throw new SnapshotConflictException(); }
    }
}
