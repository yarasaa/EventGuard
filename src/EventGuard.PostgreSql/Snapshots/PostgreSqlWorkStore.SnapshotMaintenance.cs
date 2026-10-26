using System.Text.Json;
using Npgsql;
namespace EventGuard.PostgreSql;
public sealed partial class PostgreSqlWorkStore : ISnapshotMaintenance
{
    public async Task<IReadOnlyList<SnapshotBatchEnvelope>> ReadSnapshotBatchesAsync(SnapshotDefinition definition, Guid snapshotId,
        long afterSequence=0,int limit=25,CancellationToken cancellationToken=default)
    {
        ArgumentNullException.ThrowIfNull(definition); if(afterSequence<0 || limit is <1 or >100) throw new ArgumentOutOfRangeException(nameof(limit));
        await using var c=await _dataSource.OpenConnectionAsync(cancellationToken);
        var row=await ReadSnapshotAsync(c,null,definition,snapshotId,cancellationToken)??throw new InvalidOperationException("Snapshot not found.");
        await using var cmd=StoreCommand(c,null,$"SELECT payload,fingerprint FROM {_schema}.snapshot_batches WHERE scope_id=@scope AND snapshot_id=@id AND sequence>@after ORDER BY sequence LIMIT @limit",("scope",definition.Target.Scope.Id),("id",snapshotId),("after",afterSequence),("limit",limit));
        var result=new List<SnapshotBatchEnvelope>(); await using var r=await cmd.ExecuteReaderAsync(cancellationToken);
        while(await r.ReadAsync(cancellationToken))
        {
            var bytes=r.GetFieldValue<byte[]>(0); if(ContentFingerprint.FromBytes(bytes).Value!=r.GetString(1)) throw new InvalidOperationException("Snapshot batch content is corrupt.");
            result.Add(JsonSerializer.Deserialize<SnapshotBatchEnvelope>(bytes)??throw new InvalidOperationException("Snapshot batch is empty."));
        }
        return result;
    }
    public async Task<int> PruneSnapshotStagingAsync(SnapshotDefinition definition,Guid snapshotId,DateTimeOffset olderThan,int limit=1000,CancellationToken cancellationToken=default)
    {
        ArgumentNullException.ThrowIfNull(definition); if(limit is <1 or >10000) throw new ArgumentOutOfRangeException(nameof(limit));
        await using var c=await _dataSource.OpenConnectionAsync(cancellationToken); await using var t=await c.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(c,t,"snapshot:"+_schema+":"+definition.Target.Scope.Id+":"+snapshotId,cancellationToken);
        var row=await ReadSnapshotAsync(c,t,definition,snapshotId,cancellationToken)??throw new InvalidOperationException("Snapshot not found.");
        var now = await PostgreSqlProtocol.NowAsync(c,t,cancellationToken);
        if (olderThan > now) olderThan = now; // DB clock is authoritative, including clock skew.
        await using var cmd=StoreCommand(c,t,$"""
            WITH old AS (SELECT i.ctid FROM {_schema}.snapshot_items i JOIN {_schema}.snapshots s ON s.scope_id=i.scope_id AND s.id=i.snapshot_id
              WHERE i.scope_id=@scope AND i.snapshot_id=@id AND s.updated_at<@cutoff AND (i.generation<@generation OR s.state IN(3,4))
              ORDER BY i.generation,i.ordinal LIMIT @limit)
            DELETE FROM {_schema}.snapshot_items WHERE ctid IN(SELECT ctid FROM old)
            """,("scope",definition.Target.Scope.Id),("id",snapshotId),("cutoff",olderThan.ToUniversalTime()),("generation",row.Generation),("limit",limit));
        var deleted=await cmd.ExecuteNonQueryAsync(cancellationToken); await PostgreSqlProtocol.CommitAsync(t,snapshotId,cancellationToken); return deleted;
    }
}
