using System.Text.Json;
using Microsoft.Data.SqlClient;
namespace EventGuard.SqlServer;

public sealed class SqlServerBulkReplayStore : IBulkReplayStore
{
    private readonly SqlServerDataSource _data;
    private readonly string _schema;
    private readonly SqlServerWorkStore _consumer;
    private readonly SqlServerOutboxRecoveryStore _outbox;
    public SqlServerBulkReplayStore(SqlServerDataSource data, string schema = "eventguard")
    {
        _data = data ?? throw new ArgumentNullException(nameof(data)); _schema = SqlServerProtocol.QuoteSchema(schema);
        _consumer = new(data, schema); _outbox = new(data, schema);
    }
    public async Task<BulkReplayPreparation> PrepareBulkAsync(BulkReplayRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var connection = await _data.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction, "bulk-replay:" + _schema + ":" + request.ScopeId + ":" + request.RequestId, cancellationToken);
        await using (var previous = new SqlCommand($"SELECT fingerprint FROM {_schema}.bulk_replay_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            previous.Parameters.AddValue("scope", request.ScopeId); previous.Parameters.AddValue("id", request.RequestId);
            if (await previous.ExecuteScalarCheckedAsync(cancellationToken) is string fingerprint)
                return fingerprint == request.Fingerprint ? new(BulkReplayStatus.Prepared, true) : new(BulkReplayStatus.RequestConflict);
        }
        await using var insert = new SqlCommand($"""
            INSERT INTO {_schema}.bulk_replay_requests(scope_id,id,kind,fingerprint,actor,reason,acknowledge_possible_duplicate,items)
            VALUES(@scope,@id,@kind,@fingerprint,@actor,@reason,@ack,@items)
            """, connection, transaction);
        insert.Parameters.AddValue("scope", request.ScopeId); insert.Parameters.AddValue("id", request.RequestId); insert.Parameters.AddValue("kind", (int)request.Kind);
        insert.Parameters.AddValue("fingerprint", request.Fingerprint); insert.Parameters.AddValue("actor", request.Actor); insert.Parameters.AddValue("reason", request.Reason);
        insert.Parameters.AddValue("ack", request.AcknowledgePossibleDuplicate);
        insert.Parameters.AddValue("items", JsonSerializer.Serialize(request.Items));
        await insert.ExecuteNonQueryCheckedAsync(cancellationToken);
        await SqlServerProtocol.CommitAsync(transaction, request.RequestId, cancellationToken);
        return new(BulkReplayStatus.Prepared);
    }
    public async Task<BulkReplayPreparation> RunBulkAsync(BulkReplayRequest request, CancellationToken cancellationToken = default)
    {
        var preparation = await PrepareBulkAsync(request, cancellationToken);
        if (preparation.Status != BulkReplayStatus.Prepared) return preparation;
        // Do not keep a connection or transaction open across members. Each immutable member has a stable receipt.
        for (var i = 0; i < request.Items.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (request.Kind == BulkReplayKind.Consumer) await _consumer.ReplayAsync(request.ConsumerRequest(i), cancellationToken);
            else await _outbox.ReplayAsync(request.OutboxRequest(i), cancellationToken);
        }
        return preparation;
    }
    public Task<BulkReplayProgress?> FindBulkAsync(WorkScope scope, Guid requestId, string actor, CancellationToken cancellationToken = default) =>
        FindAsync(scope ?? throw new ArgumentNullException(nameof(scope)), null, requestId, actor, cancellationToken);
    public Task<BulkReplayProgress?> FindBulkAsync(ProducerScope scope, Guid requestId, string actor, CancellationToken cancellationToken = default) =>
        FindAsync(null, scope ?? throw new ArgumentNullException(nameof(scope)), requestId, actor, cancellationToken);
    private async Task<BulkReplayProgress?> FindAsync(WorkScope? consumer, ProducerScope? outbox, Guid id, string actor, CancellationToken ct)
    {
        if (id == Guid.Empty || string.IsNullOrWhiteSpace(actor) || actor.Length > 1024) throw new ArgumentException("Invalid bulk receipt lookup.");
        var scopeId = consumer?.Id ?? outbox!.Id; var kind = consumer is null ? BulkReplayKind.Outbox : BulkReplayKind.Consumer;
        await using var connection = await _data.OpenConnectionAsync(ct);
        BulkReplayRequest request; DateTimeOffset createdAt;
        await using (var query = new SqlCommand($"SELECT fingerprint,left(reason,1024),acknowledge_possible_duplicate,CASE WHEN DATALENGTH(items)<=32768 THEN items ELSE NULL END,created_at FROM {_schema}.bulk_replay_requests WHERE scope_id=@scope AND id=@id AND kind=@kind AND actor=@actor COLLATE Latin1_General_100_BIN2", connection))
        {
            query.Parameters.AddValue("scope", scopeId); query.Parameters.AddValue("id", id); query.Parameters.AddValue("kind", (int)kind); query.Parameters.AddValue("actor", actor);
            await using var reader = await query.ExecuteReaderCheckedAsync(ct);
            if (!await reader.ReadCheckedAsync(ct)) return null;
            if (reader.IsDBNull(3) || consumer is not null && reader.GetBoolean(2)) throw new InvalidOperationException("Invalid bounded bulk plan.");
            var json = reader.GetString(3);
            if (json.Length > 16384) throw new InvalidOperationException("Bulk plan exceeds its bound.");
            var items = JsonSerializer.Deserialize<BulkReplayItem[]>(json) ?? throw new InvalidOperationException("Invalid bulk plan.");
            request = consumer is not null ? new(consumer, id, items, actor, reader.GetString(1)) : new(outbox!, id, items, actor, reader.GetString(1), reader.GetBoolean(2));
            if (reader.GetString(0) != request.Fingerprint) throw new InvalidOperationException("Bulk plan fingerprint mismatch.");
            createdAt = reader.GetFieldValue<DateTimeOffset>(4);
        }
        var entries = request.Items.Select((item, index) => new BulkReplayEntry(item.Id, request.ItemRequestId(index), item.ExpectedVersion, null, null, null)).ToArray();
        var indices = entries.Select((item, index) => (item.RequestId, index)).ToDictionary(pair => pair.RequestId, pair => pair.index);
        var names = Enumerable.Range(0, entries.Length).Select(i => "@r" + i).ToArray();
        var table = kind == BulkReplayKind.Consumer ? "replay_requests" : "outbox_replay_requests";
        var target = kind == BulkReplayKind.Consumer ? "work_id" : "intent_id";
        await using var receipts = new SqlCommand($"SELECT id,fingerprint,{target},result_status,result_generation,result_version FROM {_schema}.{table} WHERE scope_id=@scope AND id IN ({string.Join(',', names)})", connection);
        receipts.Parameters.AddValue("scope", scopeId);
        for (var i = 0; i < entries.Length; i++) receipts.Parameters.AddValue("r" + i, entries[i].RequestId);
        await using var results = await receipts.ExecuteReaderCheckedAsync(ct);
        while (await results.ReadCheckedAsync(ct))
        {
            var index = indices[results.GetGuid(0)]; var entry = entries[index];
            var fingerprint = kind == BulkReplayKind.Consumer ? request.ConsumerRequest(index).Fingerprint : request.OutboxRequest(index).Fingerprint;
            if (fingerprint != results.GetString(1)) { entries[index] = entry with { Status = "RequestConflict", Generation = 0, Version = 0 }; continue; }
            if (results.GetGuid(2) != entry.Id) throw new InvalidOperationException("Bulk member receipt mismatch.");
            var raw = results.GetInt32(3);
            var status = kind == BulkReplayKind.Consumer
                ? Enum.IsDefined((ReplayStatus)raw) ? ((ReplayStatus)raw).ToString() : throw new InvalidOperationException("Unknown replay result.")
                : Enum.IsDefined((OutboxReplayStatus)raw) ? ((OutboxReplayStatus)raw).ToString() : throw new InvalidOperationException("Unknown replay result.");
            entries[index] = entry with { Status = status, Generation = results.GetInt32(4), Version = results.GetInt64(5) };
        }
        return new(request, Array.AsReadOnly(entries), createdAt);
    }
}
