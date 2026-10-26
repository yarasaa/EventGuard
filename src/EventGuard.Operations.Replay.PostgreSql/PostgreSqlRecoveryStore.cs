using Npgsql;
namespace EventGuard.Operations.Replay.PostgreSql;

public sealed partial class PostgreSqlReplayStore
{
    public Task<IngressReleaseResult> ReleaseIngressAsync(IngressReleaseRequest request, CancellationToken ct = default) => _store.ReleaseAsync(request, ct);
    public Task<OutboxRepairResult> RepairOutboxAsync(OutboxRepairRequest request, CancellationToken ct = default) => _outbox.RepairAsync(request, ct);
    public async Task ValidateRecoveryAsync(CancellationToken ct = default)
    {
        await ValidateAsync(ct);
        
        await using var command = _data.CreateCommand($"SELECT version FROM {_schema}.schema_version WHERE singleton");
        if (await command.ExecuteScalarAsync(ct) is not int version || version != 11)
            throw new InvalidOperationException("Recovery commands require the current migrated schema.");
    }
    public async Task<IngressReleaseReceipt?> FindIngressReleaseReceiptAsync(WorkScope scope, Guid id, Guid requestId, string actor, CancellationToken ct = default)
    {
        
        await using var command = _data.CreateCommand($"SELECT result_status,result_work_id,result_version,left(reason,1024),created_at FROM {_schema}.ingress_release_requests WHERE scope_id=@scope AND quarantine_id=@id AND id=@request AND actor=@actor");
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("request", requestId); command.Parameters.AddWithValue("actor", actor);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        var status = (IngressReleaseStatus)reader.GetInt32(0);
        if (!Enum.IsDefined(status)) throw new InvalidOperationException("Unknown recovery receipt status.");
        return new(requestId, new(status, id, reader.IsDBNull(1) ? null : reader.GetGuid(1), reader.GetInt64(2)), reader.GetString(3), reader.GetFieldValue<DateTimeOffset>(4));
    }
    public async Task<OutboxRepairReceipt?> FindOutboxRepairReceiptAsync(ProducerScope scope, Guid id, Guid requestId, string actor, CancellationToken ct = default)
    {
        
        await using var command = _data.CreateCommand($"SELECT result_status,result_generation,result_version,left(reason,1024),created_at,acknowledge_possible_duplicate FROM {_schema}.outbox_repair_requests WHERE scope_id=@scope AND intent_id=@id AND id=@request AND actor=@actor");
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("request", requestId); command.Parameters.AddWithValue("actor", actor);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        var status = (OutboxRepairStatus)reader.GetInt32(0);
        if (!Enum.IsDefined(status)) throw new InvalidOperationException("Unknown recovery receipt status.");
        return new(requestId, new(status, id, reader.GetInt32(1), reader.GetInt64(2)), reader.GetString(3), reader.GetBoolean(5), reader.GetFieldValue<DateTimeOffset>(4));
    }
}
