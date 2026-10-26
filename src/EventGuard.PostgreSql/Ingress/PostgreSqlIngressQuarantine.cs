using Npgsql;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore
{
    private const string QuarantineColumns = "id,scope_id,identity_id,fingerprint,key_version,fingerprint_version,reason_code,version,released_work_id,data_purged_at";

    public async Task<IngressQuarantineResult> QuarantineAsync(WorkScope scope, IdentityPolicy policy, IngressSnapshot snapshot,
        string reasonCode, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope); ArgumentNullException.ThrowIfNull(policy); ArgumentNullException.ThrowIfNull(snapshot);
        _ = new IngressMessageRejectedException(reasonCode);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        // A rejected message binds the same durable policy as a successfully mapped message.
        await EnsureBindingAsync(connection, transaction, new(scope, policy, "quarantine-policy-binding"), cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "ingress:" + _schema + ":" + snapshot.Identity(scope), cancellationToken);
        IngressQuarantineItem? current = null;
        await using (var query = new NpgsqlCommand($"SELECT {QuarantineColumns} FROM {_schema}.ingress_quarantine WHERE identity_id=@identity FOR UPDATE", connection, transaction))
        {
            query.Parameters.AddWithValue("identity", snapshot.Identity(scope));
            await using var reader = await query.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken)) current = ReadQuarantine(reader);
        }
        var status = IngressRecovery.Accept(scope, policy, snapshot, current);
        if (status == IngressQuarantineStatus.Recorded)
        {
            current = new(Guid.NewGuid(), scope.Id, snapshot.Identity(scope), snapshot.Fingerprint,
                policy.KeyVersion, policy.FingerprintVersion, reasonCode, 0, null);
            await using var insert = new NpgsqlCommand($"""
                INSERT INTO {_schema}.ingress_quarantine(id,scope_id,identity_id,fingerprint,key_version,fingerprint_version,
                    reason_code,source_binding,source_position,codec,body,metadata,is_null_body)
                VALUES(@id,@scope,@identity,@fingerprint,@key,@fp,@reason,@binding,@position,@codec,@body,@metadata,@null)
                """, connection, transaction);
            insert.Parameters.AddWithValue("id", current.Id); insert.Parameters.AddWithValue("scope", scope.Id);
            insert.Parameters.AddWithValue("identity", current.IdentityId); insert.Parameters.AddWithValue("fingerprint", snapshot.Fingerprint);
            insert.Parameters.AddWithValue("key", policy.KeyVersion); insert.Parameters.AddWithValue("fp", policy.FingerprintVersion);
            insert.Parameters.AddWithValue("reason", reasonCode); insert.Parameters.AddWithValue("binding", snapshot.SourceBinding);
            insert.Parameters.AddWithValue("position", snapshot.SourcePosition); insert.Parameters.AddWithValue("codec", snapshot.Codec);
            insert.Parameters.AddWithValue("body", snapshot.CopyBody()); insert.Parameters.AddWithValue("metadata", snapshot.CopyMetadata());
            insert.Parameters.AddWithValue("null", snapshot.IsNullBody);
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        await CommitAsync(transaction, current!.Id, cancellationToken);
        return new(status, current);
    }

    public async Task<IngressQuarantineItem?> FindQuarantineAsync(WorkScope scope, Guid id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var query = new NpgsqlCommand($"SELECT {QuarantineColumns} FROM {_schema}.ingress_quarantine WHERE scope_id=@scope AND id=@id", connection);
        query.Parameters.AddWithValue("scope", scope.Id); query.Parameters.AddWithValue("id", id);
        await using var reader = await query.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadQuarantine(reader) : null;
    }

    public async Task<IngressSnapshot?> ReadQuarantineDataAsync(WorkScope scope, Guid id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var query = new NpgsqlCommand($"SELECT source_binding,source_position,codec,body,metadata,is_null_body,data_purged_at FROM {_schema}.ingress_quarantine WHERE scope_id=@scope AND id=@id", connection);
        query.Parameters.AddWithValue("scope", scope.Id); query.Parameters.AddWithValue("id", id);
        await using var reader = await query.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        if (!reader.IsDBNull(6)) throw new PayloadUnavailableException("ingress", id);
        return new(reader.GetString(0), reader.GetString(1), reader.GetString(2),
            (byte[])reader.GetValue(3), (byte[])reader.GetValue(4), reader.GetBoolean(5));
    }

    public async Task<IngressReleaseResult> ReleaseAsync(IngressReleaseRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var scope = request.Submission.Identity.Scope;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "ingress-release:" + _schema + ":" + scope.Id + ":" + request.RequestId, cancellationToken);
        await using (var previous = new NpgsqlCommand($"SELECT fingerprint,result_status,quarantine_id,result_work_id,result_version FROM {_schema}.ingress_release_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            previous.Parameters.AddWithValue("scope", scope.Id); previous.Parameters.AddWithValue("id", request.RequestId);
            await using var reader = await previous.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
                return IngressRecovery.Repeat(request, reader.GetString(0), new((IngressReleaseStatus)reader.GetInt32(1), reader.GetGuid(2), reader.IsDBNull(3) ? null : reader.GetGuid(3), reader.GetInt64(4)));
        }
        IngressQuarantineItem? current;
        await using (var query = new NpgsqlCommand($"SELECT {QuarantineColumns} FROM {_schema}.ingress_quarantine WHERE scope_id=@scope AND id=@id FOR UPDATE", connection, transaction))
        {
            query.Parameters.AddWithValue("scope", scope.Id); query.Parameters.AddWithValue("id", request.QuarantineId);
            await using var reader = await query.ExecuteReaderAsync(cancellationToken);
            current = await reader.ReadAsync(cancellationToken) ? ReadQuarantine(reader) : null;
        }
        var refusal = IngressRecovery.AdmitRelease(request, current);
        IngressReleaseResult result;
        if (refusal.HasValue) result = new(refusal.Value, request.QuarantineId, current?.ReleasedWorkId, current?.Version ?? 0);
        else
        {
            await transaction.SaveAsync("release_admission", cancellationToken);
            var accepted = await AcceptWithinTransactionAsync(connection, transaction, request.Submission, cancellationToken);
            if (!await PostgreSqlTenantBudgetProtocol.ConsumeAsync(connection, transaction, _schema, scope.Tenant, TenantBudgetKind.Recovery, cancellationToken))
            {
                // Work/unique-key locks precede the shared counter. Roll back acceptance and any conflict
                // data if quota admission loses a race, then persist only the refusal receipt.
                await transaction.RollbackAsync("release_admission", cancellationToken);
                result = new(IngressReleaseStatus.RateLimited, request.QuarantineId, null, current!.Version);
            }
            else if (accepted.Status == AcceptanceStatus.Conflict)
                result = new(IngressReleaseStatus.WorkConflict, request.QuarantineId, accepted.Work.Id, current!.Version);
            else
            {
                var nextVersion = checked(current!.Version + 1);
                await using var update = new NpgsqlCommand($"UPDATE {_schema}.ingress_quarantine SET released_work_id=@work,version=@next,released_at=clock_timestamp() WHERE scope_id=@scope AND id=@id AND version=@version AND released_work_id IS NULL", connection, transaction);
                update.Parameters.AddWithValue("work", accepted.Work.Id); update.Parameters.AddWithValue("next", nextVersion);
                update.Parameters.AddWithValue("scope", scope.Id); update.Parameters.AddWithValue("id", request.QuarantineId);
                update.Parameters.AddWithValue("version", current.Version);
                if (await update.ExecuteNonQueryAsync(cancellationToken) != 1) throw new InvalidOperationException("Quarantine version changed under lock.");
                result = new(accepted.Status == AcceptanceStatus.Accepted ? IngressReleaseStatus.Accepted : IngressReleaseStatus.Duplicate, request.QuarantineId, accepted.Work.Id, nextVersion);
            }
        }
        await using var receipt = new NpgsqlCommand($"""
            INSERT INTO {_schema}.ingress_release_requests(scope_id,id,fingerprint,quarantine_id,actor,reason,result_status,result_work_id,result_version)
            VALUES(@scope,@id,@fingerprint,@quarantine,@actor,@reason,@status,@work,@version)
            """, connection, transaction);
        receipt.Parameters.AddWithValue("scope", scope.Id); receipt.Parameters.AddWithValue("id", request.RequestId);
        receipt.Parameters.AddWithValue("fingerprint", request.Fingerprint); receipt.Parameters.AddWithValue("quarantine", request.QuarantineId);
        receipt.Parameters.AddWithValue("actor", request.Actor); receipt.Parameters.AddWithValue("reason", request.Reason);
        receipt.Parameters.AddWithValue("status", (int)result.Status); receipt.Parameters.AddWithValue("version", result.Version);
        PostgreSqlProtocol.Optional(receipt, "work", NpgsqlTypes.NpgsqlDbType.Uuid, result.WorkId);
        await receipt.ExecuteNonQueryAsync(cancellationToken);
        await CommitAsync(transaction, request.RequestId, cancellationToken);
        return result;
    }

    private static IngressQuarantineItem ReadQuarantine(System.Data.Common.DbDataReader reader) => new(reader.GetGuid(0), reader.GetString(1),
        reader.GetString(2), reader.GetString(3), reader.GetString(4), reader.GetString(5), reader.GetString(6), reader.GetInt64(7),
        reader.IsDBNull(8) ? null : reader.GetGuid(8)) { DataRetained = reader.IsDBNull(9) };
}
