using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

public sealed partial class SqlServerWorkStore
{
    private const string QuarantineColumns = "id,scope_id,identity_id,fingerprint,key_version,fingerprint_version,reason_code,version,released_work_id,data_purged_at";

    public async Task<IngressQuarantineResult> QuarantineAsync(WorkScope scope, IdentityPolicy policy, IngressSnapshot snapshot,
        string reasonCode, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope); ArgumentNullException.ThrowIfNull(policy); ArgumentNullException.ThrowIfNull(snapshot);
        _ = new IngressMessageRejectedException(reasonCode);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(cancellationToken);
        // A rejected message binds the same durable policy as a successfully mapped message.
        await EnsureBindingAsync(connection, transaction, new(scope, policy, "quarantine-policy-binding"), cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction, "ingress:" + _schema + ":" + snapshot.Identity(scope), cancellationToken);
        IngressQuarantineItem? current = null;
        await using (var query = new SqlCommand($"SELECT {QuarantineColumns} FROM {_schema}.ingress_quarantine WITH(UPDLOCK,HOLDLOCK,ROWLOCK) WHERE identity_id=@identity", connection, transaction))
        {
            query.Parameters.AddValue("identity", snapshot.Identity(scope));
            await using var reader = await query.ExecuteReaderCheckedAsync(cancellationToken);
            if (await reader.ReadCheckedAsync(cancellationToken)) current = ReadQuarantine(reader);
        }
        var status = IngressRecovery.Accept(scope, policy, snapshot, current);
        if (status == IngressQuarantineStatus.Recorded)
        {
            current = new(Guid.NewGuid(), scope.Id, snapshot.Identity(scope), snapshot.Fingerprint,
                policy.KeyVersion, policy.FingerprintVersion, reasonCode, 0, null);
            await using var insert = new SqlCommand($"""
                INSERT INTO {_schema}.ingress_quarantine(id,scope_id,identity_id,fingerprint,key_version,fingerprint_version,
                    reason_code,source_binding,source_position,codec,body,metadata,is_null_body)
                VALUES(@id,@scope,@identity,@fingerprint,@key,@fp,@reason,@binding,@position,@codec,@body,@metadata,@null)
                """, connection, transaction);
            insert.Parameters.AddValue("id", current.Id); insert.Parameters.AddValue("scope", scope.Id);
            insert.Parameters.AddValue("identity", current.IdentityId); insert.Parameters.AddValue("fingerprint", snapshot.Fingerprint);
            insert.Parameters.AddValue("key", policy.KeyVersion); insert.Parameters.AddValue("fp", policy.FingerprintVersion);
            insert.Parameters.AddValue("reason", reasonCode); insert.Parameters.AddValue("binding", snapshot.SourceBinding);
            insert.Parameters.AddValue("position", snapshot.SourcePosition); insert.Parameters.AddValue("codec", snapshot.Codec);
            insert.Parameters.AddValue("body", snapshot.CopyBody()); insert.Parameters.AddValue("metadata", snapshot.CopyMetadata());
            insert.Parameters.AddValue("null", snapshot.IsNullBody);
            await insert.ExecuteNonQueryCheckedAsync(cancellationToken);
        }
        await CommitAsync(transaction, current!.Id, cancellationToken);
        return new(status, current);
    }

    public async Task<IngressQuarantineItem?> FindQuarantineAsync(WorkScope scope, Guid id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var query = new SqlCommand($"SELECT {QuarantineColumns} FROM {_schema}.ingress_quarantine WHERE scope_id=@scope AND id=@id", connection);
        query.Parameters.AddValue("scope", scope.Id); query.Parameters.AddValue("id", id);
        await using var reader = await query.ExecuteReaderCheckedAsync(cancellationToken);
        return await reader.ReadCheckedAsync(cancellationToken) ? ReadQuarantine(reader) : null;
    }

    public async Task<IngressSnapshot?> ReadQuarantineDataAsync(WorkScope scope, Guid id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var query = new SqlCommand($"SELECT source_binding,source_position,codec,body,metadata,is_null_body,data_purged_at FROM {_schema}.ingress_quarantine WHERE scope_id=@scope AND id=@id", connection);
        query.Parameters.AddValue("scope", scope.Id); query.Parameters.AddValue("id", id);
        await using var reader = await query.ExecuteReaderCheckedAsync(cancellationToken);
        if (!await reader.ReadCheckedAsync(cancellationToken)) return null;
        if (!reader.IsDBNull(6)) throw new PayloadUnavailableException("ingress", id);
        return new(reader.GetString(0), reader.GetString(1), reader.GetString(2),
            (byte[])reader.GetValue(3), (byte[])reader.GetValue(4), reader.GetBoolean(5));
    }

    public async Task<IngressReleaseResult> ReleaseAsync(IngressReleaseRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var scope = request.Submission.Identity.Scope;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction, "ingress-release:" + _schema + ":" + scope.Id + ":" + request.RequestId, cancellationToken);
        await using (var previous = new SqlCommand($"SELECT fingerprint,result_status,quarantine_id,result_work_id,result_version FROM {_schema}.ingress_release_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            previous.Parameters.AddValue("scope", scope.Id); previous.Parameters.AddValue("id", request.RequestId);
            await using var reader = await previous.ExecuteReaderCheckedAsync(cancellationToken);
            if (await reader.ReadCheckedAsync(cancellationToken))
                return IngressRecovery.Repeat(request, reader.GetString(0), new((IngressReleaseStatus)reader.GetInt32(1), reader.GetGuid(2), reader.IsDBNull(3) ? null : reader.GetGuid(3), reader.GetInt64(4)));
        }
        IngressQuarantineItem? current;
        await using (var query = new SqlCommand($"SELECT {QuarantineColumns} FROM {_schema}.ingress_quarantine WITH(UPDLOCK,HOLDLOCK,ROWLOCK) WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            query.Parameters.AddValue("scope", scope.Id); query.Parameters.AddValue("id", request.QuarantineId);
            await using var reader = await query.ExecuteReaderCheckedAsync(cancellationToken);
            current = await reader.ReadCheckedAsync(cancellationToken) ? ReadQuarantine(reader) : null;
        }
        var refusal = IngressRecovery.AdmitRelease(request, current);
        IngressReleaseResult result;
        if (refusal.HasValue) result = new(refusal.Value, request.QuarantineId, current?.ReleasedWorkId, current?.Version ?? 0);
        else
        {
            await transaction.SaveAsync("release_admission", cancellationToken);
            var accepted = await AcceptWithinTransactionAsync(connection, transaction, request.Submission, cancellationToken);
            if (!await SqlServerTenantBudgetProtocol.ConsumeAsync(connection, transaction, _schema, scope.Tenant, TenantBudgetKind.Recovery, cancellationToken))
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
                await using var update = new SqlCommand($"UPDATE {_schema}.ingress_quarantine SET released_work_id=@work,version=@next,released_at=TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00') WHERE scope_id=@scope AND id=@id AND version=@version AND released_work_id IS NULL", connection, transaction);
                update.Parameters.AddValue("work", accepted.Work.Id); update.Parameters.AddValue("next", nextVersion);
                update.Parameters.AddValue("scope", scope.Id); update.Parameters.AddValue("id", request.QuarantineId);
                update.Parameters.AddValue("version", current.Version);
                if (await update.ExecuteNonQueryCheckedAsync(cancellationToken) != 1) throw new InvalidOperationException("Quarantine version changed under lock.");
                result = new(accepted.Status == AcceptanceStatus.Accepted ? IngressReleaseStatus.Accepted : IngressReleaseStatus.Duplicate, request.QuarantineId, accepted.Work.Id, nextVersion);
            }
        }
        await using var receipt = new SqlCommand($"""
            INSERT INTO {_schema}.ingress_release_requests(scope_id,id,fingerprint,quarantine_id,actor,reason,result_status,result_work_id,result_version)
            VALUES(@scope,@id,@fingerprint,@quarantine,@actor,@reason,@status,@work,@version)
            """, connection, transaction);
        receipt.Parameters.AddValue("scope", scope.Id); receipt.Parameters.AddValue("id", request.RequestId);
        receipt.Parameters.AddValue("fingerprint", request.Fingerprint); receipt.Parameters.AddValue("quarantine", request.QuarantineId);
        receipt.Parameters.AddValue("actor", request.Actor); receipt.Parameters.AddValue("reason", request.Reason);
        receipt.Parameters.AddValue("status", (int)result.Status); receipt.Parameters.AddValue("version", result.Version);
        SqlServerProtocol.Optional(receipt, "work", System.Data.SqlDbType.UniqueIdentifier, result.WorkId);
        await receipt.ExecuteNonQueryCheckedAsync(cancellationToken);
        await CommitAsync(transaction, request.RequestId, cancellationToken);
        return result;
    }

    private static IngressQuarantineItem ReadQuarantine(System.Data.Common.DbDataReader reader) => new(reader.GetGuid(0), reader.GetString(1),
        reader.GetString(2), reader.GetString(3), reader.GetString(4), reader.GetString(5), reader.GetString(6), reader.GetInt64(7),
        reader.IsDBNull(8) ? null : reader.GetGuid(8)) { DataRetained = reader.IsDBNull(9) };
}
