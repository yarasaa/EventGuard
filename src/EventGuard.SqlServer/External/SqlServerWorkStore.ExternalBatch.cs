using System.Data;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

public sealed partial class SqlServerWorkStore : IExternalActionBatchStore
{
    public async Task ValidateExternalBatchAsync(CancellationToken cancellationToken = default)
    {
        await ValidateExternalActionsAsync(cancellationToken);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new SqlCommand("""
            SELECT COUNT(*) FROM (VALUES('external_batch_ids'),('external_batch_changes'),
                ('external_batch_projections'),('external_batch_bindings')) types(name)
            WHERE COALESCE(HAS_PERMS_BY_NAME(@schema+'.'+name,'TYPE','EXECUTE'),0)<>1
               OR COALESCE(HAS_PERMS_BY_NAME(@schema+'.'+name,'TYPE','REFERENCES'),0)<>1
            """, connection);
        command.Parameters.AddValue("schema", _schema);
        if (Convert.ToInt32(await command.ExecuteScalarCheckedAsync(cancellationToken)) != 0)
            throw new InvalidOperationException("Native external batching requires EXECUTE and REFERENCES on its four external_batch_* table types. Grant these with the deployment identity before starting the worker.");
    }


    private sealed record ExternalBatchChange(ExternalActionRecord Record, ExternalResultTarget? Target,
        ExternalPhase Phase, Guid? Token = null, DateTimeOffset? Until = null, Guid? Attempt = null,
        bool CreateAttempt = false, ExternalResult? Response = null, bool Applied = true);

    private static DataTable ExternalChangesTable(IReadOnlyList<ExternalBatchChange> changes)
    {
        var table = new DataTable();
        table.Columns.Add("action_id", typeof(Guid));
        table.Columns.Add("state", typeof(int));
        table.Columns.Add("observation", typeof(int));
        table.Columns.Add("version", typeof(long));
        table.Columns.Add("submission_attempts", typeof(int));
        table.Columns.Add("next_at", typeof(DateTimeOffset));
        table.Columns.Add("observation_deadline", typeof(DateTimeOffset));
        table.Columns.Add("provider_reference", typeof(string));
        table.Columns.Add("last_reason", typeof(string));
        table.Columns.Add("result_data", typeof(byte[]));
        table.Columns.Add("lease_token", typeof(Guid));
        table.Columns.Add("lease_until", typeof(DateTimeOffset));
        table.Columns.Add("lease_phase", typeof(int));
        table.Columns.Add("write_state", typeof(bool));
        table.Columns.Add("attempt_id", typeof(Guid));
        table.Columns.Add("attempt_phase", typeof(int));
        table.Columns.Add("create_attempt", typeof(bool));
        table.Columns.Add("finish_attempt", typeof(bool));
        table.Columns.Add("response_kind", typeof(int));
        table.Columns.Add("response_reason", typeof(string));
        table.Columns.Add("response_reference", typeof(string));
        table.Columns.Add("response_data", typeof(byte[]));
        table.Columns.Add("response_fingerprint", typeof(string));
        table.Columns.Add("applied", typeof(bool));
        foreach (var c in changes)
        {
            var r = c.Record;
            table.Rows.Add(r.Action.Id, (int)r.State, (int)r.Observation, r.Version, r.SubmissionAttempts,
                r.NextAttemptAt, Db(r.ObservationDeadline), Db(r.ProviderReference), Db(r.LastReason), r.ResultData,
                Db(c.Token), Db(c.Until), c.Token.HasValue ? (int)c.Phase : DBNull.Value,
                c.Applied, Db(c.Attempt), (int)c.Phase, c.CreateAttempt, c.Response is not null,
                c.Response is null ? DBNull.Value : (int)c.Response.Kind, Db(c.Response?.Reason), Db(c.Response?.Reference),
                Db(c.Response?.CopyData()), c.Response is null ? DBNull.Value : ExternalResultDigest(c.Response), c.Applied);
        }
        return table;
    }

    private static object Db(object? value) => value ?? DBNull.Value;
    private static string ExternalResultDigest(ExternalResult result) => StableHash.Of("external/result/v1",
        ((int)result.Kind).ToString(System.Globalization.CultureInfo.InvariantCulture), result.Reason,
        result.Reference ?? "", ContentFingerprint.FromBytes(result.CopyData()).Value);

    private static (List<ExternalActionClaim> Claims, List<ExternalBatchChange> Changes) PrepareExternalClaims(
        IEnumerable<ExternalRow> rows, ExternalBatchClaimRequest request, DateTimeOffset now)
    {
        var claims = new List<ExternalActionClaim>();
        var changes = new List<ExternalBatchChange>();
        foreach (var row in rows)
        {
            var before = row.Record;
            if (before.State == ExternalActionState.Sending)
            {
                changes.Add(new(before with { State = ExternalActionState.Unknown, Version = checked(before.Version + 1),
                    LastReason = "submission-lease-expired", NextAttemptAt = now }, row.Target, ExternalPhase.Submit));
                continue;
            }
            if (request.Phase == ExternalPhase.Observe && before.ObservationDeadline <= now)
            {
                changes.Add(new(before with { Observation = ExternalObservationState.TimedOut, Version = checked(before.Version + 1),
                    LastReason = "observation-deadline-expired" }, row.Target, ExternalPhase.Observe));
                continue;
            }
            var record = before with { Version = checked(before.Version + 1),
                State = request.Phase == ExternalPhase.Submit ? ExternalActionState.Sending : before.State,
                SubmissionAttempts = request.Phase == ExternalPhase.Submit ? checked(before.SubmissionAttempts + 1) : before.SubmissionAttempts };
            var token = Guid.NewGuid();
            var until = now + request.LeaseDuration;
            claims.Add(new(record, request.Phase, token, until));
            changes.Add(new(record, row.Target, request.Phase, token, until, token, CreateAttempt: true));
        }
        return (claims, changes);
    }

    private static string ExternalDuePredicate(ExternalPhase phase) => phase switch
    {
        ExternalPhase.Submit => "(state=0 OR state=1)",
        ExternalPhase.Reconcile => "(state=4 OR state=1)",
        _ => "(state=2 AND observation=1)"
    };

    private static string ExternalDueIndex(ExternalPhase phase) => phase switch
    {
        ExternalPhase.Submit => "ix_external_submit_due",
        ExternalPhase.Reconcile => "ix_external_reconcile_due",
        _ => "ix_external_observe_due"
    };
    // The hash keeps long adapter names out of index keys. Exact names still fence hash collisions.
    private const string ExternalBindingPredicate = "binding_key=CONVERT(binary(32),HASHBYTES('SHA2_256',CONVERT(binary(4),DATALENGTH(@binding))+CONVERT(varbinary(max),@binding)+CONVERT(varbinary(max),@binding_version)))";

    public async Task<IReadOnlyList<ExternalActionClaim>> ClaimActionsAsync(ExternalBatchClaimRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(IsolationLevel.RepeatableRead, cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction, "external-batch:" + _schema + ":" + request.Scope.Id + ":" + request.RequestId, cancellationToken);
        await using (var receipt = new SqlCommand($"SELECT fingerprint FROM {_schema}.external_batch_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            receipt.Parameters.AddValue("scope", request.Scope.Id); receipt.Parameters.AddValue("id", request.RequestId);
            if (await receipt.ExecuteScalarCheckedAsync(cancellationToken) is string fingerprint)
            {
                if (fingerprint != request.Fingerprint) throw new InvalidOperationException("Batch claim request ID already has different semantics.");
                return await ReadPreviousExternalBatchAsync(connection, transaction, request, cancellationToken);
            }
        }
        var now = await SqlServerProtocol.NowAsync(connection, transaction, cancellationToken);
        var rows = new List<ExternalRow>();
        // Hydrate only the locked candidate keys. A scope scan under RepeatableRead holds shared
        // locks on other workers' candidates and can deadlock their later lock conversion.
        // Schema-owned clustered primary keys (index 1) seek both scope and ID, not just a scope prefix.
        var selectedColumns = string.Join(",", ExternalColumns.Split(',').Select(c => "a." + c));
        await using (var select = new SqlCommand($"""
            DECLARE @candidates TABLE(id uniqueidentifier PRIMARY KEY,next_at datetimeoffset(7),bytes bigint);
            INSERT INTO @candidates SELECT TOP(@maximum) id,next_at,payload_bytes FROM {_schema}.external_actions WITH(UPDLOCK,READPAST,ROWLOCK,FORCESEEK({ExternalDueIndex(request.Phase)}(scope_id,binding_key,next_at)))
            WHERE scope_id=@scope AND {ExternalBindingPredicate} AND binding_name=@binding AND binding_version=@binding_version
              AND {ExternalDuePredicate(request.Phase)} AND next_at<=@now AND (lease_until IS NULL OR lease_until<=@now) ORDER BY next_at,id;
            WITH bounded AS (SELECT id,SUM(bytes) OVER(ORDER BY next_at,id ROWS UNBOUNDED PRECEDING) AS bytes FROM @candidates)
            SELECT {selectedColumns} FROM bounded b
            JOIN {_schema}.external_actions a WITH(UPDLOCK,READPAST,ROWLOCK,FORCESEEK(1(scope_id,id))) ON a.scope_id=@scope AND a.id=b.id
            WHERE b.bytes<=@bytes ORDER BY a.next_at,a.id OPTION(FORCE ORDER,LOOP JOIN);
            """, connection, transaction))
        {
            select.Parameters.AddValue("scope", request.Scope.Id); select.Parameters.AddValue("binding", request.Binding);
            select.Parameters.AddValue("binding_version", request.BindingVersion); select.Parameters.AddValue("now", now);
            select.Parameters.AddValue("maximum", request.MaximumItems); select.Parameters.AddValue("bytes", request.MaximumPayloadBytes);
            await using var reader = await select.ExecuteReaderCheckedAsync(cancellationToken);
            while (await reader.ReadCheckedAsync(cancellationToken)) rows.Add(ReadExternal(reader));
        }
        // Empty polling must not grow the durable receipt ledger. No action was acquired or recovered.
        if (rows.Count == 0) return Array.Empty<ExternalActionClaim>();
        await using (var receipt = new SqlCommand($"INSERT INTO {_schema}.external_batch_requests(scope_id,id,fingerprint) VALUES(@scope,@id,@fingerprint)", connection, transaction))
        {
            receipt.Parameters.AddValue("scope", request.Scope.Id); receipt.Parameters.AddValue("id", request.RequestId);
            receipt.Parameters.AddValue("fingerprint", request.Fingerprint); await receipt.ExecuteNonQueryCheckedAsync(cancellationToken);
        }
        now = await SqlServerProtocol.NowAsync(connection, transaction, cancellationToken);
        var (claims, changes) = PrepareExternalClaims(rows, request, now);
        await WriteExternalBatchAsync(connection, transaction, request.Scope, request.RequestId, changes, now, cancellationToken);
        await SqlServerProtocol.CommitAsync(transaction, request.RequestId, cancellationToken);
        return claims.AsReadOnly();
    }

    private async Task<IReadOnlyList<ExternalActionClaim>> ReadPreviousExternalBatchAsync(SqlConnection connection,
        SqlTransaction transaction, ExternalBatchClaimRequest request, CancellationToken ct)
    {
        var columns = string.Join(",", ExternalColumns.Split(',').Select(c => "a." + c));
        await using var command = new SqlCommand($"""
            SELECT {columns} FROM {_schema}.external_actions a
            JOIN {_schema}.external_attempts e ON e.scope_id=a.scope_id AND e.action_id=a.id AND e.id=a.lease_token
            WHERE e.scope_id=@scope AND e.batch_request_id=@id AND a.lease_phase=@phase
              AND a.lease_until>TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00')
            ORDER BY a.next_at,a.id
            """, connection, transaction);
        command.Parameters.AddValue("scope", request.Scope.Id); command.Parameters.AddValue("id", request.RequestId);
        command.Parameters.AddValue("phase", (int)request.Phase);
        var claims = new List<ExternalActionClaim>();
        await using var reader = await command.ExecuteReaderCheckedAsync(ct);
        while (await reader.ReadCheckedAsync(ct))
        {
            var row = ReadExternal(reader);
            claims.Add(new(row.Record, request.Phase, row.Token!.Value, row.Until!.Value));
        }
        return claims.AsReadOnly();
    }

    public async Task<IReadOnlyList<ExternalSettlement>> SettleActionsAsync(ExternalBatchSettlementRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var scope = request.Items[0].Claim.Record.Action.Scope;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(IsolationLevel.RepeatableRead, cancellationToken);
        var columns = string.Join(",", ExternalColumns.Split(',').Select(c => "a." + c));
        var rows = new Dictionary<Guid, ExternalRow>();
        var attempts = new Dictionary<Guid, (Guid Action, ExternalPhase Phase, string? Fingerprint, bool Applied)>();
        await using (var select = new SqlCommand($"""
            SELECT {columns} FROM @actions x
            JOIN {_schema}.external_actions a WITH(UPDLOCK,ROWLOCK,FORCESEEK(1(scope_id,id))) ON a.scope_id=@scope AND a.id=x.id
            ORDER BY a.id OPTION(FORCE ORDER,LOOP JOIN);
            SELECT e.id,e.action_id,e.phase,e.result_fingerprint,e.applied FROM @tokens x
            JOIN {_schema}.external_attempts e WITH(FORCESEEK(1(scope_id,id))) ON e.scope_id=@scope AND e.id=x.id
            OPTION(FORCE ORDER,LOOP JOIN);
            """, connection, transaction))
        {
            select.Parameters.AddValue("scope", scope.Id);
            AddExternalIds(select, "actions", request.Items.Select(i => i.Claim.Record.Action.Id));
            AddExternalIds(select, "tokens", request.Items.Select(i => i.Claim.Token));
            await using var reader = await select.ExecuteReaderCheckedAsync(cancellationToken);
            while (await reader.ReadCheckedAsync(cancellationToken))
            { var row = ReadExternal(reader); rows.Add(row.Record.Action.Id, row); }
            await reader.NextResultCheckedAsync(cancellationToken);
            while (await reader.ReadCheckedAsync(cancellationToken)) attempts.Add(reader.GetGuid(0),
                (reader.GetGuid(1), (ExternalPhase)reader.GetInt32(2), reader.IsDBNull(3) ? null : reader.GetString(3), reader.GetBoolean(4)));
        }
        var now = await SqlServerProtocol.NowAsync(connection, transaction, cancellationToken);
        var changes = new List<ExternalBatchChange>();
        var settlements = new List<ExternalSettlement>();
        foreach (var item in request.Items)
        {
            var claim = item.Claim; var action = claim.Record.Action;
            if (!rows.TryGetValue(action.Id, out var row)) throw new InvalidOperationException("External action not found.");
            if (row.Record.Action.Fingerprint != action.Fingerprint) throw new ExternalActionConflictException(action.Id);
            if (!attempts.TryGetValue(claim.Token, out var attempt) || attempt.Action != action.Id || attempt.Phase != claim.Phase)
                throw new InvalidOperationException("Claim does not belong to the action and phase.");
            if (attempt.Fingerprint is not null)
            {
                if (attempt.Fingerprint != ExternalResultDigest(item.Result)) throw new InvalidOperationException("This claim already has different result evidence.");
                settlements.Add(new(attempt.Applied, row.Record));
                continue;
            }
            var owned = row.Token == claim.Token && row.Phase == claim.Phase && row.Record.Version == claim.Record.Version && row.Until > now;
            var after = owned ? ExternalActionLifecycle.Settle(row.Record, claim.Phase, item.Result, request.Policy, now) : row.Record;
            changes.Add(new(after, row.Target, claim.Phase, Attempt: claim.Token, Response: item.Result, Applied: owned));
            settlements.Add(new(owned, after));
        }
        await WriteExternalBatchAsync(connection, transaction, scope, null, changes, now, cancellationToken);
        await SqlServerProtocol.CommitAsync(transaction, request.Items[0].Claim.Token, cancellationToken);
        return settlements.AsReadOnly();
    }

    private async Task WriteExternalBatchAsync(SqlConnection connection, SqlTransaction transaction, ProducerScope scope,
        Guid? batchId, IReadOnlyList<ExternalBatchChange> changes, DateTimeOffset now, CancellationToken ct)
    {
        if (changes.Count == 0) return;
        using var table = ExternalChangesTable(changes);
        await using var command = new SqlCommand { Connection = connection, Transaction = transaction };
        command.Parameters.AddValue("scope", scope.Id); command.Parameters.AddValue("now", now);
        SqlServerProtocol.Optional(command, "batch", SqlDbType.UniqueIdentifier, batchId);
        command.Parameters.Add(new SqlParameter("rows", SqlDbType.Structured) { TypeName = _schema + ".external_batch_changes", Value = table });
        command.CommandText = $"""
            UPDATE a SET state=x.state, observation=x.observation, version=x.version, submission_attempts=x.submission_attempts, next_at=x.next_at, observation_deadline=x.observation_deadline, provider_reference=x.provider_reference, last_reason=x.last_reason, result_data=x.result_data, lease_token=x.lease_token, lease_until=x.lease_until, lease_phase=x.lease_phase
            FROM @rows x JOIN {_schema}.external_actions a WITH(ROWLOCK,INDEX(1)) ON a.scope_id=@scope AND a.id=x.action_id
            WHERE x.write_state=1 OPTION(FORCE ORDER,LOOP JOIN);
            INSERT INTO {_schema}.external_attempts(scope_id,id,action_id,phase,batch_request_id)
                SELECT @scope,attempt_id,action_id,attempt_phase,@batch FROM @rows WHERE create_attempt=1;
            UPDATE e SET finished_at=@now,result_kind=x.response_kind,reason=x.response_reason,
                provider_reference=x.response_reference,result_data=x.response_data,result_fingerprint=x.response_fingerprint,applied=x.applied
            FROM @rows x JOIN {_schema}.external_attempts e WITH(ROWLOCK,INDEX(1)) ON e.scope_id=@scope AND e.id=x.attempt_id
            WHERE x.finish_attempt=1 OPTION(FORCE ORDER,LOOP JOIN);
            """;
        await command.ExecuteNonQueryCheckedAsync(ct);
        await PublishExternalBatchUpdatesAsync(connection, transaction,
            changes.Where(c => c.Applied && !c.CreateAttempt && c.Target is not null).ToArray(), ct);
    }

    private void AddExternalIds(SqlCommand command, string name, IEnumerable<Guid> ids)
    {
        var table = new DataTable(); table.Columns.Add("id", typeof(Guid));
        foreach (var id in ids) table.Rows.Add(id);
        command.Parameters.Add(new SqlParameter(name, SqlDbType.Structured) { TypeName = _schema + ".external_batch_ids", Value = table });
    }
}
