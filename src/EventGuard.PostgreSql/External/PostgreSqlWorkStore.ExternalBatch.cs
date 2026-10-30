using System.Data;
using Npgsql;
using NpgsqlTypes;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore : IExternalActionBatchStore
{
    public Task ValidateExternalBatchAsync(CancellationToken cancellationToken = default) => ValidateExternalActionsAsync(cancellationToken);


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

    public async Task<IReadOnlyList<ExternalActionClaim>> ClaimActionsAsync(ExternalBatchClaimRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "external-batch:" + _schema + ":" + request.Scope.Id + ":" + request.RequestId, cancellationToken);
        await using (var receipt = new NpgsqlCommand($"SELECT fingerprint FROM {_schema}.external_batch_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            receipt.Parameters.AddWithValue("scope", request.Scope.Id); receipt.Parameters.AddWithValue("id", request.RequestId);
            if (await receipt.ExecuteScalarAsync(cancellationToken) is string fingerprint)
            {
                if (fingerprint != request.Fingerprint) throw new InvalidOperationException("Batch claim request ID already has different semantics.");
                return await ReadPreviousExternalBatchAsync(connection, transaction, request, cancellationToken);
            }
        }
        var now = await PostgreSqlProtocol.NowAsync(connection, transaction, cancellationToken);
        var rows = new List<ExternalRow>();
        var selectedColumns = string.Join(",", ExternalColumns.Split(',').Select(c => "a." + c));
        await using (var select = new NpgsqlCommand($"""
            WITH candidates AS MATERIALIZED (
                SELECT id,next_at,octet_length(payload) AS bytes FROM {_schema}.external_actions
                WHERE scope_id=@scope AND binding_name=@binding AND binding_version=@binding_version
                  AND {ExternalDuePredicate(request.Phase)} AND next_at<=@now AND (lease_until IS NULL OR lease_until<=@now)
                ORDER BY next_at,id LIMIT @maximum FOR UPDATE SKIP LOCKED),
            bounded AS (SELECT id,sum(bytes::bigint) OVER(ORDER BY next_at,id ROWS UNBOUNDED PRECEDING) AS bytes FROM candidates)
            SELECT {selectedColumns} FROM {_schema}.external_actions a JOIN bounded b ON b.id=a.id
            WHERE a.scope_id=@scope AND b.bytes<=@bytes ORDER BY a.next_at,a.id
            """, connection, transaction))
        {
            select.Parameters.AddWithValue("scope", request.Scope.Id); select.Parameters.AddWithValue("binding", request.Binding);
            select.Parameters.AddWithValue("binding_version", request.BindingVersion); select.Parameters.AddWithValue("now", now);
            select.Parameters.AddWithValue("maximum", request.MaximumItems); select.Parameters.AddWithValue("bytes", request.MaximumPayloadBytes);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken)) rows.Add(ReadExternal(reader));
        }
        // Empty polling must not grow the durable receipt ledger. No action was acquired or recovered.
        if (rows.Count == 0) return Array.Empty<ExternalActionClaim>();
        await using (var receipt = new NpgsqlCommand($"INSERT INTO {_schema}.external_batch_requests(scope_id,id,fingerprint) VALUES(@scope,@id,@fingerprint)", connection, transaction))
        {
            receipt.Parameters.AddWithValue("scope", request.Scope.Id); receipt.Parameters.AddWithValue("id", request.RequestId);
            receipt.Parameters.AddWithValue("fingerprint", request.Fingerprint); await receipt.ExecuteNonQueryAsync(cancellationToken);
        }
        now = await PostgreSqlProtocol.NowAsync(connection, transaction, cancellationToken);
        var (claims, changes) = PrepareExternalClaims(rows, request, now);
        await WriteExternalBatchAsync(connection, transaction, request.Scope, request.RequestId, changes, now, cancellationToken);
        await PostgreSqlProtocol.CommitAsync(transaction, request.RequestId, cancellationToken);
        return claims.AsReadOnly();
    }

    private async Task<IReadOnlyList<ExternalActionClaim>> ReadPreviousExternalBatchAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, ExternalBatchClaimRequest request, CancellationToken ct)
    {
        var columns = string.Join(",", ExternalColumns.Split(',').Select(c => "a." + c));
        await using var command = new NpgsqlCommand($"""
            SELECT {columns} FROM {_schema}.external_actions a
            JOIN {_schema}.external_attempts e ON e.scope_id=a.scope_id AND e.action_id=a.id AND e.id=a.lease_token
            WHERE e.scope_id=@scope AND e.batch_request_id=@id AND a.lease_phase=@phase
              AND a.lease_until>clock_timestamp()
            ORDER BY a.next_at,a.id
            """, connection, transaction);
        command.Parameters.AddWithValue("scope", request.Scope.Id); command.Parameters.AddWithValue("id", request.RequestId);
        command.Parameters.AddWithValue("phase", (int)request.Phase);
        var claims = new List<ExternalActionClaim>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
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
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        var rows = new Dictionary<Guid, ExternalRow>();
        var attempts = new Dictionary<Guid, (Guid Action, ExternalPhase Phase, string? Fingerprint, bool Applied)>();
        await using (var select = new NpgsqlCommand($"""
            SELECT {ExternalColumns} FROM {_schema}.external_actions 
            WHERE scope_id=@scope AND id = ANY(@actions) ORDER BY id FOR UPDATE;
            SELECT id,action_id,phase,result_fingerprint,applied FROM {_schema}.external_attempts
            WHERE scope_id=@scope AND id = ANY(@tokens);
            """, connection, transaction))
        {
            select.Parameters.AddWithValue("scope", scope.Id);
            select.Parameters.AddWithValue("actions", request.Items.Select(i => i.Claim.Record.Action.Id).ToArray());
            select.Parameters.AddWithValue("tokens", request.Items.Select(i => i.Claim.Token).ToArray());
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            { var row = ReadExternal(reader); rows.Add(row.Record.Action.Id, row); }
            await reader.NextResultAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken)) attempts.Add(reader.GetGuid(0),
                (reader.GetGuid(1), (ExternalPhase)reader.GetInt32(2), reader.IsDBNull(3) ? null : reader.GetString(3), reader.GetBoolean(4)));
        }
        var now = await PostgreSqlProtocol.NowAsync(connection, transaction, cancellationToken);
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
        await PostgreSqlProtocol.CommitAsync(transaction, request.Items[0].Claim.Token, cancellationToken);
        return settlements.AsReadOnly();
    }

    private async Task WriteExternalBatchAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, ProducerScope scope,
        Guid? batchId, IReadOnlyList<ExternalBatchChange> changes, DateTimeOffset now, CancellationToken ct)
    {
        if (changes.Count == 0) return;
        using var table = ExternalChangesTable(changes);
        await using var command = new NpgsqlCommand { Connection = connection, Transaction = transaction };
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("now", now);
        PostgreSqlProtocol.Optional(command, "batch", NpgsqlDbType.Uuid, batchId);
        var input = AddExternalTable(command, table);
        command.CommandText = $"""
            WITH input AS MATERIALIZED ({input}), saved AS (
                UPDATE {_schema}.external_actions a SET state=x.state, observation=x.observation, version=x.version, submission_attempts=x.submission_attempts, next_at=x.next_at, observation_deadline=x.observation_deadline, provider_reference=x.provider_reference, last_reason=x.last_reason, result_data=x.result_data, lease_token=x.lease_token, lease_until=x.lease_until, lease_phase=x.lease_phase
                FROM input x WHERE a.scope_id=@scope AND a.id=x.action_id AND x.write_state RETURNING a.id),
            created AS (INSERT INTO {_schema}.external_attempts(scope_id,id,action_id,phase,batch_request_id)
                SELECT @scope,attempt_id,action_id,attempt_phase,@batch FROM input WHERE create_attempt RETURNING id)
            UPDATE {_schema}.external_attempts e SET finished_at=@now,result_kind=x.response_kind,reason=x.response_reason,
                provider_reference=x.response_reference,result_data=x.response_data,result_fingerprint=x.response_fingerprint,applied=x.applied
            FROM input x WHERE e.scope_id=@scope AND e.id=x.attempt_id AND x.finish_attempt
            """;
        await command.ExecuteNonQueryAsync(ct);
        await PublishExternalBatchUpdatesAsync(connection, transaction,
            changes.Where(c => c.Applied && !c.CreateAttempt && c.Target is not null).ToArray(), ct);
    }

    // Typed arrays avoid JSON/base64 expansion and keep the wire size proportional to the bounded page.
    internal static string AddExternalTable(NpgsqlCommand command, DataTable table)
    {
        var arguments = new List<string>(); var columns = new List<string>();
        foreach (DataColumn column in table.Columns)
        {
            var name = "v" + column.Ordinal;
            var type = column.DataType;
            var dbType = type == typeof(Guid) ? NpgsqlDbType.Uuid : type == typeof(int) ? NpgsqlDbType.Integer
                : type == typeof(long) ? NpgsqlDbType.Bigint : type == typeof(bool) ? NpgsqlDbType.Boolean
                : type == typeof(DateTimeOffset) ? NpgsqlDbType.TimestampTz : type == typeof(byte[]) ? NpgsqlDbType.Bytea : NpgsqlDbType.Text;
            var elementType = type.IsValueType ? typeof(Nullable<>).MakeGenericType(type) : type;
            var values = Array.CreateInstance(elementType, table.Rows.Count);
            for (var i = 0; i < table.Rows.Count; i++) values.SetValue(table.Rows[i].IsNull(column) ? null : table.Rows[i][column], i);
            command.Parameters.AddWithValue(name, NpgsqlDbType.Array | dbType, values);
            arguments.Add("@" + name); columns.Add(column.ColumnName);
        }
        return "SELECT * FROM unnest(" + string.Join(",", arguments) + ") AS x(" + string.Join(",", columns) + ")";
    }
}
