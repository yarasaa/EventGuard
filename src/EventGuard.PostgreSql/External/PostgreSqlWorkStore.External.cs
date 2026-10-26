using Npgsql;
using NpgsqlTypes;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore : IExternalActionStore
{
    public async Task InitializeExternalActionsAsync(CancellationToken cancellationToken = default)
    {
        await InitializeAsync(cancellationToken);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "external-schema:" + _schema, cancellationToken);
        await using var exists = new NpgsqlCommand("SELECT to_regclass(@name)::text", connection, transaction);
        exists.Parameters.AddWithValue("name", _schema + ".external_schema_version");
        if (await exists.ExecuteScalarAsync(cancellationToken) is not string)
        {
            await using var stream = typeof(PostgreSqlWorkStore).Assembly.GetManifestResourceStream(
                "EventGuard.PostgreSql.External.Migrations.001_external.sql") ?? throw new InvalidOperationException("External action migration missing.");
            using var reader = new StreamReader(stream);
            await using var create = new NpgsqlCommand((await reader.ReadToEndAsync(cancellationToken)).Replace("__SCHEMA__", _schema, StringComparison.Ordinal), connection, transaction);
            await create.ExecuteNonQueryAsync(cancellationToken);
        }
        await using (var current = new NpgsqlCommand($"SELECT version FROM {_schema}.external_schema_version WHERE singleton", connection, transaction))
        {
            var oldVersion = (int)(await current.ExecuteScalarAsync(cancellationToken))!;
            string[] migrations = ["002_result_handlers.sql", "003_call_limits.sql", "004_observation_notices.sql"];
            if (oldVersion < 1 || oldVersion > migrations.Length + 1) throw new InvalidOperationException("Unsupported external action schema version.");
            foreach (var name in migrations.Skip(oldVersion - 1))
            {
                await using var stream = typeof(PostgreSqlWorkStore).Assembly.GetManifestResourceStream(
                    "EventGuard.PostgreSql.External.Migrations." + name) ?? throw new InvalidOperationException("External migration missing.");
                using var reader = new StreamReader(stream);
                await using var migrate = new NpgsqlCommand((await reader.ReadToEndAsync(cancellationToken)).Replace("__SCHEMA__", _schema, StringComparison.Ordinal), connection, transaction);
                await migrate.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        await using var version = new NpgsqlCommand($"SELECT version FROM {_schema}.external_schema_version WHERE singleton", connection, transaction);
        if (await version.ExecuteScalarAsync(cancellationToken) is not int number || number != 4)
            throw new InvalidOperationException("Unsupported external action schema version.");
        await PostgreSqlProtocol.CommitAsync(transaction, Guid.NewGuid(), cancellationToken);
    }

    public async Task ValidateExternalActionsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand($"SELECT version FROM {_schema}.external_schema_version WHERE singleton", connection);
        if (await command.ExecuteScalarAsync(cancellationToken) is not int version || version != 4)
            throw new InvalidOperationException("Unsupported external action schema version. Run the explicit migration.");
    }

    private const string ExternalColumns = "tenant,producer,id,binding_name,binding_version,payload,observation_timeout,fingerprint,origin_work_id,origin_request_id,state,observation,version,submission_attempts,next_at,observation_deadline,provider_reference,last_reason,result_data,lease_token,lease_until,lease_phase,result_handler,result_handler_version";
    private sealed record ExternalRow(ExternalActionRecord Record, Guid? Token, DateTimeOffset? Until, ExternalPhase? Phase, ExternalResultTarget? Target);
    private static ExternalRow ReadExternal(NpgsqlDataReader reader)
    {
        var action = new ExternalAction(new(reader.GetString(0), reader.GetString(1)), reader.GetGuid(2), reader.GetString(3), reader.GetString(4),
            reader.GetFieldValue<byte[]>(5), reader.IsDBNull(6) ? null : TimeSpan.FromMilliseconds(reader.GetInt64(6)));
        var target = reader.IsDBNull(22) ? null : new ExternalResultTarget(action.Scope, reader.GetString(22), reader.GetString(23));
        var origin = PostgreSqlExternalActionWriter.Fingerprint(action, reader.IsDBNull(8) ? null : reader.GetGuid(8), reader.IsDBNull(9) ? null : reader.GetGuid(9), target);
        if (origin != reader.GetString(7)) throw new InvalidOperationException("External action immutable content is corrupt.");
        return new(new(action, (ExternalActionState)reader.GetInt32(10), (ExternalObservationState)reader.GetInt32(11), reader.GetInt64(12),
            reader.GetInt32(13), reader.GetFieldValue<DateTimeOffset>(14), reader.IsDBNull(15) ? null : reader.GetFieldValue<DateTimeOffset>(15),
            reader.IsDBNull(16) ? null : reader.GetString(16), reader.IsDBNull(17) ? null : reader.GetString(17), reader.GetFieldValue<byte[]>(18)),
            reader.IsDBNull(19) ? null : reader.GetGuid(19), reader.IsDBNull(20) ? null : reader.GetFieldValue<DateTimeOffset>(20),
            reader.IsDBNull(21) ? null : (ExternalPhase)reader.GetInt32(21), target);
    }

    private async Task<ExternalRow?> FindExternalAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, ProducerScope scope,
        Guid id, bool locked, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT {ExternalColumns} FROM {_schema}.external_actions WHERE scope_id=@scope AND id=@id" + (locked ? " FOR UPDATE" : ""), connection, transaction);
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? ReadExternal(reader) : null;
    }

    public async Task<ExternalActionRecord?> FindActionAsync(ProducerScope scope, Guid id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (id == Guid.Empty) throw new ArgumentException("Action ID is required.", nameof(id));
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return (await FindExternalAsync(connection, null, scope, id, false, cancellationToken))?.Record;
    }

    public async Task<ExternalActionClaim?> ClaimActionAsync(ProducerScope scope, string binding, string bindingVersion,
        ExternalPhase phase, Guid requestId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope); _ = new ProducerScope(binding, bindingVersion);
        if (!Enum.IsDefined(phase) || requestId == Guid.Empty) throw new ArgumentException("Valid phase and claim request ID are required.");
        if (leaseDuration < TimeSpan.FromMilliseconds(1) || leaseDuration > TimeSpan.FromHours(1)) throw new ArgumentOutOfRangeException(nameof(leaseDuration));
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "external-claim:" + _schema + ":" + scope.Id + ":" + requestId, cancellationToken);
        Guid? previous = null;
        await using (var receipt = new NpgsqlCommand($"SELECT action_id,phase FROM {_schema}.external_attempts WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            receipt.Parameters.AddWithValue("scope", scope.Id); receipt.Parameters.AddWithValue("id", requestId);
            await using var reader = await receipt.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                if (reader.GetInt32(1) != (int)phase) throw new InvalidOperationException("Claim request ID belongs to another phase.");
                previous = reader.GetGuid(0);
            }
        }
        if (previous is { } existing)
        {
            var prior = (await FindExternalAsync(connection, transaction, scope, existing, true, cancellationToken))!;
            if (prior.Record.Action.Binding != binding || prior.Record.Action.BindingVersion != bindingVersion)
                throw new InvalidOperationException("Claim request ID belongs to another adapter binding.");
            var clock = await PostgreSqlProtocol.NowAsync(connection, transaction, cancellationToken);
            return prior.Token == requestId && prior.Until > clock ? new(prior.Record, phase, requestId, prior.Until.Value) : null;
        }
        var now = await PostgreSqlProtocol.NowAsync(connection, transaction, cancellationToken);
        var predicate = phase switch
        {
            ExternalPhase.Submit => "(state=0 OR state=1)",
            ExternalPhase.Reconcile => "(state=4 OR state=1)",
            _ => "(state=2 AND observation=1)"
        };
        ExternalRow? row;
        await using (var select = new NpgsqlCommand($"SELECT {ExternalColumns} FROM {_schema}.external_actions WHERE scope_id=@scope AND binding_name=@binding AND binding_version=@binding_version AND {predicate} AND next_at<=@now AND (lease_until IS NULL OR lease_until<=@now) ORDER BY next_at,id LIMIT 1 FOR UPDATE SKIP LOCKED", connection, transaction))
        {
            select.Parameters.AddWithValue("scope", scope.Id); select.Parameters.AddWithValue("binding", binding);
            select.Parameters.AddWithValue("binding_version", bindingVersion); select.Parameters.AddWithValue("now", now);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            row = await reader.ReadAsync(cancellationToken) ? ReadExternal(reader) : null;
        }
        if (row is null) return null;
        now = await PostgreSqlProtocol.NowAsync(connection, transaction, cancellationToken);
        var before = row.Record;
        if (before.State == ExternalActionState.Sending)
        {
            // An expired sender may already have affected the service. Recovery never resends it.
            var recovered = before with { State = ExternalActionState.Unknown, Version = checked(before.Version + 1), LastReason = "submission-lease-expired", NextAttemptAt = now };
            await SaveExternalAsync(connection, transaction, recovered, null, null, null, cancellationToken);
            await PublishExternalUpdateAsync(connection, transaction, recovered, row.Target, ExternalPhase.Submit, null, cancellationToken);
            await PostgreSqlProtocol.CommitAsync(transaction, requestId, cancellationToken);
            return null;
        }
        if (phase == ExternalPhase.Observe && before.ObservationDeadline <= now)
        {
            var expired = before with { Observation = ExternalObservationState.TimedOut,
                Version = checked(before.Version + 1), LastReason = "observation-deadline-expired" };
            await SaveExternalAsync(connection, transaction, expired, null, null, null, cancellationToken);
            await PublishExternalUpdateAsync(connection, transaction, expired, row.Target, ExternalPhase.Observe, null, cancellationToken);
            await PostgreSqlProtocol.CommitAsync(transaction, requestId, cancellationToken);
            return null;
        }
        var claimed = before with { Version = checked(before.Version + 1), State = phase == ExternalPhase.Submit ? ExternalActionState.Sending : before.State,
            SubmissionAttempts = phase == ExternalPhase.Submit ? checked(before.SubmissionAttempts + 1) : before.SubmissionAttempts };
        var until = now + leaseDuration;
        await SaveExternalAsync(connection, transaction, claimed, requestId, until, phase, cancellationToken);
        await using var attempt = new NpgsqlCommand($"INSERT INTO {_schema}.external_attempts(scope_id,id,action_id,phase) VALUES(@scope,@id,@action,@phase)", connection, transaction);
        attempt.Parameters.AddWithValue("scope", scope.Id); attempt.Parameters.AddWithValue("id", requestId);
        attempt.Parameters.AddWithValue("action", claimed.Action.Id); attempt.Parameters.AddWithValue("phase", (int)phase);
        await attempt.ExecuteNonQueryAsync(cancellationToken);
        await PostgreSqlProtocol.CommitAsync(transaction, requestId, cancellationToken);
        return new(claimed, phase, requestId, until);
    }

    private async Task SaveExternalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, ExternalActionRecord record,
        Guid? token, DateTimeOffset? until, ExternalPhase? phase, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"""
            UPDATE {_schema}.external_actions SET state=@state,observation=@observation,version=@version,submission_attempts=@attempts,
            next_at=@next,observation_deadline=@deadline,provider_reference=@reference,last_reason=@reason,result_data=@data,
            lease_token=@token,lease_until=@until,lease_phase=@phase WHERE scope_id=@scope AND id=@id
            """, connection, transaction);
        command.Parameters.AddWithValue("scope", record.Action.Scope.Id); command.Parameters.AddWithValue("id", record.Action.Id);
        command.Parameters.AddWithValue("state", (int)record.State); command.Parameters.AddWithValue("observation", (int)record.Observation);
        command.Parameters.AddWithValue("version", record.Version); command.Parameters.AddWithValue("attempts", record.SubmissionAttempts);
        command.Parameters.AddWithValue("next", record.NextAttemptAt); command.Parameters.AddWithValue("data", record.ResultData);
        PostgreSqlProtocol.Optional(command, "deadline", NpgsqlDbType.TimestampTz, record.ObservationDeadline);
        PostgreSqlProtocol.Optional(command, "reference", NpgsqlDbType.Text, record.ProviderReference);
        PostgreSqlProtocol.Optional(command, "reason", NpgsqlDbType.Text, record.LastReason);
        PostgreSqlProtocol.Optional(command, "token", NpgsqlDbType.Uuid, token);
        PostgreSqlProtocol.Optional(command, "until", NpgsqlDbType.TimestampTz, until);
        PostgreSqlProtocol.Optional(command, "phase", NpgsqlDbType.Integer, phase is { } value ? (int)value : null);
        await command.ExecuteNonQueryAsync(ct);
    }

    public async Task<ExternalSettlement> SettleActionAsync(ExternalActionClaim claim, ExternalResult result,
        ExternalActionPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(claim); ArgumentNullException.ThrowIfNull(result); ArgumentNullException.ThrowIfNull(policy);
        var action = claim.Record.Action;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var row = await FindExternalAsync(connection, transaction, action.Scope, action.Id, true, cancellationToken)
            ?? throw new InvalidOperationException("External action not found.");
        if (row.Record.Action.Fingerprint != action.Fingerprint) throw new ExternalActionConflictException(action.Id);
        var digest = StableHash.Of("external/result/v1", ((int)result.Kind).ToString(System.Globalization.CultureInfo.InvariantCulture),
            result.Reason, result.Reference ?? "", ContentFingerprint.FromBytes(result.CopyData()).Value);
        await using (var attempt = new NpgsqlCommand($"SELECT phase,result_fingerprint,applied FROM {_schema}.external_attempts WHERE scope_id=@scope AND id=@id AND action_id=@action", connection, transaction))
        {
            attempt.Parameters.AddWithValue("scope", action.Scope.Id); attempt.Parameters.AddWithValue("id", claim.Token); attempt.Parameters.AddWithValue("action", action.Id);
            await using var reader = await attempt.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken) || reader.GetInt32(0) != (int)claim.Phase)
                throw new InvalidOperationException("Claim does not belong to the action and phase.");
            if (!reader.IsDBNull(1))
            {
                if (reader.GetString(1) != digest) throw new InvalidOperationException("This claim already has different result evidence.");
                return new(reader.GetBoolean(2), row.Record);
            }
        }
        var now = await PostgreSqlProtocol.NowAsync(connection, transaction, cancellationToken);
        // A late response is evidence, but cannot overwrite the current owner or a recovered action.
        var owned = row.Token == claim.Token && row.Phase == claim.Phase && row.Record.Version == claim.Record.Version && row.Until > now;
        var after = owned ? ExternalActionLifecycle.Settle(row.Record, claim.Phase, result, policy, now) : row.Record;
        await using (var evidence = new NpgsqlCommand($"""
            UPDATE {_schema}.external_attempts SET finished_at=@now,result_kind=@kind,reason=@reason,provider_reference=@reference,
                result_data=@data,result_fingerprint=@fingerprint,applied=@applied WHERE scope_id=@scope AND id=@id
            """, connection, transaction))
        {
            evidence.Parameters.AddWithValue("scope", action.Scope.Id); evidence.Parameters.AddWithValue("id", claim.Token);
            evidence.Parameters.AddWithValue("now", now); evidence.Parameters.AddWithValue("kind", (int)result.Kind);
            evidence.Parameters.AddWithValue("reason", result.Reason); evidence.Parameters.AddWithValue("data", result.CopyData());
            evidence.Parameters.AddWithValue("fingerprint", digest); evidence.Parameters.AddWithValue("applied", owned);
            PostgreSqlProtocol.Optional(evidence, "reference", NpgsqlDbType.Text, result.Reference);
            await evidence.ExecuteNonQueryAsync(cancellationToken);
        }
        if (owned)
        {
            await SaveExternalAsync(connection, transaction, after, null, null, null, cancellationToken);
            await PublishExternalUpdateAsync(connection, transaction, after, row.Target, claim.Phase, result, cancellationToken);
        }
        await PostgreSqlProtocol.CommitAsync(transaction, claim.Token, cancellationToken);
        return new(owned, after);
    }

    public async Task<IReadOnlyList<ExternalAttemptEvidence>> ReadActionEvidenceAsync(ProducerScope scope, Guid actionId,
        int limit = 100, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (actionId == Guid.Empty || limit is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(limit));
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand($"SELECT id,phase,started_at,finished_at,result_kind,reason,provider_reference,result_data,applied FROM {_schema}.external_attempts WHERE scope_id=@scope AND action_id=@id ORDER BY started_at DESC,id DESC LIMIT @limit", connection);
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", actionId); command.Parameters.AddWithValue("limit", limit);
        var records = new List<ExternalAttemptEvidence>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken)) records.Add(new(reader.GetGuid(0), (ExternalPhase)reader.GetInt32(1), reader.GetFieldValue<DateTimeOffset>(2),
            reader.IsDBNull(3) ? null : reader.GetFieldValue<DateTimeOffset>(3), reader.IsDBNull(4) ? null : new((ExternalResultKind)reader.GetInt32(4),
                reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetString(6), reader.GetFieldValue<byte[]>(7)), reader.GetBoolean(8)));
        return records;
    }
}
