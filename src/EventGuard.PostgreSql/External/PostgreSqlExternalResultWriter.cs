using Npgsql;

namespace EventGuard.PostgreSql;

internal sealed class PostgreSqlExternalResultWriter(NpgsqlConnection connection, NpgsqlTransaction transaction,
    string schema, Guid? originWorkId) : IExternalResultWriter
{
    public async Task<ExternalResultApplyStatus> ApplyAsync(ExternalResultTarget target,
        Func<ExternalActionUpdate, CancellationToken, ValueTask> handler, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(target); ArgumentNullException.ThrowIfNull(handler);
        _ = transaction.IsolationLevel;
        if (originWorkId is not Guid work || !ReferenceEquals(transaction.Connection, connection))
            throw new InvalidOperationException("Result handling requires its active work transaction.");
        ExternalActionUpdate update;
        await using (var command = new NpgsqlCommand($"""
            SELECT a.tenant,a.producer,a.id,a.binding_name,a.binding_version,a.payload,a.observation_timeout,
                a.fingerprint,a.origin_work_id,a.origin_request_id,a.result_handler,a.result_handler_version,
                u.version,u.state,u.observation,u.phase,u.provider_reference,u.reason,u.state_data,
                u.response_kind,u.response_reason,u.response_reference,u.response_data,w.fingerprint,w.scope_id
            FROM {schema}.external_result_updates u
            JOIN {schema}.external_actions a ON a.scope_id=u.scope_id AND a.id=u.action_id
            JOIN {schema}.works w ON w.id=u.work_id
            WHERE u.work_id=@work AND u.handler_scope_id=@handler AND u.scope_id=@scope
            """, connection, transaction))
        {
            command.Parameters.AddWithValue("work", work); command.Parameters.AddWithValue("handler", target.WorkScope.Id);
            command.Parameters.AddWithValue("scope", target.Scope.Id);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken)) throw new InvalidOperationException("Work does not belong to this result handler.");
            var action = new ExternalAction(new(reader.GetString(0), reader.GetString(1)), reader.GetGuid(2), reader.GetString(3), reader.GetString(4),
                reader.GetFieldValue<byte[]>(5), reader.IsDBNull(6) ? null : TimeSpan.FromMilliseconds(reader.GetInt64(6)));
            var savedTarget = new ExternalResultTarget(action.Scope, reader.GetString(10), reader.GetString(11));
            var fingerprint = PostgreSqlExternalActionWriter.Fingerprint(action, reader.IsDBNull(8) ? null : reader.GetGuid(8),
                reader.IsDBNull(9) ? null : reader.GetGuid(9), savedTarget);
            if (fingerprint != reader.GetString(7) || savedTarget.Id != target.Id || reader.GetString(24) != target.WorkScope.Id)
                throw new InvalidOperationException("External result binding or source content is corrupt.");
            ExternalResult? response = reader.IsDBNull(19) ? null : new((ExternalResultKind)reader.GetInt32(19), reader.GetString(20),
                reader.IsDBNull(21) ? null : reader.GetString(21), reader.GetFieldValue<byte[]>(22));
            update = new(action, reader.GetInt64(12), (ExternalActionState)reader.GetInt32(13), (ExternalObservationState)reader.GetInt32(14),
                (ExternalPhase)reader.GetInt32(15), reader.IsDBNull(16) ? null : reader.GetString(16), reader.IsDBNull(17) ? null : reader.GetString(17),
                reader.GetFieldValue<byte[]>(18), response);
            if (update.Fingerprint != reader.GetString(23)) throw new InvalidOperationException("External result snapshot is corrupt.");
        }
        // Each logical action has a monotonic application projection. Workers can process different actions concurrently.
        await PostgreSqlProtocol.LockAsync(connection, transaction, "external-result-position:" + schema + ":" + target.Id + ":" + update.Action.Id, cancellationToken);
        await using (var position = new NpgsqlCommand($"SELECT version FROM {schema}.external_result_positions WHERE scope_id=@scope AND action_id=@action AND handler_scope_id=@handler", connection, transaction))
        {
            position.Parameters.AddWithValue("scope", target.Scope.Id); position.Parameters.AddWithValue("action", update.Action.Id);
            position.Parameters.AddWithValue("handler", target.WorkScope.Id);
            if (await position.ExecuteScalarAsync(cancellationToken) is long applied && applied >= update.Version)
                return ExternalResultApplyStatus.AlreadyApplied;
        }
        var savepoint = "er" + Guid.NewGuid().ToString("N")[..30];
        await transaction.SaveAsync(savepoint, cancellationToken);
        try
        {
            await handler(update, cancellationToken);
            await using var advance = new NpgsqlCommand($"""
                INSERT INTO {schema}.external_result_positions(scope_id,action_id,handler_scope_id,version) VALUES(@scope,@action,@handler,@version)
                ON CONFLICT(scope_id,action_id,handler_scope_id) DO UPDATE SET version=EXCLUDED.version
                """, connection, transaction);
            advance.Parameters.AddWithValue("scope", target.Scope.Id); advance.Parameters.AddWithValue("action", update.Action.Id);
            advance.Parameters.AddWithValue("handler", target.WorkScope.Id); advance.Parameters.AddWithValue("version", update.Version);
            await advance.ExecuteNonQueryAsync(cancellationToken);
            await transaction.ReleaseAsync(savepoint, cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(savepoint, CancellationToken.None);
            throw;
        }
        return ExternalResultApplyStatus.Applied;
    }
}
