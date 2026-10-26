using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlOutboxRecoveryStore
{
    public async Task<OutboxRepairResult> RepairAsync(OutboxRepairRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var message = request.Message;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction,
            "outbox-repair:" + _schema + ":" + message.Scope.Id + ":" + request.RequestId, cancellationToken);
        await using (var previous = new NpgsqlCommand($"SELECT fingerprint,intent_id,result_status,result_generation,result_version FROM {_schema}.outbox_repair_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            previous.Parameters.AddWithValue("scope", message.Scope.Id); previous.Parameters.AddWithValue("id", request.RequestId);
            await using var reader = await previous.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
                return OutboxRecovery.RepeatRepair(request, reader.GetString(0), new((OutboxRepairStatus)reader.GetInt32(2), reader.GetGuid(1), reader.GetInt32(3), reader.GetInt64(4)));
        }
        var current = await _repository.ReadAsync(connection, transaction, message.Scope.Id, message.IntentId, true, cancellationToken);
        string? original = null; Guid? work = null; Guid? producerRequest = null;
        if (current?.Metadata.State == OutboxState.Quarantined)
        {
            await using var query = new NpgsqlCommand($"SELECT left(fingerprint,65),origin_work_id,origin_request_id FROM {_schema}.outbox_messages WHERE scope_id=@scope AND id=@id", connection, transaction);
            query.Parameters.AddWithValue("scope", message.Scope.Id); query.Parameters.AddWithValue("id", message.IntentId);
            await using var reader = await query.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken)) throw new InvalidOperationException("Locked outbox row is missing.");
            original = reader.GetString(0); work = reader.IsDBNull(1) ? null : reader.GetGuid(1); producerRequest = reader.IsDBNull(2) ? null : reader.GetGuid(2);
        }
        var decision = OutboxRecovery.Repair(request, current?.Metadata, original, work, producerRequest);
        if (decision.After is not null)
            decision = TenantBudgets.AdmitRepair(decision, await PostgreSqlTenantBudgetProtocol.ConsumeAsync(connection, transaction,
                _schema, request.Message.Scope.Tenant, TenantBudgetKind.Recovery, cancellationToken));
        if (decision.After is not null)
        {
            await using var update = new NpgsqlCommand($"""
                UPDATE {_schema}.outbox_messages SET tenant=@tenant,producer=@producer,message_id=@message,
                    transport=@transport,connection_name=@connection,configuration_version=@config,address=@address,
                    routing_key=@routing,payload=@payload,content_type=@content,headers=@headers,not_before=@due
                WHERE scope_id=@scope AND id=@id AND state=5 AND version=@version AND quarantine_id=@quarantine
                """, connection, transaction);
            update.Parameters.AddWithValue("scope", message.Scope.Id); update.Parameters.AddWithValue("id", message.IntentId);
            update.Parameters.AddWithValue("version", current!.Metadata.Version); update.Parameters.AddWithValue("quarantine", request.QuarantineId);
            update.Parameters.AddWithValue("tenant", message.Scope.Tenant); update.Parameters.AddWithValue("producer", message.Scope.Producer);
            update.Parameters.AddWithValue("message", message.MessageId); update.Parameters.AddWithValue("transport", message.Route.Transport);
            update.Parameters.AddWithValue("connection", message.Route.Connection); update.Parameters.AddWithValue("config", message.Route.ConfigurationVersion);
            update.Parameters.AddWithValue("address", message.Route.Address); update.Parameters.AddWithValue("payload", message.CopyPayload());
            update.Parameters.AddWithValue("content", message.ContentType);
            update.Parameters.AddWithValue("headers", NpgsqlDbType.Jsonb, JsonSerializer.Serialize(message.Headers));
            PostgreSqlProtocol.Optional(update, "routing", NpgsqlDbType.Text, message.Route.RoutingKey);
            PostgreSqlProtocol.Optional(update, "due", NpgsqlDbType.TimestampTz, message.NotBefore);
            if (await update.ExecuteNonQueryAsync(cancellationToken) != 1) throw new InvalidOperationException("Quarantine changed under lock.");
            var now = await PostgreSqlProtocol.NowAsync(connection, transaction, cancellationToken);
            var due = message.NotBefore is { } scheduled && scheduled > now ? scheduled : now;
            await _repository.PersistAsync(connection, transaction, current, decision.After, null, due, cancellationToken);
        }
        var result = decision.Result;
        await using var receipt = new NpgsqlCommand($"""
            INSERT INTO {_schema}.outbox_repair_requests(scope_id,id,fingerprint,intent_id,quarantine_id,actor,reason,
                acknowledge_possible_duplicate,result_status,result_generation,result_version)
            VALUES(@scope,@id,@fingerprint,@intent,@quarantine,@actor,@reason,@ack,@status,@generation,@version)
            """, connection, transaction);
        receipt.Parameters.AddWithValue("scope", message.Scope.Id); receipt.Parameters.AddWithValue("id", request.RequestId);
        receipt.Parameters.AddWithValue("fingerprint", request.Fingerprint); receipt.Parameters.AddWithValue("intent", message.IntentId);
        receipt.Parameters.AddWithValue("quarantine", request.QuarantineId); receipt.Parameters.AddWithValue("actor", request.Actor);
        receipt.Parameters.AddWithValue("reason", request.Reason); receipt.Parameters.AddWithValue("ack", request.AcknowledgePossibleDuplicate);
        receipt.Parameters.AddWithValue("status", (int)result.Status); receipt.Parameters.AddWithValue("generation", result.Generation);
        receipt.Parameters.AddWithValue("version", result.Version);
        await receipt.ExecuteNonQueryAsync(cancellationToken);
        await PostgreSqlProtocol.CommitAsync(transaction, request.RequestId, cancellationToken);
        return result;
    }
}
