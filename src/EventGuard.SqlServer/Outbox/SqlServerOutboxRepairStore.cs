using System.Text.Json;
using Microsoft.Data.SqlClient;
using System.Data;
namespace EventGuard.SqlServer;

public sealed partial class SqlServerOutboxRecoveryStore
{
    public async Task<OutboxRepairResult> RepairAsync(OutboxRepairRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var message = request.Message;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction,
            "outbox-repair:" + _schema + ":" + message.Scope.Id + ":" + request.RequestId, cancellationToken);
        await using (var previous = new SqlCommand($"SELECT fingerprint,intent_id,result_status,result_generation,result_version FROM {_schema}.outbox_repair_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            previous.Parameters.AddValue("scope", message.Scope.Id); previous.Parameters.AddValue("id", request.RequestId);
            await using var reader = await previous.ExecuteReaderCheckedAsync(cancellationToken);
            if (await reader.ReadCheckedAsync(cancellationToken))
                return OutboxRecovery.RepeatRepair(request, reader.GetString(0), new((OutboxRepairStatus)reader.GetInt32(2), reader.GetGuid(1), reader.GetInt32(3), reader.GetInt64(4)));
        }
        var current = await _repository.ReadAsync(connection, transaction, message.Scope.Id, message.IntentId, true, cancellationToken);
        string? original = null; Guid? work = null; Guid? producerRequest = null;
        if (current?.Metadata.State == OutboxState.Quarantined)
        {
            await using var query = new SqlCommand($"SELECT left(fingerprint,65),origin_work_id,origin_request_id FROM {_schema}.outbox_messages WHERE scope_id=@scope AND id=@id", connection, transaction);
            query.Parameters.AddValue("scope", message.Scope.Id); query.Parameters.AddValue("id", message.IntentId);
            await using var reader = await query.ExecuteReaderCheckedAsync(cancellationToken);
            if (!await reader.ReadCheckedAsync(cancellationToken)) throw new InvalidOperationException("Locked outbox row is missing.");
            original = reader.GetString(0); work = reader.IsDBNull(1) ? null : reader.GetGuid(1); producerRequest = reader.IsDBNull(2) ? null : reader.GetGuid(2);
        }
        var decision = OutboxRecovery.Repair(request, current?.Metadata, original, work, producerRequest);
        if (decision.After is not null)
            decision = TenantBudgets.AdmitRepair(decision, await SqlServerTenantBudgetProtocol.ConsumeAsync(connection, transaction,
                _schema, request.Message.Scope.Tenant, TenantBudgetKind.Recovery, cancellationToken));
        if (decision.After is not null)
        {
            await using var update = new SqlCommand($"""
                UPDATE {_schema}.outbox_messages SET tenant=@tenant,producer=@producer,message_id=@message,
                    transport=@transport,connection_name=@connection,configuration_version=@config,address=@address,
                    routing_key=@routing,payload=@payload,content_type=@content,headers=@headers,not_before=@due
                WHERE scope_id=@scope AND id=@id AND state=5 AND version=@version AND quarantine_id=@quarantine
                """, connection, transaction);
            update.Parameters.AddValue("scope", message.Scope.Id); update.Parameters.AddValue("id", message.IntentId);
            update.Parameters.AddValue("version", current!.Metadata.Version); update.Parameters.AddValue("quarantine", request.QuarantineId);
            update.Parameters.AddValue("tenant", message.Scope.Tenant); update.Parameters.AddValue("producer", message.Scope.Producer);
            update.Parameters.AddValue("message", message.MessageId); update.Parameters.AddValue("transport", message.Route.Transport);
            update.Parameters.AddValue("connection", message.Route.Connection); update.Parameters.AddValue("config", message.Route.ConfigurationVersion);
            update.Parameters.AddValue("address", message.Route.Address); update.Parameters.AddValue("payload", message.CopyPayload());
            update.Parameters.AddValue("content", message.ContentType);
            update.Parameters.AddValue("headers", JsonSerializer.Serialize(message.Headers));
            SqlServerProtocol.Optional(update, "routing", SqlDbType.NVarChar, message.Route.RoutingKey);
            SqlServerProtocol.Optional(update, "due", SqlDbType.DateTimeOffset, message.NotBefore);
            if (await update.ExecuteNonQueryCheckedAsync(cancellationToken) != 1) throw new InvalidOperationException("Quarantine changed under lock.");
            var now = await SqlServerProtocol.NowAsync(connection, transaction, cancellationToken);
            var due = message.NotBefore is { } scheduled && scheduled > now ? scheduled : now;
            await _repository.PersistAsync(connection, transaction, current, decision.After, null, due, cancellationToken);
        }
        var result = decision.Result;
        await using var receipt = new SqlCommand($"""
            INSERT INTO {_schema}.outbox_repair_requests(scope_id,id,fingerprint,intent_id,quarantine_id,actor,reason,
                acknowledge_possible_duplicate,result_status,result_generation,result_version)
            VALUES(@scope,@id,@fingerprint,@intent,@quarantine,@actor,@reason,@ack,@status,@generation,@version)
            """, connection, transaction);
        receipt.Parameters.AddValue("scope", message.Scope.Id); receipt.Parameters.AddValue("id", request.RequestId);
        receipt.Parameters.AddValue("fingerprint", request.Fingerprint); receipt.Parameters.AddValue("intent", message.IntentId);
        receipt.Parameters.AddValue("quarantine", request.QuarantineId); receipt.Parameters.AddValue("actor", request.Actor);
        receipt.Parameters.AddValue("reason", request.Reason); receipt.Parameters.AddValue("ack", request.AcknowledgePossibleDuplicate);
        receipt.Parameters.AddValue("status", (int)result.Status); receipt.Parameters.AddValue("generation", result.Generation);
        receipt.Parameters.AddValue("version", result.Version);
        await receipt.ExecuteNonQueryCheckedAsync(cancellationToken);
        await SqlServerProtocol.CommitAsync(transaction, request.RequestId, cancellationToken);
        return result;
    }
}
