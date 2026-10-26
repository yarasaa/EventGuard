using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace EventGuard.PostgreSql;

internal sealed class PostgreSqlOutboxWriter(NpgsqlConnection connection, NpgsqlTransaction transaction,
    string schema, Guid? originWorkId, ProducerRequest? producerRequest) : IOutboxWriter
{
    public async Task<OutboxEnqueueStatus> EnqueueAsync(OutgoingMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        // Npgsql 8 keeps Connection after commit; IsolationLevel also checks completion/disposal.
        _ = transaction.IsolationLevel;
        if (!ReferenceEquals(transaction.Connection, connection)) throw new InvalidOperationException("The owning transaction is no longer active.");
        if (producerRequest is not null && message.Scope != producerRequest.Scope)
            throw new InvalidOperationException("Outgoing message must use the producer request scope.");
        if (originWorkId is Guid workId)
        {
            await using var owner = new NpgsqlCommand($"SELECT b.tenant FROM {schema}.works w JOIN {schema}.policy_bindings b ON b.scope_id=w.scope_id WHERE w.id=@id", connection, transaction);
            owner.Parameters.AddWithValue("id", workId);
            if (await owner.ExecuteScalarAsync(cancellationToken) is not string tenant || tenant != message.Scope.Tenant)
                throw new InvalidOperationException("Outgoing message must use the originating work tenant.");
        }
        var fingerprint = Fingerprint(message, originWorkId, producerRequest?.RequestId);
        await using var insert = new NpgsqlCommand($"""
            INSERT INTO {schema}.outbox_messages(scope_id,id,tenant,producer,message_id,transport,connection_name,
                configuration_version,address,routing_key,payload,content_type,headers,not_before,fingerprint,
                origin_work_id,origin_request_id,next_attempt_at)
            VALUES(@scope,@id,@tenant,@producer,@message,@transport,@connection,@config,@address,@routing,@payload,
                @content,@headers,@due,@fingerprint,@work,@request,COALESCE(@due,clock_timestamp()))
            ON CONFLICT(scope_id,id) DO NOTHING RETURNING id
            """, connection, transaction);
        insert.Parameters.AddWithValue("scope", message.Scope.Id);
        insert.Parameters.AddWithValue("id", message.IntentId);
        insert.Parameters.AddWithValue("tenant", message.Scope.Tenant);
        insert.Parameters.AddWithValue("producer", message.Scope.Producer);
        insert.Parameters.AddWithValue("message", message.MessageId);
        insert.Parameters.AddWithValue("transport", message.Route.Transport);
        insert.Parameters.AddWithValue("connection", message.Route.Connection);
        insert.Parameters.AddWithValue("config", message.Route.ConfigurationVersion);
        insert.Parameters.AddWithValue("address", message.Route.Address);
        PostgreSqlProtocol.Optional(insert, "routing", NpgsqlDbType.Text, message.Route.RoutingKey);
        insert.Parameters.AddWithValue("payload", message.CopyPayload());
        insert.Parameters.AddWithValue("content", message.ContentType);
        insert.Parameters.AddWithValue("headers", NpgsqlDbType.Jsonb, JsonSerializer.Serialize(message.Headers));
        PostgreSqlProtocol.Optional(insert, "due", NpgsqlDbType.TimestampTz, message.NotBefore);
        insert.Parameters.AddWithValue("fingerprint", fingerprint);
        PostgreSqlProtocol.Optional(insert, "work", NpgsqlDbType.Uuid, originWorkId);
        PostgreSqlProtocol.Optional(insert, "request", NpgsqlDbType.Uuid, producerRequest?.RequestId);
        if (await insert.ExecuteScalarAsync(cancellationToken) is Guid) return OutboxEnqueueStatus.Enqueued;
        await using var existing = new NpgsqlCommand($"SELECT fingerprint FROM {schema}.outbox_messages WHERE scope_id=@scope AND id=@id", connection, transaction);
        existing.Parameters.AddWithValue("scope", message.Scope.Id);
        existing.Parameters.AddWithValue("id", message.IntentId);
        if (await existing.ExecuteScalarAsync(cancellationToken) is not string stored || stored != fingerprint)
            throw new OutboxConflictException(message.IntentId);
        return OutboxEnqueueStatus.AlreadyEnqueued;
    }

    internal static string Fingerprint(OutgoingMessage message, Guid? work, Guid? request) =>
        OutboxRecovery.OriginFingerprint(message, work, request);
}
