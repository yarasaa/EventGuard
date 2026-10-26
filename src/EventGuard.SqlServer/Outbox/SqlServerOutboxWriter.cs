using System.Text.Json;
using Microsoft.Data.SqlClient;
using System.Data;

namespace EventGuard.SqlServer;

internal sealed class SqlServerOutboxWriter(SqlConnection connection, SqlTransaction transaction,
    string schema, Guid? originWorkId, ProducerRequest? producerRequest) : IOutboxWriter
{
    public async Task<OutboxEnqueueStatus> EnqueueAsync(OutgoingMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (!ReferenceEquals(transaction.Connection, connection)) throw new InvalidOperationException("The owning transaction is no longer active.");
        if (producerRequest is not null && message.Scope != producerRequest.Scope)
            throw new InvalidOperationException("Outgoing message must use the producer request scope.");
        if (originWorkId is Guid workId)
        {
            await using var owner = new SqlCommand($"SELECT b.tenant FROM {schema}.works w JOIN {schema}.policy_bindings b ON b.scope_id=w.scope_id WHERE w.id=@id", connection, transaction);
            owner.Parameters.AddValue("id", workId);
            if (await owner.ExecuteScalarCheckedAsync(cancellationToken) is not string tenant || tenant != message.Scope.Tenant)
                throw new InvalidOperationException("Outgoing message must use the originating work tenant.");
        }
        var fingerprint = Fingerprint(message, originWorkId, producerRequest?.RequestId);
        await SqlServerProtocol.LockAsync(connection, transaction, "outbox-intent:" + schema + ":" + message.Scope.Id + ":" + message.IntentId, cancellationToken);
        await using var insert = new SqlCommand($"""
            IF NOT EXISTS(SELECT 1 FROM {schema}.outbox_messages WHERE scope_id=@scope AND id=@id)
            INSERT INTO {schema}.outbox_messages(scope_id,id,tenant,producer,message_id,transport,connection_name,
                configuration_version,address,routing_key,payload,content_type,headers,not_before,fingerprint,
                origin_work_id,origin_request_id,next_attempt_at)
            OUTPUT INSERTED.id
            VALUES(@scope,@id,@tenant,@producer,@message,@transport,@connection,@config,@address,@routing,@payload,
                @content,@headers,@due,@fingerprint,@work,@request,COALESCE(@due,TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00')))
            """, connection, transaction);
        insert.Parameters.AddValue("scope", message.Scope.Id);
        insert.Parameters.AddValue("id", message.IntentId);
        insert.Parameters.AddValue("tenant", message.Scope.Tenant);
        insert.Parameters.AddValue("producer", message.Scope.Producer);
        insert.Parameters.AddValue("message", message.MessageId);
        insert.Parameters.AddValue("transport", message.Route.Transport);
        insert.Parameters.AddValue("connection", message.Route.Connection);
        insert.Parameters.AddValue("config", message.Route.ConfigurationVersion);
        insert.Parameters.AddValue("address", message.Route.Address);
        SqlServerProtocol.Optional(insert, "routing", SqlDbType.NVarChar, message.Route.RoutingKey);
        insert.Parameters.AddValue("payload", message.CopyPayload());
        insert.Parameters.AddValue("content", message.ContentType);
        insert.Parameters.AddValue("headers", JsonSerializer.Serialize(message.Headers));
        SqlServerProtocol.Optional(insert, "due", SqlDbType.DateTimeOffset, message.NotBefore);
        insert.Parameters.AddValue("fingerprint", fingerprint);
        SqlServerProtocol.Optional(insert, "work", SqlDbType.UniqueIdentifier, originWorkId);
        SqlServerProtocol.Optional(insert, "request", SqlDbType.UniqueIdentifier, producerRequest?.RequestId);
        if (await insert.ExecuteScalarCheckedAsync(cancellationToken) is Guid) return OutboxEnqueueStatus.Enqueued;
        await using var existing = new SqlCommand($"SELECT fingerprint FROM {schema}.outbox_messages WHERE scope_id=@scope AND id=@id", connection, transaction);
        existing.Parameters.AddValue("scope", message.Scope.Id);
        existing.Parameters.AddValue("id", message.IntentId);
        if (await existing.ExecuteScalarCheckedAsync(cancellationToken) is not string stored || stored != fingerprint)
            throw new OutboxConflictException(message.IntentId);
        return OutboxEnqueueStatus.AlreadyEnqueued;
    }

    internal static string Fingerprint(OutgoingMessage message, Guid? work, Guid? request) =>
        OutboxRecovery.OriginFingerprint(message, work, request);
}
