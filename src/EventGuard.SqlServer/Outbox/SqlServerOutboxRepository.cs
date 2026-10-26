using System.Text.Json;
using Microsoft.Data.SqlClient;
using System.Data;

namespace EventGuard.SqlServer;

// Shared native persistence for publishing and recovery. Callers own locks and transactions.
internal sealed class SqlServerOutboxRepository(string schema)
{
    internal const string MetadataColumns = "id,scope_id,state,version,attempt_count,generation_attempt,replay_generation,delivery_uncertain,left(last_reason,128),quarantine_id,lease_token,lease_until";
    internal const string Columns = MetadataColumns + ",left(tenant,1025),left(producer,1025),message_id,left(transport,1025),left(connection_name,1025),left(configuration_version,1025),left(address,1025),left(routing_key,1025),CASE WHEN DATALENGTH(payload)<=1048576 THEN payload ELSE NULL END,left(content_type,1025),CASE WHEN DATALENGTH(headers)<=1048576 THEN headers ELSE NULL END,not_before,fingerprint,origin_work_id,origin_request_id,next_attempt_at,payload_purged_at";

    internal async Task<SqlServerOutboxSnapshot?> ReadAsync(SqlConnection connection, SqlTransaction? transaction,
        string scope, Guid id, bool locked, CancellationToken ct)
    {
        await using var command = new SqlCommand($"SELECT {Columns} FROM {schema}.outbox_messages" + (locked ? " WITH(XLOCK,ROWLOCK)" : "") + " WHERE scope_id=@scope AND id=@id", connection, transaction);
        command.Parameters.AddValue("scope", scope);
        command.Parameters.AddValue("id", id);
        await using var reader = await command.ExecuteReaderCheckedAsync(ct);
        return await reader.ReadCheckedAsync(ct) ? Read(reader) : null;
    }

    internal async Task<OutboxMetadata?> ReadMetadataAsync(SqlConnection connection, ProducerScope scope, Guid id, CancellationToken ct)
    {
        await using var command = new SqlCommand($"SELECT {MetadataColumns} FROM {schema}.outbox_messages WHERE scope_id=@scope AND id=@id", connection);
        command.Parameters.AddValue("scope", scope.Id);
        command.Parameters.AddValue("id", id);
        await using var reader = await command.ExecuteReaderCheckedAsync(ct);
        return await reader.ReadCheckedAsync(ct) ? ReadMetadata(reader) : null;
    }

    internal static SqlServerOutboxSnapshot Read(SqlDataReader reader)
    {
        var metadata = ReadMetadata(reader);
        WorkLease? lease = reader.IsDBNull(10) ? null : new(reader.GetGuid(10), reader.GetFieldValue<DateTimeOffset>(11));
        if (!reader.IsDBNull(28)) return new(metadata, lease, null, "outbox-data-purged");
        if (metadata.State == OutboxState.Quarantined) return new(metadata, lease, null, "outbox-quarantined");
        if (reader.IsDBNull(20) || reader.IsDBNull(22)) return new(metadata, lease, null, "outbox-envelope-too-large");
        try
        {
            var headers = JsonSerializer.Deserialize<Dictionary<string, string>>(reader.GetString(22))
                ?? throw new JsonException("An object is required.");
            var message = new OutgoingMessage(new(reader.GetString(12), reader.GetString(13)), metadata.IntentId, reader.GetGuid(14),
                new(reader.GetString(15), reader.GetString(16), reader.GetString(17), reader.GetString(18), Text(reader, 19)),
                reader.GetFieldValue<byte[]>(20), reader.GetString(21), headers,
                reader.IsDBNull(23) ? null : reader.GetFieldValue<DateTimeOffset>(23));
            Guid? work = reader.IsDBNull(25) ? null : reader.GetGuid(25);
            Guid? request = reader.IsDBNull(26) ? null : reader.GetGuid(26);
            if (message.Scope.Id != metadata.ScopeId) return new(metadata, lease, null, "outbox-scope-mismatch");
            if (SqlServerOutboxWriter.Fingerprint(message, work, request) != reader.GetString(24))
                return new(metadata, lease, null, "outbox-fingerprint-mismatch");
            var record = new OutboxRecord(message, metadata.State, metadata.Version, metadata.AttemptCount,
                reader.GetFieldValue<DateTimeOffset>(27), metadata.DeliveryUncertain, metadata.LastReason, work, request)
            { GenerationAttempt = metadata.GenerationAttempt, ReplayGeneration = metadata.ReplayGeneration };
            return new(metadata, lease, record, null);
        }
        catch (Exception error) when (error is ArgumentException or JsonException or InvalidCastException)
        {
            // Only local envelope decoding errors qualify. DB/network/cancellation errors propagate.
            return new(metadata, lease, null, "outbox-envelope-invalid");
        }
    }

    internal static OutboxMetadata Metadata(OutboxRecord record) => new(record.Message.IntentId, record.Message.Scope.Id,
        record.State, record.Version, record.AttemptCount, record.GenerationAttempt, record.ReplayGeneration,
        record.DeliveryUncertain, record.LastReason, null);

    private static OutboxMetadata ReadMetadata(SqlDataReader reader) => new(reader.GetGuid(0), reader.GetString(1),
        (OutboxState)reader.GetInt32(2), reader.GetInt64(3), reader.GetInt32(4), reader.GetInt32(5), reader.GetInt32(6),
        reader.GetBoolean(7), Text(reader, 8), reader.IsDBNull(9) ? null : reader.GetGuid(9));

    internal async Task PersistAsync(SqlConnection connection, SqlTransaction transaction, SqlServerOutboxSnapshot before,
        OutboxMetadata after, WorkLease? lease, DateTimeOffset nextAttemptAt, CancellationToken ct)
    {
        await using var command = new SqlCommand($"""
            UPDATE {schema}.outbox_messages SET state=@state,version=@version,attempt_count=@attempts,
                generation_attempt=@generation_attempt,replay_generation=@generation,quarantine_id=@quarantine,
                next_attempt_at=@next,lease_token=@token,lease_until=@until,delivery_uncertain=@uncertain,last_reason=@reason,
                published_at=CASE WHEN @state=3 THEN TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00') ELSE published_at END
            WHERE scope_id=@scope AND id=@id AND state=@old_state AND version=@old_version
                AND (lease_token=@old_token OR (lease_token IS NULL AND @old_token IS NULL))
            """, connection, transaction);
        command.Parameters.AddValue("scope", before.Metadata.ScopeId);
        command.Parameters.AddValue("id", before.Metadata.IntentId);
        command.Parameters.AddValue("state", (int)after.State);
        command.Parameters.AddValue("version", after.Version);
        command.Parameters.AddValue("attempts", after.AttemptCount);
        command.Parameters.AddValue("generation_attempt", after.GenerationAttempt);
        command.Parameters.AddValue("generation", after.ReplayGeneration);
        command.Parameters.AddValue("next", nextAttemptAt);
        command.Parameters.AddValue("uncertain", after.DeliveryUncertain);
        command.Parameters.AddValue("old_state", (int)before.Metadata.State);
        command.Parameters.AddValue("old_version", before.Metadata.Version);
        SqlServerProtocol.Optional(command, "quarantine", SqlDbType.UniqueIdentifier, after.QuarantineId);
        SqlServerProtocol.Optional(command, "token", SqlDbType.UniqueIdentifier, lease?.Token);
        SqlServerProtocol.Optional(command, "until", SqlDbType.DateTimeOffset, lease?.Until);
        SqlServerProtocol.Optional(command, "old_token", SqlDbType.UniqueIdentifier, before.Lease?.Token);
        SqlServerProtocol.Optional(command, "reason", SqlDbType.NVarChar, after.LastReason);
        if (await command.ExecuteNonQueryCheckedAsync(ct) != 1) throw new InvalidOperationException("Outbox ownership changed under lock.");
    }

    internal async Task FinishAttemptAsync(SqlConnection connection, SqlTransaction transaction, string scope,
        Guid token, string status, string reason, CancellationToken ct)
    {
        await using var command = new SqlCommand($"UPDATE {schema}.outbox_attempts SET status=@status,reason=@reason,finished_at=TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00') WHERE scope_id=@scope AND id=@id AND status='Started'", connection, transaction);
        command.Parameters.AddValue("scope", scope);
        command.Parameters.AddValue("id", token);
        command.Parameters.AddValue("status", status);
        command.Parameters.AddValue("reason", reason);
        if (await command.ExecuteNonQueryCheckedAsync(ct) != 1) throw new InvalidOperationException("Outbox attempt was not active.");
    }

    internal async Task<OutboxMetadata> QuarantineAsync(SqlConnection connection, SqlTransaction transaction,
        SqlServerOutboxSnapshot before, CancellationToken ct)
    {
        var reason = before.EnvelopeError ?? throw new InvalidOperationException("Envelope corruption is required.");
        var after = OutboxRecovery.Quarantine(before.Metadata, Guid.NewGuid(), reason);
        // Preserve the original DB representation server-side; raw content is never returned or logged.
        await using (var snapshot = new SqlCommand($"""
            INSERT INTO {schema}.outbox_quarantine(scope_id,id,intent_id,observed_version,reason,original_record)
            SELECT scope_id,@quarantine,id,version,@reason,(SELECT original.* FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER)
            FROM {schema}.outbox_messages original WHERE scope_id=@scope AND id=@id AND version=@version
            """, connection, transaction))
        {
            snapshot.Parameters.AddValue("quarantine", after.QuarantineId!.Value);
            snapshot.Parameters.AddValue("reason", reason);
            snapshot.Parameters.AddValue("scope", before.Metadata.ScopeId);
            snapshot.Parameters.AddValue("id", before.Metadata.IntentId);
            snapshot.Parameters.AddValue("version", before.Metadata.Version);
            if (await snapshot.ExecuteNonQueryCheckedAsync(ct) != 1) throw new InvalidOperationException("Quarantine snapshot was not persisted.");
        }
        if (before.Lease is not null)
            await FinishAttemptAsync(connection, transaction, before.Metadata.ScopeId, before.Lease.Token, "Unknown", reason, ct);
        var now = await SqlServerProtocol.NowAsync(connection, transaction, ct);
        await PersistAsync(connection, transaction, before, after, null, now, ct);
        return after;
    }

    private static string? Text(SqlDataReader reader, int index) => reader.IsDBNull(index) ? null : reader.GetString(index);
}

internal sealed record SqlServerOutboxSnapshot(OutboxMetadata Metadata, WorkLease? Lease, OutboxRecord? Record, string? EnvelopeError)
{
    internal OutboxRecord RequireRecord() => Record ?? (EnvelopeError == "outbox-data-purged"
        ? throw new PayloadUnavailableException("outbox", Metadata.IntentId)
        : throw new OutboxEnvelopeException(Metadata.IntentId, EnvelopeError!));
}
