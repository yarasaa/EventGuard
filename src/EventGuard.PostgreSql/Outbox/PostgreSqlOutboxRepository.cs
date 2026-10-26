using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace EventGuard.PostgreSql;

// Shared native persistence for publishing and recovery. Callers own locks and transactions.
internal sealed class PostgreSqlOutboxRepository(string schema)
{
    internal const string MetadataColumns = "id,scope_id,state,version,attempt_count,generation_attempt,replay_generation,delivery_uncertain,left(last_reason,128),quarantine_id,lease_token,lease_until";
    internal const string Columns = MetadataColumns + ",left(tenant,1025),left(producer,1025),message_id,left(transport,1025),left(connection_name,1025),left(configuration_version,1025),left(address,1025),left(routing_key,1025),CASE WHEN octet_length(payload)<=1048576 THEN payload ELSE NULL END,left(content_type,1025),CASE WHEN octet_length(headers::text)<=524288 THEN headers ELSE NULL END,not_before,fingerprint,origin_work_id,origin_request_id,next_attempt_at,payload_purged_at";

    internal async Task<PostgreSqlOutboxSnapshot?> ReadAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction,
        string scope, Guid id, bool locked, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT {Columns} FROM {schema}.outbox_messages WHERE scope_id=@scope AND id=@id" + (locked ? " FOR UPDATE" : ""), connection, transaction);
        command.Parameters.AddWithValue("scope", scope);
        command.Parameters.AddWithValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? Read(reader) : null;
    }

    internal async Task<OutboxMetadata?> ReadMetadataAsync(NpgsqlConnection connection, ProducerScope scope, Guid id, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT {MetadataColumns} FROM {schema}.outbox_messages WHERE scope_id=@scope AND id=@id", connection);
        command.Parameters.AddWithValue("scope", scope.Id);
        command.Parameters.AddWithValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? ReadMetadata(reader) : null;
    }

    internal static PostgreSqlOutboxSnapshot Read(NpgsqlDataReader reader)
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
            if (PostgreSqlOutboxWriter.Fingerprint(message, work, request) != reader.GetString(24))
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

    private static OutboxMetadata ReadMetadata(NpgsqlDataReader reader) => new(reader.GetGuid(0), reader.GetString(1),
        (OutboxState)reader.GetInt32(2), reader.GetInt64(3), reader.GetInt32(4), reader.GetInt32(5), reader.GetInt32(6),
        reader.GetBoolean(7), Text(reader, 8), reader.IsDBNull(9) ? null : reader.GetGuid(9));

    internal async Task PersistAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, PostgreSqlOutboxSnapshot before,
        OutboxMetadata after, WorkLease? lease, DateTimeOffset nextAttemptAt, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"""
            UPDATE {schema}.outbox_messages SET state=@state,version=@version,attempt_count=@attempts,
                generation_attempt=@generation_attempt,replay_generation=@generation,quarantine_id=@quarantine,
                next_attempt_at=@next,lease_token=@token,lease_until=@until,delivery_uncertain=@uncertain,last_reason=@reason,
                published_at=CASE WHEN @state=3 THEN clock_timestamp() ELSE published_at END
            WHERE scope_id=@scope AND id=@id AND state=@old_state AND version=@old_version
                AND lease_token IS NOT DISTINCT FROM @old_token
            """, connection, transaction);
        command.Parameters.AddWithValue("scope", before.Metadata.ScopeId);
        command.Parameters.AddWithValue("id", before.Metadata.IntentId);
        command.Parameters.AddWithValue("state", (int)after.State);
        command.Parameters.AddWithValue("version", after.Version);
        command.Parameters.AddWithValue("attempts", after.AttemptCount);
        command.Parameters.AddWithValue("generation_attempt", after.GenerationAttempt);
        command.Parameters.AddWithValue("generation", after.ReplayGeneration);
        command.Parameters.AddWithValue("next", nextAttemptAt);
        command.Parameters.AddWithValue("uncertain", after.DeliveryUncertain);
        command.Parameters.AddWithValue("old_state", (int)before.Metadata.State);
        command.Parameters.AddWithValue("old_version", before.Metadata.Version);
        PostgreSqlProtocol.Optional(command, "quarantine", NpgsqlDbType.Uuid, after.QuarantineId);
        PostgreSqlProtocol.Optional(command, "token", NpgsqlDbType.Uuid, lease?.Token);
        PostgreSqlProtocol.Optional(command, "until", NpgsqlDbType.TimestampTz, lease?.Until);
        PostgreSqlProtocol.Optional(command, "old_token", NpgsqlDbType.Uuid, before.Lease?.Token);
        PostgreSqlProtocol.Optional(command, "reason", NpgsqlDbType.Text, after.LastReason);
        if (await command.ExecuteNonQueryAsync(ct) != 1) throw new InvalidOperationException("Outbox ownership changed under lock.");
    }

    internal async Task FinishAttemptAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string scope,
        Guid token, string status, string reason, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"UPDATE {schema}.outbox_attempts SET status=@status,reason=@reason,finished_at=clock_timestamp() WHERE scope_id=@scope AND id=@id AND status='Started'", connection, transaction);
        command.Parameters.AddWithValue("scope", scope);
        command.Parameters.AddWithValue("id", token);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("reason", reason);
        if (await command.ExecuteNonQueryAsync(ct) != 1) throw new InvalidOperationException("Outbox attempt was not active.");
    }

    internal async Task<OutboxMetadata> QuarantineAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        PostgreSqlOutboxSnapshot before, CancellationToken ct)
    {
        var reason = before.EnvelopeError ?? throw new InvalidOperationException("Envelope corruption is required.");
        var after = OutboxRecovery.Quarantine(before.Metadata, Guid.NewGuid(), reason);
        // Preserve the original DB representation server-side; raw content is never returned or logged.
        await using (var snapshot = new NpgsqlCommand($"""
            INSERT INTO {schema}.outbox_quarantine(scope_id,id,intent_id,observed_version,reason,original_record)
            SELECT scope_id,@quarantine,id,version,@reason,to_jsonb(original)
            FROM {schema}.outbox_messages original WHERE scope_id=@scope AND id=@id AND version=@version
            """, connection, transaction))
        {
            snapshot.Parameters.AddWithValue("quarantine", after.QuarantineId!.Value);
            snapshot.Parameters.AddWithValue("reason", reason);
            snapshot.Parameters.AddWithValue("scope", before.Metadata.ScopeId);
            snapshot.Parameters.AddWithValue("id", before.Metadata.IntentId);
            snapshot.Parameters.AddWithValue("version", before.Metadata.Version);
            if (await snapshot.ExecuteNonQueryAsync(ct) != 1) throw new InvalidOperationException("Quarantine snapshot was not persisted.");
        }
        if (before.Lease is not null)
            await FinishAttemptAsync(connection, transaction, before.Metadata.ScopeId, before.Lease.Token, "Unknown", reason, ct);
        var now = await PostgreSqlProtocol.NowAsync(connection, transaction, ct);
        await PersistAsync(connection, transaction, before, after, null, now, ct);
        return after;
    }

    private static string? Text(NpgsqlDataReader reader, int index) => reader.IsDBNull(index) ? null : reader.GetString(index);
}

internal sealed record PostgreSqlOutboxSnapshot(OutboxMetadata Metadata, WorkLease? Lease, OutboxRecord? Record, string? EnvelopeError)
{
    internal OutboxRecord RequireRecord() => Record ?? (EnvelopeError == "outbox-data-purged"
        ? throw new PayloadUnavailableException("outbox", Metadata.IntentId)
        : throw new OutboxEnvelopeException(Metadata.IntentId, EnvelopeError!));
}
