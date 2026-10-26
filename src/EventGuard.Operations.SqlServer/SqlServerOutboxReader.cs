using Microsoft.Data.SqlClient;
using System.Data.Common;

namespace EventGuard.Operations.SqlServer;

public sealed partial class SqlServerWorkReader : IOutboxReadStore
{
    private const string OutboxColumns = "id,message_id,state,version,attempt_count,replay_generation,delivery_uncertain,created_at,next_attempt_at,published_at,left(last_reason,129),left(transport,128),left(connection_name,128),left(configuration_version,128)";

    public async Task<OutboxPage> ListOutboxAsync(OutboxQuery query, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var command = new SqlCommand($"""
            SELECT TOP (@limit) {OutboxColumns} FROM {_schema}.outbox_messages WHERE scope_id=@scope AND created_at<=@cutoff
            {(query.State.HasValue ? "AND state=@state" : "")}
            {(query.Filters.CreatedFrom.HasValue ? "AND created_at>=@from" : "")}
            {(query.Filters.CreatedUntil.HasValue ? "AND created_at<@until" : "")}
            {(query.Filters.IntentId.HasValue ? "AND id=@intent" : "")}
            {(query.Filters.MinimumAttempts.HasValue ? "AND attempt_count>=@attempts" : "")}
            {(query.Filters.DeliveryUncertain.HasValue ? "AND delivery_uncertain=@uncertain" : "")}
            {(query.Filters.Replayed is { } replayed ? replayed ? "AND replay_generation>0" : "AND replay_generation=0" : "")}
            {(query.Before is not null ? "AND created_at<=@created AND (created_at<@created OR id<@id)" : "")}
            {(query.After is not null ? "AND created_at>=@created AND (created_at>@created OR id>@id)" : "")}
            ORDER BY created_at {(query.After is null ? "DESC" : "ASC")},id {(query.After is null ? "DESC" : "ASC")}
            """, connection) { CommandTimeout = 2 };
        command.Parameters.AddWithValue("scope", query.Scope.Id);
        command.Parameters.AddWithValue("cutoff", query.CreatedBefore);
        command.Parameters.AddWithValue("limit", query.PageSize + 1);
        if (query.State.HasValue) command.Parameters.AddWithValue("state", (int)query.State.Value);
        if (query.Filters.CreatedFrom is { } from) command.Parameters.AddWithValue("from", from.ToUniversalTime());
        if (query.Filters.CreatedUntil is { } until) command.Parameters.AddWithValue("until", until.ToUniversalTime());
        if (query.Filters.IntentId is { } intent) command.Parameters.AddWithValue("intent", intent);
        if (query.Filters.MinimumAttempts is { } attempts) command.Parameters.AddWithValue("attempts", attempts);
        if (query.Filters.DeliveryUncertain is { } uncertain) command.Parameters.AddWithValue("uncertain", uncertain);
        if ((query.Before ?? query.After) is { } position)
        {
            command.Parameters.AddWithValue("created", position.CreatedAt.ToUniversalTime());
            command.Parameters.AddWithValue("id", position.Id);
        }
        var rows = new List<OutboxSummary>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false)) rows.Add(OutboxRow(reader));
        return OutboxPage.FromWindow(rows, query);
    }

    public async Task<OutboxDetail?> FindOutboxAsync(ProducerScope scope, Guid id, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        OutboxSummary message;
        await using (var command = new SqlCommand($"SELECT {OutboxColumns} FROM {_schema}.outbox_messages WHERE scope_id=@scope AND id=@id", connection) { CommandTimeout = 2 })
        {
            command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", id);
            await using var r = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await r.ReadAsync(ct).ConfigureAwait(false)) return null;
            message = OutboxRow(r);
        }
        var attempts = new List<OutboxAttemptSummary>();
        await using (var command = new SqlCommand($"""
            SELECT TOP (51) number,generation,left(status,64),started_at,finished_at,left(reason,129)
            FROM {_schema}.outbox_attempts WHERE scope_id=@scope AND intent_id=@id ORDER BY number DESC 
            """, connection) { CommandTimeout = 2 })
        {
            command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", id);
            await using var r = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await r.ReadAsync(ct).ConfigureAwait(false)) attempts.Add(new(r.GetInt32(0), r.GetInt32(1),
                OutboxMetadataCodes.Attempt(r.GetString(2)), OutboxTime(r, 3), OutboxOptionalTime(r, 4), OutboxReason(r, 5)));
        }
        var replays = new List<OutboxReplaySummary>();
        await using (var command = new SqlCommand($"""
            SELECT TOP (21) id,result_status,result_generation,result_version,acknowledge_possible_duplicate,created_at
            FROM {_schema}.outbox_replay_requests WHERE scope_id=@scope AND intent_id=@id ORDER BY created_at DESC,id DESC 
            """, connection) { CommandTimeout = 2 })
        {
            command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", id);
            await using var r = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await r.ReadAsync(ct).ConfigureAwait(false)) replays.Add(new(r.GetGuid(0), (OutboxReplayStatus)r.GetInt32(1),
                r.GetInt32(2), r.GetInt64(3), r.GetBoolean(4), OutboxTime(r, 5)));
        }
        OutboxQuarantineSummary? quarantine = null;
        await using (var command = new SqlCommand($"""
            SELECT q.id,q.observed_version,left(q.reason,129),q.detected_at
            FROM {_schema}.outbox_quarantine q JOIN {_schema}.outbox_messages m
                ON m.scope_id=q.scope_id AND m.id=q.intent_id AND m.quarantine_id=q.id
            WHERE q.scope_id=@scope AND q.intent_id=@id
            """, connection) { CommandTimeout = 2 })
        {
            command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("id", id);
            await using var r = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (await r.ReadAsync(ct).ConfigureAwait(false)) quarantine = new(r.GetGuid(0), r.GetInt64(1), OutboxReason(r, 2), OutboxTime(r, 3));
        }
        return new(message, attempts.Take(50).ToArray(), attempts.Count > 50, replays.Take(20).ToArray(), replays.Count > 20, quarantine);
    }

    private static OutboxSummary OutboxRow(DbDataReader r) => new(r.GetGuid(0), r.GetGuid(1), (OutboxState)r.GetInt32(2),
        r.GetInt64(3), r.GetInt32(4), r.GetInt32(5), r.GetBoolean(6), OutboxTime(r, 7), OutboxTime(r, 8),
        OutboxOptionalTime(r, 9), OutboxReason(r, 10), r.GetString(11), r.GetString(12), r.GetString(13));
    private static DateTimeOffset OutboxTime(DbDataReader r, int i) => r.GetFieldValue<DateTimeOffset>(i);
    private static DateTimeOffset? OutboxOptionalTime(DbDataReader r, int i) => r.IsDBNull(i) ? null : OutboxTime(r, i);
    private static string? OutboxReason(DbDataReader r, int i) => OutboxMetadataCodes.Reason(r.IsDBNull(i) ? null : r.GetString(i));
}
