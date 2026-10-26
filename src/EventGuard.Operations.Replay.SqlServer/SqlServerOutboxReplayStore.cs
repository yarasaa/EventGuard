using Microsoft.Data.SqlClient;

namespace EventGuard.Operations.Replay.SqlServer;

public sealed partial class SqlServerReplayStore
{
    public Task<OutboxReplayResult> ReplayOutboxAsync(OutboxReplayRequest request, CancellationToken ct = default) => _outbox.ReplayAsync(request, ct);

    public async Task<OutboxReplayReceipt?> FindOutboxReceiptAsync(ProducerScope scope, Guid intentId, Guid requestId, string actor, CancellationToken ct = default)
    {
        await using var connection = await _data.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new SqlCommand($"""
            SELECT result_status,result_generation,result_version,left(reason,1024),acknowledge_possible_duplicate,created_at
            FROM {_schema}.outbox_replay_requests WHERE scope_id=@scope AND intent_id=@intent AND id=@request AND actor=@actor COLLATE Latin1_General_100_BIN2
            """, connection) { CommandTimeout = 2 };
        command.Parameters.AddWithValue("scope", scope.Id);
        command.Parameters.AddWithValue("intent", intentId);
        command.Parameters.AddWithValue("request", requestId);
        command.Parameters.AddWithValue("actor", actor);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        var status = (OutboxReplayStatus)reader.GetInt32(0);
        if (!Enum.IsDefined(status)) throw new InvalidOperationException("Unknown outbox replay receipt status.");
        return new(requestId, new(status, intentId, reader.GetInt32(1), reader.GetInt64(2)), reader.GetString(3),
            reader.GetBoolean(4), reader.GetFieldValue<DateTimeOffset>(5));
    }
}
