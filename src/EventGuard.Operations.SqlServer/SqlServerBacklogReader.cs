using Microsoft.Data.SqlClient;

namespace EventGuard.Operations.SqlServer;

public sealed partial class SqlServerWorkReader : IBacklogReadStore
{
    public async Task<BacklogSummary> ReadBacklogAsync(BacklogQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        var table = query.Kind == BacklogKind.Consumer ? "works" : "outbox_messages";
        var states = string.Join(",", query.States.Select(s => "(" + s.Code + ")"));
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        // RCSI gives one statement snapshot. No NOLOCK/READPAST undercount, no worker transaction.
        await using var command = new SqlCommand($"""
            SELECT s.state,b.found,b.oldest FROM (VALUES {states}) AS s(state)
            CROSS APPLY (
                SELECT count_big(*) AS found,min(created_at) AS oldest FROM (
                    SELECT TOP (@limit) created_at FROM {_schema}.{table} WHERE scope_id=@scope AND state=s.state
                    ORDER BY created_at ASC
                ) AS bounded
            ) AS b ORDER BY s.state
            """, connection) { CommandTimeout = 2 };
        command.Parameters.AddWithValue("scope", query.ScopeId);
        command.Parameters.AddWithValue("limit", query.CountLimit + 1);
        var buckets = new List<BacklogBucket>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var found = reader.GetInt64(1);
            buckets.Add(new(query.States.Single(s => s.Code == reader.GetInt32(0)).Name, (int)Math.Min(found, query.CountLimit),
                found > query.CountLimit, reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTimeOffset>(2)));
        }
        return new(query.CountLimit, buckets);
    }
}
