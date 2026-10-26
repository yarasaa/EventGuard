namespace EventGuard.Operations.PostgreSql;

public sealed partial class PostgreSqlWorkReader : IBacklogReadStore
{
    public async Task<BacklogSummary> ReadBacklogAsync(BacklogQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        var table = query.Kind == BacklogKind.Consumer ? "works" : "outbox_messages";
        // One statement snapshot. LIMIT is inside the aggregate, on an index-ordered state prefix.
        await using var command = _data.CreateCommand($"""
            SELECT s.state,b.found,b.oldest FROM unnest(@states) AS s(state)
            CROSS JOIN LATERAL (
                SELECT count(*) AS found,min(created_at) AS oldest FROM (
                    SELECT created_at FROM {_schema}.{table} WHERE scope_id=@scope AND state=s.state
                    ORDER BY created_at ASC LIMIT @limit
                ) AS bounded
            ) AS b ORDER BY s.state
            """);
        command.Parameters.AddWithValue("states", query.States.Select(s => s.Code).ToArray());
        command.Parameters.AddWithValue("scope", query.ScopeId);
        command.Parameters.AddWithValue("limit", query.CountLimit + 1);
        var buckets = new List<BacklogBucket>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var found = reader.GetInt64(1);
            buckets.Add(new(query.States.Single(s => s.Code == reader.GetInt32(0)).Name, (int)Math.Min(found, query.CountLimit),
                found > query.CountLimit, reader.IsDBNull(2) ? null : new DateTimeOffset(reader.GetDateTime(2), TimeSpan.Zero)));
        }
        return new(query.CountLimit, buckets);
    }
}
