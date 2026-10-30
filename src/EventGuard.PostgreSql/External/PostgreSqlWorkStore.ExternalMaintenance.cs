using Npgsql;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore : IExternalActionAvailability, IExternalCallPermitMaintenance
{
    public async Task<bool> HasDueActionsAsync(ProducerScope scope, string binding, string bindingVersion, ExternalPhase phase,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope); _ = new ProducerScope(binding, bindingVersion);
        if (!Enum.IsDefined(phase)) throw new ArgumentOutOfRangeException(nameof(phase));
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand($"""
            SELECT 1 FROM {_schema}.external_actions
            WHERE scope_id=@scope AND binding_name=@binding AND binding_version=@binding_version
              AND {ExternalDuePredicate(phase)} AND next_at<=clock_timestamp()
              AND (lease_until IS NULL OR lease_until<=clock_timestamp()) LIMIT 1
            """, c);
        command.Parameters.AddWithValue("scope", scope.Id); command.Parameters.AddWithValue("binding", binding);
        command.Parameters.AddWithValue("binding_version", bindingVersion);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    public async Task<int> PruneCallPermitsAsync(ExternalCallLimit limit, TimeSpan retention, int maximumItems = 1000,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(limit);
        if (retention < TimeSpan.FromHours(1) || retention > TimeSpan.FromDays(365) || maximumItems is < 1 or > 1000)
            throw new ArgumentOutOfRangeException(nameof(retention));
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var t = await c.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(c, t, "external-limit:" + _schema + ":" + limit.Id, cancellationToken);
        var row = await ReadLimitAsync(c, t, limit, cancellationToken);
        if (row is null) return 0;
        var now = await PostgreSqlProtocol.NowAsync(c, t, cancellationToken);
        var before = now - retention;
        await using var command = StoreCommand(c, t, $"""
            UPDATE {_schema}.external_limits SET retired_before=GREATEST(retired_before,@before) WHERE id=@id;
            WITH expired AS (SELECT id FROM {_schema}.external_permits
                WHERE limit_id=@id AND request_time<=@before AND expires_at<=@now ORDER BY request_time,id LIMIT @maximum),
            removed AS (DELETE FROM {_schema}.external_permits p USING expired e WHERE p.limit_id=@id AND p.id=e.id RETURNING p.id)
            SELECT count(*) FROM removed;
            """, ("id", limit.Id), ("before", before), ("now", now), ("maximum", maximumItems));
        var removed = Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken));
        await PostgreSqlProtocol.CommitAsync(t, Guid.NewGuid(), cancellationToken);
        return removed;
    }
}
