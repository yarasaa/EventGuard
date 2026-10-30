using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

public sealed partial class SqlServerWorkStore : IExternalActionAvailability, IExternalCallPermitMaintenance
{
    public async Task<bool> HasDueActionsAsync(ProducerScope scope, string binding, string bindingVersion, ExternalPhase phase,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope); _ = new ProducerScope(binding, bindingVersion);
        if (!Enum.IsDefined(phase)) throw new ArgumentOutOfRangeException(nameof(phase));
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new SqlCommand($"""
            SELECT TOP(1) 1 FROM {_schema}.external_actions WITH(FORCESEEK({ExternalDueIndex(phase)}(scope_id,binding_key,next_at)))
            WHERE scope_id=@scope AND {ExternalBindingPredicate} AND binding_name=@binding AND binding_version=@binding_version
              AND {ExternalDuePredicate(phase)} AND next_at<=TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00')
              AND (lease_until IS NULL OR lease_until<=TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'))
            """, c);
        command.Parameters.AddValue("scope", scope.Id); command.Parameters.AddValue("binding", binding);
        command.Parameters.AddValue("binding_version", bindingVersion);
        return await command.ExecuteScalarCheckedAsync(cancellationToken) is not null;
    }

    public async Task<int> PruneCallPermitsAsync(ExternalCallLimit limit, TimeSpan retention, int maximumItems = 1000,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(limit);
        if (retention < TimeSpan.FromHours(1) || retention > TimeSpan.FromDays(365) || maximumItems is < 1 or > 1000)
            throw new ArgumentOutOfRangeException(nameof(retention));
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var t = await c.BeginLocalAsync(cancellationToken);
        await SqlServerProtocol.LockAsync(c, t, "external-limit:" + _schema + ":" + limit.Id, cancellationToken);
        var row = await ReadLimitAsync(c, t, limit, cancellationToken);
        if (row is null) return 0;
        var now = await SqlServerProtocol.NowAsync(c, t, cancellationToken);
        var before = now - retention;
        await using var command = StoreCommand(c, t, $"""
            UPDATE {_schema}.external_limits SET retired_before=CASE WHEN retired_before<@before THEN @before ELSE retired_before END WHERE id=@id;
            WITH expired AS (SELECT TOP(@maximum) * FROM {_schema}.external_permits
                WHERE limit_id=@id AND request_time<=@before AND expires_at<=@now ORDER BY request_time,id)
            DELETE FROM expired;
            SELECT @@ROWCOUNT;
            """, ("id", limit.Id), ("before", before), ("now", now), ("maximum", maximumItems));
        var removed = Convert.ToInt32(await command.ExecuteScalarCheckedAsync(cancellationToken));
        await SqlServerProtocol.CommitAsync(t, Guid.NewGuid(), cancellationToken);
        return removed;
    }
}
