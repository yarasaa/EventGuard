using Npgsql;
using NpgsqlTypes;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore : IExternalCallLimiter
{
    private sealed record LimitRow(int Used, int Failures, DateTimeOffset Started, DateTimeOffset? OpenUntil, Guid? Probe, DateTimeOffset RetiredBefore);
    private async Task<LimitRow?> ReadLimitAsync(NpgsqlConnection c, NpgsqlTransaction? t, ExternalCallLimit limit, CancellationToken ct)
    {
        await using var command = StoreCommand(c, t, $"SELECT fingerprint,used,failures,window_started_at,open_until,probe,retired_before FROM {_schema}.external_limits WHERE id=@id", ("id", limit.Id));
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        if (reader.GetString(0) != limit.Fingerprint) throw new InvalidOperationException("Shared endpoint limit configuration differs between workers.");
        return new(reader.GetInt32(1), reader.GetInt32(2), reader.GetFieldValue<DateTimeOffset>(3), reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTimeOffset>(4), reader.IsDBNull(5) ? null : reader.GetGuid(5), reader.GetFieldValue<DateTimeOffset>(6));
    }
    private async Task SaveLimitAsync(NpgsqlConnection c, NpgsqlTransaction t, ExternalCallLimit limit, LimitRow row, CancellationToken ct)
    {
        await using var command = StoreCommand(c, t, $"UPDATE {_schema}.external_limits SET used=@used,failures=@failures,window_started_at=@started,open_until=@open,probe=@probe WHERE id=@id",
            ("used", row.Used), ("failures", row.Failures), ("started", row.Started), ("id", limit.Id));
        PostgreSqlProtocol.Optional(command, "open", NpgsqlDbType.TimestampTz, row.OpenUntil); PostgreSqlProtocol.Optional(command, "probe", NpgsqlDbType.Uuid, row.Probe);
        await command.ExecuteNonQueryAsync(ct);
    }
    public async Task<ExternalCallPermit?> TryAcquireCallAsync(ExternalCallLimit limit, Guid requestId, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(limit);
        if (requestId == Guid.Empty || duration < TimeSpan.FromMilliseconds(1) || duration > TimeSpan.FromHours(1)) throw new ArgumentException("Invalid call permit.");
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(c, t, "external-limit:" + _schema + ":" + limit.Id, cancellationToken);
        var now = await PostgreSqlProtocol.NowAsync(c, t, cancellationToken);
        var row = await ReadLimitAsync(c, t, limit, cancellationToken);
        if (row is null)
        {
            await using var insert = StoreCommand(c, t, $"INSERT INTO {_schema}.external_limits(id,fingerprint,window_started_at,used,failures) VALUES(@id,@fingerprint,@now,0,0)",
                ("id", limit.Id), ("fingerprint", limit.Fingerprint), ("now", now));
            await insert.ExecuteNonQueryAsync(cancellationToken); row = new(0, 0, now, null, null, DateTimeOffset.UnixEpoch);
        }
        await using (var prior = StoreCommand(c, t, $"SELECT expires_at,outcome FROM {_schema}.external_permits WHERE limit_id=@limit AND id=@id", ("limit", limit.Id), ("id", requestId)))
        {
            await using var reader = await prior.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken)) return reader.IsDBNull(1) && reader.GetFieldValue<DateTimeOffset>(0) > now ? new(limit, requestId, reader.GetFieldValue<DateTimeOffset>(0)) : null;
        }
        var requestTime = ExternalCallRequestIds.Timestamp(requestId);
        if (requestTime > now + TimeSpan.FromMinutes(5)) throw new ArgumentException("Permit request timestamp is in the future.", nameof(requestId));
        if (requestTime <= row.RetiredBefore) return null;
        if (row.OpenUntil > now) return null;
        if (row.Started + limit.Window <= now) row = row with { Used = 0, Started = now };
        var active = await ActivePermitsAsync(c, t, limit.Id, now, cancellationToken);
        if (row.Used >= limit.MaximumCalls || active >= limit.MaximumConcurrency || row.OpenUntil.HasValue && active > 0) return null;
        row = row with { Used = checked(row.Used + 1), Probe = row.OpenUntil.HasValue ? requestId : null };
        await SaveLimitAsync(c, t, limit, row, cancellationToken);
        await using var permit = StoreCommand(c, t, $"INSERT INTO {_schema}.external_permits(limit_id,id,window_started_at,expires_at,request_time) VALUES(@limit,@id,@window,@expires,@request_time)",
            ("limit", limit.Id), ("id", requestId), ("window", row.Started), ("expires", now + duration));
        PostgreSqlProtocol.Optional(permit, "request_time", NpgsqlDbType.TimestampTz, requestTime);
        await permit.ExecuteNonQueryAsync(cancellationToken); await PostgreSqlProtocol.CommitAsync(t, requestId, cancellationToken);
        return new(limit, requestId, now + duration);
    }
    private async Task<int> ActivePermitsAsync(NpgsqlConnection c, NpgsqlTransaction? t, string id, DateTimeOffset now, CancellationToken ct)
    {
        await using var command = StoreCommand(c, t, $"SELECT count(*) FROM {_schema}.external_permits WHERE limit_id=@id AND expires_at>@now AND outcome IS NULL", ("id", id), ("now", now));
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct));
    }
    public async Task CompleteCallAsync(ExternalCallPermit permit, ExternalCallOutcome outcome, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(permit);
        if (!Enum.IsDefined(outcome)) throw new ArgumentException("Invalid permit outcome.", nameof(outcome));
        var limit = permit.Limit;
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(c, t, "external-limit:" + _schema + ":" + limit.Id, cancellationToken);
        var row = await ReadLimitAsync(c, t, limit, cancellationToken) ?? throw new InvalidOperationException("Call limit not found.");
        DateTimeOffset started, expires;
        await using (var find = StoreCommand(c, t, $"SELECT window_started_at,expires_at,outcome FROM {_schema}.external_permits WHERE limit_id=@limit AND id=@id", ("limit", limit.Id), ("id", permit.Id)))
        {
            await using var reader = await find.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                if (ExternalCallRequestIds.Timestamp(permit.Id) <= row.RetiredBefore) throw new ExternalCallPermitRetiredException(permit.Id);
                throw new InvalidOperationException("Call permit not found.");
            }
            if (!reader.IsDBNull(2))
            { if (reader.GetInt32(2) != (int)outcome) throw new InvalidOperationException("Call permit already has a different outcome."); return; }
            started = reader.GetFieldValue<DateTimeOffset>(0); expires = reader.GetFieldValue<DateTimeOffset>(1);
        }
        var now = await PostgreSqlProtocol.NowAsync(c, t, cancellationToken);
        if (outcome == ExternalCallOutcome.Unused)
            row = row with { Used = started == row.Started ? Math.Max(0, row.Used - 1) : row.Used, Probe = row.Probe == permit.Id ? null : row.Probe };
        else if (outcome == ExternalCallOutcome.Unhealthy)
        {
            var failures = Math.Min(limit.FailureThreshold, row.Failures + 1);
            row = row with { Failures = failures, OpenUntil = failures >= limit.FailureThreshold ? now + limit.BreakDuration : row.OpenUntil, Probe = null };
        }
        else if (expires > now && (!row.OpenUntil.HasValue || row.Probe == permit.Id)) row = row with { Failures = 0, OpenUntil = null, Probe = null };
        await SaveLimitAsync(c, t, limit, row, cancellationToken);
        await using var save = StoreCommand(c, t, $"UPDATE {_schema}.external_permits SET outcome=@outcome WHERE limit_id=@limit AND id=@id", ("outcome", (int)outcome), ("limit", limit.Id), ("id", permit.Id));
        await save.ExecuteNonQueryAsync(cancellationToken); await PostgreSqlProtocol.CommitAsync(t, permit.Id, cancellationToken);
    }
    public async Task<ExternalCallLimitStatus?> ReadCallLimitAsync(ExternalCallLimit limit, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(limit);
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(c, t, "external-limit:" + _schema + ":" + limit.Id, cancellationToken);
        var row = await ReadLimitAsync(c, t, limit, cancellationToken); if (row is null) return null;
        var now = await PostgreSqlProtocol.NowAsync(c, t, cancellationToken);
        return new(row.Started + limit.Window <= now ? 0 : row.Used, await ActivePermitsAsync(c, t, limit.Id, now, cancellationToken), row.Failures, row.Started, row.OpenUntil);
    }
}
