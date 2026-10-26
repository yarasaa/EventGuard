using Microsoft.Data.SqlClient;
using System.Data;

namespace EventGuard.SqlServer;

public sealed partial class SqlServerWorkStore : IExternalObservationStore
{
    public async Task<ExternalNoticeReceipt> RecordNoticeAsync(ExternalObservationNotice notice, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(notice);
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginLocalAsync(IsolationLevel.RepeatableRead, cancellationToken);
        await SqlServerProtocol.LockAsync(c, t, "external-notice:" + _schema + ":" + notice.Scope.Id + ":" + notice.Id, cancellationToken);
        await using (var find = StoreCommand(c, t, $"SELECT fingerprint,status,status_reason,action_version FROM {_schema}.external_notices WHERE scope_id=@scope AND id=@id", ("scope", notice.Scope.Id), ("id", notice.Id)))
        {
            await using var reader = await find.ExecuteReaderCheckedAsync(cancellationToken);
            if (await reader.ReadCheckedAsync(cancellationToken))
            {
                if (reader.GetString(0) != notice.Fingerprint) throw new InvalidOperationException("Notice ID already has different immutable content.");
                return new(notice.Id, notice.ActionId, (ExternalNoticeStatus)reader.GetInt32(1), reader.GetString(2), reader.GetInt64(3));
            }
        }
        var action = await FindExternalAsync(c, t, notice.Scope, notice.ActionId, false, cancellationToken) ?? throw new InvalidOperationException("Report action not found.");
        if (action.Record.Action.Binding != notice.Binding || action.Record.Action.BindingVersion != notice.BindingVersion) throw new InvalidOperationException("Report adapter binding differs from the action.");
        await using var insert = StoreCommand(c, t, $"""
            INSERT INTO {_schema}.external_notices(scope_id,id,action_id,binding_name,binding_version,provider_revision,result_kind,reason,provider_reference,result_data,fingerprint)
            VALUES(@scope,@id,@action,@binding,@binding_version,@revision,@kind,@reason,@reference,@data,@fingerprint)
            """, ("scope", notice.Scope.Id), ("id", notice.Id), ("action", notice.ActionId), ("binding", notice.Binding), ("binding_version", notice.BindingVersion),
            ("kind", (int)notice.Result.Kind), ("reason", notice.Result.Reason), ("data", notice.Result.CopyData()), ("fingerprint", notice.Fingerprint));
        SqlServerProtocol.Optional(insert, "revision", SqlDbType.BigInt, notice.ProviderRevision); SqlServerProtocol.Optional(insert, "reference", SqlDbType.NVarChar, notice.Result.Reference);
        await insert.ExecuteNonQueryCheckedAsync(cancellationToken); await SqlServerProtocol.CommitAsync(t, notice.Id, cancellationToken);
        return new(notice.Id, notice.ActionId, ExternalNoticeStatus.Received, "received", 0);
    }
    public async Task<ExternalNoticeReceipt?> FindNoticeAsync(ProducerScope scope, Guid noticeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = StoreCommand(c, null, $"SELECT action_id,status,status_reason,action_version FROM {_schema}.external_notices WHERE scope_id=@scope AND id=@id", ("scope", scope.Id), ("id", noticeId));
        await using var reader = await command.ExecuteReaderCheckedAsync(cancellationToken);
        return await reader.ReadCheckedAsync(cancellationToken) ? new(noticeId, reader.GetGuid(0), (ExternalNoticeStatus)reader.GetInt32(1), reader.GetString(2), reader.GetInt64(3)) : null;
    }
    public async Task<ExternalNoticeReceipt?> ApplyNextNoticeAsync(ProducerScope scope, string binding, string bindingVersion,
        ExternalActionPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope); ArgumentNullException.ThrowIfNull(policy); _ = new ProducerScope(binding, bindingVersion);
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginLocalAsync(IsolationLevel.RepeatableRead, cancellationToken);
        var now = await SqlServerProtocol.NowAsync(c, t, cancellationToken);
        ExternalObservationNotice notice;
        await using (var next = StoreCommand(c, t, $"""
            SELECT TOP(1) id,action_id,provider_revision,result_kind,reason,provider_reference,result_data,fingerprint
            FROM {_schema}.external_notices WITH(UPDLOCK,READPAST,ROWLOCK) WHERE scope_id=@scope AND binding_name=@binding AND binding_version=@binding_version AND status=0 AND next_at<=@now
            ORDER BY next_at,id
            """, ("scope", scope.Id), ("binding", binding), ("binding_version", bindingVersion), ("now", now)))
        {
            await using var reader = await next.ExecuteReaderCheckedAsync(cancellationToken); if (!await reader.ReadCheckedAsync(cancellationToken)) return null;
            notice = new(scope, reader.GetGuid(0), reader.GetGuid(1), binding, bindingVersion,
                new((ExternalResultKind)reader.GetInt32(3), reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetString(5), reader.GetFieldValue<byte[]>(6)),
                reader.IsDBNull(2) ? null : reader.GetInt64(2));
            if (reader.GetString(7) != notice.Fingerprint) throw new InvalidOperationException("Report evidence is corrupt.");
        }
        var row = (await FindExternalAsync(c, t, scope, notice.ActionId, true, cancellationToken))!;
        var before = row.Record;
        var status = ExternalNoticeStatus.Applied; var reason = "applied"; var nextAt = now;
        long? lastRevision = null; string? lastFingerprint = null;
        await using (var position = StoreCommand(c, t, $"SELECT revision,fingerprint FROM {_schema}.external_report_positions WHERE scope_id=@scope AND action_id=@id", ("scope", scope.Id), ("id", notice.ActionId)))
        {
            await using var reader = await position.ExecuteReaderCheckedAsync(cancellationToken);
            if (await reader.ReadCheckedAsync(cancellationToken)) { lastRevision = reader.GetInt64(0); lastFingerprint = reader.GetString(1); }
        }
        if (lastRevision.HasValue && !notice.ProviderRevision.HasValue) { status = ExternalNoticeStatus.Conflict; reason = "provider-revision-required"; }
        else if (notice.ProviderRevision < lastRevision) { status = ExternalNoticeStatus.Ignored; reason = "older-provider-revision"; }
        else if (notice.ProviderRevision.HasValue && notice.ProviderRevision == lastRevision)
        { status = notice.ResultFingerprint == lastFingerprint ? ExternalNoticeStatus.Ignored : ExternalNoticeStatus.Conflict; reason = "same-provider-revision"; }
        else if (before.State == ExternalActionState.Rejected) { status = ExternalNoticeStatus.Conflict; reason = "report-for-rejected-action"; }
        else if (before.State != ExternalActionState.Accepted)
        { status = ExternalNoticeStatus.Received; reason = "awaiting-acceptance"; nextAt = now + policy.ObservationInterval; }
        else if (notice.Result.Reference is not null && before.ProviderReference is not null && notice.Result.Reference != before.ProviderReference)
        { status = ExternalNoticeStatus.Conflict; reason = "provider-reference-mismatch"; }
        else if (before.Observation == ExternalObservationState.None) { status = ExternalNoticeStatus.Conflict; reason = "observation-not-configured"; }
        else if (before.Observation is ExternalObservationState.Succeeded or ExternalObservationState.Failed)
        {
            var final = before.Observation == ExternalObservationState.Succeeded ? ExternalResultKind.Succeeded : ExternalResultKind.Failed;
            status = notice.Result.Kind is ExternalResultKind.Pending or ExternalResultKind.Unknown || notice.Result.Kind == final
                ? ExternalNoticeStatus.Ignored : ExternalNoticeStatus.Conflict;
            reason = status == ExternalNoticeStatus.Ignored ? "terminal-observation-preserved" : "conflicting-terminal-report";
        }
        else if (before.Observation == ExternalObservationState.TimedOut && notice.Result.Kind is ExternalResultKind.Pending or ExternalResultKind.Unknown)
        { status = ExternalNoticeStatus.Ignored; reason = "observation-deadline-preserved"; }
        var after = before;
        if (status == ExternalNoticeStatus.Applied)
        {
            // A trusted final callback may resolve a previously timed-out report; it never resubmits the action.
            after = ExternalActionLifecycle.Settle(before with { Observation = ExternalObservationState.Pending }, ExternalPhase.Observe, notice.Result, policy, now);
            await SaveExternalAsync(c, t, after, null, null, null, cancellationToken);
            await PublishExternalUpdateAsync(c, t, after, row.Target, ExternalPhase.Observe, notice.Result, cancellationToken);
            if (notice.ProviderRevision is { } revision)
            {
                await using var save = StoreCommand(c, t, $"""
                    UPDATE {_schema}.external_report_positions SET revision=@revision,fingerprint=@fingerprint WHERE scope_id=@scope AND action_id=@id;
                    IF @@ROWCOUNT=0 INSERT INTO {_schema}.external_report_positions(scope_id,action_id,revision,fingerprint) VALUES(@scope,@id,@revision,@fingerprint);
                    """, ("scope", scope.Id), ("id", notice.ActionId), ("revision", revision), ("fingerprint", notice.ResultFingerprint));
                await save.ExecuteNonQueryCheckedAsync(cancellationToken);
            }
        }
        await using var update = StoreCommand(c, t, $"UPDATE {_schema}.external_notices SET status=@status,status_reason=@reason,action_version=@version,next_at=@next WHERE scope_id=@scope AND id=@id",
            ("status", (int)status), ("reason", reason), ("version", after.Version), ("next", nextAt), ("scope", scope.Id), ("id", notice.Id));
        await update.ExecuteNonQueryCheckedAsync(cancellationToken); await SqlServerProtocol.CommitAsync(t, notice.Id, cancellationToken);
        return new(notice.Id, notice.ActionId, status, reason, after.Version);
    }
    public async Task<int> ExpireObservationsAsync(ProducerScope scope, string binding, string bindingVersion, int limit = 100, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope); _ = new ProducerScope(binding, bindingVersion);
        if (limit is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(limit));
        await using var c = await _dataSource.OpenConnectionAsync(cancellationToken); await using var t = await c.BeginLocalAsync(IsolationLevel.RepeatableRead, cancellationToken);
        var now = await SqlServerProtocol.NowAsync(c, t, cancellationToken);
        var rows = new List<ExternalRow>();
        await using (var select = StoreCommand(c, t, $"SELECT TOP(@limit) {ExternalColumns} FROM {_schema}.external_actions WITH(UPDLOCK,READPAST,ROWLOCK) WHERE scope_id=@scope AND binding_name=@binding AND binding_version=@binding_version AND state=2 AND observation=1 AND observation_deadline<=@now AND (lease_until IS NULL OR lease_until<=@now) ORDER BY id",
            ("scope", scope.Id), ("binding", binding), ("binding_version", bindingVersion), ("now", now), ("limit", limit)))
        {
            await using var reader = await select.ExecuteReaderCheckedAsync(cancellationToken);
            while (await reader.ReadCheckedAsync(cancellationToken)) rows.Add(ReadExternal(reader));
        }
        foreach (var row in rows)
        {
            var expired = row.Record with { Observation = ExternalObservationState.TimedOut, Version = checked(row.Record.Version + 1), LastReason = "observation-deadline-expired" };
            await SaveExternalAsync(c, t, expired, null, null, null, cancellationToken);
            await PublishExternalUpdateAsync(c, t, expired, row.Target, ExternalPhase.Observe, null, cancellationToken);
        }
        await SqlServerProtocol.CommitAsync(t, Guid.NewGuid(), cancellationToken); return rows.Count;
    }
}
