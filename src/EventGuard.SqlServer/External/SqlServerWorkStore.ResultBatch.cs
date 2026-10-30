using System.Data;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

public sealed partial class SqlServerWorkStore : IExternalResultBatchStore<SqlServerSession>
{
    private sealed record ResultCandidate(WorkItem Work, WorkLease? Ownership);
    private sealed record ResultAdmission(ResultCandidate Candidate, ClaimDecision Claim, DateTimeOffset StartedAt);
    private sealed record ResultCompletion(ResultAdmission Admission, ExecutionDecision Decision);

    public async Task ValidateExternalResultBatchAsync(CancellationToken cancellationToken = default)
    {
        await ValidateExternalActionsAsync(cancellationToken);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new SqlCommand("""
            SELECT COUNT(*) FROM (VALUES('external_result_ids'),('external_result_changes'),('external_result_versions')) types(name)
            WHERE COALESCE(HAS_PERMS_BY_NAME(@schema+'.'+name,'TYPE','EXECUTE'),0)<>1
                OR COALESCE(HAS_PERMS_BY_NAME(@schema+'.'+name,'TYPE','REFERENCES'),0)<>1
            """, connection);
        command.Parameters.AddValue("schema", _schema);
        if (Convert.ToInt32(await command.ExecuteScalarCheckedAsync(cancellationToken)) != 0)
            throw new InvalidOperationException("Result batching requires EXECUTE and REFERENCES on external_result_ids, external_result_changes and external_result_versions.");
    }

    public async Task<ExternalResultBatchExecution> ExecuteResultBatchAsync(ExternalResultTarget target,
        ExternalResultBatchOptions options,
        Func<IReadOnlyList<ExternalActionUpdate>, SqlServerSession, CancellationToken, ValueTask> handler,
        RetryPolicy retry, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(target); ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(handler); ArgumentNullException.ThrowIfNull(retry);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(options.ExecutionTimeout);
        var ct = deadline.Token;
        List<ResultAdmission> admitted;
        Exception? failure = null;
        await using (var connection = await _dataSource.OpenConnectionAsync(ct))
        await using (var transaction = await connection.BeginLocalAsync(IsolationLevel.RepeatableRead, ct))
        {
            var selected = await SelectResultCandidatesAsync(connection, transaction, target, options, null, ct);
            admitted = await AdmitResultsAsync(connection, transaction, target, options, selected, ct);
            if (admitted.Count == 0) return new(Array.Empty<ExecutionResult>(), 0, 0);
            var now = await DatabaseNowAsync(connection, transaction, ct);
            var live = admitted.Where(a => !(a.Candidate.Work.ExpiresAt <= now)).ToArray();
            ExternalActionUpdate[] fresh = [];
            try
            {
                // Reading immutable snapshots, the SQL callback, and position updates are inside the same transaction.
                var updates = await ReadResultUpdatesAsync(connection, transaction, target, live, ct);
                var positions = await LockResultPositionsAsync(connection, transaction, target, updates, ct);
                fresh = updates.GroupBy(u => u.Action.Id).Select(g => g.MaxBy(u => u.Version)!)
                    .Where(u => !positions.TryGetValue(u.Action.Id, out var position) || u.Version > position).ToArray();
                if (fresh.Length > 0)
                    await handler(Array.AsReadOnly(fresh), new SqlServerSession(connection, transaction, _schema, sqlOnly: true), ct);
                ct.ThrowIfCancellationRequested();
                await AdvanceResultPositionsAsync(connection, transaction, target, fresh, ct);
            }
            catch (Exception error)
            {
                // Confirm a full rollback before recording failures under new locks. This also handles doomed SQL transactions.
                using var cleanup = new CancellationTokenSource(options.FailureRecordingTimeout);
                await transaction.RollbackAsync(cleanup.Token);
                failure = error;
            }
            if (failure is null)
            {
                now = await DatabaseNowAsync(connection, transaction, ct);
                var completed = admitted.Select(a => new ResultCompletion(a, a.Candidate.Work.ExpiresAt <= now
                    && !live.Contains(a) ? WorkLifecycle.Expire(a.Claim.Change.After, a.Claim.Lease, a.Claim.Lease.Token, now)
                    : WorkLifecycle.Complete(a.Claim.Change.After, a.Claim.Lease, a.Claim.Lease.Token, now))).ToArray();
                await PersistResultCompletionsAsync(connection, transaction, completed, now, ct);
                // Commit uncertainty must never enter the rollback/retry path above.
                await CommitAsync(transaction, Guid.NewGuid(), ct);
                return new(Array.AsReadOnly(completed.Select(c => new ExecutionResult(c.Decision.Status, c.Decision.Change.After)).ToArray()),
                    fresh.Length, live.Length - fresh.Length);
            }
        }
        // Re-lock only unchanged candidates. Another worker may have completed them after our rollback.
        using var recording = new CancellationTokenSource(options.FailureRecordingTimeout);
        await using var retryConnection = await _dataSource.OpenConnectionAsync(recording.Token);
        await using var retryTransaction = await retryConnection.BeginLocalAsync(IsolationLevel.RepeatableRead, recording.Token);
        var unchanged = await SelectResultCandidatesAsync(retryConnection, retryTransaction, target, options,
            admitted.Select(a => a.Candidate).ToArray(), recording.Token);
        var retryAdmissions = await AdmitResultsAsync(retryConnection, retryTransaction, target, options, unchanged, recording.Token);
        var failedAt = await DatabaseNowAsync(retryConnection, retryTransaction, recording.Token);
        var kind = retry.Classify(failure!);
        var failed = retryAdmissions.Select(a => new ResultCompletion(a, a.Candidate.Work.ExpiresAt <= failedAt
            ? WorkLifecycle.Expire(a.Claim.Change.After, a.Claim.Lease, a.Claim.Lease.Token, failedAt)
            : WorkLifecycle.Fail(a.Claim.Change.After, a.Claim.Lease, a.Claim.Lease.Token, retry, kind, failedAt, Random.Shared.NextDouble())!)).ToArray();
        await PersistResultCompletionsAsync(retryConnection, retryTransaction, failed, failedAt, recording.Token);
        await CommitAsync(retryTransaction, Guid.NewGuid(), recording.Token);
        return new(Array.AsReadOnly(failed.Select(c => new ExecutionResult(c.Decision.Status, c.Decision.Change.After)).ToArray()), 0, 0);
    }

    private async Task<List<ResultAdmission>> AdmitResultsAsync(SqlConnection connection, SqlTransaction transaction,
        ExternalResultTarget target, ExternalResultBatchOptions options, IReadOnlyList<ResultCandidate> selected, CancellationToken ct)
    {
        var admitted = new List<ResultAdmission>();
        if (selected.Count == 0) return admitted;
        var now = await DatabaseNowAsync(connection, transaction, ct);
        // Follow the ordinary worker's work-row -> tenant-counter lock order. Persist the counter once per page.
        var before = await SqlServerTenantBudgetProtocol.ReadAsync(connection, transaction, _schema, target.Scope.Tenant, true, ct);
        var budget = before;
        foreach (var candidate in selected)
        {
            var work = candidate.Work;
            var claim = WorkLifecycle.Claim(work, candidate.Ownership, Guid.NewGuid(), options.ExecutionTimeout, now);
            if (claim is null) continue;
            if (!(work.ExpiresAt <= now) && budget is not null)
            {
                var allowance = TenantBudgets.Admit(budget, work.AttemptCount == 0 ? TenantBudgetKind.Claim : TenantBudgetKind.RetryClaim, now);
                if (!allowance.Allowed) continue;
                budget = allowance.After;
            }
            admitted.Add(new(candidate, claim, now));
        }
        if (budget is not null && budget != before)
            await SqlServerTenantBudgetProtocol.PersistAsync(connection, transaction, _schema, before, budget, ct);
        return admitted;
    }

    private static DataTable ResultIds(IEnumerable<ResultCandidate> candidates)
    {
        var table = new DataTable(); table.Columns.Add("work_id", typeof(Guid)); table.Columns.Add("version", typeof(long));
        foreach (var c in candidates) table.Rows.Add(c.Work.Id, c.Work.Version);
        return table;
    }

    private string AddResultInput(SqlCommand command, DataTable table, string type)
    {
        command.Parameters.Add(new SqlParameter("rows", SqlDbType.Structured) { TypeName = _schema + "." + type, Value = table });
        return "SELECT * FROM @rows";
    }

    private async Task<ExternalActionUpdate[]> ReadResultUpdatesAsync(SqlConnection connection, SqlTransaction transaction,
        ExternalResultTarget target, IReadOnlyList<ResultAdmission> admitted, CancellationToken ct)
    {
        if (admitted.Count == 0) return [];
        using var table = ResultIds(admitted.Select(a => a.Candidate));
        await using var command = new SqlCommand { Connection = connection, Transaction = transaction };
        var input = AddResultInput(command, table, "external_result_ids");
        command.CommandText = $"""
            WITH input AS ({input})
            SELECT a.tenant,a.producer,a.id,a.binding_name,a.binding_version,a.payload,a.observation_timeout,
                a.fingerprint,a.origin_work_id,a.origin_request_id,a.result_handler,a.result_handler_version,
                u.version,u.state,u.observation,u.phase,u.provider_reference,u.reason,u.state_data,
                u.response_kind,u.response_reason,u.response_reference,u.response_data,w.fingerprint,w.scope_id
            FROM input x JOIN {_schema}.works w WITH(FORCESEEK) ON w.id=x.work_id AND w.version=x.version
            JOIN {_schema}.external_result_updates u WITH(FORCESEEK) ON u.work_id=w.id
            JOIN {_schema}.external_actions a WITH(FORCESEEK) ON a.scope_id=u.scope_id AND a.id=u.action_id
            WHERE u.handler_scope_id=@handler AND u.scope_id=@scope
            OPTION(FORCE ORDER,LOOP JOIN,MAXDOP 1)
            """;
        command.Parameters.AddValue("scope", target.Scope.Id); command.Parameters.AddValue("handler", target.WorkScope.Id);
        var updates = new List<ExternalActionUpdate>();
        await using var reader = await command.ExecuteReaderCheckedAsync(ct);
        while (await reader.ReadCheckedAsync(ct)) updates.Add(SqlServerExternalResultWriter.ReadUpdate(reader, target));
        if (updates.Count != admitted.Count) throw new InvalidOperationException("Result work has no matching immutable snapshot.");
        return updates.ToArray();
    }
}
