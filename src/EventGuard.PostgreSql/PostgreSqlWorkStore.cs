using System.Data;
using Npgsql;
using NpgsqlTypes;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore : ITransactionalWorkStore<PostgreSqlSession>, IIngressQuarantineStore, IRetentionStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _schema;
    private const string Columns = "id,identity_id,scope_id,message_id,fingerprint,state,version,attempt_count,generation_attempt,replay_generation,next_attempt_at,expires_at,last_reason,correlation_id";
    private const string ClaimBudgetFilter = "AND (expires_at<=statement_timestamp() OR (@fresh=1 AND attempt_count=0) OR (@retry=1 AND attempt_count>0))";

    public PostgreSqlWorkStore(NpgsqlDataSource dataSource, string schema = "eventguard")
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _schema = PostgreSqlProtocol.QuoteSchema(schema);
    }

    public Task InitializeAsync(CancellationToken cancellationToken = default) =>
        PostgreSqlSchema.InitializeAsync(_dataSource, _schema, cancellationToken);

    public async Task<AcceptanceResult> AcceptAsync(WorkSubmission submission, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(submission);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        var result = await AcceptWithinTransactionAsync(connection, transaction, submission, cancellationToken);
        await CommitAsync(transaction, submission.MessageId, cancellationToken);
        return result;
    }

    private async Task EnsureBindingAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, WorkIdentity identity, CancellationToken cancellationToken)
    {
        var scope = identity.Scope;
        // Batch both statements in order, retaining the row lock and a separate ReadCommitted
        // statement snapshot after a concurrent insert. A data-changing CTE is not equivalent.
        await using var batch = new NpgsqlBatch(connection, transaction);
        var insert = new NpgsqlBatchCommand($"""
            INSERT INTO {_schema}.policy_bindings(scope_id,tenant,consumer,source,event_type,key_version,fingerprint_version)
            VALUES (@scope,@tenant,@consumer,@source,@event,@key_version,@fp_version) ON CONFLICT DO NOTHING
            """);
        insert.Parameters.AddWithValue("scope", scope.Id);
        insert.Parameters.AddWithValue("tenant", scope.Tenant);
        insert.Parameters.AddWithValue("consumer", scope.Consumer);
        insert.Parameters.AddWithValue("source", scope.Source);
        insert.Parameters.AddWithValue("event", scope.EventType);
        insert.Parameters.AddWithValue("key_version", identity.Policy.KeyVersion);
        insert.Parameters.AddWithValue("fp_version", identity.Policy.FingerprintVersion);
        batch.BatchCommands.Add(insert);
        var select = new NpgsqlBatchCommand($"SELECT key_version,fingerprint_version,tenant,consumer,source,event_type FROM {_schema}.policy_bindings WHERE scope_id=@scope FOR SHARE");
        select.Parameters.AddWithValue("scope", scope.Id);
        batch.BatchCommands.Add(select);
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) throw new PolicyMismatchException(scope.Id);
        WorkAcceptance.EnsurePolicyCompatible(identity, new(scope.Id, reader.GetString(2), reader.GetString(3),
            reader.GetString(4), reader.GetString(5), reader.GetString(0), reader.GetString(1)));
    }

    private async Task<AcceptanceResult> AcceptWithinTransactionAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, WorkSubmission submission, CancellationToken cancellationToken)
    {
        var identity = submission.Identity;
        var scope = identity.Scope;
        await EnsureBindingAsync(connection, transaction, identity, cancellationToken);
        var id = Guid.NewGuid();
        var payload = submission.CopyPayload();
        await using var insert = new NpgsqlCommand($"""
            INSERT INTO {_schema}.works(id,identity_id,scope_id,business_key,message_id,fingerprint,payload,state,next_attempt_at,expires_at,correlation_id)
            VALUES (@id,@identity,@scope,@key,@message,@fingerprint,@payload,0,COALESCE(@due,clock_timestamp()),@expires,@correlation)
            ON CONFLICT (identity_id) DO NOTHING RETURNING {Columns},business_key
            """, connection, transaction);
        insert.Parameters.AddWithValue("id", id);
        insert.Parameters.AddWithValue("identity", identity.Id);
        insert.Parameters.AddWithValue("scope", scope.Id);
        insert.Parameters.AddWithValue("key", identity.BusinessKey);
        insert.Parameters.AddWithValue("message", submission.MessageId);
        insert.Parameters.AddWithValue("fingerprint", submission.Fingerprint.Value);
        insert.Parameters.AddWithValue("payload", payload);
        AddOptional(insert, "due", NpgsqlDbType.TimestampTz, submission.NotBefore);
        AddOptional(insert, "expires", NpgsqlDbType.TimestampTz, submission.ExpiresAt);
        AddOptional(insert, "correlation", NpgsqlDbType.Text, submission.CorrelationId);
        WorkItem? work = null;
        AcceptanceStatus status = default;
        await using (var reader = await insert.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                work = ReadWork(reader);
                status = WorkAcceptance.Decide(submission, work, reader.GetString(14), inserted: true);
            }
        }
        if (work is null)
        {
            // A competing insert can commit while ON CONFLICT waits. Read it in a new
            // statement snapshot; combining this fallback into the INSERT can miss it.
            await using var select = new NpgsqlCommand($"SELECT {Columns},business_key FROM {_schema}.works WHERE identity_id=@identity", connection, transaction);
            select.Parameters.AddWithValue("identity", identity.Id);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken)) throw new InvalidOperationException("Accepted identity was not found.");
            work = ReadWork(reader);
            status = WorkAcceptance.Decide(submission, work, reader.GetString(14), inserted: false);
        }
        if (status == AcceptanceStatus.Conflict)
        {
            await using var conflict = new NpgsqlCommand($"INSERT INTO {_schema}.conflicts(id,work_id,message_id,fingerprint,payload) VALUES(@id,@work,@message,@fingerprint,@payload)", connection, transaction);
            conflict.Parameters.AddWithValue("id", Guid.NewGuid());
            conflict.Parameters.AddWithValue("work", work.Id);
            conflict.Parameters.AddWithValue("message", submission.MessageId);
            conflict.Parameters.AddWithValue("fingerprint", submission.Fingerprint.Value);
            conflict.Parameters.AddWithValue("payload", payload);
            await conflict.ExecuteNonQueryAsync(cancellationToken);
        }
        return new(status, work);
    }

    public async Task<WorkItem?> FindAsync(WorkScope scope, Guid workId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await ReadWorkAsync(connection, null, workId, scope.Id, false, cancellationToken);
    }

    public async Task<WorkClaim?> ClaimNextAsync(WorkScope scope, Guid claimRequestId, TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        WorkLifecycle.ValidateClaimRequest(claimRequestId, leaseDuration);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await LockOperationAsync(connection, transaction, "claim:" + claimRequestId, cancellationToken);
        // Repeating a committed claim must not claim another job, even if the old job has finished.
        await using (var previous = new NpgsqlCommand($"SELECT work_id FROM {_schema}.attempts WHERE id=@claim", connection, transaction))
        {
            previous.Parameters.AddWithValue("claim", claimRequestId);
            if (await previous.ExecuteScalarAsync(cancellationToken) is Guid previousId)
                return await ReadClaimAsync(connection, transaction, previousId, scope.Id, claimRequestId, cancellationToken);
        }

        var availability = await PostgreSqlTenantBudgetProtocol.AvailableAsync(connection, transaction, _schema, scope.Tenant, cancellationToken);
        // When both allowances are available the quota predicate is a tautology.
        // Omitting it also avoids false selectivity estimates after queue churn.
        var budgetFilter = availability.Fresh && availability.Retry ? "" : ClaimBudgetFilter;
        await using var select = new NpgsqlCommand($"""
            SELECT id FROM {_schema}.works
            WHERE state IN (0,1,2) AND scope_id=@scope
                AND (CASE WHEN state=1 THEN lease_until ELSE LEAST(next_attempt_at,expires_at) END)<=statement_timestamp()
                {budgetFilter}
            ORDER BY (CASE WHEN state=1 THEN lease_until ELSE LEAST(next_attempt_at,expires_at) END),id
            LIMIT 1 FOR UPDATE SKIP LOCKED
            """, connection, transaction);
        if (budgetFilter.Length > 0)
        {
            select.Parameters.AddWithValue("fresh", availability.Fresh ? 1 : 0);
            select.Parameters.AddWithValue("retry", availability.Retry ? 1 : 0);
        }
        select.Parameters.AddWithValue("scope", scope.Id);
        // Eligibility and ordering match ix_claim_due. A live lease remains excluded
        // even if its work deadline has passed. Read metadata only for the locked ID.
        if (await select.ExecuteScalarAsync(cancellationToken) is not Guid selectedId) return null;
        WorkItem selected;
        WorkLease? ownership;
        DateTimeOffset now;
        await using (var metadata = new NpgsqlCommand($"SELECT {Columns},lease_token,lease_until,clock_timestamp() FROM {_schema}.works WHERE id=@id", connection, transaction))
        {
            metadata.Parameters.AddWithValue("id", selectedId);
            await using var reader = await metadata.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken)) throw new InvalidOperationException("Locked work was not found.");
            selected = ReadWork(reader);
            ownership = reader.IsDBNull(14) ? null : new(reader.GetGuid(14), reader.GetFieldValue<DateTimeOffset>(15));
            // The candidate SELECT has already acquired the lock. Read database time
            // with the metadata, never from the statement that might wait for a lock.
            now = reader.GetFieldValue<DateTimeOffset>(16);
        }
        // SQL is an indexed candidate filter. Core rechecks eligibility under the held row lock.
        var decision = WorkLifecycle.Claim(selected, ownership, claimRequestId, leaseDuration, now);
        if (decision is null) return null;
        if (!(selected.ExpiresAt <= now) && !await PostgreSqlTenantBudgetProtocol.ConsumeAsync(connection, transaction, _schema,
            scope.Tenant, selected.AttemptCount == 0 ? TenantBudgetKind.Claim : TenantBudgetKind.RetryClaim, cancellationToken)) return null;
        if (decision.AbandonedAttempt is Guid abandoned)
            await FinishAttemptAsync(connection, transaction, abandoned, "Abandoned", "lease-recovered", cancellationToken);
        await PersistChangeAsync(connection, transaction, decision.Change, ownership?.Token, decision.Lease, cancellationToken);
        await using (var attempt = new NpgsqlCommand($"INSERT INTO {_schema}.attempts(id,work_id,number,generation,status,started_at) VALUES(@token,@work,@number,@generation,'Started',@now)", connection, transaction))
        {
            attempt.Parameters.AddWithValue("token", claimRequestId);
            attempt.Parameters.AddWithValue("work", selected.Id);
            attempt.Parameters.AddWithValue("number", decision.Change.After.AttemptCount);
            attempt.Parameters.AddWithValue("generation", decision.Change.After.ReplayGeneration);
            attempt.Parameters.AddWithValue("now", now);
            await attempt.ExecuteNonQueryAsync(cancellationToken);
        }
        var result = await ReadClaimAsync(connection, transaction, selected.Id, scope.Id, claimRequestId, cancellationToken);
        await CommitAsync(transaction, claimRequestId, cancellationToken);
        return result;
    }

    public async Task<ExecutionResult> ExecuteAsync(WorkClaim claim,
        Func<WorkItem, ReadOnlyMemory<byte>, PostgreSqlSession, CancellationToken, ValueTask> handler,
        RetryPolicy retryPolicy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(claim);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(retryPolicy);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var (work, current) = await ReadExecutionAsync(connection, transaction, claim, cancellationToken);
        var ownership = LeaseOf(current);
        var now = await DatabaseNowAsync(connection, transaction, cancellationToken);
        var admission = WorkLifecycle.AdmitExecution(work, ownership, claim.Token, now);
        if (admission == ExecutionAdmission.LeaseLost) return new(ExecutionStatus.LeaseLost, work);
        if (admission == ExecutionAdmission.Expire)
        {
            var decision = WorkLifecycle.Expire(work, ownership, claim.Token, now);
            var expired = await PersistChangeAsync(connection, transaction, decision.Change, claim.Token, null, cancellationToken);
            await FinishAttemptAsync(connection, transaction, claim.Token, decision.AttemptStatus, decision.Change.After.LastReason, cancellationToken);
            await CommitAsync(transaction, claim.Token, cancellationToken);
            return new(decision.Status, expired);
        }
        var commitStarted = false;
        try
        {
            // Reloaded bytes are authoritative; mutating a returned claim cannot replace durable input.
            await handler(work, current!.Payload, new PostgreSqlSession(connection, transaction, _schema, originWorkId: work.Id), cancellationToken);
            var decision = WorkLifecycle.Complete(work, ownership, claim.Token, now);
            var completed = await PersistChangeAsync(connection, transaction, decision.Change, claim.Token, null, cancellationToken);
            await FinishAttemptAsync(connection, transaction, claim.Token, decision.AttemptStatus, decision.Change.After.LastReason, cancellationToken);
            commitStarted = true;
            await CommitAsync(transaction, claim.Token, cancellationToken);
            return new(decision.Status, completed);
        }
        catch (Exception error)
        {
            if (commitStarted) throw;
            try { await transaction.RollbackAsync(CancellationToken.None); }
            catch (Exception rollbackError) { throw new CommitOutcomeUnknownException(claim.Token, rollbackError); }
            // Commit and rollback are never guessed from a missing Completed row.
            var kind = retryPolicy.Classify(error);
            return await RecordFailureAsync(connection, claim, retryPolicy, kind, CancellationToken.None);
        }
    }

    private async Task<ExecutionResult> RecordFailureAsync(NpgsqlConnection connection, WorkClaim claim, RetryPolicy policy, FailureKind kind, CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var work = await ReadWorkAsync(connection, transaction, claim.Work.Id, claim.Work.ScopeId, true, cancellationToken)
            ?? throw new InvalidOperationException("Work was removed during execution.");
        var current = await ReadClaimAsync(connection, transaction, work.Id, work.ScopeId, claim.Token, cancellationToken);
        var now = await DatabaseNowAsync(connection, transaction, cancellationToken);
        var decision = WorkLifecycle.Fail(work, LeaseOf(current), claim.Token, policy, kind, now, Random.Shared.NextDouble());
        if (decision is null) return new(ExecutionStatus.LeaseLost, work);
        var updated = await PersistChangeAsync(connection, transaction, decision.Change, claim.Token, null, cancellationToken);
        await FinishAttemptAsync(connection, transaction, claim.Token, decision.AttemptStatus, decision.Change.After.LastReason, cancellationToken);
        await CommitAsync(transaction, claim.Token, cancellationToken);
        return new(decision.Status, updated);
    }

    public async Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await LockOperationAsync(connection, transaction, "replay:" + request.Scope.Id + ":" + request.RequestId, cancellationToken);
        await using (var previous = new NpgsqlCommand($"SELECT fingerprint,work_id,result_status,result_generation,result_version FROM {_schema}.replay_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            previous.Parameters.AddWithValue("scope", request.Scope.Id);
            previous.Parameters.AddWithValue("id", request.RequestId);
            await using var reader = await previous.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
                return WorkLifecycle.RepeatReplay(request, reader.GetString(0),
                    new((ReplayStatus)reader.GetInt32(2), reader.GetGuid(1), reader.GetInt32(3), reader.GetInt64(4)));
        }
        var work = await ReadWorkAsync(connection, transaction, request.WorkId, request.Scope.Id, true, cancellationToken);
        var now = await DatabaseNowAsync(connection, transaction, cancellationToken);
        var payloadAvailable = true;
        if (work?.State == WorkState.DeadLetter)
        {
            await using var payloadState = new NpgsqlCommand($"SELECT payload_purged_at FROM {_schema}.works WHERE scope_id=@scope AND id=@id", connection, transaction);
            payloadState.Parameters.AddWithValue("scope", request.Scope.Id); payloadState.Parameters.AddWithValue("id", request.WorkId);
            payloadAvailable = await payloadState.ExecuteScalarAsync(cancellationToken) is DBNull;
        }
        var decision = WorkLifecycle.Replay(request, work, now, payloadAvailable);
        if (decision.Change is not null)
            decision = TenantBudgets.AdmitReplay(decision, await PostgreSqlTenantBudgetProtocol.ConsumeAsync(connection, transaction,
                _schema, request.Scope.Tenant, TenantBudgetKind.Recovery, cancellationToken));
        if (decision.Change is not null)
            await PersistChangeAsync(connection, transaction, decision.Change, null, null, cancellationToken);
        var result = decision.Result;
        await using var insert = new NpgsqlCommand($"""
            INSERT INTO {_schema}.replay_requests(scope_id,id,fingerprint,work_id,actor,reason,result_status,result_generation,result_version)
            VALUES(@scope,@id,@fingerprint,@work,@actor,@reason,@status,@generation,@version)
            """, connection, transaction);
        insert.Parameters.AddWithValue("scope", request.Scope.Id);
        insert.Parameters.AddWithValue("id", request.RequestId);
        insert.Parameters.AddWithValue("fingerprint", request.Fingerprint);
        insert.Parameters.AddWithValue("work", request.WorkId);
        insert.Parameters.AddWithValue("actor", request.Actor);
        insert.Parameters.AddWithValue("reason", request.Reason);
        insert.Parameters.AddWithValue("status", (int)result.Status);
        insert.Parameters.AddWithValue("generation", result.Generation);
        insert.Parameters.AddWithValue("version", result.Version);
        await insert.ExecuteNonQueryAsync(cancellationToken);
        await CommitAsync(transaction, request.RequestId, cancellationToken);
        return result;
    }

    private async Task<WorkItem?> ReadWorkAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction,
        Guid id, string scopeId, bool forUpdate, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand($"SELECT {Columns} FROM {_schema}.works WHERE id=@id AND scope_id=@scope" + (forUpdate ? " FOR UPDATE" : ""), connection, transaction);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("scope", scopeId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadWork(reader) : null;
    }

    private async Task<WorkClaim?> ReadClaimAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        Guid id, string scopeId, Guid token, CancellationToken cancellationToken)
    {
        // Keep exact-ID access separate from the ownership guard. Plain state/scope
        // predicates can pick ix_recovery and scan its dead entries after queue churn.
        await using var command = new NpgsqlCommand($"SELECT {Columns},lease_until,payload FROM {_schema}.works WHERE id=@id AND (scope_id,lease_token,state) IS NOT DISTINCT FROM (@scope,@token,1)", connection, transaction);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("scope", scopeId);
        command.Parameters.AddWithValue("token", token);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? new(ReadWork(reader), token, reader.GetFieldValue<DateTimeOffset>(14), reader.GetFieldValue<byte[]>(15)) : null;
    }

    private async Task<WorkItem> PersistChangeAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        WorkChange change, Guid? expectedToken, WorkLease? nextLease, CancellationToken cancellationToken)
    {
        var before = change.Before;
        var after = change.After;
        // Preserve every concurrency guard, including a NULL lease, without making
        // this point update eligible for a stale partial queue-index plan.
        await using var command = new NpgsqlCommand($"""
            UPDATE {_schema}.works SET state=@state,version=@version,attempt_count=@attempts,
                generation_attempt=@generation_attempt,replay_generation=@generation,
                lease_token=@token,lease_until=@until,next_attempt_at=@next,last_reason=@reason,
                completed_at=CASE WHEN @state=3 THEN clock_timestamp() ELSE completed_at END
            WHERE id=@id AND (scope_id,state,version,lease_token)
                IS NOT DISTINCT FROM (@scope,@expected_state,@expected_version,@expected_token) RETURNING {Columns}
            """, connection, transaction);
        command.Parameters.AddWithValue("state", (int)after.State);
        command.Parameters.AddWithValue("version", after.Version);
        command.Parameters.AddWithValue("attempts", after.AttemptCount);
        command.Parameters.AddWithValue("generation_attempt", after.GenerationAttempt);
        command.Parameters.AddWithValue("generation", after.ReplayGeneration);
        command.Parameters.AddWithValue("next", after.NextAttemptAt);
        command.Parameters.AddWithValue("id", before.Id);
        command.Parameters.AddWithValue("scope", before.ScopeId);
        command.Parameters.AddWithValue("expected_state", (int)before.State);
        command.Parameters.AddWithValue("expected_version", before.Version);
        AddOptional(command, "token", NpgsqlDbType.Uuid, nextLease?.Token);
        AddOptional(command, "until", NpgsqlDbType.TimestampTz, nextLease?.Until);
        AddOptional(command, "expected_token", NpgsqlDbType.Uuid, expectedToken);
        AddOptional(command, "reason", NpgsqlDbType.Text, after.LastReason);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadWork(reader) : throw new InvalidOperationException("Durable state changed during a locked transition.");
    }

    private static WorkLease? LeaseOf(WorkClaim? claim) => claim is null ? null : new(claim.Token, claim.LeaseUntil);

    private async Task<(WorkItem Work, WorkClaim? Claim)> ReadExecutionAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, WorkClaim requested, CancellationToken cancellationToken)
    {
        // Metadata, ownership and authoritative payload come from the same locked row.
        // The database clock is read by the caller after this lock has been acquired.
        await using var command = new NpgsqlCommand($"SELECT {Columns},lease_token,lease_until,payload FROM {_schema}.works WHERE id=@id AND scope_id=@scope FOR UPDATE", connection, transaction);
        command.Parameters.AddWithValue("id", requested.Work.Id);
        command.Parameters.AddWithValue("scope", requested.Work.ScopeId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) throw new InvalidOperationException("Claimed work does not exist in this scope.");
        var work = ReadWork(reader);
        var current = work.State == WorkState.Processing && !reader.IsDBNull(14) && reader.GetGuid(14) == requested.Token
            ? new WorkClaim(work, requested.Token, reader.GetFieldValue<DateTimeOffset>(15), reader.GetFieldValue<byte[]>(16)) : null;
        return (work, current);
    }

    private async Task FinishAttemptAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid id,
        string status, string? reason, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand($"UPDATE {_schema}.attempts SET status=@status,reason=@reason,finished_at=clock_timestamp() WHERE id=@id AND status='Started'", connection, transaction);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("status", status);
        AddOptional(command, "reason", NpgsqlDbType.Text, reason);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task LockOperationAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string key, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT pg_advisory_xact_lock(hashtextextended(@key,0))", connection, transaction);
        command.Parameters.AddWithValue("key", key);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<DateTimeOffset> DatabaseNowAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT clock_timestamp()", connection, transaction);
        return (DateTimeOffset)(await command.ExecuteScalarAsync(cancellationToken) is DateTime value
            ? new DateTimeOffset(value) : throw new InvalidOperationException("Database clock unavailable."));
    }

    private static async Task CommitAsync(NpgsqlTransaction transaction, Guid operationId, CancellationToken cancellationToken)
    {
        try { await transaction.CommitAsync(cancellationToken); }
        catch (Exception error) { throw new CommitOutcomeUnknownException(operationId, error); }
    }

    private static void AddOptional(NpgsqlCommand command, string name, NpgsqlDbType type, object? value) =>
        command.Parameters.AddWithValue(name, type, value ?? DBNull.Value);

    private static WorkItem ReadWork(NpgsqlDataReader reader) => new(
        reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetGuid(3), ContentFingerprint.Parse(reader.GetString(4)),
        (WorkState)reader.GetInt32(5), reader.GetInt64(6), reader.GetInt32(7), reader.GetInt32(8), reader.GetInt32(9),
        reader.GetFieldValue<DateTimeOffset>(10), reader.IsDBNull(11) ? null : reader.GetFieldValue<DateTimeOffset>(11),
        reader.IsDBNull(12) ? null : reader.GetString(12), reader.IsDBNull(13) ? null : reader.GetString(13));
}
