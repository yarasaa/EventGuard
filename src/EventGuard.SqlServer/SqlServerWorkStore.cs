using System.Data;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

public sealed partial class SqlServerWorkStore : ITransactionalWorkStore<SqlServerSession>, IIngressQuarantineStore, IRetentionStore
{
    private readonly SqlServerDataSource _dataSource;
    private readonly string _schema;
    private const string Columns = "id,identity_id,scope_id,message_id,fingerprint,state,version,attempt_count,generation_attempt,replay_generation,next_attempt_at,expires_at,last_reason,correlation_id";

    public SqlServerWorkStore(SqlServerDataSource dataSource, string schema = "eventguard")
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _schema = SqlServerProtocol.QuoteSchema(schema);
    }

    public Task InitializeAsync(CancellationToken cancellationToken = default) =>
        SqlServerSchema.InitializeAsync(_dataSource, _schema, cancellationToken);

    public async Task<AcceptanceResult> AcceptAsync(WorkSubmission submission, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(submission);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(IsolationLevel.ReadCommitted, cancellationToken);
        var result = await AcceptWithinTransactionAsync(connection, transaction, submission, cancellationToken);
        await CommitAsync(transaction, submission.MessageId, cancellationToken);
        return result;
    }

    private async Task EnsureBindingAsync(SqlConnection connection, SqlTransaction transaction, WorkIdentity identity, CancellationToken cancellationToken)
    {
        var scope = identity.Scope;
        // Existing policies take shared locks, so unrelated keys in the same scope can be accepted concurrently.
        await using (var exists = new SqlCommand($"SELECT 1 FROM {_schema}.policy_bindings WHERE scope_id=@scope", connection, transaction))
        {
            exists.Parameters.AddValue("scope", scope.Id);
            if (await exists.ExecuteScalarCheckedAsync(cancellationToken) is null)
            {
                await SqlServerProtocol.LockAsync(connection, transaction, "policy:" + _schema + ":" + scope.Id, cancellationToken);
                await using var binding = new SqlCommand($"""
                    IF NOT EXISTS(SELECT 1 FROM {_schema}.policy_bindings WHERE scope_id=@scope)
                        INSERT INTO {_schema}.policy_bindings(scope_id,tenant,consumer,source,event_type,key_version,fingerprint_version)
                        VALUES(@scope,@tenant,@consumer,@source,@event,@key_version,@fp_version)
                    """, connection, transaction);
                binding.Parameters.AddValue("scope", scope.Id);
                binding.Parameters.AddValue("tenant", scope.Tenant);
                binding.Parameters.AddValue("consumer", scope.Consumer);
                binding.Parameters.AddValue("source", scope.Source);
                binding.Parameters.AddValue("event", scope.EventType);
                binding.Parameters.AddValue("key_version", identity.Policy.KeyVersion);
                binding.Parameters.AddValue("fp_version", identity.Policy.FingerprintVersion);
                await binding.ExecuteNonQueryCheckedAsync(cancellationToken);
            }
        }
        await using (var binding = new SqlCommand($"SELECT key_version,fingerprint_version,tenant,consumer,source,event_type FROM {_schema}.policy_bindings WITH(HOLDLOCK,ROWLOCK) WHERE scope_id=@scope", connection, transaction))
        {
            binding.Parameters.AddValue("scope", scope.Id);
            await using var reader = await binding.ExecuteReaderCheckedAsync(cancellationToken);
            if (!await reader.ReadCheckedAsync(cancellationToken))
                throw new PolicyMismatchException(scope.Id);
            WorkAcceptance.EnsurePolicyCompatible(identity, new(scope.Id, reader.GetString(2), reader.GetString(3),
                reader.GetString(4), reader.GetString(5), reader.GetString(0), reader.GetString(1)));
        }
    }

    private async Task<AcceptanceResult> AcceptWithinTransactionAsync(SqlConnection connection, SqlTransaction transaction, WorkSubmission submission, CancellationToken cancellationToken)
    {
        var identity = submission.Identity;
        var scope = identity.Scope;
        await EnsureBindingAsync(connection, transaction, identity, cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction, "accept:" + _schema + ":" + identity.Id, cancellationToken);
        var id = Guid.NewGuid();
        var payload = submission.CopyPayload();
        await using var insert = new SqlCommand($"""
            IF NOT EXISTS(SELECT 1 FROM {_schema}.works WHERE identity_id=@identity)
            INSERT INTO {_schema}.works(id,identity_id,scope_id,business_key,message_id,fingerprint,payload,state,next_attempt_at,expires_at,correlation_id)
            OUTPUT INSERTED.id
            VALUES (@id,@identity,@scope,@key,@message,@fingerprint,@payload,0,COALESCE(@due,TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00')),@expires,@correlation)
            """, connection, transaction);
        insert.Parameters.AddValue("id", id);
        insert.Parameters.AddValue("identity", identity.Id);
        insert.Parameters.AddValue("scope", scope.Id);
        insert.Parameters.AddValue("key", identity.BusinessKey);
        insert.Parameters.AddValue("message", submission.MessageId);
        insert.Parameters.AddValue("fingerprint", submission.Fingerprint.Value);
        insert.Parameters.AddValue("payload", payload);
        AddOptional(insert, "due", SqlDbType.DateTimeOffset, submission.NotBefore);
        AddOptional(insert, "expires", SqlDbType.DateTimeOffset, submission.ExpiresAt);
        AddOptional(insert, "correlation", SqlDbType.NVarChar, submission.CorrelationId);
        var inserted = await insert.ExecuteScalarCheckedAsync(cancellationToken) is Guid;
        await using var select = new SqlCommand($"SELECT {Columns},business_key FROM {_schema}.works WHERE identity_id=@identity", connection, transaction);
        select.Parameters.AddValue("identity", identity.Id);
        WorkItem work;
        AcceptanceStatus status;
        await using (var reader = await select.ExecuteReaderCheckedAsync(cancellationToken))
        {
            if (!await reader.ReadCheckedAsync(cancellationToken)) throw new InvalidOperationException("Accepted identity was not found.");
            work = ReadWork(reader);
            status = WorkAcceptance.Decide(submission, work, reader.GetString(14), inserted);
        }
        if (status == AcceptanceStatus.Conflict)
        {
            await using var conflict = new SqlCommand($"INSERT INTO {_schema}.conflicts(id,work_id,message_id,fingerprint,payload) VALUES(@id,@work,@message,@fingerprint,@payload)", connection, transaction);
            conflict.Parameters.AddValue("id", Guid.NewGuid());
            conflict.Parameters.AddValue("work", work.Id);
            conflict.Parameters.AddValue("message", submission.MessageId);
            conflict.Parameters.AddValue("fingerprint", submission.Fingerprint.Value);
            conflict.Parameters.AddValue("payload", payload);
            await conflict.ExecuteNonQueryCheckedAsync(cancellationToken);
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
        await using var transaction = await connection.BeginLocalAsync(IsolationLevel.RepeatableRead, cancellationToken);
        await LockOperationAsync(connection, transaction, "claim:" + claimRequestId, cancellationToken);
        // Repeating a committed claim must not claim another job, even if the old job has finished.
        await using (var previous = new SqlCommand($"SELECT work_id FROM {_schema}.attempts WHERE id=@claim", connection, transaction))
        {
            previous.Parameters.AddValue("claim", claimRequestId);
            if (await previous.ExecuteScalarCheckedAsync(cancellationToken) is Guid previousId)
                return await ReadClaimAsync(connection, transaction, previousId, scope.Id, claimRequestId, cancellationToken);
        }

        // Stop after the first nonempty access path: selecting candidates from several
        // indexes at once can retain crossed update locks and deadlock during index writes.
        // Priority is expiry, recovered lease, then due time; no FIFO guarantee is made.
        var availability = await SqlServerTenantBudgetProtocol.AvailableAsync(connection, transaction, _schema, scope.Tenant, cancellationToken);
        await using var select = new SqlCommand($"""
            DECLARE @cutoff datetimeoffset(7)=TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00');
            SELECT TOP(1) {Columns},lease_token,lease_until
            FROM {_schema}.works WITH(UPDLOCK,READPAST,ROWLOCK,FORCESEEK(ix_claim_expiry(scope_id,expires_at)))
            WHERE scope_id=@scope AND state IN(0,2) AND expires_at<=@cutoff
            ORDER BY expires_at,id OPTION(MAXDOP 1);
            IF @@ROWCOUNT=0
            BEGIN
                SELECT TOP(1) {Columns},lease_token,lease_until
                FROM {_schema}.works WITH(UPDLOCK,READPAST,ROWLOCK,FORCESEEK(ix_recovery(scope_id,lease_until)))
                WHERE scope_id=@scope AND state=1 AND lease_until<=@cutoff AND (expires_at<=@cutoff OR @retry=1)
                ORDER BY lease_until,id OPTION(MAXDOP 1);
                IF @@ROWCOUNT=0
                BEGIN
                    SELECT TOP(1) {Columns},lease_token,lease_until
                    FROM {_schema}.works WITH(UPDLOCK,READPAST,ROWLOCK,FORCESEEK(ix_ready(scope_id,next_attempt_at)))
                    WHERE scope_id=@scope AND state IN(0,2) AND next_attempt_at<=@cutoff AND ((@fresh=1 AND attempt_count=0) OR (@retry=1 AND attempt_count>0))
                    ORDER BY next_attempt_at,id OPTION(MAXDOP 1);
                END
            END
            """, connection, transaction);
        select.Parameters.AddValue("fresh", availability.Fresh ? 1 : 0); select.Parameters.AddValue("retry", availability.Retry ? 1 : 0);
        select.Parameters.AddValue("scope", scope.Id);
        WorkItem? selected = null;
        WorkLease? ownership = null;
        await using (var reader = await select.ExecuteReaderCheckedAsync(cancellationToken))
        {
            do
            {
                if (!await reader.ReadCheckedAsync(cancellationToken)) continue;
                selected = ReadWork(reader);
                ownership = reader.IsDBNull(14) ? null : new(reader.GetGuid(14), reader.GetFieldValue<DateTimeOffset>(15));
                break;
            } while (await reader.NextResultCheckedAsync(cancellationToken));
        }
        if (selected is null) return null;
        var now = await DatabaseNowAsync(connection, transaction, cancellationToken);
        // SQL is an indexed candidate filter. Core rechecks eligibility under the held row lock.
        var decision = WorkLifecycle.Claim(selected, ownership, claimRequestId, leaseDuration, now);
        if (decision is null) return null;
        if (!(selected.ExpiresAt <= now) && !await SqlServerTenantBudgetProtocol.ConsumeAsync(connection, transaction, _schema,
            scope.Tenant, selected.AttemptCount == 0 ? TenantBudgetKind.Claim : TenantBudgetKind.RetryClaim, cancellationToken)) return null;
        if (decision.AbandonedAttempt is Guid abandoned)
            await FinishAttemptAsync(connection, transaction, abandoned, "Abandoned", "lease-recovered", cancellationToken);
        await PersistChangeAsync(connection, transaction, decision.Change, ownership?.Token, decision.Lease, cancellationToken);
        await using (var attempt = new SqlCommand($"INSERT INTO {_schema}.attempts(id,work_id,number,generation,status,started_at) VALUES(@token,@work,@number,@generation,'Started',@now)", connection, transaction))
        {
            attempt.Parameters.AddValue("token", claimRequestId);
            attempt.Parameters.AddValue("work", selected.Id);
            attempt.Parameters.AddValue("number", decision.Change.After.AttemptCount);
            attempt.Parameters.AddValue("generation", decision.Change.After.ReplayGeneration);
            attempt.Parameters.AddValue("now", now);
            await attempt.ExecuteNonQueryCheckedAsync(cancellationToken);
        }
        var result = await ReadClaimAsync(connection, transaction, selected.Id, scope.Id, claimRequestId, cancellationToken);
        await CommitAsync(transaction, claimRequestId, cancellationToken);
        return result;
    }

    public async Task<ExecutionResult> ExecuteAsync(WorkClaim claim,
        Func<WorkItem, ReadOnlyMemory<byte>, SqlServerSession, CancellationToken, ValueTask> handler,
        RetryPolicy retryPolicy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(claim);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(retryPolicy);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(cancellationToken);
        var work = await ReadWorkAsync(connection, transaction, claim.Work.Id, claim.Work.ScopeId, true, cancellationToken)
            ?? throw new InvalidOperationException("Claimed work does not exist in this scope.");
        var current = await ReadClaimAsync(connection, transaction, work.Id, work.ScopeId, claim.Token, cancellationToken);
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
            await handler(work, current!.Payload, new SqlServerSession(connection, transaction, _schema, originWorkId: work.Id), cancellationToken);
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
            var kind = cancellationToken.IsCancellationRequested ? FailureKind.Cancelled : retryPolicy.Classify(error);
            return await RecordFailureAsync(connection, claim, retryPolicy, kind, CancellationToken.None);
        }
    }

    private async Task<ExecutionResult> RecordFailureAsync(SqlConnection connection, WorkClaim claim, RetryPolicy policy, FailureKind kind, CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginLocalAsync(cancellationToken);
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
        await using var transaction = await connection.BeginLocalAsync(cancellationToken);
        await LockOperationAsync(connection, transaction, "replay:" + request.Scope.Id + ":" + request.RequestId, cancellationToken);
        await using (var previous = new SqlCommand($"SELECT fingerprint,work_id,result_status,result_generation,result_version FROM {_schema}.replay_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            previous.Parameters.AddValue("scope", request.Scope.Id);
            previous.Parameters.AddValue("id", request.RequestId);
            await using var reader = await previous.ExecuteReaderCheckedAsync(cancellationToken);
            if (await reader.ReadCheckedAsync(cancellationToken))
                return WorkLifecycle.RepeatReplay(request, reader.GetString(0),
                    new((ReplayStatus)reader.GetInt32(2), reader.GetGuid(1), reader.GetInt32(3), reader.GetInt64(4)));
        }
        var work = await ReadWorkAsync(connection, transaction, request.WorkId, request.Scope.Id, true, cancellationToken);
        var now = await DatabaseNowAsync(connection, transaction, cancellationToken);
        var payloadAvailable = true;
        if (work?.State == WorkState.DeadLetter)
        {
            await using var payloadState = new SqlCommand($"SELECT payload_purged_at FROM {_schema}.works WHERE scope_id=@scope AND id=@id", connection, transaction);
            payloadState.Parameters.AddValue("scope", request.Scope.Id); payloadState.Parameters.AddValue("id", request.WorkId);
            payloadAvailable = await payloadState.ExecuteScalarCheckedAsync(cancellationToken) is DBNull;
        }
        var decision = WorkLifecycle.Replay(request, work, now, payloadAvailable);
        if (decision.Change is not null)
            decision = TenantBudgets.AdmitReplay(decision, await SqlServerTenantBudgetProtocol.ConsumeAsync(connection, transaction,
                _schema, request.Scope.Tenant, TenantBudgetKind.Recovery, cancellationToken));
        if (decision.Change is not null)
            await PersistChangeAsync(connection, transaction, decision.Change, null, null, cancellationToken);
        var result = decision.Result;
        await using var insert = new SqlCommand($"""
            INSERT INTO {_schema}.replay_requests(scope_id,id,fingerprint,work_id,actor,reason,result_status,result_generation,result_version)
            VALUES(@scope,@id,@fingerprint,@work,@actor,@reason,@status,@generation,@version)
            """, connection, transaction);
        insert.Parameters.AddValue("scope", request.Scope.Id);
        insert.Parameters.AddValue("id", request.RequestId);
        insert.Parameters.AddValue("fingerprint", request.Fingerprint);
        insert.Parameters.AddValue("work", request.WorkId);
        insert.Parameters.AddValue("actor", request.Actor);
        insert.Parameters.AddValue("reason", request.Reason);
        insert.Parameters.AddValue("status", (int)result.Status);
        insert.Parameters.AddValue("generation", result.Generation);
        insert.Parameters.AddValue("version", result.Version);
        await insert.ExecuteNonQueryCheckedAsync(cancellationToken);
        await CommitAsync(transaction, request.RequestId, cancellationToken);
        return result;
    }

    private async Task<WorkItem?> ReadWorkAsync(SqlConnection connection, SqlTransaction? transaction,
        Guid id, string scopeId, bool forUpdate, CancellationToken cancellationToken)
    {
        // XLOCK also blocks a shared clustered lookup from a competing nonclustered READPAST scan.
        await using var command = new SqlCommand($"SELECT {Columns} FROM {_schema}.works" + (forUpdate ? " WITH(XLOCK,ROWLOCK)" : "") + " WHERE id=@id AND scope_id=@scope", connection, transaction);
        command.Parameters.AddValue("id", id);
        command.Parameters.AddValue("scope", scopeId);
        await using var reader = await command.ExecuteReaderCheckedAsync(cancellationToken);
        return await reader.ReadCheckedAsync(cancellationToken) ? ReadWork(reader) : null;
    }

    private async Task<WorkClaim?> ReadClaimAsync(SqlConnection connection, SqlTransaction transaction,
        Guid id, string scopeId, Guid token, CancellationToken cancellationToken)
    {
        await using var command = new SqlCommand($"SELECT {Columns},lease_until,payload FROM {_schema}.works WHERE id=@id AND scope_id=@scope AND lease_token=@token AND state=1", connection, transaction);
        command.Parameters.AddValue("id", id);
        command.Parameters.AddValue("scope", scopeId);
        command.Parameters.AddValue("token", token);
        await using var reader = await command.ExecuteReaderCheckedAsync(cancellationToken);
        return await reader.ReadCheckedAsync(cancellationToken)
            ? new(ReadWork(reader), token, reader.GetFieldValue<DateTimeOffset>(14), reader.GetFieldValue<byte[]>(15)) : null;
    }

    private async Task<WorkItem> PersistChangeAsync(SqlConnection connection, SqlTransaction transaction,
        WorkChange change, Guid? expectedToken, WorkLease? nextLease, CancellationToken cancellationToken)
    {
        var before = change.Before;
        var after = change.After;
        await using var command = new SqlCommand($"""
            UPDATE {_schema}.works SET state=@state,version=@version,attempt_count=@attempts,
                generation_attempt=@generation_attempt,replay_generation=@generation,
                lease_token=@token,lease_until=@until,next_attempt_at=@next,last_reason=@reason,
                completed_at=CASE WHEN @state=3 THEN TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00') ELSE completed_at END
            OUTPUT INSERTED.id,INSERTED.identity_id,INSERTED.scope_id,INSERTED.message_id,INSERTED.fingerprint,INSERTED.state,INSERTED.version,INSERTED.attempt_count,INSERTED.generation_attempt,INSERTED.replay_generation,INSERTED.next_attempt_at,INSERTED.expires_at,INSERTED.last_reason,INSERTED.correlation_id
            WHERE id=@id AND scope_id=@scope AND state=@expected_state AND version=@expected_version
                AND (lease_token=@expected_token OR (lease_token IS NULL AND @expected_token IS NULL))
            """, connection, transaction);
        command.Parameters.AddValue("state", (int)after.State);
        command.Parameters.AddValue("version", after.Version);
        command.Parameters.AddValue("attempts", after.AttemptCount);
        command.Parameters.AddValue("generation_attempt", after.GenerationAttempt);
        command.Parameters.AddValue("generation", after.ReplayGeneration);
        command.Parameters.AddValue("next", after.NextAttemptAt);
        command.Parameters.AddValue("id", before.Id);
        command.Parameters.AddValue("scope", before.ScopeId);
        command.Parameters.AddValue("expected_state", (int)before.State);
        command.Parameters.AddValue("expected_version", before.Version);
        AddOptional(command, "token", SqlDbType.UniqueIdentifier, nextLease?.Token);
        AddOptional(command, "until", SqlDbType.DateTimeOffset, nextLease?.Until);
        AddOptional(command, "expected_token", SqlDbType.UniqueIdentifier, expectedToken);
        AddOptional(command, "reason", SqlDbType.NVarChar, after.LastReason);
        await using var reader = await command.ExecuteReaderCheckedAsync(cancellationToken);
        return await reader.ReadCheckedAsync(cancellationToken) ? ReadWork(reader) : throw new InvalidOperationException("Durable state changed during a locked transition.");
    }

    private static WorkLease? LeaseOf(WorkClaim? claim) => claim is null ? null : new(claim.Token, claim.LeaseUntil);

    private async Task FinishAttemptAsync(SqlConnection connection, SqlTransaction transaction, Guid id,
        string status, string? reason, CancellationToken cancellationToken)
    {
        await using var command = new SqlCommand($"UPDATE {_schema}.attempts SET status=@status,reason=@reason,finished_at=TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00') WHERE id=@id AND status='Started'", connection, transaction);
        command.Parameters.AddValue("id", id);
        command.Parameters.AddValue("status", status);
        AddOptional(command, "reason", SqlDbType.NVarChar, reason);
        await command.ExecuteNonQueryCheckedAsync(cancellationToken);
    }

    private Task LockOperationAsync(SqlConnection connection, SqlTransaction transaction, string key, CancellationToken ct) =>
        SqlServerProtocol.LockAsync(connection, transaction, _schema + ":" + key, ct);
    private static Task<DateTimeOffset> DatabaseNowAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken ct) =>
        SqlServerProtocol.NowAsync(connection, transaction, ct);
    private static Task CommitAsync(SqlTransaction transaction, Guid id, CancellationToken ct) => SqlServerProtocol.CommitAsync(transaction, id, ct);
    private static void AddOptional(SqlCommand command, string name, SqlDbType type, object? value) => SqlServerProtocol.Optional(command, name, type, value);

    private static WorkItem ReadWork(SqlDataReader reader) => new(
        reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetGuid(3), ContentFingerprint.Parse(reader.GetString(4)),
        (WorkState)reader.GetInt32(5), reader.GetInt64(6), reader.GetInt32(7), reader.GetInt32(8), reader.GetInt32(9),
        reader.GetFieldValue<DateTimeOffset>(10), reader.IsDBNull(11) ? null : reader.GetFieldValue<DateTimeOffset>(11),
        reader.IsDBNull(12) ? null : reader.GetString(12), reader.IsDBNull(13) ? null : reader.GetString(13));
}
