using System.Data;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

public sealed partial class SqlServerOutboxStore : IOutboxStore, ITransactionalProducer<SqlServerSession>
{
    private readonly SqlServerDataSource _dataSource;
    private readonly string _schema;
    private readonly SqlServerOutboxRepository _repository;

    public SqlServerOutboxStore(SqlServerDataSource dataSource, string schema = "eventguard")
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _schema = SqlServerProtocol.QuoteSchema(schema);
        _repository = new(_schema);
    }

    public Task InitializeAsync(CancellationToken cancellationToken = default) =>
        SqlServerSchema.InitializeAsync(_dataSource, _schema, cancellationToken);

    // Caller owns this transaction and its business idempotency. ProduceAsync owns both when desired.
    public SqlServerSession Enlist(SqlTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        if (transaction.IsolationLevel != IsolationLevel.ReadCommitted)
            throw new NotSupportedException("Enlisted outbox currently requires ReadCommitted isolation.");
        var connection = transaction.Connection ?? throw new InvalidOperationException("An active transaction is required.");
        return new(connection, transaction, _schema);
    }

    public async Task<ProducerResult> ProduceAsync(ProducerRequest request,
        Func<SqlServerSession, CancellationToken, ValueTask> handler, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(handler);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(IsolationLevel.ReadCommitted, cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction, "producer:" + _schema + ":" + request.Scope.Id + ":" + request.RequestId, cancellationToken);
        await using (var existing = new SqlCommand($"SELECT fingerprint FROM {_schema}.producer_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            existing.Parameters.AddValue("scope", request.Scope.Id);
            existing.Parameters.AddValue("id", request.RequestId);
            if (await existing.ExecuteScalarCheckedAsync(cancellationToken) is string recorded)
                return new(OutboxLifecycle.ExistingProducer(request, recorded), request.RequestId);
        }
        await using (var receipt = new SqlCommand($"INSERT INTO {_schema}.producer_requests(scope_id,id,fingerprint) VALUES(@scope,@id,@fingerprint)", connection, transaction))
        {
            receipt.Parameters.AddValue("scope", request.Scope.Id);
            receipt.Parameters.AddValue("id", request.RequestId);
            receipt.Parameters.AddValue("fingerprint", request.Fingerprint.Value);
            await receipt.ExecuteNonQueryCheckedAsync(cancellationToken);
        }
        await handler(new(connection, transaction, _schema, producerRequest: request), cancellationToken);
        await using (var completed = new SqlCommand($"UPDATE {_schema}.producer_requests SET committed_at=TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00') WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            completed.Parameters.AddValue("scope", request.Scope.Id);
            completed.Parameters.AddValue("id", request.RequestId);
            await completed.ExecuteNonQueryCheckedAsync(cancellationToken);
        }
        await SqlServerProtocol.CommitAsync(transaction, request.RequestId, cancellationToken);
        return new(ProducerStatus.Committed, request.RequestId);
    }

    public async Task<OutboxRecord?> FindAsync(ProducerScope scope, Guid intentId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return (await _repository.ReadAsync(connection, null, scope.Id, intentId, false, cancellationToken))?.RequireRecord();
    }

    public async Task<OutboxClaim?> ClaimNextAsync(ProducerScope scope, Guid requestId, TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        WorkLifecycle.ValidateClaimRequest(requestId, leaseDuration);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(IsolationLevel.RepeatableRead, cancellationToken);
        await SqlServerProtocol.LockAsync(connection, transaction, "outbox-claim:" + _schema + ":" + scope.Id + ":" + requestId, cancellationToken);
        await using (var previous = new SqlCommand($"SELECT intent_id FROM {_schema}.outbox_attempts WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            previous.Parameters.AddValue("scope", scope.Id);
            previous.Parameters.AddValue("id", requestId);
            if (await previous.ExecuteScalarCheckedAsync(cancellationToken) is Guid intentId)
            {
                var saved = await _repository.ReadAsync(connection, transaction, scope.Id, intentId, false, cancellationToken);
                return saved?.Lease?.Token == requestId ? new(saved.RequireRecord(), requestId, saved.Lease.Until) : null;
            }
        }
        var availability = await SqlServerTenantBudgetProtocol.AvailableAsync(connection, transaction, _schema, scope.Tenant, cancellationToken);
        var quarantined = 0;
        // Bound maintenance work and locks per poll. Null may mean maintenance-only progress.
        while (quarantined < 16)
        {
            SqlServerOutboxSnapshot? selected;
            await using (var select = new SqlCommand($"""
                SELECT TOP(1) {SqlServerOutboxRepository.Columns} FROM {_schema}.outbox_messages WITH(UPDLOCK,READPAST,ROWLOCK) WHERE scope_id=@scope
                    AND ((@fresh=1 AND attempt_count=0) OR (@retry=1 AND attempt_count>0))
                    AND ((state IN (0,2) AND next_attempt_at<=TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00')) OR (state=1 AND lease_until<=TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00')))
                ORDER BY next_attempt_at,id 
                """, connection, transaction))
            {
                select.Parameters.AddValue("scope", scope.Id);
                select.Parameters.AddValue("fresh", availability.Fresh ? 1 : 0); select.Parameters.AddValue("retry", availability.Retry ? 1 : 0);
                await using var reader = await select.ExecuteReaderCheckedAsync(cancellationToken);
                selected = await reader.ReadCheckedAsync(cancellationToken) ? SqlServerOutboxRepository.Read(reader) : null;
            }
            if (selected is null) break;
            if (selected.EnvelopeError is not null)
            {
                await _repository.QuarantineAsync(connection, transaction, selected, cancellationToken);
                quarantined++;
                continue;
            }
            var now = await SqlServerProtocol.NowAsync(connection, transaction, cancellationToken);
            var after = OutboxLifecycle.Claim(selected.RequireRecord(), selected.Lease, now);
            if (after is null) break;
            if (!await SqlServerTenantBudgetProtocol.ConsumeAsync(connection, transaction, _schema, scope.Tenant,
                selected.Metadata.AttemptCount == 0 ? TenantBudgetKind.Claim : TenantBudgetKind.RetryClaim, cancellationToken)) break;
            if (selected.Lease is not null)
                await _repository.FinishAttemptAsync(connection, transaction, scope.Id, selected.Lease.Token, "Unknown", "publish-interrupted", cancellationToken);
            var lease = new WorkLease(requestId, now + leaseDuration);
            await _repository.PersistAsync(connection, transaction, selected, SqlServerOutboxRepository.Metadata(after), lease, after.NextAttemptAt, cancellationToken);
            await using (var attempt = new SqlCommand($"INSERT INTO {_schema}.outbox_attempts(scope_id,id,intent_id,number,generation,status) VALUES(@scope,@id,@intent,@number,@generation,'Started')", connection, transaction))
            {
                attempt.Parameters.AddValue("scope", scope.Id);
                attempt.Parameters.AddValue("id", requestId);
                attempt.Parameters.AddValue("intent", after.Message.IntentId);
                attempt.Parameters.AddValue("number", after.AttemptCount);
                attempt.Parameters.AddValue("generation", after.ReplayGeneration);
                await attempt.ExecuteNonQueryCheckedAsync(cancellationToken);
            }
            await SqlServerProtocol.CommitAsync(transaction, requestId, cancellationToken);
            return new(after, requestId, lease.Until);
        }
        if (quarantined > 0) await SqlServerProtocol.CommitAsync(transaction, requestId, cancellationToken);
        return null;
    }

    public async Task<OutboxSettlement> SettleAsync(OutboxClaim claim, PublishOutcome outcome, RetryPolicy retryPolicy,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(claim);
        ArgumentNullException.ThrowIfNull(outcome);
        ArgumentNullException.ThrowIfNull(retryPolicy);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginLocalAsync(cancellationToken);
        var message = claim.Record.Message;
        var current = await _repository.ReadAsync(connection, transaction, message.Scope.Id, message.IntentId, true, cancellationToken)
            ?? throw new InvalidOperationException("Outbox message was not found.");
        var record = current.RequireRecord();
        if (record.State != OutboxState.Publishing || current.Lease?.Token != claim.Token
            || record.Version != claim.Record.Version)
            return new(OutboxSettlementStatus.LeaseLost, record);
        var now = await SqlServerProtocol.NowAsync(connection, transaction, cancellationToken);
        var after = OutboxLifecycle.Settle(record, outcome, retryPolicy, now, Random.Shared.NextDouble());
        await _repository.PersistAsync(connection, transaction, current, SqlServerOutboxRepository.Metadata(after), null, after.NextAttemptAt, cancellationToken);
        await _repository.FinishAttemptAsync(connection, transaction, message.Scope.Id, claim.Token, outcome.Status.ToString(), outcome.ReasonCode, cancellationToken);
        await SqlServerProtocol.CommitAsync(transaction, claim.Token, cancellationToken);
        return new(OutboxSettlementStatus.Recorded, after);
    }

}
