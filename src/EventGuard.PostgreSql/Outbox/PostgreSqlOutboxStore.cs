using System.Data;
using Npgsql;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlOutboxStore : IOutboxStore, ITransactionalProducer<PostgreSqlSession>
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _schema;
    private readonly PostgreSqlOutboxRepository _repository;

    public PostgreSqlOutboxStore(NpgsqlDataSource dataSource, string schema = "eventguard")
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _schema = PostgreSqlProtocol.QuoteSchema(schema);
        _repository = new(_schema);
    }

    public Task InitializeAsync(CancellationToken cancellationToken = default) =>
        PostgreSqlSchema.InitializeAsync(_dataSource, _schema, cancellationToken);

    // Caller owns this transaction and its business idempotency. ProduceAsync owns both when desired.
    public PostgreSqlSession Enlist(NpgsqlTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        if (transaction.IsolationLevel != IsolationLevel.ReadCommitted)
            throw new NotSupportedException("Enlisted outbox currently requires ReadCommitted isolation.");
        var connection = transaction.Connection ?? throw new InvalidOperationException("An active transaction is required.");
        return new(connection, transaction, _schema);
    }

    public async Task<ProducerResult> ProduceAsync(ProducerRequest request,
        Func<PostgreSqlSession, CancellationToken, ValueTask> handler, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(handler);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "producer:" + _schema + ":" + request.Scope.Id + ":" + request.RequestId, cancellationToken);
        await using (var existing = new NpgsqlCommand($"SELECT fingerprint FROM {_schema}.producer_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            existing.Parameters.AddWithValue("scope", request.Scope.Id);
            existing.Parameters.AddWithValue("id", request.RequestId);
            if (await existing.ExecuteScalarAsync(cancellationToken) is string recorded)
                return new(OutboxLifecycle.ExistingProducer(request, recorded), request.RequestId);
        }
        await using (var receipt = new NpgsqlCommand($"INSERT INTO {_schema}.producer_requests(scope_id,id,fingerprint) VALUES(@scope,@id,@fingerprint)", connection, transaction))
        {
            receipt.Parameters.AddWithValue("scope", request.Scope.Id);
            receipt.Parameters.AddWithValue("id", request.RequestId);
            receipt.Parameters.AddWithValue("fingerprint", request.Fingerprint.Value);
            await receipt.ExecuteNonQueryAsync(cancellationToken);
        }
        await handler(new(connection, transaction, _schema, producerRequest: request), cancellationToken);
        await using (var completed = new NpgsqlCommand($"UPDATE {_schema}.producer_requests SET committed_at=clock_timestamp() WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            completed.Parameters.AddWithValue("scope", request.Scope.Id);
            completed.Parameters.AddWithValue("id", request.RequestId);
            await completed.ExecuteNonQueryAsync(cancellationToken);
        }
        await PostgreSqlProtocol.CommitAsync(transaction, request.RequestId, cancellationToken);
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
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction, "outbox-claim:" + _schema + ":" + scope.Id + ":" + requestId, cancellationToken);
        await using (var previous = new NpgsqlCommand($"SELECT intent_id FROM {_schema}.outbox_attempts WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            previous.Parameters.AddWithValue("scope", scope.Id);
            previous.Parameters.AddWithValue("id", requestId);
            if (await previous.ExecuteScalarAsync(cancellationToken) is Guid intentId)
            {
                var saved = await _repository.ReadAsync(connection, transaction, scope.Id, intentId, false, cancellationToken);
                return saved?.Lease?.Token == requestId ? new(saved.RequireRecord(), requestId, saved.Lease.Until) : null;
            }
        }
        var availability = await PostgreSqlTenantBudgetProtocol.AvailableAsync(connection, transaction, _schema, scope.Tenant, cancellationToken);
        var quarantined = 0;
        // Bound maintenance work and locks per poll. Null may mean maintenance-only progress.
        while (quarantined < 16)
        {
            PostgreSqlOutboxSnapshot? selected;
            await using (var select = new NpgsqlCommand($"""
                SELECT {PostgreSqlOutboxRepository.Columns} FROM {_schema}.outbox_messages WHERE scope_id=@scope
                    AND ((@fresh=1 AND attempt_count=0) OR (@retry=1 AND attempt_count>0))
                    AND ((state IN (0,2) AND next_attempt_at<=clock_timestamp()) OR (state=1 AND lease_until<=clock_timestamp()))
                ORDER BY next_attempt_at,id LIMIT 1 FOR UPDATE SKIP LOCKED
                """, connection, transaction))
            {
                select.Parameters.AddWithValue("scope", scope.Id);
                select.Parameters.AddWithValue("fresh", availability.Fresh ? 1 : 0); select.Parameters.AddWithValue("retry", availability.Retry ? 1 : 0);
                await using var reader = await select.ExecuteReaderAsync(cancellationToken);
                selected = await reader.ReadAsync(cancellationToken) ? PostgreSqlOutboxRepository.Read(reader) : null;
            }
            if (selected is null) break;
            if (selected.EnvelopeError is not null)
            {
                await _repository.QuarantineAsync(connection, transaction, selected, cancellationToken);
                quarantined++;
                continue;
            }
            var now = await PostgreSqlProtocol.NowAsync(connection, transaction, cancellationToken);
            var after = OutboxLifecycle.Claim(selected.RequireRecord(), selected.Lease, now);
            if (after is null) break;
            if (!await PostgreSqlTenantBudgetProtocol.ConsumeAsync(connection, transaction, _schema, scope.Tenant,
                selected.Metadata.AttemptCount == 0 ? TenantBudgetKind.Claim : TenantBudgetKind.RetryClaim, cancellationToken)) break;
            if (selected.Lease is not null)
                await _repository.FinishAttemptAsync(connection, transaction, scope.Id, selected.Lease.Token, "Unknown", "publish-interrupted", cancellationToken);
            var lease = new WorkLease(requestId, now + leaseDuration);
            await _repository.PersistAsync(connection, transaction, selected, PostgreSqlOutboxRepository.Metadata(after), lease, after.NextAttemptAt, cancellationToken);
            await using (var attempt = new NpgsqlCommand($"INSERT INTO {_schema}.outbox_attempts(scope_id,id,intent_id,number,generation,status) VALUES(@scope,@id,@intent,@number,@generation,'Started')", connection, transaction))
            {
                attempt.Parameters.AddWithValue("scope", scope.Id);
                attempt.Parameters.AddWithValue("id", requestId);
                attempt.Parameters.AddWithValue("intent", after.Message.IntentId);
                attempt.Parameters.AddWithValue("number", after.AttemptCount);
                attempt.Parameters.AddWithValue("generation", after.ReplayGeneration);
                await attempt.ExecuteNonQueryAsync(cancellationToken);
            }
            await PostgreSqlProtocol.CommitAsync(transaction, requestId, cancellationToken);
            return new(after, requestId, lease.Until);
        }
        if (quarantined > 0) await PostgreSqlProtocol.CommitAsync(transaction, requestId, cancellationToken);
        return null;
    }

    public async Task<OutboxSettlement> SettleAsync(OutboxClaim claim, PublishOutcome outcome, RetryPolicy retryPolicy,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(claim);
        ArgumentNullException.ThrowIfNull(outcome);
        ArgumentNullException.ThrowIfNull(retryPolicy);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var message = claim.Record.Message;
        var current = await _repository.ReadAsync(connection, transaction, message.Scope.Id, message.IntentId, true, cancellationToken)
            ?? throw new InvalidOperationException("Outbox message was not found.");
        var record = current.RequireRecord();
        if (record.State != OutboxState.Publishing || current.Lease?.Token != claim.Token
            || record.Version != claim.Record.Version)
            return new(OutboxSettlementStatus.LeaseLost, record);
        var now = await PostgreSqlProtocol.NowAsync(connection, transaction, cancellationToken);
        var after = OutboxLifecycle.Settle(record, outcome, retryPolicy, now, Random.Shared.NextDouble());
        await _repository.PersistAsync(connection, transaction, current, PostgreSqlOutboxRepository.Metadata(after), null, after.NextAttemptAt, cancellationToken);
        await _repository.FinishAttemptAsync(connection, transaction, message.Scope.Id, claim.Token, outcome.Status.ToString(), outcome.ReasonCode, cancellationToken);
        await PostgreSqlProtocol.CommitAsync(transaction, claim.Token, cancellationToken);
        return new(OutboxSettlementStatus.Recorded, after);
    }

}
