using Npgsql;

namespace EventGuard.PostgreSql;

// Trusted in-process recovery port. An external operations host must provide authorization.
public sealed partial class PostgreSqlOutboxRecoveryStore : IOutboxRecoveryStore, IOutboxRepairStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _schema;
    private readonly PostgreSqlOutboxRepository _repository;

    public PostgreSqlOutboxRecoveryStore(NpgsqlDataSource dataSource, string schema = "eventguard")
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _schema = PostgreSqlProtocol.QuoteSchema(schema);
        _repository = new(_schema);
    }

    public async Task<OutboxMetadata?> FindMetadataAsync(ProducerScope scope, Guid intentId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await _repository.ReadMetadataAsync(connection, scope, intentId, cancellationToken);
    }

    public async Task<OutboxReplayResult> ReplayAsync(OutboxReplayRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await PostgreSqlProtocol.LockAsync(connection, transaction,
            "outbox-replay:" + _schema + ":" + request.Scope.Id + ":" + request.RequestId, cancellationToken);
        // Receipts precede current state: even a rejected request must keep its original outcome.
        await using (var previous = new NpgsqlCommand($"SELECT fingerprint,intent_id,result_status,result_generation,result_version FROM {_schema}.outbox_replay_requests WHERE scope_id=@scope AND id=@id", connection, transaction))
        {
            previous.Parameters.AddWithValue("scope", request.Scope.Id);
            previous.Parameters.AddWithValue("id", request.RequestId);
            await using var reader = await previous.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
                return OutboxRecovery.RepeatReplay(request, reader.GetString(0),
                    new((OutboxReplayStatus)reader.GetInt32(2), reader.GetGuid(1), reader.GetInt32(3), reader.GetInt64(4)));
        }
        var current = await _repository.ReadAsync(connection, transaction, request.Scope.Id, request.IntentId, true, cancellationToken);
        var decision = OutboxRecovery.Replay(request, current?.Metadata);
        if (decision.After is not null)
        {
            if (current!.EnvelopeError is not null)
            {
                var quarantined = await _repository.QuarantineAsync(connection, transaction, current, cancellationToken);
                decision = OutboxRecovery.Replay(request, quarantined);
            }
            else
            {
                decision = TenantBudgets.AdmitReplay(decision, await PostgreSqlTenantBudgetProtocol.ConsumeAsync(connection, transaction,
                    _schema, request.Scope.Tenant, TenantBudgetKind.Recovery, cancellationToken));
                var now = await PostgreSqlProtocol.NowAsync(connection, transaction, cancellationToken);
                if (decision.After is not null) await _repository.PersistAsync(connection, transaction, current, decision.After, null, now, cancellationToken);
            }
        }
        var result = decision.Result;
        await using (var receipt = new NpgsqlCommand($"""
            INSERT INTO {_schema}.outbox_replay_requests(scope_id,id,fingerprint,intent_id,actor,reason,
                acknowledge_possible_duplicate,result_status,result_generation,result_version)
            VALUES(@scope,@id,@fingerprint,@intent,@actor,@reason,@ack,@status,@generation,@version)
            """, connection, transaction))
        {
            receipt.Parameters.AddWithValue("scope", request.Scope.Id);
            receipt.Parameters.AddWithValue("id", request.RequestId);
            receipt.Parameters.AddWithValue("fingerprint", request.Fingerprint);
            receipt.Parameters.AddWithValue("intent", request.IntentId);
            receipt.Parameters.AddWithValue("actor", request.Actor);
            receipt.Parameters.AddWithValue("reason", request.Reason);
            receipt.Parameters.AddWithValue("ack", request.AcknowledgePossibleDuplicate);
            receipt.Parameters.AddWithValue("status", (int)result.Status);
            receipt.Parameters.AddWithValue("generation", result.Generation);
            receipt.Parameters.AddWithValue("version", result.Version);
            await receipt.ExecuteNonQueryAsync(cancellationToken);
        }
        await PostgreSqlProtocol.CommitAsync(transaction, request.RequestId, cancellationToken);
        return result;
    }
}
