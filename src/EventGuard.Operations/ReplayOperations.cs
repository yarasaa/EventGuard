namespace EventGuard.Operations;

public sealed record ReplayReceipt(Guid RequestId, ReplayResult Result, string Reason, DateTimeOffset CreatedAt);

/// <summary>Optional command port. The host must authorize scope and operator identity before every call.</summary>
public interface IWorkReplayStore
{
    Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken ct = default);
    Task<ReplayReceipt?> FindReceiptAsync(WorkScope scope, Guid workId, Guid requestId, string actor, CancellationToken ct = default);
}

public sealed record OutboxReplayReceipt(Guid RequestId, OutboxReplayResult Result, string Reason,
    bool AcknowledgePossibleDuplicate, DateTimeOffset CreatedAt);

/// <summary>Optional producer command port. Requires separate producer/operator authorization in the host.</summary>
public interface IOutboxReplayStore
{
    Task<OutboxReplayResult> ReplayOutboxAsync(OutboxReplayRequest request, CancellationToken ct = default);
    Task<OutboxReplayReceipt?> FindOutboxReceiptAsync(ProducerScope scope, Guid intentId, Guid requestId, string actor, CancellationToken ct = default);
}

/// <summary>Bounded operations command admission; never retries a command or runs a business handler.</summary>
public sealed partial class OperationsReplayer
{
    private readonly IWorkReplayStore _store;
    private readonly SemaphoreSlim _slots;
    private readonly TimeSpan _timeout;

    public OperationsReplayer(IWorkReplayStore store, int maximumConcurrency = 2, TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        if (maximumConcurrency is < 1 or > 8) throw new ArgumentOutOfRangeException(nameof(maximumConcurrency));
        _timeout = timeout ?? TimeSpan.FromSeconds(3);
        if (_timeout < TimeSpan.FromMilliseconds(50) || _timeout > TimeSpan.FromSeconds(10)) throw new ArgumentOutOfRangeException(nameof(timeout));
        _store = store;
        _slots = new(maximumConcurrency, maximumConcurrency);
    }

    public Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CommandAsync(request.RequestId, token => _store.ReplayAsync(request, token), ct);
    }

    public Task<OutboxReplayResult> ReplayOutboxAsync(OutboxReplayRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (_store is not IOutboxReplayStore outbox) throw new ReplayUnavailableException(request.RequestId, false);
        return CommandAsync(request.RequestId, token => outbox.ReplayOutboxAsync(request, token), ct);
    }

    private async Task<T> CommandAsync<T>(Guid requestId, Func<CancellationToken, Task<T>> action, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (!await _slots.WaitAsync(0, ct).ConfigureAwait(false)) throw new ReplayUnavailableException(requestId, false);
        try
        {
            using var budget = CancellationTokenSource.CreateLinkedTokenSource(ct);
            budget.CancelAfter(_timeout);
            // A confirmed commit remains confirmed even if cancellation arrives just after its reply.
            return await action(budget.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
        catch (Exception) { throw new ReplayUnavailableException(requestId, true); }
        finally { _slots.Release(); }
    }

    public Task<ReplayReceipt?> FindReceiptAsync(WorkScope scope, Guid workId, Guid requestId, string actor, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ValidateLookup(workId, requestId, actor);
        return ReceiptAsync(token => _store.FindReceiptAsync(scope, workId, requestId, actor, token), ct);
    }

    public Task<OutboxReplayReceipt?> FindOutboxReceiptAsync(ProducerScope scope, Guid intentId, Guid requestId, string actor, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ValidateLookup(intentId, requestId, actor);
        if (_store is not IOutboxReplayStore outbox) throw new OperationsUnavailableException("read-unavailable");
        return ReceiptAsync(token => outbox.FindOutboxReceiptAsync(scope, intentId, requestId, actor, token), ct);
    }

    public Task<bool> CheckHealthAsync(bool consumer, bool outbox, CancellationToken ct = default) => ReceiptAsync(async token =>
    {
        if (_store is not IStoreHealthCheck health) throw new NotSupportedException("Health validation is not supported.");
        await health.CheckHealthAsync(consumer, outbox, token).ConfigureAwait(false);
        return true;
    }, ct);

    private static void ValidateLookup(Guid id, Guid requestId, string actor)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(actor);
        if (id == Guid.Empty || requestId == Guid.Empty || actor.Length > 1024) throw new ArgumentException("Invalid receipt lookup.");
    }

    private async Task<T> ReceiptAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken ct)
    {
        if (!await _slots.WaitAsync(0, ct).ConfigureAwait(false)) throw new OperationsUnavailableException("read-capacity");
        try
        {
            using var budget = CancellationTokenSource.CreateLinkedTokenSource(ct);
            budget.CancelAfter(_timeout);
            var receipt = await action(budget.Token).ConfigureAwait(false);
            budget.Token.ThrowIfCancellationRequested();
            return receipt;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
        catch (Exception) { throw new OperationsUnavailableException("read-unavailable"); }
        finally { _slots.Release(); }
    }
}

public sealed class ReplayUnavailableException(Guid requestId, bool outcomeUnconfirmed)
    : Exception("Replay response is unavailable. Keep the same request identity when checking or retrying.")
{
    public Guid RequestId { get; } = requestId;
    public bool OutcomeUnconfirmed { get; } = outcomeUnconfirmed;
}
