namespace EventGuard.Operations;

/// <summary>Per-host read admission and a total deadline, including connection-pool wait.</summary>
public sealed partial class OperationsReader
{
    private readonly IWorkReadStore _store;
    private readonly SemaphoreSlim _slots;
    private readonly TimeSpan _timeout;

    public OperationsReader(IWorkReadStore store, int maximumConcurrency = 4, TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        if (maximumConcurrency is < 1 or > 16) throw new ArgumentOutOfRangeException(nameof(maximumConcurrency));
        _timeout = timeout ?? TimeSpan.FromSeconds(3);
        if (_timeout < TimeSpan.FromMilliseconds(50) || _timeout > TimeSpan.FromSeconds(10)) throw new ArgumentOutOfRangeException(nameof(timeout));
        _store = store;
        _slots = new(maximumConcurrency, maximumConcurrency);
    }

    public Task<ReadSnapshot<WorkPage>> ListAsync(WorkQuery query, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        return ReadAsync(token => _store.ListAsync(query, token), ct);
    }

    public Task<ReadSnapshot<WorkDetail?>> FindAsync(WorkScope scope, Guid id, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (id == Guid.Empty) throw new ArgumentException("A work ID is required.", nameof(id));
        return ReadAsync(token => _store.FindAsync(scope, id, token), ct);
    }

    public Task<ReadSnapshot<WorkData?>> ReadDataAsync(WorkScope scope, Guid id, WorkDataSelection selection, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(selection);
        if (id == Guid.Empty) throw new ArgumentException("A work ID is required.", nameof(id));
        return ReadAsync(token => _store is IWorkDataReadStore data
            ? data.ReadDataAsync(scope, id, selection, token)
            : Task.FromResult<WorkData?>(new(WorkDataStatus.Unsupported, [])), ct);
    }

    public Task<ReadSnapshot<OutboxPage>> ListOutboxAsync(OutboxQuery query, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        return ReadAsync(token => OutboxStore().ListOutboxAsync(query, token), ct);
    }

    public Task<ReadSnapshot<OutboxDetail?>> FindOutboxAsync(ProducerScope scope, Guid id, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (id == Guid.Empty) throw new ArgumentException("An intent ID is required.", nameof(id));
        return ReadAsync(token => OutboxStore().FindOutboxAsync(scope, id, token), ct);
    }

    public Task<ReadSnapshot<BacklogSummary>> ReadBacklogAsync(BacklogQuery query, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        return ReadAsync(token => _store is IBacklogReadStore backlog ? backlog.ReadBacklogAsync(query, token)
            : throw new NotSupportedException("Bounded state inventory is not supported."), ct);
    }

    public Task<ReadSnapshot<bool>> CheckHealthAsync(bool consumer, bool outbox, CancellationToken ct = default) => ReadAsync(async token =>
    {
        if (_store is not IStoreHealthCheck health) throw new NotSupportedException("Health validation is not supported.");
        await health.CheckHealthAsync(consumer, outbox, token).ConfigureAwait(false);
        return true;
    }, ct);

    private IOutboxReadStore OutboxStore() => _store as IOutboxReadStore
        ?? throw new NotSupportedException("This reader does not support outbox metadata.");

    private async Task<ReadSnapshot<T>> ReadAsync<T>(Func<CancellationToken, Task<T>> read, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (!await _slots.WaitAsync(0, ct).ConfigureAwait(false)) throw new OperationsUnavailableException("read-capacity");
        try
        {
            using var budget = CancellationTokenSource.CreateLinkedTokenSource(ct);
            budget.CancelAfter(_timeout);
            var start = DateTimeOffset.UtcNow;
            // Providers must honor cancellation; do not detach a timed-out query and release its slot early.
            var result = await read(budget.Token).ConfigureAwait(false);
            budget.Token.ThrowIfCancellationRequested();
            return new(result, start, DateTimeOffset.UtcNow);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
        catch (Exception) { throw new OperationsUnavailableException("read-unavailable"); }
        finally { _slots.Release(); }
    }
}

public sealed class OperationsUnavailableException(string code) : Exception("Operations metadata is temporarily unavailable.")
{
    public string Code { get; } = code;
}
