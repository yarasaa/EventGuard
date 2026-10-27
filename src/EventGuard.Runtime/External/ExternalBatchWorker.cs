using System.Diagnostics;

namespace EventGuard.Runtime;

public sealed class ExternalBatchWorkerOptions
{
    public int MaximumBatchSize { get; }
    public WorkPumpOptions Pump { get; }
    public TimeSpan CollectionWindow { get; }

    public ExternalBatchWorkerOptions(int maximumBatchSize = 100, WorkPumpOptions? pump = null,
        TimeSpan? collectionWindow = null)
    {
        if (maximumBatchSize is < 1 or > 100) throw new ArgumentOutOfRangeException(nameof(maximumBatchSize));
        MaximumBatchSize = maximumBatchSize;
        Pump = pump ?? new(maximumConcurrency: 1);
        CollectionWindow = collectionWindow ?? TimeSpan.FromSeconds(1);
        if (CollectionWindow <= TimeSpan.Zero || CollectionWindow > TimeSpan.FromSeconds(5))
            throw new ArgumentOutOfRangeException(nameof(collectionWindow));
    }
}

public sealed record ExternalBatchRunResult(string AttemptId, int InvokedItemCount,
    IReadOnlyList<ExternalSettlement> Settlements, int UnmatchedResponseCount);

/// <summary>One provider call for up to 100 independently leased actions. No database transaction spans the call.</summary>
public sealed class ExternalBatchWorker
{
    private readonly IExternalActionStore _store;
    private readonly ProducerScope _scope;
    private readonly IExternalBatchServiceAdapter _adapter;
    private readonly ExternalPhase _phase;
    private readonly ExternalActionPolicy _policy;
    private readonly ExternalBatchWorkerOptions _options;
    private readonly ExternalCallLimit? _limit;
    private readonly IExternalCallLimiter? _limiter;

    public ExternalBatchWorker(IExternalActionStore store, ProducerScope scope, IExternalBatchServiceAdapter adapter,
        ExternalPhase phase, ExternalActionPolicy? policy = null, ExternalBatchWorkerOptions? options = null,
        ExternalCallLimit? limit = null, IExternalCallLimiter? limiter = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _scope = scope ?? throw new ArgumentNullException(nameof(scope));
        _adapter = adapter ?? throw new ArgumentNullException(nameof(adapter));
        _ = new ProducerScope(adapter.Binding, adapter.BindingVersion);
        if (!Enum.IsDefined(phase)) throw new ArgumentOutOfRangeException(nameof(phase));
        if (phase == ExternalPhase.Reconcile && !adapter.CanReconcile || phase == ExternalPhase.Observe && !adapter.CanObserve)
            throw new NotSupportedException("Adapter does not support the selected batch phase.");
        if ((limit is null) != (limiter is null)) throw new ArgumentException("Supply both a shared limit and its limiter.");
        _phase = phase; _policy = policy ?? new(); _options = options ?? new(); _limit = limit; _limiter = limiter;
    }

    public async Task<ExternalBatchRunResult?> RunOnceAsync(CancellationToken cancellationToken = default)
    {
        ExternalCallPermit? permit = null;
        if (_limiter is not null)
        {
            permit = await _limiter.TryAcquireCallAsync(_limit!, Guid.NewGuid(), _options.Pump.LeaseDuration, cancellationToken);
            if (permit is null) return null;
        }
        var invoked = false;
        var outcome = ExternalCallOutcome.Unhealthy;
        try
        {
            var claims = new List<ExternalActionClaim>();
            var started = Stopwatch.GetTimestamp();
            var collectionBudget = TimeSpan.FromTicks(Math.Min(_options.CollectionWindow.Ticks, _options.Pump.LeaseDuration.Ticks / 4));
            try
            {
                for (var i = 0; i < _options.MaximumBatchSize; i++)
                {
                    if (i > 0 && Stopwatch.GetElapsedTime(started) >= collectionBudget) break;
                    var claim = await _store.ClaimActionAsync(_scope, _adapter.Binding, _adapter.BindingVersion,
                        _phase, Guid.NewGuid(), _options.Pump.LeaseDuration, cancellationToken);
                    if (claim is null) break;
                    claims.Add(claim);
                }
            }
            catch
            {
                // These known claims have not reached the adapter. An ambiguous, unreturned claim still expires to Unknown.
                if (claims.Count > 0)
                    await SettleAsync(claims, claims.ToDictionary(c => c.Record.Action.Id, _ => NotInvoked()));
                throw;
            }
            if (claims.Count == 0) return null;
            var batch = new ExternalBatchRequest(claims);
            var results = new Dictionary<Guid, ExternalResult>();
            var callable = new List<ExternalActionClaim>();
            foreach (var claim in claims)
            {
                if (_phase == ExternalPhase.Submit && claim.Record.Action.ObservationTimeout.HasValue
                    && !_adapter.CanObserve && !_adapter.CanReceiveCallbacks)
                    results[claim.Record.Action.Id] = new(ExternalResultKind.Rejected, "observation-capability-missing");
                else callable.Add(claim);
            }
            var invokedItems = 0;
            var unmatched = 0;
            if (callable.Count > 0)
            {
                using var call = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                var deadline = claims.Min(c => c.LeaseUntil);
                if (permit is not null && permit.ExpiresAt < deadline) deadline = permit.ExpiresAt;
                var remaining = deadline - DateTimeOffset.UtcNow;
                call.CancelAfter(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
                if (remaining <= TimeSpan.Zero || call.IsCancellationRequested)
                    foreach (var claim in callable) results[claim.Record.Action.Id] = NotInvoked();
                else
                {
                    IReadOnlyList<ExternalBatchItemResult>? response = null;
                    try
                    {
                        invoked = true; invokedItems = callable.Count;
                        batch = new ExternalBatchRequest(callable);
                        response = await _adapter.ExecuteAsync(batch, call.Token);
                        // Copy the complete response before applying it; a broken collection is ambiguous evidence.
                        response = response?.Take(101).ToArray();
                        if (response?.Count > 100) response = null;
                    }
                    catch (Exception) { response = null; }
                    var expected = callable.Select(c => c.Record.Action.Id).ToHashSet();
                    unmatched = response?.Count(r => r is null || !expected.Contains(r.ActionId)) ?? 0;
                    var byId = (response ?? []).Where(r => r is not null && expected.Contains(r.ActionId))
                        .GroupBy(r => r.ActionId).ToDictionary(g => g.Key, g => g.ToArray());
                    foreach (var claim in callable)
                    {
                        var id = claim.Record.Action.Id;
                        results[id] = byId.TryGetValue(id, out var entries) && entries.Length == 1
                            && entries[0].Result is { } result && Valid(result.Kind)
                            ? result : new(ExternalResultKind.Unknown, "batch-result-ambiguous");
                    }
                }
            }
            outcome = results.Values.Any(r => r.Kind is ExternalResultKind.Unknown or ExternalResultKind.NotAcceptedRetryable)
                ? ExternalCallOutcome.Unhealthy : ExternalCallOutcome.Healthy;
            var settlements = await SettleAsync(claims, results);
            return new(batch.AttemptId, invokedItems, settlements, unmatched);
        }
        finally
        {
            if (permit is not null)
            {
                using var finish = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await _limiter!.CompleteCallAsync(permit, invoked ? outcome : ExternalCallOutcome.Unused, finish.Token);
            }
        }
    }

    private ExternalResult NotInvoked() => new(_phase == ExternalPhase.Submit
        ? ExternalResultKind.NotAcceptedRetryable : ExternalResultKind.Unknown, "batch-not-invoked");

    private bool Valid(ExternalResultKind kind) => _phase == ExternalPhase.Observe
        ? kind is ExternalResultKind.Pending or ExternalResultKind.Unknown or ExternalResultKind.Succeeded or ExternalResultKind.Failed
        : kind is ExternalResultKind.Accepted or ExternalResultKind.Rejected or ExternalResultKind.NotAcceptedRetryable or ExternalResultKind.Unknown;

    private async Task<IReadOnlyList<ExternalSettlement>> SettleAsync(IReadOnlyList<ExternalActionClaim> claims,
        IReadOnlyDictionary<Guid, ExternalResult> results)
    {
        var settled = new List<ExternalSettlement>();
        var errors = new List<Exception>();
        using var finish = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        foreach (var claim in claims)
        {
            try { settled.Add(await _store.SettleActionAsync(claim, results[claim.Record.Action.Id], _policy, finish.Token)); }
            catch (Exception error) { errors.Add(error); }
        }
        // Preserve other item results even if one write failed; never repeat the remote call to repair a status write.
        if (errors.Count > 0) throw new AggregateException("One or more batch results could not be persisted. Reconcile unresolved actions.", errors);
        return settled.AsReadOnly();
    }

    public async Task RunAsync(CancellationToken stoppingToken = default)
    {
        await _store.ValidateExternalActionsAsync(stoppingToken);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        await Task.WhenAll(Enumerable.Range(0, _options.Pump.MaximumConcurrency).Select(_ => Loop()));
        async Task Loop()
        {
            try
            {
                while (!stop.IsCancellationRequested)
                    if (await RunOnceAsync(stop.Token) is null) await Task.Delay(_options.Pump.IdleDelay, stop.Token);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
            catch { stop.Cancel(); throw; }
        }
    }
}
