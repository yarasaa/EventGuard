namespace EventGuard.Runtime;

/// <summary>Runs one phase independently; provider calls never hold a store transaction.</summary>
public sealed class ExternalActionWorker
{
    private readonly IExternalActionStore _store;
    private readonly ProducerScope _scope;
    private readonly IExternalServiceAdapter _adapter;
    private readonly ExternalPhase _phase;
    private readonly ExternalActionPolicy _policy;
    private readonly WorkPumpOptions _options;
    private readonly ExternalCallLimit? _limit;
    private readonly IExternalCallLimiter? _limiter;
    public ExternalActionWorker(IExternalActionStore store, ProducerScope scope, IExternalServiceAdapter adapter,
        ExternalPhase phase, ExternalActionPolicy? policy = null, WorkPumpOptions? options = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store)); _scope = scope ?? throw new ArgumentNullException(nameof(scope));
        _adapter = adapter ?? throw new ArgumentNullException(nameof(adapter));
        if (!Enum.IsDefined(phase)) throw new ArgumentOutOfRangeException(nameof(phase));
        if (phase == ExternalPhase.Reconcile && !adapter.CanReconcile || phase == ExternalPhase.Observe && !adapter.CanObserve)
            throw new NotSupportedException("Adapter does not support the selected phase.");
        _phase = phase; _policy = policy ?? new(); _options = options ?? new();
    }

    public ExternalActionWorker(IExternalActionStore store, ProducerScope scope, IExternalServiceAdapter adapter,
        ExternalPhase phase, ExternalCallLimit limit, IExternalCallLimiter limiter,
        ExternalActionPolicy? policy = null, WorkPumpOptions? options = null) : this(store, scope, adapter, phase, policy, options)
    {
        _limit = limit ?? throw new ArgumentNullException(nameof(limit));
        _limiter = limiter ?? throw new ArgumentNullException(nameof(limiter));
        _maintenance = new(_limiter, _limit, _options);
    }

    private readonly ExternalPermitMaintenance? _maintenance;

    public async Task<ExternalSettlement?> RunOnceAsync(CancellationToken cancellationToken = default)
    {
        ExternalCallPermit? permit = null;
        if (_limiter is not null)
        {
            await _maintenance!.RunAsync(cancellationToken);
            if (_store is IExternalActionAvailability available
                && !await available.HasDueActionsAsync(_scope, _adapter.Binding, _adapter.BindingVersion, _phase, cancellationToken)) return null;
            permit = await _limiter.TryAcquireCallAsync(_limit!, ExternalCallRequestIds.New(), _options.LeaseDuration, cancellationToken);
            if (permit is null) return null;
        }
        var invoked = false; var outcome = ExternalCallOutcome.Unhealthy;
        try
        {
            var claim = await _store.ClaimActionAsync(_scope, _adapter.Binding, _adapter.BindingVersion, _phase,
                Guid.NewGuid(), _options.LeaseDuration, cancellationToken);
            if (claim is null) return null;
            ExternalResult result;
            using var call = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var deadline = permit is not null && permit.ExpiresAt < claim.LeaseUntil ? permit.ExpiresAt : claim.LeaseUntil;
            var remaining = deadline - DateTimeOffset.UtcNow;
            if (_options.ExternalCallTimeout is { } timeout && timeout < remaining) remaining = timeout;
            call.CancelAfter(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
            if (_phase == ExternalPhase.Submit && claim.Record.Action.ObservationTimeout.HasValue && !_adapter.CanObserve && _adapter is not ICallbackExternalServiceAdapter)
                result = new(ExternalResultKind.Rejected, "observation-capability-missing");
            else if (remaining <= TimeSpan.Zero || cancellationToken.IsCancellationRequested || call.IsCancellationRequested)
                result = new(_phase == ExternalPhase.Submit ? ExternalResultKind.NotAcceptedRetryable : ExternalResultKind.Unknown, "cancelled-before-call");
            else
            {
                try
                {
                    invoked = true;
                    result = await (_phase switch
                    {
                        ExternalPhase.Submit => _adapter.SubmitAsync(claim.Record.Action, call.Token),
                        ExternalPhase.Reconcile => _adapter.ReconcileAsync(claim.Record, call.Token),
                        _ => _adapter.ObserveAsync(claim.Record, call.Token)
                    }) ?? new(ExternalResultKind.Unknown, "adapter-result-missing");
                }
                catch (Exception) { result = new(ExternalResultKind.Unknown, "adapter-result-unknown"); }
            }
            outcome = result.Kind is ExternalResultKind.Unknown or ExternalResultKind.NotAcceptedRetryable
                ? ExternalCallOutcome.Unhealthy : ExternalCallOutcome.Healthy;
            using var settle = new CancellationTokenSource(_options.SettlementTimeout);
            return await _store.SettleActionAsync(claim, result, _policy, settle.Token);
        }
        finally
        {
            if (permit is not null)
            {
                using var finish = new CancellationTokenSource(_options.SettlementTimeout);
                await _limiter!.CompleteCallAsync(permit, invoked ? outcome : ExternalCallOutcome.Unused, finish.Token);
            }
        }
    }

    public async Task RunAsync(CancellationToken stoppingToken = default)
    {
        using (var startup = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken))
        {
            startup.CancelAfter(_options.StartupTimeout);
            await _store.ValidateExternalActionsAsync(startup.Token);
        }
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        await Task.WhenAll(Enumerable.Range(0, _options.MaximumConcurrency).Select(_ => Loop()));
        async Task Loop()
        {
            try
            {
                while (!stop.IsCancellationRequested)
                    if (await RunOnceAsync(stop.Token) is null) await Task.Delay(_options.IdleDelay, stop.Token);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
            catch { stop.Cancel(); throw; }
        }
    }
}
