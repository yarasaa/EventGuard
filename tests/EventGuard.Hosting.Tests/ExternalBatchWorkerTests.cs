using EventGuard.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventGuard.Hosting.Tests;

public sealed class ExternalBatchWorkerTests
{
    private static readonly ProducerScope Scope = new("tenant", "batch-tests");
    private static readonly ExternalBatchWorkerOptions Options = new(100, new(maximumConcurrency: 1,
        leaseDuration: TimeSpan.FromMinutes(5), idleDelay: TimeSpan.FromMilliseconds(5)), TimeSpan.FromSeconds(5));

    [Fact]
    public async Task Two_hundred_fifty_items_use_three_calls_with_reordered_item_results()
    {
        var store = new Store(250); var adapter = new Adapter();
        var worker = new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit, options: Options);
        while (await worker.RunOnceAsync() is not null) { }
        Assert.Equal(new[] { 100, 100, 50 }, adapter.Requests.Select(r => r.Items.Count));
        Assert.Equal(250, adapter.Requests.SelectMany(r => r.Items).Select(r => r.Action.Id).Distinct().Count());
        Assert.All(store.Records.Values, r => { Assert.Equal(ExternalActionState.Accepted, r.State); Assert.Equal(r.Action.Id.ToString("D"), r.ProviderReference); });
    }

    [Fact]
    public async Task Mixed_results_only_retry_proven_non_acceptance_and_hold_missing_or_duplicate_results()
    {
        var store = new Store(6); var ids = store.Records.Keys.ToArray();
        var adapter = new Adapter { Respond = _ => Task.FromResult<IReadOnlyList<ExternalBatchItemResult>>([
            new(ids[0], new(ExternalResultKind.Accepted, "ok")), new(ids[1], new(ExternalResultKind.Rejected, "invalid")),
            new(ids[2], new(ExternalResultKind.NotAcceptedRetryable, "not-accepted")), new(ids[3], new(ExternalResultKind.Unknown, "timeout")),
            new(ids[5], new(ExternalResultKind.Accepted, "ok")), new(ids[5], new(ExternalResultKind.Rejected, "contradictory"))]) };
        var worker = new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit, options: Options);
        await worker.RunOnceAsync();
        Assert.Equal(new[] { ExternalActionState.Accepted, ExternalActionState.Rejected, ExternalActionState.Ready,
            ExternalActionState.Unknown, ExternalActionState.Unknown, ExternalActionState.Unknown }, ids.Select(id => store.Records[id].State));
        store.Due(); adapter.Respond = null;
        await worker.RunOnceAsync();
        Assert.Equal(ids[2], Assert.Single(adapter.Requests[1].Items).Action.Id);
        Assert.Null(await worker.RunOnceAsync());
    }

    [Fact]
    public async Task Lost_entire_reply_reconciles_and_observes_in_batches_without_resubmission()
    {
        var store = new Store(100, TimeSpan.FromHours(24));
        var adapter = new Adapter { Respond = _ => throw new IOException("lost reply") };
        var sender = new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit, options: Options);
        await sender.RunOnceAsync(); Assert.Null(await sender.RunOnceAsync());
        Assert.All(store.Records.Values, r => Assert.Equal(ExternalActionState.Unknown, r.State));
        adapter.Respond = null; store.Due();
        await new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Reconcile, options: Options).RunOnceAsync();
        store.Due();
        await new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Observe, options: Options).RunOnceAsync();
        Assert.Equal(new[] { ExternalPhase.Submit, ExternalPhase.Reconcile, ExternalPhase.Observe }, adapter.Requests.Select(r => r.Phase));
        Assert.All(store.Records.Values, r => { Assert.Equal(ExternalObservationState.Succeeded, r.Observation); Assert.Equal(1, r.SubmissionAttempts); });
    }

    [Fact]
    public async Task Invalid_phase_evidence_and_foreign_ids_do_not_corrupt_other_items()
    {
        var store = new Store(2); var ids = store.Records.Keys.ToArray();
        var adapter = new Adapter { Respond = _ => Task.FromResult<IReadOnlyList<ExternalBatchItemResult>>([
            new(ids[0], new(ExternalResultKind.Succeeded, "wrong-phase")), new(ids[1], new(ExternalResultKind.Accepted, "ok")),
            new(Guid.NewGuid(), new(ExternalResultKind.Accepted, "foreign"))]) };
        var result = await new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit, options: Options).RunOnceAsync();
        Assert.Equal(1, result!.UnmatchedResponseCount);
        Assert.Equal(ExternalActionState.Unknown, store.Records[ids[0]].State);
        Assert.Equal(ExternalActionState.Accepted, store.Records[ids[1]].State);
    }

    [Fact]
    public async Task Oversized_response_is_not_truncated_into_false_acceptance()
    {
        var store = new Store(1); var id = store.Records.Keys.Single();
        var adapter = new Adapter { Respond = _ => Task.FromResult<IReadOnlyList<ExternalBatchItemResult>>(
            Enumerable.Range(0, 101).Select(_ => new ExternalBatchItemResult(id, new(ExternalResultKind.Accepted, "ok"))).ToArray()) };
        await new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit).RunOnceAsync();
        Assert.Equal(ExternalActionState.Unknown, store.Records[id].State);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Cancellation_before_invocation_is_retryable_but_during_invocation_is_unknown(bool duringCall)
    {
        using var stop = new CancellationTokenSource();
        var store = new Store(1); var adapter = new Adapter();
        if (duringCall) adapter.Respond = _ => { stop.Cancel(); throw new OperationCanceledException(); };
        else store.AfterClaim = () => stop.Cancel();
        await new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit, options: new(1)).RunOnceAsync(stop.Token);
        Assert.Equal(duringCall ? ExternalActionState.Unknown : ExternalActionState.Ready, store.Records.Values.Single().State);
        Assert.Equal(duringCall ? 1 : 0, adapter.Requests.Count);
    }

    [Fact]
    public async Task One_failed_status_write_does_not_discard_other_results_or_repeat_the_call()
    {
        var store = new Store(3); var failed = store.Records.Keys.First(); store.FailSettlementFor = failed;
        var adapter = new Adapter();
        var worker = new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit, options: Options);
        await Assert.ThrowsAsync<AggregateException>(() => worker.RunOnceAsync());
        Assert.All(store.Records.Values.Where(r => r.Action.Id != failed), r => Assert.Equal(ExternalActionState.Accepted, r.State));
        store.FailSettlementFor = null;
        Assert.Null(await worker.RunOnceAsync()); Assert.Single(adapter.Requests);
        store.ExpireLeases();
        Assert.Null(await worker.RunOnceAsync());
        Assert.Equal(ExternalActionState.Unknown, store.Records[failed].State);
        Assert.Single(adapter.Requests);
    }

    [Fact]
    public async Task Quota_is_one_remote_call_and_denial_does_not_claim_items()
    {
        var store = new Store(100); var adapter = new Adapter(); var limiter = new Limiter();
        var worker = new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit, options: Options,
            limit: new("account", "endpoint", 1, TimeSpan.FromSeconds(1)), limiter: limiter);
        await worker.RunOnceAsync(); Assert.Single(limiter.Outcomes); Assert.Equal(ExternalCallOutcome.Healthy, limiter.Outcomes[0]);
        await worker.RunOnceAsync(); Assert.Equal(ExternalCallOutcome.Unused, limiter.Outcomes[1]);
        var claims = store.Claims; limiter.Denied = true;
        Assert.Null(await worker.RunOnceAsync()); Assert.Equal(claims, store.Claims);
    }

    [Fact]
    public async Task Single_host_registration_starts_supported_phases_and_stops_cleanly()
    {
        var store = new Store(3); var adapter = new Adapter();
        var builder = Host.CreateApplicationBuilder(); builder.Logging.ClearProviders();
        builder.Services.AddSingleton(adapter);
        builder.Services.AddEventGuard().AddExternalBatchService<Adapter>("partner", Scope, Options, storeFactory: _ => store);
        using var host = builder.Build();
        await host.StartAsync();
        await adapter.Called.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await host.StopAsync();
        Assert.All(store.Records.Values, r => Assert.Equal(ExternalActionState.Accepted, r.State));
    }

    [Fact]
    public async Task Unsupported_phase_fails_startup_before_any_claim()
    {
        var store = new Store(1); var builder = Host.CreateApplicationBuilder(); builder.Logging.ClearProviders();
        builder.Services.AddEventGuard().AddExternalBatchService<SubmitOnly>("partner", Scope, storeFactory: _ => store,
            phases: [ExternalPhase.Submit, ExternalPhase.Observe]);
        using var host = builder.Build();
        await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
        Assert.Equal(0, store.Claims);
    }

    public sealed class Adapter : IExternalBatchServiceAdapter
    {
        public string Binding => "partner"; public string BindingVersion => "v1";
        public bool CanReconcile => true; public bool CanObserve => true;
        public List<ExternalBatchRequest> Requests { get; } = [];
        public TaskCompletionSource Called { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Func<ExternalBatchRequest, Task<IReadOnlyList<ExternalBatchItemResult>>>? Respond { get; set; }
        public Task<IReadOnlyList<ExternalBatchItemResult>> ExecuteAsync(ExternalBatchRequest request, CancellationToken ct)
        {
            Requests.Add(request); Called.TrySetResult();
            return Respond?.Invoke(request) ?? Task.FromResult<IReadOnlyList<ExternalBatchItemResult>>(request.Items.Reverse()
                .Select(r => new ExternalBatchItemResult(r.Action.Id, new(request.Phase == ExternalPhase.Observe
                    ? ExternalResultKind.Succeeded : ExternalResultKind.Accepted, "ok", r.Action.Id.ToString("D")))).ToArray());
        }
    }

    public sealed class SubmitOnly : IExternalBatchServiceAdapter
    {
        public string Binding => "partner"; public string BindingVersion => "v1";
        public bool CanReconcile => false; public bool CanObserve => false;
        public Task<IReadOnlyList<ExternalBatchItemResult>> ExecuteAsync(ExternalBatchRequest request, CancellationToken ct) => throw new NotImplementedException();
    }

    private sealed class Limiter : IExternalCallLimiter
    {
        public bool Denied { get; set; }
        public List<ExternalCallOutcome> Outcomes { get; } = [];
        public Task<ExternalCallPermit?> TryAcquireCallAsync(ExternalCallLimit limit, Guid requestId, TimeSpan duration, CancellationToken ct = default)
            => Task.FromResult(Denied ? null : new ExternalCallPermit(limit, requestId, DateTimeOffset.UtcNow + duration));
        public Task CompleteCallAsync(ExternalCallPermit permit, ExternalCallOutcome outcome, CancellationToken ct = default) { Outcomes.Add(outcome); return Task.CompletedTask; }
        public Task<ExternalCallLimitStatus?> ReadCallLimitAsync(ExternalCallLimit limit, CancellationToken ct = default) => Task.FromResult<ExternalCallLimitStatus?>(null);
    }

    private sealed class Store : IExternalActionStore
    {
        private readonly object _gate = new();
        private readonly Dictionary<Guid, ExternalActionClaim> _leases = [];
        public Dictionary<Guid, ExternalActionRecord> Records { get; } = [];
        public int Claims { get; private set; }
        public Guid? FailSettlementFor { get; set; }
        public Action? AfterClaim { get; set; }
        public Store(int count, TimeSpan? observation = null)
        {
            for (var i = 0; i < count; i++)
            {
                var a = JsonExternalActions.Create(Scope, Guid.NewGuid(), "partner", "v1", new { Key = i }, observation);
                Records[a.Id] = new(a, ExternalActionState.Ready, ExternalObservationState.None, 0, 0, DateTimeOffset.UtcNow.AddMinutes(-1), null, null, null, []);
            }
        }
        public void Due() { lock (_gate) foreach (var r in Records.Values.ToArray()) Records[r.Action.Id] = r with { NextAttemptAt = DateTimeOffset.UtcNow.AddMinutes(-1) }; }
        public void ExpireLeases() { lock (_gate) foreach (var id in _leases.Keys.ToArray()) _leases[id] = _leases[id] with { LeaseUntil = DateTimeOffset.UtcNow.AddMinutes(-1) }; }
        public Task ValidateExternalActionsAsync(CancellationToken ct = default) => Task.CompletedTask;
        public Task<ExternalActionRecord?> FindActionAsync(ProducerScope scope, Guid id, CancellationToken ct = default)
            => Task.FromResult(Records.TryGetValue(id, out var r) && r.Action.Scope == scope ? r : null);
        public Task<IReadOnlyList<ExternalAttemptEvidence>> ReadActionEvidenceAsync(ProducerScope scope, Guid id, int limit = 100, CancellationToken ct = default)
            => Task.FromResult<IReadOnlyList<ExternalAttemptEvidence>>([]);
        public Task<ExternalActionClaim?> ClaimActionAsync(ProducerScope scope, string binding, string version, ExternalPhase phase, Guid token, TimeSpan duration, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            lock (_gate)
            {
                Claims++;
                var now = DateTimeOffset.UtcNow;
                var r = Records.Values.FirstOrDefault(r => r.Action.Scope == scope && r.Action.Binding == binding && r.Action.BindingVersion == version
                    && r.NextAttemptAt <= now && (!_leases.TryGetValue(r.Action.Id, out var lease) || lease.LeaseUntil <= now)
                    && (phase == ExternalPhase.Submit ? r.State is ExternalActionState.Ready or ExternalActionState.Sending
                        : phase == ExternalPhase.Reconcile ? r.State is ExternalActionState.Unknown or ExternalActionState.Sending
                        : r.State == ExternalActionState.Accepted && r.Observation == ExternalObservationState.Pending));
                if (r is null) return Task.FromResult<ExternalActionClaim?>(null);
                if (r.State == ExternalActionState.Sending)
                {
                    Records[r.Action.Id] = r with { State = ExternalActionState.Unknown, Version = r.Version + 1 };
                    _leases.Remove(r.Action.Id); return Task.FromResult<ExternalActionClaim?>(null);
                }
                r = r with { Version = r.Version + 1, State = phase == ExternalPhase.Submit ? ExternalActionState.Sending : r.State,
                    SubmissionAttempts = r.SubmissionAttempts + (phase == ExternalPhase.Submit ? 1 : 0) };
                Records[r.Action.Id] = r;
                var claim = new ExternalActionClaim(r, phase, token, now + duration);
                _leases[r.Action.Id] = claim; AfterClaim?.Invoke();
                return Task.FromResult<ExternalActionClaim?>(claim);
            }
        }
        public Task<ExternalSettlement> SettleActionAsync(ExternalActionClaim claim, ExternalResult result, ExternalActionPolicy policy, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            lock (_gate)
            {
                var id = claim.Record.Action.Id;
                if (FailSettlementFor == id) throw new IOException("injected write failure");
                if (!_leases.TryGetValue(id, out var lease) || lease.Token != claim.Token || lease.LeaseUntil <= DateTimeOffset.UtcNow)
                    return Task.FromResult(new ExternalSettlement(false, Records[id]));
                var after = ExternalActionLifecycle.Settle(Records[id], claim.Phase, result, policy, DateTimeOffset.UtcNow);
                Records[id] = after; _leases.Remove(id);
                return Task.FromResult(new ExternalSettlement(true, after));
            }
        }
    }
}
