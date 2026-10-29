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
    public void Batch_size_defaults_to_one_hundred_but_only_requires_a_positive_configuration()
    {
        Assert.Equal(100, new ExternalBatchWorkerOptions().MaximumBatchSize);
        Assert.Equal(int.MaxValue, new ExternalBatchWorkerOptions(int.MaxValue).MaximumBatchSize);
        Assert.Throws<ArgumentOutOfRangeException>(() => new ExternalBatchWorkerOptions(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ExternalBatchWorkerOptions(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ExternalBatchWorkerOptions { MaximumPayloadBytes = 1024 * 1024 - 1 });
    }

    [Fact]
    public void Native_requirement_fails_before_a_legacy_store_can_claim()
    {
        var store = new Store(1);
        Assert.False(new ExternalBatchWorker(store, Scope, new Adapter(), ExternalPhase.Submit).UsesNativeStorage);
        Assert.Throws<NotSupportedException>(() => new ExternalBatchWorker(store, Scope, new Adapter(), ExternalPhase.Submit,
            options: new() { RequireNativeStorage = true }));
        Assert.Equal(0, store.Claims);
    }

    [Fact]
    public async Task Legacy_store_reserves_space_before_claiming_another_large_action()
    {
        var store = new Store(4); var adapter = new Adapter();
        foreach (var record in store.Records.Values.ToArray())
            store.Records[record.Action.Id] = record with { Action = new(Scope, record.Action.Id, "partner", "v1", new byte[600 * 1024]) };
        var worker = new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit,
            options: new() { MaximumPayloadBytes = 1024 * 1024 });
        while (await worker.RunOnceAsync() is not null) { }
        Assert.Equal(4, adapter.Requests.Count);
        Assert.All(adapter.Requests, r => Assert.Single(r.Items));
        Assert.All(store.Records.Values, r => Assert.Equal(ExternalActionState.Accepted, r.State));
    }

    [Theory]
    [InlineData(1, 7)]
    [InlineData(25, 60)]
    [InlineData(250, 525)]
    [InlineData(1000, 1050)]
    public async Task Configured_batch_size_controls_call_groups_without_losing_item_results(int batchSize, int count)
    {
        var store = new Store(count); var adapter = new Adapter();
        var worker = new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit,
            options: new(batchSize, Options.Pump, Options.CollectionWindow));
        while (await worker.RunOnceAsync() is not null) { }
        var expected = Enumerable.Range(0, (count + batchSize - 1) / batchSize)
            .Select(i => Math.Min(batchSize, count - i * batchSize));
        Assert.Equal(expected, adapter.Requests.Select(r => r.Items.Count));
        Assert.Equal(count, adapter.Requests.SelectMany(r => r.Items).Select(r => r.Action.Id).Distinct().Count());
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
        var store = new Store(250, TimeSpan.FromHours(24));
        var options = new ExternalBatchWorkerOptions(250, Options.Pump, Options.CollectionWindow);
        var adapter = new Adapter { Respond = _ => throw new IOException("lost reply") };
        var sender = new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit, options: options);
        await sender.RunOnceAsync(); Assert.Null(await sender.RunOnceAsync());
        Assert.All(store.Records.Values, r => Assert.Equal(ExternalActionState.Unknown, r.State));
        adapter.Respond = null; store.Due();
        await new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Reconcile, options: options).RunOnceAsync();
        store.Due();
        await new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Observe, options: options).RunOnceAsync();
        Assert.Equal(new[] { ExternalPhase.Submit, ExternalPhase.Reconcile, ExternalPhase.Observe }, adapter.Requests.Select(r => r.Phase));
        Assert.All(adapter.Requests, r => Assert.Equal(250, r.Items.Count));
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

    [Theory]
    [InlineData(1)]
    [InlineData(25)]
    [InlineData(250)]
    public async Task Oversized_response_is_not_truncated_into_false_acceptance(int batchSize)
    {
        var store = new Store(1); var id = store.Records.Keys.Single();
        var adapter = new Adapter { Respond = _ => Task.FromResult<IReadOnlyList<ExternalBatchItemResult>>(
            Enumerable.Range(0, batchSize + 1).Select(i => new ExternalBatchItemResult(i == 0 ? id : Guid.NewGuid(), new(ExternalResultKind.Accepted, "ok"))).ToArray()) };
        await new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit, options: new(batchSize)).RunOnceAsync();
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

    [Fact]
    public async Task Configured_batch_worker_drains_in_requested_sizes_and_uses_selected_phase()
    {
        var store = new Store(525); var adapter = new Adapter();
        var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        adapter.Respond = request =>
        {
            if (adapter.Requests.Count == 3) drained.TrySetResult();
            return Task.FromResult<IReadOnlyList<ExternalBatchItemResult>>(request.Items.Select(r =>
                new ExternalBatchItemResult(r.Action.Id, new(ExternalResultKind.Accepted, "ok"))).ToArray());
        };
        var builder = Host.CreateApplicationBuilder(); builder.Logging.ClearProviders();
        builder.Services.AddSingleton(adapter);
        builder.Services.AddEventGuard(ConfigurationTests.Json("""
            {"EventGuard":{"Defaults":{"Batch":{"MaximumBatchSize":250,"CollectionWindow":"00:00:05"}},
              "Workers":{"partner":{"Phases":["Submit"],"Worker":{"MaximumConcurrency":1,"IdleDelay":"00:00:00.005"}}}}}
            """)).AddExternalBatchService<Adapter>("partner", Scope, new(10), storeFactory: _ => store);
        using var host = builder.Build();
        await host.StartAsync(); await drained.Task.WaitAsync(TimeSpan.FromSeconds(10)); await host.StopAsync();
        Assert.Equal(new[] { 250, 250, 25 }, adapter.Requests.Select(r => r.Items.Count));
        Assert.All(adapter.Requests, r => Assert.Equal(ExternalPhase.Submit, r.Phase));
        Assert.All(store.Records.Values, r => Assert.Equal(ExternalActionState.Accepted, r.State));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task External_call_timeout_cannot_extend_the_lease_and_keeps_uncertain_calls_unknown(bool shorterLease)
    {
        var store = new Store(1);
        var adapter = new Adapter { RespondWithCancellation = async (_, ct) =>
        {
            await Task.Delay(Timeout.Infinite, ct);
            return [];
        }};
        var pump = new WorkPumpOptions(leaseDuration: shorterLease ? TimeSpan.FromMilliseconds(80) : TimeSpan.FromSeconds(30))
            { ExternalCallTimeout = shorterLease ? TimeSpan.FromSeconds(20) : TimeSpan.FromMilliseconds(80) };
        var worker = new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit, options: new(1, pump));
        var result = await worker.RunOnceAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Single(adapter.Requests);
        if (shorterLease)
        {
            // Timer cancellation and lease expiry race. Recording Unknown just before expiry is also valid;
            // if settlement lost ownership, the next poll must recover Sending to Unknown without re-sending.
            Assert.Single(result!.Settlements);
            Assert.Null(await worker.RunOnceAsync());
        }
        Assert.Equal(ExternalActionState.Unknown, store.Records.Values.Single().State);
        Assert.Single(adapter.Requests);
    }

    [Fact]
    public async Task Configured_startup_timeout_prevents_claims()
    {
        var store = new Store(1) { StallStartup = true };
        var builder = Host.CreateApplicationBuilder(); builder.Logging.ClearProviders();
        builder.Services.AddEventGuard(ConfigurationTests.Json("""
            {"EventGuard":{"Workers":{"partner":{"Worker":{"StartupTimeout":"00:00:00.080"}}}}}
            """)).AddExternalBatchService<Adapter>("partner", Scope, storeFactory: _ => store);
        using var host = builder.Build();
        await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync().WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(0, store.Claims);
    }

    [Fact]
    public async Task Configured_settlement_timeout_bounds_a_stalled_status_write_without_resending()
    {
        var store = new Store(1) { StallSettlement = true }; var adapter = new Adapter();
        var worker = new ExternalBatchWorker(store, Scope, adapter, ExternalPhase.Submit,
            options: new(1, new WorkPumpOptions { SettlementTimeout = TimeSpan.FromMilliseconds(80) }));
        await Assert.ThrowsAsync<AggregateException>(() => worker.RunOnceAsync().WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Single(adapter.Requests);
        store.StallSettlement = false; store.ExpireLeases();
        Assert.Null(await worker.RunOnceAsync());
        Assert.Equal(ExternalActionState.Unknown, store.Records.Values.Single().State);
        Assert.Single(adapter.Requests);
    }

    public sealed class Adapter : IExternalBatchServiceAdapter
    {
        public string Binding => "partner"; public string BindingVersion => "v1";
        public bool CanReconcile => true; public bool CanObserve => true;
        public List<ExternalBatchRequest> Requests { get; } = [];
        public TaskCompletionSource Called { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Func<ExternalBatchRequest, Task<IReadOnlyList<ExternalBatchItemResult>>>? Respond { get; set; }
        public Func<ExternalBatchRequest, CancellationToken, Task<IReadOnlyList<ExternalBatchItemResult>>>? RespondWithCancellation { get; init; }
        public Task<IReadOnlyList<ExternalBatchItemResult>> ExecuteAsync(ExternalBatchRequest request, CancellationToken ct)
        {
            Requests.Add(request); Called.TrySetResult();
            return RespondWithCancellation?.Invoke(request, ct) ?? Respond?.Invoke(request) ?? Task.FromResult<IReadOnlyList<ExternalBatchItemResult>>(request.Items.Reverse()
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
        public bool StallStartup { get; init; }
        public bool StallSettlement { get; set; }
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
        public Task ValidateExternalActionsAsync(CancellationToken ct = default) => StallStartup ? Task.Delay(Timeout.Infinite, ct) : Task.CompletedTask;
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
            if (StallSettlement) return Stall();
            async Task<ExternalSettlement> Stall() { await Task.Delay(Timeout.Infinite, ct); throw new InvalidOperationException(); }
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
