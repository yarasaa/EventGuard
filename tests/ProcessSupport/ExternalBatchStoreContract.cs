using EventGuard.Runtime;

namespace EventGuard.TestSupport;

/// <summary>The same remote batch failure scenarios against both native stores.</summary>
public abstract class ExternalBatchStoreContract
{
    protected abstract IExternalActionStore Store { get; }
    protected abstract IExternalActionStore Restart();
    protected abstract Task Initialize();
    protected abstract Task Save(ExternalAction[] actions);
    protected abstract Task Due(ProducerScope scope, bool expireLeases = false);
    private static readonly ExternalBatchWorkerOptions Options = new(100, new(maximumConcurrency: 1,
        leaseDuration: TimeSpan.FromMinutes(2)), TimeSpan.FromSeconds(5));
    private IExternalActionBatchStore Native => Assert.IsAssignableFrom<IExternalActionBatchStore>(Store);
    private static ExternalBatchClaimRequest Request(ProducerScope scope, int count = 100, int bytes = 8 * 1024 * 1024,
        Guid? id = null, ExternalPhase phase = ExternalPhase.Submit) => new(scope, "bulk-service", "v1", phase,
            id ?? Guid.NewGuid(), TimeSpan.FromMinutes(2), count, bytes);
    private static ExternalBatchSettlementRequest Accepted(IEnumerable<ExternalActionClaim> claims) => new(
        claims.Select(c => new ExternalBatchSettlementItem(c, new(ExternalResultKind.Accepted, "accepted", c.Record.Action.Id.ToString("D")))), new());

    [Fact]
    public async Task Storage_request_retry_preserves_members_and_rejects_changed_semantics()
    {
        var actions = await Seed(6); var request = Request(actions[0].Scope, 3);
        var first = await Native.ClaimActionsAsync(request);
        Assert.Equal(3, first.Count);
        var restarted = (IExternalActionBatchStore)Restart();
        Assert.Equal(first.Select(c => c.Token), (await restarted.ClaimActionsAsync(request)).Select(c => c.Token));
        await Assert.ThrowsAsync<InvalidOperationException>(() => restarted.ClaimActionsAsync(Request(actions[0].Scope, 4, id: request.RequestId)));
        await Store.SettleActionAsync(first[0], Accepted(first).Items[0].Result, new());
        var resumed = await restarted.ClaimActionsAsync(request);
        Assert.Equal(first.Skip(1).Select(c => c.Token), resumed.Select(c => c.Token));
        await restarted.SettleActionsAsync(Accepted(first));
        Assert.Empty(await restarted.ClaimActionsAsync(request));
        Assert.Equal(3, (await restarted.ClaimActionsAsync(Request(actions[0].Scope))).Count);
    }

    [Fact]
    public async Task Byte_budget_splits_large_actions_without_claiming_unsent_overflow()
    {
        await Initialize(); var scope = new ProducerScope("large-payload", Guid.NewGuid().ToString("N"));
        var actions = Enumerable.Range(0, 4).Select(_ => new ExternalAction(scope, Guid.NewGuid(), "bulk-service", "v1", new byte[600 * 1024])).ToArray();
        await Save(actions); var service = new Service();
        var worker = new ExternalBatchWorker(Store, scope, service, ExternalPhase.Submit,
            options: new(100, Options.Pump, Options.CollectionWindow) { MaximumPayloadBytes = 1024 * 1024, RequireNativeStorage = true });
        Assert.True(worker.UsesNativeStorage);
        while (await worker.RunOnceAsync() is not null) { }
        Assert.Equal(4, service.Requests.Count);
        Assert.All(service.Requests, r => Assert.Single(r.Items));
        foreach (var action in actions) Assert.Equal(1, (await Store.FindActionAsync(scope, action.Id))!.SubmissionAttempts);
    }

    [Fact]
    public async Task Concurrent_native_claimers_have_disjoint_membership()
    {
        var actions = await Seed(120);
        var pages = await Task.WhenAll(Enumerable.Range(0, 6).Select(_ =>
            ((IExternalActionBatchStore)Restart()).ClaimActionsAsync(Request(actions[0].Scope, 20))));
        var claims = pages.SelectMany(p => p).ToList();
        // READPAST/SKIP LOCKED may produce short pages while a peer owns transient index/row locks.
        // After those transactions complete, every remaining ready action must still be claimable.
        while (claims.Count < actions.Length)
        {
            var remaining = await Native.ClaimActionsAsync(Request(actions[0].Scope, 20));
            Assert.NotEmpty(remaining); claims.AddRange(remaining);
        }
        Assert.Equal(120, claims.Count);
        Assert.Equal(120, claims.Select(c => c.Record.Action.Id).Distinct().Count());
        Assert.All(await Native.SettleActionsAsync(Accepted(claims)), s => Assert.True(s.Recorded));
    }

    [Fact]
    public async Task Late_batch_evidence_is_recorded_without_overwriting_lease_recovery()
    {
        var actions = await Seed(4); var request = Request(actions[0].Scope);
        var claims = await Native.ClaimActionsAsync(request);
        await Due(actions[0].Scope, expireLeases: true);
        Assert.Empty(await Native.ClaimActionsAsync(Request(actions[0].Scope)));
        Assert.Empty(await Native.ClaimActionsAsync(request));
        foreach (var settlement in await Native.SettleActionsAsync(Accepted(claims)))
        { Assert.False(settlement.Recorded); Assert.Equal(ExternalActionState.Unknown, settlement.Record.State); }
        foreach (var action in actions)
        {
            var evidence = Assert.Single(await Store.ReadActionEvidenceAsync(action.Scope, action.Id));
            Assert.False(evidence.Applied); Assert.Equal(ExternalResultKind.Accepted, evidence.Result!.Kind);
        }
        Assert.All(await Native.SettleActionsAsync(Accepted(claims)), s => Assert.False(s.Recorded));
    }

    [Fact]
    public async Task Conflicting_or_invalid_evidence_rolls_back_the_entire_storage_page()
    {
        var actions = await Seed(3); var claims = await Native.ClaimActionsAsync(Request(actions[0].Scope));
        var accepted = Accepted(claims);
        await Store.SettleActionAsync(claims[0], accepted.Items[0].Result, new());
        var conflicting = accepted.Items.Select((item, i) => i == 0 ? item with { Result = new(ExternalResultKind.Rejected, "conflict") } : item);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Native.SettleActionsAsync(new(conflicting, new())));
        var invalid = accepted.Items.Select((item, i) => i == 2 ? item with { Result = new(ExternalResultKind.Succeeded, "wrong-phase") } : item);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Native.SettleActionsAsync(new(invalid, new())));
        foreach (var claim in claims.Skip(1))
        {
            Assert.Equal(ExternalActionState.Sending, (await Store.FindActionAsync(claim.Record.Action.Scope, claim.Record.Action.Id))!.State);
            Assert.Null(Assert.Single(await Store.ReadActionEvidenceAsync(claim.Record.Action.Scope, claim.Record.Action.Id)).Result);
        }
        Assert.All(await Native.SettleActionsAsync(accepted), s => Assert.True(s.Recorded));
        Assert.All(await Native.SettleActionsAsync(accepted), s => Assert.True(s.Recorded));
    }

    private async Task<ExternalAction[]> Seed(int count, TimeSpan? observation = null)
    {
        await Initialize();
        var scope = new ProducerScope("batch-tenant", Guid.NewGuid().ToString("N"));
        var actions = Enumerable.Range(0, count).Select(i => JsonExternalActions.Create(scope, Guid.NewGuid(), "bulk-service", "v1", new { Item = i }, observation)).ToArray();
        await Save(actions);
        return actions;
    }

    [Theory]
    [InlineData(100)]
    [InlineData(250)]
    [InlineData(1100)]
    public async Task Native_batch_correlates_reordered_results_and_preserves_per_item_evidence(int batchSize)
    {
        var actions = await Seed(batchSize); var service = new Service();
        var worker = new ExternalBatchWorker(Store, actions[0].Scope, service, ExternalPhase.Submit,
            options: new(batchSize, Options.Pump, Options.CollectionWindow));
        while (await worker.RunOnceAsync() is not null) { }
        Assert.Equal(batchSize, service.Requests.Sum(r => r.Items.Count));
        Assert.Contains(service.Requests, r => r.Items.Count > 1);
        Assert.All(service.Requests, r => Assert.InRange(r.Items.Count, 1, batchSize));
        foreach (var action in actions)
        {
            var row = (await Restart().FindActionAsync(action.Scope, action.Id))!;
            Assert.Equal(ExternalActionState.Accepted, row.State);
            Assert.Equal(action.Id.ToString("D"), row.ProviderReference);
            Assert.Equal(1, row.SubmissionAttempts);
            Assert.True(Assert.Single(await Store.ReadActionEvidenceAsync(action.Scope, action.Id)).Applied);
        }
    }

    [Fact]
    public async Task Native_partial_reply_retains_unknown_across_restart_and_only_retries_proven_rejection()
    {
        var actions = await Seed(6); var accepted = actions[0].Id; var retry = actions[1].Id;
        var service = new Service { Reply = r => r.Items.Where(i => i.Action.Id == accepted || i.Action.Id == retry)
            .Select(i => new ExternalBatchItemResult(i.Action.Id, new(i.Action.Id == accepted
                ? ExternalResultKind.Accepted : ExternalResultKind.NotAcceptedRetryable, "known"))).ToArray() };
        var worker = new ExternalBatchWorker(Store, actions[0].Scope, service, ExternalPhase.Submit, options: Options);
        while (await worker.RunOnceAsync() is not null) { }
        foreach (var action in actions.Where(a => a.Id != accepted && a.Id != retry))
            Assert.Equal(ExternalActionState.Unknown, (await Restart().FindActionAsync(action.Scope, action.Id))!.State);
        await Due(actions[0].Scope); service.Reply = null; service.Requests.Clear();
        await new ExternalBatchWorker(Restart(), actions[0].Scope, service, ExternalPhase.Submit, options: Options).RunOnceAsync();
        Assert.Equal(retry, Assert.Single(Assert.Single(service.Requests).Items).Action.Id);
    }

    [Fact]
    public async Task Native_lost_reply_reconciles_and_bulk_observes_without_resending()
    {
        var actions = await Seed(12, TimeSpan.FromHours(24));
        var service = new Service { Reply = _ => throw new IOException("remote accepted but response lost") };
        var sender = new ExternalBatchWorker(Store, actions[0].Scope, service, ExternalPhase.Submit, options: Options);
        while (await sender.RunOnceAsync() is not null) { }
        Assert.Equal(12, service.Requests.Sum(r => r.Items.Count));
        await Due(actions[0].Scope); service.Reply = null;
        var reconcile = new ExternalBatchWorker(Restart(), actions[0].Scope, service, ExternalPhase.Reconcile, options: Options);
        while (await reconcile.RunOnceAsync() is not null) { }
        await Due(actions[0].Scope);
        var observer = new ExternalBatchWorker(Restart(), actions[0].Scope, service, ExternalPhase.Observe, options: Options);
        while (await observer.RunOnceAsync() is not null) { }
        Assert.Equal(12, service.Requests.Where(r => r.Phase == ExternalPhase.Submit).Sum(r => r.Items.Count));
        foreach (var action in actions)
        {
            var row = (await Store.FindActionAsync(action.Scope, action.Id))!;
            Assert.Equal(ExternalObservationState.Succeeded, row.Observation);
            Assert.Equal(1, row.SubmissionAttempts);
            Assert.Equal(3, (await Store.ReadActionEvidenceAsync(action.Scope, action.Id)).Count);
        }
    }

    [Fact]
    public async Task Native_crashed_batch_claims_recover_to_unknown_before_any_new_send()
    {
        var actions = await Seed(4);
        foreach (var _ in actions)
            Assert.NotNull(await Store.ClaimActionAsync(actions[0].Scope, "bulk-service", "v1", ExternalPhase.Submit, Guid.NewGuid(), TimeSpan.FromMinutes(1)));
        await Due(actions[0].Scope, expireLeases: true);
        var service = new Service();
        var worker = new ExternalBatchWorker(Restart(), actions[0].Scope, service, ExternalPhase.Submit, options: Options);
        // Each null recovery result can advance one expired sender without returning it for submission.
        for (var i = 0; i <= actions.Length; i++) Assert.Null(await worker.RunOnceAsync());
        Assert.Empty(service.Requests);
        foreach (var action in actions)
            Assert.Equal(ExternalActionState.Unknown, (await Store.FindActionAsync(action.Scope, action.Id))!.State);
    }

    private sealed class Service : IExternalBatchServiceAdapter
    {
        public string Binding => "bulk-service"; public string BindingVersion => "v1";
        public bool CanReconcile => true; public bool CanObserve => true;
        public List<ExternalBatchRequest> Requests { get; } = [];
        public Func<ExternalBatchRequest, IReadOnlyList<ExternalBatchItemResult>>? Reply { get; set; }
        public Task<IReadOnlyList<ExternalBatchItemResult>> ExecuteAsync(ExternalBatchRequest request, CancellationToken ct)
        {
            Requests.Add(request);
            return Task.FromResult(Reply?.Invoke(request) ?? request.Items.Reverse().Select(r => new ExternalBatchItemResult(r.Action.Id,
                new ExternalResult(request.Phase == ExternalPhase.Observe ? ExternalResultKind.Succeeded : ExternalResultKind.Accepted, "ok", r.Action.Id.ToString("D")))).ToArray());
        }
    }
}
