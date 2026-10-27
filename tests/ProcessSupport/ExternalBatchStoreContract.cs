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

    private async Task<ExternalAction[]> Seed(int count, TimeSpan? observation = null)
    {
        await Initialize();
        var scope = new ProducerScope("batch-tenant", Guid.NewGuid().ToString("N"));
        var actions = Enumerable.Range(0, count).Select(i => JsonExternalActions.Create(scope, Guid.NewGuid(), "bulk-service", "v1", new { Item = i }, observation)).ToArray();
        await Save(actions);
        return actions;
    }

    [Fact]
    public async Task Native_batch_correlates_reordered_results_and_preserves_per_item_evidence()
    {
        var actions = await Seed(100); var service = new Service();
        var worker = new ExternalBatchWorker(Store, actions[0].Scope, service, ExternalPhase.Submit, options: Options);
        while (await worker.RunOnceAsync() is not null) { }
        Assert.Equal(100, service.Requests.Sum(r => r.Items.Count));
        Assert.Contains(service.Requests, r => r.Items.Count > 1);
        Assert.All(service.Requests, r => Assert.InRange(r.Items.Count, 1, 100));
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
