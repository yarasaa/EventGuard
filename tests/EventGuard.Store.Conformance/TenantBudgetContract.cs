namespace EventGuard.Store.Conformance;

public abstract class TenantBudgetContract<TSession>(IWorkStoreFixture<TSession> db, IOutboxStoreFixture outbox)
{
    private ITenantBudgetStore Budgets => (ITenantBudgetStore)db.Store;
    private IIngressQuarantineStore Ingress => (IIngressQuarantineStore)db.Store;
    private static WorkScope Scope(string? tenant = null, string consumer = "worker") => new(tenant ?? Guid.NewGuid().ToString("N"), consumer, "source", "event");
    private static TenantBudgetRequest Configure(string tenant, long? claims = null, long? retries = null, long? recoveries = null, long version = -1) =>
        new(Guid.NewGuid(), new(tenant, 86400, claims, retries, recoveries), version, "operator", "capacity policy");
    protected abstract Task AgeWindowAsync(string tenant);
    protected abstract Task ExpireAsync(Guid workId);
    protected abstract Task CorruptAsync(OutgoingMessage message);
    [Fact]
    public async Task Multiple_store_instances_and_scopes_share_one_claim_limit_but_tenants_remain_independent()
    {
        var a = Scope(); var b = Scope(a.Tenant, "other-worker"); var other = Scope();
        await Budgets.ConfigureBudgetAsync(Configure(a.Tenant, claims: 5));
        for (var i = 0; i < 10; i++) await db.Store.AcceptAsync(StoreContractData.Submission(i % 2 == 0 ? a : b, "item-" + i));
        var stores = new[] { db.Store, db.RestartStore() };
        var replies = await Task.WhenAll(Enumerable.Range(0, 12).Select(i => stores[i % 2].ClaimNextAsync(i % 2 == 0 ? a : b, Guid.NewGuid(), TimeSpan.FromMinutes(1))));
        var claims = replies.Where(r => r is not null).Select(r => r!).ToArray();
        // Native skip-locked may leave idle slots on a concurrent pass. Drain unclaimed slots sequentially.
        var all = claims.ToList();
        for (var i = 0; i < 10 && all.Count < 5; i++)
        { var claim = await stores[i % 2].ClaimNextAsync(i % 2 == 0 ? a : b, Guid.NewGuid(), TimeSpan.FromMinutes(1)); if (claim is not null) all.Add(claim); }
        Assert.Equal(5, all.Count); Assert.Equal(5, all.Select(c => c.Work.Id).Distinct().Count());
        Assert.Equal(5, (await Budgets.FindBudgetAsync(a.Tenant))!.Claims);
        Assert.Null(await db.Claim(a)); Assert.Null(await db.Claim(b));
        await db.Store.AcceptAsync(StoreContractData.Submission(other)); Assert.NotNull(await db.Claim(other));
        var original = all[0]; var scope = original.Work.ScopeId == a.Id ? a : b;
        Assert.Equal(original.Token, (await db.Store.ClaimNextAsync(scope, original.Token, TimeSpan.FromMinutes(1)))!.Token);
        Assert.Equal(5, (await Budgets.FindBudgetAsync(a.Tenant))!.Claims);
    }
    [Fact]
    public async Task Consumer_and_outbox_share_total_capacity_and_configuration_is_idempotent()
    {
        var scope = Scope(); var producer = new ProducerScope(scope.Tenant, "publisher");
        var create = Configure(scope.Tenant, claims: 1); Assert.Equal(TenantBudgetStatus.Applied, (await Budgets.ConfigureBudgetAsync(create)).Status);
        await db.Store.AcceptAsync(StoreContractData.Submission(scope)); Assert.NotNull(await db.Claim(scope));
        var message = new OutgoingMessage(producer, Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "events"), [1]);
        await outbox.EnqueueAsync(message); Assert.Null(await outbox.Outbox.ClaimNextAsync(producer, Guid.NewGuid(), TimeSpan.FromMinutes(1)));
        Assert.True((await ((ITenantBudgetStore)db.RestartStore()).ConfigureBudgetAsync(create)).RepeatedRequest);
        Assert.Equal(1, (await Budgets.FindBudgetAsync(scope.Tenant))!.Claims);
        var update = Configure(scope.Tenant, claims: 2, version: 0); Assert.Equal(TenantBudgetStatus.Applied, (await Budgets.ConfigureBudgetAsync(update)).Status);
        var claim = (await outbox.Outbox.ClaimNextAsync(producer, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!;
        Assert.NotNull(claim); Assert.NotNull(await outbox.Outbox.ClaimNextAsync(producer, claim.Token, TimeSpan.FromMinutes(1)));
        Assert.Equal(2, (await Budgets.FindBudgetAsync(scope.Tenant))!.Claims);
        Assert.Equal(0, (await Budgets.ConfigureBudgetAsync(create)).Version); // Original receipt does not overwrite the updated policy.
        Assert.Equal(1, (await Budgets.FindBudgetAsync(scope.Tenant))!.Version);
        var changed = new TenantBudgetRequest(create.RequestId, new(scope.Tenant, maximumClaims: 9), -1, create.Actor, create.Reason);
        Assert.Equal(TenantBudgetStatus.RequestConflict, (await Budgets.ConfigureBudgetAsync(changed)).Status);
    }
    [Fact]
    public async Task Exhausted_retry_budget_does_not_block_fresh_work_and_expiry_cleanup_ignores_execution_quota()
    {
        var scope = Scope(); await db.Store.AcceptAsync(StoreContractData.Submission(scope, "retry"));
        await db.Store.ExecuteAsync((await db.Claim(scope))!, (_, _, _, _) => throw new Exception("transient"), new(initialDelay: TimeSpan.FromMilliseconds(1)));
        var fresh = await db.Store.AcceptAsync(StoreContractData.Submission(scope, "fresh"));
        await Budgets.ConfigureBudgetAsync(Configure(scope.Tenant, claims: 1, retries: 0));
        var claim = (await db.Claim(scope))!; Assert.Equal(fresh.Work.Id, claim.Work.Id); await db.Store.ExecuteAsync(claim, db.Effect, new());
        Assert.Null(await db.Claim(scope));
        var expired = await db.Store.AcceptAsync(StoreContractData.Submission(scope, "expired", notBefore: DateTimeOffset.UtcNow.AddDays(1), expiresAt: DateTimeOffset.UtcNow.AddDays(2)));
        await ExpireAsync(expired.Work.Id);
        var expiry = (await db.Claim(scope))!; Assert.Equal(expired.Work.Id, expiry.Work.Id);
        Assert.Equal(ExecutionStatus.Expired, (await db.Store.ExecuteAsync(expiry, db.Effect, new())).Status);
        var budget = (await Budgets.FindBudgetAsync(scope.Tenant))!; Assert.Equal(1, budget.Claims); Assert.Equal(0, budget.Retries);
    }
    [Fact]
    public async Task Outbox_retry_budget_filters_old_retry_and_rolls_over_using_database_time()
    {
        var scope = Scope(); var producer = new ProducerScope(scope.Tenant, "publisher");
        OutgoingMessage Message() => new(producer, Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "events"), [1]);
        var retry = Message(); await outbox.EnqueueAsync(retry);
        var first = (await outbox.Outbox.ClaimNextAsync(producer, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!;
        await outbox.Outbox.SettleAsync(first, new(PublishStatus.NotSentTransient, "test"), new(initialDelay: TimeSpan.FromMilliseconds(1)));
        var fresh = Message(); await outbox.EnqueueAsync(fresh); await Budgets.ConfigureBudgetAsync(Configure(scope.Tenant, claims: 1, retries: 0));
        var claim = (await outbox.Outbox.ClaimNextAsync(producer, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!; Assert.Equal(fresh.IntentId, claim.Record.Message.IntentId);
        await outbox.Outbox.SettleAsync(claim, new(PublishStatus.Accepted, "confirmed"), new());
        Assert.Null(await outbox.Outbox.ClaimNextAsync(producer, Guid.NewGuid(), TimeSpan.FromMinutes(1)));
        await AgeWindowAsync(scope.Tenant); // Synthetic old counter, native current DB clock determines the new window.
        await Budgets.ConfigureBudgetAsync(Configure(scope.Tenant, claims: 1, retries: 1, version: 0));
        Assert.Equal(retry.IntentId, (await outbox.Outbox.ClaimNextAsync(producer, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!.Record.Message.IntentId);
        var budget = (await Budgets.FindBudgetAsync(scope.Tenant))!; Assert.Equal(1, budget.Claims); Assert.Equal(1, budget.Retries);
    }
    [Fact]
    public async Task Recovery_quota_is_shared_and_denial_rolls_back_ingress_acceptance_while_keeping_its_receipt()
    {
        var scope = Scope(); await db.Store.AcceptAsync(StoreContractData.Submission(scope));
        var failed = await db.Store.ExecuteAsync((await db.Claim(scope))!, (_, _, _, _) => throw new PermanentWorkException("failed"), new());
        var heldScope = Scope(scope.Tenant, "held-input");
        var held = await Ingress.QuarantineAsync(heldScope, new("v1", "sha256-bytes/v1"), new("broker", "one", "test/v1", [1], []), "invalid");
        var release = new IngressReleaseRequest(Guid.NewGuid(), held.Item.Id, 0, StoreContractData.Submission(heldScope), "operator", "fixed");
        var producer = new ProducerScope(scope.Tenant, "publisher"); var message = new OutgoingMessage(producer, Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "events"), [1]);
        await outbox.EnqueueAsync(message); var publication = (await outbox.Outbox.ClaimNextAsync(producer, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!;
        await outbox.Outbox.SettleAsync(publication, new(PublishStatus.NotSentPermanent, "failed"), new());
        await Budgets.ConfigureBudgetAsync(Configure(scope.Tenant, recoveries: 1));
        var replay = new ReplayRequest(scope, Guid.NewGuid(), failed.Work.Id, failed.Work.Version, "operator", "fixed");
        Assert.Equal(ReplayStatus.Accepted, (await db.Store.ReplayAsync(replay)).Status);
        Assert.True((await db.Store.ReplayAsync(replay)).RepeatedRequest);
        Assert.Equal(IngressReleaseStatus.RateLimited, (await Ingress.ReleaseAsync(release)).Status);
        Assert.Null(await db.Claim(heldScope)); Assert.Null((await Ingress.FindQuarantineAsync(heldScope, held.Item.Id))!.ReleasedWorkId);
        var current = (await outbox.Recovery.FindMetadataAsync(producer, message.IntentId))!;
        var outgoingReplay = new OutboxReplayRequest(producer, Guid.NewGuid(), message.IntentId, current.Version, "operator", "fixed");
        var refusal = await outbox.Recovery.ReplayAsync(outgoingReplay); Assert.Equal(OutboxReplayStatus.RateLimited, refusal.Status); Assert.Equal(current.Version, refusal.Version);
        Assert.Equal(1, (await Budgets.FindBudgetAsync(scope.Tenant))!.Recoveries);
        await Budgets.ConfigureBudgetAsync(Configure(scope.Tenant, recoveries: 2, version: 0));
        Assert.Equal(IngressReleaseStatus.RateLimited, (await Ingress.ReleaseAsync(release)).Status);
        var retry = new IngressReleaseRequest(Guid.NewGuid(), release.QuarantineId, 0, release.Submission, release.Actor, release.Reason);
        Assert.Equal(IngressReleaseStatus.Accepted, (await Ingress.ReleaseAsync(retry)).Status);
        Assert.Equal(2, (await Budgets.FindBudgetAsync(scope.Tenant))!.Recoveries);
    }
    [Fact]
    public async Task Repair_refusal_preserves_quarantine_and_configuration_races_do_not_reset_spending()
    {
        var scope = Scope(); var producer = new ProducerScope(scope.Tenant, "publisher");
        var message = new OutgoingMessage(producer, Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "events"), [1]);
        await outbox.EnqueueAsync(message); await CorruptAsync(message);
        Assert.Null(await outbox.Outbox.ClaimNextAsync(producer, Guid.NewGuid(), TimeSpan.FromMinutes(1)));
        var held = (await outbox.Recovery.FindMetadataAsync(producer, message.IntentId))!;
        await Budgets.ConfigureBudgetAsync(Configure(scope.Tenant, recoveries: 0));
        var repair = new OutboxRepairRequest(Guid.NewGuid(), held.QuarantineId!.Value, message, held.Version, "operator", "restored");
        var refused = await ((IOutboxRepairStore)outbox.Recovery).RepairAsync(repair);
        Assert.Equal(OutboxRepairStatus.RateLimited, refused.Status); Assert.Equal(held.Version, refused.Version);
        Assert.Equal(held, await outbox.Recovery.FindMetadataAsync(producer, message.IntentId));
        var results = await Task.WhenAll(Budgets.ConfigureBudgetAsync(Configure(scope.Tenant, recoveries: 1, version: 0)),
            ((ITenantBudgetStore)db.RestartStore()).ConfigureBudgetAsync(Configure(scope.Tenant, recoveries: 2, version: 0)));
        Assert.Single(results, r => r.Status == TenantBudgetStatus.Applied); Assert.Single(results, r => r.Status == TenantBudgetStatus.VersionConflict);
        Assert.True((await ((IOutboxRepairStore)outbox.Recovery).RepairAsync(repair)).RepeatedRequest);
        var retry = new OutboxRepairRequest(Guid.NewGuid(), repair.QuarantineId, message, held.Version, repair.Actor, repair.Reason);
        Assert.Equal(OutboxRepairStatus.Accepted, (await ((IOutboxRepairStore)outbox.Recovery).RepairAsync(retry)).Status);
        Assert.Equal(1, (await Budgets.FindBudgetAsync(scope.Tenant))!.Recoveries);
    }

    [Fact]
    public async Task Concurrent_release_requests_cannot_create_orphan_work_when_only_one_recovery_is_allowed()
    {
        var scope = Scope(); await Budgets.ConfigureBudgetAsync(Configure(scope.Tenant, recoveries: 1));
        var requests = new List<IngressReleaseRequest>();
        for (var i = 0; i < 4; i++)
        {
            var input = await Ingress.QuarantineAsync(scope, new("v1", "sha256-bytes/v1"), new("broker", "pos-" + i, "test/v1", [1], []), "invalid");
            requests.Add(new(Guid.NewGuid(), input.Item.Id, 0, StoreContractData.Submission(scope, "key-" + i), "operator", "fixed"));
        }
        var results = await Task.WhenAll(requests.Select(request => Ingress.ReleaseAsync(request)));
        Assert.Single(results, result => result.Status == IngressReleaseStatus.Accepted);
        Assert.Equal(3, results.Count(result => result.Status == IngressReleaseStatus.RateLimited));
        Assert.NotNull(await db.Claim(scope)); Assert.Null(await db.Claim(scope));
        Assert.Equal(1, (await Budgets.FindBudgetAsync(scope.Tenant))!.Recoveries);
    }
}
