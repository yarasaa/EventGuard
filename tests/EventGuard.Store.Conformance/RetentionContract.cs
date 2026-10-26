namespace EventGuard.Store.Conformance;

public abstract class RetentionContract<TSession>(IWorkStoreFixture<TSession> workFixture, IOutboxStoreFixture outboxFixture)
{
    protected abstract Task AgeAsync(string scopeId);
    protected abstract Task<byte[]> DataAsync(Guid id, RetentionTarget target);
    protected abstract Task<Guid[]> ConflictIdsAsync(string scopeId);
    protected abstract Task CorruptAsync(OutgoingMessage message);
    private IRetentionStore Store => (IRetentionStore)workFixture.Store;
    private IIngressQuarantineStore Ingress => (IIngressQuarantineStore)workFixture.Store;
    private static readonly DateTimeOffset Cutoff = new(2021, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static IdentityPolicy Policy => new("v1", "sha256-bytes/v1");
    private static RetentionRequest Request(WorkScope scope, RetentionTarget target = RetentionTarget.ConsumerPayload, int batch = 100) => new(scope, Guid.NewGuid(), target, Cutoff, "operator", "configured retention", batch);
    private static RetentionRequest Request(ProducerScope scope, RetentionTarget target = RetentionTarget.PublishedOutboxPayload) => new(scope, Guid.NewGuid(), target, Cutoff, "operator", "configured retention");
    private async Task<AcceptanceResult> Complete(WorkScope scope, string key)
    {
        var accepted = await workFixture.Store.AcceptAsync(StoreContractData.Submission(scope, key, [1, 2]));
        var claim = (await workFixture.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!;
        Assert.Equal(accepted.Work.Id, claim.Work.Id);
        await workFixture.Store.ExecuteAsync(claim, workFixture.Effect, new());
        return accepted;
    }
    private static OutgoingMessage Message(ProducerScope scope) => new(scope, Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "resource.changed"), [1, 2]);
    private async Task Publish(OutgoingMessage message)
    {
        await outboxFixture.EnqueueAsync(message);
        var claim = (await outboxFixture.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!;
        Assert.Equal(message.IntentId, claim.Record.Message.IntentId);
        await outboxFixture.Outbox.SettleAsync(claim, new(PublishStatus.Accepted, "confirmed"), new());
    }

    [Fact]
    public async Task Completed_payload_is_removed_but_deduplication_policy_and_claim_receipts_remain()
    {
        var scope = StoreContractData.Scope(); var original = StoreContractData.Submission(scope, payload: [1, 2]);
        var accepted = await workFixture.Store.AcceptAsync(original);
        var claim = (await workFixture.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!;
        await workFixture.Store.ExecuteAsync(claim, workFixture.Effect, new()); await AgeAsync(scope.Id);
        var result = await Store.RetainAsync(Request(scope)); Assert.Equal(new[] { accepted.Work.Id }, result.PurgedIds);
        Assert.Empty(await DataAsync(accepted.Work.Id, RetentionTarget.ConsumerPayload));
        Assert.Equal(AcceptanceStatus.Duplicate, (await workFixture.Store.AcceptAsync(original)).Status);
        Assert.Equal(WorkState.Completed, (await workFixture.Store.FindAsync(scope, accepted.Work.Id))!.State);
        await Assert.ThrowsAsync<PolicyMismatchException>(() => workFixture.Store.AcceptAsync(StoreContractData.Submission(scope, version: "v2")));
        await workFixture.Store.AcceptAsync(StoreContractData.Submission(scope, "fresh-work"));
        Assert.Null(await workFixture.Store.ClaimNextAsync(scope, claim.Token, TimeSpan.FromMinutes(1)));
        Assert.Equal(1, await workFixture.EffectCount(accepted.Work.Id));
    }

    [Fact]
    public async Task Repeating_a_bounded_batch_never_removes_the_next_batch()
    {
        var scope = StoreContractData.Scope(); await Complete(scope, "first"); await Complete(scope, "second"); await AgeAsync(scope.Id);
        var request = Request(scope, batch: 1); var first = await Store.RetainAsync(request); Assert.Single(first.PurgedIds);
        var again = await ((IRetentionStore)workFixture.RestartStore()).RetainAsync(request);
        Assert.True(again.RepeatedRequest); Assert.Equal(first.PurgedIds, again.PurgedIds);
        var different = new RetentionRequest(scope, request.RequestId, request.Target, Cutoff, "operator", "different", 1);
        Assert.Equal(RetentionStatus.RequestConflict, (await Store.RetainAsync(different)).Status);
        var second = await Store.RetainAsync(Request(scope, batch: 1)); Assert.Single(second.PurgedIds);
        Assert.NotEqual(first.PurgedIds[0], second.PurgedIds[0]); Assert.Empty((await Store.RetainAsync(Request(scope))).PurgedIds);
    }

    [Fact]
    public async Task Pending_processing_retriable_deadletter_and_recent_completed_data_are_protected()
    {
        var scope = StoreContractData.Scope(); var completed = await Complete(scope, "old-completed");
        var failed = await workFixture.Store.AcceptAsync(StoreContractData.Submission(scope, "failed"));
        var failureClaim = (await workFixture.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!;
        await workFixture.Store.ExecuteAsync(failureClaim, (_, _, _, _) => throw new PermanentWorkException("test"), new());
        var pending = await workFixture.Store.AcceptAsync(StoreContractData.Submission(scope, "pending", notBefore: DateTimeOffset.UtcNow.AddDays(1)));
        var active = await workFixture.Store.AcceptAsync(StoreContractData.Submission(scope, "active"));
        Assert.NotNull(await workFixture.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1)));
        await AgeAsync(scope.Id);
        var recent = await Complete(scope, "recent");
        var result = await Store.RetainAsync(Request(scope)); Assert.Equal(new[] { completed.Work.Id }, result.PurgedIds);
        foreach (var id in new[] { failed.Work.Id, pending.Work.Id, active.Work.Id, recent.Work.Id }) Assert.NotEmpty(await DataAsync(id, RetentionTarget.ConsumerPayload));
        var tooRecent = new RetentionRequest(scope, Guid.NewGuid(), RetentionTarget.ConsumerPayload, DateTimeOffset.UtcNow, "operator", "too soon");
        await Assert.ThrowsAsync<ArgumentException>(() => Store.RetainAsync(tooRecent));
    }

    [Fact]
    public async Task Expired_deadletter_data_is_removed_without_reopening_the_work()
    {
        var scope = StoreContractData.Scope();
        var submission = StoreContractData.Submission(scope, "expires", expiresAt: DateTimeOffset.UtcNow.AddDays(1));
        var accepted = await workFixture.Store.AcceptAsync(submission);
        var claim = (await workFixture.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!;
        await workFixture.Store.ExecuteAsync(claim, (_, _, _, _) => throw new PermanentWorkException("test"), new());
        await AgeAsync(scope.Id);
        Assert.Equal(new[] { accepted.Work.Id }, (await Store.RetainAsync(Request(scope))).PurgedIds);
        var work = (await workFixture.Store.FindAsync(scope, accepted.Work.Id))!;
        Assert.Equal(WorkState.DeadLetter, work.State);
        var replay = await workFixture.Store.ReplayAsync(new(scope, Guid.NewGuid(), work.Id, work.Version, "operator", "expired"));
        Assert.Equal(ReplayStatus.PayloadExpired, replay.Status);
        Assert.Equal(AcceptanceStatus.Duplicate, (await workFixture.Store.AcceptAsync(submission)).Status);
        Assert.Equal(0, await workFixture.EffectCount(work.Id));
    }

    [Fact]
    public async Task Published_outbox_data_is_removed_without_reopening_or_forgetting_publication()
    {
        var scope = new ProducerScope("tenant", Guid.NewGuid().ToString("N")); var published = Message(scope); await Publish(published);
        var unknown = Message(scope); await outboxFixture.EnqueueAsync(unknown);
        var claim = (await outboxFixture.Outbox.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!;
        await outboxFixture.Outbox.SettleAsync(claim, new(PublishStatus.Unknown, "reply-lost"), new(maximumAttempts: 1));
        await AgeAsync(scope.Id);
        var result = await Store.RetainAsync(Request(scope)); Assert.Equal(new[] { published.IntentId }, result.PurgedIds);
        await Assert.ThrowsAsync<PayloadUnavailableException>(() => outboxFixture.Outbox.FindAsync(scope, published.IntentId));
        Assert.Equal(OutboxState.Published, (await outboxFixture.Recovery.FindMetadataAsync(scope, published.IntentId))!.State);
        await outboxFixture.EnqueueAsync(published); // Durable producer receipt still suppresses the original request.
        Assert.NotEmpty(await DataAsync(unknown.IntentId, RetentionTarget.PublishedOutboxPayload));
        var metadata = (await outboxFixture.Recovery.FindMetadataAsync(scope, published.IntentId))!;
        Assert.Equal(OutboxReplayStatus.NotFailed, (await outboxFixture.Recovery.ReplayAsync(new(scope, Guid.NewGuid(), published.IntentId, metadata.Version, "operator", "must stay published", true))).Status);
        Assert.Null(await outboxFixture.Outbox.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task Released_ingress_raw_data_can_expire_but_held_data_and_release_receipts_remain()
    {
        var scope = StoreContractData.Scope(); var raw = new IngressSnapshot("broker", "one", "test/v1", [4], [5]);
        var released = await Ingress.QuarantineAsync(scope, Policy, raw, "invalid-model");
        var held = await Ingress.QuarantineAsync(scope, Policy, new("broker", "two", "test/v1", [6], [7]), "invalid-model");
        var release = new IngressReleaseRequest(Guid.NewGuid(), released.Item.Id, 0, StoreContractData.Submission(scope), "operator", "fixed");
        var accepted = await Ingress.ReleaseAsync(release); await AgeAsync(scope.Id);
        Assert.Equal(new[] { released.Item.Id }, (await Store.RetainAsync(Request(scope, RetentionTarget.ReleasedIngressData))).PurgedIds);
        Assert.False((await Ingress.FindQuarantineAsync(scope, released.Item.Id))!.DataRetained);
        await Assert.ThrowsAsync<PayloadUnavailableException>(() => Ingress.ReadQuarantineDataAsync(scope, released.Item.Id));
        Assert.NotNull(await Ingress.ReadQuarantineDataAsync(scope, held.Item.Id));
        Assert.True((await Ingress.ReleaseAsync(release)).RepeatedRequest);
        Assert.Equal(IngressQuarantineStatus.Duplicate, (await Ingress.QuarantineAsync(scope, Policy, raw, "invalid-model")).Status);
        Assert.NotEmpty(await DataAsync(accepted.WorkId!.Value, RetentionTarget.ConsumerPayload));
    }

    [Fact]
    public async Task Conflict_data_requires_a_closed_parent_and_keeps_the_conflict_identity()
    {
        var scope = StoreContractData.Scope(); var completed = await Complete(scope, "done");
        Assert.Equal(AcceptanceStatus.Conflict, (await workFixture.Store.AcceptAsync(StoreContractData.Submission(scope, "done", [9]))).Status);
        var expected = Assert.Single(await ConflictIdsAsync(scope.Id));
        await workFixture.Store.AcceptAsync(StoreContractData.Submission(scope, "pending", [1], notBefore: DateTimeOffset.UtcNow.AddDays(1)));
        await workFixture.Store.AcceptAsync(StoreContractData.Submission(scope, "pending", [9])); await AgeAsync(scope.Id);
        Assert.Equal(new[] { expected }, (await Store.RetainAsync(Request(scope, RetentionTarget.ConflictPayload))).PurgedIds);
        Assert.Equal(2, (await ConflictIdsAsync(scope.Id)).Length);
        Assert.Empty(await DataAsync(expected, RetentionTarget.ConflictPayload)); Assert.NotEmpty(await DataAsync(completed.Work.Id, RetentionTarget.ConsumerPayload));
    }

    [Fact]
    public async Task Closed_outbox_quarantine_snapshot_can_expire_only_after_confirmed_publication()
    {
        var scope = new ProducerScope("tenant", Guid.NewGuid().ToString("N")); var repaired = Message(scope);
        await outboxFixture.EnqueueAsync(repaired); await CorruptAsync(repaired);
        Assert.Null(await outboxFixture.Outbox.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1)));
        var quarantine = (await outboxFixture.Recovery.FindMetadataAsync(scope, repaired.IntentId))!;
        await ((IOutboxRepairStore)outboxFixture.Recovery).RepairAsync(new(Guid.NewGuid(), quarantine.QuarantineId!.Value, repaired, quarantine.Version, "operator", "fixed"));
        var publishing = (await outboxFixture.Outbox.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!;
        await outboxFixture.Outbox.SettleAsync(publishing, new(PublishStatus.Accepted, "confirmed"), new());
        var held = Message(scope); await outboxFixture.EnqueueAsync(held); await CorruptAsync(held);
        Assert.Null(await outboxFixture.Outbox.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1))); await AgeAsync(scope.Id);
        var result = await Store.RetainAsync(Request(scope, RetentionTarget.PublishedOutboxQuarantineData));
        Assert.Equal(new[] { quarantine.QuarantineId.Value }, result.PurgedIds);
        Assert.Equal(OutboxState.Quarantined, (await outboxFixture.Recovery.FindMetadataAsync(scope, held.IntentId))!.State);
        Assert.NotEmpty(await DataAsync(repaired.IntentId, RetentionTarget.PublishedOutboxPayload));
    }

    [Fact]
    public async Task Concurrent_batches_are_disjoint_and_do_not_purge_another_scope()
    {
        var scope = StoreContractData.Scope(); var other = StoreContractData.Scope();
        for (var i = 0; i < 12; i++) await Complete(scope, "item-" + i);
        var protectedWork = await Complete(other, "other"); await AgeAsync(scope.Id); await AgeAsync(other.Id);
        var batches = await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => Store.RetainAsync(Request(scope, batch: 4))));
        Assert.All(batches, batch => Assert.InRange(batch.PurgedIds.Count, 0, 4));
        var ids = batches.SelectMany(r => r.PurgedIds).ToList();
        Assert.Equal(ids.Count, ids.Distinct().Count());
        // READPAST / SKIP LOCKED may return an empty batch while a peer holds eligible rows.
        // A new request after peers finish drains the remainder; a repeated request keeps its receipt.
        for (var i = 0; i < 3 && ids.Count < 12; i++)
            ids.AddRange((await Store.RetainAsync(Request(scope, batch: 4))).PurgedIds);
        Assert.Equal(12, ids.Count); Assert.Equal(12, ids.Distinct().Count());
        Assert.NotEmpty(await DataAsync(protectedWork.Work.Id, RetentionTarget.ConsumerPayload));
    }
}
