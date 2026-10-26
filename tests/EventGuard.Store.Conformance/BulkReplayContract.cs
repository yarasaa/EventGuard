namespace EventGuard.Store.Conformance;
public abstract class BulkReplayContract<TSession>(IWorkStoreFixture<TSession> db, IOutboxStoreFixture outbox)
{
    protected abstract IBulkReplayStore OpenBulk();
    protected async Task<BulkReplayRequest> PrepareRequest(bool outgoing, int count = 3)
    {
        var scope = new WorkScope(Guid.NewGuid().ToString("N"), "worker", "test", "event");
        var producer = new ProducerScope(scope.Tenant, "publisher"); var items = new List<BulkReplayItem>();
        for (var i = 0; i < count; i++)
        {
            if (outgoing)
            {
                var message = new OutgoingMessage(producer, Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "events"), [1]);
                await outbox.EnqueueAsync(message);
                var claim = (await outbox.Outbox.ClaimNextAsync(producer, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!;
                var failed = await outbox.Outbox.SettleAsync(claim, new(PublishStatus.Unknown, "reply-lost"), new(maximumAttempts: 1));
                items.Add(new(message.IntentId, failed.Record.Version));
            }
            else
            {
                await db.Store.AcceptAsync(StoreContractData.Submission(scope, "item-" + i));
                var failed = await db.Store.ExecuteAsync((await db.Claim(scope))!, (_, _, _, _) => throw new PermanentWorkException("test"), new());
                items.Add(new(failed.Work.Id, failed.Work.Version));
            }
        }
        return outgoing ? new(producer, Guid.NewGuid(), items, "operator", "reviewed selection", true)
            : new(scope, Guid.NewGuid(), items, "operator", "reviewed selection");
    }
    private static Task<BulkReplayProgress?> Find(IBulkReplayStore store, BulkReplayRequest request, string? actor = null) =>
        request.Kind == BulkReplayKind.Consumer ? store.FindBulkAsync(request.ConsumerScope!, request.RequestId, actor ?? request.Actor)
            : store.FindBulkAsync(request.OutboxScope!, request.RequestId, actor ?? request.Actor);
    [Theory] [InlineData(false)] [InlineData(true)]
    public async Task Preparing_is_read_only_for_work_and_concurrent_resumes_share_each_durable_member(bool outgoing)
    {
        var request = await PrepareRequest(outgoing); var first = OpenBulk(); var second = OpenBulk();
        Assert.Equal(BulkReplayStatus.Prepared, (await first.PrepareBulkAsync(request)).Status);
        var prepared = (await Find(second, request))!; Assert.False(prepared.Complete); Assert.All(prepared.Entries, e => Assert.Null(e.Status));
        Assert.Null(await Find(second, request, "other-actor"));
        await Task.WhenAll(first.RunBulkAsync(request), second.RunBulkAsync(request));
        var progress = (await Find(second, request))!; Assert.True(progress.Complete);
        Assert.All(progress.Entries, entry => { Assert.Equal("Accepted", entry.Status); Assert.Equal(1, entry.Generation); });
        if (outgoing)
        {
            for (var i = 0; i < request.Items.Count; i++)
            { var claim = (await outbox.Outbox.ClaimNextAsync(request.OutboxScope!, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!; await outbox.Outbox.SettleAsync(claim, new(PublishStatus.Accepted, "confirmed"), new()); }
            await first.RunBulkAsync(request);
            foreach (var item in request.Items) Assert.Equal(OutboxState.Published, (await outbox.Recovery.FindMetadataAsync(request.OutboxScope!, item.Id))!.State);
        }
        else
        {
            for (var i = 0; i < request.Items.Count; i++) await db.Store.ExecuteAsync((await db.Claim(request.ConsumerScope!))!, db.Effect, new());
            await first.RunBulkAsync(request);
            foreach (var item in request.Items) { Assert.Equal(WorkState.Completed, (await db.Store.FindAsync(request.ConsumerScope!, item.Id))!.State); Assert.Equal(1, await db.EffectCount(item.Id)); }
        }
    }
    [Theory] [InlineData(false)] [InlineData(true)]
    public async Task Changed_plan_conflicts_without_expanding_the_saved_selection_and_rate_limits_are_durable(bool outgoing)
    {
        var request = await PrepareRequest(outgoing); var store = OpenBulk();
        await ((ITenantBudgetStore)db.Store).ConfigureBudgetAsync(new(Guid.NewGuid(), new(request.ConsumerScope?.Tenant ?? request.OutboxScope!.Tenant, 86400, maximumRecoveries: 1), -1, "operator", "test limit"));
        await store.RunBulkAsync(request);
        var progress = (await Find(store, request))!;
        Assert.Equal("Accepted", progress.Entries[0].Status); Assert.All(progress.Entries.Skip(1), e => Assert.Equal("RateLimited", e.Status));
        BulkReplayRequest changed = outgoing ? new(request.OutboxScope!, request.RequestId, request.Items.Reverse(), request.Actor, request.Reason, true)
            : new(request.ConsumerScope!, request.RequestId, request.Items.Reverse(), request.Actor, request.Reason);
        Assert.Equal(BulkReplayStatus.RequestConflict, (await store.RunBulkAsync(changed)).Status);
        await ((ITenantBudgetStore)db.Store).ConfigureBudgetAsync(new(Guid.NewGuid(), new(request.ConsumerScope?.Tenant ?? request.OutboxScope!.Tenant, 86400, maximumRecoveries: 10), 0, "operator", "increase"));
        await store.RunBulkAsync(request); Assert.Equal(progress.Entries, (await Find(store, request))!.Entries);
        Assert.Equal(1, (await ((ITenantBudgetStore)db.Store).FindBudgetAsync(request.ConsumerScope?.Tenant ?? request.OutboxScope!.Tenant))!.Recoveries);
    }
    [Fact]
    public async Task A_conflicting_reserved_member_id_is_reported_without_exposing_the_other_request()
    {
        var request = await PrepareRequest(false, 1); var member = request.ConsumerRequest(0);
        await db.Store.ReplayAsync(new(member.Scope, member.RequestId, member.WorkId, member.ExpectedVersion, "another-actor", "private-reason"));
        await OpenBulk().RunBulkAsync(request);
        var progress = (await Find(OpenBulk(), request))!;
        Assert.Equal("RequestConflict", Assert.Single(progress.Entries).Status); Assert.True(progress.Complete);
        Assert.DoesNotContain("private-reason", System.Text.Json.JsonSerializer.Serialize(progress));
    }
    protected async Task ResolveMemberReplyLoss(IBulkReplayStore interrupted, Task dropped, BulkReplayRequest request)
    {
        await OpenBulk().PrepareBulkAsync(request); // Proxy must interrupt a member COMMIT, not plan creation.
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.RunBulkAsync(request));
        await dropped.WaitAsync(TimeSpan.FromSeconds(5));
        var store = OpenBulk(); var partial = (await Find(store, request))!;
        Assert.Equal("Accepted", partial.Entries[0].Status); Assert.Null(partial.Entries[1].Status);
        await store.RunBulkAsync(request);
        var completed = (await Find(store, request))!; Assert.True(completed.Complete);
        Assert.All(completed.Entries, e => { Assert.Equal("Accepted", e.Status); Assert.Equal(1, e.Generation); });
    }
}
