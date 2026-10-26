namespace EventGuard.Store.Conformance;

public abstract class WorkStoreContract<TSession>(IWorkStoreFixture<TSession> db)
{
    [Fact]
    public async Task A_rejected_replay_receipt_stays_rejected_after_the_work_later_fails()
    {
        var scope = StoreContractData.Scope();
        var work = (await db.Store.AcceptAsync(StoreContractData.Submission(scope))).Work;
        var request = new ReplayRequest(scope, Guid.NewGuid(), work.Id, work.Version, "operator", "early request");
        var rejected = await db.Store.ReplayAsync(request);
        Assert.Equal(ReplayStatus.NotFailed, rejected.Status);
        await Fail(scope);
        Assert.Equal(rejected with { RepeatedRequest = true }, await db.Store.ReplayAsync(request));
        var current = (await db.Store.FindAsync(scope, work.Id))!;
        Assert.Equal(WorkState.DeadLetter, current.State);
        Assert.Equal(0, current.ReplayGeneration);
    }

    [Fact]
    public async Task A_returned_claim_cannot_replace_the_durable_handler_payload()
    {
        var scope = StoreContractData.Scope();
        await db.Store.AcceptAsync(StoreContractData.Submission(scope, payload: [1]));
        var claim = (await db.Claim(scope))!;
        claim.Payload[0] = 99;
        byte[]? observed = null;
        var result = await db.Store.ExecuteAsync(claim, async (work, payload, session, ct) =>
        {
            observed = payload.ToArray();
            await db.Effect(work, payload, session, ct);
        }, new());
        Assert.Equal(ExecutionStatus.Completed, result.Status);
        Assert.Equal(new byte[] { 1 }, observed);
        Assert.Equal(1, await db.EffectCount(claim.Work.Id));
    }

    [Fact]
    public async Task Concurrent_accepts_create_one_work_for_different_delivery_ids()
    {
        var scope = StoreContractData.Scope();
        var results = await Task.WhenAll(Enumerable.Range(0, 32).Select(_ => db.Store.AcceptAsync(StoreContractData.Submission(scope))));
        Assert.Single(results, r => r.Status == AcceptanceStatus.Accepted);
        Assert.Equal(31, results.Count(r => r.Status == AcceptanceStatus.Duplicate));
        Assert.Single(results.Select(r => r.Work.Id).Distinct());
    }

    [Fact]
    public async Task Policy_change_is_rejected_even_after_queue_is_drained()
    {
        var scope = StoreContractData.Scope();
        await db.Store.AcceptAsync(StoreContractData.Submission(scope));
        await db.Store.ExecuteAsync((await db.Claim(scope))!, db.Effect, new());
        await Assert.ThrowsAsync<PolicyMismatchException>(() => db.Store.AcceptAsync(StoreContractData.Submission(scope, key: "new-key", version: "v2")));
    }

    [Fact]
    public async Task Business_write_and_completed_commit_together_and_stale_claim_cannot_repeat_effect()
    {
        var scope = StoreContractData.Scope();
        var accepted = await db.Store.AcceptAsync(StoreContractData.Submission(scope));
        var claim = (await db.Claim(scope))!;
        var completed = await db.Store.ExecuteAsync(claim, db.Effect, new());
        Assert.Equal(ExecutionStatus.Completed, completed.Status);
        Assert.Equal(WorkState.Completed, (await db.Store.FindAsync(scope, accepted.Work.Id))!.State);
        Assert.Equal(ExecutionStatus.LeaseLost, (await db.Store.ExecuteAsync(claim, db.Effect, new())).Status);
        Assert.Equal(1, await db.EffectCount(accepted.Work.Id));
    }

    [Fact]
    public async Task Handler_failure_rolls_back_business_and_retry_time_survives_store_restart()
    {
        var scope = StoreContractData.Scope();
        var accepted = await db.Store.AcceptAsync(StoreContractData.Submission(scope));
        var result = await db.Store.ExecuteAsync((await db.Claim(scope))!, async (w, p, s, ct) =>
        {
            await db.Effect(w, p, s, ct);
            throw new InvalidOperationException("synthetic failure");
        }, new(initialDelay: TimeSpan.FromMinutes(1)));
        Assert.Equal(ExecutionStatus.RetryScheduled, result.Status);
        Assert.Equal(0, await db.EffectCount(accepted.Work.Id));
        var restarted = db.RestartStore();
        Assert.Equal(result.Work.NextAttemptAt, (await restarted.FindAsync(scope, accepted.Work.Id))!.NextAttemptAt);
        Assert.Null(await restarted.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Repeated_replay_remains_idempotent_after_work_fails_again()
    {
        var scope = StoreContractData.Scope();
        var accepted = await db.Store.AcceptAsync(StoreContractData.Submission(scope));
        var failure = await Fail(scope);
        var request = new ReplayRequest(scope, Guid.NewGuid(), accepted.Work.Id, failure.Work.Version, "operator", "fixed configuration");
        var results = await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => db.Store.ReplayAsync(request)));
        Assert.All(results, r => Assert.Equal(ReplayStatus.Accepted, r.Status));
        Assert.Single(results, r => !r.RepeatedRequest);
        Assert.All(results, r => Assert.Equal(1, r.Generation));
        await Fail(scope);
        var repeated = await db.Store.ReplayAsync(request);
        Assert.True(repeated.RepeatedRequest);
        Assert.Equal(1, (await db.Store.FindAsync(scope, accepted.Work.Id))!.ReplayGeneration);
        Assert.Equal(WorkState.DeadLetter, (await db.Store.FindAsync(scope, accepted.Work.Id))!.State);
        var conflicting = await db.Store.ReplayAsync(new(scope, request.RequestId, accepted.Work.Id, request.ExpectedVersion, "operator", "changed request"));
        Assert.Equal(ReplayStatus.RequestConflict, conflicting.Status);
    }

    [Fact]
    public async Task Only_one_new_replay_can_advance_the_same_row_version()
    {
        var scope = StoreContractData.Scope();
        await db.Store.AcceptAsync(StoreContractData.Submission(scope));
        var failed = await Fail(scope);
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => db.Store.ReplayAsync(
            new(scope, Guid.NewGuid(), failed.Work.Id, failed.Work.Version, "operator", "retry"))));
        Assert.Single(results, r => r.Status == ReplayStatus.Accepted);
        Assert.Equal(1, (await db.Store.FindAsync(scope, failed.Work.Id))!.ReplayGeneration);
    }

    [Fact]
    public async Task Completed_work_and_other_consumer_are_not_replayed()
    {
        var a = StoreContractData.Scope(); var b = StoreContractData.Scope();
        var first = await db.Store.AcceptAsync(StoreContractData.Submission(a));
        await db.Store.AcceptAsync(StoreContractData.Submission(b));
        var completed = await db.Store.ExecuteAsync((await db.Claim(a))!, db.Effect, new());
        var failed = await Fail(b);
        Assert.Equal(ReplayStatus.Accepted, (await db.Store.ReplayAsync(new(b, Guid.NewGuid(), failed.Work.Id, failed.Work.Version, "op", "retry"))).Status);
        Assert.Equal(ReplayStatus.NotFailed, (await db.Store.ReplayAsync(new(a, Guid.NewGuid(), completed.Work.Id, completed.Work.Version, "op", "retry"))).Status);
        Assert.Equal(1, await db.EffectCount(first.Work.Id));
        Assert.Null(await db.Store.FindAsync(b, first.Work.Id));
    }

    [Fact]
    public async Task Open_business_transaction_blocks_recovery_after_lease_expires()
    {
        var scope = StoreContractData.Scope();
        var accepted = await db.Store.AcceptAsync(StoreContractData.Submission(scope));
        var claim = (await db.Claim(scope, TimeSpan.FromSeconds(1)))!;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var executing = db.Store.ExecuteAsync(claim, async (w, p, s, ct) =>
        {
            await db.Effect(w, p, s, ct); started.SetResult();
            await release.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
        }, new());
        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await WaitPast(claim.LeaseUntil);
            Assert.Null(await db.Claim(scope));
            Assert.Equal(0, await db.EffectCount(accepted.Work.Id));
        }
        finally { release.TrySetResult(); }
        Assert.Equal(ExecutionStatus.Completed, (await executing).Status);
        Assert.Equal(1, await db.EffectCount(accepted.Work.Id));
    }

    [Fact]
    public async Task Locked_expired_owner_does_not_block_claiming_the_next_ready_work()
    {
        var scope = StoreContractData.Scope();
        await db.Store.AcceptAsync(StoreContractData.Submission(scope,"held"));
        var first = (await db.Claim(scope,TimeSpan.FromSeconds(1)))!;
        var second = await db.Store.AcceptAsync(StoreContractData.Submission(scope,"ready"));
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var executing = db.Store.ExecuteAsync(first,async (w,p,s,ct) =>
        {
            await db.Effect(w,p,s,ct); started.SetResult();
            await release.Task.WaitAsync(TimeSpan.FromSeconds(15),ct);
        },new());
        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await WaitPast(first.LeaseUntil);
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var next = (await db.Store.ClaimNextAsync(scope,Guid.NewGuid(),TimeSpan.FromSeconds(10),timeout.Token))!;
            Assert.Equal(second.Work.Id,next.Work.Id);
            Assert.Equal(ExecutionStatus.Completed,(await db.Store.ExecuteAsync(next,db.Effect,new(),timeout.Token)).Status);
            Assert.Null(await db.Store.ClaimNextAsync(scope,Guid.NewGuid(),TimeSpan.FromSeconds(10),timeout.Token));
        }
        finally { release.TrySetResult(); await executing; }
        Assert.Equal(ExecutionStatus.Completed,(await executing).Status);
        Assert.Equal(1,await db.EffectCount(first.Work.Id));
        Assert.Equal(1,await db.EffectCount(second.Work.Id));
    }

    [Fact]
    public async Task Expired_owner_cannot_write_after_another_worker_claims()
    {
        var scope = StoreContractData.Scope();
        await db.Store.AcceptAsync(StoreContractData.Submission(scope));
        var old = (await db.Claim(scope, TimeSpan.FromMilliseconds(150)))!;
        await WaitPast(old.LeaseUntil);
        var current = (await db.Claim(scope))!;
        Assert.Equal(ExecutionStatus.LeaseLost, (await db.Store.ExecuteAsync(old, db.Effect, new())).Status);
        Assert.Equal(ExecutionStatus.Completed, (await db.Store.ExecuteAsync(current, db.Effect, new())).Status);
        Assert.Equal(1, await db.EffectCount(old.Work.Id));
    }

    [Fact]
    public async Task Repeated_claim_request_never_claims_a_second_work()
    {
        var scope = StoreContractData.Scope();
        await db.Store.AcceptAsync(StoreContractData.Submission(scope, "one"));
        await db.Store.AcceptAsync(StoreContractData.Submission(scope, "two"));
        var requestId = Guid.NewGuid();
        var first = (await db.Store.ClaimNextAsync(scope, requestId, TimeSpan.FromSeconds(10)))!;
        Assert.Equal(first.Work.Id, (await db.Store.ClaimNextAsync(scope, requestId, TimeSpan.FromSeconds(10)))!.Work.Id);
        await db.Store.ExecuteAsync(first, db.Effect, new());
        Assert.Null(await db.Store.ClaimNextAsync(scope, requestId, TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Retry_expiry_is_claimed_before_the_future_retry_time_without_running_business_again()
    {
        var scope=StoreContractData.Scope();
        var expiry=DateTimeOffset.UtcNow.AddSeconds(2);
        var accepted=await db.Store.AcceptAsync(StoreContractData.Submission(scope,expiresAt:expiry));
        var failure=await db.Store.ExecuteAsync((await db.Claim(scope))!,(_,_,_,_)=>throw new InvalidOperationException("synthetic transient"),new(initialDelay:TimeSpan.FromMinutes(1),jitterRatio:0));
        Assert.Equal(ExecutionStatus.RetryScheduled,failure.Status); Assert.True(failure.Work.NextAttemptAt>expiry);
        await WaitPast(expiry);
        var expired=(await db.Claim(scope))!;
        Assert.Equal(accepted.Work.Id,expired.Work.Id);
        Assert.Equal(ExecutionStatus.Expired,(await db.Store.ExecuteAsync(expired,db.Effect,new())).Status);
        Assert.Equal(0,await db.EffectCount(accepted.Work.Id)); Assert.Null(await db.Claim(scope));
    }

    [Fact]
    public async Task Concurrent_claims_ignore_same_scope_future_work_and_live_leases()
    {
        var scope=StoreContractData.Scope();
        var held=new List<WorkClaim>();
        for (var i=0;i<12;i++)
        {
            await db.Store.AcceptAsync(StoreContractData.Submission(scope,"held-"+i));
            held.Add((await db.Claim(scope,TimeSpan.FromHours(1)))!);
        }
        for (var i=0;i<24;i++)
            await db.Store.AcceptAsync(StoreContractData.Submission(scope,"scheduled-"+i,notBefore:DateTimeOffset.UtcNow.AddDays(1)));
        var due=new List<Guid>();
        for (var i=0;i<16;i++) due.Add((await db.Store.AcceptAsync(StoreContractData.Submission(scope,"due-"+i))).Work.Id);
        var other=StoreContractData.Scope(); await db.Store.AcceptAsync(StoreContractData.Submission(other));
        var claims=new List<WorkClaim>();
        for (var round=0;round<16 && claims.Count<due.Count;round++)
            claims.AddRange((await Task.WhenAll(Enumerable.Range(0,4).Select(_=>db.Claim(scope,TimeSpan.FromMinutes(1))))).OfType<WorkClaim>());
        Assert.Equal(due.Order(),claims.Select(c=>c.Work.Id).Order());
        foreach (var claim in claims)
        {
            Assert.Equal(ExecutionStatus.Completed,(await db.Store.ExecuteAsync(claim!,db.Effect,new())).Status);
            Assert.Equal(1,await db.EffectCount(claim!.Work.Id));
        }
        Assert.Null(await db.Claim(scope));
        foreach (var claim in held)
        {
            var repeat=(await db.Store.ClaimNextAsync(scope,claim.Token,TimeSpan.FromHours(1)))!;
            Assert.Equal(claim.Work,repeat.Work); Assert.Equal(claim.LeaseUntil,repeat.LeaseUntil);
        }
        Assert.NotNull(await db.Claim(other));
    }

    [Fact]
    public async Task Concurrent_claims_of_due_and_expired_work_never_return_the_same_owner()
    {
        var scope = StoreContractData.Scope();
        var expected = new List<Guid>();
        for (var i = 0; i < 12; i++)
            expected.Add((await db.Store.AcceptAsync(StoreContractData.Submission(scope, "expired-" + i,
                expiresAt: DateTimeOffset.UtcNow.AddMinutes(-1)))).Work.Id);
        // Each work is eligible through both the due-time and expiry-time branches.
        // Contending readers may temporarily skip all locked rows, so drain in bounded rounds.
        var claims = new List<WorkClaim>();
        for (var round = 0; round < 12 && claims.Count < expected.Count; round++)
        {
            var batch = await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => db.Claim(scope, TimeSpan.FromMinutes(1))));
            claims.AddRange(batch.OfType<WorkClaim>());
        }
        Assert.Equal(expected.Order(), claims.Select(c => c.Work.Id).Order());
        Assert.Null(await db.Claim(scope));
        foreach (var claim in claims)
        {
            Assert.Equal(1, claim.Work.AttemptCount);
            Assert.Equal(ExecutionStatus.Expired, (await db.Store.ExecuteAsync(claim, db.Effect, new())).Status);
            Assert.Equal(0, await db.EffectCount(claim.Work.Id));
        }
    }

    [Fact]
    public async Task Concurrent_claims_drain_mixed_expiry_recovery_and_ready_work()
    {
        var scope = StoreContractData.Scope();
        var oldOwners = new List<WorkClaim>();
        for (var i = 0; i < 4; i++)
        {
            await db.Store.AcceptAsync(StoreContractData.Submission(scope, "recover-" + i));
            oldOwners.Add((await db.Claim(scope, TimeSpan.FromSeconds(1)))!);
        }
        var expected = oldOwners.Select(c => c.Work.Id).ToList();
        var expired = new HashSet<Guid>();
        for (var i = 0; i < 4; i++)
        {
            expected.Add((await db.Store.AcceptAsync(StoreContractData.Submission(scope, "ready-" + i))).Work.Id);
            var id = (await db.Store.AcceptAsync(StoreContractData.Submission(scope, "expired-" + i,
                expiresAt: DateTimeOffset.UtcNow.AddMinutes(-1)))).Work.Id;
            expected.Add(id); expired.Add(id);
        }
        await WaitPast(oldOwners.Max(c => c.LeaseUntil));
        var claims = new List<WorkClaim>();
        for (var round = 0; round < 12 && claims.Count < expected.Count; round++)
            claims.AddRange((await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => db.Claim(scope, TimeSpan.FromMinutes(1))))).OfType<WorkClaim>());
        Assert.Equal(expected.Order(), claims.Select(c => c.Work.Id).Order());
        foreach (var old in oldOwners)
            Assert.Equal(ExecutionStatus.LeaseLost, (await db.Store.ExecuteAsync(old, db.Effect, new())).Status);
        foreach (var claim in claims)
        {
            var isExpired = expired.Contains(claim.Work.Id);
            Assert.Equal(isExpired ? ExecutionStatus.Expired : ExecutionStatus.Completed,
                (await db.Store.ExecuteAsync(claim, db.Effect, new())).Status);
            Assert.Equal(isExpired ? 0 : 1, await db.EffectCount(claim.Work.Id));
        }
        Assert.Null(await db.Claim(scope));
    }

    [Fact]
    public async Task Not_before_is_durable_and_expired_work_never_invokes_handler()
    {
        var scheduled = StoreContractData.Scope();
        await db.Store.AcceptAsync(StoreContractData.Submission(scheduled, notBefore: DateTimeOffset.UtcNow.AddHours(1)));
        Assert.Null(await db.Claim(scheduled));
        var expired = StoreContractData.Scope();
        var accepted = await db.Store.AcceptAsync(StoreContractData.Submission(expired, expiresAt: DateTimeOffset.UtcNow.AddMinutes(-1)));
        Assert.Equal(ExecutionStatus.Expired, (await db.Store.ExecuteAsync((await db.Claim(expired))!, db.Effect, new())).Status);
        Assert.Equal(0, await db.EffectCount(accepted.Work.Id));
    }

    private async Task<ExecutionResult> Fail(WorkScope scope) => await db.Store.ExecuteAsync((await db.Claim(scope))!,
        (_, _, _, _) => throw new PermanentWorkException("synthetic-permanent-failure"), new());

    private static async Task WaitPast(DateTimeOffset value)
    {
        var delay = value - DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(40);
        if (delay > TimeSpan.Zero) await Task.Delay(delay);
    }
}
