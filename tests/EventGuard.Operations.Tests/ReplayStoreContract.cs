using EventGuard.Store.Conformance;

namespace EventGuard.Operations.Tests;

public abstract class ReplayStoreContract<TSession>(IWorkStoreFixture<TSession> db)
{
    protected abstract Task<IWorkReplayStore> OpenAsync();

    [Fact]
    public async Task Two_command_instances_share_one_durable_request_and_keep_actor_reason_and_scope()
    {
        var scope = StoreContractData.Scope();
        await db.Store.AcceptAsync(StoreContractData.Submission(scope));
        var failed = await db.Store.ExecuteAsync((await db.Claim(scope))!, (_, _, _, _) => throw new PermanentWorkException("failed"), new());
        var request = new ReplayRequest(scope, Guid.NewGuid(), failed.Work.Id, failed.Work.Version, "operator-a", "Dependency repaired <script>");
        var first = await OpenAsync(); var second = await OpenAsync();
        await using var firstPool = (IAsyncDisposable)first;
        await using var secondPool = (IAsyncDisposable)second;
        var results = await Task.WhenAll(new OperationsReplayer(first).ReplayAsync(request), new OperationsReplayer(second).ReplayAsync(request));
        Assert.All(results, r => { Assert.Equal(ReplayStatus.Accepted, r.Status); Assert.Equal(1, r.Generation); });
        Assert.Single(results, r => r.RepeatedRequest);
        Assert.Equal(WorkState.Pending, (await db.Store.FindAsync(scope, failed.Work.Id))!.State);
        Assert.Equal(0, await db.EffectCount(failed.Work.Id));
        var receipt = (await second.FindReceiptAsync(scope, failed.Work.Id, request.RequestId, request.Actor))!;
        Assert.Equal(request.Reason, receipt.Reason);
        Assert.Equal(1, receipt.Result.Generation);
        Assert.Null(await second.FindReceiptAsync(scope, failed.Work.Id, request.RequestId, "operator-b"));
        Assert.Null(await second.FindReceiptAsync(scope, Guid.NewGuid(), request.RequestId, request.Actor));
        Assert.Null(await second.FindReceiptAsync(new(scope.Tenant + "other", scope.Consumer, scope.Source, scope.EventType), failed.Work.Id, request.RequestId, request.Actor));
        Assert.Equal(ReplayStatus.RequestConflict, (await first.ReplayAsync(new(scope, request.RequestId, request.WorkId, request.ExpectedVersion, request.Actor, "different reason"))).Status);
        await db.Store.ExecuteAsync((await db.Claim(scope))!, db.Effect, new());
        Assert.Equal(1, await db.EffectCount(failed.Work.Id));
        var repeat = await first.ReplayAsync(request);
        Assert.True(repeat.RepeatedRequest);
        Assert.Equal(1, repeat.Generation);
        Assert.Equal(WorkState.Completed, (await db.Store.FindAsync(scope, failed.Work.Id))!.State);
    }

    [Fact]
    public async Task Competing_requests_do_not_open_two_generations_and_completed_work_cannot_be_reopened()
    {
        var scope = StoreContractData.Scope();
        await db.Store.AcceptAsync(StoreContractData.Submission(scope));
        var failed = await db.Store.ExecuteAsync((await db.Claim(scope))!, (_, _, _, _) => throw new PermanentWorkException("failed"), new());
        var first = await OpenAsync(); var second = await OpenAsync();
        await using var firstPool = (IAsyncDisposable)first;
        await using var secondPool = (IAsyncDisposable)second;
        var results = await Task.WhenAll(first.ReplayAsync(new(scope, Guid.NewGuid(), failed.Work.Id, failed.Work.Version, "a", "fixed")),
            second.ReplayAsync(new(scope, Guid.NewGuid(), failed.Work.Id, failed.Work.Version, "b", "fixed")));
        Assert.Single(results, r => r.Status == ReplayStatus.Accepted);
        Assert.Equal(1, (await db.Store.FindAsync(scope, failed.Work.Id))!.ReplayGeneration);
        await db.Store.ExecuteAsync((await db.Claim(scope))!, db.Effect, new());
        var current = (await db.Store.FindAsync(scope, failed.Work.Id))!;
        Assert.Equal(ReplayStatus.NotFailed, (await first.ReplayAsync(new(scope, Guid.NewGuid(), current.Id, current.Version, "a", "try again"))).Status);
        Assert.Equal(1, await db.EffectCount(current.Id));
    }

    protected async Task<(WorkScope Scope, ReplayRequest Request)> FailedRequest()
    {
        var scope = StoreContractData.Scope();
        await db.Store.AcceptAsync(StoreContractData.Submission(scope));
        var failed = await db.Store.ExecuteAsync((await db.Claim(scope))!, (_, _, _, _) => throw new PermanentWorkException("failed"), new());
        return (scope, new(scope, Guid.NewGuid(), failed.Work.Id, failed.Work.Version, "operator", "fixed"));
    }

    protected async Task ResolveAfterLostReply(IWorkReplayStore interrupted, Task dropped, WorkScope scope, ReplayRequest request)
    {
        var failure = await Assert.ThrowsAsync<ReplayUnavailableException>(() => new OperationsReplayer(interrupted).ReplayAsync(request));
        Assert.True(failure.OutcomeUnconfirmed);
        Assert.Equal(request.RequestId, failure.RequestId);
        Assert.Null(failure.InnerException);
        await dropped.WaitAsync(TimeSpan.FromSeconds(5));
        var healthy = await OpenAsync();
        await using var cleanup = (IAsyncDisposable)healthy;
        Assert.Equal(ReplayStatus.Accepted, (await healthy.FindReceiptAsync(scope, request.WorkId, request.RequestId, request.Actor))!.Result.Status);
        await db.Store.ExecuteAsync((await db.Claim(scope))!, (_, _, _, _) => throw new PermanentWorkException("failed again"), new());
        var repeat = await healthy.ReplayAsync(request);
        Assert.True(repeat.RepeatedRequest);
        Assert.Equal(1, repeat.Generation);
        Assert.Equal(WorkState.DeadLetter, (await db.Store.FindAsync(scope, request.WorkId))!.State);
    }
}
