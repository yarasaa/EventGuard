namespace EventGuard.Store.Conformance;

public abstract class IngressQuarantineContract<TSession>(IWorkStoreFixture<TSession> fixture)
{
    private IIngressQuarantineStore Store => (IIngressQuarantineStore)fixture.Store;
    private static IdentityPolicy Policy => new("v1", "sha256-bytes/v1");
    private static IngressSnapshot Raw(string position = "topic:0:1", byte value = 1) => new("broker-incarnation-1", position, "test/v1", [value], [2, 3]);
    private static IngressReleaseRequest Request(WorkScope scope, Guid id, Guid? requestId = null, long version = 0, string key = "file-1", byte[]? payload = null) =>
        new(requestId ?? Guid.NewGuid(), id, version, StoreContractData.Submission(scope, key, payload), "operator", "validated corrected input");

    [Fact]
    public async Task Concurrent_recording_keeps_one_immutable_snapshot_and_policy()
    {
        var scope = StoreContractData.Scope();
        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Store.QuarantineAsync(scope, Policy, Raw(), "invalid-model")));
        Assert.Single(results, r => r.Status == IngressQuarantineStatus.Recorded);
        Assert.Single(results.Select(r => r.Item.Id).Distinct());
        Assert.Equal(15, results.Count(r => r.Status == IngressQuarantineStatus.Duplicate));
        var first = results[0].Item;
        var conflict = await Store.QuarantineAsync(scope, Policy, Raw(value: 9), "invalid-model");
        Assert.Equal(IngressQuarantineStatus.Conflict, conflict.Status);
        var raw = await Store.ReadQuarantineDataAsync(scope, first.Id);
        Assert.Equal(new byte[] { 1 }, raw!.CopyBody()); Assert.Equal(new byte[] { 2, 3 }, raw.CopyMetadata());
        Assert.Equal(first.Fingerprint, raw.Fingerprint);
        await Assert.ThrowsAsync<PolicyMismatchException>(() => fixture.Store.AcceptAsync(StoreContractData.Submission(scope, version: "v2")));
        Assert.Null(await Store.FindQuarantineAsync(StoreContractData.Scope(), first.Id));
        Assert.Null(await Store.ReadQuarantineDataAsync(StoreContractData.Scope(), first.Id));
    }

    [Fact]
    public async Task Release_and_request_receipt_are_idempotent_across_store_restart()
    {
        var scope = StoreContractData.Scope();
        var record = await Store.QuarantineAsync(scope, Policy, Raw(), "invalid-model");
        var request = Request(scope, record.Item.Id);
        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Store.ReleaseAsync(request)));
        Assert.All(results, r => Assert.Equal(IngressReleaseStatus.Accepted, r.Status));
        Assert.Single(results, r => !r.RepeatedRequest);
        Assert.Single(results.Select(r => r.WorkId).Distinct());
        var restarted = (IIngressQuarantineStore)fixture.RestartStore();
        Assert.True((await restarted.ReleaseAsync(request)).RepeatedRequest);
        Assert.Equal(IngressReleaseStatus.RequestConflict, (await restarted.ReleaseAsync(Request(scope, record.Item.Id, request.RequestId))).Status);
        Assert.Equal(IngressReleaseStatus.AlreadyReleased, (await Store.ReleaseAsync(Request(scope, record.Item.Id))).Status);
        Assert.Equal(record.Item.Fingerprint, (await Store.ReadQuarantineDataAsync(scope, record.Item.Id))!.Fingerprint);
        var claim = await fixture.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10));
        Assert.Equal(results[0].WorkId, claim!.Work.Id);
        Assert.Equal(ExecutionStatus.Completed, (await fixture.Store.ExecuteAsync(claim, fixture.Effect, new())).Status);
        Assert.Equal(1, await fixture.EffectCount(claim.Work.Id));
        Assert.Null(await fixture.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Refused_release_is_a_durable_receipt_and_cannot_be_repurposed()
    {
        var scope = StoreContractData.Scope();
        var record = await Store.QuarantineAsync(scope, Policy, Raw(), "invalid-model");
        var stale = Request(scope, record.Item.Id, version: 1);
        Assert.Equal(IngressReleaseStatus.VersionConflict, (await Store.ReleaseAsync(stale)).Status);
        var accepted = await Store.ReleaseAsync(Request(scope, record.Item.Id));
        Assert.Equal(IngressReleaseStatus.Accepted, accepted.Status);
        var repeated = await Store.ReleaseAsync(stale);
        Assert.Equal(IngressReleaseStatus.VersionConflict, repeated.Status); Assert.True(repeated.RepeatedRequest);
        var unknown = Request(scope, Guid.NewGuid());
        Assert.Equal(IngressReleaseStatus.NotFound, (await Store.ReleaseAsync(unknown)).Status);
        Assert.True((await Store.ReleaseAsync(unknown)).RepeatedRequest);
    }

    [Fact]
    public async Task Existing_identity_conflict_is_not_overwritten_or_released()
    {
        var scope = StoreContractData.Scope();
        var original = StoreContractData.Submission(scope, payload: [7]);
        var accepted = await fixture.Store.AcceptAsync(original);
        var record = await Store.QuarantineAsync(scope, Policy, Raw(), "invalid-model");
        var request = Request(scope, record.Item.Id, payload: [8]);
        Assert.Equal(IngressReleaseStatus.WorkConflict, (await Store.ReleaseAsync(request)).Status);
        Assert.True((await Store.ReleaseAsync(request)).RepeatedRequest);
        Assert.Null((await Store.FindQuarantineAsync(scope, record.Item.Id))!.ReleasedWorkId);
        var claim = await fixture.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10));
        Assert.Equal(new byte[] { 7 }, claim!.Payload); Assert.Equal(accepted.Work.Id, claim.Work.Id);
        await fixture.Store.ExecuteAsync(claim, fixture.Effect, new());
        var duplicate = await Store.ReleaseAsync(new(Guid.NewGuid(), record.Item.Id, 0, original, "operator", "already accepted"));
        Assert.Equal(IngressReleaseStatus.Duplicate, duplicate.Status);
        Assert.Equal(WorkState.Completed, (await fixture.Store.FindAsync(scope, accepted.Work.Id))!.State);
        Assert.Null(await fixture.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Different_release_requests_racing_only_admit_one_replacement()
    {
        var scope = StoreContractData.Scope();
        var record = await Store.QuarantineAsync(scope, Policy, Raw(), "invalid-model");
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(i => Store.ReleaseAsync(Request(scope, record.Item.Id, key: "key-" + i))));
        Assert.Single(results, r => r.Status == IngressReleaseStatus.Accepted);
        Assert.Equal(7, results.Count(r => r.Status == IngressReleaseStatus.AlreadyReleased));
    }

    [Fact]
    public async Task Cancellation_does_not_release_and_null_body_is_preserved()
    {
        var scope = StoreContractData.Scope();
        var raw = new IngressSnapshot("broker", "position", "codec/v1", [], [4], isNullBody: true);
        var record = await Store.QuarantineAsync(scope, Policy, raw, "unmapped-tombstone");
        using var stop = new CancellationTokenSource(); stop.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Store.ReleaseAsync(Request(scope, record.Item.Id), stop.Token));
        Assert.Null((await Store.FindQuarantineAsync(scope, record.Item.Id))!.ReleasedWorkId);
        Assert.True((await Store.ReadQuarantineDataAsync(scope, record.Item.Id))!.IsNullBody);
        Assert.Null(await fixture.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
    }
}
