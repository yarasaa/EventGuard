using EventGuard.Store.Conformance;
namespace EventGuard.Operations.Tests;

public abstract class RecoveryStoreContract<TSession>(IWorkStoreFixture<TSession> db, IOutboxStoreFixture outbox)
{
    protected abstract Task<IWorkReplayStore> OpenCommandsAsync();
    protected abstract Task<IWorkReadStore> OpenReadsAsync();
    protected abstract Task CorruptAsync(OutgoingMessage message);
    private IIngressQuarantineStore Ingress => (IIngressQuarantineStore)db.Store;
    protected async Task<IngressReleaseRequest> ReleaseRequest()
    {
        var scope = StoreContractData.Scope();
        var held = await Ingress.QuarantineAsync(scope, new("v1", "sha256-bytes/v1"), new("broker", "position", "test/v1", [1], [2]), "unsupported-contract-version");
        return new(Guid.NewGuid(), held.Item.Id, held.Item.Version, StoreContractData.Submission(scope), "operator-a", "corrected <script>");
    }
    protected async Task<OutboxRepairRequest> RepairRequest()
    {
        var scope = new ProducerScope(Guid.NewGuid().ToString("N"), "publisher");
        var message = new OutgoingMessage(scope, Guid.NewGuid(), Guid.NewGuid(), new("kafka", "events", "v1", "hidden-address"), [1]);
        await outbox.EnqueueAsync(message); await CorruptAsync(message);
        Assert.Null(await outbox.Outbox.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1)));
        var metadata = (await outbox.Recovery.FindMetadataAsync(scope, message.IntentId))!;
        return new(Guid.NewGuid(), metadata.QuarantineId!.Value, message, metadata.Version, "operator-a", "restored", true);
    }
    [Fact]
    public async Task Concurrent_operations_release_shares_a_receipt_and_scopes_actor_lookup()
    {
        var request = await ReleaseRequest(); var a = await OpenCommandsAsync(); var b = await OpenCommandsAsync();
        await using var disposeA = (IAsyncDisposable)a; await using var disposeB = (IAsyncDisposable)b;
        var first = new OperationsReplayer(a); var second = new OperationsReplayer(b);
        var replies = await Task.WhenAll(first.ReleaseIngressAsync(request), second.ReleaseIngressAsync(request));
        Assert.All(replies, result => Assert.Equal(IngressReleaseStatus.Accepted, result.Status)); Assert.Single(replies, result => result.RepeatedRequest);
        var scope = request.Submission.Identity.Scope;
        var receipt = (await second.FindIngressReleaseReceiptAsync(scope, request.QuarantineId, request.RequestId, request.Actor))!;
        Assert.Equal(request.Reason, receipt.Reason);
        Assert.Null(await second.FindIngressReleaseReceiptAsync(scope, request.QuarantineId, request.RequestId, "other"));
        Assert.Null(await second.FindIngressReleaseReceiptAsync(StoreContractData.Scope(), request.QuarantineId, request.RequestId, request.Actor));
        Assert.Null(await second.FindIngressReleaseReceiptAsync(scope, Guid.NewGuid(), request.RequestId, request.Actor));
        Assert.Equal(0, await db.EffectCount(receipt.Result.WorkId!.Value));
        await db.Store.ExecuteAsync((await db.Claim(scope))!, db.Effect, new());
        Assert.True((await second.ReleaseIngressAsync(request)).RepeatedRequest);
        Assert.Equal(1, await db.EffectCount(receipt.Result.WorkId!.Value));
    }
    [Fact]
    public async Task Operations_repair_preserves_original_envelope_and_is_actor_scoped()
    {
        var request = await RepairRequest(); var store = await OpenCommandsAsync(); await using var cleanup = (IAsyncDisposable)store;
        var commands = new OperationsReplayer(store);
        var changed = new OutgoingMessage(request.Message.Scope, request.Message.IntentId, request.Message.MessageId, request.Message.Route, [9]);
        var refusal = new OutboxRepairRequest(Guid.NewGuid(), request.QuarantineId, changed, request.ExpectedVersion, request.Actor, "changed", true);
        Assert.Equal(OutboxRepairStatus.EnvelopeMismatch, (await commands.RepairOutboxAsync(refusal)).Status);
        Assert.Equal(OutboxRepairStatus.Accepted, (await commands.RepairOutboxAsync(request)).Status);
        Assert.True((await commands.RepairOutboxAsync(request)).RepeatedRequest);
        var receipt = (await commands.FindOutboxRepairReceiptAsync(request.Message.Scope, request.Message.IntentId, request.RequestId, request.Actor))!;
        Assert.True(receipt.AcknowledgePossibleDuplicate);
        Assert.Equal(request.Reason, receipt.Reason);
        Assert.Null(await commands.FindOutboxRepairReceiptAsync(request.Message.Scope, request.Message.IntentId, request.RequestId, "other"));
        Assert.Null(await commands.FindOutboxRepairReceiptAsync(new("other", "publisher"), request.Message.IntentId, request.RequestId, request.Actor));
        Assert.Equal(request.Message.CopyPayload(), (await outbox.Outbox.FindAsync(request.Message.Scope, request.Message.IntentId))!.Message.CopyPayload());
    }
    [Fact]
    public async Task Ingress_metadata_filters_and_reverse_seek_are_bounded_and_do_not_expose_raw_input()
    {
        var request = await ReleaseRequest(); var scope = request.Submission.Identity.Scope;
        for (var i = 0; i < 29; i++) await Ingress.QuarantineAsync(scope, new("v1", "sha256-bytes/v1"), new("secret-binding", "pos-" + i, "test/v1", "secret-body"u8, "secret-metadata"u8), "private-code");
        await Ingress.ReleaseAsync(request);
        var store = await OpenReadsAsync(); await using var cleanup = (IAsyncDisposable)store;
        var reads = new OperationsReader(store); var cutoff = DateTimeOffset.UtcNow.AddDays(1);
        var first = (await reads.ListIngressAsync(new(scope, cutoff, false, 10))).Value; Assert.Equal(10, first.Items.Count);
        var second = (await reads.ListIngressAsync(new(scope, cutoff, false, 10, before: first.Next))).Value;
        var back = (await reads.ListIngressAsync(new(scope, cutoff, false, 10, after: second.Previous))).Value;
        Assert.Equal(first.Items, back.Items); Assert.Empty(first.Items.Select(i => i.Id).Intersect(second.Items.Select(i => i.Id)));
        var released = Assert.Single((await reads.ListIngressAsync(new(scope, cutoff, true))).Value.Items); Assert.Equal(request.QuarantineId, released.Id); Assert.Equal("unsupported-contract-version", released.ReasonCode);
        Assert.Null((await reads.FindIngressAsync(StoreContractData.Scope(), released.Id)).Value);
        Assert.Single((await reads.ListIngressAsync(new(scope, cutoff, id: released.Id))).Value.Items);
        var json = System.Text.Json.JsonSerializer.Serialize(first);
        Assert.DoesNotContain("secret-", json); Assert.DoesNotContain("private-code", json);
        Assert.All(first.Items, item => Assert.Equal("mapping-rejected", item.ReasonCode));
    }
    protected async Task ResolveLostRelease(IWorkReplayStore interrupted, Task dropped, IngressReleaseRequest request)
    {
        var error = await Assert.ThrowsAsync<ReplayUnavailableException>(() => new OperationsReplayer(interrupted).ReleaseIngressAsync(request));
        Assert.True(error.OutcomeUnconfirmed); Assert.Equal(request.RequestId, error.RequestId); Assert.Null(error.InnerException);
        await dropped.WaitAsync(TimeSpan.FromSeconds(5));
        var healthy = await OpenCommandsAsync(); await using var cleanup = (IAsyncDisposable)healthy;
        var commands = new OperationsReplayer(healthy);
        Assert.NotNull(await commands.FindIngressReleaseReceiptAsync(request.Submission.Identity.Scope, request.QuarantineId, request.RequestId, request.Actor));
        Assert.True((await commands.ReleaseIngressAsync(request)).RepeatedRequest);
    }
    protected async Task ResolveLostRepair(IWorkReplayStore interrupted, Task dropped, OutboxRepairRequest request)
    {
        var error = await Assert.ThrowsAsync<ReplayUnavailableException>(() => new OperationsReplayer(interrupted).RepairOutboxAsync(request));
        Assert.True(error.OutcomeUnconfirmed); Assert.Equal(request.RequestId, error.RequestId); Assert.Null(error.InnerException);
        await dropped.WaitAsync(TimeSpan.FromSeconds(5));
        var healthy = await OpenCommandsAsync(); await using var cleanup = (IAsyncDisposable)healthy;
        var commands = new OperationsReplayer(healthy);
        Assert.NotNull(await commands.FindOutboxRepairReceiptAsync(request.Message.Scope, request.Message.IntentId, request.RequestId, request.Actor));
        Assert.True((await commands.RepairOutboxAsync(request)).RepeatedRequest);
    }
}
