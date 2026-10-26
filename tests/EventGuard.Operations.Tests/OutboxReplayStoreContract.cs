using EventGuard.Store.Conformance;

namespace EventGuard.Operations.Tests;

public abstract class OutboxReplayStoreContract(IOutboxStoreFixture db)
{
    protected abstract Task<IWorkReplayStore> OpenAsync();
    protected abstract Task CorruptHeaders(ProducerScope scope, Guid id);
    private Task<OutboxClaim?> Claim(ProducerScope scope) => db.Outbox.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1));
    protected async Task<OutboxReplayRequest> FailedRequest(bool uncertain = false)
    {
        var scope = new ProducerScope(Guid.NewGuid().ToString("N"), "publisher");
        var message = new OutgoingMessage(scope, Guid.NewGuid(), Guid.NewGuid(), new("kafka", "events", "v1", "destination"), "original-payload"u8);
        await db.EnqueueAsync(message);
        var failed = await db.Outbox.SettleAsync((await Claim(scope))!,
            new(uncertain ? PublishStatus.Unknown : PublishStatus.NotSentPermanent, "sample-failure"), new(maximumAttempts: 1));
        return new(scope, Guid.NewGuid(), message.IntentId, failed.Record.Version, "operator-a", "Dependency repaired <script>", uncertain);
    }

    [Fact]
    public async Task Two_command_hosts_share_one_request_receipt_and_preserve_the_original_message()
    {
        var request = await FailedRequest(true);
        var a = await OpenAsync(); var b = await OpenAsync();
        await using var firstPool = (IAsyncDisposable)a; await using var secondPool = (IAsyncDisposable)b;
        var first = new OperationsReplayer(a); var second = new OperationsReplayer(b);
        var replies = await Task.WhenAll(first.ReplayOutboxAsync(request), second.ReplayOutboxAsync(request));
        Assert.All(replies, r => { Assert.Equal(OutboxReplayStatus.Accepted, r.Status); Assert.Equal(1, r.Generation); });
        Assert.Single(replies, r => r.RepeatedRequest);
        var receipt = (await second.FindOutboxReceiptAsync(request.Scope, request.IntentId, request.RequestId, request.Actor))!;
        Assert.True(receipt.AcknowledgePossibleDuplicate); Assert.Equal(request.Reason, receipt.Reason);
        Assert.Null(await second.FindOutboxReceiptAsync(request.Scope, request.IntentId, request.RequestId, "operator-A"));
        Assert.Null(await second.FindOutboxReceiptAsync(request.Scope, Guid.NewGuid(), request.RequestId, request.Actor));
        foreach (var other in new[] { new ProducerScope(request.Scope.Tenant + "other", request.Scope.Producer), new ProducerScope(request.Scope.Tenant, "other") })
            Assert.Null(await second.FindOutboxReceiptAsync(other, request.IntentId, request.RequestId, request.Actor));
        var pending = (await db.Outbox.FindAsync(request.Scope, request.IntentId))!;
        Assert.Equal(OutboxState.Pending, pending.State); Assert.True(pending.DeliveryUncertain);
        Assert.Equal("original-payload"u8.ToArray(), pending.Message.CopyPayload());
        Assert.Equal("destination", pending.Message.Route.Address);
        await db.Outbox.SettleAsync((await Claim(request.Scope))!, new(PublishStatus.Accepted, "sample-accepted"), new());
        var repeat = await second.ReplayOutboxAsync(request);
        Assert.True(repeat.RepeatedRequest); Assert.Equal(receipt.Result.Version, repeat.Version);
        Assert.Equal(OutboxState.Published, (await db.Recovery.FindMetadataAsync(request.Scope, request.IntentId))!.State);
        Assert.Equal(OutboxReplayStatus.NotFailed, (await first.ReplayOutboxAsync(new(request.Scope, Guid.NewGuid(), request.IntentId,
            repeat.Version, request.Actor, "new request", true))).Status);
    }

    [Fact]
    public async Task Missing_duplicate_acknowledgement_is_durable_and_changing_it_cannot_reuse_the_request()
    {
        var seed = await FailedRequest(true);
        var source = await OpenAsync(); await using var cleanup = (IAsyncDisposable)source; var commands = new OperationsReplayer(source);
        var request = new OutboxReplayRequest(seed.Scope, seed.RequestId, seed.IntentId, seed.ExpectedVersion, seed.Actor, seed.Reason);
        Assert.Equal(OutboxReplayStatus.DeliveryUncertain, (await commands.ReplayOutboxAsync(request)).Status);
        Assert.Equal(OutboxReplayStatus.RequestConflict, (await commands.ReplayOutboxAsync(seed)).Status);
        var receipt = (await commands.FindOutboxReceiptAsync(seed.Scope, seed.IntentId, seed.RequestId, seed.Actor))!;
        Assert.False(receipt.AcknowledgePossibleDuplicate); Assert.Equal(OutboxReplayStatus.DeliveryUncertain, receipt.Result.Status);
        var next = new OutboxReplayRequest(seed.Scope, Guid.NewGuid(), seed.IntentId, seed.ExpectedVersion, seed.Actor, seed.Reason, true);
        Assert.Equal(OutboxReplayStatus.Accepted, (await commands.ReplayOutboxAsync(next)).Status);
        var repeat = await commands.ReplayOutboxAsync(request);
        Assert.True(repeat.RepeatedRequest); Assert.Equal(OutboxReplayStatus.DeliveryUncertain, repeat.Status);
        Assert.Equal(1, (await db.Recovery.FindMetadataAsync(seed.Scope, seed.IntentId))!.ReplayGeneration);
    }

    [Fact]
    public async Task Competing_request_ids_open_one_generation_and_stale_versions_or_changed_content_do_not()
    {
        var request = await FailedRequest();
        var a = await OpenAsync(); var b = await OpenAsync();
        await using var firstPool = (IAsyncDisposable)a; await using var secondPool = (IAsyncDisposable)b;
        var first = new OperationsReplayer(a); var second = new OperationsReplayer(b);
        var stale = new OutboxReplayRequest(request.Scope, Guid.NewGuid(), request.IntentId, request.ExpectedVersion - 1, request.Actor, request.Reason);
        Assert.Equal(OutboxReplayStatus.VersionConflict, (await first.ReplayOutboxAsync(stale)).Status);
        var results = await Task.WhenAll(first.ReplayOutboxAsync(request), second.ReplayOutboxAsync(new(request.Scope, Guid.NewGuid(), request.IntentId,
            request.ExpectedVersion, request.Actor, request.Reason)));
        Assert.Single(results, r => r.Status == OutboxReplayStatus.Accepted);
        Assert.Equal(1, (await db.Recovery.FindMetadataAsync(request.Scope, request.IntentId))!.ReplayGeneration);
        foreach (var changed in new[] { new OutboxReplayRequest(request.Scope, request.RequestId, request.IntentId, request.ExpectedVersion, "other-actor", request.Reason),
            new OutboxReplayRequest(request.Scope, request.RequestId, request.IntentId, request.ExpectedVersion, request.Actor, "other-reason") })
            Assert.Equal(OutboxReplayStatus.RequestConflict, (await first.ReplayOutboxAsync(changed)).Status);
    }

    [Fact]
    public async Task Corrupted_dead_letter_is_atomically_quarantined_with_a_saved_rejection_and_cannot_be_released()
    {
        var request = await FailedRequest(); await CorruptHeaders(request.Scope, request.IntentId);
        var source = await OpenAsync(); await using var cleanup = (IAsyncDisposable)source; var commands = new OperationsReplayer(source);
        var result = await commands.ReplayOutboxAsync(request);
        Assert.Equal(OutboxReplayStatus.Quarantined, result.Status);
        var metadata = (await db.Recovery.FindMetadataAsync(request.Scope, request.IntentId))!;
        Assert.Equal(OutboxState.Quarantined, metadata.State); Assert.NotNull(metadata.QuarantineId); Assert.Equal(0, metadata.ReplayGeneration);
        Assert.Equal(metadata.Version, (await commands.FindOutboxReceiptAsync(request.Scope, request.IntentId, request.RequestId, request.Actor))!.Result.Version);
        Assert.True((await commands.ReplayOutboxAsync(request)).RepeatedRequest);
        Assert.Equal(OutboxReplayStatus.Quarantined, (await commands.ReplayOutboxAsync(new(request.Scope, Guid.NewGuid(), request.IntentId, metadata.Version, request.Actor, "retry", true))).Status);
        Assert.Null(await Claim(request.Scope));
    }

    [Fact]
    public async Task Missing_and_other_producer_records_are_not_reopened()
    {
        var request = await FailedRequest();
        var source = await OpenAsync(); await using var cleanup = (IAsyncDisposable)source; var commands = new OperationsReplayer(source);
        var wrong = new ProducerScope(request.Scope.Tenant, "other-producer");
        Assert.Equal(OutboxReplayStatus.NotFound, (await commands.ReplayOutboxAsync(new(wrong, Guid.NewGuid(), request.IntentId, request.ExpectedVersion, request.Actor, "retry"))).Status);
        Assert.Equal(OutboxState.DeadLetter, (await db.Recovery.FindMetadataAsync(request.Scope, request.IntentId))!.State);
    }

    protected async Task ResolveAfterLostReply(IWorkReplayStore interrupted, Task dropped, OutboxReplayRequest request)
    {
        var error = await Assert.ThrowsAsync<ReplayUnavailableException>(() => new OperationsReplayer(interrupted).ReplayOutboxAsync(request));
        Assert.True(error.OutcomeUnconfirmed); Assert.Equal(request.RequestId, error.RequestId); Assert.Null(error.InnerException);
        await dropped.WaitAsync(TimeSpan.FromSeconds(5));
        var healthy = await OpenAsync(); await using var cleanup = (IAsyncDisposable)healthy; var commands = new OperationsReplayer(healthy);
        Assert.Equal(OutboxReplayStatus.Accepted, (await commands.FindOutboxReceiptAsync(request.Scope, request.IntentId, request.RequestId, request.Actor))!.Result.Status);
        await db.Outbox.SettleAsync((await Claim(request.Scope))!, new(PublishStatus.NotSentPermanent, "sample-failed-again"), new());
        var repeat = await commands.ReplayOutboxAsync(request);
        Assert.True(repeat.RepeatedRequest); Assert.Equal(1, repeat.Generation);
        Assert.Equal(OutboxState.DeadLetter, (await db.Recovery.FindMetadataAsync(request.Scope, request.IntentId))!.State);
    }
}
