namespace EventGuard.Store.Conformance;

// Runs against each real provider. SQL corruption and network faults belong in native suites.
public abstract class OutboxStoreContract(IOutboxStoreFixture db)
{
    private static readonly RetryPolicy Retry = new(2, TimeSpan.FromMilliseconds(1), jitterRatio: 0);

    [Fact]
    public async Task Replay_preserves_the_envelope_and_lifetime_history_but_renews_attempt_budget()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        var failed = await Fail(message);
        var replay = await db.Recovery.ReplayAsync(Request(message, failed));
        Assert.Equal(OutboxReplayStatus.Accepted, replay.Status);
        var pending = (await db.Outbox.FindAsync(message.Scope, message.IntentId))!;
        Assert.Equal(message.Fingerprint, pending.Message.Fingerprint);
        Assert.Equal(1, pending.AttemptCount);
        Assert.Equal(0, pending.GenerationAttempt);
        Assert.Equal(1, pending.ReplayGeneration);
        var claim = (await Claim(message))!;
        var retry = await db.Outbox.SettleAsync(claim, new(PublishStatus.NotSentTransient, "temporary"), Retry);
        Assert.Equal(OutboxState.RetryScheduled, retry.Record.State);
        Assert.Equal(2, retry.Record.AttemptCount);
        Assert.Equal(1, retry.Record.GenerationAttempt);
        await Task.Delay(15);
        var second = (await Claim(message))!;
        var exhausted = await db.Outbox.SettleAsync(second, new(PublishStatus.NotSentTransient, "temporary"), Retry);
        Assert.Equal(OutboxState.DeadLetter, exhausted.Record.State);
        Assert.Equal(3, exhausted.Record.AttemptCount);
        Assert.Equal(2, exhausted.Record.GenerationAttempt);
    }

    [Fact]
    public async Task Concurrent_repeat_returns_one_durable_result_even_after_the_intent_fails_again()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        var request = Request(message, await Fail(message));
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => db.Recovery.ReplayAsync(request)));
        Assert.All(results, r => Assert.Equal(OutboxReplayStatus.Accepted, r.Status));
        Assert.Single(results, r => !r.RepeatedRequest);
        await Fail(message);
        var repeated = await db.RestartRecovery().ReplayAsync(request);
        Assert.True(repeated.RepeatedRequest);
        Assert.Equal(1, repeated.Generation);
        Assert.Equal(OutboxState.DeadLetter, (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!.State);
    }

    [Fact]
    public async Task Competing_new_requests_can_advance_one_expected_version_only_once()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        var failed = await Fail(message);
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => db.Recovery.ReplayAsync(Request(message, failed))));
        Assert.Single(results, r => r.Status == OutboxReplayStatus.Accepted);
        Assert.Equal(1, (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!.ReplayGeneration);
    }

    [Fact]
    public async Task Rejected_request_remains_rejected_after_later_failure()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        var request = new OutboxReplayRequest(message.Scope, Guid.NewGuid(), message.IntentId, 0, "operator", "too early");
        var rejected = await db.Recovery.ReplayAsync(request);
        Assert.Equal(OutboxReplayStatus.NotFailed, rejected.Status);
        await Fail(message);
        Assert.Equal(rejected with { RepeatedRequest = true }, await db.RestartRecovery().ReplayAsync(request));
        Assert.Equal(OutboxReplayStatus.RequestConflict, (await db.Recovery.ReplayAsync(new(message.Scope, request.RequestId,
            message.IntentId, 0, "operator", "changed reason"))).Status);
    }

    [Fact]
    public async Task Possible_delivery_requires_a_new_acknowledged_request_and_remains_visible_after_success()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        var failed = await Fail(message, PublishStatus.Unknown);
        var denied = Request(message, failed);
        Assert.Equal(OutboxReplayStatus.DeliveryUncertain, (await db.Recovery.ReplayAsync(denied)).Status);
        Assert.Equal(OutboxReplayStatus.RequestConflict, (await db.Recovery.ReplayAsync(new(message.Scope, denied.RequestId,
            message.IntentId, failed.Version, denied.Actor, denied.Reason, true))).Status);
        var accepted = await db.Recovery.ReplayAsync(Request(message, failed, true));
        Assert.Equal(OutboxReplayStatus.Accepted, accepted.Status);
        var claim = (await Claim(message))!;
        var done = await db.Outbox.SettleAsync(claim, new(PublishStatus.Accepted, "accepted"), Retry);
        Assert.True(done.Record.DeliveryUncertain);
        Assert.Equal(OutboxReplayStatus.NotFailed, (await db.Recovery.ReplayAsync(Request(message, done.Record, true))).Status);
    }

    [Fact]
    public async Task Foreign_scope_cannot_inspect_or_replay_an_intent()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        var failed = await Fail(message);
        var foreign = new ProducerScope("foreign", message.Scope.Producer);
        Assert.Null(await db.Recovery.FindMetadataAsync(foreign, message.IntentId));
        Assert.Equal(new OutboxReplayResult(OutboxReplayStatus.NotFound, message.IntentId, 0, 0),
            await db.Recovery.ReplayAsync(new(foreign, Guid.NewGuid(), message.IntentId, failed.Version, "operator", "retry")));
        Assert.Equal(OutboxState.DeadLetter, (await db.Outbox.FindAsync(message.Scope, message.IntentId))!.State);
    }

    [Fact]
    public async Task A_not_found_receipt_stays_not_found_after_the_intent_is_created()
    {
        var message = Message();
        var request = new OutboxReplayRequest(message.Scope, Guid.NewGuid(), message.IntentId, 0, "operator", "missing");
        var result = await db.Recovery.ReplayAsync(request);
        Assert.Equal(OutboxReplayStatus.NotFound, result.Status);
        await db.EnqueueAsync(message);
        await Fail(message);
        Assert.Equal(result with { RepeatedRequest = true }, await db.Recovery.ReplayAsync(request));
    }

    [Fact]
    public async Task An_old_claim_request_cannot_reclaim_a_replayed_intent()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        var first = (await Claim(message))!;
        var failed = await db.Outbox.SettleAsync(first, new(PublishStatus.NotSentPermanent, "permanent"), Retry);
        await db.Recovery.ReplayAsync(Request(message, failed.Record));
        Assert.Null(await db.Outbox.ClaimNextAsync(message.Scope, first.Token, TimeSpan.FromSeconds(10)));
        Assert.Equal(1, (await Claim(message))!.Record.ReplayGeneration);
    }

    private async Task<OutboxRecord> Fail(OutgoingMessage message, PublishStatus status = PublishStatus.NotSentPermanent) =>
        (await db.Outbox.SettleAsync((await Claim(message))!, new(status, "failed"), new(maximumAttempts: 1))).Record;
    private Task<OutboxClaim?> Claim(OutgoingMessage message) => db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10));
    private static OutboxReplayRequest Request(OutgoingMessage message, OutboxRecord record, bool ack = false) =>
        new(message.Scope, Guid.NewGuid(), message.IntentId, record.Version, "operator", "configuration fixed", ack);
    private static OutgoingMessage Message() => new(new("tenant", Guid.NewGuid().ToString("N")), Guid.NewGuid(), Guid.NewGuid(),
        new("test", "local", "v1", "files.created"), [1, 2], headers: new Dictionary<string, string> { ["correlation"] = "original" });
}
