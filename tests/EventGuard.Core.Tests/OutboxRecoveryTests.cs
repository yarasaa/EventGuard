namespace EventGuard.Core.Tests;

public sealed class OutboxRecoveryTests
{
    private static readonly ProducerScope Scope = new("tenant", "producer");
    private static OutboxMetadata Item(OutboxState state = OutboxState.DeadLetter) => new(Guid.NewGuid(), Scope.Id, state,
        8, 4, 2, 1, false, "failed", state == OutboxState.Quarantined ? Guid.NewGuid() : null);
    private static OutboxReplayRequest Request(OutboxMetadata item, bool ack = false) =>
        new(Scope, Guid.NewGuid(), item.IntentId, item.Version, "operator", "configuration fixed", ack);

    [Fact]
    public void Replay_advances_generation_without_resetting_lifetime_or_uncertainty()
    {
        var before = Item() with { DeliveryUncertain = true };
        var decision = OutboxRecovery.Replay(Request(before, true), before);
        Assert.Equal(before with
        {
            State = OutboxState.Pending,
            Version = 9,
            GenerationAttempt = 0,
            ReplayGeneration = 2,
            LastReason = "outbox-replay-requested"
        }, decision.After);
        Assert.Equal(new OutboxReplayResult(OutboxReplayStatus.Accepted, before.IntentId, 2, 9), decision.Result);
    }

    [Theory]
    [InlineData(OutboxState.Pending, OutboxReplayStatus.NotFailed)]
    [InlineData(OutboxState.Publishing, OutboxReplayStatus.NotFailed)]
    [InlineData(OutboxState.RetryScheduled, OutboxReplayStatus.NotFailed)]
    [InlineData(OutboxState.Published, OutboxReplayStatus.NotFailed)]
    [InlineData(OutboxState.Quarantined, OutboxReplayStatus.Quarantined)]
    public void Non_failed_and_quarantined_intents_cannot_be_replayed(OutboxState state, OutboxReplayStatus expected)
    {
        var item = Item(state);
        var result = OutboxRecovery.Replay(Request(item), item);
        Assert.Equal(expected, result.Result.Status);
        Assert.Null(result.After);
    }

    [Fact]
    public void Scope_version_and_possible_delivery_are_checked_before_replay()
    {
        var item = Item();
        var request = Request(item);
        Assert.Equal(new OutboxReplayResult(OutboxReplayStatus.NotFound, item.IntentId, 0, 0),
            OutboxRecovery.Replay(request, item with { ScopeId = "foreign", Version = 999 }).Result);
        Assert.Equal(OutboxReplayStatus.VersionConflict, OutboxRecovery.Replay(request, item with { Version = 9 }).Result.Status);
        Assert.Equal(OutboxReplayStatus.DeliveryUncertain, OutboxRecovery.Replay(request, item with { DeliveryUncertain = true }).Result.Status);
    }

    [Fact]
    public void Changing_duplicate_acknowledgement_changes_the_replay_request_fingerprint()
    {
        var item = Item();
        var first = Request(item);
        var changed = new OutboxReplayRequest(Scope, first.RequestId, item.IntentId, item.Version, first.Actor, first.Reason, true);
        var result = OutboxRecovery.Replay(first, item).Result;
        Assert.Equal(result with { RepeatedRequest = true }, OutboxRecovery.RepeatReplay(first, first.Fingerprint, result));
        Assert.Equal(OutboxReplayStatus.RequestConflict, OutboxRecovery.RepeatReplay(changed, first.Fingerprint, result).Status);
    }

    [Theory]
    [InlineData(OutboxState.Pending, false)]
    [InlineData(OutboxState.RetryScheduled, false)]
    [InlineData(OutboxState.DeadLetter, false)]
    [InlineData(OutboxState.Publishing, true)]
    public void Quarantine_preserves_history_and_marks_interrupted_delivery_uncertain(OutboxState state, bool uncertain)
    {
        var before = Item(state);
        var id = Guid.NewGuid();
        Assert.Equal(before with
        {
            State = OutboxState.Quarantined,
            Version = 9,
            QuarantineId = id,
            LastReason = "outbox-envelope-invalid",
            DeliveryUncertain = uncertain
        },
            OutboxRecovery.Quarantine(before, id, "outbox-envelope-invalid"));
    }

    [Fact]
    public void Invalid_metadata_and_overflow_are_rejected()
    {
        var item = Item();
        Assert.Throws<OverflowException>(() => OutboxRecovery.Replay(Request(item with { Version = long.MaxValue }), item with { Version = long.MaxValue }));
        Assert.Throws<OverflowException>(() => OutboxRecovery.Replay(Request(item), item with { ReplayGeneration = int.MaxValue }));
        Assert.Throws<InvalidOperationException>(() => OutboxRecovery.Replay(Request(item), item with { GenerationAttempt = 5 }));
        Assert.Throws<InvalidOperationException>(() => OutboxRecovery.Quarantine(Item(OutboxState.Published), Guid.NewGuid(), "invalid"));
        Assert.Throws<InvalidOperationException>(() => OutboxRecovery.Quarantine(Item(OutboxState.Quarantined), Guid.NewGuid(), "invalid"));
    }
}
