namespace EventGuard.Core.Tests;

public sealed class OutboxTests
{
    private static readonly ProducerScope Scope = new("tenant", "files");
    private static readonly DateTimeOffset Now = new(2026, 9, 14, 0, 0, 0, TimeSpan.Zero);
    private static readonly PublishRoute Route = new("test", "local", "v1", "files.created");

    [Fact]
    public void Envelope_snapshots_caller_payload_and_headers()
    {
        byte[] payload = [1, 2];
        var headers = new Dictionary<string, string> { ["correlation"] = "original" };
        var message = new OutgoingMessage(Scope, Guid.NewGuid(), Guid.NewGuid(), Route, payload, headers: headers);
        payload[0] = 9;
        headers["correlation"] = "changed";
        message.CopyPayload()[0] = 8;
        Assert.Equal(new byte[] { 1, 2 }, message.CopyPayload());
        Assert.Equal("original", message.Headers["correlation"]);
        Assert.Throws<NotSupportedException>(() => ((IDictionary<string, string>)message.Headers).Add("extra", "value"));
    }

    [Fact]
    public void Header_order_and_timezone_do_not_change_canonical_fingerprint()
    {
        var id = Guid.NewGuid();
        var messageId = Guid.NewGuid();
        var one = new OutgoingMessage(Scope, id, messageId, Route, [1],
            headers: new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" }, notBefore: Now.AddTicks(1));
        var two = new OutgoingMessage(Scope, id, messageId, Route, [1],
            headers: new Dictionary<string, string> { ["b"] = "2", ["a"] = "1" }, notBefore: Now.ToOffset(TimeSpan.FromHours(3)));
        Assert.Equal(one.Fingerprint, two.Fingerprint);
        Assert.Equal(Now, one.NotBefore);
    }

    [Fact]
    public void Identity_destination_version_and_content_are_part_of_the_immutable_envelope()
    {
        var id = Guid.NewGuid();
        var messageId = Guid.NewGuid();
        var original = new OutgoingMessage(Scope, id, messageId, Route, [1]);
        OutgoingMessage[] changed = [
            new(Scope, Guid.NewGuid(), messageId, Route, [1]),
            new(Scope, id, Guid.NewGuid(), Route, [1]),
            new(new("other", Scope.Producer), id, messageId, Route, [1]),
            new(Scope, id, messageId, new("test", "local", "v2", Route.Address), [1]),
            new(Scope, id, messageId, new("test", "local", "v1", "different"), [1]),
            new(Scope, id, messageId, new("test", "local", "v1", Route.Address, ""), [1]),
            new(Scope, id, messageId, Route, [2]),
            new(Scope, id, messageId, Route, [1], contentType: "application/octet-stream"),
            new(Scope, id, messageId, Route, [1], headers: new Dictionary<string, string> { ["x"] = "1" }),
            new(Scope, id, messageId, Route, [1], notBefore: Now)
        ];
        Assert.All(changed, message => Assert.NotEqual(original.Fingerprint, message.Fingerprint));
        Assert.NotEqual(new ProducerScope("ab", "c").Id, new ProducerScope("a", "bc").Id);
    }

    [Fact]
    public void Envelope_limits_and_stable_ids_are_required_before_storage()
    {
        Assert.Throws<ArgumentException>(() => new OutgoingMessage(Scope, Guid.Empty, Guid.NewGuid(), Route, [1]));
        Assert.Throws<ArgumentException>(() => new OutgoingMessage(Scope, Guid.NewGuid(), Guid.Empty, Route, [1]));
        Assert.Throws<ArgumentException>(() => new OutgoingMessage(Scope, Guid.NewGuid(), Guid.NewGuid(), Route, new byte[1024 * 1024 + 1]));
        Assert.Throws<ArgumentException>(() => new OutgoingMessage(Scope, Guid.NewGuid(), Guid.NewGuid(), Route, [1],
            headers: Enumerable.Range(0, 33).ToDictionary(i => i.ToString(), _ => "v")));
        Assert.Throws<ArgumentException>(() => new PublishOutcome(PublishStatus.Unknown, "exception with secret"));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PublishOutcome((PublishStatus)999, "invalid"));
    }

    [Fact]
    public void Producer_receipt_distinguishes_repeat_from_changed_business_intent()
    {
        var request = new ProducerRequest(Scope, Guid.NewGuid(), ContentFingerprint.FromBytes([1]));
        Assert.Equal(ProducerStatus.AlreadyCommitted, OutboxLifecycle.ExistingProducer(request, request.Fingerprint.Value));
        Assert.Equal(ProducerStatus.RequestConflict, OutboxLifecycle.ExistingProducer(request, ContentFingerprint.FromBytes([2]).Value));
    }

    [Theory]
    [InlineData(OutboxState.Published)]
    [InlineData(OutboxState.DeadLetter)]
    public void Terminal_intents_are_not_automatically_published_again(OutboxState state) =>
        Assert.Null(OutboxLifecycle.Claim(Record() with { State = state }, null, Now));

    [Fact]
    public void Scheduling_and_lease_recovery_use_exact_store_clock_boundaries()
    {
        var pending = Record() with { NextAttemptAt = Now.AddSeconds(1) };
        Assert.Null(OutboxLifecycle.Claim(pending, null, Now));
        var claimed = OutboxLifecycle.Claim(pending, null, Now.AddSeconds(1))!;
        var lease = new WorkLease(Guid.NewGuid(), Now.AddSeconds(2));
        Assert.False(claimed.DeliveryUncertain);
        Assert.Null(OutboxLifecycle.Claim(claimed, lease, Now.AddSeconds(1)));
        var recovered = OutboxLifecycle.Claim(claimed, lease, Now.AddSeconds(2))!;
        Assert.True(recovered.DeliveryUncertain);
        Assert.Equal(2, recovered.AttemptCount);
        Assert.Equal("publish-interrupted", recovered.LastReason);
        Assert.Same(pending.Message, recovered.Message);
    }

    [Theory]
    [InlineData(PublishStatus.Accepted, OutboxState.Published, false)]
    [InlineData(PublishStatus.NotSentTransient, OutboxState.RetryScheduled, false)]
    [InlineData(PublishStatus.NotSentPermanent, OutboxState.DeadLetter, false)]
    [InlineData(PublishStatus.Unknown, OutboxState.RetryScheduled, true)]
    public void Publication_outcomes_have_different_retry_and_uncertainty_meanings(PublishStatus status, OutboxState state, bool uncertain)
    {
        var claimed = OutboxLifecycle.Claim(Record(), null, Now)!;
        var settled = OutboxLifecycle.Settle(claimed, new(status, "test-result"), new(jitterRatio: 0), Now, 0.5);
        Assert.Equal(state, settled.State);
        Assert.Equal(uncertain, settled.DeliveryUncertain);
        Assert.Equal(claimed.Version + 1, settled.Version);
        Assert.Same(claimed.Message, settled.Message);
    }

    [Theory]
    [InlineData(PublishStatus.Accepted, OutboxState.Published)]
    [InlineData(PublishStatus.NotSentPermanent, OutboxState.DeadLetter)]
    [InlineData(PublishStatus.Unknown, OutboxState.DeadLetter)]
    public void Later_outcomes_cannot_erase_possible_earlier_delivery(PublishStatus status, OutboxState state)
    {
        var claimed = OutboxLifecycle.Claim(Record(), null, Now)! with { DeliveryUncertain = true };
        var result = OutboxLifecycle.Settle(claimed, new(status, "result"), new(maximumAttempts: 1), Now, 0.5);
        Assert.Equal(state, result.State);
        Assert.True(result.DeliveryUncertain);
    }

    [Fact]
    public void Corrupt_ownership_counters_and_overflows_do_not_become_publishable_work()
    {
        Assert.Throws<InvalidOperationException>(() => OutboxLifecycle.Claim(Record() with { State = (OutboxState)999 }, null, Now));
        Assert.Throws<InvalidOperationException>(() => OutboxLifecycle.Claim(Record() with { State = OutboxState.Publishing }, null, Now));
        Assert.Throws<InvalidOperationException>(() => OutboxLifecycle.Claim(Record(), new(Guid.NewGuid(), Now), Now));
        Assert.Throws<InvalidOperationException>(() => OutboxLifecycle.Claim(Record() with { Version = -1 }, null, Now));
        Assert.Throws<OverflowException>(() => OutboxLifecycle.Claim(Record() with { Version = long.MaxValue }, null, Now));
        Assert.Throws<OverflowException>(() => OutboxLifecycle.Claim(Record() with { AttemptCount = int.MaxValue }, null, Now));
        Assert.Throws<InvalidOperationException>(() => OutboxLifecycle.Settle(Record() with { State = OutboxState.Publishing },
            new(PublishStatus.Accepted, "accepted"), new(), Now, 0.5));
    }

    private static OutboxRecord Record() => new(new(Scope, Guid.NewGuid(), Guid.NewGuid(), Route, [1]),
        OutboxState.Pending, 0, 0, Now, false, null, null, null);
}
