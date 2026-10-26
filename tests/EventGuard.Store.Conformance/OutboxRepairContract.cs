namespace EventGuard.Store.Conformance;

public abstract class OutboxRepairContract(IOutboxStoreFixture fixture)
{
    protected abstract Task CorruptAsync(OutgoingMessage message);
    protected abstract Task<string> SnapshotAsync(OutgoingMessage message);
    private IOutboxRepairStore Repair => (IOutboxRepairStore)fixture.Recovery;
    protected static OutgoingMessage Message() => new(new("tenant", Guid.NewGuid().ToString("N")), Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "files.created"), [1, 2]);
    private Task<OutboxClaim?> Claim(OutgoingMessage message) => fixture.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10));
    private async Task<OutboxMetadata> Quarantine(OutgoingMessage message, bool uncertain = false)
    {
        await fixture.EnqueueAsync(message);
        if (uncertain)
        {
            var claim = (await Claim(message))!;
            await fixture.Outbox.SettleAsync(claim, new(PublishStatus.Unknown, "lost-reply"), new(maximumAttempts: 1));
        }
        await CorruptAsync(message);
        if (uncertain)
        {
            var metadata = (await fixture.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!;
            Assert.Equal(OutboxReplayStatus.Quarantined, (await fixture.Recovery.ReplayAsync(new(message.Scope, Guid.NewGuid(), message.IntentId, metadata.Version, "operator", "retry", true))).Status);
        }
        else Assert.Null(await Claim(message));
        var result = (await fixture.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!;
        Assert.Equal(OutboxState.Quarantined, result.State);
        return result;
    }
    protected static OutboxRepairRequest Request(OutgoingMessage message, OutboxMetadata item, bool ack = false, Guid? id = null) =>
        new(id ?? Guid.NewGuid(), item.QuarantineId!.Value, message, item.Version, "operator", "restored from trusted original", ack);

    [Fact]
    public async Task Concurrent_repair_restores_original_bytes_once_and_keeps_the_raw_audit_record()
    {
        var message = Message(); var quarantine = await Quarantine(message); var raw = await SnapshotAsync(message);
        var request = Request(message, quarantine);
        var results = await Task.WhenAll(Enumerable.Range(0, 12).Select(_ => Repair.RepairAsync(request)));
        Assert.All(results, r => Assert.Equal(OutboxRepairStatus.Accepted, r.Status));
        Assert.Single(results, r => !r.RepeatedRequest); Assert.All(results, r => Assert.Equal(1, r.Generation));
        Assert.Equal(raw, await SnapshotAsync(message));
        var record = (await fixture.Outbox.FindAsync(message.Scope, message.IntentId))!;
        Assert.Equal(message.Fingerprint, record.Message.Fingerprint); Assert.Equal(message.CopyPayload(), record.Message.CopyPayload());
        Assert.Equal(message.IntentId, record.OriginRequestId);
        var claim = (await Claim(message))!;
        await fixture.Outbox.SettleAsync(claim, new(PublishStatus.Accepted, "confirmed"), new());
        var repeated = await ((IOutboxRepairStore)fixture.RestartRecovery()).RepairAsync(request);
        Assert.True(repeated.RepeatedRequest);
        Assert.Equal(OutboxState.Published, (await fixture.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!.State);
        Assert.Null(await Claim(message));
    }

    [Theory]
    [InlineData("payload")]
    [InlineData("route")]
    [InlineData("message-id")]
    public async Task Repair_cannot_repurpose_the_immutable_publication(string change)
    {
        var message = Message(); var quarantine = await Quarantine(message);
        var replacement = new OutgoingMessage(message.Scope, message.IntentId, change == "message-id" ? Guid.NewGuid() : message.MessageId,
            change == "route" ? new("test", "local", "v1", "different") : message.Route, change == "payload" ? new byte[] { 9 } : message.CopyPayload());
        var refused = Request(replacement, quarantine);
        Assert.Equal(OutboxRepairStatus.EnvelopeMismatch, (await Repair.RepairAsync(refused)).Status);
        Assert.True((await Repair.RepairAsync(refused)).RepeatedRequest);
        Assert.Equal(OutboxRepairStatus.RequestConflict, (await Repair.RepairAsync(Request(message, quarantine, id: refused.RequestId))).Status);
        Assert.Equal(OutboxState.Quarantined, (await fixture.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!.State);
        Assert.Equal(OutboxRepairStatus.Accepted, (await Repair.RepairAsync(Request(message, quarantine))).Status);
        Assert.Equal(OutboxRepairStatus.EnvelopeMismatch, (await Repair.RepairAsync(refused)).Status);
    }

    [Fact]
    public async Task Delivery_uncertainty_requires_durable_acknowledgement_and_is_never_cleared()
    {
        var message = Message(); var quarantine = await Quarantine(message, uncertain: true);
        var refused = Request(message, quarantine);
        Assert.Equal(OutboxRepairStatus.DeliveryUncertain, (await Repair.RepairAsync(refused)).Status);
        Assert.Equal(OutboxRepairStatus.RequestConflict, (await Repair.RepairAsync(Request(message, quarantine, true, refused.RequestId))).Status);
        Assert.Equal(OutboxRepairStatus.Accepted, (await Repair.RepairAsync(Request(message, quarantine, true))).Status);
        Assert.True((await fixture.Outbox.FindAsync(message.Scope, message.IntentId))!.DeliveryUncertain);
        Assert.True((await Repair.RepairAsync(refused)).RepeatedRequest);
    }

    [Fact]
    public async Task Stale_or_wrong_quarantine_cannot_be_released_and_other_tenants_cannot_resolve_it()
    {
        var message = Message(); var quarantine = await Quarantine(message);
        var stale = new OutboxRepairRequest(Guid.NewGuid(), quarantine.QuarantineId!.Value, message, quarantine.Version + 1, "operator", "stale");
        Assert.Equal(OutboxRepairStatus.VersionConflict, (await Repair.RepairAsync(stale)).Status);
        var foreign = new OutgoingMessage(new("other", message.Scope.Producer), message.IntentId, message.MessageId, message.Route, message.CopyPayload());
        Assert.Equal(OutboxRepairStatus.NotFound, (await Repair.RepairAsync(Request(foreign, quarantine))).Status);
        Assert.Equal(OutboxRepairStatus.Accepted, (await Repair.RepairAsync(Request(message, quarantine))).Status);
        Assert.Equal(OutboxRepairStatus.NotQuarantined, (await Repair.RepairAsync(Request(message, quarantine))).Status);
        Assert.Equal(OutboxRepairStatus.VersionConflict, (await Repair.RepairAsync(stale)).Status);
    }
}
