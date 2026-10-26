namespace EventGuard.Core.Tests;

public sealed class ExternalResultTests
{
    [Fact]
    public void Results_freeze_content_and_include_version_and_response_in_their_fingerprint()
    {
        var action = new ExternalAction(new("tenant", "app"), Guid.NewGuid(), "service", "v1", [7]);
        byte[] data = [1];
        var response = new ExternalResult(ExternalResultKind.Accepted, "ok", data: data);
        ExternalActionUpdate Snapshot(long version, ExternalResult? result) => new(action, version, ExternalActionState.Accepted,
            ExternalObservationState.Pending, ExternalPhase.Submit, "remote", "ok", [1], result);
        var snapshot = new ExternalActionUpdate(action, 2, ExternalActionState.Accepted, ExternalObservationState.Pending,
            ExternalPhase.Submit, "remote", "ok", data, response);
        data[0] = 9; snapshot.CopyStateData()[0] = 8; snapshot.Response!.CopyData()[0] = 3;
        Assert.Equal(new byte[] { 1 }, snapshot.CopyStateData()); Assert.Equal(new byte[] { 1 }, snapshot.Response.CopyData());
        Assert.Equal(snapshot.Fingerprint, Snapshot(2, response).Fingerprint);
        Assert.NotEqual(snapshot.Fingerprint, Snapshot(3, response).Fingerprint);
        Assert.NotEqual(snapshot.Fingerprint, Snapshot(2, null).Fingerprint);
        Assert.Throws<ArgumentException>(() => Snapshot(0, response));
        Assert.Throws<ArgumentException>(() => new ExternalActionUpdate(action, 2, ExternalActionState.Accepted,
            ExternalObservationState.Pending, ExternalPhase.Submit, null, null, new byte[64 * 1024 + 1], null));
    }

    [Fact]
    public void Target_separates_tenants_producers_and_handler_versions()
    {
        var target = new ExternalResultTarget(new("tenant", "app"), "status", "v1");
        Assert.Equal(target.Id, new ExternalResultTarget(new("tenant", "app"), "status", "v1").Id);
        Assert.NotEqual(target.WorkScope.Id, new ExternalResultTarget(new("other", "app"), "status", "v1").WorkScope.Id);
        Assert.NotEqual(target.WorkScope.Id, new ExternalResultTarget(new("tenant", "other"), "status", "v1").WorkScope.Id);
        Assert.NotEqual(target.WorkScope.Id, new ExternalResultTarget(target.Scope, "status", "v2").WorkScope.Id);
        Assert.Throws<ArgumentException>(() => new ExternalResultTarget(target.Scope, "", "v1"));
    }

    [Fact]
    public void An_old_custom_writer_cannot_silently_discard_a_result_target()
    {
        IExternalActionWriter writer = new LegacyWriter();
        var action = new ExternalAction(new("tenant", "app"), Guid.NewGuid(), "service", "v1", [1]);
        Assert.Throws<NotSupportedException>(() => { _ = writer.EnqueueAsync(action, new ExternalResultTarget(action.Scope, "status", "v1")); });
    }
    private sealed class LegacyWriter : IExternalActionWriter
    {
        public Task<OutboxEnqueueStatus> EnqueueAsync(ExternalAction action, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Must not fall back to ignoring the result target.");
    }
}
