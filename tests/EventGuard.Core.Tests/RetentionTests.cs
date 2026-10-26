namespace EventGuard.Core.Tests;

public sealed class RetentionTests
{
    private static readonly WorkScope Scope = new("tenant", "consumer", "source", "event");
    private static readonly DateTimeOffset Now = new(2026, 9, 20, 0, 0, 0, TimeSpan.Zero);
    private static RetentionRequest Request() => new(Scope, Guid.NewGuid(), RetentionTarget.ConsumerPayload, Now.AddDays(-8), "operator", "retention");

    [Theory]
    [InlineData(WorkState.Pending)]
    [InlineData(WorkState.Processing)]
    [InlineData(WorkState.RetryScheduled)]
    public void Old_timestamp_alone_does_not_allow_removal_of_active_data(WorkState state)
        => Assert.False(Retention.CanPurge(Request(), new(Guid.NewGuid(), false, state, null, Now.AddYears(-1)), Now));

    [Fact]
    public void Removal_requires_a_terminal_time_and_honors_the_database_clock()
    {
        var request = Request();
        Assert.False(Retention.CanPurge(request, new(Guid.NewGuid(), false, WorkState.Completed, null, null), Now));
        Assert.False(Retention.CanPurge(request, new(Guid.NewGuid(), false, WorkState.Completed, null, Now.AddDays(-1)), Now));
        Assert.False(Retention.CanPurge(request, new(Guid.NewGuid(), true, WorkState.Completed, null, Now.AddDays(-9)), Now));
        Assert.True(Retention.CanPurge(request, new(Guid.NewGuid(), false, WorkState.Completed, null, request.Before), Now));
        Assert.Throws<ArgumentException>(() => Retention.ValidateWindow(request, Now.AddDays(-2)));
    }

    [Fact]
    public void Scope_type_and_batch_bounds_cannot_be_bypassed_by_an_arbitrary_target()
    {
        Assert.Throws<ArgumentException>(() => new RetentionRequest(Scope, Guid.NewGuid(), RetentionTarget.PublishedOutboxPayload, Now.AddDays(-8), "operator", "wrong scope"));
        Assert.Throws<ArgumentException>(() => new RetentionRequest(Scope, Guid.NewGuid(), RetentionTarget.ConsumerPayload, Now.AddDays(-8), "operator", "unbounded", 1001));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RetentionRequest(Scope, Guid.NewGuid(), RetentionTarget.ConsumerPayload, Now.AddDays(-8), "operator", "too short", minimumAge: TimeSpan.Zero));
    }

    [Fact]
    public void Raw_snapshot_is_immutable_and_distinguishes_null_from_empty_body()
    {
        byte[] body = [1]; byte[] metadata = [2]; var raw = new IngressSnapshot("broker", "position", "codec/v1", body, metadata);
        body[0] = 3; metadata[0] = 4; raw.CopyBody()[0] = 5; raw.CopyMetadata()[0] = 6;
        Assert.Equal(new byte[] { 1 }, raw.CopyBody()); Assert.Equal(new byte[] { 2 }, raw.CopyMetadata());
        var empty = new IngressSnapshot("broker", "position", "codec/v1", [], []);
        var tombstone = new IngressSnapshot("broker", "position", "codec/v1", [], [], true);
        Assert.Equal(empty.Identity(Scope), tombstone.Identity(Scope)); Assert.NotEqual(empty.Fingerprint, tombstone.Fingerprint);
        Assert.Throws<ArgumentException>(() => new IngressSnapshot("broker", "position", "codec/v1", new byte[1048577], []));
        Assert.Throws<ArgumentException>(() => new IngressSnapshot("broker", "position", "codec/v1", [], new byte[65537]));
    }
}
