namespace EventGuard.Core.Tests;
public sealed class BulkReplayTests
{
    [Fact]
    public void Plan_freezes_members_and_derives_stable_distinct_request_ids()
    {
        var scope = new WorkScope("t", "c", "s", "e"); var id = Guid.NewGuid();
        var items = new[] { new BulkReplayItem(Guid.NewGuid(), 1), new BulkReplayItem(Guid.NewGuid(), 2) };
        var request = new BulkReplayRequest(scope, id, items, "actor", "reason"); var first = request.Items[0];
        items[0] = new(Guid.NewGuid(), 3); Assert.Equal(first, request.Items[0]);
        var repeated = new BulkReplayRequest(scope, id, request.Items, "actor", "reason");
        Assert.Equal(request.Fingerprint, repeated.Fingerprint); Assert.Equal(request.ItemRequestId(0), repeated.ItemRequestId(0));
        Assert.NotEqual(request.ItemRequestId(0), request.ItemRequestId(1));
        Assert.NotEqual(request.Fingerprint, new BulkReplayRequest(scope, id, request.Items.Reverse(), "actor", "reason").Fingerprint);
        Assert.NotEqual(request.Fingerprint, new BulkReplayRequest(scope, id, request.Items, "other", "reason").Fingerprint);
    }
    [Fact]
    public void Empty_duplicate_oversize_and_invalid_members_are_rejected()
    {
        var scope = new ProducerScope("t", "p"); var item = new BulkReplayItem(Guid.NewGuid(), 0);
        Assert.Throws<ArgumentException>(() => new BulkReplayRequest(scope, Guid.NewGuid(), [], "a", "r"));
        Assert.Throws<ArgumentException>(() => new BulkReplayRequest(scope, Guid.NewGuid(), [item, item], "a", "r"));
        Assert.Throws<ArgumentException>(() => new BulkReplayRequest(scope, Guid.NewGuid(), Enumerable.Range(0, 101).Select(_ => new BulkReplayItem(Guid.NewGuid(), 0)), "a", "r"));
        Assert.Throws<ArgumentException>(() => new BulkReplayRequest(scope, Guid.NewGuid(), [item with { ExpectedVersion = -1 }], "a", "r"));
    }
}
