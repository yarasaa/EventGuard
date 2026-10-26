namespace EventGuard.Operations.Tests;

public sealed class ReplayBudgetTests
{
    [Fact]
    public async Task Cancelled_command_keeps_its_slot_until_provider_finishes_and_preserves_confirmed_result()
    {
        var store = new BlockingReplayStore();
        var commands = new OperationsReplayer(store, 1, TimeSpan.FromMilliseconds(50));
        var request = new ReplayRequest(new("tenant", "consumer", "source", "event"), Guid.NewGuid(), Guid.NewGuid(), 1, "operator", "test");
        var pending = commands.ReplayAsync(request);
        await store.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(3));
        var capacity = await Assert.ThrowsAsync<ReplayUnavailableException>(() => commands.ReplayAsync(request));
        Assert.False(capacity.OutcomeUnconfirmed);
        var outbox = new OutboxReplayRequest(new("tenant", "producer"), Guid.NewGuid(), Guid.NewGuid(), 1, "operator", "test");
        Assert.False((await Assert.ThrowsAsync<ReplayUnavailableException>(() => commands.ReplayOutboxAsync(outbox))).OutcomeUnconfirmed);
        await Assert.ThrowsAsync<OperationsUnavailableException>(() => commands.FindOutboxReceiptAsync(outbox.Scope, outbox.IntentId, outbox.RequestId, outbox.Actor));
        Assert.Equal(1, store.Calls);
        store.Finish.SetResult();
        Assert.Equal(ReplayStatus.Accepted, (await pending).Status);
        Assert.Equal(OutboxReplayStatus.Accepted, (await commands.ReplayOutboxAsync(outbox)).Status);
    }
    private sealed class BlockingReplayStore : IWorkReplayStore, IOutboxReplayStore
    {
        public int Calls { get; private set; }
        public TaskCompletionSource Cancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Finish { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public async Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken ct = default)
        {
            Calls++;
            using var registration = ct.Register(() => Cancelled.TrySetResult());
            await Finish.Task;
            return new(ReplayStatus.Accepted, request.WorkId, 1, 2);
        }
        public Task<OutboxReplayResult> ReplayOutboxAsync(OutboxReplayRequest request, CancellationToken ct = default)
        { Calls++; return Task.FromResult(new OutboxReplayResult(OutboxReplayStatus.Accepted, request.IntentId, 1, 2)); }
        public Task<OutboxReplayReceipt?> FindOutboxReceiptAsync(ProducerScope scope, Guid intentId, Guid requestId, string actor, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<ReplayReceipt?> FindReceiptAsync(WorkScope scope, Guid workId, Guid requestId, string actor, CancellationToken ct = default) => throw new NotSupportedException();
    }
}
