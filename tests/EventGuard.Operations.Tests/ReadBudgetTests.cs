namespace EventGuard.Operations.Tests;

public sealed class ReadBudgetTests
{
    [Fact]
    public async Task Capacity_is_rejected_without_queueing_and_stays_owned_until_native_read_finishes()
    {
        var store = new BlockingStore();
        var reader = new OperationsReader(store, maximumConcurrency: 1, timeout: TimeSpan.FromMilliseconds(50));
        var query = new WorkQuery(new("test", "consumer", "source", "event"), DateTimeOffset.UtcNow);
        var pending = reader.ListAsync(query);
        await store.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Equal("read-capacity", (await Assert.ThrowsAsync<OperationsUnavailableException>(() => reader.ListAsync(query))).Code);
        Assert.Equal("read-capacity", (await Assert.ThrowsAsync<OperationsUnavailableException>(() => reader.ListOutboxAsync(
            new(new("test", "producer"), DateTimeOffset.UtcNow)))).Code);
        Assert.Equal(1, store.Calls);
        store.Finish.SetResult();
        await Assert.ThrowsAsync<OperationsUnavailableException>(() => pending);
        Assert.Empty((await reader.ListAsync(query)).Value.Items);
    }

    [Theory]
    [InlineData(0)] [InlineData(101)]
    public void Page_size_is_bounded(int size) => Assert.Throws<ArgumentOutOfRangeException>(() =>
        new WorkQuery(new("test", "consumer", "source", "event"), DateTimeOffset.UtcNow, pageSize: size));

    [Fact]
    public async Task Caller_cancellation_is_not_reported_as_database_failure()
    {
        using var stop = new CancellationTokenSource();
        stop.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new OperationsReader(new BlockingStore()).ListAsync(
            new(new("test", "consumer", "source", "event"), DateTimeOffset.UtcNow), stop.Token));
    }

    private sealed class BlockingStore : IWorkReadStore
    {
        public int Calls { get; private set; }
        public TaskCompletionSource Cancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Finish { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public async Task<WorkPage> ListAsync(WorkQuery query, CancellationToken cancellationToken = default)
        {
            Calls++;
            using var registration = cancellationToken.Register(() => Cancelled.TrySetResult());
            await Finish.Task;
            return new([], null);
        }
        public Task<WorkDetail?> FindAsync(WorkScope scope, Guid id, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
