using System.Reflection;
using EventGuard.Runtime;

namespace EventGuard.Hosting.Tests;

public sealed class PollingConfigurationTests
{
    [Theory]
    [InlineData("snapshots")]
    [InlineData("workflows")]
    [InlineData("notices")]
    public async Task Configured_polling_reaches_store_and_shutdown_interrupts_long_idle_wait(string kind)
    {
        var settings = new EventGuardConfiguration(ConfigurationTests.Json("""
            {"EventGuard":{"Defaults":{"Polling":{"MaximumItems":375,"PollInterval":"00:02:00","SweepInterval":"00:03:00"}}}}
            """).GetSection("EventGuard"));
        var options = settings.GetPollingOptions(kind);
        using var stop = new CancellationTokenSource();
        var called = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = new JobDefinition(new("test", "polling"), "apply", 1, "items", 1, "v1", [1], [JobCapabilities.TransactionalHandler]);
        Task run;
        if (kind == "snapshots")
        {
            var store = StoreProxy.Create<ISnapshotStore>((method, args) => method.Name == nameof(ISnapshotStore.ListSnapshotsAsync)
                ? Listed((int)args[1]!, Array.Empty<SnapshotProgress>()) : throw new NotSupportedException(method.Name));
            var definition = new SnapshotDefinition(job, "selection", "source", "v1", SnapshotConsistency.SessionBound);
            run = new SnapshotWorker<Row>(store, definition, new EmptySource(), new("items", 1, _ => 1, _ => { }),
                null, options).RunAsync(stop.Token);
        }
        else if (kind == "workflows")
        {
            var store = StoreProxy.Create<IWorkflowStore>((method, args) => method.Name == nameof(IWorkflowStore.ListWorkflowsAsync)
                ? Listed((int)args[1]!, Array.Empty<Guid>()) : throw new NotSupportedException(method.Name));
            var definition = new WorkflowDefinition(job.Scope, "flow", 1, [new("wait", WorkflowStepKind.Signal)]);
            run = new WorkflowWorker(store, definition, options.PollInterval, options.MaximumItems).RunAsync(stop.Token);
        }
        else
        {
            var store = StoreProxy.Create<IExternalObservationStore>((method, args) => method.Name switch
            {
                nameof(IExternalObservationStore.ExpireObservationsAsync) => Swept((int)args[3]!),
                nameof(IExternalObservationStore.ApplyNextNoticeAsync) => Task.FromResult<ExternalNoticeReceipt?>(null),
                _ => throw new NotSupportedException(method.Name)
            });
            run = new ExternalNoticeWorker(store, new("test", "polling"), "service", "v1", null, options).RunAsync(stop.Token);
        }
        Assert.Equal(375, await called.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        stop.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(TimeSpan.FromSeconds(5)));

        Task<IReadOnlyList<T>> Listed<T>(int limit, T[] items) { called.TrySetResult(limit); return Task.FromResult<IReadOnlyList<T>>(items); }
        Task<int> Swept(int limit) { called.TrySetResult(limit); return Task.FromResult(0); }
    }

    public sealed record Row(string Id);
    private sealed class EmptySource : ISnapshotSource
    {
        public string Name => "source";
        public string Version => "v1";
        public SnapshotConsistency Consistency => SnapshotConsistency.SessionBound;
        public Task<ISnapshotReader> OpenAsync(ReadOnlyMemory<byte> parameters, string? resumeView, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
    }
    public class StoreProxy : DispatchProxy
    {
        private Func<MethodInfo, object?[], object?> _invoke = null!;
        public static T Create<T>(Func<MethodInfo, object?[], object?> invoke) where T : class
        {
            var result = Create<T, StoreProxy>();
            ((StoreProxy)(object)result)._invoke = invoke;
            return result;
        }
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => _invoke(targetMethod!, args!);
    }
}
