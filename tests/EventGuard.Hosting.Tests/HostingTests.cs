using System.Collections.Concurrent;
using System.Threading.Channels;
using EventGuard.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventGuard.Hosting.Tests;

public sealed class HostingTests
{
    private static readonly WorkScope Scope = new("test", "hosted", "source", "message");
    private static JsonConsumerOptions<Message> Options(WorkScope? scope = null) => new(scope ?? Scope,
        new("message-id/v1", "raw-json/v1"), new("message", 1, m => m.Version, _ => { }),
        m => (m.Id.ToString("D"), m.Id), new(maximumConcurrency: 2, idleDelay: TimeSpan.FromMilliseconds(5)));

    private static HostApplicationBuilder Builder()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(3));
        return builder;
    }

    [Fact]
    public async Task Typed_consumer_uses_fresh_async_scope_per_attempt_and_supplied_session()
    {
        var builder = Builder();
        var store = new FakeStore();
        var probe = new Probe();
        builder.Services.AddSingleton(probe);
        builder.Services.AddScoped<AttemptDependency>();
        builder.Services.AddSingleton<ITransactionalWorkStore<Session>>(store);
        builder.Services.AddEventGuard().AddJsonConsumer<Message, Session, Handler>("messages", Options());
        using var host = builder.Build();
        var consumer = host.Services.GetRequiredKeyedService<IEventGuardConsumer<Message>>("messages");
        var a = await consumer.SubmitAsync(new(Guid.NewGuid(), 1));
        var b = await consumer.SubmitAsync(new(Guid.NewGuid(), 1));
        await host.StartAsync();
        await UntilAsync(() => probe.Handled.Count == 2 && probe.Disposed.Count >= 3);
        await host.StopAsync();
        Assert.Equal(2, probe.Handled.Distinct().Count());
        Assert.All(probe.Handled, id => Assert.Contains(id, probe.Disposed));
        Assert.Equal(2, store.SessionsUsed);
        Assert.Equal(WorkState.Completed, (await consumer.FindAsync(a.Work.Id))!.State);
        Assert.Equal(WorkState.Completed, (await consumer.FindAsync(b.Work.Id))!.State);
    }

    [Fact]
    public async Task Missing_handler_dependency_prevents_all_workers_from_starting()
    {
        var builder = Builder();
        var store = new FakeStore();
        builder.Services.AddSingleton<ITransactionalWorkStore<Session>>(store);
        var ran = false;
        builder.Services.AddEventGuard().AddWorker("ingress", (_, _) => { ran = true; return Task.CompletedTask; })
            .AddJsonConsumer<Message, Session, Handler>("messages", Options());
        using var host = builder.Build();
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
        Assert.Contains("messages", error.Message);
        Assert.False(ran);
        Assert.Equal(0, store.Claims);
    }

    [Fact]
    public async Task Invalid_store_capabilities_fail_before_claim()
    {
        var builder = Builder();
        var store = new FakeStore { Capabilities = StoreCapabilities.None };
        builder.Services.AddSingleton<ITransactionalWorkStore<Session>>(store);
        builder.Services.AddEventGuard().AddJsonConsumer<Message, Session, NoopHandler>("messages", Options());
        using var host = builder.Build();
        await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
        Assert.Equal(0, store.Claims);
    }

    [Fact]
    public async Task Startup_errors_do_not_expose_provider_secrets()
    {
        var builder = Builder();
        builder.Services.AddEventGuard().AddWorker("broker", (_, _) => Task.CompletedTask,
            (_, _) => throw new InvalidOperationException("Password=do-not-log"));
        using var host = builder.Build();
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
        Assert.DoesNotContain("do-not-log", error.ToString());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Failure_or_unexpected_return_stops_host_and_cancels_siblings(bool returns)
    {
        var builder = Builder();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        builder.Services.AddEventGuard().AddWorker("sibling", async (_, ct) =>
        {
            started.SetResult();
            try { await Task.Delay(Timeout.Infinite, ct); }
            finally { stopped.SetResult(); }
        }).AddWorker("fault", async (_, _) =>
        {
            await started.Task;
            await Task.Yield();
            if (!returns) throw new IOException("unexpected");
        });
        using var host = builder.Build();
        await host.StartAsync();
        await stopped.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.True(host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.IsCancellationRequested);
        Assert.Equal("fault", host.Services.GetRequiredService<EventGuardHostStatus>().Failure!.WorkerName);
        await host.StopAsync();
    }

    [Fact]
    public async Task Shutdown_waits_for_cooperative_worker_cleanup()
    {
        var builder = Builder();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleaning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleaned = false;
        builder.Services.AddEventGuard().AddWorker("worker", async (_, ct) =>
        {
            started.SetResult();
            try { await Task.Delay(Timeout.Infinite, ct); }
            finally { cleaning.SetResult(); await release.Task; cleaned = true; }
        });
        using var host = builder.Build();
        await host.StartAsync();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(3));
        var stop = host.StopAsync();
        await cleaning.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.False(stop.IsCompleted);
        release.SetResult();
        await stop.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.True(cleaned);
        Assert.Null(host.Services.GetRequiredService<EventGuardHostStatus>().Failure);
    }

    [Fact]
    public async Task No_worker_configuration_fails_at_start()
    {
        var builder = Builder();
        builder.Services.AddEventGuard();
        using var host = builder.Build();
        await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
    }

    [Fact]
    public void Duplicate_names_and_durable_scopes_are_rejected_across_builder_calls()
    {
        var services = new ServiceCollection();
        services.AddEventGuard().AddJsonConsumer<Message, Session, NoopHandler>("one", Options());
        Assert.Throws<ArgumentException>(() => services.AddEventGuard().AddWorker("one", (_, _) => Task.CompletedTask));
        Assert.Throws<ArgumentException>(() => services.AddEventGuard().AddJsonConsumer<Message, Session, NoopHandler>("two", Options()));
        Assert.Single(services, d => d.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public async Task Named_consumers_of_same_type_use_their_own_store_and_scope()
    {
        var builder = Builder();
        var first = new FakeStore();
        var second = new FakeStore();
        builder.Services.AddEventGuard()
            .AddJsonConsumer<Message, Session, NoopHandler>("first", Options(), _ => first)
            .AddJsonConsumer<Message, Session, NoopHandler>("second", Options(new("other", "hosted", "source", "message")), _ => second);
        using var host = builder.Build();
        var a = host.Services.GetRequiredKeyedService<IEventGuardConsumer<Message>>("first");
        var b = host.Services.GetRequiredKeyedService<IEventGuardConsumer<Message>>("second");
        await a.SubmitAsync(new(Guid.NewGuid(), 1));
        await b.SubmitAsync(new(Guid.NewGuid(), 1));
        Assert.Equal(Scope.Id, Assert.Single(first.Items.Values).ScopeId);
        Assert.NotEqual(Scope.Id, Assert.Single(second.Items.Values).ScopeId);
    }

    private static async Task UntilAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition()) await Task.Delay(5, timeout.Token);
    }

    public sealed record Message(Guid Id, int Version);
    public sealed class Session { public bool Used { get; set; } }
    public sealed class Probe
    {
        public ConcurrentBag<Guid> Handled { get; } = [];
        public ConcurrentBag<Guid> Disposed { get; } = [];
    }
    public sealed class AttemptDependency(Probe probe) : IAsyncDisposable
    {
        public Guid Id { get; } = Guid.NewGuid();
        public ValueTask DisposeAsync() { probe.Disposed.Add(Id); return ValueTask.CompletedTask; }
    }
    public sealed class Handler(AttemptDependency dependency, Probe probe) : IEventGuardHandler<Message, Session>
    {
        public ValueTask HandleAsync(Message message, WorkItem work, Session session, CancellationToken ct)
        { session.Used = true; probe.Handled.Add(dependency.Id); return ValueTask.CompletedTask; }
    }
    public sealed class NoopHandler : IEventGuardHandler<Message, Session>
    {
        public ValueTask HandleAsync(Message message, WorkItem work, Session session, CancellationToken ct) => ValueTask.CompletedTask;
    }

    // Host/DI behavior double only; native atomicity is verified by provider suites and the generated-project smoke.
    private sealed class FakeStore : ITransactionalWorkStore<Session>, IStoreStartupCheck
    {
        private readonly Channel<WorkClaim> _pending = Channel.CreateUnbounded<WorkClaim>();
        public ConcurrentDictionary<Guid, WorkItem> Items { get; } = new();
        public StoreCapabilities Capabilities { get; init; } = StoreCapabilities.ConsumerTransactions;
        public int Claims;
        public int SessionsUsed;
        public Task<StoreStartupReport> ValidateStartupAsync(StoreStartupRequest request, CancellationToken ct = default) =>
            Task.FromResult(new StoreStartupReport("test-double", "1", 1, Capabilities, false));
        public Task<AcceptanceResult> AcceptAsync(WorkSubmission submission, CancellationToken ct = default)
        {
            var work = new WorkItem(Guid.NewGuid(), submission.Identity.Id, submission.Identity.Scope.Id, submission.MessageId,
                submission.Fingerprint, WorkState.Pending, 0, 0, 0, 0, DateTimeOffset.UtcNow, null, null, null);
            Items[work.Id] = work;
            _pending.Writer.TryWrite(new(work, Guid.NewGuid(), DateTimeOffset.UtcNow.AddMinutes(1), submission.CopyPayload()));
            return Task.FromResult(new AcceptanceResult(AcceptanceStatus.Accepted, work));
        }
        public Task<WorkItem?> FindAsync(WorkScope scope, Guid id, CancellationToken ct = default) =>
            Task.FromResult(Items.TryGetValue(id, out var work) && work.ScopeId == scope.Id ? work : null);
        public Task<WorkClaim?> ClaimNextAsync(WorkScope scope, Guid request, TimeSpan lease, CancellationToken ct = default)
        { Interlocked.Increment(ref Claims); return Task.FromResult(_pending.Reader.TryRead(out var work) ? work : null); }
        public async Task<ExecutionResult> ExecuteAsync(WorkClaim claim,
            Func<WorkItem, ReadOnlyMemory<byte>, Session, CancellationToken, ValueTask> handler, RetryPolicy policy, CancellationToken ct = default)
        {
            var session = new Session();
            await handler(claim.Work, claim.Payload, session, ct);
            if (session.Used) Interlocked.Increment(ref SessionsUsed);
            var work = claim.Work with { State = WorkState.Completed };
            Items[work.Id] = work;
            return new(ExecutionStatus.Completed, work);
        }
        public Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken ct = default) => throw new NotSupportedException();
    }
}
