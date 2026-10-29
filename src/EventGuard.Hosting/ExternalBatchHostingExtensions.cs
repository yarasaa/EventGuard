using EventGuard.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventGuard.Hosting;

public static class ExternalBatchHostingExtensions
{
    /// <summary>Register supervised submit/reconcile/observe loops supported by a thread-safe DI-owned adapter.</summary>
    public static EventGuardBuilder AddExternalBatchService<TAdapter>(this EventGuardBuilder builder,
        string name, ProducerScope scope, ExternalBatchWorkerOptions? options = null,
        ExternalActionPolicy? policy = null, ExternalCallLimit? limit = null,
        Func<IServiceProvider, IExternalActionStore>? storeFactory = null,
        IEnumerable<ExternalPhase>? phases = null)
        where TAdapter : class, IExternalBatchServiceAdapter
    {
        ArgumentNullException.ThrowIfNull(builder); ArgumentNullException.ThrowIfNull(scope);
        if (builder.Configuration is { } configuration)
        {
            options = configuration.GetBatchOptions(name, options);
            policy = configuration.GetExternalPolicy(name, policy);
            limit = configuration.GetCallLimit(name, limit);
            phases = configuration.GetPhases(name, phases);
        }
        var selected = phases?.Take(4).ToArray();
        if (selected is not null && (selected.Length is < 1 or > 3 || selected.Distinct().Count() != selected.Length
            || selected.Any(p => !Enum.IsDefined(p))))
            throw new ArgumentException("Choose distinct submit, reconcile or observe phases.", nameof(phases));
        builder.Services.TryAddSingleton<TAdapter>();
        builder.Services.AddKeyedSingleton<BatchRegistration>(name, (sp, _) =>
        {
            var adapter = sp.GetRequiredService<TAdapter>();
            var store = storeFactory is null ? sp.GetRequiredService<IExternalActionStore>() : storeFactory(sp);
            if (store is null) throw new InvalidOperationException("An external action store is required.");
            var enabled = selected ?? new[] { ExternalPhase.Submit }
                .Concat(adapter.CanReconcile ? [ExternalPhase.Reconcile] : [])
                .Concat(adapter.CanObserve ? [ExternalPhase.Observe] : []).ToArray();
            var limiter = limit is null ? null : store as IExternalCallLimiter ?? sp.GetRequiredService<IExternalCallLimiter>();
            return new(store, enabled.Select(phase => new ExternalBatchWorker(store, scope, adapter, phase,
                policy, options, limit, limiter)).ToArray());
        });
        BatchRegistration Resolve(IServiceProvider sp) => sp.GetRequiredKeyedService<BatchRegistration>(name);
        return builder.AddWorkerCore(name, name, (sp, ct) => Resolve(sp).RunAsync(ct), async (sp, ct) =>
        {
            using var startup = CancellationTokenSource.CreateLinkedTokenSource(ct);
            startup.CancelAfter(options?.Pump.StartupTimeout ?? TimeSpan.FromSeconds(5));
            if (Resolve(sp).Store is IExternalActionBatchStore native) await native.ValidateExternalBatchAsync(startup.Token);
            else await Resolve(sp).Store.ValidateExternalActionsAsync(startup.Token);
        }, options?.Pump.StartupTimeout ?? TimeSpan.FromSeconds(5));
    }

    private sealed record BatchRegistration(IExternalActionStore Store, ExternalBatchWorker[] Workers)
    {
        public async Task RunAsync(CancellationToken cancellationToken)
        {
            using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            await Task.WhenAll(Workers.Select(Run));
            async Task Run(ExternalBatchWorker worker)
            {
                try { await worker.RunAsync(stop.Token); }
                catch { stop.Cancel(); throw; }
            }
        }
    }
}
