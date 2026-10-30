using EventGuard.Runtime;
using Microsoft.Extensions.DependencyInjection;

namespace EventGuard.Hosting;

public static class ExternalResultBatchHostingExtensions
{
    /// <summary>Supervise a native result-batch worker. The callback must enlist all SQL in the supplied session.</summary>
    public static EventGuardBuilder AddExternalResultBatch<TSession>(this EventGuardBuilder builder, string name,
        ExternalResultTarget target,
        Func<IReadOnlyList<ExternalActionUpdate>, TSession, CancellationToken, ValueTask> handler,
        ExternalResultBatchOptions? batch = null, RetryPolicy? retry = null, WorkPumpOptions? pump = null,
        Func<IServiceProvider, IExternalResultBatchStore<TSession>>? storeFactory = null)
    {
        ArgumentNullException.ThrowIfNull(builder); ArgumentNullException.ThrowIfNull(target); ArgumentNullException.ThrowIfNull(handler);
        if (builder.Configuration is { } configuration)
        {
            batch = configuration.GetResultBatchOptions(name, batch);
            retry = configuration.GetRetryPolicy(name, retry);
            pump = configuration.GetWorkerOptions(name, pump);
        }
        builder.Services.AddKeyedSingleton<ExternalResultBatchWorker<TSession>>(name, (sp, _) =>
            new(storeFactory is null ? sp.GetRequiredService<IExternalResultBatchStore<TSession>>() : storeFactory(sp),
                target, handler, batch, retry, pump));
        return builder.AddWorkerCore(name, name,
            (sp, ct) => sp.GetRequiredKeyedService<ExternalResultBatchWorker<TSession>>(name).RunAsync(ct),
            (sp, ct) => (storeFactory is null ? sp.GetRequiredService<IExternalResultBatchStore<TSession>>() : storeFactory(sp))
                .ValidateExternalResultBatchAsync(ct), pump?.StartupTimeout ?? TimeSpan.FromSeconds(5));
    }
}
