using EventGuard.Runtime;
using Microsoft.Extensions.DependencyInjection;

namespace EventGuard.Hosting;

internal sealed class JsonConsumer<TMessage, TSession, THandler>(IServiceProvider services,
    ITransactionalWorkStore<TSession> store, JsonConsumerOptions<TMessage> options) : IEventGuardConsumer<TMessage>
    where TMessage : notnull where THandler : class, IEventGuardHandler<TMessage, TSession>
{
    public WorkSubmission Map(ReadOnlyMemory<byte> originalJson) =>
        options.Contract.Map(options.Scope, options.Policy, originalJson, options.Identity);

    public Task<AcceptanceResult> SubmitAsync(TMessage message, CancellationToken cancellationToken = default) =>
        SubmitJsonAsync(options.Contract.Encode(message), cancellationToken);

    public Task<AcceptanceResult> SubmitJsonAsync(ReadOnlyMemory<byte> originalJson, CancellationToken cancellationToken = default) =>
        store.AcceptAsync(Map(originalJson), cancellationToken);

    public Task<WorkItem?> FindAsync(Guid workId, CancellationToken cancellationToken = default) =>
        store.FindAsync(options.Scope, workId, cancellationToken);

    internal async Task ValidateAsync(CancellationToken ct)
    {
        if (store is not IStoreStartupCheck check)
            throw new NotSupportedException("Hosted consumers require IStoreStartupCheck. Use WorkPump directly for custom unchecked stores.");
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        var report = await check.ValidateStartupAsync(new(options.Scope.Tenant, StoreRole.Consumer, options.Worker.Startup), timeout.Token);
        timeout.Token.ThrowIfCancellationRequested();
        var required = options.Worker.Startup.RequiredCapabilities | StoreCapabilities.ConsumerTransactions;
        if ((report.Capabilities & required) != required || options.Worker.Startup.RequireTenantBudget && !report.TenantBudgetConfigured)
            throw new NotSupportedException("Store capabilities or tenant budget do not satisfy this consumer's startup requirements.");
        await using var scope = services.CreateAsyncScope();
        _ = scope.ServiceProvider.GetRequiredService<THandler>();
    }

    internal Task RunAsync(CancellationToken ct) => new WorkPump<TSession>(store, options.Scope, HandleAsync,
        options.Retry, options.Worker, telemetry: services.GetService<RuntimeTelemetry>()).RunAsync(ct);

    private async ValueTask HandleAsync(WorkItem work, ReadOnlyMemory<byte> payload, TSession session, CancellationToken ct)
    {
        var message = options.Contract.Decode(payload);
        await using var scope = services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<THandler>().HandleAsync(message, work, session, ct);
    }
}
