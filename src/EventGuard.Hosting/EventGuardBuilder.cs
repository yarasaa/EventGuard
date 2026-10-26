using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace EventGuard.Hosting;

public static class EventGuardServiceCollectionExtensions
{
    public static EventGuardBuilder AddEventGuard(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<EventGuardHostStatus>();
        services.AddHostedService<EventGuardHostedService>();
        return new(services);
    }
}

public sealed class EventGuardBuilder
{
    public IServiceCollection Services { get; }
    internal EventGuardBuilder(IServiceCollection services) => Services = services;

    public EventGuardBuilder AddJsonConsumer<TMessage, TSession, THandler>(string name,
        JsonConsumerOptions<TMessage> options,
        Func<IServiceProvider, ITransactionalWorkStore<TSession>>? storeFactory = null)
        where TMessage : notnull where THandler : class, IEventGuardHandler<TMessage, TSession>
    {
        ArgumentNullException.ThrowIfNull(options);
        CheckName(name);
        // Two handlers claiming the same scope can interpret the same durable payload differently.
        if (Services.Any(d => d.ImplementationInstance is WorkerRegistration r && r.ScopeId == options.Scope.Id))
            throw new ArgumentException("A consumer for this durable scope is already registered in this host.", nameof(options));
        Services.TryAddScoped<THandler>();
        Services.AddKeyedSingleton<IEventGuardConsumer<TMessage>>(name, (sp, _) =>
            new JsonConsumer<TMessage, TSession, THandler>(sp,
                storeFactory is null ? sp.GetRequiredService<ITransactionalWorkStore<TSession>>() : storeFactory(sp)
                    ?? throw new InvalidOperationException($"EventGuard consumer '{name}' has no store."), options));
        JsonConsumer<TMessage, TSession, THandler> Resolve(IServiceProvider sp) =>
            (JsonConsumer<TMessage, TSession, THandler>)sp.GetRequiredKeyedService<IEventGuardConsumer<TMessage>>(name);
        Services.AddSingleton(new WorkerRegistration(name, options.Scope.Id,
            (sp, ct) => Resolve(sp).ValidateAsync(ct), (sp, ct) => Resolve(sp).RunAsync(ct)));
        return this;
    }

    /// <summary>Supervise an existing ingress or dispatcher RunAsync loop. Resources must be DI-owned and cancellation-aware.</summary>
    public EventGuardBuilder AddWorker(string name, Func<IServiceProvider, CancellationToken, Task> run,
        Func<IServiceProvider, CancellationToken, Task>? validate = null)
    {
        ArgumentNullException.ThrowIfNull(run);
        CheckName(name);
        Services.AddSingleton(new WorkerRegistration(name, null, validate ?? ((_, _) => Task.CompletedTask), run));
        return this;
    }

    private void CheckName(string name)
    {
        if (string.IsNullOrWhiteSpace(name) || name.Length > 128 || name.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not '-' and not '_' and not '.'))
            throw new ArgumentException("Use a 1–128 character worker name containing ASCII letters, digits, '.', '_' or '-'.", nameof(name));
        if (Services.Any(d => d.ImplementationInstance is WorkerRegistration r && r.Name == name))
            throw new ArgumentException($"EventGuard worker '{name}' is already registered.", nameof(name));
    }
}

internal sealed record WorkerRegistration(string Name, string? ScopeId,
    Func<IServiceProvider, CancellationToken, Task> Validate, Func<IServiceProvider, CancellationToken, Task> Run);
