using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;

namespace EventGuard.Hosting;

public static class EventGuardServiceCollectionExtensions
{
    public static EventGuardBuilder AddEventGuard(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<EventGuardHostStatus>();
        services.AddHostedService<EventGuardHostedService>();
        return new(services, services.LastOrDefault(d => d.ServiceType == typeof(EventGuardConfiguration))?.ImplementationInstance as EventGuardConfiguration);
    }

    public static EventGuardBuilder AddEventGuard(this IServiceCollection services, IConfiguration configuration,
        string sectionName = "EventGuard")
    {
        ArgumentNullException.ThrowIfNull(services); ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(sectionName);
        if (services.Any(d => d.ServiceType == typeof(EventGuardConfiguration)))
            throw new InvalidOperationException("Register EventGuard configuration once per host.");
        if (services.Any(d => d.ImplementationInstance is WorkerRegistration))
            throw new InvalidOperationException("Register EventGuard configuration before adding workers.");
        var settings = new EventGuardConfiguration(configuration.GetSection(sectionName));
        services.AddSingleton(settings);
        services.Configure<HostOptions>(options => options.ShutdownTimeout = settings.ShutdownTimeout);
        services.AddEventGuard();
        return new(services, settings);
    }
}

public sealed class EventGuardBuilder
{
    public IServiceCollection Services { get; }
    public EventGuardConfiguration? Configuration { get; }
    internal EventGuardBuilder(IServiceCollection services, EventGuardConfiguration? configuration = null)
    { Services = services; Configuration = configuration; }

    public EventGuardBuilder AddJsonConsumer<TMessage, TSession, THandler>(string name,
        JsonConsumerOptions<TMessage> options,
        Func<IServiceProvider, ITransactionalWorkStore<TSession>>? storeFactory = null)
        where TMessage : notnull where THandler : class, IEventGuardHandler<TMessage, TSession>
    {
        ArgumentNullException.ThrowIfNull(options);
        CheckName(name);
        if (Configuration is { } configuration)
            options = new(options.Scope, options.Policy, options.Contract, options.Identity,
                configuration.GetWorkerOptions(name, options.Worker), configuration.GetRetryPolicy(name, options.Retry));
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
            (sp, ct) => Resolve(sp).ValidateAsync(ct), (sp, ct) => Resolve(sp).RunAsync(ct), Configuration?.IsEnabled(name) ?? true, name));
        return this;
    }

    /// <summary>Supervise an existing ingress or dispatcher RunAsync loop. Resources must be DI-owned and cancellation-aware.</summary>
    public EventGuardBuilder AddWorker(string name, Func<IServiceProvider, CancellationToken, Task> run,
        Func<IServiceProvider, CancellationToken, Task>? validate = null)
        => AddWorker(name, name, run, validate);

    /// <summary>Use a shared configuration name for companion loops such as a consumer and its broker ingress.</summary>
    public EventGuardBuilder AddWorker(string name, string configurationName, Func<IServiceProvider, CancellationToken, Task> run,
        Func<IServiceProvider, CancellationToken, Task>? validate = null)
        => AddWorkerCore(name, configurationName, run, validate, Configuration?.GetWorkerOptions(configurationName).StartupTimeout);

    internal EventGuardBuilder AddWorkerCore(string name, string configurationName, Func<IServiceProvider, CancellationToken, Task> run,
        Func<IServiceProvider, CancellationToken, Task>? validate, TimeSpan? startupTimeout)
    {
        ArgumentNullException.ThrowIfNull(run);
        CheckName(name);
        if (!EventGuardConfiguration.ValidName(configurationName)) throw new ArgumentException("Use a valid configuration name.", nameof(configurationName));
        async Task Validate(IServiceProvider services, CancellationToken ct)
        {
            if (validate is null) return;
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
            if (startupTimeout is { } timeout) deadline.CancelAfter(timeout);
            await validate(services, deadline.Token);
        }
        Services.AddSingleton(new WorkerRegistration(name, null, Validate, run,
            Configuration?.IsEnabled(configurationName) ?? true, configurationName));
        return this;
    }

    private void CheckName(string name)
    {
        if (string.IsNullOrWhiteSpace(name) || name.Length > 128 || name.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not '-' and not '_' and not '.'))
            throw new ArgumentException("Use a 1–128 character worker name containing ASCII letters, digits, '.', '_' or '-'.", nameof(name));
        if (Services.Any(d => d.ImplementationInstance is WorkerRegistration r && string.Equals(r.Name, name, StringComparison.OrdinalIgnoreCase)))
            throw new ArgumentException($"EventGuard worker '{name}' is already registered.", nameof(name));
    }
}

internal sealed record WorkerRegistration(string Name, string? ScopeId,
    Func<IServiceProvider, CancellationToken, Task> Validate, Func<IServiceProvider, CancellationToken, Task> Run,
    bool Enabled = true, string? ConfigurationName = null);
