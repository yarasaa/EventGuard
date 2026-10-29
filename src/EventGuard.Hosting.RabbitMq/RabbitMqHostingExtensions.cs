using System.Text;
using EventGuard.RabbitMq;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace EventGuard.Hosting;

public static class RabbitMqHostingExtensions
{
    /// <summary>Registers a typed worker and an owned RabbitMQ ingress connection. Provision the queue separately.</summary>
    public static EventGuardBuilder AddRabbitMqJsonConsumer<TMessage, TSession, THandler>(this EventGuardBuilder builder,
        string name, JsonConsumerOptions<TMessage> options, string queue, Action<ConnectionFactory> configure,
        ushort prefetch = 16, IngressQuarantineOptions? quarantine = null,
        Func<IServiceProvider, ITransactionalWorkStore<TSession>>? storeFactory = null)
        where TMessage : notnull where THandler : class, IEventGuardHandler<TMessage, TSession>
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(configure);
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        if (new UTF8Encoding(false, true).GetByteCount(queue) > 255) throw new ArgumentException("Queue name exceeds AMQP short-string limit.", nameof(queue));
        if (prefetch is < 1 or > 1024) throw new ArgumentOutOfRangeException(nameof(prefetch));
        var key = new object();
        builder.Services.AddKeyedSingleton<Binding<TSession>>(key, (sp, _) =>
        {
            var factory = new ConnectionFactory { AutomaticRecoveryEnabled = false, TopologyRecoveryEnabled = false };
            configure(factory);
            if (factory.AutomaticRecoveryEnabled) throw new ArgumentException("Automatic recovery must stay disabled.", nameof(configure));
            var store = storeFactory is null ? sp.GetRequiredService<ITransactionalWorkStore<TSession>>() : storeFactory(sp);
            if (store is null) throw new InvalidOperationException("No work store was provided.");
            if (quarantine is not null && store is not IIngressQuarantineStore)
                throw new ArgumentException("Quarantine requires a durable ingress quarantine store.", nameof(quarantine));
            return new(store, factory);
        });
        Binding<TSession> Resolve(IServiceProvider sp) => sp.GetRequiredKeyedService<Binding<TSession>>(key);
        builder.AddJsonConsumer<TMessage, TSession, THandler>(name, options, sp => Resolve(sp).Store);
        return builder.AddWorker(name + ".rabbitmq", name, async (sp, ct) =>
        {
            var binding = Resolve(sp);
            var consumer = sp.GetRequiredKeyedService<IEventGuardConsumer<TMessage>>(name);
            await using var connection = await RabbitMqConnection.OpenAsync(binding.Factory, ct);
            var ingress = new RabbitMqIngress(connection, binding.Store, options.Scope, options.Policy, queue,
                delivery => consumer.Map(delivery.Body), prefetch, quarantine);
            await ingress.RunAsync(ct);
        }, (sp, ct) => { _ = Resolve(sp); return Task.CompletedTask; });
    }

    private sealed record Binding<TSession>(ITransactionalWorkStore<TSession> Store, ConnectionFactory Factory);
}
