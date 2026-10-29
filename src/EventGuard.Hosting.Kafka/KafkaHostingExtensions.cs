using Confluent.Kafka;
using EventGuard.Kafka;
using Microsoft.Extensions.DependencyInjection;

namespace EventGuard.Hosting;

public static class KafkaHostingExtensions
{
    /// <summary>Registers a typed worker and a Kafka ingress with manual durable acceptance and offset commit.</summary>
    public static EventGuardBuilder AddKafkaJsonConsumer<TMessage, TSession, THandler>(this EventGuardBuilder builder,
        string name, JsonConsumerOptions<TMessage> options, ConsumerConfig configuration, IEnumerable<string> topics,
        TimeSpan? acceptanceTimeout = null, IngressQuarantineOptions? quarantine = null,
        Func<IServiceProvider, ITransactionalWorkStore<TSession>>? storeFactory = null)
        where TMessage : notnull where THandler : class, IEventGuardHandler<TMessage, TSession>
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(topics);
        var snapshot = new ConsumerConfig(configuration.ToDictionary(p => p.Key, p => p.Value));
        var topicSnapshot = topics.ToArray();
        var key = new object();
        builder.Services.AddKeyedSingleton<Binding<TSession>>(key, (sp, _) => new(
            (storeFactory is null ? sp.GetRequiredService<ITransactionalWorkStore<TSession>>() : storeFactory(sp))
                ?? throw new InvalidOperationException("No work store was provided.")));
        ITransactionalWorkStore<TSession> Store(IServiceProvider sp) => sp.GetRequiredKeyedService<Binding<TSession>>(key).Store;
        builder.AddJsonConsumer<TMessage, TSession, THandler>(name, options, Store);
        builder.Services.AddKeyedSingleton<KafkaIngress>(key, (sp, _) =>
        {
            var consumer = sp.GetRequiredKeyedService<IEventGuardConsumer<TMessage>>(name);
            return new(snapshot, Store(sp), options.Scope, options.Policy, topicSnapshot,
                delivery => delivery.Message.Value is { } body ? consumer.Map(body)
                    : throw new IngressMessageRejectedException("unmapped-tombstone"), acceptanceTimeout, quarantine);
        });
        KafkaIngress Ingress(IServiceProvider sp) => sp.GetRequiredKeyedService<KafkaIngress>(key);
        return builder.AddWorker(name + ".kafka", name, (sp, ct) => Ingress(sp).RunAsync(ct),
            (sp, ct) => { _ = Ingress(sp); return Task.CompletedTask; });
    }

    private sealed record Binding<TSession>(ITransactionalWorkStore<TSession> Store);
}
