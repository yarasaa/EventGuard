using CatalogPilot.Contracts;
using CatalogPilot.Infrastructure;
using EventGuard;
using EventGuard.PostgreSql;
using EventGuard.RabbitMq;
using EventGuard.Runtime;

namespace CatalogPilot.Hosting;

public static class CatalogWorker
{
    public static async Task RunAsync(PilotStore store, CancellationToken ct)
    {
        await store.ValidateAsync(ct);
        await using var connection = await RabbitMqConnection.OpenAsync(store.Settings.Broker(), ct);
        await using var publisher = new RabbitMqPublisher(connection, "pilot", "v1", channelCount: 2);
        var config = store.Settings;
        var ingress = new RabbitMqIngress(connection, store.Work, config.Consumer, config.Policy, config.InputQueue,
            message => ProductContract.Codec.Map(config.Consumer, config.Policy, message.Body, value => (value.EventId.ToString("D"), value.EventId)),
            prefetch: 16, quarantine: new(config.InputQueue + "/v1"));
        var retry = new RetryPolicy(maximumAttempts: 5, initialDelay: TimeSpan.FromMilliseconds(500), maximumDelay: TimeSpan.FromSeconds(2), jitterRatio: 0);
        var options = new WorkPumpOptions(maximumConcurrency: 2, leaseDuration: TimeSpan.FromSeconds(3), idleDelay: TimeSpan.FromMilliseconds(50));
        var pump = new WorkPump<PostgreSqlSession>(store.Work, config.Consumer, store.ApplyAsync, retry, options);
        using var stopped = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var tasks = new[]
        {
            ingress.RunAsync(stopped.Token), pump.RunAsync(stopped.Token),
            new OutboxDispatcher(store.Outbox, config.Producer, [publisher], retry, options).RunAsync(stopped.Token),
            new OutboxDispatcher(store.Outbox, config.IndexedProducer, [publisher], retry, options).RunAsync(stopped.Token)
        };
        Console.WriteLine("Worker started; concurrency=2; ingress prefetch=16.");
        try { await await Task.WhenAny(tasks); }
        finally { await stopped.CancelAsync(); await Task.WhenAll(tasks); }
    }
}
