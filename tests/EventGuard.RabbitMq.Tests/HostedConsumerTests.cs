using EventGuard.Hosting;
using EventGuard.PostgreSql;
using EventGuard.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace EventGuard.RabbitMq.Tests;

public sealed class HostedConsumerTests(BrokerFixture broker) : IClassFixture<BrokerFixture>
{
    [Fact]
    public async Task One_registration_processes_typed_json_deduplicates_and_releases_the_subscription_on_stop()
    {
        var queue = await broker.QueueAsync();
        var scope = BrokerFixture.Scope();
        var contract = new JsonMessageContract<Message>(scope.EventType, 1, m => m.Version, _ => { });
        var message = new Message(Guid.NewGuid(), 1);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(broker);
        var resolutions = 0;
        builder.Services.AddEventGuard().AddRabbitMqJsonConsumer<Message, PostgreSqlSession, Handler>("files",
            new(scope, BrokerFixture.Policy, contract, m => (m.Id.ToString("D"), m.Id)), queue, factory =>
            {
                factory.HostName = "127.0.0.1"; factory.Port = 5673; factory.VirtualHost = "eventguard_test";
                factory.UserName = "eventguard"; factory.Password = "eventguard-local-only";
                factory.RequestedConnectionTimeout = TimeSpan.FromSeconds(5);
            }, storeFactory: _ => { resolutions++; return broker.Store; });
        using var host = builder.Build();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        try
        {
            await host.StartAsync(timeout.Token);
            // The typed payload carries identity; broker message-id headers are optional in this helper.
            await broker.RawAsync(queue, null, contract.Encode(message));
            await broker.RawAsync(queue, null, contract.Encode(message));
            WorkItem? work;
            do
            {
                await Task.Delay(20, timeout.Token);
                work = await broker.FindByMessageAsync(scope, message.Id);
            } while (work?.State != WorkState.Completed);
            var duplicate = await host.Services.GetRequiredKeyedService<IEventGuardConsumer<Message>>("files").SubmitAsync(message, timeout.Token);
            Assert.Equal(AcceptanceStatus.Duplicate, duplicate.Status);
            Assert.Equal(1, duplicate.Work.AttemptCount);
            Assert.Equal(1, resolutions);
        }
        finally { await host.StopAsync(timeout.Token); }
        Assert.Equal(0u, (await broker.Admin.QueueDeclarePassiveAsync(queue, timeout.Token)).ConsumerCount);
        Assert.Null(host.Services.GetRequiredService<EventGuardHostStatus>().Failure);
        Assert.Equal(1, await broker.CountAsync("effects"));
    }

    public sealed record Message(Guid Id, int Version);
    public sealed class Handler(BrokerFixture db) : IEventGuardHandler<Message, PostgreSqlSession>
    {
        public ValueTask HandleAsync(Message message, WorkItem work, PostgreSqlSession session, CancellationToken ct) =>
            db.Effect(work, ReadOnlyMemory<byte>.Empty, session, ct);
    }
}
