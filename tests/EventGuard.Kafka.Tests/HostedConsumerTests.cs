using EventGuard.Hosting;
using EventGuard.PostgreSql;
using EventGuard.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace EventGuard.Kafka.Tests;

public sealed class HostedConsumerTests(BrokerFixture broker, PostgreSql.Tests.DatabaseFixture db)
    : IClassFixture<BrokerFixture>, IClassFixture<PostgreSql.Tests.DatabaseFixture>
{
    [Fact]
    public async Task One_registration_processes_typed_json_and_commits_duplicates_without_repeating_the_handler()
    {
        var topic = await broker.TopicAsync();
        var group = broker.Group();
        var scope = PostgreSql.Tests.DatabaseFixture.Scope();
        var contract = new JsonMessageContract<Message>(scope.EventType, 1, m => m.Version, _ => { });
        var message = new Message(Guid.NewGuid(), 1);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(db);
        var resolutions = 0;
        builder.Services.AddEventGuard().AddKafkaJsonConsumer<Message, PostgreSqlSession, Handler>("files",
            new(scope, new("message-id/v1", "body/v1"), contract, m => (m.Id.ToString("D"), m.Id)),
            BrokerFixture.ConsumerConfig(group), [topic], acceptanceTimeout: TimeSpan.FromSeconds(5),
            storeFactory: _ => { resolutions++; return db.Store; });
        using var host = builder.Build();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            await host.StartAsync(timeout.Token);
            await broker.RawAsync(topic, body: contract.Encode(message));
            await broker.RawAsync(topic, body: contract.Encode(message));
            await BrokerFixture.WaitCommitAsync(group, topic, 2);
            var consumer = host.Services.GetRequiredKeyedService<IEventGuardConsumer<Message>>("files");
            var duplicate = await consumer.SubmitAsync(message, timeout.Token);
            Assert.Equal(AcceptanceStatus.Duplicate, duplicate.Status);
            WorkItem? work;
            do
            {
                await Task.Delay(20, timeout.Token);
                work = await consumer.FindAsync(duplicate.Work.Id, timeout.Token);
            } while (work?.State != WorkState.Completed);
            Assert.Equal(1, work.AttemptCount);
            Assert.Equal(1, await db.EffectCount(work.Id));
            Assert.Equal(1, resolutions);
        }
        finally { await host.StopAsync(timeout.Token); }
        Assert.Null(host.Services.GetRequiredService<EventGuardHostStatus>().Failure);
    }

    public sealed record Message(Guid Id, int Version);
    public sealed class Handler(PostgreSql.Tests.DatabaseFixture db) : IEventGuardHandler<Message, PostgreSqlSession>
    {
        public ValueTask HandleAsync(Message message, WorkItem work, PostgreSqlSession session, CancellationToken ct) =>
            db.Effect(work, ReadOnlyMemory<byte>.Empty, session, ct);
    }
}
