using EventGuard.Hosting;
using EventGuard.PostgreSql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventGuard.Kafka.Tests;

public sealed class HostingConfigurationTests
{
    [Fact]
    public async Task Disabled_profile_skips_consumer_and_broker_without_connecting_or_resolving_dependencies()
    {
        var builder = Host.CreateApplicationBuilder(); builder.Logging.ClearProviders();
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
            { ["EventGuard:Workers:files:Enabled"] = "false" }).Build();
        builder.Services.AddEventGuard(config).AddKafkaJsonConsumer<HostedConsumerTests.Message, PostgreSqlSession, HostedConsumerTests.Handler>("files",
            new(new("test", "files", "source", "event"), new("id/v1", "body/v1"),
                new("event", 1, m => m.Version, _ => { }), m => (m.Id.ToString("D"), m.Id)),
            new Confluent.Kafka.ConsumerConfig { BootstrapServers = "unreachable.invalid:9092", GroupId = "test" }, ["events"],
            storeFactory: _ => throw new InvalidOperationException("Disabled store must not resolve"));
        using var host = builder.Build();
        await host.StartAsync().WaitAsync(TimeSpan.FromSeconds(5));
        await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));
    }
}
