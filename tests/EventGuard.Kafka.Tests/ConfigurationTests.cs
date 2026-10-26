using Confluent.Kafka;

namespace EventGuard.Kafka.Tests;

public sealed class ConfigurationTests
{
    [Theory]
    [InlineData("auto-commit")]
    [InlineData("auto-store")]
    [InlineData("read-uncommitted")]
    [InlineData("cooperative")]
    [InlineData("new-group-protocol")]
    [InlineData("implicit-offset-reset")]
    public void Unsafe_or_unverified_consumer_profile_is_rejected(string option)
    {
        var config = BrokerFixture.ConsumerConfig("validation");
        switch (option)
        {
            case "auto-commit": config.EnableAutoCommit = true; break;
            case "auto-store": config.EnableAutoOffsetStore = true; break;
            case "read-uncommitted": config.IsolationLevel = IsolationLevel.ReadUncommitted; break;
            case "cooperative": config.PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky; break;
            case "new-group-protocol": config.GroupProtocol = GroupProtocol.Consumer; break;
            case "implicit-offset-reset": config.AutoOffsetReset = null; break;
        }
        Assert.Throws<ArgumentException>(() => new KafkaIngress(config, new NeverCalledStore(),
            new("tenant", "consumer", "source", "event"), new("id/v1", "body/v1"), ["valid-topic"], acceptanceTimeout: TimeSpan.FromSeconds(5)));
    }
    [Theory]
    [InlineData("acks")]
    [InlineData("idempotence")]
    [InlineData("reports")]
    [InlineData("background-poll")]
    [InlineData("transaction")]
    [InlineData("infinite-timeout")]
    public void Publisher_requires_trustworthy_and_bounded_delivery_reporting(string option)
    {
        var config = BrokerFixture.ProducerConfig();
        switch (option)
        {
            case "acks": config.Acks = Acks.Leader; break;
            case "idempotence": config.EnableIdempotence = false; break;
            case "reports": config.EnableDeliveryReports = false; break;
            case "background-poll": config.EnableBackgroundPoll = false; break;
            case "transaction": config.TransactionalId = "unexpected"; break;
            case "infinite-timeout": config.MessageTimeoutMs = 0; break;
        }
        Assert.Throws<ArgumentException>(() => new KafkaPublisher(config, "local", "v1"));
    }
    private sealed class NeverCalledStore : IWorkStore
    {
        public Task<AcceptanceResult> AcceptAsync(WorkSubmission submission, CancellationToken cancellationToken = default) => throw new InvalidOperationException();
        public Task<WorkItem?> FindAsync(WorkScope scope, Guid workId, CancellationToken cancellationToken = default) => throw new InvalidOperationException();
        public Task<WorkClaim?> ClaimNextAsync(WorkScope scope, Guid requestId, TimeSpan leaseDuration, CancellationToken cancellationToken = default) => throw new InvalidOperationException();
        public Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken cancellationToken = default) => throw new InvalidOperationException();
    }
}
