using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;

namespace EventGuard.Kafka.Tests;

public sealed class CrashTests(BrokerFixture broker) : IClassFixture<BrokerFixture>
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Process_killed_after_database_acceptance_before_offset_commit_recovers_once(bool sqlServer)
    {
        await using var db = new StoreHarness(sqlServer); await db.InitializeAsync();
        var topic = await broker.TopicAsync(); var group = broker.Group();
        await broker.RawAsync(topic, Guid.NewGuid());
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (root is not null && !File.Exists(Path.Combine(root.FullName, "global.json"))) root = root.Parent;
        Assert.NotNull(root);
        var output = new DirectoryInfo(AppContext.BaseDirectory);
        var worker = Path.Combine(root.FullName, "tests", "EventGuard.Kafka.CrashWorker", "bin", output.Parent!.Name, output.Name, "EventGuard.Kafka.CrashWorker.dll");
        var dotnet = Environment.GetEnvironmentVariable("DOTNET_ROOT");
        var start = new ProcessStartInfo(string.IsNullOrEmpty(dotnet) ? "dotnet" : Path.Combine(dotnet, "dotnet"))
        { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(worker);
        start.Environment["EVENTGUARD_TEST_CONNECTION"] = db.ConnectionString;
        start.Environment["EVENTGUARD_TEST_PROVIDER"] = sqlServer ? "sqlserver" : "postgres";
        start.Environment["EVENTGUARD_TEST_SCHEMA"] = db.Schema;
        start.Environment["EVENTGUARD_TEST_TOPIC"] = topic;
        start.Environment["EVENTGUARD_TEST_GROUP"] = group;
        start.Environment["EVENTGUARD_TEST_CONSUMER"] = db.Scope.Consumer;
        using var process = Process.Start(start)!;
        var stderr = process.StandardError.ReadToEndAsync();
        try
        {
            var line = await process.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(15));
            Assert.True(line is not null, process.HasExited ? await stderr : "Crash worker did not signal acceptance.");
            using var signal = JsonDocument.Parse(line!);
            Assert.Equal("durable-before-offset", signal.RootElement.GetProperty("stage").GetString());
            var id = signal.RootElement.GetProperty("workId").GetGuid();
            Assert.Equal(Offset.Unset.Value, BrokerFixture.Committed(group, topic));
            process.Kill(entireProcessTree: true); await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
            var observed = new ObservedStore(db.Store);
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var running = IngressTests.Ingress(db, observed, topic, group).RunAsync(stop.Token);
            try { await BrokerFixture.WaitCommitAsync(group, topic, 1); }
            finally { stop.Cancel(); await running; }
            Assert.Equal(AcceptanceStatus.Duplicate, Assert.Single(observed.Results).Status);
            var claim = (await db.Store.ClaimNextAsync(db.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)))!;
            Assert.Equal(id, claim.Work.Id);
            Assert.Equal(ExecutionStatus.Completed, (await db.ExecuteAsync(claim)).Status);
            Assert.Equal(1, await db.EffectCount(id));
            Assert.Equal(1, await db.CountAsync("works"));
        }
        finally { if (!process.HasExited) { process.Kill(entireProcessTree: true); await process.WaitForExitAsync(); } }
    }
}
