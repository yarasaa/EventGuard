using System.Diagnostics;
using System.Text.Json;

namespace EventGuard.SqlServer.Tests;

public sealed class CrashTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Killing_a_process_with_open_business_and_outbox_writes_rolls_back_both()
    {
        var scope = DatabaseFixture.Scope();
        var work = (await db.Store.AcceptAsync(DatabaseFixture.Submission(scope))).Work;
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (root is not null && !File.Exists(Path.Combine(root.FullName, "global.json"))) root = root.Parent;
        Assert.NotNull(root);
        var output = new DirectoryInfo(AppContext.BaseDirectory);
        var path = Path.Combine(root.FullName, "tests", "EventGuard.SqlServer.CrashWorker", "bin", output.Parent!.Name, output.Name, "EventGuard.SqlServer.CrashWorker.dll");
        Assert.True(File.Exists(path));
        var dotnet = Environment.GetEnvironmentVariable("DOTNET_ROOT");
        var start = new ProcessStartInfo(string.IsNullOrEmpty(dotnet) ? "dotnet" : Path.Combine(dotnet, "dotnet"))
        { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(path);
        start.Environment["EVENTGUARD_SQLSERVER_TEST_CONNECTION"] = db.ConnectionString;
        start.Environment["EVENTGUARD_TEST_SCHEMA"] = db.Schema;
        start.Environment["EVENTGUARD_TEST_CONSUMER"] = scope.Consumer;
        using var process = Process.Start(start)!;
        try
        {
            var line = await process.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(15));
            Assert.NotNull(line);
            using var signal = JsonDocument.Parse(line);
            Assert.Equal("transaction-open", signal.RootElement.GetProperty("stage").GetString());
            Assert.Equal(work.Id, signal.RootElement.GetProperty("workId").GetGuid());
            Assert.Equal(0, await db.EffectCount(work.Id));
            var producer = new ProducerScope(scope.Tenant, scope.Consumer);
            Assert.Null(await db.Outbox.FindAsync(producer, work.Id));
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            WorkClaim? recovered;
            while ((recovered = await db.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10), timeout.Token)) is null)
                await Task.Delay(25, timeout.Token);
            Assert.Equal(2, recovered.Work.AttemptCount);
            Assert.Null(await db.Outbox.FindAsync(producer, work.Id));
            Assert.Equal(ExecutionStatus.Completed, (await db.Store.ExecuteAsync(recovered, async (w, p, s, ct) =>
            {
                await db.Effect(w, p, s, ct);
                await s.Outbox.EnqueueAsync(new(producer, work.Id, work.Id, new("test", "local", "v1", "files.created"), [1]), ct);
            }, new())).Status);
            Assert.Equal(1, await db.EffectCount(work.Id));
            Assert.NotNull(await db.Outbox.FindAsync(producer, work.Id));
        }
        finally { if (!process.HasExited) { process.Kill(entireProcessTree: true); await process.WaitForExitAsync(); } }
    }
}
