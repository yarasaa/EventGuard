using System.Diagnostics;
using System.Text.Json;
using Npgsql;

namespace EventGuard.PostgreSql.Tests;

public sealed class CrashTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Lost_commit_response_is_resolved_without_a_second_business_effect()
    {
        var scope = DatabaseFixture.Scope();
        var accepted = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope));
        var claim = (await db.Claim(scope))!;
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new CommitReplyDropProxy(settings.Port);
        settings.Port = proxy.Port;
        settings.SslMode = SslMode.Disable;
        settings.DisableGssEncryptionForFaultProxy();
        settings.Pooling = false;
        await using var throughProxy = NpgsqlDataSource.Create(settings.ConnectionString);
        var interrupted = new PostgreSqlWorkStore(throughProxy, db.Schema);
        var error = await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.ExecuteAsync(claim, db.Effect, new()));
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(claim.Token, error.OperationId);
        Assert.Equal(WorkState.Completed, (await db.Store.FindAsync(scope, accepted.Work.Id))!.State);
        Assert.Equal(ExecutionStatus.LeaseLost, (await db.Store.ExecuteAsync(claim, db.Effect, new())).Status);
        Assert.Equal(1, await db.EffectCount(accepted.Work.Id));
    }

    [Fact]
    public async Task Lost_producer_commit_reply_is_resolved_by_the_durable_request_receipt()
    {
        var scope = new ProducerScope("tenant", Guid.NewGuid().ToString("N"));
        var request = new ProducerRequest(scope, Guid.NewGuid(), ContentFingerprint.FromBytes([1]));
        var message = new OutgoingMessage(scope, Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "files.created"), [1]);
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new CommitReplyDropProxy(settings.Port);
        settings.Port = proxy.Port;
        settings.SslMode = SslMode.Disable;
        settings.DisableGssEncryptionForFaultProxy();
        settings.Pooling = false;
        await using var throughProxy = NpgsqlDataSource.Create(settings.ConnectionString);
        var interrupted = new PostgreSqlOutboxStore(throughProxy, db.Schema);
        var error = await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.ProduceAsync(request, async (session, ct) =>
        {
            await using var command = session.CreateCommand($"INSERT INTO {db.Schema}.business_effects VALUES(@id,'producer-before-reply-drop')");
            command.Parameters.AddWithValue("id", request.RequestId);
            await command.ExecuteNonQueryAsync(ct);
            await session.Outbox.EnqueueAsync(message, ct);
        }));
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(request.RequestId, error.OperationId);
        var repeated = await db.Outbox.ProduceAsync(request, (_, _) => throw new InvalidOperationException("Must not repeat business effect."));
        Assert.Equal(ProducerStatus.AlreadyCommitted, repeated.Status);
        Assert.Equal(1, await db.EffectCount(request.RequestId));
        Assert.Equal(OutboxState.Pending, (await db.Outbox.FindAsync(scope, message.IntentId))!.State);
    }

    [Fact]
    public async Task Lost_settlement_commit_reply_does_not_undo_a_recorded_publication()
    {
        var scope = new ProducerScope("tenant", Guid.NewGuid().ToString("N"));
        var message = new OutgoingMessage(scope, Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "files.created"), [1]);
        await db.Outbox.ProduceAsync(new(scope, Guid.NewGuid(), ContentFingerprint.FromBytes([1])),
            async (session, ct) => { await session.Outbox.EnqueueAsync(message, ct); });
        var claim = (await db.Outbox.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)))!;
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new CommitReplyDropProxy(settings.Port);
        settings.Port = proxy.Port;
        settings.SslMode = SslMode.Disable;
        settings.DisableGssEncryptionForFaultProxy();
        settings.Pooling = false;
        await using var throughProxy = NpgsqlDataSource.Create(settings.ConnectionString);
        var interrupted = new PostgreSqlOutboxStore(throughProxy, db.Schema);
        var outcome = new PublishOutcome(PublishStatus.Accepted, "test-accepted");
        var error = await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.SettleAsync(claim, outcome, new()));
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(claim.Token, error.OperationId);
        Assert.Equal(OutboxState.Published, (await db.Outbox.FindAsync(scope, message.IntentId))!.State);
        Assert.Equal(OutboxSettlementStatus.LeaseLost, (await db.Outbox.SettleAsync(claim, outcome, new())).Status);
        Assert.Null(await db.Outbox.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Process_kill_during_business_transaction_rolls_back_and_recovers()
    {
        var scope = DatabaseFixture.Scope();
        var accepted = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope));
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (root is not null && !File.Exists(Path.Combine(root.FullName, "global.json"))) root = root.Parent;
        Assert.NotNull(root);
        var output = new DirectoryInfo(AppContext.BaseDirectory);
        var worker = Path.Combine(root.FullName, "tests", "EventGuard.CrashWorker", "bin", output.Parent!.Name,
            output.Name, "EventGuard.CrashWorker.dll");
        Assert.True(File.Exists(worker), $"Crash worker was not built: {worker}");
        var dotnetRoot = Environment.GetEnvironmentVariable("DOTNET_ROOT");
        var executable = string.IsNullOrEmpty(dotnetRoot) ? "dotnet" : Path.Combine(dotnetRoot, OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet");
        var start = new ProcessStartInfo(executable)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        start.ArgumentList.Add(worker);
        start.Environment["EVENTGUARD_TEST_CONNECTION"] = db.ConnectionString;
        start.Environment["EVENTGUARD_TEST_SCHEMA"] = db.Schema;
        start.Environment["EVENTGUARD_TEST_CONSUMER"] = scope.Consumer;
        using var process = Process.Start(start) ?? throw new InvalidOperationException("Could not start crash worker.");
        try
        {
            var line = await process.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(15));
            Assert.NotNull(line);
            using var signal = JsonDocument.Parse(line);
            Assert.Equal("transaction-open", signal.RootElement.GetProperty("stage").GetString());
            Assert.Equal(accepted.Work.Id, signal.RootElement.GetProperty("workId").GetGuid());
            Assert.Equal(0, await db.EffectCount(accepted.Work.Id));
            var producerScope = new ProducerScope(scope.Tenant, scope.Consumer);
            Assert.Null(await db.Outbox.FindAsync(producerScope, accepted.Work.Id));
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
            Assert.NotEqual(0, process.ExitCode);
            WorkClaim? recovered = null;
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            while (recovered is null)
            {
                recovered = await db.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10), deadline.Token);
                if (recovered is null) await Task.Delay(25, deadline.Token);
            }
            Assert.Equal(2, recovered.Work.AttemptCount);
            Assert.Null(await db.Outbox.FindAsync(producerScope, accepted.Work.Id));
            Assert.Equal(ExecutionStatus.Completed, (await db.Store.ExecuteAsync(recovered, async (work, payload, session, ct) =>
            {
                await db.Effect(work, payload, session, ct);
                await session.Outbox.EnqueueAsync(new(producerScope, work.Id, work.Id,
                    new("test", "local", "v1", "files.processed"), [1]), ct);
            }, new())).Status);
            Assert.Equal(1, await db.EffectCount(accepted.Work.Id));
            Assert.Equal(accepted.Work.Id, (await db.Outbox.FindAsync(producerScope, accepted.Work.Id))!.OriginWorkId);
            await using var command = db.DataSource.CreateCommand($"SELECT count(*) FROM {db.Schema}.attempts WHERE work_id=@id AND status='Abandoned'");
            command.Parameters.AddWithValue("id", accepted.Work.Id);
            Assert.Equal(1L, (long)(await command.ExecuteScalarAsync())!);
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync();
            }
        }
    }
}
