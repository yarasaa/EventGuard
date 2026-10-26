using System.Diagnostics;
using System.Text.Json;
using EventGuard.PostgreSql;
using Npgsql;

namespace EventGuard.RabbitMq.Tests;

public sealed class IngressTests(BrokerFixture broker) : IClassFixture<BrokerFixture>
{
    [Fact]
    public async Task Accepted_duplicate_and_conflict_are_acked_only_after_durable_storage()
    {
        var queue = await broker.QueueAsync();
        var scope = BrokerFixture.Scope();
        var id = Guid.NewGuid();
        await broker.RawAsync(queue, id.ToString("D"), [1]);
        await broker.RawAsync(queue, id.ToString("D"), [1]);
        await broker.RawAsync(queue, id.ToString("D"), [2]);
        var recording = new RecordingStore(broker.Store);
        await broker.DrainAsync(queue, scope, recording);
        Assert.Equal(new[] { AcceptanceStatus.Accepted, AcceptanceStatus.Duplicate, AcceptanceStatus.Conflict }, recording.Outcomes);
        var work = await broker.FindByMessageAsync(scope, id);
        Assert.NotNull(work);
        var claim = await broker.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10));
        Assert.Equal(new byte[] { 1 }, claim!.Payload);
        await using var command = broker.DataSource.CreateCommand($"SELECT payload FROM {broker.Schema}.conflicts WHERE work_id=@id");
        command.Parameters.AddWithValue("id", work.Id);
        Assert.Equal(new byte[] { 2 }, (byte[])(await command.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task Real_database_failure_leaves_delivery_available_for_a_healthy_receiver()
    {
        var queue = await broker.QueueAsync();
        var scope = BrokerFixture.Scope();
        var id = Guid.NewGuid();
        await broker.RawAsync(queue, id.ToString("D"), [1]);
        var broken = new PostgreSqlWorkStore(broker.DataSource, "eg_missing_" + Guid.NewGuid().ToString("N"));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var error = await Assert.ThrowsAsync<PostgresException>(() => new RabbitMqIngress(broker.Connection, broken, scope, BrokerFixture.Policy, queue).RunAsync(timeout.Token));
        Assert.Equal(PostgresErrorCodes.UndefinedTable, error.SqlState);
        Assert.Null(await broker.FindByMessageAsync(scope, id));
        var pending = await broker.Admin.BasicGetAsync(queue, autoAck: false);
        Assert.NotNull(pending);
        Assert.True(pending.Redelivered);
        await broker.Admin.BasicNackAsync(pending.DeliveryTag, multiple: false, requeue: true);
        await broker.DrainAsync(queue, scope);
        Assert.NotNull(await broker.FindByMessageAsync(scope, id));
    }

    [Theory]
    [InlineData("scope")]
    [InlineData("policy")]
    public async Task Mapper_cannot_change_its_pinned_scope_or_policy(string change)
    {
        var queue = await broker.QueueAsync();
        var scope = BrokerFixture.Scope();
        var id = Guid.NewGuid();
        await broker.RawAsync(queue, id.ToString("D"), [1]);
        var ingress = new RabbitMqIngress(broker.Connection, broker.Store, scope, BrokerFixture.Policy, queue,
            d => new(new(change == "scope" ? BrokerFixture.Scope() : scope,
                change == "policy" ? new("other", "body/v1") : BrokerFixture.Policy, "file"), id, d.Body.Span, ContentFingerprint.FromBytes(d.Body.Span)));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await Assert.ThrowsAsync<InvalidOperationException>(() => ingress.RunAsync(timeout.Token));
        Assert.Null(await broker.FindByMessageAsync(scope, id));
        Assert.Equal(1u, (await broker.Admin.QueueDeclarePassiveAsync(queue)).MessageCount);
        await broker.DrainAsync(queue, scope);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("external-id-needs-a-custom-mapper")]
    [InlineData("00000000-0000-0000-0000-000000000000")]
    public async Task Invalid_default_message_identity_is_retained_without_inventing_a_new_id(string? messageId)
    {
        var queue = await broker.QueueAsync();
        await broker.RawAsync(queue, messageId, [1]);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await Assert.ThrowsAsync<IngressMessageRejectedException>(() => new RabbitMqIngress(broker.Connection, broker.Store,
            BrokerFixture.Scope(), BrokerFixture.Policy, queue).RunAsync(timeout.Token));
        var pending = await broker.Admin.BasicGetAsync(queue, true);
        Assert.NotNull(pending);
        Assert.True(pending.Redelivered);
        Assert.Equal(messageId, pending.BasicProperties.MessageId);
    }

    [Fact]
    public async Task Oversized_delivery_is_retained_without_allocating_a_work_submission()
    {
        var queue = await broker.QueueAsync();
        var id = Guid.NewGuid();
        var scope = BrokerFixture.Scope();
        await broker.RawAsync(queue, id.ToString("D"), new byte[1024 * 1024 + 1]);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await Assert.ThrowsAsync<InvalidOperationException>(() => new RabbitMqIngress(broker.Connection, broker.Store, scope,
            BrokerFixture.Policy, queue).RunAsync(timeout.Token));
        Assert.Null(await broker.FindByMessageAsync(scope, id));
        var pending = await broker.Admin.BasicGetAsync(queue, true);
        Assert.NotNull(pending);
        Assert.Equal(1024 * 1024 + 1, pending.Body.Length);
    }

    [Fact]
    public async Task Lost_consumer_ack_redelivers_the_already_durable_message_as_a_duplicate()
    {
        var queue = await broker.QueueAsync();
        var scope = BrokerFixture.Scope();
        var id = Guid.NewGuid();
        await broker.RawAsync(queue, id.ToString("D"), [1]);
        await using var proxy = new AmqpAckDropProxy(publisherConfirm: false);
        await using var connection = await RabbitMqConnection.OpenAsync(BrokerFixture.Factory(proxy.Port));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var run = new RabbitMqIngress(connection, broker.Store, scope, BrokerFixture.Policy, queue).RunAsync(timeout.Token);
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.ThrowsAnyAsync<Exception>(() => run);
        Assert.NotNull(await broker.FindByMessageAsync(scope, id));
        var recording = new RecordingStore(broker.Store);
        await broker.DrainAsync(queue, scope, recording);
        Assert.Equal(new[] { AcceptanceStatus.Duplicate }, recording.Outcomes);
    }

    [Fact]
    public async Task Process_kill_after_database_acceptance_before_ack_recovers_without_second_work()
    {
        var queue = await broker.QueueAsync();
        var scope = BrokerFixture.Scope();
        var id = Guid.NewGuid();
        await broker.RawAsync(queue, id.ToString("D"), [1]);
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (root is not null && !File.Exists(Path.Combine(root.FullName, "global.json"))) root = root.Parent;
        Assert.NotNull(root);
        var output = new DirectoryInfo(AppContext.BaseDirectory);
        var worker = Path.Combine(root.FullName, "tests", "EventGuard.RabbitMq.CrashWorker", "bin", output.Parent!.Name, output.Name, "EventGuard.RabbitMq.CrashWorker.dll");
        Assert.True(File.Exists(worker));
        var dotnetRoot = Environment.GetEnvironmentVariable("DOTNET_ROOT");
        var start = new ProcessStartInfo(string.IsNullOrEmpty(dotnetRoot) ? "dotnet" : Path.Combine(dotnetRoot, "dotnet"))
        { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(worker);
        start.Environment["EVENTGUARD_TEST_CONNECTION"] = broker.DbConnectionString;
        start.Environment["EVENTGUARD_TEST_SCHEMA"] = broker.Schema;
        start.Environment["EVENTGUARD_TEST_CONSUMER"] = scope.Consumer;
        start.Environment["EVENTGUARD_TEST_QUEUE"] = queue;
        using var process = Process.Start(start)!;
        try
        {
            var line = await process.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(15));
            Assert.NotNull(line);
            using var signal = JsonDocument.Parse(line);
            Assert.Equal("durable-before-ack", signal.RootElement.GetProperty("stage").GetString());
            var before = await broker.FindByMessageAsync(scope, id);
            Assert.NotNull(before);
            Assert.Equal(before.Id, signal.RootElement.GetProperty("workId").GetGuid());
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
            var recording = new RecordingStore(broker.Store);
            await broker.DrainAsync(queue, scope, recording);
            Assert.Equal(new[] { AcceptanceStatus.Duplicate }, recording.Outcomes);
            var claim = await broker.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10));
            Assert.Equal(before.Id, claim!.Work.Id);
            Assert.Equal(ExecutionStatus.Completed, (await broker.Store.ExecuteAsync(claim, broker.Effect, new())).Status);
            Assert.Null(await broker.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
            await using var count = broker.DataSource.CreateCommand($"SELECT count(*) FROM {broker.Schema}.effects WHERE work_id=@id");
            count.Parameters.AddWithValue("id", before.Id);
            Assert.Equal(1L, (long)(await count.ExecuteScalarAsync())!);
        }
        finally
        {
            if (!process.HasExited) { process.Kill(entireProcessTree: true); await process.WaitForExitAsync(); }
        }
    }

    [Fact]
    public async Task Host_cancellation_during_database_lock_wait_returns_the_unaccepted_delivery()
    {
        var queue = await broker.QueueAsync();
        var scope = BrokerFixture.Scope();
        var id = Guid.NewGuid();
        await broker.RawAsync(queue, id.ToString("D"), [1]);
        await using var blocker = await broker.DataSource.OpenConnectionAsync();
        await using var transaction = await blocker.BeginTransactionAsync();
        await using (var block = new NpgsqlCommand($"LOCK TABLE {broker.Schema}.works IN ACCESS EXCLUSIVE MODE", blocker, transaction))
            await block.ExecuteNonQueryAsync();
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var ingress = new RabbitMqIngress(broker.Connection, broker.Store, scope, BrokerFixture.Policy, queue);
        var run = ingress.RunAsync(stop.Token);
        try
        {
            await using var waiting = broker.DataSource.CreateCommand("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation=CAST(@table AS regclass) AND NOT granted)");
            waiting.Parameters.AddWithValue("table", broker.Schema + ".works");
            while (!(bool)(await waiting.ExecuteScalarAsync(stop.Token))!) await Task.Delay(10, stop.Token);
        }
        finally { stop.Cancel(); await run.WaitAsync(TimeSpan.FromSeconds(5)); }
        await transaction.RollbackAsync();
        Assert.Null(await broker.FindByMessageAsync(scope, id));
        Assert.Equal(1u, (await broker.Admin.QueueDeclarePassiveAsync(queue)).MessageCount);
        await broker.DrainAsync(queue, scope);
    }

    [Fact]
    public async Task Broker_cancellation_is_observed_by_the_host()
    {
        var queue = await broker.QueueAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var run = new RabbitMqIngress(broker.Connection, broker.Store, BrokerFixture.Scope(), BrokerFixture.Policy, queue).RunAsync(timeout.Token);
        while ((await broker.Admin.QueueDeclarePassiveAsync(queue)).ConsumerCount == 0) await Task.Delay(10, timeout.Token);
        await broker.DeleteQueueAsync(queue);
        await Assert.ThrowsAsync<IOException>(() => run);
    }
}
