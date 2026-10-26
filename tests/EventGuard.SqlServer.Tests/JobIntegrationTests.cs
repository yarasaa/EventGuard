using Microsoft.Data.SqlClient;
using EventGuard.Runtime;

namespace EventGuard.SqlServer.Tests;

public sealed class JobIntegrationTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private async Task<JobDefinition> Definition()
    {
        await db.Store.InitializeJobsAsync();
        var definition = new JobDefinition(new("tenant", Guid.NewGuid().ToString("N")), "rebuild", 1, "query", 1, "handler-v1", [1]);
        await db.Store.RegisterAsync(definition); return definition;
    }

    [Fact]
    public async Task Lost_commit_reply_resolves_receipt_and_never_creates_another_work_item()
    {
        var definition = await Definition();
        var request = new JobSubmission(definition, Guid.NewGuid(), "business", "operator", [1]);
        await using var proxy = new CommitReplyDropProxy();
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false, Pooling = false };
        var source = new SqlServerDataSource(settings.ConnectionString);
        var interrupted = new SqlServerWorkStore(source, db.Schema);
        var error = await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.SubmitAsync(request));
        Assert.Equal(request.RequestId, error.OperationId);
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var receipt = await db.Store.FindRequestAsync(definition.Scope, request.RequestId);
        Assert.NotNull(receipt); Assert.Equal(JobSubmissionStatus.Accepted, receipt.Status);
        var retried = await db.Store.SubmitAsync(request);
        Assert.Equal(JobSubmissionStatus.AlreadyAccepted, retried.Status); Assert.Equal(receipt.OperationId, retried.OperationId);
        var claim = await db.Claim(definition.WorkScope); Assert.NotNull(claim);
        await db.Store.ExecuteAsync(claim, db.Effect, new());
        Assert.Equal(1, await db.EffectCount(receipt.OperationId)); Assert.Null(await db.Claim(definition.WorkScope));
    }

    [Fact]
    public async Task Receipt_failure_rolls_back_operation_and_executable_work()
    {
        var definition = await Definition();
        var request = new JobSubmission(definition, Guid.NewGuid(), "business", "operator", [1]);
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new SqlCommand($"ALTER TABLE {db.Schema}.job_requests ADD CONSTRAINT reject_job_receipt CHECK(id <> '{request.RequestId:D}')", connection);
        await command.ExecuteNonQueryAsync();
        try { await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(() => db.Store.SubmitAsync(request)); }
        finally { command.CommandText = $"ALTER TABLE {db.Schema}.job_requests DROP CONSTRAINT reject_job_receipt"; await command.ExecuteNonQueryAsync(); }
        Assert.Null(await db.Store.FindRequestAsync(definition.Scope, request.RequestId));
        Assert.Null(await db.Claim(definition.WorkScope));
        Assert.Equal(JobSubmissionStatus.Accepted, (await db.Store.SubmitAsync(request)).Status);
    }

    [Fact]
    public async Task Typed_worker_commits_business_effect_and_command_progress_together()
    {
        var definition = await Definition();
        var contract = new JsonMessageContract<Query>("query", 1, q => q.ContractVersion,
            q => { if (q.Limit < 1) throw new IngressMessageRejectedException("invalid-limit"); });
        var client = new JsonJobClient<Query>(db.Store, definition, contract);
        var requestId = Guid.NewGuid();
        await Assert.ThrowsAsync<IngressMessageRejectedException>(() => client.SubmitAsync(requestId, "business", "operator", new(1, 0)));
        Assert.Null(await client.FindRequestAsync(requestId));
        var result = await client.SubmitAsync(requestId, "business", "operator", new(1, 25));
        var worker = new JsonJobWorker<Query, SqlServerSession>(db.Store, definition, contract,
            async (query, work, session, ct) =>
            {
                Assert.Equal(25, query.Limit); await db.Effect(work, ReadOnlyMemory<byte>.Empty, session, ct);
            }, options: new(maximumConcurrency: 1, idleDelay: TimeSpan.FromMilliseconds(20)));
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var running = worker.RunAsync(stop.Token);
        try
        {
            while ((await client.FindAsync(result.OperationId, stop.Token))!.State != WorkState.Completed)
            {
                if (running.IsFaulted) await running;
                await Task.Delay(20, stop.Token);
            }
        }
        finally { stop.Cancel(); await running; }
        Assert.Equal(1, await db.EffectCount(result.OperationId));
    }

    [Fact]
    public async Task Changed_definition_worker_fails_before_claiming_saved_work()
    {
        var definition = await Definition();
        var request = new JobSubmission(definition, Guid.NewGuid(), "business", "operator", [1]);
        var accepted = await db.Store.SubmitAsync(request);
        var changed = new JobDefinition(definition.Scope, definition.Name, 1, "query", 1, "different-handler", [1]);
        var contract = new JsonMessageContract<Query>("query", 1, q => q.ContractVersion, _ => { });
        var worker = new JsonJobWorker<Query, SqlServerSession>(db.Store, changed, contract,
            (_, _, _, _) => throw new InvalidOperationException("Must not execute"));
        await Assert.ThrowsAsync<JobDefinitionConflictException>(() => worker.RunAsync());
        Assert.Equal(0, (await db.Store.FindAsync(definition.Scope, accepted.OperationId))!.AttemptCount);
    }

    public sealed record Query(int ContractVersion, int Limit);
}
