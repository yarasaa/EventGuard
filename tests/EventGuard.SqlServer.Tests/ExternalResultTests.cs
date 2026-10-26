using EventGuard.Runtime;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class ExternalResultTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private static readonly ExternalActionPolicy Policy = new(retryDelay: TimeSpan.FromMilliseconds(10), observationInterval: TimeSpan.FromMilliseconds(10));
    private static readonly RetryPolicy Retry = new(initialDelay: TimeSpan.FromMilliseconds(1), jitterRatio: 0);
    private async Task<(ExternalAction Action, ExternalResultTarget Target)> Setup(bool observe = false)
    {
        await db.Store.InitializeExternalActionsAsync();
        var scope = new ProducerScope("tenant", Guid.NewGuid().ToString("N"));
        var action = new ExternalAction(scope, Guid.NewGuid(), "service", "v1", "same-phone-application-payload"u8,
            observe ? TimeSpan.FromMinutes(1) : null);
        var target = new ExternalResultTarget(scope, "application-status", "v1");
        await db.Outbox.ProduceAsync(new(scope, action.Id, ContentFingerprint.FromBytes(action.CopyPayload())), async (session, ct) =>
        {
            Assert.Equal(OutboxEnqueueStatus.Enqueued, await session.ExternalActions.EnqueueAsync(action, target, ct));
            Assert.Equal(OutboxEnqueueStatus.AlreadyEnqueued, await session.ExternalActions.EnqueueAsync(action, target, ct));
        });
        return (action, target);
    }
    private Task<ExternalActionClaim?> Claim(ExternalAction action, ExternalPhase phase = ExternalPhase.Submit) =>
        db.Store.ClaimActionAsync(action.Scope, action.Binding, action.BindingVersion, phase, Guid.NewGuid(), TimeSpan.FromSeconds(30));
    private async Task Due(ExternalAction a, bool expireLease = false, bool expireObservation = false)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new SqlCommand($"UPDATE {db.Schema}.external_actions SET next_at=@past" +
            (expireLease ? ",lease_until=@past" : "") + (expireObservation ? ",observation_deadline=@past" : "") + " WHERE scope_id=@scope AND id=@id", connection);
        command.Parameters.AddWithValue("past", DateTimeOffset.UtcNow.AddMinutes(-1)); command.Parameters.AddWithValue("scope", a.Scope.Id); command.Parameters.AddWithValue("id", a.Id);
        await command.ExecuteNonQueryAsync();
    }
    private async Task Write(ExternalActionUpdate update, SqlServerSession session, CancellationToken ct)
    {
        await using var command = session.CreateCommand($"INSERT INTO {db.Schema}.business_effects VALUES(@id,@status)");
        command.Parameters.AddWithValue("id", update.Action.Id); command.Parameters.AddWithValue("status", update.Observation == ExternalObservationState.Succeeded ? "delivered" : update.State.ToString());
        await command.ExecuteNonQueryAsync(ct);
    }
    private Task<ExecutionResult> Apply(WorkClaim claim, ExternalResultTarget target, bool fail = false) =>
        db.Store.ExecuteAsync(claim, async (_, _, session, ct) =>
        {
            await session.ExternalResults.ApplyAsync(target, async (update, token) =>
            {
                Assert.Equal("same-phone-application-payload"u8.ToArray(), update.Action.CopyPayload());
                await Write(update, session, token);
                if (fail) throw new IOException("status database write failed");
            }, ct);
        }, Retry);

    [Fact]
    public async Task Application_update_retries_after_restart_without_resending_the_accepted_action()
    {
        var (a, target) = await Setup(); var service = new Service();
        var sender = new ExternalActionWorker(db.Store, a.Scope, service, ExternalPhase.Submit, Policy);
        Assert.Equal(ExternalActionState.Accepted, (await sender.RunOnceAsync())!.Record.State);
        var resultClaim = (await db.Claim(target.WorkScope))!;
        Assert.Equal(ExecutionStatus.RetryScheduled, (await Apply(resultClaim, target, fail: true)).Status);
        Assert.Equal(0, await db.EffectCount(a.Id)); Assert.Null(await sender.RunOnceAsync());
        var restarted = new SqlServerWorkStore(db.DataSource, db.Schema);
        var worker = new ExternalResultWorker<SqlServerSession>(restarted, target,
            async (update, session, ct) => { Assert.Equal("remote-1", update.ProviderReference); await Write(update, session, ct); }, Retry,
            new(maximumConcurrency: 2, idleDelay: TimeSpan.FromMilliseconds(10)));
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(15)); var running = worker.RunAsync(stop.Token);
        try
        {
            while (await db.EffectCount(a.Id) != 1) { if (running.IsFaulted) await running; await Task.Delay(10, stop.Token); }
        }
        finally { stop.Cancel(); await running; }
        Assert.Equal(1, service.Submits); Assert.Equal(1, await db.EffectCount(a.Id)); Assert.Null(await sender.RunOnceAsync());
    }

    [Fact]
    public async Task Late_older_result_cannot_regress_a_delivered_application_status()
    {
        var (a, target) = await Setup(observe: true);
        var accepted = await db.Store.SettleActionAsync((await Claim(a))!, new(ExternalResultKind.Accepted, "accepted", "remote-1"), Policy);
        var older = (await db.Claim(target.WorkScope))!;
        Assert.Equal(ExecutionStatus.RetryScheduled, (await Apply(older, target, fail: true)).Status);
        await Due(a);
        var delivered = await db.Store.SettleActionAsync((await Claim(a, ExternalPhase.Observe))!, new(ExternalResultKind.Succeeded, "delivered", data: [7]), Policy);
        // Hide the old retry temporarily to deliberately process the newest revision first.
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var postpone = new SqlCommand($"UPDATE {db.Schema}.works SET next_attempt_at=@due WHERE id=@id", connection);
        postpone.Parameters.AddWithValue("id", older.Work.Id); postpone.Parameters.AddWithValue("due", DateTimeOffset.UtcNow.AddMinutes(1)); await postpone.ExecuteNonQueryAsync();
        Assert.Equal(ExecutionStatus.Completed, (await Apply((await db.Claim(target.WorkScope))!, target)).Status);
        postpone.Parameters["due"].Value = DateTimeOffset.UtcNow.AddMinutes(-1); await postpone.ExecuteNonQueryAsync();
        var repeated = (await db.Claim(target.WorkScope))!;
        Assert.Equal(ExecutionStatus.Completed, (await Apply(repeated, target)).Status);
        Assert.Equal(1, await db.EffectCount(a.Id)); Assert.True(delivered.Record.Version > accepted.Record.Version);
        await using var status = new SqlCommand($"SELECT note FROM {db.Schema}.business_effects WHERE work_id=@id", connection);
        status.Parameters.AddWithValue("id", a.Id); Assert.Equal("delivered", await status.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Concurrent_result_workers_preserve_monotonic_projection()
    {
        var (a, target) = await Setup(observe: true);
        await db.Store.SettleActionAsync((await Claim(a))!, new(ExternalResultKind.Accepted, "accepted"), Policy);
        await Due(a); var final = await db.Store.SettleActionAsync((await Claim(a, ExternalPhase.Observe))!, new(ExternalResultKind.Succeeded, "delivered"), Policy);
        var first = (await db.Claim(target.WorkScope))!; var second = (await db.Claim(target.WorkScope))!;
        Assert.All(await Task.WhenAll(Apply(first, target), Apply(second, target)), r => Assert.Equal(ExecutionStatus.Completed, r.Status));
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var position = new SqlCommand($"SELECT version FROM {db.Schema}.external_result_positions WHERE scope_id=@scope AND action_id=@id", connection);
        position.Parameters.AddWithValue("scope", a.Scope.Id); position.Parameters.AddWithValue("id", a.Id);
        Assert.Equal(final.Record.Version, (long)(await position.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task Failed_notification_insert_rolls_back_settlement_and_work_then_retries_once()
    {
        var (a, target) = await Setup(); var claim = (await Claim(a))!;
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new SqlCommand($"ALTER TABLE {db.Schema}.external_result_updates ADD CONSTRAINT reject_result CHECK(action_id <> '{a.Id:D}')", connection);
        await command.ExecuteNonQueryAsync();
        try { await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(() => db.Store.SettleActionAsync(claim, new(ExternalResultKind.Accepted, "ok"), Policy)); }
        finally { command.CommandText = $"ALTER TABLE {db.Schema}.external_result_updates DROP CONSTRAINT reject_result"; await command.ExecuteNonQueryAsync(); }
        Assert.Equal(ExternalActionState.Sending, (await db.Store.FindActionAsync(a.Scope, a.Id))!.State);
        Assert.Null(await db.Claim(target.WorkScope)); Assert.Null(Assert.Single(await db.Store.ReadActionEvidenceAsync(a.Scope, a.Id)).Result);
        await db.Store.SettleActionAsync(claim, new(ExternalResultKind.Accepted, "ok"), Policy);
        Assert.Equal(ExecutionStatus.Completed, (await Apply((await db.Claim(target.WorkScope))!, target)).Status);
        await db.Store.SettleActionAsync(claim, new(ExternalResultKind.Accepted, "ok"), Policy);
        Assert.Null(await db.Claim(target.WorkScope)); Assert.Equal(1, await db.EffectCount(a.Id));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Recovery_and_observation_timeout_also_publish_durable_status(bool observation)
    {
        var (a, target) = await Setup(observe: observation); var claim = (await Claim(a))!;
        if (observation)
        {
            await db.Store.SettleActionAsync(claim, new(ExternalResultKind.Accepted, "accepted"), Policy);
            await Apply((await db.Claim(target.WorkScope))!, target);
            await Due(a, expireObservation: true); Assert.Null(await Claim(a, ExternalPhase.Observe));
        }
        else { await Due(a, expireLease: true); Assert.Null(await Claim(a)); }
        var queued = (await db.Claim(target.WorkScope))!;
        var applied = await db.Store.ExecuteAsync(queued, async (_, _, session, ct) =>
        {
            await session.ExternalResults.ApplyAsync(target, (update, _) =>
            {
                Assert.Null(update.Response);
                if (observation) { Assert.Equal(ExternalActionState.Accepted, update.State); Assert.Equal(ExternalObservationState.TimedOut, update.Observation); }
                else Assert.Equal(ExternalActionState.Unknown, update.State);
                return ValueTask.CompletedTask;
            }, ct);
        }, Retry);
        Assert.Equal(ExecutionStatus.Completed, applied.Status); Assert.Null(await Claim(a));
    }

    [Fact]
    public async Task Result_route_is_immutable_and_scope_checked()
    {
        await db.Store.InitializeExternalActionsAsync();
        var scope = new ProducerScope("tenant", Guid.NewGuid().ToString("N"));
        var a = new ExternalAction(scope, Guid.NewGuid(), "service", "v1", [1]); var target = new ExternalResultTarget(scope, "status", "v1");
        await Assert.ThrowsAsync<ExternalActionConflictException>(() => db.Outbox.ProduceAsync(new(scope, a.Id, ContentFingerprint.FromBytes([1])), async (session, ct) =>
        {
            await session.ExternalActions.EnqueueAsync(a, target, ct);
            await session.ExternalActions.EnqueueAsync(a, new ExternalResultTarget(scope, "status", "v2"), ct);
        }));
        Assert.Null(await db.Store.FindActionAsync(scope, a.Id));
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Outbox.ProduceAsync(new(scope, a.Id, ContentFingerprint.FromBytes([1])), async (session, ct) =>
        { await session.ExternalActions.EnqueueAsync(a, new ExternalResultTarget(new("other", "app"), "status", "v1"), ct); }));
    }

    [Fact]
    public async Task Wrong_handler_and_modified_result_evidence_cannot_update_application_rows()
    {
        var (a, target) = await Setup(); await db.Store.SettleActionAsync((await Claim(a))!, new(ExternalResultKind.Accepted, "accepted"), Policy);
        var claim = (await db.Claim(target.WorkScope))!;
        var wrong = new ExternalResultTarget(target.Scope, target.Name, "different");
        Assert.Equal(ExecutionStatus.RetryScheduled, (await Apply(claim, wrong)).Status); Assert.Equal(0, await db.EffectCount(a.Id));
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var corrupt = new SqlCommand($"UPDATE {db.Schema}.external_result_updates SET reason='corrupt' WHERE action_id=@id", connection);
        corrupt.Parameters.AddWithValue("id", a.Id); await corrupt.ExecuteNonQueryAsync(); await Task.Delay(20);
        Assert.Equal(ExecutionStatus.RetryScheduled, (await Apply((await db.Claim(target.WorkScope))!, target)).Status);
        Assert.Equal(0, await db.EffectCount(a.Id));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Lost_commit_reply_keeps_one_result_work_and_one_application_update(bool applying)
    {
        var (a, target) = await Setup(); var claim = (await Claim(a))!;
        var response = new ExternalResult(ExternalResultKind.Accepted, "accepted");
        WorkClaim? resultClaim = null;
        if (applying)
        {
            await db.Store.SettleActionAsync(claim, response, Policy);
            resultClaim = (await db.Claim(target.WorkScope))!;
        }
        await using var proxy = new CommitReplyDropProxy();
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false, Pooling = false };
        var source = new SqlServerDataSource(settings.ConnectionString);
        var interrupted = new SqlServerWorkStore(source, db.Schema);
        var error = await Assert.ThrowsAsync<CommitOutcomeUnknownException>(async () =>
        {
            if (applying) await interrupted.ExecuteAsync(resultClaim!, async (_, _, session, ct) =>
            { await session.ExternalResults.ApplyAsync(target, (update, token) => new ValueTask(Write(update, session, token)), ct); }, Retry);
            else await interrupted.SettleActionAsync(claim, response, Policy);
        });
        Assert.Equal(applying ? resultClaim!.Token : claim.Token, error.OperationId);
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        if (applying)
        {
            Assert.Equal(WorkState.Completed, (await db.Store.FindAsync(target.WorkScope, resultClaim!.Work.Id))!.State);
            Assert.Equal(ExecutionStatus.LeaseLost, (await Apply(resultClaim, target)).Status);
        }
        else
        {
            Assert.True((await db.Store.SettleActionAsync(claim, response, Policy)).Recorded);
            Assert.Equal(ExecutionStatus.Completed, (await Apply((await db.Claim(target.WorkScope))!, target)).Status);
        }
        Assert.Null(await db.Claim(target.WorkScope)); Assert.Equal(1, await db.EffectCount(a.Id)); Assert.Null(await Claim(a));
    }

    [Fact]
    public async Task Dead_lettered_result_can_replay_business_sql_and_outbox_atomically_without_resend()
    {
        var (a, target) = await Setup();
        await db.Store.SettleActionAsync((await Claim(a))!, new(ExternalResultKind.Accepted, "accepted"), Policy);
        var claim = (await db.Claim(target.WorkScope))!;
        var message = new OutgoingMessage(a.Scope, Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "status.changed"), [1]);
        Task<ExecutionResult> Execute(WorkClaim work, bool fail) => db.Store.ExecuteAsync(work, async (_, _, session, ct) =>
        {
            await session.ExternalResults.ApplyAsync(target, async (update, token) =>
            {
                await Write(update, session, token);
                await session.Outbox.EnqueueAsync(message, token);
                if (fail) throw new IOException("status transaction failed after outbox insert");
            }, ct);
        }, new(maximumAttempts: 1));
        var failed = await Execute(claim, true);
        Assert.Equal(WorkState.DeadLetter, failed.Work.State);
        Assert.Equal(0, await db.EffectCount(a.Id)); Assert.Null(await db.Outbox.FindAsync(a.Scope, message.IntentId));
        var replay = new ReplayRequest(target.WorkScope, Guid.NewGuid(), failed.Work.Id, failed.Work.Version, "operator", "fixed mapping");
        Assert.Equal(ReplayStatus.Accepted, (await db.Store.ReplayAsync(replay)).Status);
        Assert.Equal(ExecutionStatus.Completed, (await Execute((await db.Claim(target.WorkScope))!, false)).Status);
        Assert.Equal(1, await db.EffectCount(a.Id)); Assert.Equal(OutboxState.Pending, (await db.Outbox.FindAsync(a.Scope, message.IntentId))!.State);
        Assert.Null(await Claim(a)); Assert.Null(await db.Claim(target.WorkScope));
    }

    private sealed class Service : IExternalServiceAdapter
    {
        public int Submits;
        public string Binding => "service"; public string BindingVersion => "v1";
        public bool CanReconcile => false; public bool CanObserve => false;
        public Task<ExternalResult> SubmitAsync(ExternalAction action, CancellationToken ct)
        { Submits++; return Task.FromResult(new ExternalResult(ExternalResultKind.Accepted, "accepted", "remote-1", [1])); }
        public Task<ExternalResult> ReconcileAsync(ExternalActionRecord action, CancellationToken ct) => throw new NotSupportedException();
        public Task<ExternalResult> ObserveAsync(ExternalActionRecord action, CancellationToken ct) => throw new NotSupportedException();
    }
}
