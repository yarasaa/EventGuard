using EventGuard.Runtime;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class ExternalNoticeTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private static readonly ExternalActionPolicy Policy = new(observationInterval: TimeSpan.FromMilliseconds(10));
    private async Task<(ExternalAction Action, ExternalResultTarget Target)> Setup(bool accepted = true)
    {
        await db.Store.InitializeExternalActionsAsync();
        var a = new ExternalAction(new("tenant", Guid.NewGuid().ToString("N")), Guid.NewGuid(), "service", "v1", [1], TimeSpan.FromMinutes(1));
        var target = new ExternalResultTarget(a.Scope, "status", "v1");
        await db.Outbox.ProduceAsync(new(a.Scope, a.Id, ContentFingerprint.FromBytes([1])), async (session, ct) => { await session.ExternalActions.EnqueueAsync(a, target, ct); });
        if (accepted) await Accept(a); return (a, target);
    }
    private async Task Accept(ExternalAction a)
    {
        var worker = new ExternalActionWorker(db.Store, a.Scope, new CallbackService(), ExternalPhase.Submit, Policy);
        Assert.Equal(ExternalActionState.Accepted, (await worker.RunOnceAsync())!.Record.State);
    }
    private static ExternalObservationNotice Notice(ExternalAction a, ExternalResultKind kind = ExternalResultKind.Succeeded, long? revision = 18) =>
        new(a.Scope, Guid.NewGuid(), a.Id, a.Binding, a.BindingVersion, new(kind, "provider-report", "remote-1", [7]), revision);
    private Task<ExternalNoticeReceipt?> Apply(ExternalAction a) => db.Store.ApplyNextNoticeAsync(a.Scope, a.Binding, a.BindingVersion, Policy);
    private async Task Due(ExternalAction a, bool deadline = false)
    {
        await using var c = await db.DataSource.OpenConnectionAsync();
        await using var sql = new SqlCommand(deadline ? $"UPDATE {db.Schema}.external_actions SET observation_deadline=@past WHERE scope_id=@scope AND id=@id" :
            $"UPDATE {db.Schema}.external_notices SET next_at=@past WHERE scope_id=@scope AND action_id=@id", c);
        sql.Parameters.AddWithValue("past", DateTimeOffset.UtcNow.AddMinutes(-1)); sql.Parameters.AddWithValue("scope", a.Scope.Id); sql.Parameters.AddWithValue("id", a.Id); await sql.ExecuteNonQueryAsync();
    }
    [Fact]
    public async Task Early_report_waits_for_acceptance_then_updates_application_status_without_resend()
    {
        var (a, target) = await Setup(false); var notice = Notice(a);
        Assert.Equal(ExternalNoticeStatus.Received, (await db.Store.RecordNoticeAsync(notice)).Status);
        var waiting = (await Apply(a))!; Assert.Equal("awaiting-acceptance", waiting.Reason);
        await Accept(a); await Due(a); Assert.Equal(ExternalNoticeStatus.Applied, (await Apply(a))!.Status);
        Assert.Equal(ExternalNoticeStatus.Applied, (await db.Store.RecordNoticeAsync(notice)).Status);
        var action = (await db.Store.FindActionAsync(a.Scope, a.Id))!; Assert.Equal(ExternalObservationState.Succeeded, action.Observation); Assert.Equal(1, action.SubmissionAttempts);
        var seen = new List<ExternalObservationState>();
        while (await db.Claim(target.WorkScope) is { } claim)
        {
            var result = await db.Store.ExecuteAsync(claim, async (_, _, session, ct) =>
            { await session.ExternalResults.ApplyAsync(target, (update, _) => { seen.Add(update.Observation); return ValueTask.CompletedTask; }, ct); }, new());
            Assert.Equal(ExecutionStatus.Completed, result.Status);
        }
        Assert.Equal(ExternalObservationState.Succeeded, seen[^1]); Assert.Null(await Apply(a));
    }
    [Fact]
    public async Task Provider_revisions_and_terminal_conflicts_are_durable_and_cannot_regress()
    {
        var (a, _) = await Setup(); var latest = Notice(a, ExternalResultKind.Pending, 18);
        await db.Store.RecordNoticeAsync(latest); Assert.Equal(ExternalNoticeStatus.Applied, (await Apply(a))!.Status);
        await db.Store.RecordNoticeAsync(Notice(a, ExternalResultKind.Failed, 17)); Assert.Equal(ExternalNoticeStatus.Ignored, (await Apply(a))!.Status);
        await db.Store.RecordNoticeAsync(Notice(a, ExternalResultKind.Failed, 18)); Assert.Equal(ExternalNoticeStatus.Conflict, (await Apply(a))!.Status);
        await db.Store.RecordNoticeAsync(Notice(a, ExternalResultKind.Failed, null)); Assert.Equal("provider-revision-required", (await Apply(a))!.Reason);
        var terminal = Notice(a, ExternalResultKind.Succeeded, 19); await db.Store.RecordNoticeAsync(terminal); await Apply(a);
        await db.Store.RecordNoticeAsync(Notice(a, ExternalResultKind.Pending, 20)); Assert.Equal(ExternalNoticeStatus.Ignored, (await Apply(a))!.Status);
        var conflicting = Notice(a, ExternalResultKind.Failed, 21); await db.Store.RecordNoticeAsync(conflicting);
        Assert.Equal("conflicting-terminal-report", (await Apply(a))!.Reason);
        Assert.Equal(ExternalNoticeStatus.Conflict, (await db.Store.FindNoticeAsync(a.Scope, conflicting.Id))!.Status);
        Assert.Equal(ExternalObservationState.Succeeded, (await db.Store.FindActionAsync(a.Scope, a.Id))!.Observation);
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Store.RecordNoticeAsync(new(a.Scope, terminal.Id, a.Id, a.Binding, a.BindingVersion, new(ExternalResultKind.Failed, "changed"), 19)));
    }
    [Fact]
    public async Task Late_final_callback_can_resolve_a_report_timeout_without_sending_again()
    {
        var (a, _) = await Setup(); await Due(a, true);
        Assert.Equal(1, await db.Store.ExpireObservationsAsync(a.Scope, a.Binding, a.BindingVersion));
        Assert.Equal(ExternalObservationState.TimedOut, (await db.Store.FindActionAsync(a.Scope, a.Id))!.Observation);
        await db.Store.RecordNoticeAsync(Notice(a, ExternalResultKind.Pending, null)); Assert.Equal(ExternalNoticeStatus.Ignored, (await Apply(a))!.Status);
        await db.Store.RecordNoticeAsync(Notice(a, ExternalResultKind.Succeeded, null)); Assert.Equal(ExternalNoticeStatus.Applied, (await Apply(a))!.Status);
        var saved = (await db.Store.FindActionAsync(a.Scope, a.Id))!; Assert.Equal(ExternalObservationState.Succeeded, saved.Observation); Assert.Equal(1, saved.SubmissionAttempts);
    }
    [Fact]
    public async Task Foreign_scope_binding_and_reference_cannot_update_the_action()
    {
        var (a, _) = await Setup();
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Store.RecordNoticeAsync(new(new("other", a.Scope.Producer), Guid.NewGuid(), a.Id, a.Binding, a.BindingVersion, new(ExternalResultKind.Succeeded, "report"))));
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Store.RecordNoticeAsync(new(a.Scope, Guid.NewGuid(), a.Id, "wrong", a.BindingVersion, new(ExternalResultKind.Succeeded, "report"))));
        await db.Store.RecordNoticeAsync(new(a.Scope, Guid.NewGuid(), a.Id, a.Binding, a.BindingVersion, new(ExternalResultKind.Succeeded, "report", "different-reference")));
        Assert.Equal("provider-reference-mismatch", (await Apply(a))!.Reason);
        Assert.Equal(ExternalObservationState.Pending, (await db.Store.FindActionAsync(a.Scope, a.Id))!.Observation);
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Lost_notice_commit_reply_preserves_one_receipt_and_transition(bool applying)
    {
        var (a, _) = await Setup(); var notice = Notice(a);
        if (applying) await db.Store.RecordNoticeAsync(notice);
        await using var proxy = new CommitReplyDropProxy();
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false, Pooling = false };
        var source = new SqlServerDataSource(settings.ConnectionString);
        var interrupted = new SqlServerWorkStore(source, db.Schema);
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(async () =>
        {
            if (applying) await interrupted.ApplyNextNoticeAsync(a.Scope, a.Binding, a.BindingVersion, Policy);
            else await interrupted.RecordNoticeAsync(notice);
        });
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var receipt = await db.Store.RecordNoticeAsync(notice);
        Assert.Equal(applying ? ExternalNoticeStatus.Applied : ExternalNoticeStatus.Received, receipt.Status);
        if (!applying) await Apply(a); Assert.Null(await Apply(a));
        Assert.Equal(ExternalObservationState.Succeeded, (await db.Store.FindActionAsync(a.Scope, a.Id))!.Observation);
    }
    private sealed class CallbackService : ICallbackExternalServiceAdapter
    {
        public string Binding => "service"; public string BindingVersion => "v1"; public bool CanReconcile => false; public bool CanObserve => false;
        public Task<ExternalResult> SubmitAsync(ExternalAction action, CancellationToken cancellationToken) => Task.FromResult(new ExternalResult(ExternalResultKind.Accepted, "accepted", "remote-1"));
        public Task<ExternalResult> ReconcileAsync(ExternalActionRecord action, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<ExternalResult> ObserveAsync(ExternalActionRecord action, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
