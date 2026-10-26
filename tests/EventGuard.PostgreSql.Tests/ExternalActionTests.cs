using EventGuard.Runtime;
using Npgsql;

namespace EventGuard.PostgreSql.Tests;

public sealed class ExternalActionTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private static readonly ExternalActionPolicy Policy = new(2, TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(10));
    private async Task<ExternalAction> Action(TimeSpan? observation = null)
    {
        await db.Store.InitializeExternalActionsAsync();
        await db.Store.InitializeExternalActionsAsync();
        return new(new("tenant", Guid.NewGuid().ToString("N")), Guid.NewGuid(), "service", "v1", [1,2], observation);
    }
    private Task<ProducerResult> Save(ExternalAction action, bool fail = false) => db.Outbox.ProduceAsync(
        new(action.Scope, action.Id, ContentFingerprint.FromBytes(action.CopyPayload())), async (session, ct) =>
        {
            await using var effect = session.CreateCommand($"INSERT INTO {db.Schema}.business_effects VALUES(@id,'external')");
            effect.Parameters.AddWithValue("id", action.Id); await effect.ExecuteNonQueryAsync(ct);
            Assert.Equal(OutboxEnqueueStatus.Enqueued, await session.ExternalActions.EnqueueAsync(action, ct));
            Assert.Equal(OutboxEnqueueStatus.AlreadyEnqueued, await session.ExternalActions.EnqueueAsync(action, ct));
            if (fail) throw new InvalidOperationException("injected failure");
        });
    private Task<ExternalActionClaim?> Claim(ExternalAction a, ExternalPhase phase = ExternalPhase.Submit, Guid? token = null, int leaseMs = 10000) =>
        db.Store.ClaimActionAsync(a.Scope, a.Binding, a.BindingVersion, phase, token ?? Guid.NewGuid(), TimeSpan.FromMilliseconds(leaseMs));
    private async Task Expire(ExternalAction a, bool deadline = false)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"UPDATE {db.Schema}.external_actions SET next_at=@past" +
            (deadline ? ",observation_deadline=@past" : ",lease_until=CASE WHEN lease_token IS NULL THEN NULL ELSE @past END") + " WHERE scope_id=@scope AND id=@id", connection);
        command.Parameters.AddWithValue("past", DateTimeOffset.UtcNow.AddMinutes(-1));
        command.Parameters.AddWithValue("scope", a.Scope.Id); command.Parameters.AddWithValue("id", a.Id);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Intent_and_business_effect_commit_or_rollback_together()
    {
        var a = await Action(); await Assert.ThrowsAsync<InvalidOperationException>(() => Save(a, true));
        Assert.Null(await db.Store.FindActionAsync(a.Scope, a.Id)); Assert.Equal(0, await db.EffectCount(a.Id));
        await Save(a); Assert.Equal(ProducerStatus.AlreadyCommitted, (await Save(a)).Status);
        Assert.Equal(1, await db.EffectCount(a.Id)); Assert.Equal(ExternalActionState.Ready, (await db.Store.FindActionAsync(a.Scope, a.Id))!.State);
        await Assert.ThrowsAsync<ExternalActionConflictException>(() => db.Outbox.ProduceAsync(
            new(a.Scope, Guid.NewGuid(), ContentFingerprint.FromBytes([9])), async (s, ct) => { await s.ExternalActions.EnqueueAsync(a, ct); }));
    }

    [Fact]
    public async Task Changed_payload_and_cross_tenant_intents_are_rejected()
    {
        var a = await Action();
        await Assert.ThrowsAsync<ExternalActionConflictException>(() => db.Outbox.ProduceAsync(
            new(a.Scope, a.Id, ContentFingerprint.FromBytes([1])), async (s, ct) =>
            {
                await s.ExternalActions.EnqueueAsync(a, ct);
                await s.ExternalActions.EnqueueAsync(new(a.Scope, a.Id, a.Binding, a.BindingVersion, [9]), ct);
            }));
        Assert.Null(await db.Store.FindActionAsync(a.Scope, a.Id));
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Outbox.ProduceAsync(
            new(new("other", "producer"), a.Id, ContentFingerprint.FromBytes([1])), async (s, ct) => { await s.ExternalActions.EnqueueAsync(a, ct); }));
    }

    [Fact]
    public async Task Concurrent_claimers_and_repeated_claim_request_have_one_owner()
    {
        var a = await Action(); await Save(a);
        var claims = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Claim(a)));
        var claim = Assert.Single(claims, c => c is not null)!;
        Assert.Equal(claim.Token, (await Claim(a, token: claim.Token))!.Token);
        Assert.Equal(1, claim.Record.SubmissionAttempts);
        Assert.Null(await db.Store.FindActionAsync(new("other", a.Scope.Producer), a.Id));
        Assert.Null(await db.Store.ClaimActionAsync(a.Scope, "SERVICE", "v1", ExternalPhase.Submit, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Expired_sender_is_unknown_and_never_automatically_resent()
    {
        var a = await Action(); await Save(a); var claim = (await Claim(a))!;
        await Expire(a); Assert.Null(await Claim(a)); Assert.Null(await Claim(a));
        Assert.Equal(ExternalActionState.Unknown, (await db.Store.FindActionAsync(a.Scope, a.Id))!.State);
        var late = await db.Store.SettleActionAsync(claim, new(ExternalResultKind.Accepted, "accepted", "remote-1"), Policy);
        Assert.False(late.Recorded); Assert.Equal(ExternalActionState.Unknown, late.Record.State);
        var evidence = Assert.Single(await db.Store.ReadActionEvidenceAsync(a.Scope, a.Id));
        Assert.False(evidence.Applied); Assert.Equal("remote-1", evidence.Result!.Reference);
        var reconcile = (await Claim(a, ExternalPhase.Reconcile))!;
        var accepted = await db.Store.SettleActionAsync(reconcile, new(ExternalResultKind.Accepted, "found", "remote-1"), Policy);
        Assert.True(accepted.Recorded); Assert.Equal(1, accepted.Record.SubmissionAttempts);
        Assert.Null(await Claim(a));
    }

    [Fact]
    public async Task Worker_lost_response_reconciles_acceptance_then_observes_without_resending()
    {
        var a = await Action(TimeSpan.FromMinutes(1)); await Save(a); var service = new Service { LoseReply = true };
        var send = new ExternalActionWorker(db.Store, a.Scope, service, ExternalPhase.Submit, Policy);
        Assert.Equal(ExternalActionState.Unknown, (await send.RunOnceAsync())!.Record.State);
        Assert.Null(await send.RunOnceAsync()); await Expire(a);
        var reconcile = new ExternalActionWorker(db.Store, a.Scope, service, ExternalPhase.Reconcile, Policy);
        var accepted = (await reconcile.RunOnceAsync())!.Record;
        Assert.Equal(ExternalActionState.Accepted, accepted.State); Assert.Equal(ExternalObservationState.Pending, accepted.Observation);
        await Expire(a); var observe = new ExternalActionWorker(db.Store, a.Scope, service, ExternalPhase.Observe, Policy);
        Assert.Equal(ExternalObservationState.Pending, (await observe.RunOnceAsync())!.Record.Observation);
        service.Completed = true; await Expire(a);
        var completed = (await observe.RunOnceAsync())!.Record;
        Assert.Equal(ExternalObservationState.Succeeded, completed.Observation); Assert.Equal(ExternalActionState.Accepted, completed.State);
        Assert.Equal(1, service.Submits); Assert.Equal(1, service.Lookups); Assert.Equal(2, service.Observations);
        Assert.Null(await send.RunOnceAsync()); Assert.Null(await observe.RunOnceAsync());
    }

    [Fact]
    public async Task Only_explicit_not_accepted_evidence_permits_retry_and_attempt_limit_is_enforced()
    {
        var a = await Action(); await Save(a);
        var first = (await Claim(a))!;
        Assert.Equal(ExternalActionState.Ready, (await db.Store.SettleActionAsync(first, new(ExternalResultKind.NotAcceptedRetryable, "not-sent"), Policy)).Record.State);
        await Expire(a); var second = (await Claim(a))!;
        var exhausted = await db.Store.SettleActionAsync(second, new(ExternalResultKind.NotAcceptedRetryable, "not-sent"), Policy);
        Assert.Equal(ExternalActionState.Rejected, exhausted.Record.State); Assert.Equal("submission-attempts-exhausted", exhausted.Record.LastReason);
        Assert.Null(await Claim(a));
    }

    [Fact]
    public async Task Settlement_retry_is_idempotent_and_conflicting_evidence_is_rejected()
    {
        var a = await Action(); await Save(a); var claim = (await Claim(a))!;
        var result = new ExternalResult(ExternalResultKind.Accepted, "accepted", "remote-1", [7]);
        var first = await db.Store.SettleActionAsync(claim, result, Policy);
        var second = await db.Store.SettleActionAsync(claim, result, Policy);
        Assert.True(second.Recorded); Assert.Equal(first.Record.Version, second.Record.Version);
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Store.SettleActionAsync(claim, new(ExternalResultKind.Rejected, "rejected"), Policy));
        Assert.Single(await db.Store.ReadActionEvidenceAsync(a.Scope, a.Id));
    }

    [Fact]
    public async Task Observation_deadline_expires_without_changing_acceptance_or_sending_again()
    {
        var a = await Action(TimeSpan.FromSeconds(1)); await Save(a);
        await db.Store.SettleActionAsync((await Claim(a))!, new(ExternalResultKind.Accepted, "accepted"), Policy);
        await Expire(a, true); Assert.Null(await Claim(a, ExternalPhase.Observe));
        var record = (await db.Store.FindActionAsync(a.Scope, a.Id))!;
        Assert.Equal(ExternalActionState.Accepted, record.State); Assert.Equal(ExternalObservationState.TimedOut, record.Observation);
        Assert.Null(await Claim(a));
    }

    [Fact]
    public async Task Expired_observer_is_reclaimed_and_old_result_cannot_overwrite_new_owner()
    {
        var a = await Action(TimeSpan.FromMinutes(1)); await Save(a);
        await db.Store.SettleActionAsync((await Claim(a))!, new(ExternalResultKind.Accepted, "accepted"), Policy);
        await Expire(a); var old = (await Claim(a, ExternalPhase.Observe))!;
        await Expire(a); var current = (await Claim(a, ExternalPhase.Observe))!;
        Assert.False((await db.Store.SettleActionAsync(old, new(ExternalResultKind.Failed, "old-result"), Policy)).Recorded);
        var final = await db.Store.SettleActionAsync(current, new(ExternalResultKind.Succeeded, "done"), Policy);
        Assert.True(final.Recorded); Assert.Equal(ExternalObservationState.Succeeded, final.Record.Observation);
    }

    [Fact]
    public async Task Restart_preserves_unknown_state_and_claim_history()
    {
        var a = await Action(); await Save(a); var claim = (await Claim(a))!;
        await db.Store.SettleActionAsync(claim, new(ExternalResultKind.Unknown, "timeout"), Policy);
        var restarted = new PostgreSqlWorkStore(db.DataSource, db.Schema);
        Assert.Equal(ExternalActionState.Unknown, (await restarted.FindActionAsync(a.Scope, a.Id))!.State);
        Assert.Single(await restarted.ReadActionEvidenceAsync(a.Scope, a.Id));
        Assert.Null(await restarted.ClaimActionAsync(a.Scope, a.Binding, a.BindingVersion, ExternalPhase.Submit, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Missing_observation_capability_rejects_before_calling_service()
    {
        var a = await Action(TimeSpan.FromSeconds(1)); await Save(a); var service = new Service { ObserveSupported = false };
        var worker = new ExternalActionWorker(db.Store, a.Scope, service, ExternalPhase.Submit, Policy);
        Assert.Equal(ExternalActionState.Rejected, (await worker.RunOnceAsync())!.Record.State); Assert.Equal(0, service.Submits);
        Assert.Throws<NotSupportedException>(() => new ExternalActionWorker(db.Store, a.Scope, service, ExternalPhase.Observe));
    }

    private sealed class Service : IExternalServiceAdapter
    {
        public string Binding => "service"; public string BindingVersion => "v1";
        public bool CanReconcile => true; public bool CanObserve => ObserveSupported;
        public bool ObserveSupported { get; init; } = true;
        public bool LoseReply { get; init; } public bool Completed { get; set; }
        public int Submits, Lookups, Observations;
        public Task<ExternalResult> SubmitAsync(ExternalAction action, CancellationToken ct)
        {
            Submits++;
            if (LoseReply) throw new IOException("Provider accepted; reply was lost.");
            return Task.FromResult(new ExternalResult(ExternalResultKind.Accepted, "accepted", "remote-1"));
        }
        public Task<ExternalResult> ReconcileAsync(ExternalActionRecord action, CancellationToken ct)
        { Lookups++; return Task.FromResult(new ExternalResult(ExternalResultKind.Accepted, "found", "remote-1")); }
        public Task<ExternalResult> ObserveAsync(ExternalActionRecord action, CancellationToken ct)
        { Observations++; return Task.FromResult(new ExternalResult(Completed ? ExternalResultKind.Succeeded : ExternalResultKind.Pending, "observed")); }
    }
}
