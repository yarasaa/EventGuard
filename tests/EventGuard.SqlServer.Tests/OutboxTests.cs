using System.Collections.Concurrent;
using EventGuard.Runtime;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class OutboxTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private static readonly RetryPolicy Retry = new(initialDelay: TimeSpan.FromMilliseconds(1), jitterRatio: 0);

    [Fact]
    public async Task Concurrent_producer_retries_commit_one_business_effect_and_one_intent()
    {
        var message = Message();
        var request = Request(message);
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => db.Outbox.ProduceAsync(request,
            (session, ct) => ProduceEffect(session, request, message, ct))));
        Assert.Single(results, result => result.Status == ProducerStatus.Committed);
        Assert.Equal(7, results.Count(result => result.Status == ProducerStatus.AlreadyCommitted));
        Assert.Equal(1, await db.EffectCount(request.RequestId));
        var saved = (await db.Outbox.FindAsync(message.Scope, message.IntentId))!;
        Assert.Equal(request.RequestId, saved.OriginRequestId);
        Assert.Equal(OutboxState.Pending, saved.State);
        var conflict = await db.Outbox.ProduceAsync(new(request.Scope, request.RequestId, ContentFingerprint.FromBytes([9])),
            (_, _) => throw new InvalidOperationException("Must not run changed request."));
        Assert.Equal(ProducerStatus.RequestConflict, conflict.Status);
    }

    [Fact]
    public async Task Producer_rollback_removes_business_receipt_and_intent_so_request_can_retry()
    {
        var message = Message();
        var request = Request(message);
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Outbox.ProduceAsync(request, async (session, ct) =>
        {
            await ProduceEffect(session, request, message, ct);
            Assert.Null(await db.Outbox.FindAsync(message.Scope, message.IntentId));
            throw new InvalidOperationException("Abort transaction.");
        }));
        Assert.Equal(0, await db.EffectCount(request.RequestId));
        Assert.Null(await db.Outbox.FindAsync(message.Scope, message.IntentId));
        Assert.Equal(ProducerStatus.Committed, (await Save(message, request)).Status);
        Assert.Equal(1, await db.EffectCount(request.RequestId));
    }

    [Fact]
    public async Task Duplicate_intent_is_idempotent_but_changed_content_rolls_back_the_whole_transaction()
    {
        var message = Message();
        var request = Request(message);
        await Assert.ThrowsAsync<OutboxConflictException>(() => db.Outbox.ProduceAsync(request, async (session, ct) =>
        {
            await ProduceEffect(session, request, message, ct);
            Assert.Equal(OutboxEnqueueStatus.AlreadyEnqueued, await session.Outbox.EnqueueAsync(message, ct));
            await session.Outbox.EnqueueAsync(new(message.Scope, message.IntentId, message.MessageId, message.Route, [9]), ct);
        }));
        Assert.Equal(0, await db.EffectCount(request.RequestId));
        Assert.Null(await db.Outbox.FindAsync(message.Scope, message.IntentId));
        await Save(message, request);
        var another = Request(message);
        await Assert.ThrowsAsync<OutboxConflictException>(() => Save(message, another));
        Assert.Equal(0, await db.EffectCount(another.RequestId));
        Assert.Equal(message.Fingerprint, (await db.Outbox.FindAsync(message.Scope, message.IntentId))!.Message.Fingerprint);
    }

    [Fact]
    public async Task Enlisting_an_existing_transaction_follows_its_commit_or_rollback()
    {
        var rollback = Message();
        var commit = Message(rollback.Scope);
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using (var transaction = (SqlTransaction)await connection.BeginTransactionAsync())
        {
            var session = db.Outbox.Enlist(transaction);
            await session.Outbox.EnqueueAsync(rollback);
            await Effect(session, rollback.IntentId, default);
            await transaction.RollbackAsync();
        }
        await using (var transaction = (SqlTransaction)await connection.BeginTransactionAsync())
        {
            var session = db.Outbox.Enlist(transaction);
            await session.Outbox.EnqueueAsync(commit);
            await Effect(session, commit.IntentId, default);
            await transaction.CommitAsync();
            await Assert.ThrowsAsync<InvalidOperationException>(() => session.Outbox.EnqueueAsync(rollback));
        }
        Assert.Null(await db.Outbox.FindAsync(rollback.Scope, rollback.IntentId));
        Assert.Equal(0, await db.EffectCount(rollback.IntentId));
        Assert.NotNull(await db.Outbox.FindAsync(commit.Scope, commit.IntentId));
        Assert.Equal(1, await db.EffectCount(commit.IntentId));
    }

    [Fact]
    public async Task Consumer_business_completion_and_new_intent_commit_or_rollback_together()
    {
        var scope = DatabaseFixture.Scope();
        var accepted = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope));
        var message = Message(new(scope.Tenant, scope.Consumer));
        var claim = (await db.Claim(scope))!;
        var failed = await db.Store.ExecuteAsync(claim, async (work, payload, session, ct) =>
        {
            await db.Effect(work, payload, session, ct);
            await session.Outbox.EnqueueAsync(message, ct);
            throw new InvalidOperationException("Try again.");
        }, Retry);
        Assert.Equal(ExecutionStatus.RetryScheduled, failed.Status);
        Assert.Null(await db.Outbox.FindAsync(message.Scope, message.IntentId));
        Assert.Equal(0, await db.EffectCount(accepted.Work.Id));
        await Task.Delay(15);
        var retried = (await db.Claim(scope))!;
        Assert.NotNull(retried);
        var done = await db.Store.ExecuteAsync(retried, async (work, payload, session, ct) =>
        {
            await db.Effect(work, payload, session, ct);
            await session.Outbox.EnqueueAsync(message, ct);
        }, Retry);
        Assert.Equal(ExecutionStatus.Completed, done.Status);
        Assert.Equal(1, await db.EffectCount(accepted.Work.Id));
        Assert.Equal(accepted.Work.Id, (await db.Outbox.FindAsync(message.Scope, message.IntentId))!.OriginWorkId);
    }

    [Fact]
    public async Task Tenant_and_producer_scope_mismatches_cannot_add_foreign_intents()
    {
        var scope = DatabaseFixture.Scope();
        var accepted = await db.Store.AcceptAsync(DatabaseFixture.Submission(scope));
        var foreign = Message(new("other-tenant", scope.Consumer));
        var claim = (await db.Claim(scope))!;
        var failed = await db.Store.ExecuteAsync(claim, async (work, payload, session, ct) =>
        {
            await db.Effect(work, payload, session, ct);
            await session.Outbox.EnqueueAsync(foreign, ct);
        }, new(maximumAttempts: 1));
        Assert.Equal(ExecutionStatus.DeadLetter, failed.Status);
        Assert.Equal(0, await db.EffectCount(accepted.Work.Id));
        var request = new ProducerRequest(new("tenant", "another-producer"), Guid.NewGuid(), ContentFingerprint.FromBytes([1]));
        await Assert.ThrowsAsync<InvalidOperationException>(() => Save(foreign, request));
        Assert.Null(await db.Outbox.FindAsync(foreign.Scope, foreign.IntentId));
    }

    [Fact]
    public async Task Claim_requests_are_durable_and_stale_publishers_cannot_overwrite_newer_results()
    {
        var message = Message();
        await Save(message);
        var id = Guid.NewGuid();
        var first = (await db.Outbox.ClaimNextAsync(message.Scope, id, TimeSpan.FromMilliseconds(20)))!;
        var repeated = (await db.Outbox.ClaimNextAsync(message.Scope, id, TimeSpan.FromMilliseconds(20)))!;
        Assert.Equal(first.Token, repeated.Token);
        Assert.Equal(first.Record.Version, repeated.Record.Version);
        Assert.Equal(1, repeated.Record.AttemptCount);
        await Task.Delay(50);
        var recovered = (await db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)))!;
        Assert.True(recovered.Record.DeliveryUncertain);
        Assert.Equal(2, recovered.Record.AttemptCount);
        Assert.Equal(OutboxSettlementStatus.LeaseLost, (await db.Outbox.SettleAsync(first, Accepted(), Retry)).Status);
        var result = await db.Outbox.SettleAsync(recovered, Accepted(), Retry);
        Assert.Equal(OutboxState.Published, result.Record.State);
        Assert.True(result.Record.DeliveryUncertain);
        Assert.Null(await db.Outbox.ClaimNextAsync(message.Scope, id, TimeSpan.FromSeconds(10)));
        await using var attemptsConnection = await db.DataSource.OpenConnectionAsync();
        await using var attempts = new SqlCommand($"SELECT COUNT_BIG(*) FROM {db.Schema}.outbox_attempts WHERE scope_id=@scope AND status='Unknown' AND reason='publish-interrupted'", attemptsConnection);
        attempts.Parameters.AddWithValue("scope", message.Scope.Id);
        Assert.Equal(1L, await attempts.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Unknown_response_retries_exact_envelope_after_restart_and_keeps_uncertainty()
    {
        // Test envelope precision independently of host/container clock alignment.
        var message = Message(notBefore: new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero).AddTicks(1_234_567));
        await Save(message);
        var saved = await db.Outbox.FindAsync(message.Scope, message.IntentId);
        Assert.NotNull(saved); Assert.Equal(message.NotBefore, saved.Message.NotBefore);
        Assert.Equal(message.Fingerprint, saved.Message.Fingerprint);
        var first = await db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10));
        Assert.NotNull(first);
        var pending = await db.Outbox.SettleAsync(first, new(PublishStatus.Unknown, "confirm-lost"), Retry);
        Assert.Equal(OutboxState.RetryScheduled, pending.Record.State);
        await Task.Delay(15);
        var restarted = new SqlServerOutboxStore(db.DataSource, db.Schema);
        var second = await restarted.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10));
        Assert.NotNull(second);
        Assert.Equal(message.Fingerprint, second.Record.Message.Fingerprint);
        Assert.Equal(message.CopyPayload(), second.Record.Message.CopyPayload());
        Assert.Equal(message.MessageId, second.Record.Message.MessageId);
        Assert.Equal(message.Route, second.Record.Message.Route);
        var published = await restarted.SettleAsync(second, Accepted(), Retry);
        Assert.True(published.Record.DeliveryUncertain);
        Assert.Equal(OutboxState.Published, published.Record.State);
    }

    [Fact]
    public async Task Scheduled_intents_wait_and_other_scopes_cannot_claim_them()
    {
        var scheduled = Message(notBefore: DateTimeOffset.UtcNow.AddHours(1));
        await Save(scheduled);
        Assert.Equal(scheduled.Fingerprint, (await db.Outbox.FindAsync(scheduled.Scope, scheduled.IntentId))!.Message.Fingerprint);
        Assert.Null(await db.Outbox.ClaimNextAsync(scheduled.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
        var otherScope = new ProducerScope("other", scheduled.Scope.Producer);
        Assert.Null(await db.Outbox.FindAsync(otherScope, scheduled.IntentId));
        Assert.Null(await db.Outbox.ClaimNextAsync(otherScope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
        var ready = Message(scheduled.Scope);
        await Save(ready);
        Assert.Equal(ready.IntentId, (await db.Outbox.ClaimNextAsync(ready.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)))!.Record.Message.IntentId);
    }

    [Fact]
    public async Task Independent_targets_record_independent_acceptance_and_permanent_failure()
    {
        var one = Message();
        var two = new OutgoingMessage(one.Scope, Guid.NewGuid(), one.MessageId, new("test", "local", "v1", "unavailable"), one.CopyPayload());
        var request = Request(one);
        await db.Outbox.ProduceAsync(request, async (session, ct) =>
        {
            await session.Outbox.EnqueueAsync(one, ct);
            await session.Outbox.EnqueueAsync(two, ct);
        });
        var transport = new TestTransport((message, _) => Task.FromResult(message.Route.Address == "unavailable"
            ? new PublishOutcome(PublishStatus.NotSentPermanent, "address-rejected") : Accepted()));
        var dispatcher = new OutboxDispatcher(db.Outbox, one.Scope, [transport]);
        await dispatcher.DispatchNextAsync();
        await dispatcher.DispatchNextAsync();
        Assert.Equal(OutboxState.Published, (await db.Outbox.FindAsync(one.Scope, one.IntentId))!.State);
        Assert.Equal(OutboxState.DeadLetter, (await db.Outbox.FindAsync(two.Scope, two.IntentId))!.State);
        Assert.Null(await dispatcher.DispatchNextAsync());
    }

    [Fact]
    public async Task Dispatcher_does_not_reroute_old_intents_to_new_binding_versions()
    {
        var message = Message();
        await Save(message);
        var replacement = new TestTransport((_, _) => throw new InvalidOperationException("Must not reroute."), "v2");
        var dispatcher = new OutboxDispatcher(db.Outbox, message.Scope, [replacement]);
        var result = (await dispatcher.DispatchNextAsync())!;
        Assert.Equal(OutboxState.DeadLetter, result.Record.State);
        Assert.False(result.Record.DeliveryUncertain);
        Assert.Equal("transport-binding-missing", result.Record.LastReason);
        Assert.Empty(replacement.Messages);
        Assert.Throws<ArgumentException>(() => new OutboxDispatcher(db.Outbox, message.Scope, [replacement, replacement]));
    }

    [Fact]
    public async Task Cancellation_after_external_send_is_recorded_as_unknown_with_a_separate_settlement_token()
    {
        var message = Message();
        await Save(message);
        using var stop = new CancellationTokenSource();
        var transport = new TestTransport((_, ct) =>
        {
            stop.Cancel();
            ct.ThrowIfCancellationRequested();
            return Task.FromResult(Accepted());
        });
        var result = (await new OutboxDispatcher(db.Outbox, message.Scope, [transport], Retry).DispatchNextAsync(stop.Token))!;
        Assert.Equal(OutboxState.RetryScheduled, result.Record.State);
        Assert.True(result.Record.DeliveryUncertain);
        Assert.Equal("publish-outcome-unknown", result.Record.LastReason);
    }

    [Fact]
    public async Task Dispatcher_releases_database_connection_before_calling_transport()
    {
        var message = Message();
        await Save(message);
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { MaxPoolSize = 1, ConnectTimeout = 2 };
        var singleConnection = new SqlServerDataSource(settings.ConnectionString);
        var store = new SqlServerOutboxStore(singleConnection, db.Schema);
        var transport = new TestTransport(async (outgoing, ct) =>
        {
            // This needs the same one-slot pool; it would fail if dispatch held the connection.
            var saved = await store.FindAsync(outgoing.Scope, outgoing.IntentId, ct);
            Assert.Equal(OutboxState.Publishing, saved!.State);
            return Accepted();
        });
        var result = (await new OutboxDispatcher(store, message.Scope, [transport]).DispatchNextAsync())!;
        Assert.Equal(OutboxState.Published, result.Record.State);
    }

    [Fact]
    public async Task Bounded_dispatcher_publishes_each_available_intent_and_stops_all_workers()
    {
        var first = Message();
        var messages = Enumerable.Range(0, 24).Select(_ => Message(first.Scope)).ToArray();
        await db.Outbox.ProduceAsync(Request(first), async (session, ct) =>
        {
            foreach (var message in messages) await session.Outbox.EnqueueAsync(message, ct);
        });
        var active = 0;
        var maximum = 0;
        var transport = new TestTransport(async (_, ct) =>
        {
            var count = Interlocked.Increment(ref active);
            int prior;
            do { prior = Volatile.Read(ref maximum); } while (count > prior && Interlocked.CompareExchange(ref maximum, count, prior) != prior);
            try { await Task.Delay(10, ct); return Accepted(); }
            finally { Interlocked.Decrement(ref active); }
        });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
        var dispatcher = new OutboxDispatcher(db.Outbox, first.Scope, [transport], options: new(maximumConcurrency: 3, idleDelay: TimeSpan.FromMilliseconds(10)));
        var running = dispatcher.RunAsync(stop.Token);
        try
        {
            while (true)
            {
                var records = await Task.WhenAll(messages.Select(message => db.Outbox.FindAsync(message.Scope, message.IntentId, timeout.Token)));
                if (records.All(record => record!.State == OutboxState.Published)) break;
                await Task.Delay(20, timeout.Token);
            }
        }
        finally { stop.Cancel(); await running; }
        Assert.InRange(maximum, 1, 3);
        Assert.Equal(0, active);
        Assert.Equal(24, transport.Messages.Count);
        Assert.Equal(24, transport.Messages.Select(message => message.IntentId).Distinct().Count());
    }

    private Task<ProducerResult> Save(OutgoingMessage message, ProducerRequest? request = null)
    {
        request ??= Request(message);
        return db.Outbox.ProduceAsync(request, (session, ct) => ProduceEffect(session, request, message, ct));
    }

    private async ValueTask ProduceEffect(SqlServerSession session, ProducerRequest request, OutgoingMessage message, CancellationToken ct)
    {
        await Effect(session, request.RequestId, ct);
        await session.Outbox.EnqueueAsync(message, ct);
    }

    private async Task Effect(SqlServerSession session, Guid id, CancellationToken ct)
    {
        await using var command = session.CreateCommand($"INSERT INTO {db.Schema}.business_effects VALUES(@id,'producer')");
        command.Parameters.AddWithValue("id", id);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static ProducerRequest Request(OutgoingMessage message) => new(message.Scope, Guid.NewGuid(), ContentFingerprint.FromBytes(message.CopyPayload()));
    private static PublishOutcome Accepted() => new(PublishStatus.Accepted, "test-accepted");
    private static OutgoingMessage Message(ProducerScope? scope = null, DateTimeOffset? notBefore = null) =>
        new(scope ?? new("tenant", "producer-" + Guid.NewGuid().ToString("N")), Guid.NewGuid(), Guid.NewGuid(),
            new("test", "local", "v1", "files.created"), [1, 2, 3], headers: new Dictionary<string, string> { ["correlation"] = "test" }, notBefore: notBefore);

    // An in-process test double, not a Kafka/RabbitMQ integration or delivery proof.
    private sealed class TestTransport(Func<OutgoingMessage, CancellationToken, Task<PublishOutcome>> send, string version = "v1") : IOutboundTransport
    {
        public string Transport => "test";
        public string Connection => "local";
        public string ConfigurationVersion => version;
        public ConcurrentQueue<OutgoingMessage> Messages { get; } = new();
        public Task<PublishOutcome> PublishAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Messages.Enqueue(message);
            return send(message, cancellationToken);
        }
    }
}
