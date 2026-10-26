using Npgsql;

namespace EventGuard.PostgreSql.Tests;

public sealed class OutboxRecoveryTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Valid_maximum_headers_with_json_escaping_are_not_mistaken_for_corruption()
    {
        var seed = Message();
        var headers = Enumerable.Range(0, 32).ToDictionary(i => new string('\u0001', 1000) + i,
            _ => new string('\u0002', 1024));
        var message = new OutgoingMessage(seed.Scope, seed.IntentId, seed.MessageId, seed.Route, [1], headers: headers);
        await db.EnqueueAsync(message);
        var claim = (await Claim(message))!;
        Assert.Equal(message.Fingerprint, claim.Record.Message.Fingerprint);
        Assert.Equal(32, claim.Record.Message.Headers.Count);
        Assert.Equal(0, await QuarantineCount(message.Scope));
    }

    [Theory]
    [InlineData("[]", "outbox-envelope-invalid")]
    [InlineData("null", "outbox-envelope-invalid")]
    [InlineData("{\"x\":null}", "outbox-envelope-invalid")]
    [InlineData("{\"x\":\"changed\"}", "outbox-fingerprint-mismatch")]
    public async Task Invalid_headers_are_preserved_in_quarantine_and_healthy_work_can_continue(string headers, string reason)
    {
        var broken = Message();
        var healthy = Message(broken.Scope);
        await db.EnqueueAsync(broken);
        await db.EnqueueAsync(healthy);
        await using (var corrupt = db.DataSource.CreateCommand($"UPDATE {db.Schema}.outbox_messages SET headers=@headers::jsonb,next_attempt_at=clock_timestamp()-interval '1 hour' WHERE scope_id=@scope AND id=@id"))
        {
            corrupt.Parameters.AddWithValue("scope", broken.Scope.Id);
            corrupt.Parameters.AddWithValue("id", broken.IntentId);
            corrupt.Parameters.AddWithValue("headers", headers);
            await corrupt.ExecuteNonQueryAsync();
        }
        var claimed = (await Claim(healthy))!;
        Assert.Equal(healthy.IntentId, claimed.Record.Message.IntentId);
        var metadata = (await db.Recovery.FindMetadataAsync(broken.Scope, broken.IntentId))!;
        Assert.Equal(OutboxState.Quarantined, metadata.State);
        Assert.Equal(reason, metadata.LastReason);
        Assert.False(metadata.DeliveryUncertain);
        Assert.NotNull(metadata.QuarantineId);
        var error = await Assert.ThrowsAsync<OutboxEnvelopeException>(() => db.Outbox.FindAsync(broken.Scope, broken.IntentId));
        Assert.DoesNotContain(headers, error.Message);
        await using var snapshot = db.DataSource.CreateCommand($"SELECT original_record->'headers' = @headers::jsonb AND (original_record->>'state')::int=0 AND (original_record->>'version')::bigint=0 FROM {db.Schema}.outbox_quarantine WHERE scope_id=@scope AND id=@id");
        snapshot.Parameters.AddWithValue("scope", broken.Scope.Id);
        snapshot.Parameters.AddWithValue("id", metadata.QuarantineId!.Value);
        snapshot.Parameters.AddWithValue("headers", headers);
        Assert.Equal(true, await snapshot.ExecuteScalarAsync());
        Assert.Equal(OutboxReplayStatus.Quarantined, (await db.Recovery.ReplayAsync(Request(broken, metadata.Version, true))).Status);
    }

    [Fact]
    public async Task Oversized_raw_payload_is_quarantined_without_returning_it_to_the_worker()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        await using (var corrupt = db.DataSource.CreateCommand($"UPDATE {db.Schema}.outbox_messages SET payload=decode(repeat('ab',1048577),'hex') WHERE id=@id"))
        {
            corrupt.Parameters.AddWithValue("id", message.IntentId);
            await corrupt.ExecuteNonQueryAsync();
        }
        Assert.Null(await Claim(message));
        var metadata = (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!;
        Assert.Equal("outbox-envelope-too-large", metadata.LastReason);
        await using var retained = db.DataSource.CreateCommand($"SELECT length(original_record->>'payload') FROM {db.Schema}.outbox_quarantine WHERE scope_id=@scope AND id=@id");
        retained.Parameters.AddWithValue("scope", message.Scope.Id);
        retained.Parameters.AddWithValue("id", metadata.QuarantineId!.Value);
        Assert.Equal(2 + 1048577 * 2, await retained.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Altered_tenant_is_not_reinterpreted_as_a_different_scope()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        await using (var corrupt = db.DataSource.CreateCommand($"UPDATE {db.Schema}.outbox_messages SET tenant='foreign' WHERE id=@id"))
        {
            corrupt.Parameters.AddWithValue("id", message.IntentId);
            await corrupt.ExecuteNonQueryAsync();
        }
        Assert.Null(await Claim(message));
        var metadata = (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!;
        Assert.Equal(OutboxState.Quarantined, metadata.State);
        Assert.Equal("outbox-scope-mismatch", metadata.LastReason);
        Assert.Null(await db.Recovery.FindMetadataAsync(new("foreign", message.Scope.Producer), message.IntentId));
    }

    [Fact]
    public async Task A_poll_limits_quarantine_work_and_later_polls_reach_healthy_intents()
    {
        var healthy = Message();
        var broken = Enumerable.Range(0, 17).Select(_ => Message(healthy.Scope)).ToArray();
        foreach (var message in broken) await db.EnqueueAsync(message);
        await using (var corrupt = db.DataSource.CreateCommand($"UPDATE {db.Schema}.outbox_messages SET headers='[]'::jsonb WHERE scope_id=@scope"))
        {
            corrupt.Parameters.AddWithValue("scope", healthy.Scope.Id);
            await corrupt.ExecuteNonQueryAsync();
        }
        await db.EnqueueAsync(healthy);
        Assert.Null(await Claim(healthy));
        var rows = await Task.WhenAll(broken.Select(message => db.Recovery.FindMetadataAsync(message.Scope, message.IntentId)));
        Assert.Equal(16, rows.Count(row => row!.State == OutboxState.Quarantined));
        Assert.Equal(healthy.IntentId, (await Claim(healthy))!.Record.Message.IntentId);
        Assert.Equal(17, await QuarantineCount(healthy.Scope));
    }

    [Fact]
    public async Task Active_publisher_is_not_interrupted_and_expired_corrupt_attempt_keeps_delivery_uncertainty()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        var old = (await db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromMilliseconds(300)))!;
        await Corrupt(message);
        Assert.Null(await Claim(message));
        Assert.Equal(OutboxState.Publishing, (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!.State);
        var delay = old.LeaseUntil - DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(40);
        if (delay > TimeSpan.Zero) await Task.Delay(delay);
        Assert.Null(await Claim(message));
        var metadata = (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!;
        Assert.Equal(OutboxState.Quarantined, metadata.State);
        Assert.True(metadata.DeliveryUncertain);
        Assert.Equal(1, metadata.AttemptCount);
        await Assert.ThrowsAsync<OutboxEnvelopeException>(() => db.Outbox.SettleAsync(old, new(PublishStatus.Accepted, "late"), new()));
        await using var attempt = db.DataSource.CreateCommand($"SELECT status FROM {db.Schema}.outbox_attempts WHERE scope_id=@scope AND id=@id");
        attempt.Parameters.AddWithValue("scope", message.Scope.Id);
        attempt.Parameters.AddWithValue("id", old.Token);
        Assert.Equal("Unknown", await attempt.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Replay_of_a_corrupt_failed_envelope_atomically_records_quarantine_and_rejection()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        var failed = await Fail(message);
        await Corrupt(message);
        var request = Request(message, failed.Version);
        var result = await db.Recovery.ReplayAsync(request);
        Assert.Equal(OutboxReplayStatus.Quarantined, result.Status);
        Assert.Equal(failed.Version + 1, result.Version);
        Assert.Equal(0, result.Generation);
        Assert.Equal(result with { RepeatedRequest = true }, await db.RestartRecovery().ReplayAsync(request));
        Assert.Equal(1, await QuarantineCount(message.Scope));
        Assert.Null(await Claim(message));
    }

    [Fact]
    public async Task Failed_quarantine_state_write_rolls_back_raw_snapshot_and_claim_changes()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        await Corrupt(message);
        await using (var guard = db.DataSource.CreateCommand($"""
            CREATE FUNCTION {db.Schema}.reject_quarantine() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.state=5 THEN RAISE EXCEPTION 'test quarantine unavailable'; END IF; RETURN NEW; END $$;
            CREATE TRIGGER reject_quarantine BEFORE UPDATE ON {db.Schema}.outbox_messages
            FOR EACH ROW EXECUTE FUNCTION {db.Schema}.reject_quarantine();
            """))
            await guard.ExecuteNonQueryAsync();
        try
        {
            await Assert.ThrowsAsync<PostgresException>(() => Claim(message));
            Assert.Equal(0, await QuarantineCount(message.Scope));
            Assert.Equal(OutboxState.Pending, (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!.State);
        }
        finally
        {
            await using var remove = db.DataSource.CreateCommand($"DROP TRIGGER reject_quarantine ON {db.Schema}.outbox_messages");
            await remove.ExecuteNonQueryAsync();
        }
        Assert.Null(await Claim(message));
        Assert.Equal(1, await QuarantineCount(message.Scope));
    }

    [Fact]
    public async Task Lost_replay_commit_reply_is_resolved_without_opening_another_generation()
    {
        var message = Message();
        await db.EnqueueAsync(message);
        var request = Request(message, (await Fail(message)).Version);
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new CommitReplyDropProxy(settings.Port);
        ConfigureProxy(settings, proxy.Port);
        await using var interrupted = NpgsqlDataSource.Create(settings.ConnectionString);
        var recovery = new PostgreSqlOutboxRecoveryStore(interrupted, db.Schema);
        var error = await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => recovery.ReplayAsync(request));
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(request.RequestId, error.OperationId);
        await Fail(message);
        var repeat = await db.Recovery.ReplayAsync(request);
        Assert.True(repeat.RepeatedRequest);
        Assert.Equal(1, repeat.Generation);
        Assert.Equal(OutboxState.DeadLetter, (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!.State);
    }

    [Fact]
    public async Task Lost_quarantine_and_claim_commit_reply_keeps_both_results_atomic()
    {
        var broken = Message();
        var healthy = Message(broken.Scope);
        await db.EnqueueAsync(broken);
        await Corrupt(broken);
        await db.EnqueueAsync(healthy);
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new CommitReplyDropProxy(settings.Port);
        ConfigureProxy(settings, proxy.Port);
        await using var interrupted = NpgsqlDataSource.Create(settings.ConnectionString);
        var store = new PostgreSqlOutboxStore(interrupted, db.Schema);
        var claimId = Guid.NewGuid();
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => store.ClaimNextAsync(healthy.Scope, claimId, TimeSpan.FromSeconds(10)));
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(OutboxState.Quarantined, (await db.Recovery.FindMetadataAsync(broken.Scope, broken.IntentId))!.State);
        Assert.Equal(1, await QuarantineCount(broken.Scope));
        var repeated = (await db.Outbox.ClaimNextAsync(healthy.Scope, claimId, TimeSpan.FromSeconds(10)))!;
        Assert.Equal(healthy.IntentId, repeated.Record.Message.IntentId);
        Assert.Equal(1, repeated.Record.AttemptCount);
        Assert.Equal(OutboxState.Published, (await db.Outbox.SettleAsync(repeated, new(PublishStatus.Accepted, "accepted"), new())).Record.State);
    }

    private static void ConfigureProxy(NpgsqlConnectionStringBuilder settings, int port)
    {
        settings.Port = port;
        settings.SslMode = SslMode.Disable;
        settings.DisableGssEncryptionForFaultProxy();
        settings.Pooling = false;
    }

    private async Task Corrupt(OutgoingMessage message)
    {
        await using var command = db.DataSource.CreateCommand($"UPDATE {db.Schema}.outbox_messages SET headers='[]'::jsonb WHERE scope_id=@scope AND id=@id");
        command.Parameters.AddWithValue("scope", message.Scope.Id);
        command.Parameters.AddWithValue("id", message.IntentId);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<long> QuarantineCount(ProducerScope scope)
    {
        await using var command = db.DataSource.CreateCommand($"SELECT count(*) FROM {db.Schema}.outbox_quarantine WHERE scope_id=@scope");
        command.Parameters.AddWithValue("scope", scope.Id);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private async Task<OutboxRecord> Fail(OutgoingMessage message) =>
        (await db.Outbox.SettleAsync((await Claim(message))!, new(PublishStatus.NotSentPermanent, "failed"), new())).Record;
    private Task<OutboxClaim?> Claim(OutgoingMessage message) => db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10));
    private static OutboxReplayRequest Request(OutgoingMessage message, long version, bool ack = false) =>
        new(message.Scope, Guid.NewGuid(), message.IntentId, version, "operator", "fixed configuration", ack);
    private static OutgoingMessage Message(ProducerScope? scope = null) => new(scope ?? new("tenant", Guid.NewGuid().ToString("N")),
        Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "files.created"), [1, 2]);
}
