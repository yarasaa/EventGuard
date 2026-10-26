using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class RecoveryTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Theory]
    [InlineData("[]", "outbox-envelope-invalid")]
    [InlineData("null", "outbox-envelope-invalid")]
    [InlineData("not-json", "outbox-envelope-invalid")]
    [InlineData("{\"x\":null}", "outbox-envelope-invalid")]
    [InlineData("{\"x\":\"changed\"}", "outbox-fingerprint-mismatch")]
    public async Task Invalid_envelope_retains_original_snapshot_and_allows_healthy_claim(string headers, string reason)
    {
        var broken = CommitTests.Message();
        var healthy = CommitTests.Message(broken.Scope);
        await db.EnqueueAsync(broken);
        await db.EnqueueAsync(healthy);
        await Corrupt(broken, headers);
        var claim = await Claim(healthy);
        Assert.Equal(healthy.IntentId, claim!.Record.Message.IntentId);
        var metadata = (await db.Recovery.FindMetadataAsync(broken.Scope, broken.IntentId))!;
        Assert.Equal(OutboxState.Quarantined, metadata.State);
        Assert.Equal(reason, metadata.LastReason);
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new SqlCommand($"SELECT original_record FROM {db.Schema}.outbox_quarantine WHERE id=@id", connection);
        command.Parameters.AddWithValue("id", metadata.QuarantineId!.Value);
        using var raw = JsonDocument.Parse((string)(await command.ExecuteScalarAsync())!);
        Assert.Equal(headers, raw.RootElement.GetProperty("headers").GetString());
        Assert.Equal(0, raw.RootElement.GetProperty("state").GetInt32());
        Assert.Equal(broken.CopyPayload(), raw.RootElement.GetProperty("payload").GetBytesFromBase64());
        await Assert.ThrowsAsync<OutboxEnvelopeException>(() => db.Outbox.FindAsync(broken.Scope, broken.IntentId));
        Assert.Equal(OutboxReplayStatus.Quarantined, (await db.Recovery.ReplayAsync(new(broken.Scope, Guid.NewGuid(), broken.IntentId,
            metadata.Version, "operator", "retry", true))).Status);
    }
    [Fact]
    public async Task Maximum_escaped_headers_roundtrip_without_false_quarantine()
    {
        var seed = CommitTests.Message();
        var headers = Enumerable.Range(0, 32).ToDictionary(i => new string('\u0001', 1000) + i, _ => new string('\u0002', 1024));
        var message = new OutgoingMessage(seed.Scope, seed.IntentId, seed.MessageId, seed.Route, [1], headers: headers);
        await db.EnqueueAsync(message);
        Assert.Equal(message.Fingerprint, (await Claim(message))!.Record.Message.Fingerprint);
    }
    [Fact]
    public async Task Oversized_payload_is_bounded_on_read_and_fully_preserved_server_side()
    {
        var message = CommitTests.Message();
        await db.EnqueueAsync(message);
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using (var corrupt = new SqlCommand($"UPDATE {db.Schema}.outbox_messages SET payload=@payload WHERE id=@id", connection))
        {
            corrupt.Parameters.AddWithValue("id", message.IntentId);
            corrupt.Parameters.AddWithValue("payload", new byte[1048577]);
            await corrupt.ExecuteNonQueryAsync();
        }
        Assert.Null(await Claim(message));
        var metadata = (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!;
        Assert.Equal("outbox-envelope-too-large", metadata.LastReason);
        await using var snapshot = new SqlCommand($"SELECT original_record FROM {db.Schema}.outbox_quarantine WHERE id=@id", connection);
        snapshot.Parameters.AddWithValue("id", metadata.QuarantineId!.Value);
        using var raw = JsonDocument.Parse((string)(await snapshot.ExecuteScalarAsync())!);
        Assert.Equal(1048577, raw.RootElement.GetProperty("payload").GetBytesFromBase64().Length);
    }
    [Fact]
    public async Task Bounded_poll_quarantines_sixteen_then_reaches_healthy_work_on_next_poll()
    {
        var healthy = CommitTests.Message();
        for (var i = 0; i < 17; i++)
        {
            var broken = CommitTests.Message(healthy.Scope);
            await db.EnqueueAsync(broken);
            await Corrupt(broken, "[]");
        }
        await db.EnqueueAsync(healthy);
        Assert.Null(await Claim(healthy));
        Assert.Equal(16, await Count(healthy.Scope));
        Assert.Equal(healthy.IntentId, (await Claim(healthy))!.Record.Message.IntentId);
        Assert.Equal(17, await Count(healthy.Scope));
    }
    [Fact]
    public async Task Quarantine_trigger_failure_rolls_back_snapshot_and_state_together()
    {
        var message = CommitTests.Message();
        await db.EnqueueAsync(message);
        await Corrupt(message, "[]");
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using (var guard = new SqlCommand($"CREATE TRIGGER {db.Schema}.reject_quarantine ON {db.Schema}.outbox_messages AFTER UPDATE AS BEGIN IF EXISTS(SELECT 1 FROM inserted WHERE state=5) THROW 51003,'Test quarantine unavailable.',1; END", connection))
            await guard.ExecuteNonQueryAsync();
        try
        {
            await Assert.ThrowsAsync<SqlException>(() => Claim(message));
            Assert.Equal(0, await Count(message.Scope));
            Assert.Equal(OutboxState.Pending, (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!.State);
        }
        finally
        {
            await using var remove = new SqlCommand($"DROP TRIGGER {db.Schema}.reject_quarantine", connection);
            await remove.ExecuteNonQueryAsync();
        }
        Assert.Null(await Claim(message));
        Assert.Equal(1, await Count(message.Scope));
    }
    [Fact]
    public async Task Active_lease_is_preserved_and_later_quarantine_retains_uncertainty()
    {
        var message = CommitTests.Message();
        await db.EnqueueAsync(message);
        var old = (await db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(1)))!;
        await Corrupt(message, "[]");
        Assert.Null(await Claim(message));
        Assert.Equal(OutboxState.Publishing, (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!.State);
        await Task.Delay(old.LeaseUntil - DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(80));
        Assert.Null(await Claim(message));
        var metadata = (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!;
        Assert.Equal(OutboxState.Quarantined, metadata.State);
        Assert.True(metadata.DeliveryUncertain);
        await Assert.ThrowsAsync<OutboxEnvelopeException>(() => db.Outbox.SettleAsync(old, new(PublishStatus.Accepted, "late"), new()));
    }
    [Fact]
    public async Task Quarantine_and_healthy_claim_survive_a_lost_commit_reply_together()
    {
        var broken = CommitTests.Message();
        var healthy = CommitTests.Message(broken.Scope);
        await db.EnqueueAsync(broken);
        await Corrupt(broken, "[]");
        await db.EnqueueAsync(healthy);
        await using var proxy = new CommitReplyDropProxy();
        var options = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false, Pooling = false };
        var interrupted = new SqlServerOutboxStore(new(options.ConnectionString), db.Schema);
        var request = Guid.NewGuid();
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => interrupted.ClaimNextAsync(healthy.Scope, request, TimeSpan.FromSeconds(10)));
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, await Count(broken.Scope));
        Assert.Equal(healthy.IntentId, (await db.Outbox.ClaimNextAsync(healthy.Scope, request, TimeSpan.FromSeconds(10)))!.Record.Message.IntentId);
    }
    private async Task Corrupt(OutgoingMessage message, string headers)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new SqlCommand($"UPDATE {db.Schema}.outbox_messages SET headers=@headers,next_attempt_at=DATEADD(hour,-1,SYSDATETIMEOFFSET()) WHERE id=@id AND scope_id=@scope", connection);
        command.Parameters.AddWithValue("id", message.IntentId);
        command.Parameters.AddWithValue("scope", message.Scope.Id);
        command.Parameters.AddWithValue("headers", headers);
        await command.ExecuteNonQueryAsync();
    }
    private async Task<long> Count(ProducerScope scope)
    {
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new SqlCommand($"SELECT COUNT_BIG(*) FROM {db.Schema}.outbox_quarantine WHERE scope_id=@scope", connection);
        command.Parameters.AddWithValue("scope", scope.Id);
        return (long)(await command.ExecuteScalarAsync())!;
    }
    private Task<OutboxClaim?> Claim(OutgoingMessage message) => db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10));
}
