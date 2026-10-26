using Npgsql;
namespace EventGuard.PostgreSql.Tests;

public sealed class OutboxRepairFaultTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private async Task<OutboxRepairRequest> Prepare()
    {
        var message = new OutgoingMessage(new("tenant", Guid.NewGuid().ToString("N")), Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "files.created"), [1, 2]);
        await db.EnqueueAsync(message);
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var corrupt = new NpgsqlCommand($"UPDATE {db.Schema}.outbox_messages SET headers='[]' WHERE scope_id=@scope AND id=@id", connection);
        corrupt.Parameters.AddWithValue("scope", message.Scope.Id); corrupt.Parameters.AddWithValue("id", message.IntentId);
        await corrupt.ExecuteNonQueryAsync();
        Assert.Null(await db.Outbox.ClaimNextAsync(message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
        var metadata = (await db.Recovery.FindMetadataAsync(message.Scope, message.IntentId))!;
        return new(Guid.NewGuid(), metadata.QuarantineId!.Value, message, metadata.Version, "operator", "fixed");
    }

    [Fact]
    public async Task Actual_commit_reply_loss_does_not_restore_or_schedule_a_second_generation()
    {
        var request = await Prepare();
        var settings = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new CommitReplyDropProxy(settings.Port);
        settings.Port = proxy.Port; settings.Pooling = false; settings.SslMode = SslMode.Disable; settings.DisableGssEncryptionForFaultProxy();
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        var recovery = new PostgreSqlOutboxRecoveryStore(source, db.Schema);
        var error = await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => recovery.RepairAsync(request));
        Assert.Equal(request.RequestId, error.OperationId); await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var repeated = await ((IOutboxRepairStore)db.RestartRecovery()).RepairAsync(request);
        Assert.True(repeated.RepeatedRequest); Assert.Equal(OutboxRepairStatus.Accepted, repeated.Status); Assert.Equal(1, repeated.Generation);
        var claim = (await db.Outbox.ClaimNextAsync(request.Message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)))!;
        Assert.Equal(request.Message.Fingerprint, claim.Record.Message.Fingerprint);
        await db.Outbox.SettleAsync(claim, new(PublishStatus.Accepted, "confirmed"), new());
        Assert.True((await ((IOutboxRepairStore)db.Recovery).RepairAsync(request)).RepeatedRequest);
        Assert.Null(await db.Outbox.ClaimNextAsync(request.Message.Scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Native_receipt_failure_rolls_back_envelope_state_and_generation_together()
    {
        var request = await Prepare();
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"ALTER TABLE {db.Schema}.outbox_repair_requests ADD CONSTRAINT test_reject_repair CHECK(id <> '{request.RequestId:D}')", connection);
        await command.ExecuteNonQueryAsync();
        try { await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(() => ((IOutboxRepairStore)db.Recovery).RepairAsync(request)); }
        finally { command.CommandText = $"ALTER TABLE {db.Schema}.outbox_repair_requests DROP CONSTRAINT test_reject_repair"; await command.ExecuteNonQueryAsync(); }
        var original = (await db.Recovery.FindMetadataAsync(request.Message.Scope, request.Message.IntentId))!;
        Assert.Equal(OutboxState.Quarantined, original.State); Assert.Equal(request.ExpectedVersion, original.Version);
        command.CommandText = $"SELECT headers FROM {db.Schema}.outbox_messages WHERE scope_id=@scope AND id=@id";
        command.Parameters.AddWithValue("scope", request.Message.Scope.Id); command.Parameters.AddWithValue("id", request.Message.IntentId);
        Assert.Equal("[]", await command.ExecuteScalarAsync());
        var result = await ((IOutboxRepairStore)db.Recovery).RepairAsync(request);
        Assert.Equal(OutboxRepairStatus.Accepted, result.Status); Assert.False(result.RepeatedRequest);
    }
}
