using System.Text.Json;
using EventGuard.Runtime;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class SnapshotTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private static JsonMessageContract<Row> Items => new("record", 1, _ => 1, _ => { });
    private async Task<SnapshotRequest> Setup(SnapshotConsistency consistency = SnapshotConsistency.SessionBound, SnapshotDelivery delivery = SnapshotDelivery.LocalJob, int batch = 100)
    {
        await db.Store.InitializeSnapshotsAsync();
        var target = new JobDefinition(new("tenant", Guid.NewGuid().ToString("N")), "apply", 1, "records", 1, "v1", [1],
            [JobCapabilities.TransactionalHandler, JobCapabilities.TransactionalItems]);
        await db.Store.RegisterAsync(target);
        var definition = new SnapshotDefinition(target, "selection", "source", "v1", consistency, batch, delivery,
            delivery == SnapshotDelivery.LocalJob ? null : new("kafka", "local", "v1", "batches"));
        return new(definition, Guid.NewGuid(), "campaign-1", "operator", "{}"u8);
    }
    private async Task<SnapshotLease> Start(SnapshotRequest request, string view = "view-1")
    {
        await db.Store.CreateSnapshotAsync(request);
        var lease = (await db.Store.ClaimSnapshotAsync(request, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!;
        await db.Store.BeginSnapshotAsync(lease, view); return lease;
    }
    private static SnapshotItem Item(string key, string data = "value") => new(key, Items.Encode(new(key, data)));
    private async Task Expire(SnapshotRequest request)
    {
        await using var c = await db.DataSource.OpenConnectionAsync();
        await using var command = new SqlCommand($"UPDATE {db.Schema}.snapshots SET lease_until=@past WHERE id=@id", c);
        command.Parameters.AddWithValue("past", DateTimeOffset.UtcNow.AddMinutes(-1)); command.Parameters.AddWithValue("id", request.Id);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Source_capture_deduplicates_and_builds_100_item_batches_including_the_tail()
    {
        var request = await Setup(); await db.Store.CreateSnapshotAsync(request);
        var source = new Source(Enumerable.Range(0, 205).Select(i => Item(i.ToString())).Concat([Item("4")]).ToArray());
        var worker = new SnapshotWorker<Row>(db.Store, request.Definition, source, Items);
        Assert.True(await worker.RunOnceAsync(request));
        var progress = (await db.Store.FindSnapshotAsync(request.Definition, request.Id))!;
        Assert.Equal(205, progress.ItemCount); Assert.Equal(205, progress.BatchedItems); Assert.Equal(3, progress.BatchCount);
        Assert.Equal(SnapshotState.Batched, progress.State);
        Assert.False(await worker.RunOnceAsync(request));
        var sizes = new List<int>();
        while (await db.Claim(request.Definition.Target.WorkScope) is { } claim)
        {
            var result = await db.Store.ExecuteAsync(claim, async (_, bytes, session, ct) =>
            {
                var batch = JsonJobBatches.Contract("records", 1, Items).Decode(bytes); sizes.Add(batch.Items.Count);
                await JsonJobBatches.ExecuteAsync(request.Definition.Target, batch, Items, session, async (row, s, token) =>
                {
                    await using var sql = s.CreateCommand($"INSERT INTO {db.Schema}.business_effects VALUES(@id,@key)");
                    sql.Parameters.AddWithValue("id", request.Id); sql.Parameters.AddWithValue("key", row.Id); await sql.ExecuteNonQueryAsync(token);
                }, ct);
            }, new());
            Assert.Equal(ExecutionStatus.Completed, result.Status);
        }
        Assert.Equal(new[] { 5, 100, 100 }, sizes.Order().ToArray()); Assert.Equal(205, await db.EffectCount(request.Id));
    }

    [Fact]
    public async Task Pages_are_atomic_repeatable_and_never_publish_before_seal()
    {
        var request = await Setup(); var lease = await Start(request);
        var page = new SnapshotPage([Item("a"), Item("a")], [1], false);
        var saved = await db.Store.AppendSnapshotAsync(lease, 1, 0, page);
        Assert.Equal(1, saved.ItemCount); Assert.Equal(1, (await db.Store.AppendSnapshotAsync(lease, 1, 0, page)).ItemCount);
        Assert.Null(await db.Claim(request.Definition.Target.WorkScope));
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.Store.ReadSnapshotBatchAsync(lease));
        await Assert.ThrowsAsync<SnapshotConflictException>(() => db.Store.AppendSnapshotAsync(lease, 1, 1, new([Item("b"), Item("a", "changed")], [2], true)));
        saved = (await db.Store.FindSnapshotAsync(request.Definition, request.Id))!; Assert.Equal(1, saved.ItemCount); Assert.Equal(1, saved.Page);
        await db.Store.AppendSnapshotAsync(lease, 1, 1, new([Item("b")], [2], true));
        var candidate = await db.Store.ReadSnapshotBatchAsync(lease);
        Assert.Equal(2, candidate.Items.Count);
        await db.Store.PublishSnapshotBatchAsync(lease, candidate); await db.Store.PublishSnapshotBatchAsync(lease, candidate);
        Assert.NotNull(await db.Claim(request.Definition.Target.WorkScope)); Assert.Null(await db.Claim(request.Definition.Target.WorkScope));
    }

    [Theory]
    [InlineData(SnapshotConsistency.SessionBound)]
    [InlineData(SnapshotConsistency.ResumableView)]
    public async Task Restart_keeps_only_one_consistent_view_and_fences_the_previous_owner(SnapshotConsistency consistency)
    {
        var request = await Setup(consistency); var old = await Start(request);
        await db.Store.AppendSnapshotAsync(old, 1, 0, new([Item("old")], [1], false)); await Expire(request);
        var next = (await db.Store.ClaimSnapshotAsync(request, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!;
        await Assert.ThrowsAsync<SnapshotLeaseLostException>(() => db.Store.AppendSnapshotAsync(old, 1, 1, new([], [2], true)));
        if (consistency == SnapshotConsistency.ResumableView)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => db.Store.BeginSnapshotAsync(next, "other-view"));
            var kept = await db.Store.BeginSnapshotAsync(next, "view-1"); Assert.Equal(1, kept.ItemCount); Assert.Equal(1, kept.Page);
        }
        else
        {
            var restarted = await db.Store.BeginSnapshotAsync(next, "new-view"); Assert.Equal(0, restarted.ItemCount); Assert.Equal(2, restarted.Generation);
            await Assert.ThrowsAsync<SnapshotLeaseLostException>(() => db.Store.AppendSnapshotAsync(next, 1, 0, new([Item("wrong")], [1], true)));
            await db.Store.AppendSnapshotAsync(next, 2, 0, new([Item("new")], [1], true));
            Assert.Equal("new", Assert.Single((await db.Store.ReadSnapshotBatchAsync(next)).Items).Item.Key);
        }
    }

    [Fact]
    public async Task Concurrent_claims_and_controls_cannot_publish_cancelled_capture()
    {
        var request = await Setup(); await db.Store.CreateSnapshotAsync(request);
        var claims = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => db.Store.ClaimSnapshotAsync(request, Guid.NewGuid(), TimeSpan.FromMinutes(1))));
        var claim = Assert.Single(claims, c => c is not null)!; var row = await db.Store.BeginSnapshotAsync(claim, "view");
        var paused = await db.Store.ControlSnapshotAsync(request.Definition, request.Id, row.Version, true, false, "operator", "review");
        await Assert.ThrowsAsync<SnapshotLeaseLostException>(() => db.Store.AppendSnapshotAsync(claim, row.Generation, row.Page, new([], [], true)));
        Assert.Empty(await db.Store.ListSnapshotsAsync(request.Definition));
        var cancelled = await db.Store.ControlSnapshotAsync(request.Definition, request.Id, paused.Version, false, true, "operator", "cancel");
        Assert.Equal(SnapshotState.Cancelled, cancelled.State); Assert.Null(await db.Store.ClaimSnapshotAsync(request, Guid.NewGuid(), TimeSpan.FromMinutes(1)));
    }

    [Theory]
    [InlineData(SnapshotDelivery.Outbox)]
    [InlineData(SnapshotDelivery.Cdc)]
    public async Task Frozen_delivery_and_broker_redelivery_preserve_one_job(SnapshotDelivery delivery)
    {
        var request = await Setup(delivery: delivery); var lease = await Start(request);
        await db.Store.AppendSnapshotAsync(lease, 1, 0, new([Item("a")], [1], true));
        var batch = await db.Store.ReadSnapshotBatchAsync(lease); var envelope = SnapshotBatchCodec.Create(batch.Progress, batch.Items);
        await db.Store.PublishSnapshotBatchAsync(lease, batch);
        var outbox = await db.Outbox.FindAsync(request.Definition.OutputScope, envelope.BatchId);
        Assert.Equal(delivery == SnapshotDelivery.Outbox, outbox is not null);
        Assert.Null(await db.Claim(request.Definition.Target.WorkScope));
        var receiver = new SnapshotBatchReceiver(db.Store, request.Definition.Target); var bytes = SnapshotBatchCodec.Encode(envelope);
        Assert.Equal(JobSubmissionStatus.Accepted, (await receiver.AcceptAsync(bytes)).Status);
        Assert.Equal(JobSubmissionStatus.AlreadyAccepted, (await receiver.AcceptAsync(bytes)).Status);
        Assert.NotNull(await db.Claim(request.Definition.Target.WorkScope)); Assert.Null(await db.Claim(request.Definition.Target.WorkScope));
        var changed = new SnapshotDefinition(request.Definition.Target, "selection", "source", "v1", delivery: SnapshotDelivery.LocalJob);
        await Assert.ThrowsAsync<SnapshotConflictException>(() => db.Store.CreateSnapshotAsync(new(changed, request.RequestId, request.BusinessKey, request.Actor, request.CopyParameters())));
    }

    [Fact]
    public async Task Native_source_uses_one_read_only_consistent_view_and_parameterized_filter()
    {
        await using var c = await db.DataSource.OpenConnectionAsync();
        await using var setup = new SqlCommand($"CREATE TABLE {db.Schema}.source_rows(id int PRIMARY KEY,category int NOT NULL); INSERT INTO {db.Schema}.source_rows VALUES(1,5),(2,5),(3,7)", c);
        await setup.ExecuteNonQueryAsync();
        await using (var enable = new SqlCommand("ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON", c)) await enable.ExecuteNonQueryAsync();
        var source = new SqlServerSnapshotSource(db.DataSource, "source", "v1", (connection, transaction, parameters) =>
        {
            var command = new SqlCommand($"SELECT id FROM {db.Schema}.source_rows WHERE category=@category ORDER BY id", connection, transaction);
            command.Parameters.AddWithValue("category", JsonSerializer.Deserialize<int>(parameters.Span)); return command;
        }, reader => Item(reader.GetInt32(0).ToString()));
        await using var reader = await source.OpenAsync("5"u8.ToArray(), null);
        var first = await reader.ReadAsync(ReadOnlyMemory<byte>.Empty, 1); Assert.Equal("1", Assert.Single(first.Items).Key);
        setup.CommandText = $"INSERT INTO {db.Schema}.source_rows VALUES(4,5)"; await setup.ExecuteNonQueryAsync();
        var tail = await reader.ReadAsync(first.CopyNextCursor(), 100);
        Assert.Equal("2", Assert.Single(tail.Items).Key); Assert.True(tail.IsLast);
        await Assert.ThrowsAsync<InvalidOperationException>(() => source.OpenAsync("5"u8.ToArray(), reader.View));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Lost_page_or_batch_commit_reply_is_idempotent(bool batch)
    {
        var request = await Setup(); var lease = await Start(request);
        var page = new SnapshotPage([Item("a")], [1], true);
        SnapshotBatchCandidate? candidate = null;
        if (batch)
        {
            await db.Store.AppendSnapshotAsync(lease, 1, 0, page);
            candidate = await db.Store.ReadSnapshotBatchAsync(lease);
        }
        await using var proxy = new CommitReplyDropProxy();
        var settings = new SqlConnectionStringBuilder(db.ConnectionString) { DataSource = "127.0.0.1," + proxy.Port, Encrypt = false, Pooling = false };
        var source = new SqlServerDataSource(settings.ConnectionString);
        var interrupted = new SqlServerWorkStore(source, db.Schema);
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(async () =>
        {
            if (batch) await interrupted.PublishSnapshotBatchAsync(lease, candidate!);
            else await interrupted.AppendSnapshotAsync(lease, 1, 0, page);
        });
        await proxy.Dropped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        if (batch)
        {
            Assert.Equal(1, (await db.Store.PublishSnapshotBatchAsync(lease, candidate!)).BatchCount);
            Assert.NotNull(await db.Claim(request.Definition.Target.WorkScope)); Assert.Null(await db.Claim(request.Definition.Target.WorkScope));
        }
        else Assert.Equal(1, (await db.Store.AppendSnapshotAsync(lease, 1, 0, page)).ItemCount);
    }

    [Fact]
    public async Task Batch_receipt_failure_rolls_back_job_creation_and_progress()
    {
        var request = await Setup(); var lease = await Start(request);
        await db.Store.AppendSnapshotAsync(lease, 1, 0, new([Item("a")], [1], true));
        var candidate = await db.Store.ReadSnapshotBatchAsync(lease);
        await using var connection = await db.DataSource.OpenConnectionAsync();
        await using var command = new SqlCommand($"ALTER TABLE {db.Schema}.snapshot_batches ADD CONSTRAINT reject_batch CHECK(snapshot_id <> '{request.Id:D}')", connection);
        await command.ExecuteNonQueryAsync();
        try { await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(() => db.Store.PublishSnapshotBatchAsync(lease, candidate)); }
        finally { command.CommandText = $"ALTER TABLE {db.Schema}.snapshot_batches DROP CONSTRAINT reject_batch"; await command.ExecuteNonQueryAsync(); }
        Assert.Null(await db.Claim(request.Definition.Target.WorkScope));
        Assert.Equal(0, (await db.Store.FindSnapshotAsync(request.Definition, request.Id))!.BatchCount);
        Assert.Equal(1, (await db.Store.PublishSnapshotBatchAsync(lease, candidate)).BatchCount);
    }

    [Fact]
    public async Task Empty_selection_finishes_without_creating_empty_work()
    {
        var request = await Setup(); await db.Store.CreateSnapshotAsync(request);
        Assert.True(await new SnapshotWorker<Row>(db.Store, request.Definition, new Source([]), Items).RunOnceAsync(request));
        var row = (await db.Store.FindSnapshotAsync(request.Definition, request.Id))!;
        Assert.Equal(0, row.BatchCount); Assert.Equal(SnapshotState.Batched, row.State); Assert.Null(await db.Claim(request.Definition.Target.WorkScope));
    }

    [Fact]
    public async Task Staging_cleanup_is_bounded_and_keeps_batch_exports_and_duplicate_receipts()
    {
        var request = await Setup(delivery: SnapshotDelivery.Cdc); var lease = await Start(request);
        await db.Store.AppendSnapshotAsync(lease, 1, 0, new([Item("a"),Item("b")], [1], true));
        Assert.Equal(0, await db.Store.PruneSnapshotStagingAsync(request.Definition, request.Id, DateTimeOffset.UtcNow));
        var candidate = await db.Store.ReadSnapshotBatchAsync(lease); await db.Store.PublishSnapshotBatchAsync(lease, candidate);
        var exported = Assert.Single(await db.Store.ReadSnapshotBatchesAsync(request.Definition, request.Id));
        await using var agingConnection = await db.DataSource.OpenConnectionAsync();
        await using var aging = new SqlCommand($"UPDATE {db.Schema}.snapshots SET updated_at=@past WHERE id=@id", agingConnection);
        aging.Parameters.AddWithValue("past", DateTimeOffset.UtcNow.AddDays(-1)); aging.Parameters.AddWithValue("id", request.Id); await aging.ExecuteNonQueryAsync();
        Assert.Equal(1, await db.Store.PruneSnapshotStagingAsync(request.Definition, request.Id, DateTimeOffset.UtcNow, 1));
        Assert.Equal(1, await db.Store.PruneSnapshotStagingAsync(request.Definition, request.Id, DateTimeOffset.UtcNow, 1));
        Assert.Equal(0, await db.Store.PruneSnapshotStagingAsync(request.Definition, request.Id, DateTimeOffset.UtcNow));
        Assert.Equal(exported.Payload, Assert.Single(await db.Store.ReadSnapshotBatchesAsync(request.Definition, request.Id)).Payload);
        Assert.Equal(1, (await db.Store.PublishSnapshotBatchAsync(lease, candidate)).BatchCount);
        Assert.Equal(SnapshotState.Batched, (await db.Store.CreateSnapshotAsync(request)).State);
    }

    private sealed record Row(string Id, string Value);
    private sealed class Source(SnapshotItem[] items) : ISnapshotSource
    {
        public string Name => "source"; public string Version => "v1"; public SnapshotConsistency Consistency => SnapshotConsistency.SessionBound;
        public Task<ISnapshotReader> OpenAsync(ReadOnlyMemory<byte> parameters, string? resumeView, CancellationToken cancellationToken = default) =>
            Task.FromResult<ISnapshotReader>(new Reader(items));
        private sealed class Reader(SnapshotItem[] values) : ISnapshotReader
        {
            private int _offset;
            public string View { get; } = Guid.NewGuid().ToString("D");
            public Task<SnapshotPage> ReadAsync(ReadOnlyMemory<byte> cursor, int maximumItems, CancellationToken cancellationToken = default)
            {
                var page = values.Skip(_offset).Take(maximumItems).ToArray(); _offset += page.Length;
                return Task.FromResult(new SnapshotPage(page, BitConverter.GetBytes(_offset), _offset == values.Length));
            }
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
