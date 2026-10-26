using System.Text;
using System.Text.Json;
using EventGuard.Store.Conformance;

namespace EventGuard.Operations.Tests;

public abstract class ReadStoreContract<TSession>(IWorkStoreFixture<TSession> db)
{
    protected abstract Task<IWorkReadStore> OpenAsync();
    protected abstract Task SetCreatedAt(WorkScope scope, DateTimeOffset time);

    [Theory]
    [InlineData(2, false)][InlineData(3, false)][InlineData(4, true)]
    public async Task Backlog_bounds_each_state_and_preserves_oldest_and_scope(int count, bool lowerBound)
    {
        var scope = StoreContractData.Scope();
        await db.Store.AcceptAsync(StoreContractData.Submission(scope, "backlog0"));
        var oldest = DateTimeOffset.Parse("2020-01-01T00:00:00Z"); await SetCreatedAt(scope, oldest);
        for (var i = 1; i < count; i++) await db.Store.AcceptAsync(StoreContractData.Submission(scope, "backlog" + i));
        var source = await OpenAsync(); await using var cleanup = (IAsyncDisposable)source;
        var reader = new OperationsReader(source);
        var summary = (await reader.ReadBacklogAsync(BacklogQuery.Consumer(scope, 3))).Value;
        var pending = Assert.Single(summary.Buckets, b => b.State == "Pending");
        Assert.Equal(Math.Min(count, 3), pending.Count); Assert.Equal(lowerBound, pending.IsLowerBound); Assert.Equal(oldest, pending.OldestCreatedAt);
        Assert.All(summary.Buckets.Where(b => b.State != "Pending"), b => { Assert.Equal(0, b.Count); Assert.Null(b.OldestCreatedAt); Assert.False(b.IsLowerBound); });
        Assert.DoesNotContain(summary.Buckets, b => b.State == "Completed");
        var foreign = new WorkScope(scope.Tenant + "other", scope.Consumer, scope.Source, scope.EventType);
        Assert.All((await reader.ReadBacklogAsync(BacklogQuery.Consumer(foreign))).Value.Buckets, b => Assert.Equal(0, b.Count));
        await db.Store.ExecuteAsync((await db.Claim(scope))!, db.Effect, new());
        var after = (await reader.ReadBacklogAsync(BacklogQuery.Consumer(scope, 3))).Value;
        Assert.Equal(count - 1, Assert.Single(after.Buckets, b => b.State == "Pending").Count);
        Assert.All(after.Buckets, b => Assert.False(b.IsLowerBound));
    }

    [Fact]
    public async Task Advanced_filters_combine_with_scope_and_exclusive_date_boundaries()
    {
        var scope = StoreContractData.Scope();
        var work = (await db.Store.AcceptAsync(StoreContractData.Submission(scope, "replayed"))).Work;
        var failed = await db.Store.ExecuteAsync((await db.Claim(scope))!, (_, _, _, _) => throw new PermanentWorkException("test"), new());
        await db.Store.ReplayAsync(new(scope, Guid.NewGuid(), work.Id, failed.Work.Version, "operator", "test"));
        await db.Store.AcceptAsync(StoreContractData.Submission(scope, "never-replayed"));
        var time = DateTimeOffset.Parse("2020-01-01T00:00:00Z");
        await SetCreatedAt(scope, time);
        var source = await OpenAsync();
        await using var cleanup = (IAsyncDisposable)source;
        var filters = new WorkFilters(time, time.AddSeconds(1), work.Id, 1, true);
        var page = await source.ListAsync(new(scope, time.AddDays(1), filters: filters));
        Assert.Equal(work.Id, Assert.Single(page.Items).Id);
        Assert.Empty((await source.ListAsync(new(scope, time.AddDays(1), filters: filters with { CreatedFrom = time.AddMilliseconds(1) }))).Items);
        Assert.Empty((await source.ListAsync(new(scope, time.AddDays(1), filters: filters with { CreatedFrom = time.AddSeconds(-1), CreatedUntil = time }))).Items);
        Assert.Empty((await source.ListAsync(new(scope, time.AddDays(1), filters: filters with { MinimumAttempts = 2 }))).Items);
        Assert.Empty((await source.ListAsync(new(scope, time.AddDays(1), filters: filters with { Replayed = false }))).Items);
        Assert.Empty((await source.ListAsync(new(scope, time.AddDays(1), filters: filters with { WorkId = Guid.NewGuid() }))).Items);
        var other = new WorkScope(scope.Tenant + "other", scope.Consumer, scope.Source, scope.EventType);
        Assert.Empty((await source.ListAsync(new(other, time.AddDays(1), filters: filters))).Items);
    }

    [Fact]
    public async Task Previous_pages_restore_the_same_order_with_equal_timestamps_and_a_state_filter()
    {
        var scope = StoreContractData.Scope();
        for (var i = 0; i < 11; i++) await db.Store.AcceptAsync(StoreContractData.Submission(scope, "page" + i));
        var cutoff = DateTimeOffset.Parse("2020-01-01T00:00:00Z");
        await SetCreatedAt(scope, cutoff);
        var source = await OpenAsync();
        await using var cleanup = (IAsyncDisposable)source;
        var pages = new List<WorkPage> { await source.ListAsync(new(scope, cutoff, WorkState.Pending, 3)) };
        Assert.Null(pages[0].Previous);
        while (pages[^1].Next is { } next) pages.Add(await source.ListAsync(new(scope, cutoff, WorkState.Pending, 3, before: next)));
        Assert.Equal(4, pages.Count);
        Assert.Null(pages[^1].Next);
        var current = pages[^1];
        for (var i = pages.Count - 2; i >= 0; i--)
        {
            Assert.NotNull(current.Previous);
            current = await source.ListAsync(new(scope, cutoff, WorkState.Pending, 3, after: current.Previous));
            Assert.Equal(pages[i].Items.Select(w => w.Id), current.Items.Select(w => w.Id));
            var forward = await source.ListAsync(new(scope, cutoff, WorkState.Pending, 3, before: current.Next));
            Assert.Equal(pages[i + 1].Items.Select(w => w.Id), forward.Items.Select(w => w.Id));
        }
        Assert.Null(current.Previous);
    }

    [Fact]
    public async Task Data_projection_is_scoped_bounded_and_never_returns_unselected_content()
    {
        var scope = StoreContractData.Scope();
        var other = new WorkScope(scope.Tenant + "other", scope.Consumer, scope.Source, scope.EventType);
        var source = await OpenAsync();
        await using var cleanup = (IAsyncDisposable)source;
        var reader = new OperationsReader(source);
        var selection = new WorkDataSelection([new("Document", "/document/id"), new("Revision", "/document/revision")]);
        var inputs = new[]
        {
            ("{\"document\":{\"id\":\"DOC-1\",\"revision\":18},\"secret\":\"never-display\"}", WorkDataStatus.Available),
            ("not-json-never-display", WorkDataStatus.InvalidJson),
            ("{\"secret\":\"" + new string('x', 70_000) + "\"}", WorkDataStatus.TooLarge)
        };
        foreach (var (input, status) in inputs)
        {
            var bytes = Encoding.UTF8.GetBytes(input);
            var work = (await db.Store.AcceptAsync(new(new(scope, new("key/v1", "body/v1"), Guid.NewGuid().ToString()),
                Guid.NewGuid(), bytes, ContentFingerprint.FromBytes(bytes)))).Work;
            Assert.Null((await reader.ReadDataAsync(other, work.Id, selection)).Value);
            var data = (await reader.ReadDataAsync(scope, work.Id, selection)).Value!;
            Assert.Equal(status, data.Status);
            Assert.DoesNotContain("never-display", JsonSerializer.Serialize(data));
            if (status == WorkDataStatus.Available) Assert.Equal(new[] { "DOC-1", "18" }, data.Fields.Select(f => f.Value));
            else Assert.Empty(data.Fields);
        }
    }

    [Fact]
    public async Task Keyset_pages_with_identical_timestamps_do_not_skip_or_repeat_and_exclude_newer_inserts()
    {
        var scope = StoreContractData.Scope();
        var ids = new HashSet<Guid>();
        for (var i = 0; i < 11; i++) ids.Add((await db.Store.AcceptAsync(StoreContractData.Submission(scope, "item" + i))).Work.Id);
        // Fixed fixture time: the host and Docker VM clocks need not be synchronized.
        var cutoff = DateTimeOffset.Parse("2020-01-01T00:00:00Z");
        await SetCreatedAt(scope, cutoff);
        var source = await OpenAsync();
        await using var cleanup = (IAsyncDisposable)source;
        var result = new List<Guid>();
        var page = await source.ListAsync(new(scope, cutoff, pageSize: 3));
        // A subsequently accepted row is beyond the fixed creation cutoff.
        await db.Store.AcceptAsync(StoreContractData.Submission(scope, "later"));
        while (true)
        {
            Assert.InRange(page.Items.Count, 1, 3);
            result.AddRange(page.Items.Select(w => w.Id));
            if (page.Next is null) break;
            page = await source.ListAsync(new(scope, cutoff, pageSize: 3, before: page.Next));
        }
        Assert.Equal(11, result.Count);
        Assert.Equal(ids.Order(), result.Order());
    }

    [Fact]
    public async Task List_and_detail_isolate_tenant_and_consumer_and_apply_state_filter()
    {
        var scope = StoreContractData.Scope();
        var otherTenant = new WorkScope(scope.Tenant + "other", scope.Consumer, scope.Source, scope.EventType);
        var otherConsumer = new WorkScope(scope.Tenant, scope.Consumer + "other", scope.Source, scope.EventType);
        var accepted = (await db.Store.AcceptAsync(StoreContractData.Submission(scope))).Work;
        await db.Store.AcceptAsync(StoreContractData.Submission(otherTenant));
        await db.Store.AcceptAsync(StoreContractData.Submission(otherConsumer));
        await db.Store.ExecuteAsync((await db.Claim(scope))!, db.Effect, new());
        var source = await OpenAsync();
        await using var cleanup = (IAsyncDisposable)source;
        Assert.Single((await source.ListAsync(new(scope, DateTimeOffset.UtcNow, WorkState.Completed))).Items);
        Assert.Empty((await source.ListAsync(new(scope, DateTimeOffset.UtcNow, WorkState.Pending))).Items);
        Assert.Null(await source.FindAsync(otherTenant, accepted.Id));
        Assert.Null(await source.FindAsync(otherConsumer, accepted.Id));
        Assert.Null(await source.FindAsync(scope, Guid.NewGuid()));
        Assert.Equal(WorkState.Completed, (await source.FindAsync(scope, accepted.Id))!.Work.State);
    }

    [Fact]
    public async Task Metadata_shows_real_attempt_and_idempotent_replay_without_payload_key_correlation_or_actor()
    {
        const string secret = "sensitive-<script>alert(1)</script>";
        var scope = StoreContractData.Scope();
        var bytes = Encoding.UTF8.GetBytes(secret);
        var accepted = (await db.Store.AcceptAsync(new(new(scope, new("key/v1", "body/v1"), secret), Guid.NewGuid(),
            bytes, ContentFingerprint.FromBytes(bytes), correlationId: secret))).Work;
        var failed = await db.Store.ExecuteAsync((await db.Claim(scope))!, (_, _, _, _) => throw new PermanentWorkException(secret), new());
        var request = new ReplayRequest(scope, Guid.NewGuid(), accepted.Id, failed.Work.Version, secret, secret);
        await db.Store.ReplayAsync(request);
        await db.Store.ReplayAsync(request);
        await db.Store.ExecuteAsync((await db.Claim(scope))!, db.Effect, new());
        var source = await OpenAsync();
        await using var cleanup = (IAsyncDisposable)source;
        var detail = (await source.FindAsync(scope, accepted.Id))!;
        Assert.Equal(WorkState.Completed, detail.Work.State);
        Assert.Equal(new[] { "Completed", "Failed" }, detail.Attempts.Select(a => a.Status));
        Assert.Equal("permanent-failure", detail.Attempts[1].ReasonCode);
        Assert.Single(detail.Replays);
        Assert.Equal(request.RequestId, detail.Replays[0].RequestId);
        Assert.Equal(ReplayStatus.Accepted, detail.Replays[0].Status);
        var json = JsonSerializer.Serialize(detail);
        Assert.DoesNotContain("sensitive", json);
        foreach (var property in new[] { "Payload", "BusinessKey", "CorrelationId", "Fingerprint", "Actor", "LeaseToken" })
            Assert.DoesNotContain(property, json);
    }

    [Fact]
    public async Task Attempt_and_replay_history_are_bounded_and_report_truncation()
    {
        var scope = StoreContractData.Scope();
        var accepted = (await db.Store.AcceptAsync(StoreContractData.Submission(scope))).Work;
        for (var i = 0; i < 52; i++)
        {
            var failed = await db.Store.ExecuteAsync((await db.Claim(scope))!, (_, _, _, _) => throw new PermanentWorkException("test"), new());
            await db.Store.ReplayAsync(new(scope, Guid.NewGuid(), accepted.Id, failed.Work.Version, "operator", "test"));
        }
        var source = await OpenAsync();
        await using var cleanup = (IAsyncDisposable)source;
        var detail = (await source.FindAsync(scope, accepted.Id))!;
        Assert.Equal(50, detail.Attempts.Count);
        Assert.Equal(52, detail.Attempts[0].Number);
        Assert.True(detail.HasOlderAttempts);
        Assert.Equal(20, detail.Replays.Count);
        Assert.True(detail.HasOlderReplays);
    }

    [Fact]
    public async Task Stopping_the_operations_pool_does_not_stop_worker_completion()
    {
        var scope = StoreContractData.Scope();
        var work = (await db.Store.AcceptAsync(StoreContractData.Submission(scope))).Work;
        var source = await OpenAsync();
        await ((IAsyncDisposable)source).DisposeAsync();
        var result = await db.Store.ExecuteAsync((await db.Claim(scope))!, db.Effect, new());
        Assert.Equal(ExecutionStatus.Completed, result.Status);
        Assert.Equal(1, await db.EffectCount(work.Id));
    }
}
