using System.Text.Json;
using EventGuard.Store.Conformance;

namespace EventGuard.Operations.Tests;

public abstract class OutboxReadStoreContract(IOutboxStoreFixture db)
{
    protected abstract Task<IWorkReadStore> OpenAsync();
    protected abstract Task SetCreatedAt(ProducerScope scope, DateTimeOffset time);
    protected abstract Task CorruptHeaders(ProducerScope scope, Guid id);
    protected static ProducerScope Scope() => new(Guid.NewGuid().ToString("N"), "publisher");
    protected static OutgoingMessage Message(ProducerScope scope) => new(scope, Guid.NewGuid(), Guid.NewGuid(),
        new("kafka", "document-events", "v1", "sensitive-address", "sensitive-routing-key"),
        "sensitive-payload"u8, headers: new Dictionary<string, string> { ["token"] = "sensitive-header" });
    private Task<OutboxClaim?> Claim(ProducerScope scope) => db.Outbox.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromMinutes(1));

    [Theory]
    [InlineData(2, false)][InlineData(3, false)][InlineData(4, true)]
    public async Task Backlog_caps_outbox_states_without_reading_envelopes(int count, bool lowerBound)
    {
        var scope = Scope();
        await db.EnqueueAsync(Message(scope));
        var oldest = DateTimeOffset.Parse("2020-01-01T00:00:00Z"); await SetCreatedAt(scope, oldest);
        for (var i = 1; i < count; i++) await db.EnqueueAsync(Message(scope));
        var source = await OpenAsync(); await using var cleanup = (IAsyncDisposable)source;
        var reader = new OperationsReader(source);
        var summary = (await reader.ReadBacklogAsync(BacklogQuery.Outbox(scope, 3))).Value;
        var pending = Assert.Single(summary.Buckets, b => b.State == "Pending");
        Assert.Equal(Math.Min(count, 3), pending.Count); Assert.Equal(lowerBound, pending.IsLowerBound); Assert.Equal(oldest, pending.OldestCreatedAt);
        Assert.DoesNotContain(summary.Buckets, b => b.State == "Published");
        Assert.All((await reader.ReadBacklogAsync(BacklogQuery.Outbox(Scope()))).Value.Buckets, b => { Assert.Equal(0, b.Count); Assert.Null(b.OldestCreatedAt); });
        await db.Outbox.SettleAsync((await Claim(scope))!, new(PublishStatus.Accepted, "test-accepted"), new());
        var after = (await reader.ReadBacklogAsync(BacklogQuery.Outbox(scope, 3))).Value;
        Assert.Equal(count - 1, Assert.Single(after.Buckets, b => b.State == "Pending").Count);
        Assert.All(after.Buckets, b => Assert.False(b.IsLowerBound));
    }

    [Fact]
    public async Task Native_keyset_handles_equal_times_new_inserts_and_both_page_directions()
    {
        var scope = Scope();
        for (var i = 0; i < 11; i++) await db.EnqueueAsync(Message(scope));
        var cutoff = DateTimeOffset.Parse("2020-01-01T00:00:00Z");
        await SetCreatedAt(scope, cutoff);
        var source = await OpenAsync(); await using var cleanup = (IAsyncDisposable)source;
        var reader = new OperationsReader(source);
        var first = (await reader.ListOutboxAsync(new(scope, cutoff, OutboxState.Pending, 3))).Value;
        await db.EnqueueAsync(Message(scope));
        var pages = new List<OutboxPage> { first };
        while (pages[^1].Next is { } next) pages.Add((await reader.ListOutboxAsync(new(scope, cutoff, OutboxState.Pending, 3, before: next))).Value);
        Assert.Equal(11, pages.SelectMany(p => p.Items).Select(i => i.Id).Distinct().Count());
        Assert.Equal(11, pages.Sum(p => p.Items.Count));
        var current = pages[^1];
        for (var i = pages.Count - 2; i >= 0; i--)
        {
            Assert.NotNull(current.Previous);
            current = (await reader.ListOutboxAsync(new(scope, cutoff, OutboxState.Pending, 3, after: current.Previous))).Value;
            Assert.Equal(pages[i].Items.Select(x => x.Id), current.Items.Select(x => x.Id));
        }
        Assert.Null(current.Previous);
    }

    [Fact]
    public async Task Filters_combine_dates_state_id_attempts_replay_and_sticky_uncertainty()
    {
        var scope = Scope(); var message = Message(scope); await db.EnqueueAsync(message);
        var failed = await db.Outbox.SettleAsync((await Claim(scope))!, new(PublishStatus.Unknown, "kafka-delivery-unknown"), new(maximumAttempts: 1));
        var request = new OutboxReplayRequest(scope, Guid.NewGuid(), message.IntentId, failed.Record.Version, "sensitive-actor", "sensitive-reason", true);
        await db.Recovery.ReplayAsync(request);
        await db.Outbox.SettleAsync((await Claim(scope))!, new(PublishStatus.Accepted, "kafka-persisted"), new());
        await db.EnqueueAsync(Message(scope));
        var time = DateTimeOffset.Parse("2020-01-01T00:00:00Z"); await SetCreatedAt(scope, time);
        var source = await OpenAsync(); await using var cleanup = (IAsyncDisposable)source; var reader = new OperationsReader(source);
        var filters = new OutboxFilters(time, time.AddSeconds(1), message.IntentId, 2, true, true);
        var page = (await reader.ListOutboxAsync(new(scope, time.AddDays(1), OutboxState.Published, filters: filters))).Value;
        Assert.True(Assert.Single(page.Items).DeliveryUncertain);
        foreach (var excluded in new[] { filters with { CreatedFrom = time.AddMilliseconds(1) }, filters with { CreatedFrom = time.AddSeconds(-1), CreatedUntil = time },
            filters with { IntentId = Guid.NewGuid() }, filters with { MinimumAttempts = 3 }, filters with { Replayed = false }, filters with { DeliveryUncertain = false } })
            Assert.Empty((await reader.ListOutboxAsync(new(scope, time.AddDays(1), filters: excluded))).Value.Items);
        Assert.Empty((await reader.ListOutboxAsync(new(scope, time.AddDays(1), OutboxState.Pending, filters: filters))).Value.Items);
        var detail = (await reader.FindOutboxAsync(scope, message.IntentId)).Value!;
        Assert.Equal(new[] { "Accepted", "Unknown" }, detail.Attempts.Select(a => a.Status));
        Assert.True(Assert.Single(detail.Replays).AcknowledgePossibleDuplicate);
        Assert.Equal(request.RequestId, detail.Replays[0].RequestId);
        Assert.NotNull(detail.Message.PublishedAt);
        Assert.DoesNotContain("sensitive", JsonSerializer.Serialize(detail));
    }

    [Fact]
    public async Task List_and_detail_are_isolated_by_tenant_and_producer_even_with_same_intent_id()
    {
        var scope = Scope(); var message = Message(scope); await db.EnqueueAsync(message);
        var source = await OpenAsync(); await using var cleanup = (IAsyncDisposable)source; var reader = new OperationsReader(source);
        foreach (var other in new[] { new ProducerScope(scope.Tenant + "other", scope.Producer), new ProducerScope(scope.Tenant, scope.Producer + "other") })
        {
            Assert.Null((await reader.FindOutboxAsync(other, message.IntentId)).Value);
            Assert.Empty((await reader.ListOutboxAsync(new(other, DateTimeOffset.UtcNow.AddDays(1)))).Value.Items);
            await db.EnqueueAsync(new(other, message.IntentId, Guid.NewGuid(), message.Route, "other-tenant"u8));
            var otherDetail = (await reader.FindOutboxAsync(other, message.IntentId)).Value!;
            Assert.NotEqual(message.MessageId, otherDetail.Message.MessageId);
        }
        Assert.Equal(message.MessageId, (await reader.FindOutboxAsync(scope, message.IntentId)).Value!.Message.MessageId);
        Assert.Null((await reader.FindOutboxAsync(scope, Guid.NewGuid())).Value);
    }

    [Fact]
    public async Task Corrupt_envelope_remains_readable_and_quarantine_does_not_expose_raw_snapshot()
    {
        var scope = Scope(); var message = Message(scope); await db.EnqueueAsync(message);
        await CorruptHeaders(scope, message.IntentId);
        var source = await OpenAsync(); await using var cleanup = (IAsyncDisposable)source; var reader = new OperationsReader(source);
        Assert.Equal(OutboxState.Pending, (await reader.FindOutboxAsync(scope, message.IntentId)).Value!.Message.State);
        Assert.Null(await Claim(scope));
        Assert.Equal(1, Assert.Single((await reader.ReadBacklogAsync(BacklogQuery.Outbox(scope))).Value.Buckets, b => b.State == "Quarantined").Count);
        var detail = (await reader.FindOutboxAsync(scope, message.IntentId)).Value!;
        Assert.Equal(OutboxState.Quarantined, detail.Message.State);
        Assert.Equal("outbox-envelope-invalid", detail.Quarantine!.ReasonCode);
        Assert.Equal(0, detail.Quarantine.ObservedVersion);
        Assert.Single((await reader.ListOutboxAsync(new(scope, DateTimeOffset.UtcNow.AddDays(1), OutboxState.Quarantined))).Value.Items);
        var json = JsonSerializer.Serialize(detail);
        foreach (var hidden in new[] { "sensitive", "Payload", "Headers", "OriginalRecord", "LeaseToken", "Fingerprint", "Actor", "RoutingKey", "Address" }) Assert.DoesNotContain(hidden, json);
        Assert.Null((await reader.FindOutboxAsync(Scope(), message.IntentId)).Value);
    }

    [Fact]
    public async Task Histories_are_bounded_and_old_replay_repeats_do_not_add_rows()
    {
        var scope = Scope(); var message = Message(scope); await db.EnqueueAsync(message);
        for (var i = 0; i < 52; i++)
        {
            var failed = await db.Outbox.SettleAsync((await Claim(scope))!, new(PublishStatus.NotSentPermanent, "rabbitmq-unroutable"), new());
            var request = new OutboxReplayRequest(scope, Guid.NewGuid(), message.IntentId, failed.Record.Version, "actor", "reason");
            await db.Recovery.ReplayAsync(request); await db.Recovery.ReplayAsync(request);
        }
        var source = await OpenAsync(); await using var cleanup = (IAsyncDisposable)source;
        var detail = (await new OperationsReader(source).FindOutboxAsync(scope, message.IntentId)).Value!;
        Assert.Equal(52, detail.Message.ReplayGeneration);
        Assert.Equal(50, detail.Attempts.Count); Assert.True(detail.HasOlderAttempts); Assert.Equal(52, detail.Attempts[0].Number);
        Assert.Equal(20, detail.Replays.Count); Assert.True(detail.HasOlderReplays);
    }

    [Fact]
    public async Task Disposing_the_panel_pool_does_not_prevent_publication_settlement()
    {
        var scope = Scope(); var message = Message(scope); await db.EnqueueAsync(message);
        var source = await OpenAsync(); await ((IAsyncDisposable)source).DisposeAsync();
        var settled = await db.Outbox.SettleAsync((await Claim(scope))!, new(PublishStatus.Accepted, "rabbitmq-confirmed"), new());
        Assert.Equal(OutboxState.Published, settled.Record.State);
    }
}
