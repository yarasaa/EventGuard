using EventGuard.Runtime;

namespace EventGuard.SqlServer.Tests;

public sealed class JobBatchTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    private static JsonMessageContract<Item> ItemContract() => new("item", 1, _ => 1,
        i => { if (i.Value < 0) throw new IngressMessageRejectedException("negative-value"); });
    private async Task<JobDefinition> Definition()
    {
        await db.Store.InitializeJobsAsync();
        var definition = new JobDefinition(new("tenant", Guid.NewGuid().ToString("N")), "batch", 1,
            "items", 1, "handler-1", [1], [JobCapabilities.TransactionalHandler, JobCapabilities.TransactionalItems]);
        await db.Store.RegisterAsync(definition); return definition;
    }
    private static JobBatch<Item> Batch(params Item[] items) => new(1, items.Select(i => new JobBatchItem<Item>(i.Key, i)).ToArray());
    private static JsonMessageContract<JobBatch<Item>> Contract() => JsonJobBatches.Contract("items", 1, ItemContract(), maximumItems: 100);
    private Task<JobSubmissionResult> Submit(JobDefinition d, string key, JobBatch<Item> batch) =>
        new JsonJobClient<JobBatch<Item>>(db.Store, d, Contract()).SubmitAsync(Guid.NewGuid(), key, "operator", batch);
    private Task<ExecutionResult> Execute(JobDefinition d, WorkClaim claim, bool fail = false)
    {
        var calls = 0;
        return db.Store.ExecuteAsync(claim, async (work, bytes, session, ct) =>
        {
            await JsonJobBatches.ExecuteAsync(d, Contract().Decode(bytes), ItemContract(), session,
                async (_, s, token) =>
                {
                    await db.Effect(work, ReadOnlyMemory<byte>.Empty, s, token);
                    if (fail && ++calls == 2) throw new InvalidOperationException("injected item failure");
                }, ct);
        }, new(classify: _ => FailureKind.Permanent));
    }

    [Fact]
    public async Task Rebatching_and_concurrent_workers_apply_each_item_once()
    {
        var d = await Definition();
        var a = await Submit(d, "batch-a", Batch(new("one", 1), new("two", 2)));
        var b = await Submit(d, "batch-b", Batch(new("three", 3), new("two", 2)));
        var first = (await db.Claim(d.WorkScope))!; var second = (await db.Claim(d.WorkScope))!;
        var results = await Task.WhenAll(Execute(d, first), Execute(d, second));
        Assert.All(results, r => Assert.Equal(ExecutionStatus.Completed, r.Status));
        Assert.Equal(3, await db.EffectCount(a.OperationId) + await db.EffectCount(b.OperationId));
    }

    [Fact]
    public async Task Failed_item_rolls_back_all_item_receipts_and_effects_in_the_batch()
    {
        var d = await Definition(); var batch = Batch(new("one", 1), new("two", 2));
        var a = await Submit(d, "batch-a", batch);
        Assert.Equal(ExecutionStatus.DeadLetter, (await Execute(d, (await db.Claim(d.WorkScope))!, true)).Status);
        Assert.Equal(0, await db.EffectCount(a.OperationId));
        var b = await Submit(d, "batch-b", batch);
        Assert.Equal(ExecutionStatus.Completed, (await Execute(d, (await db.Claim(d.WorkScope))!)).Status);
        Assert.Equal(2, await db.EffectCount(b.OperationId));
    }

    [Fact]
    public async Task Changed_item_content_is_rejected_and_cannot_replace_a_committed_effect()
    {
        var d = await Definition(); var a = await Submit(d, "batch-a", Batch(new Item("one", 1)));
        Assert.Equal(ExecutionStatus.Completed, (await Execute(d, (await db.Claim(d.WorkScope))!)).Status);
        var b = await Submit(d, "batch-b", Batch(new("one", 9), new("other", 2)));
        Assert.Equal(ExecutionStatus.DeadLetter, (await Execute(d, (await db.Claim(d.WorkScope))!)).Status);
        Assert.Equal(1, await db.EffectCount(a.OperationId)); Assert.Equal(0, await db.EffectCount(b.OperationId));
        var c = await Submit(d, "batch-c", Batch(new Item("other", 2)));
        Assert.Equal(ExecutionStatus.Completed, (await Execute(d, (await db.Claim(d.WorkScope))!)).Status);
        Assert.Equal(1, await db.EffectCount(c.OperationId));
    }

    [Fact]
    public async Task Duplicate_keys_and_invalid_items_fail_before_submission()
    {
        var d = await Definition();
        await Assert.ThrowsAsync<IngressMessageRejectedException>(() => Submit(d, "batch", Batch(new("one", 1), new("one", 1))));
        await Assert.ThrowsAsync<IngressMessageRejectedException>(() => Submit(d, "batch", Batch(new Item("one", -1))));
        Assert.Null(await db.Claim(d.WorkScope));
    }

    [Fact]
    public async Task Caught_item_failure_cannot_leave_partial_sql_or_a_false_applied_receipt()
    {
        var d = await Definition(); var a = await Submit(d, "batch", Batch(new Item("one", 1)));
        var executed = await db.Store.ExecuteAsync((await db.Claim(d.WorkScope))!, async (work, bytes, session, ct) =>
        {
            var item = new JobItemSubmission(d, "one", new("item", 1), [1]);
            await Assert.ThrowsAsync<InvalidOperationException>(() => session.JobItems.ApplyAsync(item, async token =>
            {
                await db.Effect(work, ReadOnlyMemory<byte>.Empty, session, token);
                throw new InvalidOperationException("injected failure");
            }, ct));
            Assert.Equal(JobItemStatus.Applied, await session.JobItems.ApplyAsync(item,
                token => db.Effect(work, ReadOnlyMemory<byte>.Empty, session, token), ct));
        }, new());
        Assert.Equal(ExecutionStatus.Completed, executed.Status); Assert.Equal(1, await db.EffectCount(a.OperationId));
    }

    public sealed record Item(string Key, int Value);
}
