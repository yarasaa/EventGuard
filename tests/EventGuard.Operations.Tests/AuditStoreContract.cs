using EventGuard.Store.Conformance;
namespace EventGuard.Operations.Tests;
public abstract class AuditStoreContract<TSession>(IWorkStoreFixture<TSession> db, IOutboxStoreFixture outbox)
{
    protected abstract Task<IWorkReadStore> OpenReadsAsync();
    protected abstract Task<IWorkReplayStore> OpenCommandsAsync();
    protected abstract Task SetTimesAsync(string scopeId, bool outgoing, DateTimeOffset timestamp);
    [Theory] [InlineData(false)] [InlineData(true)]
    public async Task Audit_seeks_tied_requests_forward_and_back_without_exposing_payloads(bool outgoing)
    {
        var scope = StoreContractData.Scope(); var producer = new ProducerScope(scope.Tenant, "publisher");
        var items = new List<BulkReplayItem>();
        for (var i = 0; i < 12; i++)
        {
            if (outgoing)
            {
                var message = new OutgoingMessage(producer, Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "secret-destination"), "secret-body"u8);
                await outbox.EnqueueAsync(message);
                var result = await outbox.Outbox.SettleAsync((await outbox.Outbox.ClaimNextAsync(producer, Guid.NewGuid(), TimeSpan.FromMinutes(1)))!, new(PublishStatus.Unknown, "reply-lost"), new(maximumAttempts: 1));
                items.Add(new(message.IntentId, result.Record.Version));
            }
            else
            {
                await db.Store.AcceptAsync(StoreContractData.Submission(scope, "audit-" + i));
                var result = await db.Store.ExecuteAsync((await db.Claim(scope))!, (_, _, _, _) => throw new PermanentWorkException("failed"), new());
                items.Add(new(result.Work.Id, result.Work.Version));
            }
        }
        var raw = await OpenCommandsAsync(); await using var closeCommands = (IAsyncDisposable)raw;
        var commands = new OperationsReplayer(raw);
        BulkReplayRequest plan = outgoing ? new(producer, Guid.NewGuid(), items, "audited-actor", "review <script>", true) : new(scope, Guid.NewGuid(), items, "audited-actor", "review <script>");
        Assert.Equal(BulkReplayStatus.Prepared, (await commands.RunBulkAsync(plan)).Status);
        var progress = outgoing ? await commands.FindBulkAsync(producer, plan.RequestId, plan.Actor) : await commands.FindBulkAsync(scope, plan.RequestId, plan.Actor);
        Assert.True(progress!.Complete); Assert.All(progress.Entries, e => Assert.Equal("Accepted", e.Status));
        Assert.Null(outgoing ? await commands.FindBulkAsync(producer, plan.RequestId, "another") : await commands.FindBulkAsync(scope, plan.RequestId, "another"));
        var time = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        await SetTimesAsync(plan.ScopeId, outgoing, time);
        var store = await OpenReadsAsync(); await using var cleanup = (IAsyncDisposable)store;
        var reader = new OperationsReader(store); Assert.True((await reader.CheckAuditHealthAsync(!outgoing, outgoing)).Value);
        AuditQuery Query(AuditPosition? before = null, AuditPosition? after = null) => outgoing
            ? new(producer, time.AddHours(-1), time.AddHours(1), 5, before, after) : new(scope, time.AddHours(-1), time.AddHours(1), 5, before, after);
        var first = (await reader.ListAuditAsync(Query())).Value;
        var second = (await reader.ListAuditAsync(Query(first.Next))).Value;
        var back = (await reader.ListAuditAsync(Query(after: second.Previous))).Value;
        Assert.Equal(first.Items, back.Items); Assert.Empty(first.Items.Select(e => e.RequestId).Intersect(second.Items.Select(e => e.RequestId)));
        var all = new List<AuditEntry>(first.Items); var page = first;
        while (page.Next is not null) { page = (await reader.ListAuditAsync(Query(page.Next))).Value; all.AddRange(page.Items); }
        Assert.Equal(13, all.Count); Assert.Equal(13, all.Select(e => e.RequestId).Distinct().Count());
        Assert.Single(all, e => e.Kind == AuditKind.BulkPlan); Assert.All(all, e => { Assert.Equal(plan.Actor, e.Actor); Assert.Equal(plan.Reason, e.Reason); Assert.Equal(outgoing, e.AcknowledgePossibleDuplicate); });
        Assert.DoesNotContain("secret-", System.Text.Json.JsonSerializer.Serialize(all));
        Assert.Empty((await reader.ListAuditAsync(new AuditQuery(StoreContractData.Scope(), time.AddHours(-1), time.AddHours(1)))).Value.Items);
        Assert.Empty((await reader.ListAuditAsync(outgoing ? new AuditQuery(producer, time.AddHours(-1), time) : new AuditQuery(scope, time.AddHours(-1), time))).Value.Items);
    }
    [Fact]
    public async Task Release_and_retention_receipts_are_visible_only_in_their_scope()
    {
        var scope = StoreContractData.Scope(); var ingress = (IIngressQuarantineStore)db.Store;
        var held = await ingress.QuarantineAsync(scope, new("v1", "sha256-bytes/v1"), new("secret-source", "secret-position", "test/v1", [1], [2]), "invalid");
        var release = new IngressReleaseRequest(Guid.NewGuid(), held.Item.Id, 0, StoreContractData.Submission(scope), "operator", "fixed");
        await ingress.ReleaseAsync(release);
        await ((IRetentionStore)db.Store).RetainAsync(new(scope, Guid.NewGuid(), RetentionTarget.ConsumerPayload, DateTimeOffset.UtcNow.AddDays(-10), "operator", "retention"));
        var store = await OpenReadsAsync(); await using var cleanup = (IAsyncDisposable)store;
        var page = await ((IAuditReadStore)store).ListAuditAsync(new(scope, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddMinutes(1)));
        Assert.Equal(2, page.Items.Count); Assert.Contains(page.Items, e => e.Kind == AuditKind.IngressRelease && e.SubjectId == held.Item.Id && e.Outcome == "Accepted");
        Assert.Contains(page.Items, e => e.Kind == AuditKind.Retention && e.SubjectId is null && e.Outcome == "Recorded");
        Assert.DoesNotContain("secret", System.Text.Json.JsonSerializer.Serialize(page));
    }
}
