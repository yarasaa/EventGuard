using System.Globalization;

namespace EventGuard.Store.Conformance;

public sealed record ProcessLedgerRow(string Kind, Guid WorkId, Guid ReferenceId, string Value);
public sealed record ProcessScenarioEvidence(bool OwnerKilled, int[] ProcessIds, Guid HeldWorkId,
    int Works, int DuplicateDeliveries, int ConflictingDeliveries, int Effects, int OutboxIntents, int AbandonedAttempts);

public static class MultiProcessScenario
{
    public static async Task<ProcessScenarioEvidence> VerifyAsync<T>(IWorkStoreFixture<T> db, IOutboxStore outbox,
        Func<WorkScope, string, NativeWorkerProcess> start,
        Func<WorkScope, CancellationToken, Task<IReadOnlyList<ProcessLedgerRow>>> readLedger, bool killOwner)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(75));
        var ct = deadline.Token;
        var scope = StoreContractData.Scope();
        var producer = new ProducerScope(scope.Tenant, scope.Consumer);
        var heldPayload = new byte[] { 42, 1 };
        var held = (await db.Store.AcceptAsync(StoreContractData.Submission(scope, "held", heldPayload), ct)).Work;
        var expected = new Dictionary<Guid, byte[]> { [held.Id] = heldPayload };
        await using var owner = start(scope, "held");
        var signal = await owner.ReadAsync("transaction-open", ct);
        Assert.Equal(held.Id, signal.GetProperty("workId").GetGuid());
        var uncommittedIntent = signal.GetProperty("intentId").GetGuid();
        var owned = (await db.Store.FindAsync(scope, held.Id, ct))!;
        var stale = new WorkClaim(owned, signal.GetProperty("token").GetGuid(), signal.GetProperty("leaseUntil").GetDateTimeOffset(), heldPayload);

        await using var first = start(scope, "peer");
        await using var second = start(scope, "peer");
        await first.ReadAsync("ready", ct); await second.ReadAsync("ready", ct);
        var pids = new[] { owner.Id, first.Id, second.Id };
        Assert.Equal(3, pids.Distinct().Count()); Assert.DoesNotContain(Environment.ProcessId, pids);
        await first.SendAsync("start", ct); await second.SendAsync("start", ct);
        await first.ReadAsync("idle", ct); await second.ReadAsync("idle", ct);
        var delay = stale.LeaseUntil - DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(100);
        if (delay > TimeSpan.Zero) await Task.Delay(delay, ct);
        Assert.False(owner.HasExited); Assert.False(first.HasExited); Assert.False(second.HasExited);
        Assert.Null(await db.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10), ct));
        Assert.Equal(0, await db.EffectCount(held.Id));
        Assert.Null(await outbox.FindAsync(producer, uncommittedIntent, ct));
        var stillOwned = (await db.Store.FindAsync(scope, held.Id, ct))!;
        Assert.Equal(WorkState.Processing, stillOwned.State); Assert.Equal(1, stillOwned.AttemptCount);

        // New work and redeliveries arrive while both peers contend and the expired
        // owner's real transaction remains open. No warm-up or throughput assertion.
        var conflicts = new Dictionary<Guid, string>();
        var duplicateDeliveries = 0;
        for (var i = 0; i < 24; i++)
        {
            var key = "new-" + i.ToString(CultureInfo.InvariantCulture);
            var payload = new byte[] { (byte)i, 7 };
            var accepted = await db.Store.AcceptAsync(StoreContractData.Submission(scope, key, payload), ct);
            Assert.Equal(AcceptanceStatus.Accepted, accepted.Status); expected.Add(accepted.Work.Id, payload);
            var repeated = await Task.WhenAll(Enumerable.Range(0, 2).Select(_ =>
                db.RestartStore().AcceptAsync(StoreContractData.Submission(scope, key, payload), ct)));
            Assert.All(repeated, r => { Assert.Equal(AcceptanceStatus.Duplicate, r.Status); Assert.Equal(accepted.Work.Id, r.Work.Id); });
            duplicateDeliveries += repeated.Length;
            var changed = StoreContractData.Submission(scope, key, [(byte)i, 255]);
            var conflict = await db.Store.AcceptAsync(changed, ct);
            Assert.Equal(AcceptanceStatus.Conflict, conflict.Status); Assert.Equal(accepted.Work.Id, conflict.Work.Id);
            conflicts.Add(accepted.Work.Id, changed.Fingerprint.Value);
        }
        await CompletedAsync(expected.Keys.Where(id => id != held.Id));
        Assert.False(owner.HasExited); Assert.False(first.HasExited); Assert.False(second.HasExited);
        Assert.Equal(0, await db.EffectCount(held.Id));
        Assert.Equal(1, (await db.Store.FindAsync(scope, held.Id, ct))!.AttemptCount);
        Assert.Null(await outbox.FindAsync(producer, uncommittedIntent, ct));

        if (killOwner) await owner.KillAsync(ct);
        else
        {
            await owner.SendAsync("commit", ct);
            await owner.ReadAsync("completed", ct);
            Assert.Equal(0, await owner.ExitAsync(ct));
        }
        await CompletedAsync([held.Id]);
        await first.SendAsync("stop", ct); await second.SendAsync("stop", ct);
        var firstStop = await first.ReadAsync("stopped", ct); var secondStop = await second.ReadAsync("stopped", ct);
        Assert.Equal(0, await first.ExitAsync(ct)); Assert.Equal(0, await second.ExitAsync(ct));
        Assert.Equal(24 + (killOwner ? 1 : 0), firstStop.GetProperty("completed").GetInt32() + secondStop.GetProperty("completed").GetInt32());
        Assert.True(firstStop.GetProperty("empty").GetInt32() > 0); Assert.True(secondStop.GetProperty("empty").GetInt32() > 0);
        Assert.Equal(ExecutionStatus.LeaseLost, (await db.Store.ExecuteAsync(stale, db.Effect, new(), ct)).Status);
        var finalHeld = (await db.RestartStore().FindAsync(scope, held.Id, ct))!;
        var replay = new ReplayRequest(scope, Guid.NewGuid(), held.Id, finalHeld.Version, "local-test", "completed cannot reopen");
        var refused = await db.Store.ReplayAsync(replay, ct);
        Assert.Equal(ReplayStatus.NotFailed, refused.Status);
        Assert.Equal(refused with { RepeatedRequest = true }, await db.RestartStore().ReplayAsync(replay, ct));

        var rows = await readLedger(scope, ct);
        Assert.All(rows, row => Assert.Contains(row.WorkId, expected.Keys));
        Assert.Equal(25, rows.Count(r => r.Kind == "work"));
        Assert.Equal(25, rows.Count(r => r.Kind == "effect"));
        Assert.Equal(25, rows.Count(r => r.Kind == "outbox"));
        Assert.Equal(25 + (killOwner ? 1 : 0), rows.Count(r => r.Kind == "attempt"));
        Assert.Equal(24, rows.Count(r => r.Kind == "conflict"));
        var replayRow = Assert.Single(rows, r => r.Kind == "replay");
        Assert.Equal(replay.RequestId, replayRow.ReferenceId);
        Assert.Equal(((int)ReplayStatus.NotFailed).ToString(CultureInfo.InvariantCulture), replayRow.Value);
        foreach (var (id, payload) in expected)
        {
            var work = (await db.RestartStore().FindAsync(scope, id, ct))!;
            Assert.Equal(WorkState.Completed, work.State); Assert.Equal(0, work.ReplayGeneration);
            Assert.Equal(ContentFingerprint.FromBytes(payload), work.Fingerprint);
            Assert.Equal(work.Fingerprint.Value, Assert.Single(rows, r => r.Kind == "work" && r.WorkId == id).Value);
            Assert.Equal(id == held.Id && killOwner ? 2 : 1, work.AttemptCount);
            var effect = Assert.Single(rows, r => r.Kind == "effect" && r.WorkId == id);
            var note = effect.Value.Split('|'); Assert.Equal(2, note.Length);
            var actor = int.Parse(note[0], CultureInfo.InvariantCulture);
            Assert.Contains(actor, pids); Assert.Equal(ContentFingerprint.FromBytes(payload).Value, note[1]);
            if (id == held.Id) Assert.Equal(!killOwner, actor == owner.Id);
            var intent = Assert.Single(rows, r => r.Kind == "outbox" && r.WorkId == id);
            var outgoing = (await outbox.FindAsync(producer, intent.ReferenceId, ct))!;
            Assert.Equal(id, outgoing.OriginWorkId); Assert.Null(outgoing.OriginRequestId);
            Assert.Equal(OutboxState.Pending, outgoing.State); Assert.Equal(0, outgoing.AttemptCount);
            Assert.Equal(payload, outgoing.Message.CopyPayload()); Assert.Equal(id, outgoing.Message.MessageId);
            if (id == held.Id) Assert.Equal(!killOwner, intent.ReferenceId == uncommittedIntent);
            Assert.Single(rows, r => r.Kind == "attempt" && r.WorkId == id && r.Value == "Completed");
            Assert.Equal(id == held.Id && killOwner ? 1 : 0, rows.Count(r => r.Kind == "attempt" && r.WorkId == id && r.Value == "Abandoned"));
            if (conflicts.TryGetValue(id, out var fingerprint))
                Assert.Equal(fingerprint, Assert.Single(rows, r => r.Kind == "conflict" && r.WorkId == id).Value);
        }
        Assert.Null(await db.Store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10), ct));
        return new(killOwner, pids, held.Id, rows.Count(r => r.Kind == "work"), duplicateDeliveries,
            rows.Count(r => r.Kind == "conflict"), rows.Count(r => r.Kind == "effect"), rows.Count(r => r.Kind == "outbox"),
            rows.Count(r => r.Kind == "attempt" && r.Value == "Abandoned"));

        async Task CompletedAsync(IEnumerable<Guid> ids)
        {
            var pending = ids.ToHashSet();
            while (pending.Count > 0)
            {
                ct.ThrowIfCancellationRequested();
                foreach (var id in pending.ToArray())
                    if ((await db.Store.FindAsync(scope, id, ct))?.State == WorkState.Completed) pending.Remove(id);
                if (pending.Count > 0) await Task.Delay(20, ct);
            }
        }
    }
}
