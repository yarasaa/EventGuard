using System.Text.Json;
using EventGuard;

namespace EventGuard.ProcessSupport;

// Linked only into the two native test executables. Every child owns its own store,
// connections and process; no parent-side in-memory lock coordinates execution.
internal static class CoordinatedWorker
{
    public static async Task<int> RunAsync<T>(string mode, ITransactionalWorkStore<T> store, WorkScope scope,
        Func<WorkItem, ReadOnlyMemory<byte>, T, CancellationToken, ValueTask<Guid>> write)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        try
        {
            var ct = deadline.Token;
            if (mode == "held")
            {
                var claim = await store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(1), ct)
                    ?? throw new InvalidOperationException("No held work.");
                var result = await store.ExecuteAsync(claim, async (work, payload, session, token) =>
                {
                    var intent = await write(work, payload, session, token);
                    await Signal(new { stage = "transaction-open", pid = Environment.ProcessId, workId = work.Id,
                        intentId = intent, token = claim.Token, leaseUntil = claim.LeaseUntil });
                    if (await Console.In.ReadLineAsync(token) != "commit") throw new InvalidOperationException("Commit command required.");
                }, new(), ct);
                if (result.Status != ExecutionStatus.Completed) throw new InvalidOperationException("Held execution did not complete.");
                await Signal(new { stage = "completed", pid = Environment.ProcessId, workId = result.Work.Id });
                return 0;
            }
            if (mode != "peer") throw new ArgumentException("Unknown coordinated worker mode.");
            await Signal(new { stage = "ready", pid = Environment.ProcessId });
            if (await Console.In.ReadLineAsync(ct) != "start") throw new InvalidOperationException("Start command required.");
            var stop = 0; var completed = 0; var empty = 0;
            async Task Run()
            {
                while (Volatile.Read(ref stop) == 0)
                {
                    // Normal stop takes effect between native operations. The test deadline
                    // still cancels an outstanding operation and makes the child fail.
                    var claim = await store.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10), ct);
                    if (claim is null)
                    {
                        if (++empty == 1) await Signal(new { stage = "idle", pid = Environment.ProcessId });
                        await Task.Delay(10, ct); continue;
                    }
                    var result = await store.ExecuteAsync(claim, async (work, payload, session, token) =>
                    { await write(work, payload, session, token); }, new(), ct);
                    if (result.Status != ExecutionStatus.Completed) throw new InvalidOperationException("Unexpected peer execution outcome.");
                    completed++;
                }
            }
            var run = Run();
            var command = Console.In.ReadLineAsync(ct).AsTask();
            if (await Task.WhenAny(run, command) == run) { await run; throw new InvalidOperationException("Peer stopped without a command."); }
            if (await command != "stop") throw new InvalidOperationException("Stop command required.");
            Volatile.Write(ref stop, 1); await run;
            await Signal(new { stage = "stopped", pid = Environment.ProcessId, completed, empty });
            return 0;
        }
        catch (Exception error)
        {
            // Never emit connection strings, payloads or native exception messages.
            Console.Error.WriteLine(error.GetType().Name);
            return 1;
        }
    }
    private static async Task Signal(object value)
    {
        Console.WriteLine(JsonSerializer.Serialize(value)); await Console.Out.FlushAsync();
    }
}
