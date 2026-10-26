using System.Text.Json;

namespace EventGuard.LoadTests;

// Rollback-only plan evidence, separate from throughput. A due-time update models
// stale statistics after queue changes; it is not a production scheduling API.
internal static class ClaimDuePlanProbe
{
    internal const string DueExpression = "CASE WHEN state=1 THEN lease_until ELSE LEAST(next_attempt_at,expires_at) END";
    internal const string PreviousQuery = """
        SELECT id FROM {_schema}.works
        WHERE state IN (0,1,2) AND scope_id=@scope
            AND (expires_at<=statement_timestamp() OR (@fresh=1 AND attempt_count=0) OR (@retry=1 AND attempt_count>0)) AND (
            (state IN (0,2) AND (next_attempt_at<=statement_timestamp() OR expires_at<=statement_timestamp()))
            OR (state=1 AND lease_until<=statement_timestamp()))
        ORDER BY next_attempt_at,id LIMIT 1 FOR UPDATE SKIP LOCKED
        """;

    internal static async Task SeedAsync<T>(LoadDatabase<T> db, string shape, CancellationToken ct)
    {
        if (shape is not ("ready" or "scheduled-empty" or "leased-empty" or "expiry" or "recovery"))
            throw new ArgumentException("Unknown diagnostic shape.", nameof(shape));
        await db.ExecuteAsync($"TRUNCATE {db.Schema}.works CASCADE", ct);
        await db.ExecuteAsync($"""
            INSERT INTO {db.Schema}.works(id,identity_id,scope_id,business_key,message_id,fingerprint,payload,state,next_attempt_at)
            SELECT gen_random_uuid(),n::text,'probe',n::text,gen_random_uuid(),repeat('0',64),decode('00','hex'),0,
                statement_timestamp()+interval '1 day' FROM generate_series(1,100000) n;
            ANALYZE {db.Schema}.works;
            """, ct);
        var change = shape switch
        {
            "ready" => "next_attempt_at=statement_timestamp()-interval '1 hour'",
            "expiry" => "expires_at=statement_timestamp()-interval '1 hour'",
            "recovery" => "state=1,lease_token=gen_random_uuid(),lease_until=statement_timestamp()-interval '1 hour'",
            "leased-empty" => "state=1,lease_token=gen_random_uuid(),lease_until=statement_timestamp()+interval '1 day',expires_at=statement_timestamp()-interval '1 hour'",
            _ => null
        };
        if (change is not null) await db.ExecuteAsync($"UPDATE {db.Schema}.works SET {change}", ct);
    }

    public static async Task<int> RunAsync(string[] args)
    {
        if (args.Length != 1 || Path.Exists(args[0])) throw new ArgumentException("Use --claim-due-plans NEW_OUTPUT_DIRECTORY from the repository root.");
        var output = Path.GetFullPath(args[0]); Directory.CreateDirectory(output);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(10));
        var ct = deadline.Token;
        await using var db = LocalDatabases.PostgreSql(2);
        var observations = new List<object>(); var removed = false; string? error = null;
        var current = ClaimPlanProbe.ReadQuery("postgresql");
        try
        {
            await db.InitializeAsync(ct);
            await db.ExecuteAsync($"INSERT INTO {db.Schema}.policy_bindings VALUES('probe','probe','probe','probe','probe','v1','v1'); ALTER TABLE {db.Schema}.works SET (autovacuum_enabled=false)", ct);
            foreach (var shape in new[] { "ready", "scheduled-empty", "leased-empty", "expiry", "recovery" })
            {
                await SeedAsync(db, shape, ct);
                foreach (var candidate in new[] { false, true })
                {
                    await db.ExecuteAsync(candidate
                        ? $"CREATE INDEX ix_claim_due ON {db.Schema}.works(scope_id,({DueExpression}),id) WHERE state IN (0,1,2); DROP INDEX {db.Schema}.ix_claim_active"
                        : $"CREATE INDEX ix_claim_active ON {db.Schema}.works(scope_id,next_attempt_at,id) WHERE state IN (0,1,2); DROP INDEX {db.Schema}.ix_claim_due", ct);
                    foreach (var analyzed in new[] { false, true })
                    {
                        // Restore the same stale column statistics for each index layout.
                        // Re-seeding is outside every measured SELECT.
                        await SeedAsync(db, shape, ct);
                        if (analyzed) await db.ExecuteAsync($"ANALYZE {db.Schema}.works", ct);
                        for (var repetition = 1; repetition <= 3; repetition++)
                        {
                            var plan = await ClaimPlanProbe.PlanAsync(db, "postgresql", candidate ? current : PreviousQuery, ct);
                            var name = $"{shape}-{(candidate ? "current" : "dev23")}-{(analyzed ? "analyzed" : "stale")}-{repetition}.json";
                            await File.WriteAllTextAsync(Path.Combine(output, name), plan, ct);
                            using var parsed = JsonDocument.Parse(plan);
                            var entry = parsed.RootElement[0]; var root = entry.GetProperty("Plan");
                            observations.Add(new { Shape = shape, Candidate = candidate, Analyzed = analyzed, Repetition = repetition, Plan = name,
                                ExecutionMs = entry.GetProperty("Execution Time").GetDouble(), TempWrittenBlocks = root.GetProperty("Temp Written Blocks").GetInt64() });
                        }
                    }
                }
                Console.WriteLine("Claim plans captured: " + shape);
            }
        }
        catch (Exception e) { error = e.GetType().Name; }
        finally
        {
            try { using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30)); await db.CleanupAsync(cleanup.Token); removed = true; }
            catch (Exception e) { error ??= e.GetType().Name; }
            await File.WriteAllTextAsync(Path.Combine(output, "manifest.json"), JsonSerializer.Serialize(new {
                SchemaRemoved = removed, ErrorType = error, PreviousQuery, CurrentQuery = current, Observations = observations,
                Method = "100,000 one-byte synthetic records; dev.23/current index layouts; stale/analyzed statistics; three rollback-only EXPLAIN ANALYZE SELECTs. No WorkPump throughput or durability claim. Autovacuum disabled only for this disposable fixture."
            }, new JsonSerializerOptions { WriteIndented = true }));
        }
        return removed && error is null ? 0 : 1;
    }
}
