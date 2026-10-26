using System.Text.Json;
using System.Text.RegularExpressions;
using EventGuard.PostgreSql;
using Npgsql;
using NpgsqlTypes;

namespace EventGuard.LoadTests;

// Exact-ID queries must remain independent of the size of a changing queue.
// These rollback-only plans diagnose access paths, not end-to-end throughput.
internal static class PointLookupPlanProbe
{
    internal sealed record Fixture(Guid Id, Guid Token, DateTimeOffset NextAttempt);

    internal static string ReadQuery(string operation, string repository = ".")
    {
        var source = File.ReadAllText(Path.Combine(repository, "src/EventGuard.PostgreSql/PostgreSqlWorkStore.cs"));
        var (start, end, pattern) = operation switch
        {
            "read" => ("private async Task<WorkClaim?> ReadClaimAsync", "private async Task<WorkItem> PersistChangeAsync", "new NpgsqlCommand\\(\\$\"([^\"\\r\\n]+)\""),
            "update" => ("private async Task<WorkItem> PersistChangeAsync", "private static WorkLease? LeaseOf", "new NpgsqlCommand\\(\\$\"\"\"([\\s\\S]*?)\"\"\""),
            _ => throw new ArgumentException("Unknown point operation.", nameof(operation))
        };
        var first = source.IndexOf(start, StringComparison.Ordinal);
        var last = source.IndexOf(end, StringComparison.Ordinal);
        if (first < 0 || last <= first) throw new InvalidOperationException("Point-query source changed.");
        var commands = Regex.Matches(source[first..last], pattern);
        var columns = Regex.Match(source, "private const string Columns = \"([^\"]+)\";");
        if (commands.Count != 1 || !columns.Success) throw new InvalidOperationException("Point-query extraction failed.");
        var query = commands[0].Groups[1].Value.Trim().Replace("{Columns}", columns.Groups[1].Value, StringComparison.Ordinal);
        if (!query.Contains("{_schema}.works", StringComparison.Ordinal) || query.Replace("{_schema}", "", StringComparison.Ordinal).Contains('{'))
            throw new InvalidOperationException("Unexpected point-query template.");
        return query;
    }

    private static string PreviousQuery(string current, string operation)
    {
        var guard = operation == "read"
            ? @"AND \(scope_id,lease_token,state\) IS NOT DISTINCT FROM \(@scope,@token,1\)"
            : @"AND \(scope_id,state,version,lease_token\)\s+IS NOT DISTINCT FROM \(@scope,@expected_state,@expected_version,@expected_token\)";
        var previous = operation == "read"
            ? "AND scope_id=@scope AND lease_token=@token AND state=1"
            : "AND scope_id=@scope AND state=@expected_state AND version=@expected_version AND lease_token IS NOT DISTINCT FROM @expected_token";
        if (Regex.Matches(current, guard).Count != 1) throw new InvalidOperationException("Review the frozen dev.24 point guard.");
        return Regex.Replace(current, guard, previous);
    }

    internal static async Task<Fixture> SeedAsync(LoadDatabase<PostgreSqlSession> db, int count, CancellationToken ct)
    {
        if (count is not (100000 or 300000)) throw new ArgumentOutOfRangeException(nameof(count));
        await db.ExecuteAsync($"""
            INSERT INTO {db.Schema}.policy_bindings VALUES('probe','probe','probe','probe','probe','v1','v1') ON CONFLICT DO NOTHING;
            ALTER TABLE {db.Schema}.works SET (autovacuum_enabled=false);
            TRUNCATE {db.Schema}.works CASCADE;
            INSERT INTO {db.Schema}.works(id,identity_id,scope_id,business_key,message_id,fingerprint,payload,state,next_attempt_at)
            SELECT gen_random_uuid(),n::text,'probe',n::text,gen_random_uuid(),repeat('0',64),decode('00','hex'),0,
                statement_timestamp()+n*interval '1 microsecond' FROM generate_series(1,{count}) n;
            ANALYZE {db.Schema}.works;
            UPDATE {db.Schema}.works SET state=1,lease_token=gen_random_uuid(),lease_until=clock_timestamp()+interval '1 hour';
            UPDATE {db.Schema}.works SET state=3,lease_token=NULL,lease_until=NULL WHERE business_key::int < {count}-7;
            """, ct);
        await using var connection = await db.OpenAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT id,lease_token,next_attempt_at FROM {db.Schema}.works WHERE business_key='{count}'";
        await using var reader = await command.ExecuteReaderAsync(ct); await reader.ReadAsync(ct);
        return new(reader.GetGuid(0), reader.GetGuid(1), reader.GetFieldValue<DateTimeOffset>(2));
    }

    internal static async Task<string> PlanAsync(LoadDatabase<PostgreSqlSession> db, Fixture fixture, string operation,
        string query, CancellationToken ct, string? rejectedGuard = null)
    {
        await using var connection = (NpgsqlConnection)await db.OpenAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + query.Replace("{_schema}", db.Schema, StringComparison.Ordinal), connection, transaction);
        command.CommandTimeout = 30;
        command.Parameters.AddWithValue("id", fixture.Id);
        command.Parameters.AddWithValue("scope", rejectedGuard == "scope" ? "wrong-scope" : "probe");
        if (operation == "read") command.Parameters.AddWithValue("token", rejectedGuard == "token" ? Guid.NewGuid() : fixture.Token);
        else
        {
            command.Parameters.AddWithValue("state", 3); command.Parameters.AddWithValue("version", 1L);
            command.Parameters.AddWithValue("attempts", 1); command.Parameters.AddWithValue("generation_attempt", 1);
            command.Parameters.AddWithValue("generation", 0); command.Parameters.AddWithValue("next", fixture.NextAttempt);
            command.Parameters.AddWithValue("token", NpgsqlDbType.Uuid, DBNull.Value);
            command.Parameters.AddWithValue("until", NpgsqlDbType.TimestampTz, DBNull.Value);
            command.Parameters.AddWithValue("reason", NpgsqlDbType.Text, DBNull.Value);
            command.Parameters.AddWithValue("expected_state", rejectedGuard == "state" ? 0 : 1);
            command.Parameters.AddWithValue("expected_version", rejectedGuard == "version" ? 1L : 0L);
            command.Parameters.AddWithValue("expected_token", rejectedGuard == "token" ? Guid.NewGuid() : fixture.Token);
        }
        var result = (string)(await command.ExecuteScalarAsync(ct))!;
        await transaction.RollbackAsync(CancellationToken.None);
        return result;
    }

    public static async Task<int> RunAsync(string[] args)
    {
        if (args.Length != 1 || Path.Exists(args[0])) throw new ArgumentException("Use --point-lookup-plans NEW_OUTPUT_DIRECTORY from the repository root.");
        var output = Path.GetFullPath(args[0]); Directory.CreateDirectory(output);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(5)); var ct = deadline.Token;
        await using var db = LocalDatabases.PostgreSql(2);
        var observations = new List<object>(); bool removed = false; string? error = null;
        try
        {
            await db.InitializeAsync(ct);
            foreach (var count in new[] { 100000, 300000 })
            {
                var fixture = await SeedAsync(db, count, ct);
                foreach (var analyzed in new[] { false, true })
                {
                    if (analyzed) await db.ExecuteAsync($"ANALYZE {db.Schema}.works", ct);
                    foreach (var operation in new[] { "read", "update" })
                    foreach (var candidate in new[] { false, true })
                    for (var repetition = 1; repetition <= 3; repetition++)
                    {
                        var current = ReadQuery(operation);
                        var query = candidate ? current : PreviousQuery(current, operation);
                        var plan = await PlanAsync(db, fixture, operation, query, ct);
                        var name = $"{count}-{operation}-{(candidate ? "current" : "dev24")}-{(analyzed ? "analyzed" : "stale")}-{repetition}.json";
                        await File.WriteAllTextAsync(Path.Combine(output, name), plan, ct);
                        using var parsed = JsonDocument.Parse(plan); var entry = parsed.RootElement[0]; var root = entry.GetProperty("Plan");
                        if (root.GetProperty("Actual Rows").GetDouble() != 1) throw new InvalidOperationException("Point plan did not match one work record.");
                        observations.Add(new { Count = count, Operation = operation, Candidate = candidate, Analyzed = analyzed, Repetition = repetition,
                            Plan = name, ExecutionMs = entry.GetProperty("Execution Time").GetDouble(),
                            SharedBlocks = root.GetProperty("Shared Hit Blocks").GetInt64() + root.GetProperty("Shared Read Blocks").GetInt64() });
                    }
                }
                Console.WriteLine("Point lookup plans captured: " + count);
            }
        }
        catch (Exception e) { error = e.GetType().Name; Console.Error.WriteLine(e.Message); }
        finally
        {
            try { using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30)); await db.CleanupAsync(cleanup.Token); removed = true; }
            catch (Exception e) { error ??= e.GetType().Name; }
            await File.WriteAllTextAsync(Path.Combine(output, "manifest.json"), JsonSerializer.Serialize(new { SchemaRemoved = removed, ErrorType = error, Observations = observations,
                Method = "100k/300k one-byte fixtures; eight live leases after queue churn; stale/analyzed statistics; dev.24/current actual SQL; three rollback-only plans. Autovacuum disabled only for the disposable fixture. Not WorkPump throughput." }, new JsonSerializerOptions { WriteIndented = true }));
        }
        return removed && error is null ? 0 : 1;
    }
}
