using System.Data;
using System.Data.Common;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace EventGuard.LoadTests;

// A separate diagnostic, never part of a throughput measurement. Only generated local fixtures.
internal static class ClaimPlanProbe
{
    public static async Task<int> RunAsync(string[] args)
    {
        if (args.Length != 2 || args[0] is not ("postgresql" or "sqlserver"))
            throw new ArgumentException("Use --claim-plans postgresql|sqlserver NEW_OUTPUT_DIRECTORY from the repository root.");
        if (args[0] == "postgresql") return await ClaimDuePlanProbe.RunAsync([args[1]]);
        var output = Path.GetFullPath(args[1]);
        if (Directory.Exists(output) || File.Exists(output)) throw new ArgumentException("Choose a new output directory.");
        Directory.CreateDirectory(output);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(10));
        return args[0] == "postgresql"
            ? await RunAsync(LocalDatabases.PostgreSql(2),args[0],output,deadline.Token)
            : await RunAsync(LocalDatabases.SqlServer(2),args[0],output,deadline.Token);
    }

    internal static string ReadQuery(string provider,string repository = ".",bool budgetFilter = false)
    {
        var name = provider == "postgresql" ? "PostgreSql" : "SqlServer";
        var source = File.ReadAllText(Path.Combine(repository,$"src/EventGuard.{name}/{name}WorkStore.cs"));
        var start = source.IndexOf("public async Task<WorkClaim?> ClaimNextAsync",StringComparison.Ordinal);
        var end = source.IndexOf("public async Task<ExecutionResult> ExecuteAsync",start,StringComparison.Ordinal);
        var column = Regex.Match(source,"private const string Columns = \"([^\"]+)\";");
        var queries = Regex.Matches(source[start..end],"await using var select = new \\w+Command\\(\\$\"\"\"([\\s\\S]*?)\"\"\", connection, transaction\\);");
        if (!column.Success || queries.Count != 1) throw new InvalidOperationException("Claim SQL source changed; update the diagnostic extractor.");
        var query = queries[0].Groups[1].Value.Replace("{Columns}",column.Groups[1].Value,StringComparison.Ordinal).Trim();
        if (provider == "postgresql")
        {
            var budget = Regex.Match(source,"private const string ClaimBudgetFilter = \"([^\"]+)\";");
            if (!budget.Success || !query.Contains("{budgetFilter}",StringComparison.Ordinal))
                throw new InvalidOperationException("Claim quota SQL source changed; update the diagnostic extractor.");
            query = query.Replace("{budgetFilter}", budgetFilter ? budget.Groups[1].Value : "", StringComparison.Ordinal);
        }
        if (!query.Contains("{_schema}.works",StringComparison.Ordinal) || query.Replace("{_schema}","",StringComparison.Ordinal).Contains('{'))
            throw new InvalidOperationException("Unexpected claim SQL template.");
        return query;
    }

    private static async Task<int> RunAsync<T>(LoadDatabase<T> db,string provider,string output,CancellationToken ct)
    {
        await using var owned = db;
        var sourceQuery = ReadQuery(provider);
        var query = provider == "postgresql"
            ? sourceQuery.Replace("statement_timestamp()","clock_timestamp()",StringComparison.Ordinal)
            : LegacySqlServerQuery(sourceQuery);
        var candidateQuery = sourceQuery;
        var index = "ix_claim_expiry";
        var indexDefinition = "(scope_id,expires_at,id) WHERE state IN(0,2)";
        var observations = new List<object>();
        var removed = false;
        string? errorType = null;
        Dictionary<string,string>? profile = null;
        try
        {
            await db.InitializeAsync(ct);
            profile = await db.ProfileAsync(ct);
            await db.ExecuteAsync($"INSERT INTO {db.Schema}.policy_bindings VALUES('probe','probe','probe','probe','probe','v1','v1')",ct);
            // Remove the candidate if it is already present in a newer production migration.
            await db.ExecuteAsync(provider == "postgresql" ? $"DROP INDEX IF EXISTS {db.Schema}.{index}"
                : $"DROP INDEX IF EXISTS {index} ON {db.Schema}.works",ct);
            foreach (var count in new[] { 2000,10000,100000 })
            foreach (var shape in new[] { "ready","completed-history","leased-history","scheduled-expired","scheduled-empty","leased-empty","recovery-due","recovery-dense","expiry-dense" })
            {
                await db.ExecuteAsync($"DELETE FROM {db.Schema}.works",ct);
                await SeedAsync(db,provider,count,shape,ct);
                await db.ExecuteAsync(provider == "postgresql" ? $"VACUUM (ANALYZE) {db.Schema}.works"
                    : $"ALTER INDEX ALL ON {db.Schema}.works REBUILD; UPDATE STATISTICS {db.Schema}.works WITH FULLSCAN",ct);
                foreach (var candidate in new[] { false,true })
                {
                    if (candidate) await db.ExecuteAsync($"CREATE INDEX {index} ON {db.Schema}.works{indexDefinition}",ct);
                    for (var repeat = 1; repeat <= 3; repeat++)
                    {
                        var filename = $"{count}-{shape}-{(candidate ? "candidate" : "baseline")}-{repeat}." + (provider == "postgresql" ? "json" : "xml");
                        var plan = await PlanAsync(db,provider,candidate ? candidateQuery : query,ct);
                        await File.WriteAllTextAsync(Path.Combine(output,filename),plan,ct);
                        observations.Add(new { Count=count,Shape=shape,CandidateIndex=candidate,Repetition=repeat,Plan=filename,Sha256=Hash(plan) });
                    }
                    if (candidate) await db.ExecuteAsync(provider == "postgresql" ? $"DROP INDEX {db.Schema}.{index}"
                        : $"DROP INDEX {index} ON {db.Schema}.works",ct);
                }
                Console.WriteLine($"{provider}: {count} {shape} plans captured.");
            }
            await db.CleanupAsync(ct); removed=true;
        }
        catch (Exception error) { errorType=error.GetType().Name; }
        await File.WriteAllTextAsync(Path.Combine(output,"manifest.json"),JsonSerializer.Serialize(new
        {
            Provider=provider,At=DateTimeOffset.UtcNow,db.Schema,SchemaRemoved=removed,ErrorType=errorType,DatabaseProfile=profile,Comparison="dev.17 versus current source",CandidateIndex=index+indexDefinition,
            Method="Synthetic plan-only fixture: one-byte payload, analyzed statistics (SQL indexes rebuilt per shape), three rollback-only locked SELECTs per shape/index. Not WorkPump throughput or a correctness test. SQL Server plan includes per-query waits when supplied by engine. PostgreSQL EXPLAIN does not measure waits.",
            Shapes="ready: all due; completed-history: only last row ready; leased-history: first N-1 have future lease; scheduled-expired: first N-1 scheduled, last expired despite future due time; scheduled-empty/leased-empty: none eligible; recovery-due: one expired lease; recovery-dense/expiry-dense: all eligible",
            SourceQuery=sourceQuery,Query=query,QuerySha256=Hash(query),CandidateQuery=candidateQuery,CandidateQuerySha256=Hash(candidateQuery),Observations=observations
        },new JsonSerializerOptions {WriteIndented=true}),CancellationToken.None);
        return removed && errorType is null ? 0 : 1;
    }

    private static Task SeedAsync<T>(LoadDatabase<T> db,string provider,int count,string shape,CancellationToken ct)
    {
        // Shape names and counts are internal constants. All data is synthetic; no application scope is read.
        var history = shape == "completed-history";
        var leased = shape.StartsWith("leased",StringComparison.Ordinal) || shape.StartsWith("recovery",StringComparison.Ordinal);
        var allLeased = shape != "leased-history";
        var scheduled = shape.StartsWith("scheduled",StringComparison.Ordinal) || shape == "expiry-dense";
        var state = history ? $"CASE WHEN n<{count} THEN 3 ELSE 0 END" : leased ? $"CASE WHEN n<={ (allLeased ? count : count-1) } THEN 1 ELSE 0 END" : "0";
        var now = provider == "postgresql" ? "statement_timestamp()" : "TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00')";
        var future = provider == "postgresql" ? $"{now}+interval '1 day'" : $"DATEADD(day,1,{now})";
        var past = provider == "postgresql" ? $"{now}-interval '1 day'" : $"DATEADD(day,-1,{now})";
        var token = leased ? $"CASE WHEN n<={ (allLeased ? count : count-1) } THEN {(provider == "postgresql" ? "gen_random_uuid()" : "NEWID()")} ELSE NULL END" : "NULL";
        var lease = leased ? $"CASE WHEN n<={ (allLeased ? count : count-1) } THEN {future} ELSE NULL END" : "NULL";
        if (shape == "recovery-due") lease=$"CASE WHEN n={count} THEN {past} ELSE {future} END";
        if (shape == "recovery-dense") lease=past;
        var expires = shape == "scheduled-expired" ? $"CASE WHEN n={count} THEN {past} ELSE NULL END" : shape == "expiry-dense" ? past : "NULL";
        var columns = "id,identity_id,scope_id,business_key,message_id,fingerprint,payload,state,next_attempt_at,lease_token,lease_until,expires_at";
        return db.ExecuteAsync(provider == "postgresql" ? $"""
            INSERT INTO {db.Schema}.works({columns})
            SELECT gen_random_uuid(),md5(n::text),'probe',n::text,gen_random_uuid(),repeat('0',64),decode('00','hex'),{state},
                {(scheduled ? future : past)}+n*interval '1 microsecond',{token},{lease},{expires}
            FROM generate_series(1,{count}) n
            """ : $"""
            WITH numbers AS (SELECT TOP({count}) ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b)
            INSERT INTO {db.Schema}.works({columns})
            SELECT NEWID(),CONVERT(nvarchar(64),n),'probe',CONVERT(nvarchar(20),n),NEWID(),REPLICATE(N'0',64),0x00,{state},
                DATEADD(microsecond,CONVERT(int,n),{(scheduled ? future : past)}),{token},{lease},{expires} FROM numbers
            """,ct);
    }

    private static string LegacySqlServerQuery(string source)
    {
        // Freeze the dev.17 predicate while reusing the unchanged metadata projection.
        var columns=Regex.Match(source,@"SELECT TOP\(1\) ([^\r\n]+)").Groups[1].Value;
        if (columns.Length == 0) throw new InvalidOperationException("Unexpected SQL Server projection.");
        var table="{_schema}.works";
        return $"""
            SELECT TOP(1) {columns} FROM {table} WITH(UPDLOCK,READPAST,ROWLOCK)
            WHERE state IN (0,1,2) AND scope_id=@scope AND (
                (state IN (0,2) AND (next_attempt_at<=TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00') OR expires_at<=TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00')))
                OR (state=1 AND lease_until<=TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00')))
            ORDER BY next_attempt_at,id
            """;
    }

    internal static async Task<string> PlanAsync<T>(LoadDatabase<T> db,string provider,string query,CancellationToken ct)
    {
        await using var connection = await db.OpenAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(provider == "postgresql" ? IsolationLevel.ReadCommitted : IsolationLevel.RepeatableRead,ct);
        await using var command = connection.CreateCommand(); command.Transaction=transaction; command.CommandTimeout=30;
        var sql = query.Replace("{_schema}",db.Schema,StringComparison.Ordinal);
        command.CommandText = provider == "postgresql" ? "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + sql
            : "SET STATISTICS XML ON; " + sql + "; SET STATISTICS XML OFF;";
        var scope = command.CreateParameter(); scope.ParameterName="scope"; scope.Value="probe"; scope.DbType=DbType.String; scope.Size=64; command.Parameters.Add(scope);
        foreach (var name in new[] { "fresh", "retry" })
        {
            if (!query.Contains("@" + name, StringComparison.Ordinal)) continue;
            var parameter = command.CreateParameter(); parameter.ParameterName=name;
            parameter.DbType=DbType.Int32; parameter.Value=1; command.Parameters.Add(parameter);
        }
        var plans = new List<string>();
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            do
            {
                while (await reader.ReadAsync(ct))
                    if (reader.FieldCount == 1 && reader.GetValue(0) is string value) plans.Add(value);
            } while (await reader.NextResultAsync(ct));
        }
        await transaction.RollbackAsync(ct);
        if (plans.Count == 0) throw new InvalidOperationException("Database did not return an actual plan.");
        // A conditional SQL Server batch can emit one plan for each executed SELECT.
        return provider == "postgresql" ? plans.Single() : new System.Xml.Linq.XElement("ClaimPlans",
            plans.Select(System.Xml.Linq.XElement.Parse)).ToString();
    }

    private static string Hash(string text) => Convert.ToHexString(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(text)));
}
