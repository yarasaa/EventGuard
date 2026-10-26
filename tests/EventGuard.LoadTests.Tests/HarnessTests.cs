using EventGuard.LoadTests;
namespace EventGuard.LoadTests.Tests;
public sealed class HarnessTests
{
    [Fact]
    public async Task PostgreSql_claim_does_not_sort_a_large_queue_when_schedule_statistics_are_stale()
    {
        await using var db = LocalDatabases.PostgreSql(2);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var ct = deadline.Token;
        await db.InitializeAsync(ct);
        try
        {
            await db.ExecuteAsync($"INSERT INTO {db.Schema}.policy_bindings VALUES('probe','probe','probe','probe','probe','v1','v1'); ALTER TABLE {db.Schema}.works SET (autovacuum_enabled=false)", ct);
            await ClaimDuePlanProbe.SeedAsync(db, "ready", ct);
            var root = new DirectoryInfo(AppContext.BaseDirectory);
            while (root is not null && !File.Exists(Path.Combine(root.FullName, "EventGuard.slnx"))) root = root.Parent;
            Assert.NotNull(root);
            using var plan = System.Text.Json.JsonDocument.Parse(await ClaimPlanProbe.PlanAsync(db, "postgresql", ClaimPlanProbe.ReadQuery("postgresql", root.FullName), ct));
            var node = plan.RootElement[0].GetProperty("Plan");
            Assert.Equal(1, node.GetProperty("Actual Rows").GetDouble());
            Assert.Equal(0, node.GetProperty("Temp Written Blocks").GetInt64());
            Check(node);
            static void Check(System.Text.Json.JsonElement node)
            {
                Assert.DoesNotContain("Sort", node.GetProperty("Node Type").GetString()!);
                if (node.TryGetProperty("Plans", out var children)) foreach (var child in children.EnumerateArray()) Check(child);
            }
        }
        finally { await db.CleanupAsync(CancellationToken.None); }
    }

    [Theory]
    [InlineData("postgresql")][InlineData("sqlserver")]
    public async Task Native_current_claim_plan_binds_budget_filters(string provider)
    {
        if (provider == "postgresql") await Probe(LocalDatabases.PostgreSql(2));
        else await Probe(LocalDatabases.SqlServer(2));
        async Task Probe<T>(LoadDatabase<T> database)
        {
            await using var owned = database;
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await database.InitializeAsync(deadline.Token);
            try
            {
                var root = new DirectoryInfo(AppContext.BaseDirectory);
                while (root is not null && !File.Exists(Path.Combine(root.FullName, "EventGuard.slnx"))) root = root.Parent;
                Assert.NotNull(root);
                var plan = await ClaimPlanProbe.PlanAsync(database, provider, ClaimPlanProbe.ReadQuery(provider, root.FullName, budgetFilter: true), deadline.Token);
                Assert.Contains(provider == "postgresql" ? "Plan" : "ShowPlanXML", plan);
            }
            finally { await database.CleanupAsync(CancellationToken.None); }
        }
    }
    [Theory]
    [InlineData("postgresql")][InlineData("sqlserver")]
    public void Diagnostic_claim_extraction_fails_closed_and_keeps_native_locking(string provider)
    {
        // Test host cwd is not the repository root; do not mutate the process-wide cwd.
        var current = new DirectoryInfo(AppContext.BaseDirectory);
        while (current is not null && !File.Exists(Path.Combine(current.FullName,"EventGuard.slnx"))) current=current.Parent;
        Assert.NotNull(current);
        var query = ClaimPlanProbe.ReadQuery(provider,current.FullName);
        Assert.Contains("{_schema}.works",query);
        Assert.Contains(provider == "postgresql" ? "ORDER BY (CASE WHEN state=1 THEN lease_until ELSE LEAST(next_attempt_at,expires_at) END),id" : "ORDER BY next_attempt_at,id",query);
        Assert.Contains(provider == "postgresql" ? "FOR UPDATE SKIP LOCKED" : "WITH(UPDLOCK,READPAST,ROWLOCK",query);
    }

    [Fact]
    public void Histogram_reports_known_quantiles_with_bounded_rounding_and_no_empty_zero()
    {
        var h = new LatencyHistogram(); Assert.Null(h.Snapshot().P99UpperMs);
        for (var i = 1; i <= 1000; i++) h.AddMilliseconds(i);
        var result = h.Snapshot(); Assert.Equal(1000,result.Samples); Assert.Equal(500.5,result.MeanMs);
        Assert.InRange(result.P50UpperMs!.Value,500,511); Assert.InRange(result.P95UpperMs!.Value,950,971); Assert.InRange(result.P99UpperMs!.Value,990,1012);
        Assert.Throws<ArgumentOutOfRangeException>(()=>h.AddMilliseconds(double.NaN));
    }
    [Fact]
    public void Options_bound_task_pool_memory_and_fixture_identity_is_repeatable()
    {
        foreach (var args in new[] { new[]{"--concurrency","1,1"},new[]{"--count","1000001"},new[]{"--concurrency","32","--pool-size","4"},new[]{"--unknown","x"},
            new[]{"--background-count","100001"},new[]{"--background-kind","ready"},new[]{"--idle-polls","1001"} })
            Assert.ThrowsAny<ArgumentException>(()=>LoadOptions.Parse(args));
        var scope = new WorkScope("test","load","fixture","test"); var baseline = new byte[64]; new Random(1729).NextBytes(baseline);
        var a = LoadScenario.Submission(scope,10,baseline); var b = LoadScenario.Submission(scope,10,baseline);
        Assert.Equal(a.MessageId,b.MessageId); Assert.Equal(a.Fingerprint,b.Fingerprint); Assert.Equal(a.CopyPayload(),b.CopyPayload());
        Assert.NotEqual(a.Fingerprint,LoadScenario.Submission(scope,11,baseline).Fingerprint);
    }
    [Theory]
    [InlineData("postgresql")][InlineData("sqlserver")]
    public async Task Native_smoke_runs_the_real_pump_and_preserves_one_effect_per_completed_work(string provider)
    {
        var options = LoadOptions.Parse(["--provider",provider,"--count","24","--concurrency","2","--warmup","8","--repetitions","1"]);
        var result = await LoadScenario.RunAsync(options,2,1,CancellationToken.None);
        Assert.True(result.Valid, System.Text.Json.JsonSerializer.Serialize(result));
        Assert.Equal(24,result.Measured!.Oracle!.Effects); Assert.Equal(24,result.Measured.ExecuteCalls.Samples);
        Assert.Equal(24,result.Measured.SubmissionToConfirmedCompletion.Samples); Assert.True(result.Measured.VerifiedCompletionsPerSecond > 0);
        Assert.True(result.SchemaRemoved);
    }
    [Theory]
    [InlineData("postgresql","scheduled")][InlineData("postgresql","leased")]
    [InlineData("sqlserver","scheduled")][InlineData("sqlserver","leased")]
    public async Task Native_background_fixture_keeps_unavailable_work_untouched(string provider,string kind)
    {
        var options=LoadOptions.Parse(["--provider",provider,"--count","12","--concurrency","2","--warmup","0","--repetitions","1","--background-count","24","--background-kind",kind,"--idle-polls","3"]);
        var result=await LoadScenario.RunAsync(options,2,1,CancellationToken.None);
        Assert.True(result.Valid,System.Text.Json.JsonSerializer.Serialize(result));
        Assert.Equal(24,result.Measured!.Oracle!.Background); Assert.Equal(0,result.Measured.Oracle.InvalidBackground);
        Assert.Equal(12,result.Measured.Oracle.Effects);
        Assert.Equal(3,result.Measured.IdleProbe.Samples);
    }

    [Theory]
    [InlineData("postgresql")][InlineData("sqlserver")]
    public async Task Native_oracle_detects_duplicate_business_effect_without_a_unique_constraint_masking_it(string provider)
    {
        if (provider=="postgresql") await CheckAsync(LocalDatabases.PostgreSql(4)); else await CheckAsync(LocalDatabases.SqlServer(4));
    }
    private static async Task CheckAsync<T>(LoadDatabase<T> db)
    {
        await using var owned = db;
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await db.InitializeAsync(deadline.Token);
        try
        {
            var scope = new WorkScope("test","oracle","native","test");
            await db.Store.AcceptAsync(LoadScenario.Submission(scope,0,new byte[64]),deadline.Token);
            var pending = await db.VerifyAsync(scope,1,deadline.Token);
            Assert.False(pending.Valid); Assert.Equal(0,pending.Completed); Assert.Equal(1,pending.WorksWithoutExactlyOneEffect);
            var claim = await db.Store.ClaimNextAsync(scope,Guid.NewGuid(),TimeSpan.FromSeconds(30),deadline.Token);
            await db.Store.ExecuteAsync(claim!, async (work,bytes,session,ct)=> {await db.Effect(work,bytes,session,ct);await db.Effect(work,bytes,session,ct);},new(),deadline.Token);
            var oracle = await db.VerifyAsync(scope,1,deadline.Token);
            Assert.False(oracle.Valid); Assert.Equal(1,oracle.Completed); Assert.Equal(2,oracle.Effects); Assert.Equal(1,oracle.WorksWithoutExactlyOneEffect);
        }
        finally { await db.CleanupAsync(CancellationToken.None); }
    }
}
