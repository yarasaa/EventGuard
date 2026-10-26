using System.Text.Json;
using EventGuard.LoadTests;

namespace EventGuard.LoadTests.Tests;

public sealed class PointLookupTests
{
    [Fact]
    public async Task PostgreSql_point_access_stays_bounded_after_churn_and_rejects_wrong_ownership()
    {
        await using var db = LocalDatabases.PostgreSql(2);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2)); var ct = deadline.Token;
        await db.InitializeAsync(ct);
        try
        {
            var fixture = await PointLookupPlanProbe.SeedAsync(db, 100000, ct);
            var root = new DirectoryInfo(AppContext.BaseDirectory);
            while (root is not null && !File.Exists(Path.Combine(root.FullName, "EventGuard.slnx"))) root = root.Parent;
            Assert.NotNull(root);
            foreach (var operation in new[] { "read", "update" })
            {
                var query = PointLookupPlanProbe.ReadQuery(operation, root.FullName);
                using var result = JsonDocument.Parse(await PointLookupPlanProbe.PlanAsync(db, fixture, operation, query, ct));
                var plan = result.RootElement[0].GetProperty("Plan");
                Assert.Equal(1, plan.GetProperty("Actual Rows").GetDouble());
                Assert.InRange(plan.GetProperty("Shared Hit Blocks").GetInt64() + plan.GetProperty("Shared Read Blocks").GetInt64(), 1, 128);
                AssertPrimaryKey(plan);
                foreach (var wrong in operation == "read" ? new[] { "scope", "token" } : new[] { "scope", "token", "state", "version" })
                {
                    using var rejected = JsonDocument.Parse(await PointLookupPlanProbe.PlanAsync(db, fixture, operation, query, ct, wrong));
                    Assert.Equal(0, rejected.RootElement[0].GetProperty("Plan").GetProperty("Actual Rows").GetDouble());
                }
            }
        }
        finally { await db.CleanupAsync(CancellationToken.None); }
    }

    private static void AssertPrimaryKey(JsonElement node)
    {
        if (node.GetProperty("Node Type").GetString() == "ModifyTable")
        {
            var children = node.GetProperty("Plans"); Assert.Single(children.EnumerateArray()); AssertPrimaryKey(children[0]);
        }
        else
        {
            Assert.Equal("Index Scan", node.GetProperty("Node Type").GetString());
            Assert.Equal("works_pkey", node.GetProperty("Index Name").GetString());
        }
    }
}
