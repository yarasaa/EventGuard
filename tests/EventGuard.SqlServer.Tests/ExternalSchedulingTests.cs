using Microsoft.Data.SqlClient;
using Xunit.Abstractions;

namespace EventGuard.SqlServer.Tests;

public sealed class ExternalSchedulingTests(DatabaseFixture db, ITestOutputHelper output) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Completed_history_does_not_increase_native_claim_reads()
    {
        await db.Store.InitializeExternalActionsAsync();
        var scope = new ProducerScope("history", Guid.NewGuid().ToString("N"));
        var binding = "sağlayıcı-" + new string('x', 900);
        var actions = Enumerable.Range(0, 100).Select(_ => new ExternalAction(scope, Guid.NewGuid(), binding, "v1", [1])).ToArray();
        await db.Outbox.ProduceAsync(new(scope, Guid.NewGuid(), ContentFingerprint.FromBytes([1])), async (session, ct) =>
        { foreach (var a in actions) await session.ExternalActions.EnqueueAsync(a, ct); });
        await using var c = await db.DataSource.OpenConnectionAsync();
        async Task<long> Reads()
        {
            await using var command = new SqlCommand("""
                SELECT COALESCE(SUM(q.total_logical_reads),0) FROM sys.dm_exec_query_stats q
                CROSS APPLY sys.dm_exec_sql_text(q.sql_handle) t
                WHERE t.text LIKE @schema AND t.text LIKE '%DECLARE @candidates TABLE%'
                """, c);
            command.Parameters.AddWithValue("schema", "%" + db.Schema + "%");
            return Convert.ToInt64(await command.ExecuteScalarAsync());
        }
        async Task<long> Claim()
        {
            var before = await Reads();
            var claims = await db.Store.ClaimActionsAsync(new(scope, binding, "v1", ExternalPhase.Submit,
                Guid.NewGuid(), TimeSpan.FromMinutes(2), 100, 1024 * 1024));
            Assert.Equal(100, claims.Count);
            Assert.All(claims, claim => Assert.Contains(claim.Record.Action.Id, actions.Select(a => a.Id)));
            var reads = await Reads() - before;
            await using var reset = new SqlCommand($"UPDATE {db.Schema}.external_actions SET state=0,lease_token=NULL,lease_until=NULL,lease_phase=NULL WHERE scope_id=@scope AND state=1", c);
            reset.Parameters.AddWithValue("scope", scope.Id); await reset.ExecuteNonQueryAsync();
            return reads;
        }
        await Claim(); // compile and warm the same SQL before reading its counters
        var emptyHistory = await Claim();
        await using (var seed = new SqlCommand($"""
            INSERT INTO {db.Schema}.external_actions(scope_id,id,tenant,producer,binding_name,binding_version,payload,fingerprint,state,observation,next_at)
            SELECT TOP(50000) @scope,NEWID(),'history',@producer,@binding,'v1',0x01,REPLICATE('0',64),2,0,DATEADD(day,-1,SYSDATETIMEOFFSET())
            FROM sys.all_objects a CROSS JOIN sys.all_objects b;
            """, c))
        {
            seed.Parameters.AddWithValue("scope", scope.Id); seed.Parameters.AddWithValue("producer", scope.Producer);
            seed.Parameters.AddWithValue("binding", binding); await seed.ExecuteNonQueryAsync();
        }
        var largeHistory = await Claim();
        output.WriteLine(System.Text.Json.JsonSerializer.Serialize(new { ready = 100, history = 50000, emptyHistory, largeHistory }));
        Assert.True(emptyHistory > 0);
        Assert.InRange(largeHistory, 1, emptyHistory * 2 + 200);
    }
}
