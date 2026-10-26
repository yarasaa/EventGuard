using EventGuard.Store.Conformance;
namespace EventGuard.SqlServer.Tests;

public sealed class MultiProcessTests(DatabaseFixture db, Xunit.Abstractions.ITestOutputHelper output) : IClassFixture<DatabaseFixture>
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Separate_processes_preserve_one_effect_and_outbox_intent_after_expired_owner_commit_or_kill(bool killOwner)
    {
        output.WriteLine("Fixture: " + db.Schema);
        var evidence = await MultiProcessScenario.VerifyAsync(db, db.Outbox, Start, ReadLedger, killOwner);
        output.WriteLine("Verified: " + System.Text.Json.JsonSerializer.Serialize(evidence));
    }

    private NativeWorkerProcess Start(WorkScope scope, string mode) => NativeWorkerProcess.Start("EventGuard.SqlServer.CrashWorker", new Dictionary<string,string>
    {
        ["EVENTGUARD_SQLSERVER_TEST_CONNECTION"] = db.ConnectionString, ["EVENTGUARD_TEST_SCHEMA"] = db.Schema,
        ["EVENTGUARD_TEST_CONSUMER"] = scope.Consumer, ["EVENTGUARD_TEST_MODE"] = mode
    });
    private async Task<IReadOnlyList<ProcessLedgerRow>> ReadLedger(WorkScope scope, CancellationToken ct)
    {
        var sql = $"""
            SELECT 'work',w.id,w.id,w.fingerprint COLLATE DATABASE_DEFAULT FROM {db.Schema}.works w WHERE w.scope_id=@scope
            UNION ALL SELECT 'effect',e.work_id,e.work_id,e.note FROM {db.Schema}.business_effects e JOIN {db.Schema}.works w ON w.id=e.work_id WHERE w.scope_id=@scope
            UNION ALL SELECT 'outbox',o.origin_work_id,o.id,'' FROM {db.Schema}.outbox_messages o JOIN {db.Schema}.works w ON w.id=o.origin_work_id WHERE w.scope_id=@scope
            UNION ALL SELECT 'attempt',a.work_id,a.id,a.status FROM {db.Schema}.attempts a JOIN {db.Schema}.works w ON w.id=a.work_id WHERE w.scope_id=@scope
            UNION ALL SELECT 'conflict',c.work_id,c.id,c.fingerprint FROM {db.Schema}.conflicts c JOIN {db.Schema}.works w ON w.id=c.work_id WHERE w.scope_id=@scope
            UNION ALL SELECT 'replay',r.work_id,r.id,CAST(r.result_status AS varchar(32)) FROM {db.Schema}.replay_requests r WHERE r.scope_id=@scope
            """;
        await using var connection = await db.DataSource.OpenConnectionAsync(ct);
        await using var command = new Microsoft.Data.SqlClient.SqlCommand(sql, connection);
        command.Parameters.AddWithValue("scope", scope.Id);
        var rows = new List<ProcessLedgerRow>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) rows.Add(new(reader.GetString(0), reader.GetGuid(1), reader.GetGuid(2), reader.GetString(3)));
        return rows;
    }
}
