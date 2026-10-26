using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class SchemaTests
{
    [Theory]
    [InlineData(false,1)]
    [InlineData(true,1)]
    [InlineData(false,2)]
    [InlineData(true,2)]
    public async Task Claim_index_upgrade_is_atomic_and_preserves_a_durable_claim(bool collision,int previousVersion)
    {
        var db = new DatabaseFixture(); await db.InitializeAsync();
        try
        {
            // Index-only migrations: create the durable claim first, then reproduce the old index layout.
            await using var connection = await db.DataSource.OpenConnectionAsync();
            await using var command = new SqlCommand { Connection=connection };
            var scope = DatabaseFixture.Scope(); var submission = DatabaseFixture.Submission(scope);
            var accepted = await db.Store.AcceptAsync(submission);
            var request = Guid.NewGuid();
            var claim = (await db.Store.ClaimNextAsync(scope,request,TimeSpan.FromMinutes(1)))!;
            command.CommandText=$"DROP INDEX ix_claim_expiry ON {db.Schema}.works; " +
                (previousVersion == 1 ? $"DROP INDEX ix_claim_active ON {db.Schema}.works; " : "") +
                $"DROP INDEX ix_retention_work ON {db.Schema}.works; DROP INDEX ix_retention_expired ON {db.Schema}.works; DROP INDEX ix_retention_outbox ON {db.Schema}.outbox_messages; " +
                $"ALTER TABLE {db.Schema}.works DROP CONSTRAINT works_purged_terminal; ALTER TABLE {db.Schema}.works DROP COLUMN payload_purged_at; " +
                $"ALTER TABLE {db.Schema}.outbox_messages DROP CONSTRAINT outbox_purged_terminal; ALTER TABLE {db.Schema}.outbox_messages DROP COLUMN payload_purged_at; " +
                $"ALTER TABLE {db.Schema}.conflicts DROP COLUMN payload_purged_at; ALTER TABLE {db.Schema}.outbox_quarantine DROP COLUMN data_purged_at; " +
                $"DROP TABLE {db.Schema}.bulk_replay_requests; DROP TABLE {db.Schema}.tenant_budget_requests; DROP TABLE {db.Schema}.tenant_budgets; DROP TABLE {db.Schema}.retention_requests; DROP TABLE {db.Schema}.outbox_repair_requests; DROP TABLE {db.Schema}.ingress_release_requests; DROP TABLE {db.Schema}.ingress_quarantine; UPDATE {db.Schema}.schema_version SET version={previousVersion}";
            await command.ExecuteNonQueryAsync();
            if (collision)
            {
                command.CommandText=$"CREATE INDEX {(previousVersion == 1 ? "ix_claim_active" : "ix_claim_expiry")} ON {db.Schema}.works(id)"; await command.ExecuteNonQueryAsync();
                await Assert.ThrowsAsync<SqlException>(()=>db.Store.InitializeAsync());
            }
            else await Task.WhenAll(db.Store.InitializeAsync(),db.Outbox.InitializeAsync(),db.Store.InitializeAsync());
            command.CommandText=$"SELECT version FROM {db.Schema}.schema_version WHERE singleton=1";
            Assert.Equal(collision ? previousVersion : 8,await command.ExecuteScalarAsync());
            var repeated = (await db.Store.ClaimNextAsync(scope,request,TimeSpan.FromMinutes(1)))!;
            Assert.Equal(claim.Work,repeated.Work); Assert.Equal(claim.Token,repeated.Token); Assert.Equal(claim.LeaseUntil,repeated.LeaseUntil);
            Assert.Equal(AcceptanceStatus.Duplicate,(await db.Store.AcceptAsync(submission)).Status);
            Assert.Equal(ExecutionStatus.Completed,(await db.Store.ExecuteAsync(repeated,db.Effect,new())).Status);
            Assert.Null(await db.Store.ClaimNextAsync(scope,request,TimeSpan.FromMinutes(1)));
            Assert.Equal(1,await db.EffectCount(accepted.Work.Id));
        }
        finally { await db.DisposeAsync(); }
    }
}
