using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer.Tests;

public sealed class ExternalBatchPermissionTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task Restricted_worker_checks_type_grants_before_claiming_and_can_use_the_native_path_without_DDL()
    {
        await db.Store.InitializeExternalActionsAsync();
        var user = "eg_batch_role_" + Guid.NewGuid().ToString("N");
        var scope = new ProducerScope("type-grants", Guid.NewGuid().ToString("N"));
        var action = new ExternalAction(scope, Guid.NewGuid(), "service", "v1", [1]);
        var target = new ExternalResultTarget(scope, "application-status", "v1");
        await db.Outbox.ProduceAsync(new(scope, Guid.NewGuid(), ContentFingerprint.FromBytes([1])),
            async (session, ct) => { await session.ExternalActions.EnqueueAsync(action, target, ct); });
        await using var admin = await db.DataSource.OpenConnectionAsync();
        await using var command = new SqlCommand($"""
            CREATE LOGIN [{user}] WITH PASSWORD='EventGuard-Local-Only!2026', CHECK_POLICY=OFF;
            CREATE USER [{user}] FOR LOGIN [{user}];
            GRANT SELECT,INSERT,UPDATE,DELETE ON SCHEMA::{db.Schema} TO [{user}];
            """, admin);
        try
        {
            await command.ExecuteNonQueryAsync();
            var settings = new SqlConnectionStringBuilder(db.ConnectionString)
                { UserID = user, Password = "EventGuard-Local-Only!2026", Pooling = false };
            var worker = new SqlServerWorkStore(new(settings.ConnectionString), db.Schema);
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => worker.ValidateExternalBatchAsync());
            Assert.Contains("EXECUTE and REFERENCES", error.Message);
            Assert.Equal(ExternalActionState.Ready, (await db.Store.FindActionAsync(scope, action.Id))!.State);
            foreach (var type in new[] { "external_batch_ids", "external_batch_changes", "external_batch_projections", "external_batch_bindings" })
            {
                command.CommandText = $"GRANT EXECUTE,REFERENCES ON TYPE::{db.Schema}.{type} TO [{user}]";
                await command.ExecuteNonQueryAsync();
            }
            await worker.ValidateExternalBatchAsync();
            var claims = await worker.ClaimActionsAsync(new(scope, "service", "v1", ExternalPhase.Submit, Guid.NewGuid(), TimeSpan.FromMinutes(1), 10, 1024));
            Assert.Single(claims);
            Assert.True(Assert.Single(await worker.SettleActionsAsync(new(claims.Select(c =>
                new ExternalBatchSettlementItem(c, new(ExternalResultKind.Accepted, "accepted"))), new()))).Recorded);
            Assert.NotNull(await db.Claim(target.WorkScope));
        }
        finally
        {
            command.CommandText = $"IF USER_ID('{user}') IS NOT NULL DROP USER [{user}]; IF SUSER_ID('{user}') IS NOT NULL DROP LOGIN [{user}];";
            await command.ExecuteNonQueryAsync();
        }
    }
}
