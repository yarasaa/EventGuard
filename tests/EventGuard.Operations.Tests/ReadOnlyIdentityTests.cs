using EventGuard.Operations.PostgreSql;
using EventGuard.Operations.SqlServer;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace EventGuard.Operations.Tests;

public sealed class PostgreSqlReadOnlyIdentity(EventGuard.PostgreSql.Tests.DatabaseFixture db) : IClassFixture<EventGuard.PostgreSql.Tests.DatabaseFixture>
{
    [Fact]
    public async Task Select_only_identity_can_validate_and_read_but_cannot_update()
    {
        var role = "eg_ops_" + Guid.NewGuid().ToString("N");
        var scope = EventGuard.PostgreSql.Tests.DatabaseFixture.Scope();
        var work = (await db.Store.AcceptAsync(EventGuard.PostgreSql.Tests.DatabaseFixture.Submission(scope))).Work;
        var producer = new ProducerScope(Guid.NewGuid().ToString("N"), "publisher");
        var outgoing = new OutgoingMessage(producer, Guid.NewGuid(), Guid.NewGuid(), new("kafka", "events", "v1", "private-address"), "private-payload"u8);
        await db.EnqueueAsync(outgoing);
        await using var setup = db.DataSource.CreateCommand($"""
            {EventGuard.Operations.PostgreSql.OperationsIndexes.CreateScript(db.Schema)}
            {EventGuard.Operations.PostgreSql.OutboxReadIndexes.CreateScript(db.Schema)}
            {EventGuard.Operations.PostgreSql.AuditIndexes.CreateScript(db.Schema)}
            CREATE ROLE {role} LOGIN PASSWORD 'local-test-only';
            GRANT USAGE ON SCHEMA {db.Schema} TO {role};
            GRANT SELECT ON {db.Schema}.schema_version,{db.Schema}.works,{db.Schema}.attempts,{db.Schema}.replay_requests TO {role};
            GRANT SELECT (scope_id,id,message_id,state,version,attempt_count,replay_generation,delivery_uncertain,created_at,next_attempt_at,published_at,last_reason,transport,connection_name,configuration_version,quarantine_id) ON {db.Schema}.outbox_messages TO {role};
            GRANT SELECT (scope_id,id,version,key_version,fingerprint_version,reason_code,released_work_id,data_purged_at,created_at) ON {db.Schema}.ingress_quarantine TO {role};
            GRANT SELECT (scope_id,id,intent_id,observed_version,reason,detected_at) ON {db.Schema}.outbox_quarantine TO {role};
            GRANT SELECT (scope_id,intent_id,number,generation,status,started_at,finished_at,reason) ON {db.Schema}.outbox_attempts TO {role};
            GRANT SELECT (scope_id,intent_id,id,result_status,result_generation,result_version,acknowledge_possible_duplicate,created_at) ON {db.Schema}.outbox_replay_requests TO {role};
            """);
        await setup.ExecuteNonQueryAsync();
        try
        {
            var options = new NpgsqlConnectionStringBuilder(db.ConnectionString) { Username = role, Password = "local-test-only", Pooling = false };
            await using var source = new PostgreSqlWorkReader(options.ConnectionString, db.Schema);
            await source.ValidateAsync(); await source.ValidateOutboxAsync(); await source.ValidateRecoveryAsync();
            await using var workerSource = NpgsqlDataSource.Create(options.ConnectionString);
            await Assert.ThrowsAsync<InvalidOperationException>(() => new EventGuard.PostgreSql.PostgreSqlWorkStore(workerSource, db.Schema).ValidateStartupAsync(new(scope.Tenant, StoreRole.Consumer)));
            Assert.Empty((await source.ListIngressAsync(new(scope, DateTimeOffset.UtcNow))).Items);
            Assert.Equal(1, Assert.Single((await source.ReadBacklogAsync(BacklogQuery.Consumer(scope))).Buckets, b => b.State == "Pending").Count);
            Assert.Equal(1, Assert.Single((await source.ReadBacklogAsync(BacklogQuery.Outbox(producer))).Buckets, b => b.State == "Pending").Count);
            Assert.True((await new OperationsReader(source).CheckHealthAsync(true, true)).Value);
            Assert.Single((await source.ListOutboxAsync(new(producer, DateTimeOffset.UtcNow.AddDays(1)))).Items);
            Assert.NotNull(await source.FindOutboxAsync(producer, outgoing.IntentId));
            Assert.Single((await source.ListAsync(new(scope, DateTimeOffset.UtcNow.AddDays(1)))).Items);
            Assert.NotNull(await source.FindAsync(scope, work.Id));
            Assert.NotNull(await source.ReadDataAsync(scope, work.Id, new([new("Value", "/value")])));
            await using var connection = new NpgsqlConnection(options.ConnectionString);
            await connection.OpenAsync();
            await using var update = new NpgsqlCommand($"UPDATE {db.Schema}.works SET state=3", connection);
            Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, (await Assert.ThrowsAsync<PostgresException>(() => update.ExecuteNonQueryAsync())).SqlState);
            foreach (var target in new[] { "payload FROM {db.Schema}.outbox_messages", "headers FROM {db.Schema}.outbox_messages", "original_record FROM {db.Schema}.outbox_quarantine", "actor FROM {db.Schema}.outbox_replay_requests", "body FROM {db.Schema}.ingress_quarantine", "metadata FROM {db.Schema}.ingress_quarantine" })
            {
                await using var hidden = new NpgsqlCommand("SELECT " + target.Replace("{db.Schema}", db.Schema), connection);
                Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, (await Assert.ThrowsAsync<PostgresException>(() => hidden.ExecuteNonQueryAsync())).SqlState);
            }
            await Assert.ThrowsAnyAsync<Exception>(() => source.ValidateAuditAsync(true, true));
            await using var grantAudit = db.DataSource.CreateCommand($"""
                GRANT SELECT (scope_id,id,work_id,result_status,actor,reason,created_at) ON {db.Schema}.replay_requests TO {role};
                GRANT SELECT (scope_id,id,quarantine_id,result_status,actor,reason,created_at) ON {db.Schema}.ingress_release_requests TO {role};
                GRANT SELECT (scope_id,id,intent_id,result_status,actor,reason,acknowledge_possible_duplicate,created_at) ON {db.Schema}.outbox_replay_requests TO {role};
                GRANT SELECT (scope_id,id,intent_id,result_status,actor,reason,acknowledge_possible_duplicate,created_at) ON {db.Schema}.outbox_repair_requests TO {role};
                GRANT SELECT (scope_id,id,target,actor,reason,created_at) ON {db.Schema}.retention_requests TO {role};
                GRANT SELECT (scope_id,id,kind,actor,reason,acknowledge_possible_duplicate,created_at) ON {db.Schema}.bulk_replay_requests TO {role};
                """);
            await grantAudit.ExecuteNonQueryAsync();
            await source.ValidateAuditAsync(true, true);
            Assert.Empty((await source.ListAuditAsync(new(scope, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddMinutes(1)))).Items);
            Assert.Empty((await source.ListAuditAsync(new(producer, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddMinutes(1)))).Items);
            foreach (var target in new[] { "fingerprint FROM {db.Schema}.bulk_replay_requests", "items FROM {db.Schema}.bulk_replay_requests", "fingerprint FROM {db.Schema}.outbox_repair_requests" })
            {
                await using var hiddenAudit = new NpgsqlCommand("SELECT " + target.Replace("{db.Schema}", db.Schema), connection);
                await Assert.ThrowsAnyAsync<Exception>(() => hiddenAudit.ExecuteNonQueryAsync());
            }
        }
        finally
        {
            await using var remove = db.DataSource.CreateCommand($"DROP OWNED BY {role}; DROP ROLE {role}");
            await remove.ExecuteNonQueryAsync();
        }
    }
}

public sealed class SqlServerReadOnlyIdentity(EventGuard.SqlServer.Tests.DatabaseFixture db) : IClassFixture<EventGuard.SqlServer.Tests.DatabaseFixture>
{
    [Fact]
    public async Task Select_only_identity_can_validate_and_read_but_cannot_update()
    {
        var role = "eg_ops_" + Guid.NewGuid().ToString("N");
        var scope = EventGuard.SqlServer.Tests.DatabaseFixture.Scope();
        var work = (await db.Store.AcceptAsync(EventGuard.SqlServer.Tests.DatabaseFixture.Submission(scope))).Work;
        var producer = new ProducerScope(Guid.NewGuid().ToString("N"), "publisher");
        var outgoing = new OutgoingMessage(producer, Guid.NewGuid(), Guid.NewGuid(), new("kafka", "events", "v1", "private-address"), "private-payload"u8);
        await db.EnqueueAsync(outgoing);
        await using var admin = await db.DataSource.OpenConnectionAsync();
        await using var setup = new SqlCommand($"""
            {EventGuard.Operations.SqlServer.OperationsIndexes.CreateScript(db.Schema)}
            {EventGuard.Operations.SqlServer.OutboxReadIndexes.CreateScript(db.Schema)}
            {EventGuard.Operations.SqlServer.AuditIndexes.CreateScript(db.Schema)}
            CREATE LOGIN [{role}] WITH PASSWORD='Local-Test-Only!2026',CHECK_POLICY=OFF;
            CREATE USER [{role}] FOR LOGIN [{role}];
            GRANT SELECT ON {db.Schema}.schema_version TO [{role}];
            GRANT SELECT ON {db.Schema}.works TO [{role}];
            GRANT SELECT ON {db.Schema}.attempts TO [{role}];
            GRANT SELECT ON {db.Schema}.replay_requests TO [{role}];
            GRANT SELECT ON {db.Schema}.outbox_messages (scope_id,id,message_id,state,version,attempt_count,replay_generation,delivery_uncertain,created_at,next_attempt_at,published_at,last_reason,transport,connection_name,configuration_version,quarantine_id) TO [{role}];
            GRANT SELECT ON {db.Schema}.ingress_quarantine (scope_id,id,version,key_version,fingerprint_version,reason_code,released_work_id,data_purged_at,created_at) TO [{role}];
            GRANT SELECT ON {db.Schema}.outbox_quarantine (scope_id,id,intent_id,observed_version,reason,detected_at) TO [{role}];
            GRANT SELECT ON {db.Schema}.outbox_attempts (scope_id,intent_id,number,generation,status,started_at,finished_at,reason) TO [{role}];
            GRANT SELECT ON {db.Schema}.outbox_replay_requests (scope_id,intent_id,id,result_status,result_generation,result_version,acknowledge_possible_duplicate,created_at) TO [{role}];
            """, admin);
        await setup.ExecuteNonQueryAsync();
        try
        {
            var options = new SqlConnectionStringBuilder(db.ConnectionString) { UserID = role, Password = "Local-Test-Only!2026", Pooling = false };
            await using var source = new SqlServerWorkReader(options.ConnectionString, db.Schema);
            await source.ValidateAsync(); await source.ValidateOutboxAsync(); await source.ValidateRecoveryAsync();
            var workerSource = new EventGuard.SqlServer.SqlServerDataSource(options.ConnectionString);
            await Assert.ThrowsAsync<InvalidOperationException>(() => new EventGuard.SqlServer.SqlServerWorkStore(workerSource, db.Schema).ValidateStartupAsync(new(scope.Tenant, StoreRole.Consumer)));
            Assert.Empty((await source.ListIngressAsync(new(scope, DateTimeOffset.UtcNow))).Items);
            Assert.Equal(1, Assert.Single((await source.ReadBacklogAsync(BacklogQuery.Consumer(scope))).Buckets, b => b.State == "Pending").Count);
            Assert.Equal(1, Assert.Single((await source.ReadBacklogAsync(BacklogQuery.Outbox(producer))).Buckets, b => b.State == "Pending").Count);
            Assert.True((await new OperationsReader(source).CheckHealthAsync(true, true)).Value);
            Assert.Single((await source.ListOutboxAsync(new(producer, DateTimeOffset.UtcNow.AddDays(1)))).Items);
            Assert.NotNull(await source.FindOutboxAsync(producer, outgoing.IntentId));
            Assert.Single((await source.ListAsync(new(scope, DateTimeOffset.UtcNow.AddDays(1)))).Items);
            Assert.NotNull(await source.FindAsync(scope, work.Id));
            Assert.NotNull(await source.ReadDataAsync(scope, work.Id, new([new("Value", "/value")])));
            await using var connection = new SqlConnection(options.ConnectionString);
            await connection.OpenAsync();
            await using var update = new SqlCommand($"UPDATE {db.Schema}.works SET state=3", connection);
            Assert.Equal(229, (await Assert.ThrowsAsync<SqlException>(() => update.ExecuteNonQueryAsync())).Number);
            foreach (var target in new[] { "payload FROM {db.Schema}.outbox_messages", "headers FROM {db.Schema}.outbox_messages", "original_record FROM {db.Schema}.outbox_quarantine", "actor FROM {db.Schema}.outbox_replay_requests", "body FROM {db.Schema}.ingress_quarantine", "metadata FROM {db.Schema}.ingress_quarantine" })
            {
                await using var hidden = new SqlCommand("SELECT " + target.Replace("{db.Schema}", db.Schema), connection);
                var error = await Assert.ThrowsAsync<SqlException>(() => hidden.ExecuteNonQueryAsync());
                Assert.Contains(error.Number, new[] { 229, 230 });
            }
            await Assert.ThrowsAnyAsync<Exception>(() => source.ValidateAuditAsync(true, true));
            await using var grantAudit = new SqlCommand($"""
                GRANT SELECT ON {db.Schema}.replay_requests (scope_id,id,work_id,result_status,actor,reason,created_at) TO [{role}];
                GRANT SELECT ON {db.Schema}.ingress_release_requests (scope_id,id,quarantine_id,result_status,actor,reason,created_at) TO [{role}];
                GRANT SELECT ON {db.Schema}.outbox_replay_requests (scope_id,id,intent_id,result_status,actor,reason,acknowledge_possible_duplicate,created_at) TO [{role}];
                GRANT SELECT ON {db.Schema}.outbox_repair_requests (scope_id,id,intent_id,result_status,actor,reason,acknowledge_possible_duplicate,created_at) TO [{role}];
                GRANT SELECT ON {db.Schema}.retention_requests (scope_id,id,target,actor,reason,created_at) TO [{role}];
                GRANT SELECT ON {db.Schema}.bulk_replay_requests (scope_id,id,kind,actor,reason,acknowledge_possible_duplicate,created_at) TO [{role}];
                """, admin);
            await grantAudit.ExecuteNonQueryAsync();
            await source.ValidateAuditAsync(true, true);
            Assert.Empty((await source.ListAuditAsync(new(scope, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddMinutes(1)))).Items);
            Assert.Empty((await source.ListAuditAsync(new(producer, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddMinutes(1)))).Items);
            foreach (var target in new[] { "fingerprint FROM {db.Schema}.bulk_replay_requests", "items FROM {db.Schema}.bulk_replay_requests", "fingerprint FROM {db.Schema}.outbox_repair_requests" })
            {
                await using var hiddenAudit = new SqlCommand("SELECT " + target.Replace("{db.Schema}", db.Schema), connection);
                await Assert.ThrowsAnyAsync<Exception>(() => hiddenAudit.ExecuteNonQueryAsync());
            }
        }
        finally
        {
            await using var remove = new SqlCommand($"DROP USER [{role}]; DROP LOGIN [{role}]", admin);
            await remove.ExecuteNonQueryAsync();
        }
    }
}
