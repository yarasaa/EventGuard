namespace EventGuard.PostgreSql.Tests;

public sealed class SchemaTests(DatabaseFixture db) : IClassFixture<DatabaseFixture>
{
    [Theory]
    [InlineData(false,3)]
    [InlineData(true,3)]
    [InlineData(false,4)]
    [InlineData(true,4)]
    [InlineData(false,10)]
    [InlineData(true,10)]
    public async Task Claim_index_upgrade_is_atomic_and_preserves_a_durable_claim(bool collision,int previousVersion)
    {
        var schema = db.Schema + "_v" + previousVersion + "_" + (collision ? "collision" : "upgrade");
        try
        {
            await CreateVersionOne(schema);
            var names = typeof(PostgreSqlWorkStore).Assembly.GetManifestResourceNames()
                .Where(n => n.StartsWith("EventGuard.PostgreSql.Migrations.", StringComparison.Ordinal))
                .Select(n => n["EventGuard.PostgreSql.Migrations.".Length..])
                .Where(n => int.Parse(n[..3], System.Globalization.CultureInfo.InvariantCulture) is var v && v > 1 && v <= previousVersion)
                .Order(StringComparer.Ordinal);
            foreach (var name in names)
            {
                await using var stream = typeof(PostgreSqlWorkStore).Assembly.GetManifestResourceStream("EventGuard.PostgreSql.Migrations." + name)!;
                using var reader = new StreamReader(stream);
                await using var command = db.DataSource.CreateCommand((await reader.ReadToEndAsync()).Replace("__SCHEMA__",schema,StringComparison.Ordinal));
                await command.ExecuteNonQueryAsync();
            }
            var store = new PostgreSqlWorkStore(db.DataSource,schema);
            var scope = DatabaseFixture.Scope(); var submission = DatabaseFixture.Submission(scope);
            var work = await store.AcceptAsync(submission);
            var request = Guid.NewGuid();
            // Seed a legacy durable claim using its native layout. Current claims require current migrations.
            await using (var seed = db.DataSource.CreateCommand($"""
                BEGIN;
                UPDATE {schema}.works SET state=1,version=1,attempt_count=1,generation_attempt=1,lease_token=@request,lease_until=clock_timestamp()+interval '1 minute' WHERE id=@work;
                INSERT INTO {schema}.attempts(id,work_id,number,generation,status,started_at) VALUES(@request,@work,1,0,'Started',clock_timestamp());
                COMMIT;
                """))
            { seed.Parameters.AddWithValue("request",request); seed.Parameters.AddWithValue("work",work.Work.Id); await seed.ExecuteNonQueryAsync(); }
            var claim = (await store.ClaimNextAsync(scope,request,TimeSpan.FromMinutes(1)))!;
            if (collision)
            {
                var index = previousVersion switch { 3 => "ix_claim_active", 4 => "ix_claim_expiry", _ => "ix_claim_due" };
                await using var conflict = db.DataSource.CreateCommand($"CREATE INDEX {index} ON {schema}.works(id)");
                await conflict.ExecuteNonQueryAsync();
                await Assert.ThrowsAsync<Npgsql.PostgresException>(()=>store.InitializeAsync());
            }
            else await Task.WhenAll(store.InitializeAsync(),new PostgreSqlOutboxStore(db.DataSource,schema).InitializeAsync(),store.InitializeAsync());
            await using var version = db.DataSource.CreateCommand($"SELECT version FROM {schema}.schema_version WHERE singleton");
            Assert.Equal(collision ? previousVersion : 11,await version.ExecuteScalarAsync());
            var repeated = (await store.ClaimNextAsync(scope,request,TimeSpan.FromMinutes(1)))!;
            Assert.Equal(claim.Work,repeated.Work); Assert.Equal(claim.Token,repeated.Token); Assert.Equal(claim.LeaseUntil,repeated.LeaseUntil);
            Assert.Equal(AcceptanceStatus.Duplicate,(await store.AcceptAsync(submission)).Status);
            Assert.Equal(ExecutionStatus.Completed,(await store.ExecuteAsync(repeated,(_,_,_,_)=>ValueTask.CompletedTask,new())).Status);
            Assert.Null(await store.ClaimNextAsync(scope,request,TimeSpan.FromMinutes(1)));
            Assert.Equal(work.Work.Id,repeated.Work.Id);
        }
        finally
        {
            await using var drop = db.DataSource.CreateCommand($"DROP SCHEMA IF EXISTS {schema} CASCADE");
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task Version_one_upgrades_once_under_concurrent_initializers_and_preserves_work()
    {
        var schema = db.Schema + "_upgrade";
        try
        {
            await CreateVersionOne(schema);
            var store = new PostgreSqlWorkStore(db.DataSource, schema);
            var scope = DatabaseFixture.Scope();
            var submission = DatabaseFixture.Submission(scope);
            var accepted = await store.AcceptAsync(submission);
            var outbox = new PostgreSqlOutboxStore(db.DataSource, schema);
            await Task.WhenAll(store.InitializeAsync(), outbox.InitializeAsync(), store.InitializeAsync());
            Assert.Equal(accepted.Work, await store.FindAsync(scope, accepted.Work.Id));
            Assert.Equal(AcceptanceStatus.Duplicate, (await store.AcceptAsync(submission)).Status);
            await using var version = db.DataSource.CreateCommand($"SELECT version FROM {schema}.schema_version WHERE singleton");
            Assert.Equal(11, await version.ExecuteScalarAsync());
            var producer = new ProducerScope(scope.Tenant, scope.Consumer);
            var message = new OutgoingMessage(producer, Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "files.created"), [1]);
            await outbox.ProduceAsync(new(producer, Guid.NewGuid(), ContentFingerprint.FromBytes([1])),
                async (session, ct) => { await session.Outbox.EnqueueAsync(message, ct); });
            Assert.NotNull(await outbox.FindAsync(producer, message.IntentId));
        }
        finally
        {
            await using var drop = db.DataSource.CreateCommand($"DROP SCHEMA IF EXISTS {schema} CASCADE");
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task Version_two_preserves_envelope_and_old_attempt_budget_before_enabling_replay()
    {
        var schema = db.Schema + "_v2";
        try
        {
            await CreateVersionOne(schema);
            await using (var source = typeof(PostgreSqlWorkStore).Assembly.GetManifestResourceStream("EventGuard.PostgreSql.Migrations.002_outbox.sql")!)
            using (var reader = new StreamReader(source))
            await using (var migration = db.DataSource.CreateCommand((await reader.ReadToEndAsync()).Replace("__SCHEMA__", schema, StringComparison.Ordinal)))
                await migration.ExecuteNonQueryAsync();
            var outbox = new PostgreSqlOutboxStore(db.DataSource, schema);
            var scope = new ProducerScope("tenant", "legacy");
            var message = new OutgoingMessage(scope, Guid.NewGuid(), Guid.NewGuid(), new("test", "local", "v1", "files.created"), [1]);
            await outbox.ProduceAsync(new(scope, Guid.NewGuid(), ContentFingerprint.FromBytes([1])),
                async (session, ct) => { await session.Outbox.EnqueueAsync(message, ct); });
            await using (var legacy = db.DataSource.CreateCommand($"""
                UPDATE {schema}.outbox_messages SET state=4,version=8,attempt_count=4 WHERE id=@id;
                INSERT INTO {schema}.outbox_attempts(scope_id,id,intent_id,number,status,finished_at)
                SELECT @scope,gen_random_uuid(),@id,n,'NotSentPermanent',clock_timestamp() FROM generate_series(1,4) n;
                """))
            {
                legacy.Parameters.AddWithValue("id", message.IntentId);
                legacy.Parameters.AddWithValue("scope", scope.Id);
                await legacy.ExecuteNonQueryAsync();
            }
            await outbox.InitializeAsync();
            var saved = (await outbox.FindAsync(scope, message.IntentId))!;
            Assert.Equal(message.Fingerprint, saved.Message.Fingerprint);
            Assert.Equal(4, saved.AttemptCount);
            Assert.Equal(4, saved.GenerationAttempt);
            Assert.Equal(0, saved.ReplayGeneration);
            var recovery = new PostgreSqlOutboxRecoveryStore(db.DataSource, schema);
            Assert.Equal(OutboxReplayStatus.Accepted, (await recovery.ReplayAsync(new(scope, Guid.NewGuid(), message.IntentId, 8, "operator", "retry"))).Status);
            var next = (await outbox.ClaimNextAsync(scope, Guid.NewGuid(), TimeSpan.FromSeconds(10)))!;
            Assert.Equal(5, next.Record.AttemptCount);
            Assert.Equal(1, next.Record.GenerationAttempt);
            Assert.Equal(1, next.Record.ReplayGeneration);
            await using var attempts = db.DataSource.CreateCommand($"SELECT count(*) FROM {schema}.outbox_attempts WHERE generation=0");
            Assert.Equal(4L, await attempts.ExecuteScalarAsync());
        }
        finally
        {
            await using var drop = db.DataSource.CreateCommand($"DROP SCHEMA IF EXISTS {schema} CASCADE");
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task Unknown_schema_version_is_rejected_without_applying_outbox_migration()
    {
        var schema = db.Schema + "_future";
        try
        {
            await CreateVersionOne(schema);
            await using (var update = db.DataSource.CreateCommand($"UPDATE {schema}.schema_version SET version=999"))
                await update.ExecuteNonQueryAsync();
            await Assert.ThrowsAsync<InvalidOperationException>(() => new PostgreSqlOutboxStore(db.DataSource, schema).InitializeAsync());
            await using var check = db.DataSource.CreateCommand($"SELECT version FROM {schema}.schema_version WHERE singleton");
            Assert.Equal(999, await check.ExecuteScalarAsync());
            await using var table = db.DataSource.CreateCommand("SELECT to_regclass(@name)::text");
            table.Parameters.AddWithValue("name", schema + ".outbox_messages");
            Assert.Equal(DBNull.Value, await table.ExecuteScalarAsync());
        }
        finally
        {
            await using var drop = db.DataSource.CreateCommand($"DROP SCHEMA IF EXISTS {schema} CASCADE");
            await drop.ExecuteNonQueryAsync();
        }
    }

    private async Task CreateVersionOne(string schema)
    {
        await using var source = typeof(PostgreSqlWorkStore).Assembly.GetManifestResourceStream("EventGuard.PostgreSql.Migrations.001_initial.sql")!;
        using var reader = new StreamReader(source);
        var sql = (await reader.ReadToEndAsync()).Replace("__SCHEMA__", schema, StringComparison.Ordinal);
        await using var command = db.DataSource.CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }
}
