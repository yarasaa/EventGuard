using Microsoft.Data.SqlClient;
using EventGuard.Store.Conformance;

namespace EventGuard.SqlServer.Tests;

public sealed class DatabaseFixture : IAsyncLifetime, IWorkStoreFixture<SqlServerSession>, IOutboxStoreFixture, IJobStoreFixture<SqlServerSession>
{
    public const string LocalConnection = "Server=127.0.0.1,51433;Database=eventguard_test;User ID=sa;Password=EventGuard-Local-Only!2026;Encrypt=False;TrustServerCertificate=True;Connect Timeout=5;Max Pool Size=8;ConnectRetryCount=0";
    public string ConnectionString { get; } = Environment.GetEnvironmentVariable("EVENTGUARD_SQLSERVER_TEST_CONNECTION") ?? LocalConnection;
    public string Schema { get; } = "eg_sql_test_" + Guid.NewGuid().ToString("N");
    public SqlServerDataSource DataSource { get; private set; } = null!;
    public SqlServerWorkStore Store { get; private set; } = null!;
    public SqlServerOutboxStore Outbox { get; private set; } = null!;
    public IOutboxRecoveryStore Recovery { get; private set; } = null!;
    ITransactionalWorkStore<SqlServerSession> IWorkStoreFixture<SqlServerSession>.Store => Store;
    IOutboxStore IOutboxStoreFixture.Outbox => Outbox;
    public ITransactionalWorkStore<SqlServerSession> RestartStore() => new SqlServerWorkStore(DataSource, Schema);
    public IOutboxRecoveryStore RestartRecovery() => new SqlServerOutboxRecoveryStore(DataSource, Schema);
    public async Task EnqueueAsync(OutgoingMessage message) => await Outbox.ProduceAsync(
        new(message.Scope, message.IntentId, ContentFingerprint.FromBytes(message.CopyPayload())),
        async (session, ct) => { await session.Outbox.EnqueueAsync(message, ct); });

    public ITransactionalJobStore<SqlServerSession> Jobs => Store;
    public Task InitializeJobsAsync() => Store.InitializeJobsAsync();

    public async Task InitializeAsync()
    {
        var options = new SqlConnectionStringBuilder(ConnectionString);
        if (options.DataSource is not ("127.0.0.1,51433" or "localhost,51433") || options.InitialCatalog != "eventguard_test")
            throw new InvalidOperationException("Integration tests only use the local eventguard_test SQL Server on port 51433.");
        // Only the test harness creates the database. Library initialization never changes DB-wide options.
        var adminOptions = new SqlConnectionStringBuilder(ConnectionString) { InitialCatalog = "master" };
        await using (var admin = new SqlConnection(adminOptions.ConnectionString))
        {
            await admin.OpenAsync();
            await using var command = new SqlCommand("""
                DECLARE @result int;
                EXEC @result=sys.sp_getapplock @Resource='EventGuard-test-database-setup',@LockMode='Exclusive',@LockOwner='Session',@LockTimeout=15000;
                IF @result<0 THROW 51002,'Test database lock unavailable.',1;
                IF DB_ID('eventguard_test') IS NULL
                BEGIN
                    CREATE DATABASE eventguard_test;
                    ALTER DATABASE eventguard_test SET READ_COMMITTED_SNAPSHOT ON;
                END;
                EXEC sys.sp_releaseapplock @Resource='EventGuard-test-database-setup',@LockOwner='Session';
                """, admin);
            await command.ExecuteNonQueryAsync();
        }
        DataSource = new(ConnectionString);
        Store = new(DataSource, Schema);
        await Store.InitializeAsync();
        Outbox = new(DataSource, Schema);
        Recovery = RestartRecovery();
        await using var connection = await DataSource.OpenConnectionAsync();
        await using var setup = new SqlCommand($"CREATE TABLE {Schema}.business_effects(work_id uniqueidentifier NOT NULL,note nvarchar(128) NOT NULL)", connection);
        await setup.ExecuteNonQueryAsync();
    }
    public async Task DisposeAsync()
    {
        if (DataSource is null) return;
        await using var connection = await DataSource.OpenConnectionAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted);
        await using var command = new SqlCommand("""
            -- Different schemas still share SQL Server catalog rows. Serialize fixture teardown across processes.
            DECLARE @result int;
            EXEC @result=sys.sp_getapplock @Resource='EventGuard-test-schema-cleanup',@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=15000;
            IF @result<0 THROW 51004,'Test schema cleanup lock unavailable.',1;
            IF SCHEMA_ID(@schema) IS NULL RETURN;
            DECLARE @sql nvarchar(max)=N'';
            SELECT @sql+=N'ALTER TABLE '+QUOTENAME(s.name)+N'.'+QUOTENAME(t.name)+N' DROP CONSTRAINT '+QUOTENAME(f.name)+N';'
            FROM sys.foreign_keys f JOIN sys.tables t ON f.parent_object_id=t.object_id JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE s.name=@schema;
            SELECT @sql+=N'DROP TABLE '+QUOTENAME(@schema)+N'.'+QUOTENAME(name)+N';' FROM sys.tables WHERE schema_id=SCHEMA_ID(@schema);
            SET @sql+=N'DROP SCHEMA '+QUOTENAME(@schema)+N';';
            EXEC sys.sp_executesql @sql;
            """, connection, transaction);
        command.Parameters.AddWithValue("schema", Schema);
        await command.ExecuteNonQueryAsync();
        await transaction.CommitAsync();
    }
    public async ValueTask Effect(WorkItem work, ReadOnlyMemory<byte> payload, SqlServerSession session, CancellationToken ct)
    {
        await using var command = session.CreateCommand($"INSERT INTO {Schema}.business_effects VALUES(@id,'processed')");
        command.Parameters.AddWithValue("id", work.Id);
        await command.ExecuteNonQueryAsync(ct);
    }
    public async Task<long> EffectCount(Guid workId)
    {
        await using var connection = await DataSource.OpenConnectionAsync();
        await using var command = new SqlCommand($"SELECT COUNT_BIG(*) FROM {Schema}.business_effects WHERE work_id=@id", connection);
        command.Parameters.AddWithValue("id", workId);
        return (long)(await command.ExecuteScalarAsync())!;
    }
    public static WorkScope Scope() => StoreContractData.Scope();
    public static WorkSubmission Submission(WorkScope scope, string key = "file-1", byte[]? payload = null,
        string version = "v1", DateTimeOffset? notBefore = null, DateTimeOffset? expiresAt = null) =>
        StoreContractData.Submission(scope, key, payload, version, notBefore, expiresAt);
    public Task<WorkClaim?> Claim(WorkScope scope, TimeSpan? lease = null) => Store.ClaimNextAsync(scope, Guid.NewGuid(), lease ?? TimeSpan.FromSeconds(10));
}
