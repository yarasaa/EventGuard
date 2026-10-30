using System.Data.Common;
using EventGuard.PostgreSql;
using EventGuard.SqlServer;
using Npgsql;
using Microsoft.Data.SqlClient;
namespace EventGuard.LoadTests;

internal sealed class LoadDatabase<TSession>(ITransactionalWorkStore<TSession> store, string schema, string provider,
    Func<CancellationToken,Task> initialize, Func<CancellationToken,Task<DbConnection>> open,
    Func<TSession,string,DbCommand> transactionCommand, Func<ValueTask> dispose) : IAsyncDisposable
{
    public ITransactionalWorkStore<TSession> Store { get; } = store;
    public string Schema { get; } = schema;
    internal Task<DbConnection> OpenAsync(CancellationToken ct) => open(ct);
    private string Count => provider == "postgresql" ? "count(*)" : "count_big(*)";
    public async Task InitializeAsync(CancellationToken ct)
    {
        await initialize(ct);
        await ExecuteAsync(provider == "postgresql"
            ? $"CREATE TABLE {Schema}.effects(scope_id text NOT NULL,work_id uuid NOT NULL,ordinal integer NOT NULL,fingerprint text NOT NULL); CREATE INDEX ix_effects ON {Schema}.effects(scope_id,work_id)"
            : $"CREATE TABLE {Schema}.effects(scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,work_id uniqueidentifier NOT NULL,ordinal int NOT NULL,fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL); CREATE INDEX ix_effects ON {Schema}.effects(scope_id,work_id)", ct);
    }
    public async ValueTask Effect(WorkItem work, ReadOnlyMemory<byte> payload, TSession session, CancellationToken ct)
    {
        var ordinal = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(payload.Span);
        // Deliberately no unique work_id constraint: a double business application must be observable.
        await using var command = transactionCommand(session, $"INSERT INTO {Schema}.effects(scope_id,work_id,ordinal,fingerprint) VALUES(@scope,@id,@ordinal,@fingerprint)");
        Add(command,"scope",work.ScopeId); Add(command,"id",work.Id); Add(command,"ordinal",ordinal);
        Add(command,"fingerprint",ContentFingerprint.FromBytes(payload.Span).Value);
        await command.ExecuteNonQueryAsync(ct);
    }
    public async Task<Oracle> VerifyAsync(WorkScope scope, int expected, CancellationToken ct,int background = 0,bool leased = false,int? ordinalLimit = null)
    {
        await using var connection = await open(ct);
        await using var command = connection.CreateCommand(); command.CommandTimeout = 30;
        command.CommandText = $"""
            SELECT
              (SELECT {Count} FROM {Schema}.works WHERE scope_id=@scope),
              (SELECT {Count} FROM {Schema}.works WHERE scope_id=@scope AND state=3),
              (SELECT {Count} FROM {Schema}.effects WHERE scope_id=@scope),
              (SELECT {Count} FROM {Schema}.works w WHERE scope_id=@scope AND business_key NOT LIKE 'background-%' AND (SELECT {Count} FROM {Schema}.effects e WHERE e.scope_id=w.scope_id AND e.work_id=w.id)<>1),
              (SELECT {Count} FROM {Schema}.effects e LEFT JOIN {Schema}.works w ON w.id=e.work_id AND w.scope_id=e.scope_id
                  WHERE e.scope_id=@scope AND (w.id IS NULL OR e.fingerprint<>w.fingerprint OR e.ordinal<0 OR e.ordinal>=@expected OR e.ordinal<>CASE WHEN w.business_key LIKE 'background-%' THEN -1 ELSE CAST(w.business_key AS int) END)),
              (SELECT {Count} FROM (SELECT ordinal FROM {Schema}.effects WHERE scope_id=@scope GROUP BY ordinal) AS ordinals),
              (SELECT {Count} FROM {Schema}.works WHERE scope_id=@scope AND business_key NOT LIKE 'background-%' AND attempt_count<>1),
              (SELECT {Count} FROM {Schema}.works WHERE scope_id=@scope AND business_key LIKE 'background-%'),
              (SELECT {Count} FROM {Schema}.works WHERE scope_id=@scope AND business_key LIKE 'background-%' AND
                (state<>@backgroundState OR version<>@backgroundState OR attempt_count<>@backgroundState OR expires_at IS NOT NULL OR
                  {(leased ? "lease_until" : "next_attempt_at")}<={(provider == "postgresql" ? "statement_timestamp()" : "TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00')")}))
            """;
        Add(command,"scope",scope.Id); Add(command,"expected",ordinalLimit ?? expected); Add(command,"backgroundState",leased ? 1 : 0);
        await using var reader = await command.ExecuteReaderAsync(ct); await reader.ReadAsync(ct);
        return new(expected, reader.GetInt64(0),reader.GetInt64(1),reader.GetInt64(2),reader.GetInt64(3),reader.GetInt64(4),reader.GetInt64(5),reader.GetInt64(6),background,reader.GetInt64(7),reader.GetInt64(8));
    }
    // Arrival ordinals can have holes. Counts/ranges alone cannot prove the exact accepted set.
    public async Task<AdmissionSetCheck> VerifyAdmissionSetAsync(WorkScope scope,Guid[] receipts,CancellationToken ct)
    {
        var expected = receipts.Count(id => id != Guid.Empty);
        long matched = 0, unexpected = 0;
        await using var connection = await open(ct);
        await using var command = connection.CreateCommand(); command.CommandTimeout=30;
        command.CommandText=$"SELECT business_key,id FROM {Schema}.works WHERE scope_id=@scope";
        Add(command,"scope",scope.Id);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var key=reader.GetString(0);
            if (!int.TryParse(key,System.Globalization.NumberStyles.None,System.Globalization.CultureInfo.InvariantCulture,out var ordinal)
                || ordinal < 0 || ordinal >= receipts.Length || receipts[ordinal] == Guid.Empty || receipts[ordinal] != reader.GetGuid(1)) unexpected++;
            else matched++;
        }
        return new(expected,matched,unexpected);
    }
    public async Task<Dictionary<string,string>> ProfileAsync(CancellationToken ct)
    {
        var sql = provider == "postgresql" ? """
            SELECT 'server_version',version() UNION ALL SELECT name,setting FROM pg_settings WHERE name IN
            ('fsync','synchronous_commit','full_page_writes','shared_buffers','max_connections','wal_level')
            """ : """
            SELECT 'server_version',CONVERT(nvarchar(4000),@@VERSION) COLLATE DATABASE_DEFAULT
            UNION ALL SELECT 'rcsi',CONVERT(nvarchar(50),is_read_committed_snapshot_on) COLLATE DATABASE_DEFAULT FROM sys.databases WHERE database_id=DB_ID()
            UNION ALL SELECT 'delayed_durability',CONVERT(nvarchar(50),delayed_durability_desc) COLLATE DATABASE_DEFAULT FROM sys.databases WHERE database_id=DB_ID()
            UNION ALL SELECT 'recovery_model',CONVERT(nvarchar(50),recovery_model_desc) COLLATE DATABASE_DEFAULT FROM sys.databases WHERE database_id=DB_ID()
            """;
        await using var connection = await open(ct); await using var command = connection.CreateCommand(); command.CommandText = sql;
        var result = new Dictionary<string,string>(); await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) result.Add(reader.GetString(0),reader.GetString(1));
        if (provider == "postgresql" && (result["fsync"] != "on" || result["synchronous_commit"] != "on" || result["full_page_writes"] != "on"))
            throw new InvalidOperationException("Load tests require the normal durable PostgreSQL profile.");
        return result;
    }
    public async Task CleanupAsync(CancellationToken ct)
    {
        if (!Schema.StartsWith("eg_load_",StringComparison.Ordinal) || Schema.Length != 40 || Schema[8..].Any(c=>!Uri.IsHexDigit(c)))
            throw new InvalidOperationException("Only this run's generated fixture schema can be removed.");
        await ExecuteAsync(provider == "postgresql" ? $"DROP SCHEMA {Schema} CASCADE" : $"""
            DECLARE @sql nvarchar(max)=N'';
            SELECT @sql+=N'ALTER TABLE '+QUOTENAME(s.name)+N'.'+QUOTENAME(t.name)+N' DROP CONSTRAINT '+QUOTENAME(f.name)+N';'
            FROM sys.foreign_keys f JOIN sys.tables t ON f.parent_object_id=t.object_id JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE s.name='{Schema}';
            SELECT @sql+=N'DROP TABLE '+QUOTENAME('{Schema}')+N'.'+QUOTENAME(name)+N';' FROM sys.tables WHERE schema_id=SCHEMA_ID('{Schema}');
            SELECT @sql+=N'DROP TYPE '+QUOTENAME('{Schema}')+N'.'+QUOTENAME(name)+N';' FROM sys.types WHERE is_user_defined=1 AND schema_id=SCHEMA_ID('{Schema}');
            SET @sql+=N'DROP SCHEMA '+QUOTENAME('{Schema}')+N';'; EXEC sys.sp_executesql @sql;
            """, ct);
    }
    internal async Task ExecuteAsync(string sql, CancellationToken ct)
    {
        await using var connection = await open(ct); await using var command = connection.CreateCommand();
        command.CommandText = sql; command.CommandTimeout = 30; await command.ExecuteNonQueryAsync(ct);
    }
    private static void Add(DbCommand command, string name, object value)
    { var parameter = command.CreateParameter(); parameter.ParameterName = name; parameter.Value = value; command.Parameters.Add(parameter); }
    public ValueTask DisposeAsync() => dispose();
}
internal sealed record AdmissionSetCheck(int Expected,long Matched,long Unexpected)
{
    public bool Valid => Matched == Expected && Unexpected == 0;
}
internal sealed record Oracle(int Expected, long Stored, long Completed, long Effects, long WorksWithoutExactlyOneEffect,
    long InvalidEffects, long DistinctOrdinals, long WorksWithUnexpectedAttempts,int ExpectedBackground=0,long Background=0,long InvalidBackground=0)
{
    public bool Valid => Stored == Expected + ExpectedBackground && Background == ExpectedBackground && InvalidBackground == 0
        && Completed == Expected && Effects == Expected && DistinctOrdinals == Expected
        && WorksWithoutExactlyOneEffect == 0 && InvalidEffects == 0 && WorksWithUnexpectedAttempts == 0;
}

internal static class LocalDatabases
{
    public static LoadDatabase<PostgreSqlSession> PostgreSql(int pool)
    {
        var config = new NpgsqlConnectionStringBuilder(Environment.GetEnvironmentVariable("EVENTGUARD_LOAD_POSTGRES")
            ?? "Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only");
        ValidatePostgreSqlTarget(config, Environment.GetEnvironmentVariable("EVENTGUARD_LOAD_KUBERNETES_TEST_NAMESPACE"));
        config.MaxPoolSize=pool; config.MinPoolSize=0; config.Timeout=5; config.CommandTimeout=10; config.Enlist=false; config.Multiplexing=false;
        var data = NpgsqlDataSource.Create(config.ConnectionString); var schema = "eg_load_" + Guid.NewGuid().ToString("N");
        var store = new PostgreSqlWorkStore(data,schema);
        return new(store,schema,"postgresql",store.InitializeAsync,async ct=>await data.OpenConnectionAsync(ct),
            (session,sql)=>session.CreateCommand(sql), data.DisposeAsync);
    }
    internal static void ValidatePostgreSqlTarget(NpgsqlConnectionStringBuilder config, string? testNamespace)
    {
        var local = string.IsNullOrEmpty(testNamespace)
            && config.Host is "127.0.0.1" or "localhost" && config.Port == 55432;
        // Explicit opt-in, dedicated test namespace/service and database; never allow arbitrary remote targets.
        var cluster = testNamespace is { Length: <= 63 }
            && testNamespace.StartsWith("eventguard-load-", StringComparison.Ordinal)
            && testNamespace.Length > "eventguard-load-".Length
            && testNamespace.All(c => char.IsAsciiLetterLower(c) || char.IsAsciiDigit(c) || c == '-')
            && testNamespace[^1] != '-'
            && config.Host == $"eventguard-load-postgres.{testNamespace}.svc" && config.Port == 5432;
        if (config.Database != "eventguard_test" || (!local && !cluster))
            throw new ArgumentException("Load tests require the guarded local database or an explicitly selected eventguard-load-* Kubernetes test service and eventguard_test database.");
    }
    public static LoadDatabase<SqlServerSession> SqlServer(int pool)
    {
        var config = new SqlConnectionStringBuilder(Environment.GetEnvironmentVariable("EVENTGUARD_LOAD_SQLSERVER")
            ?? "Server=127.0.0.1,51433;Database=eventguard_test;User ID=sa;Password=EventGuard-Local-Only!2026;Encrypt=False;TrustServerCertificate=True");
        if (config.DataSource is not ("127.0.0.1,51433" or "localhost,51433") || config.InitialCatalog != "eventguard_test")
            throw new ArgumentException("Load tests only use local eventguard_test on port 51433.");
        config.MaxPoolSize=pool; config.MinPoolSize=0; config.ConnectTimeout=5; config.CommandTimeout=10;
        config.Enlist=false; config.MultipleActiveResultSets=false; config.ConnectRetryCount=0;
        var data = new SqlServerDataSource(config.ConnectionString); var schema = "eg_load_" + Guid.NewGuid().ToString("N");
        var store = new SqlServerWorkStore(data,schema);
        return new(store,schema,"sqlserver",store.InitializeAsync,async ct=>await data.OpenConnectionAsync(ct),
            (session,sql)=>session.CreateCommand(sql), ()=>ValueTask.CompletedTask);
    }
}
