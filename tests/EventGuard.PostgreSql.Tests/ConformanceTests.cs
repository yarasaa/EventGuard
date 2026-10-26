using EventGuard.Store.Conformance;

namespace EventGuard.PostgreSql.Tests;

public sealed class ConformanceTests(DatabaseFixture fixture)
    : WorkStoreContract<PostgreSqlSession>(fixture), IClassFixture<DatabaseFixture>;
