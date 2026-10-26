using EventGuard.Store.Conformance;

namespace EventGuard.PostgreSql.Tests;

public sealed class JobTests(DatabaseFixture fixture) : JobStoreContractTests<PostgreSqlSession>(fixture), IClassFixture<DatabaseFixture>;
