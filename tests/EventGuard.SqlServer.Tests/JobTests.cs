using EventGuard.Store.Conformance;

namespace EventGuard.SqlServer.Tests;

public sealed class JobTests(DatabaseFixture fixture) : JobStoreContractTests<SqlServerSession>(fixture), IClassFixture<DatabaseFixture>;
