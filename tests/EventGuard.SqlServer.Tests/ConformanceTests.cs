using EventGuard.Store.Conformance;

namespace EventGuard.SqlServer.Tests;

public sealed class ConformanceTests(DatabaseFixture db) : WorkStoreContract<SqlServerSession>(db), IClassFixture<DatabaseFixture>;
public sealed class OutboxConformanceTests(DatabaseFixture db) : OutboxStoreContract(db), IClassFixture<DatabaseFixture>;
