using EventGuard.Store.Conformance;

namespace EventGuard.PostgreSql.Tests;

public sealed class OutboxConformanceTests(DatabaseFixture fixture) : OutboxStoreContract(fixture), IClassFixture<DatabaseFixture>;
