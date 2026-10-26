using EventGuard.Store.Conformance;
namespace EventGuard.PostgreSql.Tests;
public sealed class IngressQuarantineTests(DatabaseFixture fixture)
    : IngressQuarantineContract<PostgreSqlSession>(fixture), IClassFixture<DatabaseFixture>;
