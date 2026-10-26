using EventGuard.Store.Conformance;
namespace EventGuard.SqlServer.Tests;
public sealed class IngressQuarantineTests(DatabaseFixture fixture)
    : IngressQuarantineContract<SqlServerSession>(fixture), IClassFixture<DatabaseFixture>;
