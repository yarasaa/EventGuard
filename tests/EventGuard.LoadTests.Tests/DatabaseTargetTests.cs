using EventGuard.LoadTests;
using Npgsql;

namespace EventGuard.LoadTests.Tests;

public sealed class DatabaseTargetTests
{
    [Theory]
    [InlineData("127.0.0.1", 55432, "eventguard_test", null)]
    [InlineData("localhost", 55432, "eventguard_test", "")]
    [InlineData("eventguard-load-postgres.eventguard-load-crc.svc", 5432, "eventguard_test", "eventguard-load-crc")]
    public void Accepts_explicit_test_targets(string host, int port, string database, string? testNamespace)
        => LocalDatabases.ValidatePostgreSqlTarget(new() { Host = host, Port = port, Database = database }, testNamespace);

    [Theory]
    [InlineData("127.0.0.1", 5432, "eventguard_test", null)]
    [InlineData("127.0.0.1", 55432, "application", null)]
    [InlineData("production.example.com", 5432, "eventguard_test", null)]
    [InlineData("eventguard-load-postgres.eventguard-load-crc.svc", 5432, "eventguard_test", null)]
    [InlineData("eventguard-load-postgres.eventguard-load-crc.svc", 5432, "application", "eventguard-load-crc")]
    [InlineData("eventguard-load-postgres.eventguard-load-crc.svc", 5432, "eventguard_test", "eventguard-load-other")]
    [InlineData("eventguard-load-postgres.production.svc", 5432, "eventguard_test", "production")]
    [InlineData("eventguard-load-postgres.eventguard-load-.svc", 5432, "eventguard_test", "eventguard-load-")]
    [InlineData("eventguard-load-postgres.eventguard-load-crc-.svc", 5432, "eventguard_test", "eventguard-load-crc-")]
    [InlineData("eventguard-load-postgres.eventguard-load-c.rc.svc", 5432, "eventguard_test", "eventguard-load-c.rc")]
    [InlineData("eventguard-load-postgres.eventguard-load-crc.svc,production", 5432, "eventguard_test", "eventguard-load-crc")]
    [InlineData("localhost", 55432, "eventguard_test", "eventguard-load-crc")]
    public void Rejects_other_databases_hosts_or_implicit_remote_access(string host, int port, string database, string? testNamespace)
        => Assert.Throws<ArgumentException>(() => LocalDatabases.ValidatePostgreSqlTarget(
            new() { Host = host, Port = port, Database = database }, testNamespace));
}
