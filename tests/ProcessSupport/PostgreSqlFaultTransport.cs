namespace Npgsql;

internal static class PostgreSqlFaultTransport
{
    // Test-only wire proxies inspect PostgreSQL frames. Npgsql 8 predates GSS encryption support.
    public static void DisableGssEncryptionForFaultProxy(this NpgsqlConnectionStringBuilder settings)
    {
#if !NET8_0
        settings.GssEncryptionMode = GssEncryptionMode.Disable;
#endif
    }
}
