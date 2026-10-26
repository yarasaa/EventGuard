using System.Text.RegularExpressions;
using EventGuard;
using Npgsql;
using RabbitMQ.Client;

namespace CatalogPilot.Hosting;

public sealed class PilotSettings
{
    public string Schema { get; } = Environment.GetEnvironmentVariable("PILOT_SCHEMA") ?? "pilot_catalog_manual";
    public int Port { get; } = int.Parse(Environment.GetEnvironmentVariable("PILOT_PORT") ?? "5194");
    public string ConnectionString { get; } = Environment.GetEnvironmentVariable("PILOT_DB")
        ?? "Host=127.0.0.1;Port=55432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only;Maximum Pool Size=12;Command Timeout=10";
    public string InputQueue => Schema + ".changes";
    public string OutputQueue => Schema + ".indexed";
    public string QuotedSchema => '"' + Schema + '"';
    public WorkScope Consumer { get; } = new("pilot", "catalog-indexer", "catalog-api", "product.changed");
    public ProducerScope Producer { get; } = new("pilot", "catalog-api");
    public ProducerScope IndexedProducer { get; } = new("pilot", "catalog-indexer");
    public IdentityPolicy Policy { get; } = new("event-id/v1", "sha256-bytes/v1");
    public ConnectionFactory Broker() => new()
    {
        HostName = "127.0.0.1", Port = 5673, VirtualHost = "eventguard_test",
        UserName = "eventguard", Password = "eventguard-local-only",
        AutomaticRecoveryEnabled = false, TopologyRecoveryEnabled = false
    };
    public PilotSettings()
    {
        if (!Regex.IsMatch(Schema, "^pilot_catalog_[a-z0-9_]{1,32}$") || Port is < 1024 or > 65535)
            throw new ArgumentException("Use a bounded pilot_catalog_ schema and a local unprivileged port.");
        var db = new NpgsqlConnectionStringBuilder(ConnectionString);
        if (db.Host != "127.0.0.1" || db.Port != 55432 || db.Database != "eventguard_test")
            throw new ArgumentException("This pilot only connects to the local development database.");
    }
}
