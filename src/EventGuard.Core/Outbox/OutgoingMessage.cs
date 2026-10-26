using System.Collections.ObjectModel;
using System.Globalization;

namespace EventGuard;

public sealed record ProducerScope
{
    public string Tenant { get; }
    public string Producer { get; }
    public string Id { get; }

    public ProducerScope(string tenant, string producer)
    {
        Tenant = IdentityRules.Required(tenant, nameof(tenant));
        Producer = IdentityRules.Required(producer, nameof(producer));
        Id = StableHash.Of("producer/v1", Tenant, Producer);
    }
}

// Connection is a versioned logical binding, never a connection string or credential.
public sealed record PublishRoute
{
    public string Transport { get; }
    public string Connection { get; }
    public string ConfigurationVersion { get; }
    public string Address { get; }
    public string? RoutingKey { get; }

    public PublishRoute(string transport, string connection, string configurationVersion, string address, string? routingKey = null)
    {
        Transport = IdentityRules.Required(transport, nameof(transport));
        Connection = IdentityRules.Required(connection, nameof(connection));
        ConfigurationVersion = IdentityRules.Required(configurationVersion, nameof(configurationVersion));
        Address = IdentityRules.Required(address, nameof(address));
        if (routingKey is not null && routingKey.Length > 1024) throw new ArgumentException("Routing key exceeds limit.", nameof(routingKey));
        if (routingKey is not null) _ = StableHash.Of(routingKey);
        RoutingKey = routingKey;
    }
}

public sealed class OutgoingMessage
{
    private readonly byte[] _payload;
    public ProducerScope Scope { get; }
    public Guid IntentId { get; }
    public Guid MessageId { get; }
    public PublishRoute Route { get; }
    public string ContentType { get; }
    public IReadOnlyDictionary<string, string> Headers { get; }
    public DateTimeOffset? NotBefore { get; }
    public string Fingerprint { get; }
    public byte[] CopyPayload() => (byte[])_payload.Clone();

    public OutgoingMessage(ProducerScope scope, Guid intentId, Guid messageId, PublishRoute route,
        ReadOnlySpan<byte> payload, string contentType = "application/json",
        IReadOnlyDictionary<string, string>? headers = null, DateTimeOffset? notBefore = null)
    {
        Scope = scope ?? throw new ArgumentNullException(nameof(scope));
        Route = route ?? throw new ArgumentNullException(nameof(route));
        if (intentId == Guid.Empty || messageId == Guid.Empty) throw new ArgumentException("Stable intent and message IDs are required.");
        if (payload.Length > 1024 * 1024) throw new ArgumentException("Inline payload limit is 1 MiB.", nameof(payload));
        ContentType = IdentityRules.Required(contentType, nameof(contentType));
        if (headers?.Count > 32) throw new ArgumentException("At most 32 headers are supported.", nameof(headers));
        var copied = new SortedDictionary<string, string>(StringComparer.Ordinal);
        if (headers is not null)
            foreach (var (key, value) in headers)
                copied.Add(IdentityRules.Required(key, nameof(headers)), IdentityRules.Required(value, nameof(headers)));
        Headers = new ReadOnlyDictionary<string, string>(copied);
        IntentId = intentId;
        MessageId = messageId;
        _payload = payload.ToArray();
        // Canonical scheduling precision is one microsecond across providers and restarts.
        NotBefore = notBefore is { } due ? new DateTimeOffset(due.UtcTicks - due.UtcTicks % 10, TimeSpan.Zero) : null;
        var parts = new List<string> { "outbox/v1", Scope.Id, IntentId.ToString("D"), MessageId.ToString("D"),
            Route.Transport, Route.Connection, Route.ConfigurationVersion, Route.Address,
            Route.RoutingKey is null ? "0" : "1", Route.RoutingKey ?? "", ContentType,
            NotBefore?.UtcDateTime.Ticks.ToString(CultureInfo.InvariantCulture) ?? "",
            ContentFingerprint.FromBytes(payload).Value };
        foreach (var (key, value) in copied) { parts.Add(key); parts.Add(value); }
        Fingerprint = StableHash.Of(parts.ToArray());
    }
}
