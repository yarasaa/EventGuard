using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
namespace EventGuard.Dashboard;

// Operator-supplied replacement/backup data. Never loaded automatically from raw quarantine storage.
public sealed record RecoveryManifest(Guid MessageId, string PayloadBase64, string? BusinessKey = null,
    string? CanonicalFingerprint = null, DateTimeOffset? NotBefore = null, DateTimeOffset? ExpiresAt = null,
    string? CorrelationId = null, string? Transport = null, string? Connection = null, string? ConfigurationVersion = null,
    string? Address = null, string? RoutingKey = null, string ContentType = "application/json", Dictionary<string, string>? Headers = null)
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    { UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow, MaxDepth = 8 };
    public static RecoveryManifest Parse(string json)
    {
        if (string.IsNullOrWhiteSpace(json) || Encoding.UTF8.GetByteCount(json) > 4096) throw new ArgumentException("The recovery manifest must fit in 4 KiB.");
        // Reject duplicate property names; a review must have one unambiguous interpretation.
        using var document = JsonDocument.Parse(json, new() { MaxDepth = 8 });
        ValidateProperties(document.RootElement);
        return JsonSerializer.Deserialize<RecoveryManifest>(json, Options) ?? throw new ArgumentException("A recovery manifest is required.");
    }
    private static void ValidateProperties(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var property in element.EnumerateObject())
            { if (!names.Add(property.Name)) throw new ArgumentException("Duplicate manifest properties are not allowed."); ValidateProperties(property.Value); }
        }
        else if (element.ValueKind == JsonValueKind.Array)
            foreach (var item in element.EnumerateArray()) ValidateProperties(item);
    }
    private byte[] Payload()
    {
        if (PayloadBase64 is null || PayloadBase64.Length > 3072) throw new ArgumentException("The dashboard accepts at most 2 KiB of replacement payload.");
        var bytes = Convert.FromBase64String(PayloadBase64);
        if (bytes.Length > 2048) throw new ArgumentException("The dashboard accepts at most 2 KiB of replacement payload.");
        return bytes;
    }
    public WorkSubmission Submission(WorkScope scope, string keyVersion, string fingerprintVersion)
    {
        var bytes = Payload();
        var fingerprint = CanonicalFingerprint is null && fingerprintVersion == "sha256-bytes/v1"
            ? ContentFingerprint.FromBytes(bytes) : ContentFingerprint.Parse(CanonicalFingerprint ?? "");
        if (fingerprintVersion == "sha256-bytes/v1" && fingerprint != ContentFingerprint.FromBytes(bytes))
            throw new ArgumentException("The byte fingerprint does not match the replacement payload.");
        return new(new(scope, new(keyVersion, fingerprintVersion), BusinessKey!), MessageId, bytes, fingerprint, NotBefore, ExpiresAt, CorrelationId);
    }
    public OutgoingMessage Outgoing(ProducerScope scope, Guid id) => new(scope, id, MessageId,
        new(Transport!, Connection!, ConfigurationVersion!, Address!, RoutingKey), Payload(), ContentType, Headers, NotBefore);
}
