using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace EventGuard;

public sealed record WorkScope
{
    public string Tenant { get; }
    public string Consumer { get; }
    public string Source { get; }
    public string EventType { get; }
    public string Id { get; }

    public WorkScope(string tenant, string consumer, string source, string eventType)
    {
        Tenant = IdentityRules.Required(tenant, nameof(tenant));
        Consumer = IdentityRules.Required(consumer, nameof(consumer));
        Source = IdentityRules.Required(source, nameof(source));
        EventType = IdentityRules.Required(eventType, nameof(eventType));
        Id = StableHash.Of("scope/v1", Tenant, Consumer, Source, EventType);
    }
}

public sealed record IdentityPolicy
{
    public string KeyVersion { get; }
    public string FingerprintVersion { get; }

    public IdentityPolicy(string keyVersion, string fingerprintVersion)
    {
        KeyVersion = IdentityRules.Required(keyVersion, nameof(keyVersion));
        FingerprintVersion = IdentityRules.Required(fingerprintVersion, nameof(fingerprintVersion));
    }
}

public sealed record WorkIdentity
{
    public WorkScope Scope { get; }
    public IdentityPolicy Policy { get; }
    public string BusinessKey { get; }
    public string Id { get; }

    public WorkIdentity(WorkScope scope, IdentityPolicy policy, string businessKey)
    {
        Scope = scope ?? throw new ArgumentNullException(nameof(scope));
        Policy = policy ?? throw new ArgumentNullException(nameof(policy));
        BusinessKey = IdentityRules.Required(businessKey, nameof(businessKey));
        // Policy versions are a persisted scope binding, not a way to create a second job.
        Id = StableHash.Of("work/v1", Scope.Id, BusinessKey);
    }
}

public sealed record ContentFingerprint
{
    public string Value { get; }

    private ContentFingerprint(string value) => Value = value;

    public static ContentFingerprint FromBytes(ReadOnlySpan<byte> canonicalContent) =>
        new(Convert.ToHexString(SHA256.HashData(canonicalContent)));

    public static ContentFingerprint Parse(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value.Length != 64 || value.Any(c => !Uri.IsHexDigit(c)))
            throw new ArgumentException("Fingerprint must be a SHA-256 hexadecimal digest.", nameof(value));
        return new(value.ToUpperInvariant());
    }
}

public static class StableHash
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    public static string Of(params string[] parts)
    {
        ArgumentNullException.ThrowIfNull(parts);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> length = stackalloc byte[4];
        foreach (var part in parts)
        {
            ArgumentNullException.ThrowIfNull(part);
            var bytes = StrictUtf8.GetBytes(part);
            BinaryPrimitives.WriteInt32BigEndian(length, bytes.Length);
            hash.AppendData(length);
            hash.AppendData(bytes);
        }
        return Convert.ToHexString(hash.GetHashAndReset());
    }
}

internal static class IdentityRules
{
    public static string Required(string value, string parameter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, parameter);
        if (Encoding.UTF8.GetByteCount(value) > 1024)
            throw new ArgumentException("Identity component exceeds 1024 UTF-8 bytes.", parameter);
        // No trimming, casing, Unicode normalization or culture-dependent conversion is implicit.
        _ = StableHash.Of(value);
        return value;
    }
}
