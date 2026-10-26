using System.Collections.ObjectModel;

namespace EventGuard;

public sealed record MessageContractVersion
{
    public string Name { get; }
    public int Version { get; }
    public MessageContractVersion(string name, int version)
    {
        Name = IdentityRules.Required(name, nameof(name));
        if (version < 1) throw new ArgumentOutOfRangeException(nameof(version));
        Version = version;
    }
}

/// <summary>
/// An explicit version allowlist. Every decoder converts original bytes to the application's current model.
/// Wire format, schema registry and model validation belong to the application; no implicit latest-version fallback.
/// </summary>
public sealed class VersionedMessageContract<T> where T : notnull
{
    private readonly Func<ReadOnlyMemory<byte>, MessageContractVersion> _identify;
    private readonly IReadOnlyDictionary<int, Func<ReadOnlyMemory<byte>, T>> _decoders;
    private readonly Action<T> _validate;
    public string Name { get; }
    public int CurrentVersion { get; }
    public IReadOnlyList<int> SupportedVersions { get; }

    public VersionedMessageContract(string name, int currentVersion,
        Func<ReadOnlyMemory<byte>, MessageContractVersion> identify,
        IReadOnlyDictionary<int, Func<ReadOnlyMemory<byte>, T>> decoders, Action<T> validate)
    {
        Name = IdentityRules.Required(name, nameof(name));
        if (currentVersion < 1) throw new ArgumentOutOfRangeException(nameof(currentVersion));
        ArgumentNullException.ThrowIfNull(identify); ArgumentNullException.ThrowIfNull(decoders); ArgumentNullException.ThrowIfNull(validate);
        if (decoders.Count is < 1 or > 64 || !decoders.ContainsKey(currentVersion)
            || decoders.Any(p => p.Key < 1 || p.Key > currentVersion || p.Value is null))
            throw new ArgumentException("Register 1–64 explicit versions including the current version; future versions are not compatible.", nameof(decoders));
        CurrentVersion = currentVersion; _identify = identify; _validate = validate;
        _decoders = new ReadOnlyDictionary<int, Func<ReadOnlyMemory<byte>, T>>(new Dictionary<int, Func<ReadOnlyMemory<byte>, T>>(decoders));
        SupportedVersions = Array.AsReadOnly(decoders.Keys.Order().ToArray());
    }

    public T Decode(ReadOnlyMemory<byte> originalPayload)
    {
        if (originalPayload.Length > 1024 * 1024) throw new IngressMessageRejectedException("contract-payload-too-large");
        var version = _identify(originalPayload) ?? throw new IngressMessageRejectedException("missing-contract");
        if (!string.Equals(version.Name, Name, StringComparison.Ordinal)) throw new IngressMessageRejectedException("wrong-contract");
        if (!_decoders.TryGetValue(version.Version, out var decode)) throw new IngressMessageRejectedException("unsupported-contract-version");
        var model = decode(originalPayload);
        if (model is null) throw new IngressMessageRejectedException("missing-contract-model");
        _validate(model);
        return model;
    }

    /// <summary>Preserves the original bytes and their fingerprint while explicitly mapping business identity.</summary>
    public WorkSubmission Map(WorkScope scope, IdentityPolicy policy, ReadOnlyMemory<byte> originalPayload,
        Func<T, (string BusinessKey, Guid MessageId)> identity)
    {
        ArgumentNullException.ThrowIfNull(scope); ArgumentNullException.ThrowIfNull(policy); ArgumentNullException.ThrowIfNull(identity);
        if (scope.EventType != Name) throw new InvalidOperationException("The registered scope and message contract must agree.");
        if (originalPayload.Length > 1024 * 1024) throw new IngressMessageRejectedException("contract-payload-too-large");
        // Freeze before running user codecs so a converter cannot rewrite durable input through its backing array.
        var snapshot = originalPayload.ToArray();
        var model = Decode(snapshot.ToArray());
        var mapped = identity(model);
        return new(new(scope, policy, mapped.BusinessKey), mapped.MessageId, snapshot, ContentFingerprint.FromBytes(snapshot));
    }
}
