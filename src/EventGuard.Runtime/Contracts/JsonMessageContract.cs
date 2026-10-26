using System.Text.Json;

namespace EventGuard.Runtime;

/// <summary>A single, explicit JSON contract version. Original wire bytes are retained for deduplication.</summary>
public sealed class JsonMessageContract<T> where T : notnull
{
    private readonly Func<T, int> _version;
    private readonly Action<T> _validate;
    private readonly JsonSerializerOptions _json;
    public string Name { get; }
    public int Version { get; }

    public JsonMessageContract(string name, int version, Func<T, int> getVersion, Action<T> validate,
        JsonSerializerOptions? serializerOptions = null)
    {
        Name = new MessageContractVersion(name, version).Name;
        Version = version;
        _version = getVersion ?? throw new ArgumentNullException(nameof(getVersion));
        _validate = validate ?? throw new ArgumentNullException(nameof(validate));
        _json = serializerOptions is null ? new(JsonSerializerDefaults.Web) : new(serializerOptions);
        _json.MakeReadOnly(populateMissingResolver: true);
    }

    public T Decode(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length > 1024 * 1024) throw new IngressMessageRejectedException("contract-payload-too-large");
        T? model;
        try { model = JsonSerializer.Deserialize<T>(payload.Span, _json); }
        catch (JsonException) { throw new IngressMessageRejectedException("invalid-json"); }
        if (model is null) throw new IngressMessageRejectedException("missing-contract-model");
        Validate(model);
        return model;
    }

    public byte[] Encode(T message)
    {
        ArgumentNullException.ThrowIfNull(message);
        Validate(message);
        var bytes = JsonSerializer.SerializeToUtf8Bytes(message, _json);
        if (bytes.Length > 1024 * 1024) throw new IngressMessageRejectedException("contract-payload-too-large");
        return bytes;
    }

    public WorkSubmission Map(WorkScope scope, IdentityPolicy policy, ReadOnlyMemory<byte> payload,
        Func<T, (string BusinessKey, Guid MessageId)> identity)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(identity);
        if (scope.EventType != Name) throw new ArgumentException("Scope.EventType must match the JSON contract name.", nameof(scope));
        if (payload.Length > 1024 * 1024) throw new IngressMessageRejectedException("contract-payload-too-large");
        var snapshot = payload.ToArray();
        var mapped = identity(Decode(snapshot));
        return new(new(scope, policy, mapped.BusinessKey), mapped.MessageId, snapshot, ContentFingerprint.FromBytes(snapshot));
    }

    private void Validate(T message)
    {
        if (_version(message) != Version) throw new IngressMessageRejectedException("unsupported-contract-version");
        _validate(message);
    }
}
