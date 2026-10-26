using System.Text.Json;

namespace EventGuard.Kafka.Sample.Contracts;

// Application models. EventGuard does not impose a broker-specific event type or wire format.
public sealed record FileUpdated(Guid EventId, int ContractVersion, string FileId, long Revision, string Path);
public sealed record FileUpdatedV2(Guid EventId, int ContractVersion, string FileId, long Revision, string Location);

public static class FileUpdatedJson
{
    public static VersionedMessageContract<FileUpdated> Contract { get; } = new("file.updated", 2, Identify,
        new Dictionary<int, Func<ReadOnlyMemory<byte>, FileUpdated>>
        {
            [1] = bytes => Read<FileUpdated>(bytes) with { ContractVersion = 2 },
            [2] = bytes => { var current = Read<FileUpdatedV2>(bytes); return new(current.EventId, 2, current.FileId, current.Revision, current.Location); }
        }, Validate);

    public static FileUpdated Decode(ReadOnlySpan<byte> payload) => Contract.Decode(payload.ToArray());

    private static MessageContractVersion Identify(ReadOnlyMemory<byte> bytes)
    {
        try
        {
            using var document = JsonDocument.Parse(bytes);
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty("ContractVersion", out var field)
                || field.ValueKind != JsonValueKind.Number || !field.TryGetInt32(out var version) || version < 1)
                throw new IngressMessageRejectedException("invalid-contract-version");
            return new("file.updated", version);
        }
        catch (JsonException) { throw new IngressMessageRejectedException("invalid-json"); }
    }

    private static T Read<T>(ReadOnlyMemory<byte> bytes) where T : class
    {
        try { return JsonSerializer.Deserialize<T>(bytes.Span) ?? throw new IngressMessageRejectedException("missing-event-body"); }
        catch (JsonException) { throw new IngressMessageRejectedException("invalid-json"); }
    }

    private static void Validate(FileUpdated value)
    {
        if (value.EventId == Guid.Empty || string.IsNullOrWhiteSpace(value.FileId) || value.FileId.Length > 128
            || value.Revision < 0 || string.IsNullOrWhiteSpace(value.Path) || value.Path.Length > 1024)
            throw new IngressMessageRejectedException("invalid-file-updated");
    }
}
