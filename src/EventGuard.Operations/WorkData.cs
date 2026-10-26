using System.Text.Json;

namespace EventGuard.Operations;

/// <summary>Application-owned allowlist. JSON pointers select scalar values only; never entire objects.</summary>
public sealed record WorkDataField(string Label, string Pointer);

public sealed class WorkDataSelection
{
    public const int MaximumPayloadBytes = 65_536;
    public IReadOnlyList<WorkDataField> Fields { get; }

    public WorkDataSelection(IEnumerable<WorkDataField> fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        var copy = fields.Take(17).ToArray();
        if (copy.Length is < 1 or > 16) throw new ArgumentException("Select 1–16 data fields.", nameof(fields));
        foreach (var field in copy)
        {
            if (field is null || string.IsNullOrWhiteSpace(field.Label) || field.Label.Length > 80
                || string.IsNullOrEmpty(field.Pointer) || field.Pointer.Length > 256 || !field.Pointer.StartsWith('/')
                || field.Pointer.Count(c => c == '/') > 8)
                throw new ArgumentException("Invalid data field.", nameof(fields));
            for (var i = 0; i < field.Pointer.Length; i++)
                if (field.Pointer[i] == '~' && (++i >= field.Pointer.Length || field.Pointer[i] is not ('0' or '1')))
                    throw new ArgumentException("Invalid JSON pointer escape.", nameof(fields));
        }
        Fields = Array.AsReadOnly(copy);
    }
}

public enum WorkDataStatus { Available, InvalidJson, TooLarge, Unsupported, Removed }
public enum WorkDataValueStatus { Present, Missing, NonScalar, Truncated }
public sealed record WorkDataValue(string Label, string? Value, WorkDataValueStatus Status);
public sealed record WorkData(WorkDataStatus Status, IReadOnlyList<WorkDataValue> Fields);

/// <summary>Optional port. The caller must authorize scope AND data access before invoking it.</summary>
public interface IWorkDataReadStore
{
    Task<WorkData?> ReadDataAsync(WorkScope scope, Guid id, WorkDataSelection selection, CancellationToken ct = default);
}

public static class WorkDataProjection
{
    public static WorkData FromJson(ReadOnlyMemory<byte> payload, WorkDataSelection selection)
    {
        ArgumentNullException.ThrowIfNull(selection);
        if (payload.Length > WorkDataSelection.MaximumPayloadBytes) return new(WorkDataStatus.TooLarge, []);
        try
        {
            using var json = JsonDocument.Parse(payload, new() { MaxDepth = 32 });
            return new(WorkDataStatus.Available, selection.Fields.Select(field => Project(json.RootElement, field)).ToArray());
        }
        catch (JsonException) { return new(WorkDataStatus.InvalidJson, []); }
    }

    private static WorkDataValue Project(JsonElement value, WorkDataField field)
    {
        foreach (var segment in field.Pointer.Split('/').Skip(1))
        {
            var key = segment.Replace("~1", "/", StringComparison.Ordinal).Replace("~0", "~", StringComparison.Ordinal);
            if (value.ValueKind != JsonValueKind.Object || !value.TryGetProperty(key, out value))
                return new(field.Label, null, WorkDataValueStatus.Missing);
        }
        if (value.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
            return new(field.Label, null, WorkDataValueStatus.NonScalar);
        var text = value.ValueKind == JsonValueKind.String ? value.GetString()! : value.GetRawText();
        var truncated = text.Length > 256;
        if (truncated) text = text[..(char.IsHighSurrogate(text[255]) ? 255 : 256)] + "…";
        return new(field.Label, text, truncated ? WorkDataValueStatus.Truncated : WorkDataValueStatus.Present);
    }
}
