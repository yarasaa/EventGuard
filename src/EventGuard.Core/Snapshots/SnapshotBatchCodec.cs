using System.Globalization;
using System.Text.Json;

namespace EventGuard;

public sealed record SnapshotBatchEnvelope(string ScopeId, string DefinitionId, string DefinitionFingerprint,
    Guid SnapshotId, long Sequence, Guid BatchId, string BusinessKey, string Actor, byte[] Payload, PublishRoute? Route);

public static class SnapshotBatchCodec
{
    public static SnapshotBatchEnvelope Create(SnapshotProgress progress, IReadOnlyList<SnapshotBatchItem> items)
    {
        ArgumentNullException.ThrowIfNull(progress); ArgumentNullException.ThrowIfNull(items);
        var definition = progress.Request.Definition;
        if (items.Count is < 1 || items.Count > definition.BatchSize) throw new ArgumentException("Invalid batch size.", nameof(items));
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject(); writer.WriteNumber("contractVersion", definition.Target.ContractVersion);
            writer.WriteStartArray("items");
            foreach (var item in items)
            {
                writer.WriteStartObject(); writer.WriteString("key", item.Item.Key);
                writer.WritePropertyName("value"); writer.WriteRawValue(item.Item.CopyPayload()); writer.WriteEndObject();
            }
            writer.WriteEndArray(); writer.WriteEndObject();
        }
        var sequence = checked(progress.BatchCount + 1);
        var key = "snapshot:" + progress.Request.Id.ToString("D") + ":" + sequence.ToString(CultureInfo.InvariantCulture);
        var id = new Guid(Convert.FromHexString(StableHash.Of("snapshot-batch/v1", definition.Target.Scope.Id, key)).AsSpan(0, 16));
        var envelope = new SnapshotBatchEnvelope(definition.Target.Scope.Id, definition.Target.Id, definition.Target.Fingerprint,
            progress.Request.Id, sequence, id, key, progress.Request.Actor, buffer.ToArray(), definition.Route);
        if (envelope.Payload.Length > 1024 * 1024 || definition.Delivery != SnapshotDelivery.LocalJob && Encode(envelope).Length > 1024 * 1024)
            throw new ArgumentException("Encoded batch exceeds 1 MiB.", nameof(items));
        return envelope;
    }
    public static byte[] Encode(SnapshotBatchEnvelope envelope) => JsonSerializer.SerializeToUtf8Bytes(envelope);
}
