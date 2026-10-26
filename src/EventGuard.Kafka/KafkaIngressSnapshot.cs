using System.Globalization;
using Confluent.Kafka;

namespace EventGuard.Kafka;

internal static class KafkaIngressSnapshot
{
    // BinaryWriter codec: nullable key (int length/-1, bytes), timestamp type/int64,
    // header count, then each UTF-8 name and nullable byte[] value, preserving order/duplicates.
    public static IngressSnapshot Capture(ConsumeResult<byte[], byte[]> delivery, IngressQuarantineOptions options)
    {
        var message = delivery.Message;
        var headers = message.Headers ?? new Headers();
        if (headers.Count > 4096) throw new InvalidOperationException("Kafka quarantine header count exceeds its bounded profile.");
        long size = 32L + (message.Key?.Length ?? 0);
        foreach (var header in headers)
        {
            size += 12L + KafkaEnvelope.Utf8.GetByteCount(header.Key) + (header.GetValueBytes()?.Length ?? 0);
            if (size > 64 * 1024) throw new InvalidOperationException("Kafka quarantine metadata exceeds 64 KiB.");
        }
        if (size > 64 * 1024) throw new InvalidOperationException("Kafka quarantine metadata exceeds 64 KiB.");
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, KafkaEnvelope.Utf8, leaveOpen: true))
        {
            Bytes(writer, message.Key);
            writer.Write((int)message.Timestamp.Type); writer.Write(message.Timestamp.UnixTimestampMs);
            writer.Write(headers.Count);
            foreach (var header in headers) { writer.Write(header.Key); Bytes(writer, header.GetValueBytes()); }
        }
        var position = delivery.Topic + ":" + delivery.Partition.Value.ToString(CultureInfo.InvariantCulture)
            + ":" + delivery.Offset.Value.ToString(CultureInfo.InvariantCulture);
        return new(options.SourceBinding, position, "kafka-raw/v1", message.Value ?? [], stream.ToArray(), message.Value is null);
    }
    private static void Bytes(BinaryWriter writer, byte[]? bytes)
    {
        writer.Write(bytes?.Length ?? -1);
        if (bytes is not null) writer.Write(bytes);
    }
}
