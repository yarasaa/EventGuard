using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace EventGuard.RabbitMq;

internal static class RabbitMqIngressSnapshot
{
    // Stable content-addressed source position: redelivery flag/tag are deliberately excluded.
    // Identical invalid deliveries aggregate; this is not a distinct application message identity.
    public static IngressSnapshot Capture(BasicDeliverEventArgs delivery, string queue, IngressQuarantineOptions options)
    {
        IAmqpWriteable properties = new BasicProperties(delivery.BasicProperties);
        var size = properties.GetRequiredBufferSize();
        if (size > 63 * 1024) throw new InvalidOperationException("RabbitMQ quarantine properties exceed the metadata budget.");
        var bytes = new byte[size];
        properties.WriteTo(bytes);
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, new UTF8Encoding(false, true), leaveOpen: true))
        {
            writer.Write(queue); writer.Write(delivery.Exchange); writer.Write(delivery.RoutingKey);
            writer.Write(bytes.Length); writer.Write(bytes);
        }
        var metadata = stream.ToArray();
        var position = StableHash.Of("amqp-content/v1", ContentFingerprint.FromBytes(metadata).Value,
            ContentFingerprint.FromBytes(delivery.Body.Span).Value);
        return new(options.SourceBinding, position, "amqp-0-9-1-properties/v1", delivery.Body.Span, metadata);
    }
}
