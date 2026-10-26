using System.Text;
using Confluent.Kafka;

namespace EventGuard.Kafka;

internal static class KafkaEnvelope
{
    internal static readonly UTF8Encoding Utf8 = new(false, true);
    public static bool ValidTopic(string topic) => topic.Length is > 0 and <= 249 && topic is not "." and not ".."
        && topic.All(c => char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or '-');
    public static bool ReservedHeader(string key) => key.Equals(KafkaPublisher.MessageIdHeader, StringComparison.OrdinalIgnoreCase)
        || key.Equals(KafkaPublisher.ContentTypeHeader, StringComparison.OrdinalIgnoreCase);

    public static WorkSubmission Map(ConsumeResult<byte[], byte[]> delivery, WorkScope scope, IdentityPolicy policy)
    {
        var values = delivery.Message.Headers?.Where(h => h.Key == KafkaPublisher.MessageIdHeader).ToArray() ?? [];
        if (values.Length != 1 || values[0].GetValueBytes() is not { Length: 36 } bytes
            || bytes.Any(b => b > 127) || !Guid.TryParseExact(Encoding.ASCII.GetString(bytes), "D", out var id) || id == Guid.Empty)
            throw new IngressMessageRejectedException("invalid-message-id");
        if (delivery.Message.Value is not { } body)
            throw new IngressMessageRejectedException("unmapped-tombstone");
        return new(new(scope, policy, id.ToString("D")), id, body, ContentFingerprint.FromBytes(body));
    }
}
