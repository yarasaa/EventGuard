using EventGuard;
using EventGuard.Runtime;

namespace EventGuardWorker.Contracts;

public sealed record ProductChanged(Guid MessageId, int SchemaVersion, string ProductId, long Revision, string Name)
{
    public static JsonMessageContract<ProductChanged> Contract { get; } = new("product.changed", 1,
        message => message.SchemaVersion, message =>
        {
            if (message.MessageId == Guid.Empty || string.IsNullOrWhiteSpace(message.ProductId)
                || message.ProductId.Length > 100 || message.Revision < 1
                || string.IsNullOrWhiteSpace(message.Name) || message.Name.Length > 200)
                throw new IngressMessageRejectedException("invalid-product");
        });
}
