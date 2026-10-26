using System.Text.Json;
using CatalogPilot.Domain;
using EventGuard;

namespace CatalogPilot.Contracts;

public static class ProductContract
{
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    public static readonly VersionedMessageContract<ProductChanged> Codec = new("product.changed", 2, Identify,
        new Dictionary<int, Func<ReadOnlyMemory<byte>, ProductChanged>>
        {
            [1] = bytes => Read<ProductChanged>(bytes) with { ContractVersion = 2 },
            [2] = bytes => { var v = Read<ProductV2>(bytes); return new(v.EventId, 2, v.ProductId, v.Revision, v.DisplayName, v.Category); }
        }, Validate);

    public static void Validate(ProductChanged value)
    {
        if (value.EventId == Guid.Empty || value.Revision < 0
            || string.IsNullOrWhiteSpace(value.ProductId) || value.ProductId.Length > 100
            || string.IsNullOrWhiteSpace(value.Name) || value.Name.Length > 200
            || string.IsNullOrWhiteSpace(value.Category) || value.Category.Length > 80)
            throw new IngressMessageRejectedException("invalid-product");
    }

    private static MessageContractVersion Identify(ReadOnlyMemory<byte> bytes)
    {
        try
        {
            using var json = JsonDocument.Parse(bytes);
            if (json.RootElement.ValueKind != JsonValueKind.Object
                || !json.RootElement.TryGetProperty("contractVersion", out var field)
                || field.ValueKind != JsonValueKind.Number
                || !field.TryGetInt32(out var version) || version < 1)
                throw new IngressMessageRejectedException("invalid-contract-version");
            return new("product.changed", version);
        }
        catch (JsonException) { throw new IngressMessageRejectedException("invalid-json"); }
    }
    private static T Read<T>(ReadOnlyMemory<byte> bytes) where T : class
    {
        try { return JsonSerializer.Deserialize<T>(bytes.Span, Json) ?? throw new IngressMessageRejectedException("invalid-json"); }
        catch (JsonException) { throw new IngressMessageRejectedException("invalid-json"); }
    }
    private sealed record ProductV2(Guid EventId, int ContractVersion, string ProductId, long Revision, string DisplayName, string Category);
}
