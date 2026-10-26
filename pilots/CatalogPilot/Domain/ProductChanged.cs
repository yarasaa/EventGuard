namespace CatalogPilot.Domain;

public sealed record ProductChanged(Guid EventId, int ContractVersion, string ProductId,
    long Revision, string Name, string Category);
