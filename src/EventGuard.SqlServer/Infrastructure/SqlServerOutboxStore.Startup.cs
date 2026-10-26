namespace EventGuard.SqlServer;
public sealed partial class SqlServerOutboxStore : IStoreStartupCheck
{
    public Task<StoreStartupReport> ValidateStartupAsync(StoreStartupRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Role != StoreRole.Outbox) throw new NotSupportedException("This store instance does not implement the requested runtime role.");
        return SqlServerStartupCheck.ValidateAsync(_dataSource, _schema, request, cancellationToken);
    }
}
