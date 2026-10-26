namespace EventGuard.PostgreSql;
public sealed partial class PostgreSqlWorkStore : IStoreStartupCheck
{
    public Task<StoreStartupReport> ValidateStartupAsync(StoreStartupRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Role != StoreRole.Consumer) throw new NotSupportedException("This store instance does not implement the requested runtime role.");
        return PostgreSqlStartupCheck.ValidateAsync(_dataSource, _schema, request, cancellationToken);
    }
}
