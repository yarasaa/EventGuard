using Npgsql;

namespace EventGuard.PostgreSql;

// The store owns commit/rollback. Application commands must enlist in this exact transaction.
public sealed class PostgreSqlSession : IJobItemSession, IExternalActionSession, IExternalResultSession, IWorkflowSignalSession
{
    public NpgsqlConnection Connection { get; }
    public NpgsqlTransaction Transaction { get; }
    private readonly IOutboxWriter _outbox;
    private readonly bool _sqlOnly;
    public IOutboxWriter Outbox => RequireWorkContext(_outbox);
    private readonly string _quotedSchema;
    private readonly Guid? _originWorkId;
    private readonly ProducerRequest? _producerRequest;
    private IWorkflowSignalWriter? _workflowSignals;
    public IWorkflowSignalWriter WorkflowSignals => RequireWorkContext(_workflowSignals ??= new PostgreSqlWorkflowSignalWriter(Connection, Transaction, _quotedSchema, _originWorkId, _producerRequest));
    private IExternalResultWriter? _externalResults;
    public IExternalResultWriter ExternalResults => RequireWorkContext(_externalResults ??= new PostgreSqlExternalResultWriter(Connection, Transaction, _quotedSchema, _originWorkId));
    private IJobItemWriter? _jobItems;
    private IExternalActionWriter? _externalActions;
    public IJobItemWriter JobItems => RequireWorkContext(_jobItems ??= new PostgreSqlJobItemWriter(Connection, Transaction, _quotedSchema, _originWorkId));
    public IExternalActionWriter ExternalActions => RequireWorkContext(_externalActions ??= new PostgreSqlExternalActionWriter(Connection, Transaction, _quotedSchema, _originWorkId, _producerRequest));

    internal PostgreSqlSession(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string quotedSchema, Guid? originWorkId = null, ProducerRequest? producerRequest = null, bool sqlOnly = false)
    {
        _sqlOnly = sqlOnly;
        _quotedSchema = quotedSchema; _originWorkId = originWorkId; _producerRequest = producerRequest;
        Connection = connection;
        Transaction = transaction;
        _outbox = new PostgreSqlOutboxWriter(connection, transaction, quotedSchema, originWorkId, producerRequest);
    }

    private T RequireWorkContext<T>(T value) => !_sqlOnly ? value : throw new InvalidOperationException(
        "A result batch exposes application SQL only; use a per-result handler for work-scoped intents or signals.");

    public NpgsqlCommand CreateCommand(string sql)
    {
        _ = Transaction.IsolationLevel;
        return new(sql, Connection, Transaction);
    }
}
