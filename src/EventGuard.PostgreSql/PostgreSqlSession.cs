using Npgsql;

namespace EventGuard.PostgreSql;

// The store owns commit/rollback. Application commands must enlist in this exact transaction.
public sealed class PostgreSqlSession : IJobItemSession, IExternalActionSession, IExternalResultSession, IWorkflowSignalSession
{
    public NpgsqlConnection Connection { get; }
    public NpgsqlTransaction Transaction { get; }
    public IOutboxWriter Outbox { get; }
    private readonly string _quotedSchema;
    private readonly Guid? _originWorkId;
    private readonly ProducerRequest? _producerRequest;
    private IWorkflowSignalWriter? _workflowSignals;
    public IWorkflowSignalWriter WorkflowSignals => _workflowSignals ??= new PostgreSqlWorkflowSignalWriter(Connection, Transaction, _quotedSchema, _originWorkId, _producerRequest);
    private IExternalResultWriter? _externalResults;
    public IExternalResultWriter ExternalResults => _externalResults ??= new PostgreSqlExternalResultWriter(Connection, Transaction, _quotedSchema, _originWorkId);
    private IJobItemWriter? _jobItems;
    private IExternalActionWriter? _externalActions;
    public IJobItemWriter JobItems => _jobItems ??= new PostgreSqlJobItemWriter(Connection, Transaction, _quotedSchema, _originWorkId);
    public IExternalActionWriter ExternalActions => _externalActions ??= new PostgreSqlExternalActionWriter(Connection, Transaction, _quotedSchema, _originWorkId, _producerRequest);

    internal PostgreSqlSession(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string quotedSchema, Guid? originWorkId = null, ProducerRequest? producerRequest = null)
    {
        _quotedSchema = quotedSchema; _originWorkId = originWorkId; _producerRequest = producerRequest;
        Connection = connection;
        Transaction = transaction;
        Outbox = new PostgreSqlOutboxWriter(connection, transaction, quotedSchema, originWorkId, producerRequest);
    }

    public NpgsqlCommand CreateCommand(string sql)
    {
        _ = Transaction.IsolationLevel;
        return new(sql, Connection, Transaction);
    }
}
