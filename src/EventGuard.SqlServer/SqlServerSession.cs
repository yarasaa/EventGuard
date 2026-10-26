using System.Data;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

// The store owns commit/rollback. Application commands must enlist in this exact transaction.
public sealed class SqlServerSession : IJobItemSession, IExternalActionSession, IExternalResultSession, IWorkflowSignalSession
{
    public SqlConnection Connection { get; }
    public SqlTransaction Transaction { get; }
    public IOutboxWriter Outbox { get; }
    private readonly string _quotedSchema;
    private readonly Guid? _originWorkId;
    private readonly ProducerRequest? _producerRequest;
    private IWorkflowSignalWriter? _workflowSignals;
    public IWorkflowSignalWriter WorkflowSignals => _workflowSignals ??= new SqlServerWorkflowSignalWriter(Connection, Transaction, _quotedSchema, _originWorkId, _producerRequest);
    private IExternalResultWriter? _externalResults;
    public IExternalResultWriter ExternalResults => _externalResults ??= new SqlServerExternalResultWriter(Connection, Transaction, _quotedSchema, _originWorkId);
    private IJobItemWriter? _jobItems;
    private IExternalActionWriter? _externalActions;
    public IJobItemWriter JobItems => _jobItems ??= new SqlServerJobItemWriter(Connection, Transaction, _quotedSchema, _originWorkId);
    public IExternalActionWriter ExternalActions => _externalActions ??= new SqlServerExternalActionWriter(Connection, Transaction, _quotedSchema, _originWorkId, _producerRequest);

    internal SqlServerSession(SqlConnection connection, SqlTransaction transaction,
        string quotedSchema, Guid? originWorkId = null, ProducerRequest? producerRequest = null)
    {
        _quotedSchema = quotedSchema; _originWorkId = originWorkId; _producerRequest = producerRequest;
        Connection = connection;
        Transaction = transaction;
        Outbox = new SqlServerOutboxWriter(connection, transaction, quotedSchema, originWorkId, producerRequest);
    }

    public SqlCommand CreateCommand(string sql) => new(sql, Connection, Transaction);
}
