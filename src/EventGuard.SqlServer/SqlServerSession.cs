using System.Data;
using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

// The store owns commit/rollback. Application commands must enlist in this exact transaction.
public sealed class SqlServerSession : IJobItemSession, IExternalActionSession, IExternalResultSession, IWorkflowSignalSession
{
    public SqlConnection Connection { get; }
    public SqlTransaction Transaction { get; }
    private readonly IOutboxWriter _outbox;
    private readonly bool _sqlOnly;
    public IOutboxWriter Outbox => RequireWorkContext(_outbox);
    private readonly string _quotedSchema;
    private readonly Guid? _originWorkId;
    private readonly ProducerRequest? _producerRequest;
    private IWorkflowSignalWriter? _workflowSignals;
    public IWorkflowSignalWriter WorkflowSignals => RequireWorkContext(_workflowSignals ??= new SqlServerWorkflowSignalWriter(Connection, Transaction, _quotedSchema, _originWorkId, _producerRequest));
    private IExternalResultWriter? _externalResults;
    public IExternalResultWriter ExternalResults => RequireWorkContext(_externalResults ??= new SqlServerExternalResultWriter(Connection, Transaction, _quotedSchema, _originWorkId));
    private IJobItemWriter? _jobItems;
    private IExternalActionWriter? _externalActions;
    public IJobItemWriter JobItems => RequireWorkContext(_jobItems ??= new SqlServerJobItemWriter(Connection, Transaction, _quotedSchema, _originWorkId));
    public IExternalActionWriter ExternalActions => RequireWorkContext(_externalActions ??= new SqlServerExternalActionWriter(Connection, Transaction, _quotedSchema, _originWorkId, _producerRequest));

    internal SqlServerSession(SqlConnection connection, SqlTransaction transaction,
        string quotedSchema, Guid? originWorkId = null, ProducerRequest? producerRequest = null, bool sqlOnly = false)
    {
        _sqlOnly = sqlOnly;
        _quotedSchema = quotedSchema; _originWorkId = originWorkId; _producerRequest = producerRequest;
        Connection = connection;
        Transaction = transaction;
        _outbox = new SqlServerOutboxWriter(connection, transaction, quotedSchema, originWorkId, producerRequest);
    }

    private T RequireWorkContext<T>(T value) => !_sqlOnly ? value : throw new InvalidOperationException(
        "A result batch exposes application SQL only; use a per-result handler for work-scoped intents or signals.");

    public SqlCommand CreateCommand(string sql) => new(sql, Connection, Transaction);
}
