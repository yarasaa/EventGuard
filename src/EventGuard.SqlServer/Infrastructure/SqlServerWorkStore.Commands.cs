using Microsoft.Data.SqlClient;

namespace EventGuard.SqlServer;

public sealed partial class SqlServerWorkStore
{
    private static SqlCommand StoreCommand(SqlConnection c, SqlTransaction? t, string sql, params (string Name, object Value)[] parameters)
    {
        var command = new SqlCommand(sql, c, t);
        foreach (var (name, value) in parameters) command.Parameters.AddValue(name, value);
        return command;
    }
}
