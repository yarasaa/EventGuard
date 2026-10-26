using Npgsql;

namespace EventGuard.PostgreSql;

public sealed partial class PostgreSqlWorkStore
{
    private static NpgsqlCommand StoreCommand(NpgsqlConnection c, NpgsqlTransaction? t, string sql, params (string Name, object Value)[] parameters)
    {
        var command = new NpgsqlCommand(sql, c, t);
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value);
        return command;
    }
}
