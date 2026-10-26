namespace EventGuard.Operations.PostgreSql;

/// <summary>Execute separately with a migration identity, never with the dashboard read identity.</summary>
public static class OperationsIndexes
{
    public static string CreateScript(string schema = "eventguard")
    {
        var name = Quote(schema);
        return $"""
            CREATE INDEX IF NOT EXISTS ix_ops_created ON {name}.works(scope_id,created_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_ops_state_created ON {name}.works(scope_id,state,created_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_ops_replays ON {name}.replay_requests(scope_id,work_id,created_at DESC,id DESC);
            """;
    }

    internal static string Quote(string schema)
    {
        if (string.IsNullOrEmpty(schema) || schema.Length > 63 || !char.IsAsciiLetter(schema[0])
            || schema.Any(c => !char.IsAsciiLetterOrDigit(c) && c != '_')) throw new ArgumentException("Invalid schema name.", nameof(schema));
        return '"' + schema + '"';
    }
}
