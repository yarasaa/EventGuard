namespace EventGuard.Operations.SqlServer;

/// <summary>Execute separately with a migration identity, never with the dashboard read identity.</summary>
public static class OperationsIndexes
{
    public static string CreateScript(string schema = "eventguard")
    {
        var name = Quote(schema);
        return $"""
            SET XACT_ABORT ON;
            BEGIN TRANSACTION;
            DECLARE @result int;
            EXEC @result=sys.sp_getapplock @Resource='EventGuard-operations-indexes-{schema}',@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=10000;
            IF @result<0 THROW 51005,'Operations index migration lock unavailable.',1;
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('{name}.works') AND name='ix_ops_created')
                CREATE INDEX ix_ops_created ON {name}.works(scope_id,created_at DESC,id DESC);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('{name}.works') AND name='ix_ops_state_created')
                CREATE INDEX ix_ops_state_created ON {name}.works(scope_id,state,created_at DESC,id DESC);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('{name}.replay_requests') AND name='ix_ops_replays')
                CREATE INDEX ix_ops_replays ON {name}.replay_requests(scope_id,work_id,created_at DESC,id DESC);
            COMMIT;
            """;
    }

    internal static string Quote(string schema)
    {
        if (string.IsNullOrEmpty(schema) || schema.Length > 63 || !char.IsAsciiLetter(schema[0])
            || schema.Any(c => !char.IsAsciiLetterOrDigit(c) && c != '_')) throw new ArgumentException("Invalid schema name.", nameof(schema));
        return '[' + schema + ']';
    }
}
