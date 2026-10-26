namespace EventGuard.Operations.SqlServer;

/// <summary>Optional outbox read indexes. Run with a migration identity before enabling outbox scopes.</summary>
public static class OutboxReadIndexes
{
    public static string CreateScript(string schema = "eventguard")
    {
        var name = OperationsIndexes.Quote(schema);
        return $"""
            SET XACT_ABORT ON;
            BEGIN TRANSACTION;
            DECLARE @outboxReadIndexLockResult int;
            EXEC @outboxReadIndexLockResult=sys.sp_getapplock @Resource='EventGuard-outbox-read-indexes-{schema}',@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=10000;
            IF @outboxReadIndexLockResult<0 THROW 51005,'Outbox read index migration lock unavailable.',1;
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('{name}.outbox_messages') AND name='ix_ops_outbox_created')
                CREATE INDEX ix_ops_outbox_created ON {name}.outbox_messages(scope_id,created_at DESC,id DESC);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('{name}.outbox_messages') AND name='ix_ops_outbox_state_created')
                CREATE INDEX ix_ops_outbox_state_created ON {name}.outbox_messages(scope_id,state,created_at DESC,id DESC);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('{name}.outbox_replay_requests') AND name='ix_ops_outbox_replays')
                CREATE INDEX ix_ops_outbox_replays ON {name}.outbox_replay_requests(scope_id,intent_id,created_at DESC,id DESC);
            COMMIT;
            """;
    }
}
