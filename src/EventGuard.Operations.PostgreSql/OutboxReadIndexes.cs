namespace EventGuard.Operations.PostgreSql;

/// <summary>Optional outbox read indexes. Run with a migration identity before enabling outbox scopes.</summary>
public static class OutboxReadIndexes
{
    public static string CreateScript(string schema = "eventguard")
    {
        var name = OperationsIndexes.Quote(schema);
        return $"""
            CREATE INDEX IF NOT EXISTS ix_ops_outbox_created ON {name}.outbox_messages(scope_id,created_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_ops_outbox_state_created ON {name}.outbox_messages(scope_id,state,created_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_ops_outbox_replays ON {name}.outbox_replay_requests(scope_id,intent_id,created_at DESC,id DESC);
            """;
    }
}
