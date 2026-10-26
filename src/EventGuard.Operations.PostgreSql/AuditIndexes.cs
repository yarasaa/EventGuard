namespace EventGuard.Operations.PostgreSql;
/// <summary>Explicit migration-identity setup. The dashboard never installs these indexes.</summary>
public static class AuditIndexes
{
    public static string CreateScript(string schema = "eventguard")
    {
        var name = OperationsIndexes.Quote(schema);
        return $"""
            CREATE INDEX IF NOT EXISTS ix_audit_replay_requests ON {name}.replay_requests(scope_id,created_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_audit_ingress_release_requests ON {name}.ingress_release_requests(scope_id,created_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_audit_outbox_replay_requests ON {name}.outbox_replay_requests(scope_id,created_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_audit_outbox_repair_requests ON {name}.outbox_repair_requests(scope_id,created_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_audit_retention_requests ON {name}.retention_requests(scope_id,created_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_audit_bulk_replay_requests ON {name}.bulk_replay_requests(scope_id,created_at DESC,id DESC);
            """;
    }
}
