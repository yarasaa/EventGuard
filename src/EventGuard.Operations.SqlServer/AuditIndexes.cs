namespace EventGuard.Operations.SqlServer;
/// <summary>Explicit migration-identity setup. The dashboard never installs these indexes.</summary>
public static class AuditIndexes
{
    public static string CreateScript(string schema = "eventguard")
    {
        var name = OperationsIndexes.Quote(schema);
        return $"""
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'{name}.replay_requests') AND name=N'ix_audit_replay_requests') CREATE INDEX ix_audit_replay_requests ON {name}.replay_requests(scope_id,created_at DESC,id DESC);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'{name}.ingress_release_requests') AND name=N'ix_audit_ingress_release_requests') CREATE INDEX ix_audit_ingress_release_requests ON {name}.ingress_release_requests(scope_id,created_at DESC,id DESC);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'{name}.outbox_replay_requests') AND name=N'ix_audit_outbox_replay_requests') CREATE INDEX ix_audit_outbox_replay_requests ON {name}.outbox_replay_requests(scope_id,created_at DESC,id DESC);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'{name}.outbox_repair_requests') AND name=N'ix_audit_outbox_repair_requests') CREATE INDEX ix_audit_outbox_repair_requests ON {name}.outbox_repair_requests(scope_id,created_at DESC,id DESC);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'{name}.retention_requests') AND name=N'ix_audit_retention_requests') CREATE INDEX ix_audit_retention_requests ON {name}.retention_requests(scope_id,created_at DESC,id DESC);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'{name}.bulk_replay_requests') AND name=N'ix_audit_bulk_replay_requests') CREATE INDEX ix_audit_bulk_replay_requests ON {name}.bulk_replay_requests(scope_id,created_at DESC,id DESC);
            """;
    }
}
