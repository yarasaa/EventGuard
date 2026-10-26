ALTER TABLE __SCHEMA__.works ADD payload_purged_at datetimeoffset(7),
    CONSTRAINT works_purged_terminal CHECK(payload_purged_at IS NULL OR state IN (3,4,5));
ALTER TABLE __SCHEMA__.conflicts ADD payload_purged_at datetimeoffset(7);
ALTER TABLE __SCHEMA__.outbox_messages ADD payload_purged_at datetimeoffset(7),
    CONSTRAINT outbox_purged_terminal CHECK(payload_purged_at IS NULL OR state=3);
ALTER TABLE __SCHEMA__.ingress_quarantine ADD released_at datetimeoffset(7), data_purged_at datetimeoffset(7),
    CONSTRAINT ingress_purged_released CHECK(data_purged_at IS NULL OR released_work_id IS NOT NULL);
-- Compile the backfill after ALTER TABLE has introduced the column, inside the same transaction.
EXEC(N'UPDATE __SCHEMA__.ingress_quarantine SET released_at=TODATETIMEOFFSET(SYSUTCDATETIME(),''+00:00'') WHERE released_work_id IS NOT NULL;');
ALTER TABLE __SCHEMA__.outbox_quarantine ADD data_purged_at datetimeoffset(7);
EXEC(N'CREATE INDEX ix_retention_work ON __SCHEMA__.works(scope_id,completed_at,id) WHERE payload_purged_at IS NULL AND state=3;');
EXEC(N'CREATE INDEX ix_retention_expired ON __SCHEMA__.works(scope_id,expires_at,id) WHERE payload_purged_at IS NULL AND state IN (4,5);');
EXEC(N'CREATE INDEX ix_retention_outbox ON __SCHEMA__.outbox_messages(scope_id,published_at,id) WHERE payload_purged_at IS NULL AND state=3;');
CREATE TABLE __SCHEMA__.retention_requests (
    scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, id uniqueidentifier NOT NULL,
    fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, target int NOT NULL,
    actor nvarchar(1024) NOT NULL, reason nvarchar(1024) NOT NULL, purged_ids nvarchar(max) NOT NULL,
    created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'), PRIMARY KEY(scope_id,id)
);
UPDATE __SCHEMA__.schema_version SET version=6 WHERE singleton=1 AND version=5;
