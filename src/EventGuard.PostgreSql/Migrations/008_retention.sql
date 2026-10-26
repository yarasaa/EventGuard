ALTER TABLE __SCHEMA__.works ADD COLUMN payload_purged_at timestamptz,
    ADD CONSTRAINT works_purged_terminal CHECK(payload_purged_at IS NULL OR state IN (3,4,5));
ALTER TABLE __SCHEMA__.conflicts ADD COLUMN payload_purged_at timestamptz;
ALTER TABLE __SCHEMA__.outbox_messages ADD COLUMN payload_purged_at timestamptz,
    ADD CONSTRAINT outbox_purged_terminal CHECK(payload_purged_at IS NULL OR state=3);
ALTER TABLE __SCHEMA__.ingress_quarantine ADD COLUMN released_at timestamptz, ADD COLUMN data_purged_at timestamptz,
    ADD CONSTRAINT ingress_purged_released CHECK(data_purged_at IS NULL OR released_work_id IS NOT NULL);
UPDATE __SCHEMA__.ingress_quarantine SET released_at=clock_timestamp() WHERE released_work_id IS NOT NULL;
ALTER TABLE __SCHEMA__.outbox_quarantine ADD COLUMN data_purged_at timestamptz;
CREATE INDEX ix_retention_work ON __SCHEMA__.works(scope_id,completed_at,id) WHERE payload_purged_at IS NULL AND state=3;
CREATE INDEX ix_retention_expired ON __SCHEMA__.works(scope_id,expires_at,id) WHERE payload_purged_at IS NULL AND state IN (4,5);
CREATE INDEX ix_retention_outbox ON __SCHEMA__.outbox_messages(scope_id,published_at,id) WHERE payload_purged_at IS NULL AND state=3;
CREATE TABLE __SCHEMA__.retention_requests (
    scope_id text NOT NULL, id uuid NOT NULL, fingerprint text NOT NULL, target integer NOT NULL,
    actor text NOT NULL, reason text NOT NULL, purged_ids jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(scope_id,id)
);
UPDATE __SCHEMA__.schema_version SET version=8 WHERE singleton AND version=7;
