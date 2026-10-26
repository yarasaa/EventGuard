CREATE TABLE __SCHEMA__.job_items(
 id text PRIMARY KEY, work_id uuid NOT NULL REFERENCES __SCHEMA__.job_operations(id),
 item_key text NOT NULL, fingerprint text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE INDEX ix_job_items_work ON __SCHEMA__.job_items(work_id);
UPDATE __SCHEMA__.job_schema_version SET version=2 WHERE singleton;
