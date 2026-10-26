CREATE TABLE __SCHEMA__.job_items(
 id nvarchar(64) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,
 work_id uniqueidentifier NOT NULL REFERENCES __SCHEMA__.job_operations(id),
 item_key nvarchar(1024) NOT NULL, fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'));
CREATE INDEX ix_job_items_work ON __SCHEMA__.job_items(work_id);
UPDATE __SCHEMA__.job_schema_version SET version=2 WHERE singleton=1;
