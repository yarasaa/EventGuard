CREATE TABLE __SCHEMA__.external_batch_requests(
 scope_id text NOT NULL, id uuid NOT NULL, fingerprint text NOT NULL,
 PRIMARY KEY(scope_id,id));
ALTER TABLE __SCHEMA__.external_attempts ADD COLUMN batch_request_id uuid;
ALTER TABLE __SCHEMA__.external_attempts ADD FOREIGN KEY(scope_id,batch_request_id) REFERENCES __SCHEMA__.external_batch_requests(scope_id,id);
CREATE INDEX ix_external_batch_members ON __SCHEMA__.external_attempts(scope_id,batch_request_id) WHERE batch_request_id IS NOT NULL;
UPDATE __SCHEMA__.external_schema_version SET version=5 WHERE singleton;
