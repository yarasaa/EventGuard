CREATE TABLE __SCHEMA__.external_notices(
 scope_id text NOT NULL,id uuid NOT NULL,action_id uuid NOT NULL,binding_name text NOT NULL,binding_version text NOT NULL,
 provider_revision bigint,result_kind integer NOT NULL,reason text NOT NULL,provider_reference text,result_data bytea NOT NULL,
 fingerprint text NOT NULL,status integer NOT NULL DEFAULT 0,status_reason text NOT NULL DEFAULT 'received',action_version bigint NOT NULL DEFAULT 0,
 next_at timestamptz NOT NULL DEFAULT clock_timestamp(),received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(scope_id,id),FOREIGN KEY(scope_id,action_id) REFERENCES __SCHEMA__.external_actions(scope_id,id));
CREATE INDEX ix_external_notices_due ON __SCHEMA__.external_notices(scope_id,binding_name,binding_version,next_at,id) WHERE status=0;
CREATE TABLE __SCHEMA__.external_report_positions(scope_id text NOT NULL,action_id uuid NOT NULL,revision bigint NOT NULL,fingerprint text NOT NULL,
 PRIMARY KEY(scope_id,action_id),FOREIGN KEY(scope_id,action_id) REFERENCES __SCHEMA__.external_actions(scope_id,id));
UPDATE __SCHEMA__.external_schema_version SET version=4 WHERE singleton;

CREATE INDEX ix_notice_status_id ON __SCHEMA__.external_notices(scope_id,status,id);
