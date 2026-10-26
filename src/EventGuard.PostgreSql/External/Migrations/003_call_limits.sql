CREATE TABLE __SCHEMA__.external_limits(
 id text PRIMARY KEY,fingerprint text NOT NULL,window_started_at timestamptz NOT NULL,used integer NOT NULL,
 failures integer NOT NULL,open_until timestamptz,probe uuid);
CREATE TABLE __SCHEMA__.external_permits(
 limit_id text NOT NULL REFERENCES __SCHEMA__.external_limits(id),id uuid NOT NULL,window_started_at timestamptz NOT NULL,
 expires_at timestamptz NOT NULL,outcome integer,PRIMARY KEY(limit_id,id));
CREATE INDEX ix_external_permits_active ON __SCHEMA__.external_permits(limit_id,expires_at) WHERE outcome IS NULL;
UPDATE __SCHEMA__.external_schema_version SET version=3 WHERE singleton;

CREATE INDEX ix_external_state_id ON __SCHEMA__.external_actions(scope_id,state,id);
