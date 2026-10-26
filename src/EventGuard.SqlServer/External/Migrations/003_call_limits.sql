CREATE TABLE __SCHEMA__.external_limits(
 id nvarchar(64) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,window_started_at datetimeoffset(7) NOT NULL,used int NOT NULL,
 failures int NOT NULL,open_until datetimeoffset(7),probe uniqueidentifier);
CREATE TABLE __SCHEMA__.external_permits(
 limit_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL REFERENCES __SCHEMA__.external_limits(id),id uniqueidentifier NOT NULL,window_started_at datetimeoffset(7) NOT NULL,
 expires_at datetimeoffset(7) NOT NULL,outcome int,PRIMARY KEY(limit_id,id));
CREATE INDEX ix_external_permits_active ON __SCHEMA__.external_permits(limit_id,expires_at) WHERE outcome IS NULL;
UPDATE __SCHEMA__.external_schema_version SET version=3 WHERE singleton=1;

CREATE INDEX ix_external_state_id ON __SCHEMA__.external_actions(scope_id,state,id);
