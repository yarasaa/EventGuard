CREATE TABLE __SCHEMA__.snapshot_schema_version(singleton boolean PRIMARY KEY CHECK(singleton),version integer NOT NULL);
INSERT INTO __SCHEMA__.snapshot_schema_version VALUES(true,1);
CREATE TABLE __SCHEMA__.snapshots(
 scope_id text NOT NULL,id uuid NOT NULL,definition_id text NOT NULL,definition_fingerprint text NOT NULL,
 request_id uuid NOT NULL,business_key text NOT NULL,actor text NOT NULL,parameters bytea NOT NULL,fingerprint text NOT NULL,
 state integer NOT NULL DEFAULT 0 CHECK(state BETWEEN 0 AND 4),generation bigint NOT NULL DEFAULT 0,page_no bigint NOT NULL DEFAULT 0,
 item_count bigint NOT NULL DEFAULT 0,batched_items bigint NOT NULL DEFAULT 0,batch_count bigint NOT NULL DEFAULT 0,
 source_view text,source_cursor bytea NOT NULL DEFAULT ''::bytea,paused boolean NOT NULL DEFAULT false,
 owner uuid,lease_until timestamptz,version bigint NOT NULL DEFAULT 0,updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(scope_id,id));
CREATE INDEX ix_snapshot_ready ON __SCHEMA__.snapshots(scope_id,definition_id,state,updated_at,id);
CREATE TABLE __SCHEMA__.snapshot_requests(scope_id text NOT NULL,id uuid NOT NULL,snapshot_id uuid NOT NULL,fingerprint text NOT NULL,
 PRIMARY KEY(scope_id,id),FOREIGN KEY(scope_id,snapshot_id) REFERENCES __SCHEMA__.snapshots(scope_id,id));
CREATE TABLE __SCHEMA__.snapshot_pages(scope_id text NOT NULL,snapshot_id uuid NOT NULL,generation bigint NOT NULL,page_no bigint NOT NULL,fingerprint text NOT NULL,
 PRIMARY KEY(scope_id,snapshot_id,generation,page_no),FOREIGN KEY(scope_id,snapshot_id) REFERENCES __SCHEMA__.snapshots(scope_id,id));
CREATE TABLE __SCHEMA__.snapshot_items(scope_id text NOT NULL,snapshot_id uuid NOT NULL,generation bigint NOT NULL,ordinal bigint NOT NULL,
 key_hash text NOT NULL,item_key text NOT NULL,fingerprint text NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(scope_id,snapshot_id,generation,key_hash),UNIQUE(scope_id,snapshot_id,generation,ordinal),
 FOREIGN KEY(scope_id,snapshot_id) REFERENCES __SCHEMA__.snapshots(scope_id,id));
CREATE TABLE __SCHEMA__.snapshot_batches(scope_id text NOT NULL,id uuid NOT NULL,snapshot_id uuid NOT NULL,sequence bigint NOT NULL,
 delivery integer NOT NULL,first_ordinal bigint NOT NULL,last_ordinal bigint NOT NULL,payload bytea NOT NULL,fingerprint text NOT NULL,
 PRIMARY KEY(scope_id,id),UNIQUE(scope_id,snapshot_id,sequence),FOREIGN KEY(scope_id,snapshot_id) REFERENCES __SCHEMA__.snapshots(scope_id,id));
CREATE TABLE __SCHEMA__.snapshot_controls(scope_id text NOT NULL,snapshot_id uuid NOT NULL,version bigint NOT NULL,actor text NOT NULL,reason text NOT NULL,
 paused boolean NOT NULL,cancelled boolean NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(scope_id,snapshot_id,version));

CREATE INDEX ix_snapshot_state_id ON __SCHEMA__.snapshots(scope_id,state,id);
