CREATE TABLE __SCHEMA__.external_schema_version(singleton boolean PRIMARY KEY CHECK(singleton), version integer NOT NULL);
INSERT INTO __SCHEMA__.external_schema_version VALUES(true,1);
CREATE TABLE __SCHEMA__.external_actions(
 scope_id text NOT NULL,id uuid NOT NULL,tenant text NOT NULL,producer text NOT NULL,
 binding_name text NOT NULL,binding_version text NOT NULL,payload bytea NOT NULL,observation_timeout bigint,
 fingerprint text NOT NULL,origin_work_id uuid REFERENCES __SCHEMA__.works(id),origin_request_id uuid,
 state integer NOT NULL DEFAULT 0 CHECK(state BETWEEN 0 AND 4),observation integer NOT NULL DEFAULT 0 CHECK(observation BETWEEN 0 AND 4),
 version bigint NOT NULL DEFAULT 0,submission_attempts integer NOT NULL DEFAULT 0,
 next_at timestamptz NOT NULL DEFAULT clock_timestamp(),observation_deadline timestamptz,
 provider_reference text,last_reason text,result_data bytea NOT NULL DEFAULT ''::bytea,
 lease_token uuid,lease_until timestamptz,lease_phase integer,
 PRIMARY KEY(scope_id,id),FOREIGN KEY(scope_id,origin_request_id) REFERENCES __SCHEMA__.producer_requests(scope_id,id));
CREATE INDEX ix_external_due ON __SCHEMA__.external_actions(scope_id,binding_name,binding_version,next_at,id) WHERE state IN(0,1,4) OR observation=1;
CREATE TABLE __SCHEMA__.external_attempts(
 scope_id text NOT NULL,id uuid NOT NULL,action_id uuid NOT NULL,phase integer NOT NULL,
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(),finished_at timestamptz,
 result_fingerprint text,applied boolean NOT NULL DEFAULT false,result_kind integer,reason text,provider_reference text,result_data bytea,
 PRIMARY KEY(scope_id,id),FOREIGN KEY(scope_id,action_id) REFERENCES __SCHEMA__.external_actions(scope_id,id));
CREATE INDEX ix_external_attempts_action ON __SCHEMA__.external_attempts(scope_id,action_id);
