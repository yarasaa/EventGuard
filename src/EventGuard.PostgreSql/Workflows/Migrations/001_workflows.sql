CREATE TABLE __SCHEMA__.workflow_schema_version(singleton boolean PRIMARY KEY CHECK(singleton),version integer NOT NULL);
INSERT INTO __SCHEMA__.workflow_schema_version VALUES(true,1);
CREATE TABLE __SCHEMA__.workflows(scope_id text NOT NULL,id uuid NOT NULL,tenant text NOT NULL,definition_id text NOT NULL,definition_fingerprint text NOT NULL,
 request_id uuid NOT NULL,business_key text NOT NULL,actor text NOT NULL,expires_at timestamptz,fingerprint text NOT NULL,state integer NOT NULL DEFAULT 0,
 version bigint NOT NULL DEFAULT 0,updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(scope_id,id));
CREATE INDEX ix_workflow_ready ON __SCHEMA__.workflows(scope_id,definition_id,state,updated_at,id);
CREATE TABLE __SCHEMA__.workflow_requests(scope_id text NOT NULL,id uuid NOT NULL,workflow_id uuid NOT NULL,fingerprint text NOT NULL,PRIMARY KEY(scope_id,id),
 FOREIGN KEY(scope_id,workflow_id) REFERENCES __SCHEMA__.workflows(scope_id,id));
CREATE TABLE __SCHEMA__.workflow_steps(scope_id text NOT NULL,workflow_id uuid NOT NULL,name text NOT NULL,kind integer NOT NULL,parameters bytea NOT NULL,
 state integer NOT NULL DEFAULT 0,job_id uuid REFERENCES __SCHEMA__.job_operations(id),due_at timestamptz,value text,reason text NOT NULL DEFAULT 'waiting-for-dependencies',
 PRIMARY KEY(scope_id,workflow_id,name),FOREIGN KEY(scope_id,workflow_id) REFERENCES __SCHEMA__.workflows(scope_id,id));
CREATE TABLE __SCHEMA__.workflow_signals(scope_id text NOT NULL,workflow_id uuid NOT NULL,step_name text NOT NULL,value text NOT NULL,
 PRIMARY KEY(scope_id,workflow_id,step_name),FOREIGN KEY(scope_id,workflow_id) REFERENCES __SCHEMA__.workflows(scope_id,id));
CREATE TABLE __SCHEMA__.workflow_signal_requests(scope_id text NOT NULL,id uuid NOT NULL,workflow_id uuid NOT NULL,step_name text NOT NULL,fingerprint text NOT NULL,actor text NOT NULL,
 PRIMARY KEY(scope_id,id),FOREIGN KEY(scope_id,workflow_id) REFERENCES __SCHEMA__.workflows(scope_id,id));
CREATE TABLE __SCHEMA__.workflow_controls(scope_id text NOT NULL,workflow_id uuid NOT NULL,version bigint NOT NULL,actor text NOT NULL,reason text NOT NULL,
 paused boolean NOT NULL,cancelled boolean NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(scope_id,workflow_id,version));

CREATE INDEX ix_workflow_state_id ON __SCHEMA__.workflows(scope_id,state,id);
