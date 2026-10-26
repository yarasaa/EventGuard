CREATE TABLE __SCHEMA__.job_schema_version(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), version integer NOT NULL);
INSERT INTO __SCHEMA__.job_schema_version VALUES(true,1);
CREATE TABLE __SCHEMA__.job_definitions(
 id text PRIMARY KEY, scope_id text NOT NULL, tenant text NOT NULL, application text NOT NULL,
 name text NOT NULL, version integer NOT NULL CHECK(version>0), fingerprint text NOT NULL,
 contract_name text NOT NULL, contract_version integer NOT NULL, handler_version text NOT NULL,
 definition_plan bytea NOT NULL, capabilities text NOT NULL, work_scope_id text NOT NULL UNIQUE);
CREATE TABLE __SCHEMA__.job_operations(
 id uuid PRIMARY KEY REFERENCES __SCHEMA__.works(id), scope_id text NOT NULL,
 definition_id text NOT NULL REFERENCES __SCHEMA__.job_definitions(id), business_identity text NOT NULL,
 business_key text NOT NULL, fingerprint text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), UNIQUE(scope_id,business_identity));
CREATE TABLE __SCHEMA__.job_requests(
 scope_id text NOT NULL, id uuid NOT NULL, fingerprint text NOT NULL, actor text NOT NULL,
 operation_id uuid NOT NULL REFERENCES __SCHEMA__.job_operations(id), status integer NOT NULL CHECK(status IN(0,1,3)),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(scope_id,id));
CREATE INDEX ix_job_requests_operation ON __SCHEMA__.job_requests(operation_id);
