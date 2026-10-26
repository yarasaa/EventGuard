CREATE TABLE __SCHEMA__.job_schema_version(singleton bit PRIMARY KEY CHECK(singleton=1), version int NOT NULL);
INSERT INTO __SCHEMA__.job_schema_version VALUES(1,1);
CREATE TABLE __SCHEMA__.job_definitions(
 id nvarchar(64) COLLATE Latin1_General_100_BIN2 PRIMARY KEY, scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 tenant nvarchar(1024) NOT NULL, application nvarchar(1024) NOT NULL,
 name nvarchar(1024) NOT NULL, version int NOT NULL CHECK(version>0), fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 contract_name nvarchar(1024) NOT NULL, contract_version int NOT NULL, handler_version nvarchar(1024) NOT NULL,
 definition_plan varbinary(max) NOT NULL, capabilities nvarchar(max) NOT NULL,
 work_scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE);
CREATE TABLE __SCHEMA__.job_operations(
 id uniqueidentifier PRIMARY KEY REFERENCES __SCHEMA__.works(id), scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 definition_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL REFERENCES __SCHEMA__.job_definitions(id),
 business_identity nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 business_key nvarchar(1024) NOT NULL, fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'), UNIQUE(scope_id,business_identity));
CREATE TABLE __SCHEMA__.job_requests(
 scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, id uniqueidentifier NOT NULL,
 fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, actor nvarchar(1024) NOT NULL,
 operation_id uniqueidentifier NOT NULL REFERENCES __SCHEMA__.job_operations(id), status int NOT NULL CHECK(status IN(0,1,3)),
 created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'), PRIMARY KEY(scope_id,id));
CREATE INDEX ix_job_requests_operation ON __SCHEMA__.job_requests(operation_id);
