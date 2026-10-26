CREATE TABLE __SCHEMA__.external_schema_version(singleton bit PRIMARY KEY CHECK(singleton=1), version int NOT NULL);
INSERT INTO __SCHEMA__.external_schema_version VALUES(1,1);
CREATE TABLE __SCHEMA__.external_actions(
 scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,id uniqueidentifier NOT NULL,
 tenant nvarchar(1024) NOT NULL,producer nvarchar(1024) NOT NULL,
 binding_name nvarchar(1024) COLLATE Latin1_General_100_BIN2 NOT NULL,binding_version nvarchar(1024) COLLATE Latin1_General_100_BIN2 NOT NULL,payload varbinary(max) NOT NULL,observation_timeout bigint,
 fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 origin_work_id uniqueidentifier REFERENCES __SCHEMA__.works(id),origin_request_id uniqueidentifier,
 state int NOT NULL DEFAULT 0 CHECK(state BETWEEN 0 AND 4),observation int NOT NULL DEFAULT 0 CHECK(observation BETWEEN 0 AND 4),
 version bigint NOT NULL DEFAULT 0,submission_attempts int NOT NULL DEFAULT 0,
 next_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'),observation_deadline datetimeoffset(7),
 provider_reference nvarchar(1024),last_reason nvarchar(128),result_data varbinary(max) NOT NULL DEFAULT 0x,
 lease_token uniqueidentifier,lease_until datetimeoffset(7),lease_phase int,
 PRIMARY KEY(scope_id,id),FOREIGN KEY(scope_id,origin_request_id) REFERENCES __SCHEMA__.producer_requests(scope_id,id));
CREATE INDEX ix_external_due ON __SCHEMA__.external_actions(scope_id,next_at,id) INCLUDE(state,observation,lease_until);
CREATE TABLE __SCHEMA__.external_attempts(
 scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,id uniqueidentifier NOT NULL,action_id uniqueidentifier NOT NULL,phase int NOT NULL,
 started_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'),finished_at datetimeoffset(7),
 result_fingerprint nvarchar(64),applied bit NOT NULL DEFAULT 0,result_kind int,reason nvarchar(128),provider_reference nvarchar(1024),result_data varbinary(max),
 PRIMARY KEY(scope_id,id),FOREIGN KEY(scope_id,action_id) REFERENCES __SCHEMA__.external_actions(scope_id,id));
CREATE INDEX ix_external_attempts_action ON __SCHEMA__.external_attempts(scope_id,action_id);
