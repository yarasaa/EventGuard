CREATE TABLE __SCHEMA__.workflow_schema_version(singleton bit PRIMARY KEY CHECK(singleton=1),version int NOT NULL);
INSERT INTO __SCHEMA__.workflow_schema_version VALUES(1,1);
CREATE TABLE __SCHEMA__.workflows(scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,id uniqueidentifier NOT NULL,tenant nvarchar(1024) NOT NULL,definition_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,definition_fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 request_id uniqueidentifier NOT NULL,business_key nvarchar(1024) NOT NULL,actor nvarchar(1024) NOT NULL,expires_at datetimeoffset(7),fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,state int NOT NULL DEFAULT 0,
 version bigint NOT NULL DEFAULT 0,updated_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'),PRIMARY KEY(scope_id,id));
CREATE INDEX ix_workflow_ready ON __SCHEMA__.workflows(scope_id,definition_id,state,updated_at,id);
CREATE TABLE __SCHEMA__.workflow_requests(scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,id uniqueidentifier NOT NULL,workflow_id uniqueidentifier NOT NULL,fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,PRIMARY KEY(scope_id,id),
 FOREIGN KEY(scope_id,workflow_id) REFERENCES __SCHEMA__.workflows(scope_id,id));
CREATE TABLE __SCHEMA__.workflow_steps(scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,workflow_id uniqueidentifier NOT NULL,name nvarchar(256) COLLATE Latin1_General_100_BIN2 NOT NULL,kind int NOT NULL,parameters varbinary(max) NOT NULL,
 state int NOT NULL DEFAULT 0,job_id uniqueidentifier REFERENCES __SCHEMA__.job_operations(id),due_at datetimeoffset(7),value nvarchar(1024),reason nvarchar(1024) NOT NULL DEFAULT 'waiting-for-dependencies',
 PRIMARY KEY(scope_id,workflow_id,name),FOREIGN KEY(scope_id,workflow_id) REFERENCES __SCHEMA__.workflows(scope_id,id));
CREATE TABLE __SCHEMA__.workflow_signals(scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,workflow_id uniqueidentifier NOT NULL,step_name nvarchar(256) COLLATE Latin1_General_100_BIN2 NOT NULL,value nvarchar(1024) NOT NULL,
 PRIMARY KEY(scope_id,workflow_id,step_name),FOREIGN KEY(scope_id,workflow_id) REFERENCES __SCHEMA__.workflows(scope_id,id));
CREATE TABLE __SCHEMA__.workflow_signal_requests(scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,id uniqueidentifier NOT NULL,workflow_id uniqueidentifier NOT NULL,step_name nvarchar(256) COLLATE Latin1_General_100_BIN2 NOT NULL,fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,actor nvarchar(1024) NOT NULL,
 PRIMARY KEY(scope_id,id),FOREIGN KEY(scope_id,workflow_id) REFERENCES __SCHEMA__.workflows(scope_id,id));
CREATE TABLE __SCHEMA__.workflow_controls(scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,workflow_id uniqueidentifier NOT NULL,version bigint NOT NULL,actor nvarchar(1024) NOT NULL,reason nvarchar(1024) NOT NULL,
 paused bit NOT NULL,cancelled bit NOT NULL,recorded_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'),PRIMARY KEY(scope_id,workflow_id,version));

CREATE INDEX ix_workflow_state_id ON __SCHEMA__.workflows(scope_id,state,id);
