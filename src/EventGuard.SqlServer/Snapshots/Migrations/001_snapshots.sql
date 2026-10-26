CREATE TABLE __SCHEMA__.snapshot_schema_version(singleton bit PRIMARY KEY CHECK(singleton=1),version int NOT NULL);
INSERT INTO __SCHEMA__.snapshot_schema_version VALUES(1,1);
CREATE TABLE __SCHEMA__.snapshots(
 scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,id uniqueidentifier NOT NULL,definition_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,definition_fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 request_id uniqueidentifier NOT NULL,business_key nvarchar(1024) NOT NULL,actor nvarchar(1024) NOT NULL,parameters varbinary(max) NOT NULL,fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 state int NOT NULL DEFAULT 0 CHECK(state BETWEEN 0 AND 4),generation bigint NOT NULL DEFAULT 0,page_no bigint NOT NULL DEFAULT 0,
 item_count bigint NOT NULL DEFAULT 0,batched_items bigint NOT NULL DEFAULT 0,batch_count bigint NOT NULL DEFAULT 0,
 source_view nvarchar(1024),source_cursor varbinary(max) NOT NULL DEFAULT 0x,paused bit NOT NULL DEFAULT 0,
 owner uniqueidentifier,lease_until datetimeoffset(7),version bigint NOT NULL DEFAULT 0,updated_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'),
 PRIMARY KEY(scope_id,id));
CREATE INDEX ix_snapshot_ready ON __SCHEMA__.snapshots(scope_id,definition_id,state,updated_at,id);
CREATE TABLE __SCHEMA__.snapshot_requests(scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,id uniqueidentifier NOT NULL,snapshot_id uniqueidentifier NOT NULL,fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 PRIMARY KEY(scope_id,id),FOREIGN KEY(scope_id,snapshot_id) REFERENCES __SCHEMA__.snapshots(scope_id,id));
CREATE TABLE __SCHEMA__.snapshot_pages(scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,snapshot_id uniqueidentifier NOT NULL,generation bigint NOT NULL,page_no bigint NOT NULL,fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 PRIMARY KEY(scope_id,snapshot_id,generation,page_no),FOREIGN KEY(scope_id,snapshot_id) REFERENCES __SCHEMA__.snapshots(scope_id,id));
CREATE TABLE __SCHEMA__.snapshot_items(scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,snapshot_id uniqueidentifier NOT NULL,generation bigint NOT NULL,ordinal bigint NOT NULL,
 key_hash nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,item_key nvarchar(1024) NOT NULL,fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,payload varbinary(max) NOT NULL,
 PRIMARY KEY(scope_id,snapshot_id,generation,key_hash),UNIQUE(scope_id,snapshot_id,generation,ordinal),
 FOREIGN KEY(scope_id,snapshot_id) REFERENCES __SCHEMA__.snapshots(scope_id,id));
CREATE TABLE __SCHEMA__.snapshot_batches(scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,id uniqueidentifier NOT NULL,snapshot_id uniqueidentifier NOT NULL,sequence bigint NOT NULL,
 delivery int NOT NULL,first_ordinal bigint NOT NULL,last_ordinal bigint NOT NULL,payload varbinary(max) NOT NULL,fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 PRIMARY KEY(scope_id,id),UNIQUE(scope_id,snapshot_id,sequence),FOREIGN KEY(scope_id,snapshot_id) REFERENCES __SCHEMA__.snapshots(scope_id,id));
CREATE TABLE __SCHEMA__.snapshot_controls(scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,snapshot_id uniqueidentifier NOT NULL,version bigint NOT NULL,actor nvarchar(1024) NOT NULL,reason nvarchar(1024) NOT NULL,
 paused bit NOT NULL,cancelled bit NOT NULL,recorded_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'),PRIMARY KEY(scope_id,snapshot_id,version));

CREATE INDEX ix_snapshot_state_id ON __SCHEMA__.snapshots(scope_id,state,id);
