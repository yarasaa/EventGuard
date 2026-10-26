CREATE TABLE __SCHEMA__.external_notices(
 scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,id uniqueidentifier NOT NULL,action_id uniqueidentifier NOT NULL,binding_name nvarchar(1024) COLLATE Latin1_General_100_BIN2 NOT NULL,binding_version nvarchar(1024) COLLATE Latin1_General_100_BIN2 NOT NULL,
 provider_revision bigint,result_kind int NOT NULL,reason nvarchar(1024) NOT NULL,provider_reference nvarchar(1024),result_data varbinary(max) NOT NULL,
 fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,status int NOT NULL DEFAULT 0,status_reason nvarchar(1024) NOT NULL DEFAULT 'received',action_version bigint NOT NULL DEFAULT 0,
 next_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'),received_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'),
 PRIMARY KEY(scope_id,id),FOREIGN KEY(scope_id,action_id) REFERENCES __SCHEMA__.external_actions(scope_id,id));
CREATE INDEX ix_external_notices_due ON __SCHEMA__.external_notices(scope_id,next_at,id) INCLUDE(binding_name,binding_version) WHERE status=0;
CREATE TABLE __SCHEMA__.external_report_positions(scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,action_id uniqueidentifier NOT NULL,revision bigint NOT NULL,fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 PRIMARY KEY(scope_id,action_id),FOREIGN KEY(scope_id,action_id) REFERENCES __SCHEMA__.external_actions(scope_id,id));
UPDATE __SCHEMA__.external_schema_version SET version=4 WHERE singleton=1;

CREATE INDEX ix_notice_status_id ON __SCHEMA__.external_notices(scope_id,status,id);
