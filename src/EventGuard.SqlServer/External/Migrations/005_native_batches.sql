CREATE TABLE __SCHEMA__.external_batch_requests(
 scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, id uniqueidentifier NOT NULL, fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 PRIMARY KEY(scope_id,id));
ALTER TABLE __SCHEMA__.external_attempts ADD batch_request_id uniqueidentifier;
ALTER TABLE __SCHEMA__.external_attempts ADD FOREIGN KEY(scope_id,batch_request_id) REFERENCES __SCHEMA__.external_batch_requests(scope_id,id);
-- Compile the filtered predicate after the new column exists in this migration transaction.
EXEC(N'CREATE INDEX ix_external_batch_members ON __SCHEMA__.external_attempts(scope_id,batch_request_id) WHERE batch_request_id IS NOT NULL');
EXEC(N'CREATE TYPE __SCHEMA__.external_batch_ids AS TABLE(id uniqueidentifier NOT NULL PRIMARY KEY)');
EXEC(N'CREATE TYPE __SCHEMA__.external_batch_changes AS TABLE(
 action_id uniqueidentifier NOT NULL PRIMARY KEY,
 state int,
 observation int,
 version bigint,
 submission_attempts int,
 next_at datetimeoffset(7),
 observation_deadline datetimeoffset(7),
 provider_reference nvarchar(1024),
 last_reason nvarchar(1024),
 result_data varbinary(max),
 lease_token uniqueidentifier,
 lease_until datetimeoffset(7),
 lease_phase int,
 write_state bit,
 attempt_id uniqueidentifier,
 attempt_phase int,
 create_attempt bit,
 finish_attempt bit,
 response_kind int,
 response_reason nvarchar(1024),
 response_reference nvarchar(1024),
 response_data varbinary(max),
 response_fingerprint nvarchar(1024),
 applied bit)');
UPDATE __SCHEMA__.external_schema_version SET version=5 WHERE singleton=1;
EXEC(N'CREATE TYPE __SCHEMA__.external_batch_projections AS TABLE(
 scope_id nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 action_id uniqueidentifier,
 version bigint,
 work_id uniqueidentifier,
 handler_scope_id nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 state int,
 observation int,
 phase int,
 provider_reference nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 reason nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 state_data varbinary(max),
 response_kind int,
 response_reason nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 response_reference nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 response_data varbinary(max),
 identity_id nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 business_key nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 message_id uniqueidentifier,
 fingerprint nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 payload varbinary(max),
 correlation_id nvarchar(1024) COLLATE Latin1_General_100_BIN2)');
EXEC(N'CREATE TYPE __SCHEMA__.external_batch_bindings AS TABLE(
 scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
 tenant nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 consumer nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 source nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 event_type nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 key_version nvarchar(1024) COLLATE Latin1_General_100_BIN2,
 fingerprint_version nvarchar(1024) COLLATE Latin1_General_100_BIN2)');
