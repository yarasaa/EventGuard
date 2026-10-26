ALTER TABLE __SCHEMA__.external_actions ADD result_handler nvarchar(1024), result_handler_version nvarchar(1024);
EXEC(N'ALTER TABLE __SCHEMA__.external_actions ADD CHECK((result_handler IS NULL AND result_handler_version IS NULL) OR (result_handler IS NOT NULL AND result_handler_version IS NOT NULL))');
CREATE TABLE __SCHEMA__.external_result_updates(
 scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, action_id uniqueidentifier NOT NULL, version bigint NOT NULL CHECK(version>0),
 work_id uniqueidentifier NOT NULL UNIQUE REFERENCES __SCHEMA__.works(id), handler_scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
 state int NOT NULL CHECK(state BETWEEN 0 AND 4), observation int NOT NULL CHECK(observation BETWEEN 0 AND 4),
 phase int NOT NULL CHECK(phase BETWEEN 0 AND 2), provider_reference nvarchar(1024), reason nvarchar(128), state_data varbinary(max) NOT NULL,
 response_kind int, response_reason nvarchar(128), response_reference nvarchar(1024), response_data varbinary(max),
 PRIMARY KEY(scope_id,action_id,version), FOREIGN KEY(scope_id,action_id) REFERENCES __SCHEMA__.external_actions(scope_id,id));
CREATE TABLE __SCHEMA__.external_result_positions(
 scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, action_id uniqueidentifier NOT NULL,
 handler_scope_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, version bigint NOT NULL CHECK(version>0),
 PRIMARY KEY(scope_id,action_id,handler_scope_id), FOREIGN KEY(scope_id,action_id) REFERENCES __SCHEMA__.external_actions(scope_id,id));
UPDATE __SCHEMA__.external_schema_version SET version=2 WHERE singleton=1;
