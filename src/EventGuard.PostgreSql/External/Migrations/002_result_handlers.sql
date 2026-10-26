ALTER TABLE __SCHEMA__.external_actions ADD COLUMN result_handler text, ADD COLUMN result_handler_version text;
ALTER TABLE __SCHEMA__.external_actions ADD CHECK((result_handler IS NULL) = (result_handler_version IS NULL));
CREATE TABLE __SCHEMA__.external_result_updates(
 scope_id text NOT NULL, action_id uuid NOT NULL, version bigint NOT NULL CHECK(version>0),
 work_id uuid NOT NULL UNIQUE REFERENCES __SCHEMA__.works(id), handler_scope_id text NOT NULL,
 state integer NOT NULL CHECK(state BETWEEN 0 AND 4), observation integer NOT NULL CHECK(observation BETWEEN 0 AND 4),
 phase integer NOT NULL CHECK(phase BETWEEN 0 AND 2), provider_reference text, reason text, state_data bytea NOT NULL,
 response_kind integer, response_reason text, response_reference text, response_data bytea,
 PRIMARY KEY(scope_id,action_id,version), FOREIGN KEY(scope_id,action_id) REFERENCES __SCHEMA__.external_actions(scope_id,id));
CREATE TABLE __SCHEMA__.external_result_positions(
 scope_id text NOT NULL, action_id uuid NOT NULL, handler_scope_id text NOT NULL, version bigint NOT NULL CHECK(version>0),
 PRIMARY KEY(scope_id,action_id,handler_scope_id), FOREIGN KEY(scope_id,action_id) REFERENCES __SCHEMA__.external_actions(scope_id,id));
UPDATE __SCHEMA__.external_schema_version SET version=2 WHERE singleton;
