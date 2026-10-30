CREATE INDEX ix_external_submit_due ON __SCHEMA__.external_actions(scope_id,binding_name,binding_version,next_at,id) WHERE state IN(0,1);
CREATE INDEX ix_external_reconcile_due ON __SCHEMA__.external_actions(scope_id,binding_name,binding_version,next_at,id) WHERE state IN(1,4);
CREATE INDEX ix_external_observe_due ON __SCHEMA__.external_actions(scope_id,binding_name,binding_version,next_at,id) WHERE state=2 AND observation=1;
DROP INDEX __SCHEMA__.ix_external_due;
UPDATE __SCHEMA__.external_schema_version SET version=6 WHERE singleton;
