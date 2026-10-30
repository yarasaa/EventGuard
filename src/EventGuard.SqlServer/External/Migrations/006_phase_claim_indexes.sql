-- Keep completed history and unrelated phases off the scheduling access path.
ALTER TABLE __SCHEMA__.external_actions ADD
 binding_key AS CONVERT(binary(32),HASHBYTES('SHA2_256',CONVERT(binary(4),DATALENGTH(binding_name))+CONVERT(varbinary(max),binding_name)+CONVERT(varbinary(max),binding_version))) PERSISTED,
 payload_bytes AS CONVERT(bigint,DATALENGTH(payload)) PERSISTED;
EXEC(N'CREATE INDEX ix_external_submit_due ON __SCHEMA__.external_actions(scope_id,binding_key,next_at,id)
 INCLUDE(binding_name,binding_version,state,observation,lease_until,payload_bytes) WHERE state IN(0,1)');
EXEC(N'CREATE INDEX ix_external_reconcile_due ON __SCHEMA__.external_actions(scope_id,binding_key,next_at,id)
 INCLUDE(binding_name,binding_version,state,observation,lease_until,payload_bytes) WHERE state IN(1,4)');
EXEC(N'CREATE INDEX ix_external_observe_due ON __SCHEMA__.external_actions(scope_id,binding_key,next_at,id)
 INCLUDE(binding_name,binding_version,state,observation,lease_until,payload_bytes) WHERE state=2 AND observation=1');
DROP INDEX ix_external_due ON __SCHEMA__.external_actions;
UPDATE __SCHEMA__.external_schema_version SET version=6 WHERE singleton=1;
