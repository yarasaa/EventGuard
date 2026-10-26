-- Required by the expiry branch's native FORCESEEK. Do not remove independently.
CREATE INDEX ix_claim_expiry ON __SCHEMA__.works(scope_id,expires_at,id) WHERE state IN (0,2);
UPDATE __SCHEMA__.schema_version SET version=3 WHERE singleton=1 AND version=2;
