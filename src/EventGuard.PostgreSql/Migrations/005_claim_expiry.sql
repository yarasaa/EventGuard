-- The stable statement cutoff can seek expiry independently of next-attempt ordering.
CREATE INDEX ix_claim_expiry ON __SCHEMA__.works(scope_id,expires_at,id) WHERE state IN (0,2);
UPDATE __SCHEMA__.schema_version SET version=5 WHERE singleton AND version=4;
