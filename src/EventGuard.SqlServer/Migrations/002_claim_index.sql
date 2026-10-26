-- Keep the native row-lock hints and the ready/recovery indexes. This index
-- provides an ordered active-work access path; it is not an ownership check.
CREATE INDEX ix_claim_active ON __SCHEMA__.works(scope_id,next_attempt_at,id) WHERE state IN (0,1,2);
UPDATE __SCHEMA__.schema_version SET version=2 WHERE singleton=1 AND version=1;
