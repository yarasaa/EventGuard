-- Eligible states in claim SQL explicitly imply this predicate. Keep the specialized
-- ready/recovery indexes: sparse due work can still benefit from other access paths.
CREATE INDEX ix_claim_active ON __SCHEMA__.works(scope_id,next_attempt_at,id) WHERE state IN (0,1,2);
UPDATE __SCHEMA__.schema_version SET version=4 WHERE singleton AND version=3;
