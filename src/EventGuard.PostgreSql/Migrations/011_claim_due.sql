-- One ordered range covers ready work, deadline cleanup and expired leases. Keeping
-- their eligibility predicates behind OR previously allowed a large bitmap + sort
-- below LockRows, spilling the active queue to disk for a one-row claim.
-- LEAST ignores a NULL deadline; a live lease is governed ONLY by lease_until.
CREATE INDEX ix_claim_due ON __SCHEMA__.works
    (scope_id,(CASE WHEN state=1 THEN lease_until ELSE LEAST(next_attempt_at,expires_at) END),id)
    WHERE state IN (0,1,2);
DROP INDEX __SCHEMA__.ix_claim_active;
UPDATE __SCHEMA__.schema_version SET version=11 WHERE singleton AND version=10;
