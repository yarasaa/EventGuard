ALTER TABLE __SCHEMA__.external_limits ADD retired_before datetimeoffset(7) NOT NULL DEFAULT '1970-01-01T00:00:00+00:00';
ALTER TABLE __SCHEMA__.external_permits ADD request_time datetimeoffset(7);
EXEC(N'CREATE INDEX ix_external_permits_retention ON __SCHEMA__.external_permits(limit_id,request_time,id) INCLUDE(expires_at) WHERE request_time IS NOT NULL');
UPDATE __SCHEMA__.external_schema_version SET version=7 WHERE singleton=1;
