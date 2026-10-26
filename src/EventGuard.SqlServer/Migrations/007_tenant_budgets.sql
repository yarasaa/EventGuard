CREATE TABLE __SCHEMA__.tenant_budgets (
    tenant_id nvarchar(64) COLLATE Latin1_General_100_BIN2 PRIMARY KEY, tenant nvarchar(1024) COLLATE Latin1_General_100_BIN2 NOT NULL,
    version bigint NOT NULL CHECK(version>=0), window_seconds int NOT NULL CHECK(window_seconds BETWEEN 1 AND 86400),
    maximum_claims bigint, maximum_retries bigint, maximum_recoveries bigint,
    window_started_at datetimeoffset(7) NOT NULL, claims bigint NOT NULL CHECK(claims>=0),
    retries bigint NOT NULL CHECK(retries>=0), recoveries bigint NOT NULL CHECK(recoveries>=0),
    CHECK(retries<=claims), CHECK(maximum_claims BETWEEN 0 AND 1000000000), CHECK(maximum_retries BETWEEN 0 AND 1000000000), CHECK(maximum_recoveries BETWEEN 0 AND 1000000000)
);
CREATE TABLE __SCHEMA__.tenant_budget_requests (
    tenant_id nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL, id uniqueidentifier NOT NULL,
    fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
    expected_version bigint NOT NULL, requested_window_seconds int NOT NULL, requested_max_claims bigint, requested_max_retries bigint, requested_max_recoveries bigint,
    actor nvarchar(1024) NOT NULL, reason nvarchar(1024) NOT NULL, result_status int NOT NULL, result_version bigint NOT NULL,
    created_at datetimeoffset(7) NOT NULL DEFAULT TODATETIMEOFFSET(SYSUTCDATETIME(),'+00:00'), PRIMARY KEY(tenant_id,id)
);
UPDATE __SCHEMA__.schema_version SET version=7 WHERE singleton=1 AND version=6;
