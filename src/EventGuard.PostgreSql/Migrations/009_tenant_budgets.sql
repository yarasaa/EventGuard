CREATE TABLE __SCHEMA__.tenant_budgets (
    tenant_id text PRIMARY KEY, tenant text NOT NULL, version bigint NOT NULL CHECK(version>=0),
    window_seconds integer NOT NULL CHECK(window_seconds BETWEEN 1 AND 86400),
    maximum_claims bigint, maximum_retries bigint, maximum_recoveries bigint,
    window_started_at timestamptz NOT NULL, claims bigint NOT NULL CHECK(claims>=0),
    retries bigint NOT NULL CHECK(retries>=0), recoveries bigint NOT NULL CHECK(recoveries>=0),
    CHECK(retries<=claims), CHECK(maximum_claims BETWEEN 0 AND 1000000000), CHECK(maximum_retries BETWEEN 0 AND 1000000000), CHECK(maximum_recoveries BETWEEN 0 AND 1000000000)
);
CREATE TABLE __SCHEMA__.tenant_budget_requests (
    tenant_id text NOT NULL, id uuid NOT NULL, fingerprint text NOT NULL,
    expected_version bigint NOT NULL, requested_window_seconds integer NOT NULL, requested_max_claims bigint, requested_max_retries bigint, requested_max_recoveries bigint,
    actor text NOT NULL, reason text NOT NULL, result_status integer NOT NULL, result_version bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(tenant_id,id)
);
UPDATE __SCHEMA__.schema_version SET version=9 WHERE singleton AND version=8;
