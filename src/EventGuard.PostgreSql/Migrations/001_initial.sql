CREATE SCHEMA IF NOT EXISTS __SCHEMA__;
CREATE TABLE IF NOT EXISTS __SCHEMA__.schema_version (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), version integer NOT NULL
);
INSERT INTO __SCHEMA__.schema_version VALUES (true, 1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS __SCHEMA__.policy_bindings (
    scope_id text PRIMARY KEY, tenant text NOT NULL, consumer text NOT NULL,
    source text NOT NULL, event_type text NOT NULL,
    key_version text NOT NULL, fingerprint_version text NOT NULL
);
CREATE TABLE IF NOT EXISTS __SCHEMA__.works (
    id uuid PRIMARY KEY, identity_id text NOT NULL UNIQUE,
    scope_id text NOT NULL REFERENCES __SCHEMA__.policy_bindings(scope_id),
    business_key text NOT NULL, message_id uuid NOT NULL,
    fingerprint text NOT NULL CHECK (length(fingerprint) = 64), payload bytea NOT NULL,
    state integer NOT NULL CHECK (state BETWEEN 0 AND 5), version bigint NOT NULL DEFAULT 0,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    generation_attempt integer NOT NULL DEFAULT 0 CHECK (generation_attempt >= 0),
    replay_generation integer NOT NULL DEFAULT 0 CHECK (replay_generation >= 0),
    next_attempt_at timestamptz NOT NULL, expires_at timestamptz,
    lease_token uuid, lease_until timestamptz, last_reason text, correlation_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), completed_at timestamptz,
    CHECK ((state = 1 AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
        OR (state <> 1 AND lease_token IS NULL AND lease_until IS NULL))
);
CREATE INDEX IF NOT EXISTS ix_ready ON __SCHEMA__.works(scope_id, next_attempt_at, id)
    WHERE state IN (0, 2);
CREATE INDEX IF NOT EXISTS ix_recovery ON __SCHEMA__.works(scope_id, lease_until, id)
    WHERE state = 1;
CREATE TABLE IF NOT EXISTS __SCHEMA__.attempts (
    id uuid PRIMARY KEY, work_id uuid NOT NULL REFERENCES __SCHEMA__.works(id),
    number integer NOT NULL, generation integer NOT NULL, status text NOT NULL,
    started_at timestamptz NOT NULL, finished_at timestamptz, reason text,
    UNIQUE(work_id, number)
);
CREATE TABLE IF NOT EXISTS __SCHEMA__.conflicts (
    id uuid PRIMARY KEY, work_id uuid NOT NULL REFERENCES __SCHEMA__.works(id),
    message_id uuid NOT NULL, fingerprint text NOT NULL, payload bytea NOT NULL,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS __SCHEMA__.replay_requests (
    scope_id text NOT NULL, id uuid NOT NULL, fingerprint text NOT NULL,
    work_id uuid NOT NULL, actor text NOT NULL, reason text NOT NULL,
    result_status integer NOT NULL, result_generation integer NOT NULL, result_version bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(scope_id, id)
);
