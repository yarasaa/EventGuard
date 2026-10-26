CREATE TABLE __SCHEMA__.producer_requests (
    scope_id text NOT NULL, id uuid NOT NULL, fingerprint text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), committed_at timestamptz,
    PRIMARY KEY(scope_id,id)
);
CREATE TABLE __SCHEMA__.outbox_messages (
    scope_id text NOT NULL, id uuid NOT NULL, tenant text NOT NULL, producer text NOT NULL,
    message_id uuid NOT NULL, transport text NOT NULL, connection_name text NOT NULL,
    configuration_version text NOT NULL, address text NOT NULL, routing_key text,
    payload bytea NOT NULL, content_type text NOT NULL, headers jsonb NOT NULL,
    not_before timestamptz, fingerprint text NOT NULL CHECK(length(fingerprint)=64),
    origin_work_id uuid REFERENCES __SCHEMA__.works(id), origin_request_id uuid,
    state integer NOT NULL DEFAULT 0 CHECK(state BETWEEN 0 AND 4),
    version bigint NOT NULL DEFAULT 0 CHECK(version>=0), attempt_count integer NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
    next_attempt_at timestamptz NOT NULL, lease_token uuid, lease_until timestamptz,
    delivery_uncertain boolean NOT NULL DEFAULT false, last_reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), published_at timestamptz,
    PRIMARY KEY(scope_id,id),
    FOREIGN KEY(scope_id,origin_request_id) REFERENCES __SCHEMA__.producer_requests(scope_id,id),
    CHECK(origin_work_id IS NULL OR origin_request_id IS NULL),
    CHECK((state=1 AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
        OR (state<>1 AND lease_token IS NULL AND lease_until IS NULL))
);
CREATE INDEX ix_outbox_ready ON __SCHEMA__.outbox_messages(scope_id,next_attempt_at,id) WHERE state IN (0,2);
CREATE INDEX ix_outbox_recovery ON __SCHEMA__.outbox_messages(scope_id,lease_until,id) WHERE state=1;
CREATE TABLE __SCHEMA__.outbox_attempts (
    scope_id text NOT NULL, id uuid NOT NULL, intent_id uuid NOT NULL, number integer NOT NULL,
    status text NOT NULL, reason text, started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
    PRIMARY KEY(scope_id,id), UNIQUE(scope_id,intent_id,number),
    FOREIGN KEY(scope_id,intent_id) REFERENCES __SCHEMA__.outbox_messages(scope_id,id)
);
UPDATE __SCHEMA__.schema_version SET version=2 WHERE singleton AND version=1;
