CREATE TABLE __SCHEMA__.outbox_repair_requests (
    scope_id text NOT NULL, id uuid NOT NULL, fingerprint text NOT NULL,
    intent_id uuid NOT NULL, quarantine_id uuid NOT NULL, actor text NOT NULL, reason text NOT NULL,
    acknowledge_possible_duplicate boolean NOT NULL,
    result_status integer NOT NULL, result_generation integer NOT NULL, result_version bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(scope_id,id)
);
UPDATE __SCHEMA__.schema_version SET version=7 WHERE singleton AND version=6;
