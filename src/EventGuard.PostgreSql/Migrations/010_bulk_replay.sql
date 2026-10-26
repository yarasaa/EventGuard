CREATE TABLE __SCHEMA__.bulk_replay_requests (
    scope_id text NOT NULL, id uuid NOT NULL, kind integer NOT NULL CHECK(kind IN (0,1)), fingerprint text NOT NULL,
    actor text NOT NULL, reason text NOT NULL, acknowledge_possible_duplicate boolean NOT NULL,
    items jsonb NOT NULL CHECK(jsonb_typeof(items)='array' AND jsonb_array_length(items) BETWEEN 1 AND 100),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(scope_id,id)
);
UPDATE __SCHEMA__.schema_version SET version=10 WHERE singleton AND version=9;
