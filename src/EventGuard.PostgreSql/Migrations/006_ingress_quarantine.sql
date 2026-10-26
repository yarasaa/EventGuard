CREATE TABLE __SCHEMA__.ingress_quarantine (
    id uuid PRIMARY KEY, scope_id text NOT NULL, identity_id text NOT NULL UNIQUE,
    fingerprint text NOT NULL, key_version text NOT NULL, fingerprint_version text NOT NULL,
    reason_code text NOT NULL, version bigint NOT NULL DEFAULT 0 CHECK(version>=0), released_work_id uuid,
    source_binding text NOT NULL, source_position text NOT NULL, codec text NOT NULL,
    body bytea NOT NULL CHECK(octet_length(body)<=1048576), metadata bytea NOT NULL CHECK(octet_length(metadata)<=65536),
    is_null_body boolean NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(scope_id) REFERENCES __SCHEMA__.policy_bindings(scope_id),
    FOREIGN KEY(released_work_id) REFERENCES __SCHEMA__.works(id),
    CHECK(NOT is_null_body OR octet_length(body)=0)
);
CREATE INDEX ingress_quarantine_scope ON __SCHEMA__.ingress_quarantine(scope_id,created_at,id);
CREATE TABLE __SCHEMA__.ingress_release_requests (
    scope_id text NOT NULL, id uuid NOT NULL, fingerprint text NOT NULL,
    quarantine_id uuid NOT NULL, actor text NOT NULL, reason text NOT NULL,
    result_status integer NOT NULL, result_work_id uuid, result_version bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(scope_id,id)
);
UPDATE __SCHEMA__.schema_version SET version=6 WHERE singleton AND version=5;
