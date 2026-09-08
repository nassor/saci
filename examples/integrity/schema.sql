-- The integrity example's audit table and its logical-replication artefacts.
--
-- Compose mounts this file into the container's /docker-entrypoint-initdb.d/,
-- which PostgreSQL runs once on first initialisation of an empty data
-- directory. `wal_level = logical` comes from the compose `command`.
--
-- Columns match the `AuditRow` the publisher inserts and the `audit`
-- workflow's `schema_fields`, field for field, in order: audit_id, order_id,
-- status, revision, changed_ms. One row is one revision of one order, so a
-- single order_id appears several times with distinct audit_ids.

CREATE TABLE public.order_audit (
    audit_id   BIGINT PRIMARY KEY,
    order_id   BIGINT NOT NULL,
    status     TEXT NOT NULL,
    revision   INTEGER NOT NULL,
    changed_ms BIGINT NOT NULL
);

-- `PostgresSource` with mode kind="cdc_logical" creates its replication slot
-- (`slot_autocreate`) but never a publication, so this one has to exist before
-- the audit service opens the slot. The name is what
-- examples/integrity/integrity_audit.kdl's `publication` key must say.
CREATE PUBLICATION saci_integrity_pub FOR TABLE public.order_audit;

-- REQUIRED, not a nicety. Without it an UPDATE's old tuple and a DELETE carry
-- only replica-identity columns, so every non-key field would have to be
-- declared nullable in the audit workflow's `schema_fields`. The example
-- updates rows as well as inserting them, which is what makes op = "U" reach
-- the sink.
ALTER TABLE public.order_audit REPLICA IDENTITY FULL;
