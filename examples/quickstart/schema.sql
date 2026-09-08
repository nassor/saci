-- The Quick Start's destination table.
--
-- `PostgresSink` never issues CREATE TABLE, so this has to exist before the
-- sink's first write. Compose mounts it into the container's
-- /docker-entrypoint-initdb.d/, which PostgreSQL runs once on first
-- initialisation of an empty data directory.
--
-- All twelve `Order` columns, in schema order. A sink's `schema_fields` must
-- match the component's RecordBatch field for field, in order: `PostgresSink`
-- projects nothing, and a batch with a different column count is rejected
-- rather than partially written. The five columns the Quick Start's two
-- stages never write (usd_amount, usd_amount_display, risk_score, flagged,
-- settlement) therefore arrive as the zero values the publisher sent.
--
-- review_tier is the settlement decision the C# stage writes:
--
--   0  settled
--   1  held for manual review, amount above hold_above
--   2  rejected, the Go stage found the amount below min_amount
CREATE TABLE settlements (
    id                 BIGINT PRIMARY KEY,
    region             TEXT NOT NULL,
    currency           TEXT NOT NULL,
    amount             DOUBLE PRECISION NOT NULL,
    valid              BOOLEAN NOT NULL,
    usd_amount         DOUBLE PRECISION NOT NULL,
    usd_amount_display TEXT NOT NULL,
    risk_score         DOUBLE PRECISION NOT NULL,
    flagged            BOOLEAN NOT NULL,
    fee                DOUBLE PRECISION NOT NULL,
    review_tier        BIGINT NOT NULL,
    settlement         TEXT NOT NULL
);
