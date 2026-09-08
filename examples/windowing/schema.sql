-- The windowing demos' destination tables: one per mode, and two for
-- tumbling, so its wasm and plugin paths land in visibly separate places.
--
-- `PostgresSink` never issues CREATE TABLE, so these have to exist before
-- the sinks' first write. Compose mounts this file into the container's
-- /docker-entrypoint-initdb.d/, which PostgreSQL runs once on first
-- initialisation of an empty data directory.
--
-- Columns match each mode's emitted component field for field, in order:
-- `WindowTotal` for tumbling, `SlidingTotal` for sliding, `SessionTotal`
-- for session. Every primary key is the one the mode's sink names in
-- `conflict_columns`, which is what makes its `upsert` idempotent across
-- re-runs and late re-fires.

CREATE TABLE wasm_window_totals (
    window_id BIGINT NOT NULL,
    symbol    TEXT NOT NULL,
    count     BIGINT NOT NULL,
    sum       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_id, symbol)
);

CREATE TABLE plugin_window_totals (
    window_id BIGINT NOT NULL,
    symbol    TEXT NOT NULL,
    count     BIGINT NOT NULL,
    sum       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_id, symbol)
);

CREATE TABLE sliding_window_totals (
    window_start_ms BIGINT NOT NULL,
    window_end_ms   BIGINT NOT NULL,
    symbol          TEXT NOT NULL,
    count           BIGINT NOT NULL,
    sum             DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start_ms, symbol)
);

CREATE TABLE session_window_totals (
    session_start_ms BIGINT NOT NULL,
    session_end_ms   BIGINT NOT NULL,
    symbol           TEXT NOT NULL,
    count            BIGINT NOT NULL,
    sum              DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (session_start_ms, symbol)
);