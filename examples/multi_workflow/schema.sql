CREATE TABLE rush_sales (
    timestamp_ms BIGINT NOT NULL,
    symbol       TEXT NOT NULL,
    amount       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (timestamp_ms, symbol)
);

CREATE TABLE window_totals (
    window_id BIGINT NOT NULL,
    symbol    TEXT NOT NULL,
    count     BIGINT NOT NULL,
    sum       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_id, symbol)
);
