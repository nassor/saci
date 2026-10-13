//! [`PostgresSink`] against a real PostgreSQL server.
//!
//! The append test is the end to end proof that `encode.rs` and `values.rs`
//! agree with the server: every supported type is written through
//! `COPY … FORMAT binary` and read back with `SELECT`, value for value.
//!
//! Soft-skips without Docker; see `common::try_start`.

mod common;

use std::sync::Arc;

use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::{
    BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow_schema::Schema;
use saci_connector::from_kdl_str;
use saci_connector_postgresql::{PostgresSink, PostgresSinkConfig};
use saci_core::io::sink::Sink;
use serde::Deserialize as _;

fn sink(dsn: &str, body: &str) -> PostgresSink {
    let text = format!(
        "{body}\n\nconnection dsn={} sslmode=\"disable\"\n",
        common::quoted(dsn)
    );
    let cfg = PostgresSinkConfig::deserialize(from_kdl_str(&text).expect("parse kdl"))
        .expect("parse config");
    PostgresSink::new(cfg).expect("build sink")
}

/// PostgreSQL's canonical `uuid` text form for 16 raw bytes.
fn uuid_text(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Every supported declared type, in the order the DDL below creates them.
const ALL_TYPES_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"flag\" type=\"boolean\"
schema_fields \"small\" type=\"int16\"
schema_fields \"medium\" type=\"int32\"
schema_fields \"real_value\" type=\"float32\"
schema_fields \"double_value\" type=\"float64\"
schema_fields \"label\" type=\"utf8\"
schema_fields \"blob\" type=\"binary\"
schema_fields \"day\" type=\"date32\"
schema_fields \"clock\" type=\"time64_micros\"
schema_fields \"naive_ts\" type=\"timestamp_micros\"
schema_fields \"utc_ts\" type=\"timestamp_micros_utc\"
schema_fields \"uid\" type=\"uuid\"
schema_fields \"doc\" type=\"json\"
schema_fields \"amount\" type=\"decimal128\" precision=18 scale=4
";

const ALL_TYPES_DDL: &str = "CREATE TABLE wide ( \
     id bigint PRIMARY KEY, \
     flag boolean, \
     small smallint, \
     medium integer, \
     real_value real, \
     double_value double precision, \
     label text, \
     blob bytea, \
     day date, \
     clock time, \
     naive_ts timestamp, \
     utc_ts timestamptz, \
     uid uuid, \
     doc jsonb, \
     amount numeric(18,4) \
 )";

const ROWS: usize = 1000;

/// Build a batch covering every supported type, with a NULL in every nullable
/// column on one row so the null path is exercised too.
fn wide_batch(schema: Arc<Schema>) -> RecordBatch {
    let nullable = |row: usize| row % 97 == 5;

    let mut uuids = FixedSizeBinaryBuilder::with_capacity(ROWS, 16);
    for row in 0..ROWS {
        if nullable(row) {
            uuids.append_null();
        } else {
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&(row as u64).to_be_bytes());
            bytes[8..].copy_from_slice(&(row as u64).to_le_bytes());
            uuids.append_value(bytes).expect("16 bytes");
        }
    }

    let opt = |row: usize, value: i64| if nullable(row) { None } else { Some(value) };

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values((0..ROWS).map(|r| r as i64))),
            Arc::new(BooleanArray::from_iter(
                (0..ROWS).map(|r| if nullable(r) { None } else { Some(r % 3 == 0) }),
            )),
            Arc::new(Int16Array::from_iter(
                (0..ROWS).map(|r| opt(r, (r as i64 % 30_000) - 15_000).map(|v| v as i16)),
            )),
            Arc::new(Int32Array::from_iter(
                (0..ROWS).map(|r| opt(r, r as i64 * 1_000 - 500_000).map(|v| v as i32)),
            )),
            Arc::new(Float32Array::from_iter((0..ROWS).map(|r| {
                if nullable(r) {
                    None
                } else {
                    Some(r as f32 * 0.5)
                }
            }))),
            Arc::new(Float64Array::from_iter((0..ROWS).map(|r| {
                if nullable(r) {
                    None
                } else {
                    Some(r as f64 * 1.25e10)
                }
            }))),
            Arc::new(StringArray::from_iter((0..ROWS).map(|r| {
                if nullable(r) {
                    None
                } else {
                    // Include a quote and a non-ASCII character.
                    Some(format!("row \"{r}\" \u{2713}"))
                }
            }))),
            Arc::new(BinaryArray::from_iter((0..ROWS).map(|r| {
                if nullable(r) {
                    None
                } else {
                    Some(vec![0u8, 255, (r % 256) as u8])
                }
            }))),
            // Days since 1970-01-01, spanning both sides of the Postgres epoch.
            Arc::new(Date32Array::from_iter(
                (0..ROWS).map(|r| opt(r, r as i64 * 13 - 5_000).map(|v| v as i32)),
            )),
            Arc::new(Time64MicrosecondArray::from_iter(
                (0..ROWS).map(|r| opt(r, (r as i64 * 86_399_999) % 86_400_000_000)),
            )),
            Arc::new(TimestampMicrosecondArray::from_iter(
                (0..ROWS).map(|r| opt(r, r as i64 * 1_000_000_007 - 900_000_000_000)),
            )),
            Arc::new(
                TimestampMicrosecondArray::from_iter(
                    (0..ROWS).map(|r| opt(r, r as i64 * 999_999_937 - 500_000_000_000)),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(uuids.finish()),
            Arc::new(StringArray::from_iter((0..ROWS).map(|r| {
                if nullable(r) {
                    None
                } else {
                    Some(format!("{{\"n\":{r}}}"))
                }
            }))),
            Arc::new(
                Decimal128Array::from_iter(
                    (0..ROWS).map(|r| opt(r, r as i64 * 1_234_567 - 3_000_000).map(i128::from)),
                )
                .with_precision_and_scale(18, 4)
                .expect("scale"),
            ),
        ],
    )
    .expect("batch")
}

#[tokio::test]
async fn append_writes_every_supported_type_and_reads_back_equal() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(ALL_TYPES_DDL).await.expect("fixture");

    let mut sink = sink(
        &pg.dsn(),
        &format!("name \"wide\"\ntable \"wide\"\nchunk_rows 137\n{ALL_TYPES_FIELDS}"),
    );

    let batch = wide_batch(sink.schema());
    sink.write_batch(&batch).await.expect("write");
    sink.finish().await.expect("finish");

    let count: i64 = client
        .query_one("SELECT count(*) FROM wide", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count as usize, ROWS);

    // Read every column back through the connector's own decoder by selecting
    // it, so a disagreement between encode.rs and values.rs shows up as a value
    // mismatch rather than as a silent re-encode.
    let rows = client
        .query(
            "SELECT id, flag, small, medium, real_value, double_value, label, blob, \
             day - DATE '1970-01-01' AS day_offset, \
             (EXTRACT(EPOCH FROM clock) * 1000000)::int8 AS clock_micros, \
             (EXTRACT(EPOCH FROM naive_ts) * 1000000)::int8 AS naive_micros, \
             (EXTRACT(EPOCH FROM utc_ts) * 1000000)::int8 AS utc_micros, \
             uid::text AS uid_text, doc::text, (amount * 10000)::int8 AS amount_unscaled \
             FROM wide ORDER BY id",
            &[],
        )
        .await
        .expect("read back");
    assert_eq!(rows.len(), ROWS);

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let flags = batch
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let smalls = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int16Array>()
        .unwrap();
    let mediums = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let reals = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    let doubles = batch
        .column(5)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let labels = batch
        .column(6)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let blobs = batch
        .column(7)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let days = batch
        .column(8)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    let clocks = batch
        .column(9)
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap();
    let naive = batch
        .column(10)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let utc = batch
        .column(11)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let uids = batch
        .column(12)
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .unwrap();
    let docs = batch
        .column(13)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let amounts = batch
        .column(14)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();

    use arrow_array::Array;
    for (row, record) in rows.iter().enumerate() {
        assert_eq!(record.get::<_, i64>("id"), ids.value(row), "row {row} id");
        assert_eq!(
            record.get::<_, Option<bool>>("flag"),
            (!flags.is_null(row)).then(|| flags.value(row)),
            "row {row} flag"
        );
        assert_eq!(
            record.get::<_, Option<i16>>("small"),
            (!smalls.is_null(row)).then(|| smalls.value(row)),
            "row {row} small"
        );
        assert_eq!(
            record.get::<_, Option<i32>>("medium"),
            (!mediums.is_null(row)).then(|| mediums.value(row)),
            "row {row} medium"
        );
        assert_eq!(
            record.get::<_, Option<f32>>("real_value"),
            (!reals.is_null(row)).then(|| reals.value(row)),
            "row {row} real"
        );
        assert_eq!(
            record.get::<_, Option<f64>>("double_value"),
            (!doubles.is_null(row)).then(|| doubles.value(row)),
            "row {row} double"
        );
        assert_eq!(
            record.get::<_, Option<&str>>("label"),
            (!labels.is_null(row)).then(|| labels.value(row)),
            "row {row} label"
        );
        assert_eq!(
            record.get::<_, Option<&[u8]>>("blob"),
            (!blobs.is_null(row)).then(|| blobs.value(row)),
            "row {row} blob"
        );
        assert_eq!(
            record.get::<_, Option<i32>>("day_offset"),
            (!days.is_null(row)).then(|| days.value(row)),
            "row {row} day"
        );
        assert_eq!(
            record.get::<_, Option<i64>>("clock_micros"),
            (!clocks.is_null(row)).then(|| clocks.value(row)),
            "row {row} clock"
        );
        assert_eq!(
            record.get::<_, Option<i64>>("naive_micros"),
            (!naive.is_null(row)).then(|| naive.value(row)),
            "row {row} naive timestamp"
        );
        assert_eq!(
            record.get::<_, Option<i64>>("utc_micros"),
            (!utc.is_null(row)).then(|| utc.value(row)),
            "row {row} utc timestamp"
        );
        // `uuid` has no &[u8] FromSql, so compare PostgreSQL's canonical text
        // form against the 16 bytes the encoder wrote.
        let uid: Option<&str> = record.get("uid_text");
        assert_eq!(
            uid.map(str::to_string),
            (!uids.is_null(row)).then(|| uuid_text(uids.value(row))),
            "row {row} uuid"
        );
        let doc: Option<&str> = record.get("doc");
        match (doc, docs.is_null(row)) {
            (None, true) => {}
            (Some(text), false) => {
                // jsonb normalises whitespace, so compare the parsed shape.
                assert_eq!(
                    text.replace(' ', ""),
                    docs.value(row).replace(' ', ""),
                    "row {row} doc"
                );
            }
            other => panic!("row {row} doc mismatch: {other:?}"),
        }
        assert_eq!(
            record.get::<_, Option<i64>>("amount_unscaled"),
            (!amounts.is_null(row)).then(|| amounts.value(row) as i64),
            "row {row} amount"
        );
    }
}

const SMALL_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"label\" type=\"utf8\"
schema_fields \"seq\" type=\"int64\" nullable=#false
";

fn small_batch(schema: Arc<Schema>, rows: &[(i64, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )
    .expect("batch")
}

async fn create_small(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "CREATE TABLE small (id bigint PRIMARY KEY, label text, seq bigint NOT NULL)",
        )
        .await
        .expect("fixture");
}

#[tokio::test]
async fn upsert_updates_in_place_and_ignore_conflicts_does_not() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    create_small(&client).await;

    let mut upsert = sink(
        &pg.dsn(),
        &format!(
            "name \"up\"\ntable \"small\"\nwrite_mode \"upsert\"\n\
             conflict_columns \"id\"\n{SMALL_FIELDS}"
        ),
    );
    let schema = upsert.schema();
    upsert
        .write_batch(&small_batch(
            Arc::clone(&schema),
            &[(1, "first", 1), (2, "first", 1)],
        ))
        .await
        .expect("first write");
    upsert
        .write_batch(&small_batch(
            Arc::clone(&schema),
            &[(1, "second", 2), (2, "second", 2)],
        ))
        .await
        .expect("second write");
    upsert.finish().await.expect("finish");

    let rows = client
        .query("SELECT id, label, seq FROM small ORDER BY id", &[])
        .await
        .expect("read back");
    assert_eq!(rows.len(), 2, "upsert must not add rows for existing keys");
    assert_eq!(rows[0].get::<_, &str>(1), "second");
    assert_eq!(rows[0].get::<_, i64>(2), 2);

    let mut ignore = sink(
        &pg.dsn(),
        &format!(
            "name \"ig\"\ntable \"small\"\nwrite_mode \"ignore_conflicts\"\n\
             conflict_columns \"id\"\n{SMALL_FIELDS}"
        ),
    );
    ignore
        .write_batch(&small_batch(
            Arc::clone(&schema),
            &[(1, "third", 3), (3, "new", 3)],
        ))
        .await
        .expect("write");
    ignore.finish().await.expect("finish");

    let rows = client
        .query("SELECT id, label FROM small ORDER BY id", &[])
        .await
        .expect("read back");
    assert_eq!(rows.len(), 3, "the new key must be inserted");
    assert_eq!(
        rows[0].get::<_, &str>(1),
        "second",
        "ignore_conflicts must leave the existing row alone"
    );
}

#[tokio::test]
async fn a_repeated_conflict_key_needs_dedupe_order_column() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    create_small(&client).await;

    // Without the option, Postgres refuses the second hit on one row.
    let mut plain = sink(
        &pg.dsn(),
        &format!(
            "name \"plain\"\ntable \"small\"\nwrite_mode \"upsert\"\n\
             conflict_columns \"id\"\n{SMALL_FIELDS}"
        ),
    );
    let schema = plain.schema();
    let duplicated = small_batch(Arc::clone(&schema), &[(1, "low", 1), (1, "high", 2)]);
    let err = plain
        .write_batch(&duplicated)
        .await
        .expect_err("a repeated conflict key must be rejected");
    assert!(
        err.message().contains("dedupe_order_column"),
        "the message must name the fix: {}",
        err.message()
    );

    // With it, the highest ordering value wins.
    let mut deduped = sink(
        &pg.dsn(),
        &format!(
            "name \"dedup\"\ntable \"small\"\nwrite_mode \"upsert\"\n\
             conflict_columns \"id\"\ndedupe_order_column \"seq\"\n{SMALL_FIELDS}"
        ),
    );
    deduped.write_batch(&duplicated).await.expect("write");
    deduped.finish().await.expect("finish");

    let rows = client
        .query("SELECT id, label, seq FROM small", &[])
        .await
        .expect("read back");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "high");
    assert_eq!(rows[0].get::<_, i64>(2), 2);
}

#[tokio::test]
async fn flush_rows_buffers_until_the_threshold_and_finish_drains_the_rest() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    create_small(&client).await;

    let mut sink = sink(
        &pg.dsn(),
        &format!("name \"buf\"\ntable \"small\"\nflush_rows 5\n{SMALL_FIELDS}"),
    );
    let schema = sink.schema();

    sink.write_batch(&small_batch(
        Arc::clone(&schema),
        &[(1, "a", 1), (2, "b", 2)],
    ))
    .await
    .expect("write");
    assert_eq!(sink.pending_rows(), Some(2));
    let count: i64 = client
        .query_one("SELECT count(*) FROM small", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 0, "below flush_rows nothing reaches the server");

    sink.write_batch(&small_batch(
        Arc::clone(&schema),
        &[(3, "c", 3), (4, "d", 4), (5, "e", 5)],
    ))
    .await
    .expect("write");
    assert_eq!(sink.pending_rows(), Some(0), "the threshold flushed");
    let count: i64 = client
        .query_one("SELECT count(*) FROM small", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 5);

    sink.write_batch(&small_batch(Arc::clone(&schema), &[(6, "f", 6)]))
        .await
        .expect("write");
    sink.finish().await.expect("finish");
    let count: i64 = client
        .query_one("SELECT count(*) FROM small", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 6, "finish must drain the remainder");
}

#[tokio::test]
async fn truncate_before_first_write_replaces_the_table_contents_once() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    create_small(&client).await;
    client
        .batch_execute("INSERT INTO small VALUES (99, 'stale', 99)")
        .await
        .expect("seed");

    let mut sink = sink(
        &pg.dsn(),
        &format!(
            "name \"tr\"\ntable \"small\"\ntruncate_before_first_write #true\n\
             {SMALL_FIELDS}"
        ),
    );
    let schema = sink.schema();
    sink.write_batch(&small_batch(Arc::clone(&schema), &[(1, "a", 1)]))
        .await
        .expect("first write");
    sink.write_batch(&small_batch(Arc::clone(&schema), &[(2, "b", 2)]))
        .await
        .expect("second write");
    sink.finish().await.expect("finish");

    let ids: Vec<i64> = client
        .query("SELECT id FROM small ORDER BY id", &[])
        .await
        .expect("read back")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        ids,
        vec![1, 2],
        "the truncate runs once, so the second write is additive"
    );
}

#[tokio::test]
async fn a_missing_table_is_a_configuration_error_naming_it_on_every_flush() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let mut sink = sink(
        &pg.dsn(),
        &format!("name \"nope\"\ntable \"public.absent\"\n{SMALL_FIELDS}"),
    );
    let schema = sink.schema();
    for attempt in 1..=2 {
        let err = sink
            .write_batch(&small_batch(Arc::clone(&schema), &[(1, "a", 1)]))
            .await
            .expect_err("the table does not exist");
        assert_eq!(err.category(), "configuration", "attempt {attempt}");
        assert!(
            err.message().contains("public.absent"),
            "attempt {attempt}: {}",
            err.message()
        );
    }
}

#[tokio::test]
async fn a_declared_type_the_target_column_cannot_hold_is_rejected() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE small (id bigint PRIMARY KEY, label text, seq integer NOT NULL)",
        )
        .await
        .expect("fixture");

    let mut sink = sink(
        &pg.dsn(),
        &format!("name \"mismatch\"\ntable \"small\"\n{SMALL_FIELDS}"),
    );
    let schema = sink.schema();
    let err = sink
        .write_batch(&small_batch(schema, &[(1, "a", 1)]))
        .await
        .expect_err("int64 cannot fill an integer column");
    assert_eq!(err.category(), "configuration");
    assert!(err.message().contains("'seq'"), "{}", err.message());
    assert!(err.message().contains("int4"), "{}", err.message());
}

#[tokio::test]
async fn a_table_name_with_a_quote_is_written_not_injected() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE \"my\"\"table\" (id bigint PRIMARY KEY, label text, \
             seq bigint NOT NULL)",
        )
        .await
        .expect("fixture");

    let mut sink = sink(
        &pg.dsn(),
        &format!("name \"quoted\"\ntable \"my\\\"table\"\n{SMALL_FIELDS}"),
    );
    let schema = sink.schema();
    sink.write_batch(&small_batch(schema, &[(1, "a", 1)]))
        .await
        .expect("write");
    sink.finish().await.expect("finish");

    let count: i64 = client
        .query_one("SELECT count(*) FROM \"my\"\"table\"", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 1);
}

// ------------------------------------------------- retry, drop and reconnect

async fn ids(client: &tokio_postgres::Client, table: &str) -> Vec<i64> {
    client
        .query(&format!("SELECT id FROM {table} ORDER BY id"), &[])
        .await
        .expect("read back")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

async fn create_keyless(client: &tokio_postgres::Client, table: &str) {
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (id bigint NOT NULL, label text, seq bigint NOT NULL)"
        ))
        .await
        .expect("fixture");
}

async fn create_keyed(client: &tokio_postgres::Client, table: &str) {
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (id bigint PRIMARY KEY, label text, seq bigint NOT NULL)"
        ))
        .await
        .expect("fixture");
}

/// A sink over `SMALL_FIELDS` writing `table`, with `extra` config lines.
fn small_sink(pg: &common::PgContainer, table: &str, mode: &str, extra: &str) -> PostgresSink {
    let keys = if mode == "append" {
        ""
    } else {
        "conflict_columns \"id\"\n"
    };
    sink(
        &pg.dsn(),
        &format!(
            "name \"s\"\ntable \"{table}\"\nwrite_mode \"{mode}\"\n{keys}{extra}{SMALL_FIELDS}"
        ),
    )
}

fn one(schema: &Arc<Schema>, id: i64) -> RecordBatch {
    small_batch(Arc::clone(schema), &[(id, "v", id)])
}

/// The service's `RetryingSink` re-drives the batch whose `write_batch` failed.
/// A flush that failed must not leave that batch buffered, or the retry
/// buffers it a second time and a table without a key gets every row twice.
#[tokio::test]
async fn a_batch_the_caller_retries_is_written_once() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    let mut sink = small_sink(&pg, "retried_once", "append", "");
    let schema = sink.schema();
    let batch = small_batch(Arc::clone(&schema), &[(1, "a", 1), (2, "b", 2)]);

    sink.write_batch(&batch)
        .await
        .expect_err("the table does not exist yet");
    assert_eq!(sink.pending_rows(), Some(0), "a refused batch is not held");

    create_keyless(&client, "retried_once").await;
    sink.write_batch(&batch).await.expect("the retry");
    sink.finish().await.expect("nothing left to write");
    assert_eq!(
        ids(&client, "retried_once").await,
        vec![1, 2],
        "each row is written once"
    );
}

#[tokio::test]
async fn a_failed_flush_keeps_the_rows_an_earlier_call_accepted() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    create_keyed(&client, "retried").await;
    client
        .batch_execute("INSERT INTO retried VALUES (1, 'x', 1)")
        .await
        .expect("seed");
    let mut sink = small_sink(&pg, "retried", "append", "flush_rows 3\n");
    let schema = sink.schema();

    sink.write_batch(&one(&schema, 2)).await.expect("buffered");
    let refused = small_batch(Arc::clone(&schema), &[(1, "a", 1), (3, "c", 3)]);
    sink.write_batch(&refused)
        .await
        .expect_err("append collides on 1");
    assert_eq!(
        sink.pending_rows(),
        Some(1),
        "the accepted row stays; the refused batch is the caller's to re-drive"
    );
    assert_eq!(ids(&client, "retried").await, vec![1], "rolled back whole");

    client
        .batch_execute("DELETE FROM retried")
        .await
        .expect("clear");
    sink.write_batch(&refused)
        .await
        .expect("the re-driven batch");
    assert_eq!(sink.pending_rows(), Some(0));
    assert_eq!(ids(&client, "retried").await, vec![1, 2, 3]);
}

#[tokio::test]
async fn a_failed_finish_keeps_its_rows_for_the_next_attempt() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    create_keyed(&client, "finished").await;
    client
        .batch_execute("INSERT INTO finished VALUES (1, 'x', 1)")
        .await
        .expect("seed");
    let mut sink = small_sink(&pg, "finished", "append", "flush_rows 100\n");
    let schema = sink.schema();

    sink.write_batch(&small_batch(
        Arc::clone(&schema),
        &[(2, "b", 2), (1, "a", 1)],
    ))
    .await
    .expect("buffered");
    sink.finish().await.expect_err("append collides on 1");
    assert_eq!(sink.pending_rows(), Some(2));
    assert_eq!(ids(&client, "finished").await, vec![1], "rolled back whole");

    client
        .batch_execute("DELETE FROM finished")
        .await
        .expect("clear");
    sink.finish().await.expect("the buffered rows are retried");
    assert_eq!(sink.pending_rows(), Some(0));
    assert_eq!(ids(&client, "finished").await, vec![1, 2]);
}

#[tokio::test]
async fn a_failed_truncating_flush_rolls_the_truncate_back() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    create_keyed(&client, "guarded").await;
    client
        .batch_execute("INSERT INTO guarded VALUES (1, 'old', 1), (2, 'old', 2)")
        .await
        .expect("seed");
    let mut sink = small_sink(
        &pg,
        "guarded",
        "append",
        "truncate_before_first_write #true\n",
    );
    let schema = sink.schema();

    let duplicated = small_batch(Arc::clone(&schema), &[(7, "a", 1), (7, "b", 2)]);
    sink.write_batch(&duplicated)
        .await
        .expect_err("a duplicate inside the batch");
    assert_eq!(
        ids(&client, "guarded").await,
        vec![1, 2],
        "the truncate is part of the failed transaction"
    );
    sink.write_batch(&small_batch(
        Arc::clone(&schema),
        &[(7, "a", 1), (8, "b", 2)],
    ))
    .await
    .expect("the first write that lands still truncates");
    assert_eq!(ids(&client, "guarded").await, vec![7, 8]);
}

/// A reconnect starts a session, not a run: rows an earlier flush committed
/// must survive it.
#[tokio::test]
async fn first_write_truncates_only_once_across_a_reconnect() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    create_keyed(&client, "reconnected").await;
    client
        .batch_execute("INSERT INTO reconnected VALUES (100, 'old', 100)")
        .await
        .expect("seed");
    let mut sink = small_sink(
        &pg,
        "reconnected",
        "append",
        "truncate_before_first_write #true\n",
    );
    let schema = sink.schema();
    sink.write_batch(&one(&schema, 1)).await.expect("first");

    // Terminate every other backend, the sink's included.
    client
        .batch_execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE pid <> pg_backend_pid() AND backend_type = 'client backend'",
        )
        .await
        .expect("terminate");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    sink.write_batch(&one(&schema, 2))
        .await
        .expect("the sink reconnects");
    assert_eq!(
        ids(&client, "reconnected").await,
        vec![1, 2],
        "a reconnect must not truncate the rows the first flush wrote"
    );
}

/// How long a dropped call runs first, in microseconds: from not at all to long
/// enough to finish, so the drop lands in the connect, the catalog read, the
/// transaction and the commit in turn.
const DROP_AFTER_MICROS: [u64; 15] = [
    0, 100, 200, 300, 500, 800, 1_200, 2_000, 3_000, 5_000, 8_000, 13_000, 21_000, 40_000, 100_000,
];

/// A runner that `select!`s against a cancel token drops the call it was
/// awaiting. Dropped while its `COPY` waits on a table lock, the transaction is
/// open on the server; the next call must neither reuse its state nor commit
/// it.
#[tokio::test]
async fn a_write_dropped_mid_copy_is_redriven_once() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    let admin = pg.connect().await;
    create_keyless(&client, "dropped").await;
    let mut sink = small_sink(&pg, "dropped", "append", "");
    let schema = sink.schema();
    sink.write_batch(&one(&schema, 1))
        .await
        .expect("opens the session");

    client
        .batch_execute("BEGIN; LOCK TABLE dropped IN ACCESS EXCLUSIVE MODE")
        .await
        .expect("lock");
    let batch = small_batch(Arc::clone(&schema), &[(2, "b", 2), (3, "c", 3)]);
    {
        let write = sink.write_batch(&batch);
        tokio::pin!(write);
        let parked = async {
            loop {
                let waiting: i64 = admin
                    .query_one(
                        "SELECT count(*) FROM pg_stat_activity \
                         WHERE wait_event_type = 'Lock' AND query LIKE 'COPY%dropped%'",
                        &[],
                    )
                    .await
                    .expect("activity")
                    .get(0);
                if waiting > 0 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        };
        tokio::select! {
            outcome = &mut write => panic!("the COPY cannot finish under the lock: {outcome:?}"),
            () = parked => {}
        }
        // `write` is dropped here, with its transaction open on the server.
    }
    client.batch_execute("COMMIT").await.expect("unlock");

    sink.write_batch(&batch).await.expect("the re-drive");
    sink.finish().await.expect("nothing left to write");
    assert_eq!(sink.pending_rows(), Some(0));
    assert_eq!(
        ids(&client, "dropped").await,
        vec![1, 2, 3],
        "each row once: the dropped call's transaction is not committed by the next one"
    );
}

/// Drop `write_batch` after each interval in turn, the sink warm and cold, with
/// rows an earlier call accepted waiting in its buffer. The caller re-drives
/// the batch it dropped, so the table must end with every row exactly once,
/// the accepted ones included, and no call may fail or panic on the way.
#[tokio::test]
async fn a_dropped_flush_at_any_point_loses_nothing_and_repeats_nothing() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    let mut n = 0;
    for primed in [false, true] {
        for micros in DROP_AFTER_MICROS {
            n += 1;
            let table = format!("sweep_flush_{n}");
            let at = format!("primed={primed} after {micros} us");
            create_keyed(&client, &table).await;
            // `upsert`, because a drop that lands after the `COMMIT` went out
            // writes the accepted rows a second time, as any retry of a lost
            // commit does.
            let mut sink = small_sink(&pg, &table, "upsert", "flush_rows 5\nchunk_rows 2\n");
            let schema = sink.schema();
            let mut want: Vec<i64> = vec![1, 2, 3, 4, 5];
            if primed {
                let warm = small_batch(
                    Arc::clone(&schema),
                    &[
                        (100, "w", 1),
                        (101, "w", 1),
                        (102, "w", 1),
                        (103, "w", 1),
                        (104, "w", 1),
                    ],
                );
                sink.write_batch(&warm).await.expect("warms the session");
                want.extend(100..105);
            }
            sink.write_batch(&small_batch(
                Arc::clone(&schema),
                &[(1, "a", 1), (2, "b", 2)],
            ))
            .await
            .expect("buffered");
            let own = small_batch(
                Arc::clone(&schema),
                &[(3, "c", 3), (4, "d", 4), (5, "e", 5)],
            );
            let outcome = tokio::time::timeout(
                std::time::Duration::from_micros(micros),
                sink.write_batch(&own),
            )
            .await;
            match outcome {
                Ok(result) => result.unwrap_or_else(|e| panic!("{at}: {e}")),
                Err(_elapsed) => sink
                    .write_batch(&own)
                    .await
                    .unwrap_or_else(|e| panic!("{at}: the re-drive failed: {e}")),
            }
            sink.finish()
                .await
                .unwrap_or_else(|e| panic!("{at}: finish: {e}"));
            assert_eq!(sink.pending_rows(), Some(0), "{at}");
            assert_eq!(ids(&client, &table).await, want, "{at}");
        }
    }
}

/// The same sweep over `finish`: a dropped one keeps what it held, and a second
/// `finish` writes it.
#[tokio::test]
async fn a_dropped_finish_at_any_point_loses_nothing() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    let mut n = 0;
    for primed in [false, true] {
        for micros in DROP_AFTER_MICROS {
            n += 1;
            let table = format!("sweep_finish_{n}");
            let at = format!("primed={primed} after {micros} us");
            create_keyed(&client, &table).await;
            let mut sink = small_sink(&pg, &table, "upsert", "flush_rows 100\nchunk_rows 1\n");
            let schema = sink.schema();
            let mut want: Vec<i64> = vec![1, 2, 3];
            if primed {
                sink.write_batch(&one(&schema, 100))
                    .await
                    .expect("buffered");
                sink.finish().await.expect("warms the session");
                want.push(100);
            }
            sink.write_batch(&small_batch(
                Arc::clone(&schema),
                &[(1, "a", 1), (2, "b", 2), (3, "c", 3)],
            ))
            .await
            .expect("buffered");
            let outcome =
                tokio::time::timeout(std::time::Duration::from_micros(micros), sink.finish()).await;
            if let Ok(result) = outcome {
                result.unwrap_or_else(|e| panic!("{at}: {e}"));
            }
            sink.finish()
                .await
                .unwrap_or_else(|e| panic!("{at}: the second finish: {e}"));
            assert_eq!(sink.pending_rows(), Some(0), "{at}");
            assert_eq!(ids(&client, &table).await, want, "{at}");
        }
    }
}

/// A dropped first flush may or may not have committed its `TRUNCATE`, so the
/// sink cannot know the target was cleared: the re-driven write clears it again
/// and the table ends with the new rows once and none of the old.
#[tokio::test]
async fn a_dropped_truncating_flush_still_truncates_once() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    for (n, micros) in DROP_AFTER_MICROS.into_iter().enumerate() {
        let table = format!("sweep_truncate_{n}");
        let at = format!("after {micros} us");
        create_keyless(&client, &table).await;
        client
            .batch_execute(&format!(
                "INSERT INTO {table} VALUES (900, 'old', 1), (901, 'old', 1)"
            ))
            .await
            .expect("seed");
        let mut sink = small_sink(&pg, &table, "append", "truncate_before_first_write #true\n");
        let schema = sink.schema();
        let own = small_batch(Arc::clone(&schema), &[(1, "a", 1), (2, "b", 2)]);
        let outcome = tokio::time::timeout(
            std::time::Duration::from_micros(micros),
            sink.write_batch(&own),
        )
        .await;
        match outcome {
            Ok(result) => result.unwrap_or_else(|e| panic!("{at}: {e}")),
            Err(_elapsed) => sink
                .write_batch(&own)
                .await
                .unwrap_or_else(|e| panic!("{at}: the re-drive failed: {e}")),
        }
        assert_eq!(ids(&client, &table).await, vec![1, 2], "{at}");
    }
}
