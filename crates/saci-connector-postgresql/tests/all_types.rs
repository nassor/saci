//! Every PostgreSQL type family the full-type-coverage work adds, read
//! through the cursor source, written through the sink, and (for four
//! families exercising the genuinely new code paths) read again through
//! `cdc_logical`'s text tuples.
//!
//! One `#[tokio::test]` per family, plus one per forced-`pg_type` case from
//! the design's declaration rules, so a failure names exactly which family or
//! rule broke. Every family follows the same shape: seed the source table
//! with SQL literals, drain it through a `mode kind="polling"` source and
//! assert the decoded values, then feed the very same batches into a sink
//! writing a second, identically-declared table, and assert the two tables
//! render identical `::text` for every declared column -- which sidesteps
//! having to hand-predict PostgreSQL's canonical text for the more exotic
//! types (composites, ranges, tsvector, geometrics, ...) since both sides are
//! compared against each other rather than against a guessed literal.
//!
//! Soft-skips without Docker; see `common::try_start`.

mod common;

use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    ListArray, RecordBatch, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use saci_connector::from_kdl_str;
use saci_connector_postgresql::{
    PostgresSink, PostgresSinkConfig, PostgresSource, PostgresSourceConfig,
};
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use serde::Deserialize as _;
use tokio_postgres::Client;

// -------------------------------------------------------------- shared helpers

/// Build a source config from a KDL fragment plus the container's DSN.
fn source(dsn: &str, body: &str) -> PostgresSource {
    let text = format!(
        "{body}\n\nconnection dsn={} sslmode=\"disable\"\n",
        common::quoted(dsn)
    );
    let cfg = PostgresSourceConfig::deserialize(from_kdl_str(&text).expect("parse kdl"))
        .expect("parse config");
    PostgresSource::new(cfg).expect("build source")
}

/// Build a sink config from a KDL fragment plus the container's DSN.
fn sink(dsn: &str, body: &str) -> PostgresSink {
    let text = format!(
        "{body}\n\nconnection dsn={} sslmode=\"disable\"\n",
        common::quoted(dsn)
    );
    let cfg = PostgresSinkConfig::deserialize(from_kdl_str(&text).expect("parse kdl"))
        .expect("parse config");
    PostgresSink::new(cfg).expect("build sink")
}

/// Drain one cycle, returning every batch until `Ok(None)`.
async fn drain(source: &mut PostgresSource) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    while let Some(batch) = source.next_batch().await.expect("next_batch") {
        batches.push(batch);
    }
    batches
}

/// Write every batch through `sink`, then finish.
async fn write_all(sink: &mut PostgresSink, batches: &[RecordBatch]) {
    for batch in batches {
        sink.write_batch(batch).await.expect("write");
    }
    sink.finish().await.expect("finish");
}

/// `col::text` for every row of `table`, ordered by `id`.
async fn text_column(client: &Client, table: &str, col: &str) -> Vec<Option<String>> {
    let sql = format!("SELECT \"{col}\"::text FROM {table} ORDER BY id");
    client
        .query(&sql, &[])
        .await
        .unwrap_or_else(|e| panic!("select {col} from {table}: {e}"))
        .iter()
        .map(|row| row.get::<_, Option<String>>(0))
        .collect()
}

/// `format('%s', col)` for every row of `table`, ordered by `id` -- the
/// type's own output function, which the cursor projects for a `Wire::Text`
/// column. Not always the same string as `col::text`: `inet`, `cidr`, `bool`
/// and `bpchar` each carry a distinct cast-to-`text` function of their own,
/// so `text_column` is the wrong oracle for those. Everything else (jsonb,
/// geometrics, ranges, enums, composites, tsvector, ...) has no such cast
/// and the two functions agree, so `text_column` remains fine there.
async fn output_column(client: &Client, table: &str, col: &str) -> Vec<Option<String>> {
    let sql = format!(
        "SELECT CASE WHEN \"{col}\" IS NULL THEN NULL ELSE pg_catalog.format('%s', \"{col}\") END FROM {table} ORDER BY id"
    );
    client
        .query(&sql, &[])
        .await
        .unwrap_or_else(|e| panic!("select {col} from {table}: {e}"))
        .iter()
        .map(|row| row.get::<_, Option<String>>(0))
        .collect()
}

/// Assert `table_a` and `table_b` render identical `::text` for every column
/// in `columns`, ordered by `id`. This is the one check every family's sink
/// step runs, and it needs no hand-predicted canonical text on either side.
async fn assert_tables_text_equal(client: &Client, table_a: &str, table_b: &str, columns: &[&str]) {
    for &col in columns {
        let a = text_column(client, table_a, col).await;
        let b = text_column(client, table_b, col).await;
        assert_eq!(a, b, "{table_a}.{col} vs {table_b}.{col}");
    }
}

/// A declared `utf8` column's values, in row order across every batch.
fn utf8_values(batches: &[RecordBatch], col: &str) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("{col} is not utf8"));
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row).to_string()))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn bool_values(batches: &[RecordBatch], col: &str) -> Vec<Option<bool>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap_or_else(|| panic!("{col} is not boolean"));
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn i16_values(batches: &[RecordBatch], col: &str) -> Vec<Option<i16>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap_or_else(|| panic!("{col} is not int16"));
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn i32_values(batches: &[RecordBatch], col: &str) -> Vec<Option<i32>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap_or_else(|| panic!("{col} is not int32"));
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn i64_values(batches: &[RecordBatch], col: &str) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("{col} is not int64"));
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn f32_values(batches: &[RecordBatch], col: &str) -> Vec<Option<f32>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap_or_else(|| panic!("{col} is not float32"));
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn f64_values(batches: &[RecordBatch], col: &str) -> Vec<Option<f64>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap_or_else(|| panic!("{col} is not float64"));
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn date32_values(batches: &[RecordBatch], col: &str) -> Vec<Option<i32>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap_or_else(|| panic!("{col} is not date32"));
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn time64_values(batches: &[RecordBatch], col: &str) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap_or_else(|| panic!("{col} is not time64_micros"));
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn timestamp_values(batches: &[RecordBatch], col: &str) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap_or_else(|| panic!("{col} is not a timestamp"));
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// `(months, days, nanoseconds)` per row of an `interval_month_day_nano`
/// column.
fn interval_values(batches: &[RecordBatch], col: &str) -> Vec<Option<(i32, i32, i64)>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .unwrap_or_else(|| panic!("{col} is not an interval"));
            (0..array.len())
                .map(|row| {
                    (!array.is_null(row)).then(|| {
                        let v = array.value(row);
                        (v.months, v.days, v.nanoseconds)
                    })
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The unscaled `i128` per row of a `decimal128` column, ignoring its scale
/// (the caller already knows what scale it declared).
fn decimal_values(batches: &[RecordBatch], col: &str) -> Vec<Option<i128>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap_or_else(|| panic!("{col} is not decimal128"));
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// One row's worth of a `list item="utf8"` column, as `Vec<Option<String>>`,
/// or `None` for a null array.
fn utf8_list_values(batches: &[RecordBatch], col: &str) -> Vec<Option<Vec<Option<String>>>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap_or_else(|| panic!("{col} is not a list"));
            let values = array
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("{col}'s element is not utf8"));
            (0..array.len())
                .map(|row| {
                    if array.is_null(row) {
                        return None;
                    }
                    let offsets = array.value_offsets();
                    let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
                    Some(
                        (start..end)
                            .map(|i| (!values.is_null(i)).then(|| values.value(i).to_string()))
                            .collect(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// One row's worth of a `list item="int32"` column.
fn i32_list_values(batches: &[RecordBatch], col: &str) -> Vec<Option<Vec<Option<i32>>>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col}"))
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap_or_else(|| panic!("{col} is not a list"));
            let values = array
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap_or_else(|| panic!("{col}'s element is not int32"));
            (0..array.len())
                .map(|row| {
                    if array.is_null(row) {
                        return None;
                    }
                    let offsets = array.value_offsets();
                    let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
                    Some(
                        (start..end)
                            .map(|i| (!values.is_null(i)).then(|| values.value(i)))
                            .collect(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

// ============================================================== 1. scalars

const SCALARS_DDL: &str = r#"
CREATE TABLE t_scalars_in (
    id bigint PRIMARY KEY, flag boolean, small smallint, medium integer,
    big bigint, r real, d double precision
);
CREATE TABLE t_scalars_out (LIKE t_scalars_in INCLUDING ALL);
INSERT INTO t_scalars_in VALUES
  (1, true, 100, 100000, 10000000000, 12.5, 98765.432109),
  (2, NULL, NULL, NULL, NULL, NULL, NULL),
  (3, false, -32768, -2147483648, -9223372036854775808, 3.4028235e38, 1.7976931348623157e308);
"#;

const SCALARS_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"flag\" type=\"boolean\"
schema_fields \"small\" type=\"int16\"
schema_fields \"medium\" type=\"int32\"
schema_fields \"big\" type=\"int64\"
schema_fields \"r\" type=\"float32\"
schema_fields \"d\" type=\"float64\"
";

fn assert_scalars(batches: &[RecordBatch]) {
    assert_eq!(i64_values(batches, "id"), vec![Some(1), Some(2), Some(3)]);
    assert_eq!(
        bool_values(batches, "flag"),
        vec![Some(true), None, Some(false)]
    );
    assert_eq!(
        i16_values(batches, "small"),
        vec![Some(100), None, Some(-32768)]
    );
    assert_eq!(
        i32_values(batches, "medium"),
        vec![Some(100000), None, Some(-2147483648)]
    );
    assert_eq!(
        i64_values(batches, "big"),
        vec![Some(10000000000), None, Some(-9223372036854775808)]
    );
    assert_eq!(
        f32_values(batches, "r"),
        vec![Some(12.5_f32), None, Some(3.4028235e38_f32)]
    );
    assert_eq!(
        f64_values(batches, "d"),
        vec![
            Some(98765.432109_f64),
            None,
            Some(1.7976931348623157e308_f64)
        ]
    );
}

#[tokio::test]
async fn scalars_round_trip_and_cdc_logical_text_decode_agree() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(SCALARS_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"scalars_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_scalars_in\" cursor_column=\"id\"\n{SCALARS_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_scalars(&batches);

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"scalars_out\"\ntable \"t_scalars_out\"\n{SCALARS_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_scalars_in",
        "t_scalars_out",
        &["id", "flag", "small", "medium", "big", "r", "d"],
    )
    .await;

    // cdc_logical: the same six scalars, but every value now decodes from a
    // pgoutput *text* tuple instead of the binary wire (design's regression
    // concern: these six types already worked before this change).
    client
        .batch_execute(
            "CREATE TABLE t_scalars_logical (LIKE t_scalars_in INCLUDING ALL); \
             CREATE PUBLICATION scalars_pub FOR TABLE t_scalars_logical",
        )
        .await
        .expect("logical ddl");
    let mut logical = source(
        &pg.dsn(),
        &format!(
            "name \"scalars_logical\"\nbatch_rows 100\n\n\
             mode kind=\"cdc_logical\" slot=\"scalars_slot\" publication=\"scalars_pub\" table=\"t_scalars_logical\"\n\
             {SCALARS_FIELDS}"
        ),
    );
    assert!(drain(&mut logical).await.is_empty(), "slot creation cycle");
    client
        .batch_execute(
            "INSERT INTO t_scalars_logical VALUES \
             (1, true, 100, 100000, 10000000000, 12.5, 98765.432109), \
             (2, NULL, NULL, NULL, NULL, NULL, NULL), \
             (3, false, -32768, -2147483648, -9223372036854775808, 3.4028235e38, 1.7976931348623157e308)",
        )
        .await
        .expect("insert");
    let logical_batches = drain(&mut logical).await;
    assert_scalars(&logical_batches);
}

// ======================================================= 2. numeric / money

const NUMERIC_DDL: &str = r#"
CREATE TABLE t_numeric_in (id bigint PRIMARY KEY, unc numeric, fixed numeric(12,2), price money);
CREATE TABLE t_numeric_out (LIKE t_numeric_in INCLUDING ALL);
INSERT INTO t_numeric_in VALUES
  (1, 123.456, 123.40, 1234.56),
  (2, NULL, NULL, NULL),
  (3, 'NaN', 9999999999.99, -92233720368547758.08);
"#;

const NUMERIC_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"unc\" type=\"utf8\"
schema_fields \"fixed\" type=\"decimal128\" precision=12 scale=2
schema_fields \"price\" type=\"decimal128\" precision=19 scale=2
";

#[tokio::test]
async fn numeric_and_money_round_trip() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(NUMERIC_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"numeric_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_numeric_in\" cursor_column=\"id\"\n{NUMERIC_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        utf8_values(&batches, "unc"),
        vec![Some("123.456".to_string()), None, Some("NaN".to_string())]
    );
    assert_eq!(
        decimal_values(&batches, "fixed"),
        vec![Some(12340), None, Some(999999999999)]
    );
    assert_eq!(
        decimal_values(&batches, "price"),
        vec![Some(123456), None, Some(-9223372036854775808)]
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"numeric_out\"\ntable \"t_numeric_out\"\n{NUMERIC_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_numeric_in",
        "t_numeric_out",
        &["id", "unc", "fixed", "price"],
    )
    .await;
}

// ============================================================ 3. temporal

const TEMPORAL_DDL: &str = r#"
CREATE TABLE t_temporal_in (id bigint PRIMARY KEY, d date, t time, ts timestamp, tstz timestamptz);
CREATE TABLE t_temporal_out (LIKE t_temporal_in INCLUDING ALL);
INSERT INTO t_temporal_in VALUES
  (1, '2024-01-02', '04:05:06.789123', '2024-01-02 03:04:05.123456', '2024-01-02 03:04:05.123456+02'),
  (2, NULL, NULL, NULL, NULL),
  (3, '9999-12-31', '24:00:00', '9999-12-31 23:59:59.999999', '9999-12-31 23:59:59.999999+00');
"#;

const TEMPORAL_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"d\" type=\"date32\"
schema_fields \"t\" type=\"time64_micros\"
schema_fields \"ts\" type=\"timestamp_micros\"
schema_fields \"tstz\" type=\"timestamp_micros_utc\"
";

fn assert_temporal(batches: &[RecordBatch]) {
    assert_eq!(
        date32_values(batches, "d"),
        vec![Some(19724), None, Some(2932896)]
    );
    assert_eq!(
        time64_values(batches, "t"),
        vec![Some(14706789123), None, Some(86400000000)]
    );
    assert_eq!(
        timestamp_values(batches, "ts"),
        vec![Some(1704164645123456), None, Some(253402300799999999)]
    );
    assert_eq!(
        timestamp_values(batches, "tstz"),
        vec![Some(1704157445123456), None, Some(253402300799999999)]
    );
}

#[tokio::test]
async fn temporal_types_round_trip_and_cdc_logical_agree() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(TEMPORAL_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"temporal_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_temporal_in\" cursor_column=\"id\"\n{TEMPORAL_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_temporal(&batches);

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"temporal_out\"\ntable \"t_temporal_out\"\n{TEMPORAL_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_temporal_in",
        "t_temporal_out",
        &["id", "d", "t", "ts", "tstz"],
    )
    .await;

    // The BC era and the infinity sentinels have no Arrow representation and
    // are refused by name rather than misparsed -- but that check lives in
    // the *text* parser (`push_text`), and a plain `type="date32"` over a
    // real `date` column takes the binary route on the cursor (no BC concept
    // there: a negative day count decodes as an ordinary early date). Every
    // `cdc_logical` value is a text tuple unconditionally, so that path is
    // the one place a bare `date32` declaration actually reaches the BC
    // refusal.
    client
        .batch_execute(
            "CREATE TABLE t_temporal_bc (id bigint PRIMARY KEY, d date); \
             CREATE PUBLICATION temporal_bc_pub FOR TABLE t_temporal_bc",
        )
        .await
        .expect("bc ddl");
    let mut bc_src = source(
        &pg.dsn(),
        "name \"temporal_bc\"\nbatch_rows 100\n\nmode kind=\"cdc_logical\" slot=\"temporal_bc_slot\" publication=\"temporal_bc_pub\" table=\"t_temporal_bc\"\nschema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"d\" type=\"date32\"",
    );
    assert!(drain(&mut bc_src).await.is_empty(), "slot creation cycle");
    client
        .batch_execute("INSERT INTO t_temporal_bc VALUES (1, '2024-01-01 BC')")
        .await
        .expect("insert bc row");
    let err = bc_src.next_batch().await.expect_err("BC era is refused");
    assert!(err.message().contains("BC"), "{}", err.message());

    // cdc_logical: temporal text tuples, including the same values above.
    client
        .batch_execute(
            "CREATE TABLE t_temporal_logical (LIKE t_temporal_in INCLUDING ALL); \
             CREATE PUBLICATION temporal_pub FOR TABLE t_temporal_logical",
        )
        .await
        .expect("logical ddl");
    let mut logical = source(
        &pg.dsn(),
        &format!(
            "name \"temporal_logical\"\nbatch_rows 100\n\n\
             mode kind=\"cdc_logical\" slot=\"temporal_slot\" publication=\"temporal_pub\" table=\"t_temporal_logical\"\n\
             {TEMPORAL_FIELDS}"
        ),
    );
    assert!(drain(&mut logical).await.is_empty());
    client
        .batch_execute(
            "INSERT INTO t_temporal_logical VALUES \
             (1, '2024-01-02', '04:05:06.789123', '2024-01-02 03:04:05.123456', '2024-01-02 03:04:05.123456+02'), \
             (2, NULL, NULL, NULL, NULL), \
             (3, '9999-12-31', '24:00:00', '9999-12-31 23:59:59.999999', '9999-12-31 23:59:59.999999+00')",
        )
        .await
        .expect("insert");
    assert_temporal(&drain(&mut logical).await);
}

// ============================================================ 4. interval

const INTERVAL_DDL: &str = r#"
CREATE TABLE t_interval_in (id bigint PRIMARY KEY, iv interval);
CREATE TABLE t_interval_out (LIKE t_interval_in INCLUDING ALL);
INSERT INTO t_interval_in VALUES
  (1, '1 year 2 mons 3 days 04:05:06.789'),
  (2, NULL),
  (3, '-1 year -2 mons 3 days -04:05:06');
"#;

const INTERVAL_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"iv\" type=\"interval_month_day_nano\"
";

fn assert_interval(batches: &[RecordBatch]) {
    assert_eq!(
        interval_values(batches, "iv"),
        vec![
            Some((14, 3, 14706789000000)),
            None,
            Some((-14, 3, -14706000000000)),
        ]
    );
}

#[tokio::test]
async fn interval_round_trips_and_refuses_sub_microsecond_and_overflow() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(INTERVAL_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"interval_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_interval_in\" cursor_column=\"id\"\n{INTERVAL_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_interval(&batches);

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"interval_out\"\ntable \"t_interval_out\"\n{INTERVAL_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(&client, "t_interval_in", "t_interval_out", &["id", "iv"]).await;

    // cdc_logical: the `postgres`-style parser, including the pluralization
    // and mixed-sign quirks the matrix verified live (`-1 years`, not
    // `-1 year`).
    client
        .batch_execute(
            "CREATE TABLE t_interval_logical (LIKE t_interval_in INCLUDING ALL); \
             CREATE PUBLICATION interval_pub FOR TABLE t_interval_logical",
        )
        .await
        .expect("logical ddl");
    let mut logical = source(
        &pg.dsn(),
        &format!(
            "name \"interval_logical\"\nbatch_rows 100\n\n\
             mode kind=\"cdc_logical\" slot=\"interval_slot\" publication=\"interval_pub\" table=\"t_interval_logical\"\n\
             {INTERVAL_FIELDS}"
        ),
    );
    assert!(drain(&mut logical).await.is_empty());
    client
        .batch_execute(
            "INSERT INTO t_interval_logical VALUES \
             (1, '1 year 2 mons 3 days 04:05:06.789'), \
             (2, NULL), \
             (3, '-1 year -2 mons 3 days -04:05:06')",
        )
        .await
        .expect("insert");
    assert_interval(&drain(&mut logical).await);
}

// ========================================================= 5. text family

const TEXT_FAMILY_DDL: &str = r#"
CREATE EXTENSION IF NOT EXISTS citext;
CREATE TABLE t_text_in (
    id bigint PRIMARY KEY, t text, vc varchar(10), bp bpchar(4), nm name, ch "char", ci citext
);
CREATE TABLE t_text_out (LIKE t_text_in INCLUDING ALL);
INSERT INTO t_text_in VALUES
  (1, 'hello', 'hi', 'hi', 'hi', 'x', 'HeLLo'),
  (2, NULL, NULL, NULL, NULL, NULL, NULL),
  (3, 'quote " and comma , here', '1234567890', 'wxyz', 'another_name', 'y', 'MiXeD');
"#;

const TEXT_FAMILY_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"t\" type=\"utf8\"
schema_fields \"vc\" type=\"utf8\"
schema_fields \"bp\" type=\"utf8\"
schema_fields \"nm\" type=\"utf8\"
schema_fields \"ch\" type=\"utf8\" pg_type=\"\\\"char\\\"\"
schema_fields \"ci\" type=\"utf8\" pg_type=\"citext\"
";

#[tokio::test]
async fn text_family_including_extension_types_round_trips() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(TEXT_FAMILY_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"text_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_text_in\" cursor_column=\"id\"\n{TEXT_FAMILY_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        utf8_values(&batches, "t"),
        vec![
            Some("hello".to_string()),
            None,
            Some("quote \" and comma , here".to_string())
        ]
    );
    assert_eq!(
        utf8_values(&batches, "vc"),
        vec![Some("hi".to_string()), None, Some("1234567890".to_string())]
    );
    assert_eq!(
        utf8_values(&batches, "bp"),
        vec![Some("hi  ".to_string()), None, Some("wxyz".to_string())],
        "bpchar's binary wire carries the physical space padding; only its \
         text OUTPUT function trims it"
    );
    assert_eq!(
        utf8_values(&batches, "ci"),
        vec![Some("HeLLo".to_string()), None, Some("MiXeD".to_string())],
        "citext preserves case on output"
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"text_out\"\ntable \"t_text_out\"\n{TEXT_FAMILY_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_text_in",
        "t_text_out",
        &["id", "t", "vc", "bp", "nm", "ch", "ci"],
    )
    .await;
}

/// `"char"` (oid 18) is one of two canonical type names that are also SQL
/// *keywords* carrying a length default of 1, `bit` being the other: an
/// unquoted `char` cast target means `character(1)`, which would keep the
/// leading `\` of the four-character `\310` the output function renders for a
/// high-bit byte. The sink's cast target is quoted and `pg_catalog`
/// qualified, so the value survives the staging route intact.
#[tokio::test]
async fn a_high_bit_char_value_survives_the_sink_cast() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            r#"CREATE TABLE t_char_in (id bigint PRIMARY KEY, ch "char");
               CREATE TABLE t_char_out (LIKE t_char_in INCLUDING ALL);
               INSERT INTO t_char_in VALUES (1, '\310'), (2, 'x'), (3, NULL);"#,
        )
        .await
        .expect("ddl");

    let fields = "schema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"ch\" type=\"utf8\" pg_type=\"\\\"char\\\"\"";
    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"char_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_char_in\" cursor_column=\"id\"\n{fields}"
        ),
    );
    let batches = drain(&mut src).await;
    let expected = vec![Some("\\310".to_string()), Some("x".to_string()), None];
    assert_eq!(
        utf8_values(&batches, "ch"),
        expected,
        "charout renders a high-bit byte as its four-character octal escape"
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"char_out\"\ntable \"t_char_out\"\n{fields}"),
    );
    write_all(&mut snk, &batches).await;
    // Read back through the output function, not `::text`: the cast from
    // `"char"` to `text` hands back the raw byte, which is not valid UTF-8.
    assert_eq!(output_column(&client, "t_char_out", "ch").await, expected);
}

// ==================================================== 6. json/jsonb/jsonpath/xml

const JSON_FAMILY_DDL: &str = r#"
CREATE TABLE t_json_in (id bigint PRIMARY KEY, j json, jb jsonb, jp jsonpath, x xml);
CREATE TABLE t_json_out (LIKE t_json_in INCLUDING ALL);
INSERT INTO t_json_in VALUES
  (1, '{"b":2,"a":1}', '{"b":2,"a":1}', '$.a.b[0]', '<a><b>1</b></a>'),
  (2, NULL, NULL, NULL, NULL),
  (3, '[1,2,3]', '{"reading":1.230e-5}', '$.tags[*]', '<foo>bar</foo>');
"#;

const JSON_FAMILY_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"j\" type=\"json\"
schema_fields \"jb\" type=\"json\"
schema_fields \"jp\" type=\"utf8\"
schema_fields \"x\" type=\"utf8\"
";

#[tokio::test]
async fn json_jsonb_jsonpath_and_xml_round_trip() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(JSON_FAMILY_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"json_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_json_in\" cursor_column=\"id\"\n{JSON_FAMILY_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    // json echoes verbatim.
    assert_eq!(
        utf8_values(&batches, "j"),
        vec![
            Some("{\"b\":2,\"a\":1}".to_string()),
            None,
            Some("[1,2,3]".to_string())
        ]
    );
    // jsonb reorders keys and reformats numbers; compare against the live
    // server rather than a hand-typed guess.
    assert_eq!(
        utf8_values(&batches, "jb"),
        text_column(&client, "t_json_in", "jb").await
    );
    assert_eq!(
        utf8_values(&batches, "jp"),
        text_column(&client, "t_json_in", "jp").await
    );
    assert_eq!(
        utf8_values(&batches, "x"),
        text_column(&client, "t_json_in", "x").await
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"json_out\"\ntable \"t_json_out\"\n{JSON_FAMILY_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_json_in",
        "t_json_out",
        &["id", "j", "jb", "jp", "x"],
    )
    .await;
}

// ============================================================== 7. network

const NETWORK_DDL: &str = r#"
CREATE TABLE t_network_in (id bigint PRIMARY KEY, ip inet, net cidr, mac macaddr, m8 macaddr8);
CREATE TABLE t_network_out (LIKE t_network_in INCLUDING ALL);
INSERT INTO t_network_in VALUES
  (1, '192.168.0.1/24', '192.168.100.0/24', '08:00:2b:01:02:03', '08:00:2b:01:02:03'),
  (2, NULL, NULL, NULL, NULL),
  (3, '::1', NULL, NULL, NULL);
"#;

const NETWORK_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"ip\" type=\"utf8\"
schema_fields \"net\" type=\"utf8\"
schema_fields \"mac\" type=\"utf8\"
schema_fields \"m8\" type=\"utf8\"
";

fn assert_network(batches: &[RecordBatch], expected_ip: &[Option<String>]) {
    assert_eq!(utf8_values(batches, "ip"), expected_ip);
}

#[tokio::test]
async fn network_types_round_trip_and_cdc_logical_agrees() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(NETWORK_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"network_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_network_in\" cursor_column=\"id\"\n{NETWORK_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    let expected_ip = output_column(&client, "t_network_in", "ip").await;
    assert_eq!(
        expected_ip,
        vec![
            Some("192.168.0.1/24".to_string()),
            None,
            Some("::1".to_string())
        ],
        "inet_out suppresses a host address's full-width netmask, unlike the ::text cast"
    );
    assert_network(&batches, &expected_ip);
    assert_eq!(
        utf8_values(&batches, "mac"),
        text_column(&client, "t_network_in", "mac").await
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"network_out\"\ntable \"t_network_out\"\n{NETWORK_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_network_in",
        "t_network_out",
        &["id", "ip", "net", "mac", "m8"],
    )
    .await;

    client
        .batch_execute(
            "CREATE TABLE t_network_logical (LIKE t_network_in INCLUDING ALL); \
             CREATE PUBLICATION network_pub FOR TABLE t_network_logical",
        )
        .await
        .expect("logical ddl");
    let mut logical = source(
        &pg.dsn(),
        &format!(
            "name \"network_logical\"\nbatch_rows 100\n\n\
             mode kind=\"cdc_logical\" slot=\"network_slot\" publication=\"network_pub\" table=\"t_network_logical\"\n\
             {NETWORK_FIELDS}"
        ),
    );
    assert!(drain(&mut logical).await.is_empty());
    client
        .batch_execute(
            "INSERT INTO t_network_logical VALUES \
             (1, '192.168.0.1/24', '192.168.100.0/24', '08:00:2b:01:02:03', '08:00:2b:01:02:03'), \
             (2, NULL, NULL, NULL, NULL), \
             (3, '::1', NULL, NULL, NULL)",
        )
        .await
        .expect("insert");
    let logical_batches = drain(&mut logical).await;
    assert_network(&logical_batches, &expected_ip);
}

// ============================================================= 8. geometric

const GEOMETRIC_DDL: &str = r#"
CREATE TABLE t_geometric_in (
    id bigint PRIMARY KEY, pt point, ln line, seg lseg, bx box, p_c path, poly polygon, c circle
);
CREATE TABLE t_geometric_out (LIKE t_geometric_in INCLUDING ALL);
INSERT INTO t_geometric_in VALUES
  (1, '(1,2)', '{1,2,3}', '((1,2),(3,4))', '((1,2),(3,4))', '((1,2),(3,4),(5,6))', '((0,0),(1,0),(1,1))', '<(1,2),3>'),
  (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (3, '(5,6)', '{2,3,4}', '((5,6),(7,8))', '((5,6),(7,8))', '((5,6),(7,8),(9,10))', '((0,0),(2,0),(2,2))', '<(5,6),4>');
"#;

const GEOMETRIC_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"pt\" type=\"utf8\"
schema_fields \"ln\" type=\"utf8\"
schema_fields \"seg\" type=\"utf8\"
schema_fields \"bx\" type=\"utf8\"
schema_fields \"p_c\" type=\"utf8\"
schema_fields \"poly\" type=\"utf8\"
schema_fields \"c\" type=\"utf8\"
";

#[tokio::test]
async fn geometric_types_round_trip() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(GEOMETRIC_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"geometric_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_geometric_in\" cursor_column=\"id\"\n{GEOMETRIC_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    for col in ["pt", "ln", "seg", "bx", "p_c", "poly", "c"] {
        assert_eq!(
            utf8_values(&batches, col),
            text_column(&client, "t_geometric_in", col).await,
            "column {col}"
        );
    }
    // The point value is the one geometric type verified live in the matrix.
    assert_eq!(
        utf8_values(&batches, "pt"),
        vec![Some("(1,2)".to_string()), None, Some("(5,6)".to_string())]
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"geometric_out\"\ntable \"t_geometric_out\"\n{GEOMETRIC_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_geometric_in",
        "t_geometric_out",
        &["id", "pt", "ln", "seg", "bx", "p_c", "poly", "c"],
    )
    .await;
}

// ============================================================ 9. bit/varbit

const BIT_DDL: &str = r#"
CREATE TABLE t_bit_in (id bigint PRIMARY KEY, b bit(3), vb bit varying(5));
CREATE TABLE t_bit_out (LIKE t_bit_in INCLUDING ALL);
INSERT INTO t_bit_in VALUES
  (1, B'101', B'00'),
  (2, NULL, NULL),
  (3, B'100', B'');
"#;

const BIT_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"b\" type=\"utf8\"
schema_fields \"vb\" type=\"utf8\"
";

#[tokio::test]
async fn bit_and_varbit_round_trip_including_empty() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(BIT_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"bit_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_bit_in\" cursor_column=\"id\"\n{BIT_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        utf8_values(&batches, "b"),
        vec![Some("101".to_string()), None, Some("100".to_string())]
    );
    assert_eq!(
        utf8_values(&batches, "vb"),
        vec![Some("00".to_string()), None, Some(String::new())],
        "an empty varbit renders as an empty string, not NULL"
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"bit_out\"\ntable \"t_bit_out\"\n{BIT_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(&client, "t_bit_in", "t_bit_out", &["id", "b", "vb"]).await;
}

// ============================================================== 10. ranges

const RANGES_DDL: &str = r#"
CREATE TABLE t_ranges_in (
    id bigint PRIMARY KEY, ir int4range, nr numrange, dr daterange, tsr tsrange, tstzr tstzrange
);
CREATE TABLE t_ranges_out (LIKE t_ranges_in INCLUDING ALL);
INSERT INTO t_ranges_in VALUES
  (1, '[3,7)', '[1.5,2.5)', '[2024-01-01,2024-02-01)',
      '["2024-01-01 00:00:00","2024-02-01 00:00:00")',
      '["2024-01-01 00:00:00+00","2024-02-01 00:00:00+00")'),
  (2, NULL, NULL, NULL, NULL, NULL),
  (3, 'empty', '(,)', NULL, NULL, NULL);
"#;

const RANGES_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"ir\" type=\"utf8\"
schema_fields \"nr\" type=\"utf8\"
schema_fields \"dr\" type=\"utf8\"
schema_fields \"tsr\" type=\"utf8\"
schema_fields \"tstzr\" type=\"utf8\"
";

#[tokio::test]
async fn range_types_round_trip_with_canonicalization() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(RANGES_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"ranges_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_ranges_in\" cursor_column=\"id\"\n{RANGES_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        utf8_values(&batches, "ir"),
        vec![Some("[3,7)".to_string()), None, Some("empty".to_string())]
    );
    assert_eq!(
        utf8_values(&batches, "nr"),
        vec![Some("[1.5,2.5)".to_string()), None, Some("(,)".to_string())]
    );
    for col in ["dr", "tsr", "tstzr"] {
        assert_eq!(
            utf8_values(&batches, col),
            text_column(&client, "t_ranges_in", col).await,
            "column {col}"
        );
    }

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"ranges_out\"\ntable \"t_ranges_out\"\n{RANGES_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_ranges_in",
        "t_ranges_out",
        &["id", "ir", "nr", "dr", "tsr", "tstzr"],
    )
    .await;
}

// ========================================================= 11. multiranges

const MULTIRANGES_DDL: &str = r#"
CREATE TABLE t_multiranges_in (id bigint PRIMARY KEY, im int4multirange, nm nummultirange);
CREATE TABLE t_multiranges_out (LIKE t_multiranges_in INCLUDING ALL);
INSERT INTO t_multiranges_in VALUES
  (1, '{[3,7),[8,9)}', '{[1.5,2.5)}'),
  (2, NULL, NULL),
  (3, '{}', '{}');
"#;

const MULTIRANGES_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"im\" type=\"utf8\"
schema_fields \"nm\" type=\"utf8\"
";

#[tokio::test]
async fn multirange_types_round_trip() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(MULTIRANGES_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"multiranges_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_multiranges_in\" cursor_column=\"id\"\n{MULTIRANGES_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        utf8_values(&batches, "im"),
        vec![
            Some("{[3,7),[8,9)}".to_string()),
            None,
            Some("{}".to_string())
        ]
    );
    assert_eq!(
        utf8_values(&batches, "nm"),
        vec![
            Some("{[1.5,2.5)}".to_string()),
            None,
            Some("{}".to_string())
        ]
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"multiranges_out\"\ntable \"t_multiranges_out\"\n{MULTIRANGES_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_multiranges_in",
        "t_multiranges_out",
        &["id", "im", "nm"],
    )
    .await;
}

// ===================================================== 12. tsvector/tsquery

const TSVECTOR_DDL: &str = r#"
CREATE TABLE t_tsvector_in (id bigint PRIMARY KEY, tv tsvector, tq tsquery);
CREATE TABLE t_tsvector_out (LIKE t_tsvector_in INCLUDING ALL);
INSERT INTO t_tsvector_in VALUES
  (1, $$a fat cat sat on a mat and ate a fat rat$$, 'fat & (rat | cat)'),
  (2, NULL, NULL),
  (3, '', 'fat:AB & cat');
"#;

const TSVECTOR_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"tv\" type=\"utf8\"
schema_fields \"tq\" type=\"utf8\"
";

#[tokio::test]
async fn tsvector_and_tsquery_round_trip() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(TSVECTOR_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"tsvector_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_tsvector_in\" cursor_column=\"id\"\n{TSVECTOR_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        utf8_values(&batches, "tv"),
        text_column(&client, "t_tsvector_in", "tv").await
    );
    assert_eq!(
        utf8_values(&batches, "tq"),
        text_column(&client, "t_tsvector_in", "tq").await
    );
    assert_eq!(
        utf8_values(&batches, "tv"),
        vec![
            Some("'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat'".to_string()),
            None,
            Some(String::new()),
        ]
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"tsvector_out\"\ntable \"t_tsvector_out\"\n{TSVECTOR_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_tsvector_in",
        "t_tsvector_out",
        &["id", "tv", "tq"],
    )
    .await;
}

// ================================================ 13. pg_lsn/oid-family/reg*

const LSN_OID_DDL: &str = r#"
CREATE TABLE t_lsn_oid_in (id bigint PRIMARY KEY, lsn pg_lsn, o oid, rc regclass, rt regtype);
CREATE TABLE t_lsn_oid_out (LIKE t_lsn_oid_in INCLUDING ALL);
INSERT INTO t_lsn_oid_in VALUES
  (1, '16/B374D848', 564182, 'pg_type', 'int4'),
  (2, NULL, NULL, NULL, NULL),
  (3, '0/0', 4294967295, 'pg_class', 'text');
"#;

const LSN_OID_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"lsn\" type=\"utf8\"
schema_fields \"o\" type=\"int64\"
schema_fields \"rc\" type=\"utf8\"
schema_fields \"rt\" type=\"utf8\"
";

#[tokio::test]
async fn lsn_oid_family_and_reg_types_round_trip() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(LSN_OID_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"lsn_oid_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_lsn_oid_in\" cursor_column=\"id\"\n{LSN_OID_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        utf8_values(&batches, "lsn"),
        vec![
            Some("16/B374D848".to_string()),
            None,
            Some("0/0".to_string())
        ]
    );
    assert_eq!(
        i64_values(&batches, "o"),
        vec![Some(564182), None, Some(4294967295)]
    );
    assert_eq!(
        utf8_values(&batches, "rc"),
        vec![
            Some("pg_type".to_string()),
            None,
            Some("pg_class".to_string())
        ]
    );
    assert_eq!(
        utf8_values(&batches, "rt"),
        vec![Some("integer".to_string()), None, Some("text".to_string())]
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"lsn_oid_out\"\ntable \"t_lsn_oid_out\"\n{LSN_OID_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_lsn_oid_in",
        "t_lsn_oid_out",
        &["id", "lsn", "o", "rc", "rt"],
    )
    .await;
}

// ============================================================ 14. uuid/bytea

const UUID_BYTEA_DDL: &str = r#"
CREATE TABLE t_uuidbytea_in (id bigint PRIMARY KEY, u uuid, b bytea);
CREATE TABLE t_uuidbytea_out (LIKE t_uuidbytea_in INCLUDING ALL);
INSERT INTO t_uuidbytea_in VALUES
  (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\x00ff'),
  (2, NULL, NULL),
  (3, '11111111-2222-4333-8444-555555555555', '\x');
"#;

const UUID_BYTEA_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"u\" type=\"uuid\"
schema_fields \"b\" type=\"binary\"
";

#[tokio::test]
async fn uuid_and_bytea_edge_cases_round_trip() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(UUID_BYTEA_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"uuidbytea_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_uuidbytea_in\" cursor_column=\"id\"\n{UUID_BYTEA_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    let batch = &batches[0];
    let u = batch
        .column_by_name("u")
        .expect("u column")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("uuid column");
    assert_eq!(
        u.value(0),
        [
            0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38,
            0x0a, 0x11
        ]
    );
    assert!(u.is_null(1));
    assert_eq!(
        u.value(2),
        [
            0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x43, 0x33, 0x84, 0x44, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55
        ]
    );
    let b = batch
        .column_by_name("b")
        .expect("b column")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("bytea column");
    assert_eq!(b.value(0), &[0x00u8, 0xff]);
    assert!(b.is_null(1));
    assert_eq!(
        b.value(2),
        &[] as &[u8],
        "an empty bytea decodes as zero bytes, not the two-character text form \\x"
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"uuidbytea_out\"\ntable \"t_uuidbytea_out\"\n{UUID_BYTEA_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_uuidbytea_in",
        "t_uuidbytea_out",
        &["id", "u", "b"],
    )
    .await;
}

// ================================================================ 15. hstore

const HSTORE_DDL: &str = r#"
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE TABLE t_hstore_in (id bigint PRIMARY KEY, h hstore);
CREATE TABLE t_hstore_out (LIKE t_hstore_in INCLUDING ALL);
INSERT INTO t_hstore_in VALUES
  (1, 'a=>1, b=>NULL, "c d"=>"e,f"'),
  (2, NULL),
  (3, '');
"#;

const HSTORE_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"h\" type=\"utf8\" pg_type=\"hstore\"
";

#[tokio::test]
async fn hstore_round_trips_including_empty() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(HSTORE_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"hstore_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_hstore_in\" cursor_column=\"id\"\n{HSTORE_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        utf8_values(&batches, "h"),
        vec![
            Some("\"a\"=>\"1\", \"b\"=>NULL, \"c d\"=>\"e,f\"".to_string()),
            None,
            Some(String::new()),
        ]
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"hstore_out\"\ntable \"t_hstore_out\"\n{HSTORE_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(&client, "t_hstore_in", "t_hstore_out", &["id", "h"]).await;
}

// ================================================ 16. enum + domain + composite

const ENUM_DOMAIN_DDL: &str = r#"
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE DOMAIN posint AS integer CHECK (VALUE > 0);
CREATE TYPE full_name AS (first text, last text);
CREATE TABLE t_enumdom_in (id bigint PRIMARY KEY, m mood, p posint, fn full_name);
CREATE TABLE t_enumdom_out (LIKE t_enumdom_in INCLUDING ALL);
INSERT INTO t_enumdom_in VALUES
  (1, 'happy', 5, ROW('Ann', 'Lee')),
  (2, NULL, NULL, NULL),
  (3, 'sad', 1, ROW('Bob', NULL));
"#;

const ENUM_DOMAIN_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"m\" type=\"utf8\" pg_type=\"public.mood\"
schema_fields \"p\" type=\"int32\" pg_type=\"public.posint\"
schema_fields \"fn\" type=\"utf8\" pg_type=\"public.full_name\"
";

#[tokio::test]
async fn enum_domain_and_composite_round_trip() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(ENUM_DOMAIN_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"enumdom_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_enumdom_in\" cursor_column=\"id\"\n{ENUM_DOMAIN_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        utf8_values(&batches, "m"),
        vec![Some("happy".to_string()), None, Some("sad".to_string())]
    );
    assert_eq!(
        i32_values(&batches, "p"),
        vec![Some(5), None, Some(1)],
        "a domain over int4 decodes as int32, with no domain-specific decoration"
    );
    assert_eq!(
        utf8_values(&batches, "fn"),
        text_column(&client, "t_enumdom_in", "fn").await
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"enumdom_out\"\ntable \"t_enumdom_out\"\n{ENUM_DOMAIN_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_enumdom_in",
        "t_enumdom_out",
        &["id", "m", "p", "fn"],
    )
    .await;
}

// ============================================================== 17. arrays

const ARRAYS_DDL: &str = r#"
CREATE TABLE t_arrays_in (id bigint PRIMARY KEY, nums int4[], tags text[], nums2 int4[]);
CREATE TABLE t_arrays_out (LIKE t_arrays_in INCLUDING ALL);
INSERT INTO t_arrays_in VALUES
  (1, ARRAY[1,2,-2147483648], ARRAY['a','b,c','with "quote"'], ARRAY[10,20,30]),
  (2, NULL, NULL, NULL),
  (3, ARRAY[]::int4[], ARRAY[]::text[], ARRAY[]::int4[]);
"#;

const ARRAYS_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"nums\" type=\"list\" item=\"int32\"
schema_fields \"tags\" type=\"list\" item=\"utf8\"
schema_fields \"nums2\" type=\"list\" item=\"utf8\" pg_type=\"int4[]\"
";

#[tokio::test]
async fn arrays_round_trip_native_and_forced_text_element() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client.batch_execute(ARRAYS_DDL).await.expect("ddl");

    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"arrays_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_arrays_in\" cursor_column=\"id\"\n{ARRAYS_FIELDS}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        i32_list_values(&batches, "nums"),
        vec![
            Some(vec![Some(1), Some(2), Some(-2147483648)]),
            None,
            Some(vec![]),
        ]
    );
    assert_eq!(
        utf8_list_values(&batches, "tags"),
        vec![
            Some(vec![
                Some("a".to_string()),
                Some("b,c".to_string()),
                Some("with \"quote\"".to_string())
            ]),
            None,
            Some(vec![]),
        ]
    );
    assert_eq!(
        utf8_list_values(&batches, "nums2"),
        vec![
            Some(vec![
                Some("10".to_string()),
                Some("20".to_string()),
                Some("30".to_string())
            ]),
            None,
            Some(vec![]),
        ],
        "item = \"utf8\" over a non-text element (int4[]) needs pg_type naming the array"
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"arrays_out\"\ntable \"t_arrays_out\"\n{ARRAYS_FIELDS}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_arrays_in",
        "t_arrays_out",
        &["id", "nums", "tags", "nums2"],
    )
    .await;
}

// ============================================================= forced cases

/// Matrix case: `type = "utf8" pg_type = "jsonb"`, on both halves.
#[tokio::test]
async fn forced_utf8_pg_type_jsonb_both_halves() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE t_forced_jsonb_in (id bigint PRIMARY KEY, payload jsonb); \
             CREATE TABLE t_forced_jsonb_out (LIKE t_forced_jsonb_in INCLUDING ALL); \
             INSERT INTO t_forced_jsonb_in VALUES (1, '{\"b\":2,\"a\":1}'), (2, NULL)",
        )
        .await
        .expect("ddl");

    let fields = "schema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"payload\" type=\"utf8\" pg_type=\"jsonb\"";
    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"forced_jsonb_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_forced_jsonb_in\" cursor_column=\"id\"\n{fields}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        utf8_values(&batches, "payload"),
        text_column(&client, "t_forced_jsonb_in", "payload").await
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"forced_jsonb_out\"\ntable \"t_forced_jsonb_out\"\n{fields}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_forced_jsonb_in",
        "t_forced_jsonb_out",
        &["id", "payload"],
    )
    .await;
}

/// Matrix case: `type = "utf8" pg_type = "int8"` reads a bigint as text, and
/// the same declaration with no `pg_type` is refused once the connection
/// opens and discovers the column is not `utf8`-default.
#[tokio::test]
async fn forced_utf8_pg_type_int8_and_bare_utf8_is_refused_at_first_connect() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE t_forced_int8 (id bigint PRIMARY KEY, big bigint); \
             INSERT INTO t_forced_int8 VALUES (1, 12345), (2, NULL)",
        )
        .await
        .expect("ddl");

    let mut ok_src = source(
        &pg.dsn(),
        "name \"forced_int8\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_forced_int8\" cursor_column=\"id\"\nschema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"big\" type=\"utf8\" pg_type=\"int8\"",
    );
    let batches = drain(&mut ok_src).await;
    assert_eq!(
        utf8_values(&batches, "big"),
        vec![Some("12345".to_string()), None]
    );

    let mut bad_src = source(
        &pg.dsn(),
        "name \"forced_int8_bad\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_forced_int8\" cursor_column=\"id\"\nschema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"big\" type=\"utf8\"",
    );
    let err = bad_src
        .next_batch()
        .await
        .expect_err("utf8 with no pg_type over a non-utf8-default column is refused");
    assert!(err.message().contains("big"), "{}", err.message());
}

/// Matrix case: `type = "int64" pg_type = "int2"` on the sink, with an
/// overflowing row. Refused naming the column and the row; the target table
/// is left empty because the whole flush is one transaction.
#[tokio::test]
async fn forced_int2_narrowing_sink_overflow_is_refused_and_writes_nothing() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute("CREATE TABLE t_forced_int2 (id bigint PRIMARY KEY, amount smallint)")
        .await
        .expect("ddl");

    let mut snk = sink(
        &pg.dsn(),
        "name \"forced_int2\"\ntable \"t_forced_int2\"\nschema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"amount\" type=\"int64\" pg_type=\"int2\"",
    );
    let batch = RecordBatch::try_new(
        snk.schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, -32768, 40000])),
        ],
    )
    .expect("batch");

    let err = async {
        snk.write_batch(&batch).await?;
        snk.finish().await
    }
    .await
    .expect_err("40000 does not fit an int2");
    assert!(err.message().contains("amount"), "{}", err.message());
    assert!(err.message().contains("row"), "{}", err.message());
    assert!(err.message().contains("40000"), "{}", err.message());

    let count: i64 = client
        .query_one("SELECT count(*) FROM t_forced_int2", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(
        count, 0,
        "the whole flush is one transaction; nothing is written"
    );
}

/// Matrix case: `type = "decimal128" precision=12 scale=2 pg_type =
/// "numeric(12,2)"`, plus the connect-time refusal when the target's own
/// typmod keeps fewer fractional digits than declared.
#[tokio::test]
async fn forced_decimal128_numeric_typmod_matches_and_narrower_target_is_refused() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE t_forced_numeric (id bigint PRIMARY KEY, total numeric(12,2)); \
             CREATE TABLE t_forced_numeric_narrow (id bigint PRIMARY KEY, total numeric(12,1)); \
             INSERT INTO t_forced_numeric VALUES (1, 123.40), (2, NULL)",
        )
        .await
        .expect("ddl");

    let mut src = source(
        &pg.dsn(),
        "name \"forced_numeric\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_forced_numeric\" cursor_column=\"id\"\nschema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"total\" type=\"decimal128\" precision=12 scale=2 pg_type=\"numeric(12,2)\"",
    );
    let batches = drain(&mut src).await;
    assert_eq!(decimal_values(&batches, "total"), vec![Some(12340), None]);

    let text = format!(
        "name \"forced_numeric_narrow\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_forced_numeric_narrow\" cursor_column=\"id\"\nschema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"total\" type=\"decimal128\" precision=12 scale=2 pg_type=\"numeric(12,1)\"\n\nconnection dsn={} sslmode=\"disable\"\n",
        common::quoted(&pg.dsn())
    );
    let cfg = PostgresSourceConfig::deserialize(from_kdl_str(&text).expect("parse kdl"))
        .expect("parse config");
    // numeric(12,1) keeps 1 fractional digit but the field declares scale 2;
    // this is a load-time refusal, checked from the static typmod alone with
    // no connection needed, so it fails inside `PostgresSource::new` itself.
    let err = PostgresSource::new(cfg)
        .err()
        .expect("a narrower target would round rather than error");
    assert!(err.message().contains("total"), "{}", err.message());
    assert!(err.message().contains("numeric(12,1)"), "{}", err.message());
}

/// Matrix case: `type = "utf8" pg_type = "public.mood"` for the enum, and the
/// identity assertion refusing a schema that does not match the server's.
#[tokio::test]
async fn forced_enum_matches_and_wrong_schema_is_refused_at_first_connect() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy'); \
             CREATE TABLE t_forced_enum (id bigint PRIMARY KEY, m mood); \
             INSERT INTO t_forced_enum VALUES (1, 'happy'), (2, NULL)",
        )
        .await
        .expect("ddl");

    let mut ok_src = source(
        &pg.dsn(),
        "name \"forced_enum\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_forced_enum\" cursor_column=\"id\"\nschema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"m\" type=\"utf8\" pg_type=\"public.mood\"",
    );
    assert_eq!(
        utf8_values(&drain(&mut ok_src).await, "m"),
        vec![Some("happy".to_string()), None]
    );

    let mut wrong_schema_src = source(
        &pg.dsn(),
        "name \"forced_enum_wrong\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_forced_enum\" cursor_column=\"id\"\nschema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"m\" type=\"utf8\" pg_type=\"other.mood\"",
    );
    let err = wrong_schema_src
        .next_batch()
        .await
        .expect_err("public.mood is not other.mood");
    assert!(err.message().contains("other.mood"), "{}", err.message());
}

/// Matrix case: a mixed-case enum, `type = "utf8" pg_type = "public.\"Mood\""`
/// on both ends, and the unquoted spelling refused because PostgreSQL folds
/// it to a name the catalog does not carry.
#[tokio::test]
async fn forced_quoted_enum_round_trips_and_the_folded_spelling_is_refused() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TYPE public.\"Mood\" AS ENUM ('Happy', 'Sad'); \
             CREATE TABLE t_quoted_enum_in (id bigint PRIMARY KEY, m public.\"Mood\"); \
             CREATE TABLE t_quoted_enum_out (LIKE t_quoted_enum_in INCLUDING ALL); \
             INSERT INTO t_quoted_enum_in VALUES (1, 'Happy'), (2, NULL), (3, 'Sad')",
        )
        .await
        .expect("ddl");

    let fields = "schema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"m\" type=\"utf8\" pg_type=\"public.\\\"Mood\\\"\"";
    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"quoted_enum_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_quoted_enum_in\" cursor_column=\"id\"\n{fields}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        utf8_values(&batches, "m"),
        vec![Some("Happy".to_string()), None, Some("Sad".to_string())]
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"quoted_enum_out\"\ntable \"t_quoted_enum_out\"\n{fields}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_quoted_enum_in",
        "t_quoted_enum_out",
        &["id", "m"],
    )
    .await;

    // An unquoted identifier folds to lower case, so `public.mood` names a
    // type this server does not have.
    let mut folded_src = source(
        &pg.dsn(),
        "name \"quoted_enum_folded\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_quoted_enum_in\" cursor_column=\"id\"\nschema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"m\" type=\"utf8\" pg_type=\"public.mood\"",
    );
    let err = folded_src
        .next_batch()
        .await
        .expect_err("public.mood is not public.\"Mood\"");
    assert!(err.message().contains("'m'"), "{}", err.message());
    assert!(err.message().contains("public.Mood"), "{}", err.message());
}

/// Matrix case: `format_type`'s own spelling of a `timestamptz(3)` column
/// puts the modifier before the trailing words, and the sink asserts a forced
/// modifier against exactly that display.
#[tokio::test]
async fn forced_timestamptz_modifier_before_the_trailing_words_round_trips() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE t_forced_tstz_in (id bigint PRIMARY KEY, tstz timestamptz(3)); \
             CREATE TABLE t_forced_tstz_out (LIKE t_forced_tstz_in INCLUDING ALL); \
             INSERT INTO t_forced_tstz_in VALUES \
               (1, '2024-01-02 03:04:05.123+02'), (2, NULL)",
        )
        .await
        .expect("ddl");

    let fields = "schema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"tstz\" type=\"timestamp_micros_utc\" pg_type=\"timestamp(3) with time zone\"";
    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"forced_tstz_in\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_forced_tstz_in\" cursor_column=\"id\"\n{fields}"
        ),
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        timestamp_values(&batches, "tstz"),
        vec![Some(1704157445123000), None]
    );

    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"forced_tstz_out\"\ntable \"t_forced_tstz_out\"\n{fields}"),
    );
    write_all(&mut snk, &batches).await;
    assert_tables_text_equal(
        &client,
        "t_forced_tstz_in",
        "t_forced_tstz_out",
        &["id", "tstz"],
    )
    .await;
}

/// Matrix case: a domain over `int4` reads as plain `int32`, and a
/// CHECK-violating write is refused by the server through the sink's cast
/// route, not silently accepted.
#[tokio::test]
async fn forced_domain_reads_as_int32_and_check_violation_is_refused_on_write() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE DOMAIN posint AS integer CHECK (VALUE > 0); \
             CREATE TABLE t_forced_domain (id bigint PRIMARY KEY, p posint)",
        )
        .await
        .expect("ddl");

    let fields = "schema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"p\" type=\"int32\" pg_type=\"public.posint\"";
    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"forced_domain\"\ntable \"t_forced_domain\"\n{fields}"),
    );
    let batch = RecordBatch::try_new(
        snk.schema(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![-1])),
        ],
    )
    .expect("batch");
    let err = async {
        snk.write_batch(&batch).await?;
        snk.finish().await
    }
    .await
    .expect_err("posint's CHECK (VALUE > 0) refuses -1");
    assert!(
        err.message().to_lowercase().contains("posint")
            || err.message().to_lowercase().contains("check"),
        "{}",
        err.message()
    );

    let count: i64 = client
        .query_one("SELECT count(*) FROM t_forced_domain", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 0);

    // Reading a domain column back is exactly int32, no decoration.
    client
        .batch_execute("INSERT INTO t_forced_domain VALUES (1, 5), (2, NULL)")
        .await
        .expect("seed");
    let mut src = source(
        &pg.dsn(),
        &format!(
            "name \"forced_domain_read\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_forced_domain\" cursor_column=\"id\"\n{fields}"
        ),
    );
    assert_eq!(i32_values(&drain(&mut src).await, "p"), vec![Some(5), None]);
}

/// A domain over a *character* type carries its modifier on its base, and an
/// explicit `"col"::<domain>` cast applies that modifier with explicit-cast
/// semantics -- which truncates `'hello'` to `'hel'` instead of raising
/// `22001`. The column therefore stages as the domain itself, where
/// `domain_recv` hands `varchar_recv` the domain's own typmod and then runs
/// the domain's CHECK, both during the `COPY`.
#[tokio::test]
async fn a_domain_over_a_character_type_refuses_an_overlong_value_instead_of_truncating() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE DOMAIN short AS varchar(3) CHECK (VALUE <> 'no'); \
             CREATE TABLE t_short (id bigint PRIMARY KEY, v short)",
        )
        .await
        .expect("ddl");

    let fields = "schema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"v\" type=\"utf8\" pg_type=\"public.short\"";
    // A fresh sink per case: a refused flush keeps its batch buffered for a
    // retry, so reusing one would replay the previous refusal.
    for (case, value, fragment) in [
        (
            "overlong",
            "hello",
            "value too long for type character varying(3)",
        ),
        ("violating", "no", "short"),
    ] {
        let mut snk = sink(
            &pg.dsn(),
            &format!("name \"short_{case}\"\ntable \"t_short\"\n{fields}"),
        );
        let batch = RecordBatch::try_new(
            snk.schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![value])),
            ],
        )
        .expect("batch");
        let outcome = async {
            snk.write_batch(&batch).await?;
            snk.finish().await
        }
        .await;
        let Err(err) = outcome else {
            panic!("{case}: '{value}' must be refused, not silently accepted");
        };
        assert!(
            err.message().contains(fragment),
            "{case}: {}",
            err.message()
        );
    }
    let count: i64 = client
        .query_one("SELECT count(*) FROM t_short", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 0, "a refused row must not land truncated");

    // An in-range value still lands, byte for byte.
    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"short_ok\"\ntable \"t_short\"\n{fields}"),
    );
    let batch = RecordBatch::try_new(
        snk.schema(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["abc"])),
        ],
    )
    .expect("batch");
    write_all(&mut snk, &[batch]).await;
    assert_eq!(
        text_column(&client, "t_short", "v").await,
        vec![Some("abc".to_string())]
    );
}

/// `bit(n)` and `bit varying(n)` are read and written as PostgreSQL text, so
/// their modifier reaches the server only through the staging route's cast --
/// and an explicit `::bit varying(4)` truncates an over-long bit string while
/// an explicit `::bit(4)` zero-pads a short one. The cast therefore names the
/// bare base type, leaving the modifier to the assignment coercion the
/// `INSERT` performs, which raises for both.
#[tokio::test]
async fn a_bit_string_modifier_is_enforced_by_the_insert_rather_than_the_cast() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute("CREATE TABLE t_bits (id bigint PRIMARY KEY, vb bit varying(4), b bit(4))")
        .await
        .expect("ddl");

    let fields = "schema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"vb\" type=\"utf8\" pg_type=\"bit varying(4)\"\nschema_fields \"b\" type=\"utf8\" pg_type=\"bit(4)\"";
    for (case, vb, b, fragment) in [
        ("too long", "11111111", "1111", "too long"),
        ("too short", "1111", "11", "does not match"),
    ] {
        let mut snk = sink(
            &pg.dsn(),
            &format!(
                "name \"bits_{}\"\ntable \"t_bits\"\n{fields}",
                case.replace(' ', "_")
            ),
        );
        let batch = RecordBatch::try_new(
            snk.schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![vb])),
                Arc::new(StringArray::from(vec![b])),
            ],
        )
        .expect("batch");
        let outcome = async {
            snk.write_batch(&batch).await?;
            snk.finish().await
        }
        .await;
        let Err(err) = outcome else {
            panic!("{case}: must be refused, not padded or truncated");
        };
        assert!(
            err.message().contains(fragment),
            "{case}: {}",
            err.message()
        );
    }
    let count: i64 = client
        .query_one("SELECT count(*) FROM t_bits", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 0, "a refused row must not land resized");

    // A bit string of exactly the declared width still lands.
    let mut snk = sink(
        &pg.dsn(),
        &format!("name \"bits_ok\"\ntable \"t_bits\"\n{fields}"),
    );
    let batch = RecordBatch::try_new(
        snk.schema(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["101"])),
            Arc::new(StringArray::from(vec!["1010"])),
        ],
    )
    .expect("batch");
    write_all(&mut snk, &[batch]).await;
    assert_eq!(
        text_column(&client, "t_bits", "vb").await,
        vec![Some("101".to_string())]
    );
    assert_eq!(
        text_column(&client, "t_bits", "b").await,
        vec![Some("1010".to_string())]
    );
}

/// Matrix case: `type = "list" item = "utf8"` over a real `text[]` column
/// with a NULL element and a quote-and-comma-hostile element.
#[tokio::test]
async fn forced_list_utf8_over_text_array_with_null_and_quoted_element() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE t_forced_list (id bigint PRIMARY KEY, tags text[]); \
             INSERT INTO t_forced_list VALUES (1, ARRAY['a', NULL, 'with \"quote\", and comma'])",
        )
        .await
        .expect("ddl");

    let mut src = source(
        &pg.dsn(),
        "name \"forced_list\"\nbatch_rows 100\n\nmode kind=\"polling\" table=\"t_forced_list\" cursor_column=\"id\"\nschema_fields \"id\" type=\"int64\" nullable=#false\nschema_fields \"tags\" type=\"list\" item=\"utf8\"",
    );
    let batches = drain(&mut src).await;
    assert_eq!(
        utf8_list_values(&batches, "tags"),
        vec![Some(vec![
            Some("a".to_string()),
            None,
            Some("with \"quote\", and comma".to_string()),
        ])]
    );
}
