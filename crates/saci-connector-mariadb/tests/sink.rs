//! [`MariadbSink`] against real MariaDB and MySQL servers.
//!
//! Every case runs once per flavour (`mariadb::case`, `mysql::case`). The first
//! one is the end to end proof that `encode.rs` and the strict session mode
//! agree with the servers: every supported type is written through a prepared
//! `INSERT` and read back with `SELECT`, value for value.
//!
//! Soft-skips without Docker; see `common::try_start`.

mod common;

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use common::Server;
use mysql_async::prelude::*;
use mysql_async::{Conn, Row, Value};
use saci_connector_mariadb::MariadbSink;
use saci_core::io::sink::Sink;

common::on_every_server!(
    append_writes_every_declared_type_and_reads_back_equal,
    upsert_updates_in_place_and_ignore_conflicts_leaves_existing_rows,
    a_key_only_upsert_resolves_like_ignore_conflicts,
    dedupe_order_column_picks_the_highest_order_value,
    apply_changes_inserts_updates_and_deletes_in_order,
    apply_changes_with_a_composite_key_deletes_the_right_rows,
    apply_changes_deletes_in_chunks_of_a_thousand_keys,
    an_unknown_op_fails_the_flush_and_writes_nothing,
    a_delete_for_a_null_key_is_refused,
    conflict_columns_that_match_no_unique_key_are_refused,
    flush_rows_buffers_until_the_threshold_and_finish_drains_the_rest,
    a_failed_flush_keeps_the_rows_an_earlier_call_accepted,
    a_failed_finish_keeps_its_rows_for_the_next_attempt,
    truncate_before_first_write_replaces_the_contents_once,
    a_failed_truncating_flush_rolls_the_delete_back,
    first_write_truncates_only_once_across_a_reconnect,
    a_missing_table_is_a_configuration_error_naming_it,
    strict_mode_refuses_an_out_of_range_value_and_writes_nothing,
    a_value_finer_than_the_column_fraction_is_refused,
    a_table_name_with_a_backtick_is_written_not_injected,
    chunking_stays_under_the_placeholder_limit,
    an_altered_column_type_between_flushes_is_written_with_the_new_type,
    a_batch_the_caller_retries_is_written_once,
    a_decimal_delete_key_matches_exactly,
    a_statement_over_max_allowed_packet_is_split,
    a_repeated_key_applies_in_batch_order_without_a_dedupe_column,
    the_edges_of_every_temporal_and_decimal_type_round_trip,
    a_connection_lost_mid_flush_is_recovered_by_a_retry,
    a_write_dropped_mid_statement_is_redriven_once,
    a_dropped_flush_at_any_point_loses_nothing_and_repeats_nothing,
    a_dropped_finish_at_any_point_loses_nothing,
    a_dropped_truncating_flush_still_truncates_once,
);

// ------------------------------------------------------------------ helpers

async fn exec(conn: &mut Conn, sql: &str) {
    conn.query_drop(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Every row of `sql` as display text, `NULL` for a NULL.
async fn grid(conn: &mut Conn, sql: &str) -> Vec<Vec<String>> {
    let rows: Vec<Row> = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.into_iter()
        .map(|row| {
            row.unwrap()
                .into_iter()
                .map(|value| match value {
                    Value::NULL => "NULL".to_string(),
                    Value::Bytes(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                    Value::Int(v) => v.to_string(),
                    Value::UInt(v) => v.to_string(),
                    other => other.as_sql(false),
                })
                .collect()
        })
        .collect()
}

fn strings(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(ToString::to_string).collect())
        .collect()
}

async fn count(conn: &mut Conn, table: &str) -> i64 {
    conn.query_first(format!("SELECT COUNT(*) FROM {table}"))
        .await
        .expect("count")
        .expect("a count row")
}

fn id_batch(sink: &MariadbSink, ids: &[i64]) -> RecordBatch {
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(ids.to_vec()))];
    RecordBatch::try_new(sink.schema(), columns).expect("batch")
}

fn id_value_batch(sink: &MariadbSink, rows: &[(i64, &str)]) -> RecordBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(sink.schema(), columns).expect("batch")
}

const ID_VALUE_FIELDS: &str = "schema_fields \"id\" type=\"int64\" nullable=#false\n\
                               schema_fields \"v\" type=\"utf8\"\n";

fn id_value_sink(server: &Server, table: &str, mode: &str, extra: &str) -> MariadbSink {
    common::sink(
        &server.dsn(),
        &format!("name \"s\"\ntable \"{table}\"\nwrite_mode \"{mode}\"\n{extra}{ID_VALUE_FIELDS}"),
    )
}

// -------------------------------------------------------------------- tests

async fn append_writes_every_declared_type_and_reads_back_equal(server: &Server) {
    let mut conn = server.conn().await;
    exec(&mut conn, "SET time_zone = '+00:00'").await;
    exec(
        &mut conn,
        "CREATE TABLE every (id BIGINT NOT NULL PRIMARY KEY, c_flag TINYINT(1), c_i8 TINYINT, \
         c_u8 TINYINT UNSIGNED, c_i16 SMALLINT, c_u16 SMALLINT UNSIGNED, c_i32 INT, \
         c_u32 INT UNSIGNED, c_i64 BIGINT, c_u64 BIGINT UNSIGNED, c_f32 FLOAT, c_f64 DOUBLE, \
         c_dec DECIMAL(20,4), c_txt VARCHAR(100) CHARACTER SET utf8mb4, c_bin VARBINARY(16), \
         c_date DATE, c_time TIME(6), c_dt DATETIME(6), c_ts TIMESTAMP(6) NULL)",
    )
    .await;

    let mut sink = common::sink(
        &server.dsn(),
        "name \"every\"\ntable \"every\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"c_flag\" type=\"boolean\"\n\
         schema_fields \"c_i8\" type=\"int8\"\n\
         schema_fields \"c_u8\" type=\"uint8\"\n\
         schema_fields \"c_i16\" type=\"int16\"\n\
         schema_fields \"c_u16\" type=\"uint16\"\n\
         schema_fields \"c_i32\" type=\"int32\"\n\
         schema_fields \"c_u32\" type=\"uint32\"\n\
         schema_fields \"c_i64\" type=\"int64\"\n\
         schema_fields \"c_u64\" type=\"uint64\"\n\
         schema_fields \"c_f32\" type=\"float32\"\n\
         schema_fields \"c_f64\" type=\"float64\"\n\
         schema_fields \"c_dec\" type=\"decimal128\" precision=20 scale=4\n\
         schema_fields \"c_txt\" type=\"utf8\"\n\
         schema_fields \"c_bin\" type=\"binary\"\n\
         schema_fields \"c_date\" type=\"date32\"\n\
         schema_fields \"c_time\" type=\"time64_micros\"\n\
         schema_fields \"c_dt\" type=\"timestamp_micros\"\n\
         schema_fields \"c_ts\" type=\"timestamp_micros_utc\"\n",
    );

    // 2023-11-14 22:13:20.123456 UTC
    let micros = 1_700_000_000_123_456_i64;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1_i64, 2])),
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(Int8Array::from(vec![Some(-128_i8), None])),
        Arc::new(UInt8Array::from(vec![Some(255_u8), None])),
        Arc::new(Int16Array::from(vec![Some(-32_768_i16), None])),
        Arc::new(UInt16Array::from(vec![Some(65_535_u16), None])),
        Arc::new(Int32Array::from(vec![Some(i32::MIN), None])),
        Arc::new(UInt32Array::from(vec![Some(u32::MAX), None])),
        Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
        Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
        Arc::new(Float32Array::from(vec![Some(1.5_f32), None])),
        Arc::new(Float64Array::from(vec![Some(2.5_f64), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(123_456_789_012_345_678_i128), None])
                .with_precision_and_scale(20, 4)
                .expect("decimal"),
        ),
        Arc::new(StringArray::from(vec![Some("h\u{e9}llo \u{1F600}"), None])),
        Arc::new(BinaryArray::from(vec![
            Some(&[0x00_u8, 0xff, 0x10][..]),
            None,
        ])),
        Arc::new(Date32Array::from(vec![Some(19_000), None])),
        Arc::new(Time64MicrosecondArray::from(vec![
            Some(47_655_123_456_i64),
            None,
        ])),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(micros), None])),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(micros), None]).with_timezone("UTC")),
    ];
    let batch = RecordBatch::try_new(sink.schema(), columns).expect("batch");
    sink.write_batch(&batch).await.expect("write");
    sink.finish().await.expect("finish");

    let rows = grid(
        &mut conn,
        "SELECT id, CAST(c_flag AS CHAR), CAST(c_i8 AS CHAR), CAST(c_u8 AS CHAR), \
         CAST(c_i16 AS CHAR), CAST(c_u16 AS CHAR), CAST(c_i32 AS CHAR), CAST(c_u32 AS CHAR), \
         CAST(c_i64 AS CHAR), CAST(c_u64 AS CHAR), CAST(c_f32 AS CHAR), CAST(c_f64 AS CHAR), \
         CAST(c_dec AS CHAR), c_txt, HEX(c_bin), CAST(c_date AS CHAR), CAST(c_time AS CHAR), \
         CAST(c_dt AS CHAR), CAST(c_ts AS CHAR) FROM every ORDER BY id",
    )
    .await;
    assert_eq!(
        rows[0],
        vec![
            "1",
            "1",
            "-128",
            "255",
            "-32768",
            "65535",
            "-2147483648",
            "4294967295",
            "-9223372036854775808",
            "18446744073709551615",
            "1.5",
            "2.5",
            "12345678901234.5678",
            "h\u{e9}llo \u{1F600}",
            "00FF10",
            "2022-01-08",
            "13:14:15.123456",
            "2023-11-14 22:13:20.123456",
            "2023-11-14 22:13:20.123456",
        ]
    );
    let mut nulls = vec!["NULL".to_string(); 19];
    nulls[0] = "2".to_string();
    // HEX(NULL) is NULL, and CAST(NULL AS CHAR) is NULL.
    assert_eq!(rows[1], nulls);
}

async fn upsert_updates_in_place_and_ignore_conflicts_leaves_existing_rows(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE small (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(20), note VARCHAR(20) \
         DEFAULT 'kept', touched TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) \
         ON UPDATE CURRENT_TIMESTAMP(6))",
    )
    .await;
    exec(
        &mut conn,
        "INSERT INTO small (id, v) VALUES (1, 'old'), (2, 'two')",
    )
    .await;
    let before = grid(
        &mut conn,
        "SELECT CAST(touched AS CHAR) FROM small WHERE id = 2",
    )
    .await;

    let mut upsert = id_value_sink(server, "small", "upsert", "conflict_columns \"id\"\n");
    let batch = id_value_batch(&upsert, &[(1, "new"), (3, "three")]);
    upsert.write_batch(&batch).await.expect("upsert");
    assert_eq!(
        grid(&mut conn, "SELECT id, v, note FROM small ORDER BY id").await,
        strings(&[
            &["1", "new", "kept"],
            &["2", "two", "kept"],
            &["3", "three", "kept"]
        ])
    );

    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let mut ignore = id_value_sink(
        server,
        "small",
        "ignore_conflicts",
        "conflict_columns \"id\"\n",
    );
    let batch = id_value_batch(&ignore, &[(1, "ignored"), (2, "ignored"), (4, "four")]);
    ignore.write_batch(&batch).await.expect("ignore");
    assert_eq!(
        grid(&mut conn, "SELECT id, v FROM small ORDER BY id").await,
        strings(&[
            &["1", "new"],
            &["2", "two"],
            &["3", "three"],
            &["4", "four"]
        ])
    );
    // A duplicate the sink skipped is untouched, ON UPDATE columns included.
    assert_eq!(
        grid(
            &mut conn,
            "SELECT CAST(touched AS CHAR) FROM small WHERE id = 2"
        )
        .await,
        before
    );
}

async fn a_key_only_upsert_resolves_like_ignore_conflicts(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE keys_only (id BIGINT NOT NULL PRIMARY KEY)",
    )
    .await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"k\"\ntable \"keys_only\"\nwrite_mode \"upsert\"\nconflict_columns \"id\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n",
    );
    let batch = id_batch(&sink, &[1, 2]);
    sink.write_batch(&batch).await.expect("first");
    let batch = id_batch(&sink, &[2, 3]);
    sink.write_batch(&batch)
        .await
        .expect("a duplicate key is not an error");
    assert_eq!(
        grid(&mut conn, "SELECT id FROM keys_only ORDER BY id").await,
        strings(&[&["1"], &["2"], &["3"]])
    );
}

async fn dedupe_order_column_picks_the_highest_order_value(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE deduped (id BIGINT NOT NULL PRIMARY KEY, seq BIGINT, v VARCHAR(10))",
    )
    .await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"d\"\ntable \"deduped\"\nwrite_mode \"upsert\"\nconflict_columns \"id\"\n\
         dedupe_order_column \"seq\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"seq\" type=\"int64\"\n\
         schema_fields \"v\" type=\"utf8\"\n",
    );
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1_i64, 1, 1, 2, 2])),
        Arc::new(Int64Array::from(vec![5_i64, 9, 7, 1, 1])),
        Arc::new(StringArray::from(vec!["a", "b", "c", "x", "y"])),
    ];
    let batch = RecordBatch::try_new(sink.schema(), columns).expect("batch");
    sink.write_batch(&batch).await.expect("write");
    assert_eq!(
        grid(&mut conn, "SELECT id, seq, v FROM deduped ORDER BY id").await,
        strings(&[&["1", "9", "b"], &["2", "1", "y"]])
    );
}

const CHANGES_FIELDS: &str = "schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
                              schema_fields \"id\" type=\"int64\" nullable=#false\n\
                              schema_fields \"v\" type=\"utf8\"\n";

fn changes_batch(sink: &MariadbSink, rows: &[(&str, i64, &str)]) -> RecordBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(sink.schema(), columns).expect("batch")
}

fn changes_sink(server: &Server, table: &str) -> MariadbSink {
    common::sink(
        &server.dsn(),
        &format!(
            "name \"c\"\ntable \"{table}\"\nwrite_mode \"apply_changes\"\n\
             conflict_columns \"id\"\n{CHANGES_FIELDS}"
        ),
    )
}

async fn apply_changes_inserts_updates_and_deletes_in_order(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE changed (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(10))",
    )
    .await;
    let mut sink = changes_sink(server, "changed");
    let batch = changes_batch(
        &sink,
        &[
            ("I", 1, "a"),
            ("I", 2, "b"),
            ("I", 3, "c"),
            ("U", 2, "b2"),
            ("D", 3, ""),
            ("R", 4, "d"),
            ("D", 1, ""),
            ("I", 1, "a2"),
            // A primary key change arrives as a delete of the old key and an
            // insert of the new one.
            ("D", 2, ""),
            ("I", 20, "b3"),
        ],
    );
    sink.write_batch(&batch).await.expect("apply");
    assert_eq!(
        grid(&mut conn, "SELECT id, v FROM changed ORDER BY id").await,
        strings(&[&["1", "a2"], &["4", "d"], &["20", "b3"]])
    );
}

async fn apply_changes_with_a_composite_key_deletes_the_right_rows(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE pairs (a BIGINT NOT NULL, b VARCHAR(10) NOT NULL, v INT, PRIMARY KEY (a, b))",
    )
    .await;
    exec(
        &mut conn,
        "INSERT INTO pairs VALUES (1,'x',1),(1,'y',2),(2,'x',3),(2,'y',4)",
    )
    .await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"p\"\ntable \"pairs\"\nwrite_mode \"apply_changes\"\nconflict_columns \"a\" \"b\"\n\
         schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
         schema_fields \"a\" type=\"int64\" nullable=#false\n\
         schema_fields \"b\" type=\"utf8\" nullable=#false\n\
         schema_fields \"v\" type=\"int32\"\n",
    );
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["D", "D", "U", "I"])),
        Arc::new(Int64Array::from(vec![1_i64, 2, 1, 3])),
        Arc::new(StringArray::from(vec!["y", "x", "x", "z"])),
        Arc::new(Int32Array::from(vec![0, 0, 10, 30])),
    ];
    let batch = RecordBatch::try_new(sink.schema(), columns).expect("batch");
    sink.write_batch(&batch).await.expect("apply");
    assert_eq!(
        grid(&mut conn, "SELECT a, b, v FROM pairs ORDER BY a, b").await,
        strings(&[&["1", "x", "10"], &["2", "y", "4"], &["3", "z", "30"]])
    );
}

async fn apply_changes_deletes_in_chunks_of_a_thousand_keys(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE many (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(10))",
    )
    .await;
    exec(
        &mut conn,
        "CREATE TABLE many_pairs (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a, b))",
    )
    .await;
    for start in (0..2500).step_by(500) {
        let single: Vec<String> = (start..start + 500)
            .map(|i| format!("({i}, 'v')"))
            .collect();
        exec(
            &mut conn,
            &format!("INSERT INTO many VALUES {}", single.join(",")),
        )
        .await;
        let pairs: Vec<String> = (start..start + 500)
            .map(|i| format!("({i}, {})", i + 1))
            .collect();
        exec(
            &mut conn,
            &format!("INSERT INTO many_pairs VALUES {}", pairs.join(",")),
        )
        .await;
    }
    assert_eq!(count(&mut conn, "many").await, 2500);

    let mut sink = changes_sink(server, "many");
    let rows: Vec<(&str, i64, &str)> = (0..2500).map(|i| ("D", i, "")).collect();
    let batch = changes_batch(&sink, &rows);
    sink.write_batch(&batch)
        .await
        .expect("delete 2500 single keys");
    assert_eq!(count(&mut conn, "many").await, 0);

    let mut pairs = common::sink(
        &server.dsn(),
        "name \"mp\"\ntable \"many_pairs\"\nwrite_mode \"apply_changes\"\n\
         conflict_columns \"a\" \"b\"\n\
         schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
         schema_fields \"a\" type=\"int64\" nullable=#false\n\
         schema_fields \"b\" type=\"int64\" nullable=#false\n",
    );
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["D"; 2500])),
        Arc::new(Int64Array::from((0..2500_i64).collect::<Vec<_>>())),
        Arc::new(Int64Array::from((1..=2500_i64).collect::<Vec<_>>())),
    ];
    let batch = RecordBatch::try_new(pairs.schema(), columns).expect("batch");
    pairs
        .write_batch(&batch)
        .await
        .expect("delete 2500 composite keys");
    assert_eq!(count(&mut conn, "many_pairs").await, 0);
}

async fn an_unknown_op_fails_the_flush_and_writes_nothing(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE unknown_op (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(10))",
    )
    .await;
    let mut sink = changes_sink(server, "unknown_op");
    let batch = changes_batch(&sink, &[("I", 1, "a"), ("X", 2, "b")]);
    let err = sink.write_batch(&batch).await.expect_err("X is no op");
    assert_eq!(
        err.message(),
        "MariadbSink: row 1 has __op 'X'; apply_changes understands I, U, R and D"
    );
    assert_eq!(
        count(&mut conn, "unknown_op").await,
        0,
        "the I row before X is not written"
    );
    assert_eq!(
        sink.pending_rows(),
        Some(0),
        "the refused batch is not held; a re-drive is refused the same way"
    );
}

async fn a_delete_for_a_null_key_is_refused(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE null_key (k BIGINT NULL, v VARCHAR(10), UNIQUE KEY (k))",
    )
    .await;
    exec(
        &mut conn,
        "INSERT INTO null_key VALUES (NULL, 'a'), (1, 'b')",
    )
    .await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"n\"\ntable \"null_key\"\nwrite_mode \"apply_changes\"\nconflict_columns \"k\"\n\
         schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
         schema_fields \"k\" type=\"int64\"\n\
         schema_fields \"v\" type=\"utf8\"\n",
    );
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["D"])),
        Arc::new(Int64Array::from(vec![None::<i64>])),
        Arc::new(StringArray::from(vec!["a"])),
    ];
    let batch = RecordBatch::try_new(sink.schema(), columns).expect("batch");
    let err = sink
        .write_batch(&batch)
        .await
        .expect_err("a NULL key matches no row");
    assert!(
        err.message().contains("NULL conflict key"),
        "{}",
        err.message()
    );
    assert_eq!(count(&mut conn, "null_key").await, 2);
}

async fn conflict_columns_that_match_no_unique_key_are_refused(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE keyed (id BIGINT NOT NULL PRIMARY KEY, code VARCHAR(10) NOT NULL, \
         v VARCHAR(10), UNIQUE KEY by_code (code))",
    )
    .await;
    let fields = "schema_fields \"id\" type=\"int64\" nullable=#false\n\
                  schema_fields \"code\" type=\"utf8\" nullable=#false\n\
                  schema_fields \"v\" type=\"utf8\"\n";
    let build = |key: &str| {
        common::sink(
            &server.dsn(),
            &format!(
                "name \"u\"\ntable \"keyed\"\nwrite_mode \"upsert\"\nconflict_columns {key}\n{fields}"
            ),
        )
    };
    let columns = |sink: &MariadbSink| -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1_i64])),
            Arc::new(StringArray::from(vec!["c1"])),
            Arc::new(StringArray::from(vec!["x"])),
        ];
        RecordBatch::try_new(sink.schema(), columns).expect("batch")
    };

    let mut wrong = build("\"v\"");
    let batch = columns(&wrong);
    let err = wrong
        .write_batch(&batch)
        .await
        .expect_err("v is not unique");
    assert_eq!(err.category(), "configuration");
    assert!(
        err.message().contains(
            "conflict_columns (v) match no PRIMARY KEY or UNIQUE index of 'saci.keyed'; \
             ON DUPLICATE KEY UPDATE would resolve on a different key"
        ),
        "{}",
        err.message()
    );
    let mut partial = build("\"id\" \"code\"");
    let batch = columns(&partial);
    let err = partial
        .write_batch(&batch)
        .await
        .expect_err("id+code is no unique key");
    assert_eq!(err.category(), "configuration");
    assert_eq!(count(&mut conn, "keyed").await, 0);

    // A non-primary unique index is a valid conflict key.
    let mut by_code = build("\"code\"");
    let batch = columns(&by_code);
    by_code.write_batch(&batch).await.expect("code is unique");
    assert_eq!(count(&mut conn, "keyed").await, 1);
}

async fn flush_rows_buffers_until_the_threshold_and_finish_drains_the_rest(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE buffered (id BIGINT NOT NULL PRIMARY KEY)",
    )
    .await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"b\"\ntable \"buffered\"\nflush_rows 5\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n",
    );
    let batch = id_batch(&sink, &[1, 2]);
    sink.write_batch(&batch).await.expect("buffer");
    assert_eq!(sink.pending_rows(), Some(2));
    assert_eq!(count(&mut conn, "buffered").await, 0);

    let batch = id_batch(&sink, &[3, 4, 5]);
    sink.write_batch(&batch)
        .await
        .expect("reaches the threshold");
    assert_eq!(sink.pending_rows(), Some(0));
    assert_eq!(count(&mut conn, "buffered").await, 5);

    let batch = id_batch(&sink, &[6]);
    sink.write_batch(&batch).await.expect("buffer");
    assert_eq!(sink.pending_rows(), Some(1));
    sink.finish().await.expect("finish drains");
    assert_eq!(sink.pending_rows(), Some(0));
    assert_eq!(count(&mut conn, "buffered").await, 6);
}

async fn a_failed_flush_keeps_the_rows_an_earlier_call_accepted(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE retried (id BIGINT NOT NULL PRIMARY KEY)",
    )
    .await;
    exec(&mut conn, "INSERT INTO retried VALUES (1)").await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"r\"\ntable \"retried\"\nflush_rows 3\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n",
    );
    let accepted = id_batch(&sink, &[2]);
    sink.write_batch(&accepted).await.expect("buffered");
    let batch = id_batch(&sink, &[1, 3]);
    let err = sink
        .write_batch(&batch)
        .await
        .expect_err("append collides on 1");
    assert!(
        err.message()
            .starts_with("MariadbSink: INSERT into 'saci.retried' failed: "),
        "{}",
        err.message()
    );
    assert_eq!(
        sink.pending_rows(),
        Some(1),
        "the accepted row stays; the refused batch is the caller's to re-drive"
    );
    assert_eq!(
        count(&mut conn, "retried").await,
        1,
        "row 2 is rolled back with the rest"
    );

    exec(&mut conn, "DELETE FROM retried").await;
    sink.write_batch(&batch).await.expect("the re-driven batch");
    assert_eq!(
        grid(&mut conn, "SELECT id FROM retried ORDER BY id").await,
        strings(&[&["1"], &["2"], &["3"]])
    );
}

async fn a_failed_finish_keeps_its_rows_for_the_next_attempt(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE finished (id BIGINT NOT NULL PRIMARY KEY)",
    )
    .await;
    exec(&mut conn, "INSERT INTO finished VALUES (1)").await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"f\"\ntable \"finished\"\nflush_rows 100\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n",
    );
    let batch = id_batch(&sink, &[2, 1]);
    sink.write_batch(&batch).await.expect("buffered");
    sink.finish().await.expect_err("append collides on 1");
    assert_eq!(sink.pending_rows(), Some(2));
    assert_eq!(
        count(&mut conn, "finished").await,
        1,
        "row 2 is rolled back with the rest"
    );

    exec(&mut conn, "DELETE FROM finished").await;
    sink.finish().await.expect("the buffered rows are retried");
    assert_eq!(sink.pending_rows(), Some(0));
    assert_eq!(
        grid(&mut conn, "SELECT id FROM finished ORDER BY id").await,
        strings(&[&["1"], &["2"]])
    );
}

async fn truncate_before_first_write_replaces_the_contents_once(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE replaced (id BIGINT NOT NULL PRIMARY KEY)",
    )
    .await;
    exec(&mut conn, "INSERT INTO replaced VALUES (1), (2), (3)").await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"t\"\ntable \"replaced\"\ntruncate_before_first_write #true\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n",
    );
    let batch = id_batch(&sink, &[10, 11]);
    sink.write_batch(&batch).await.expect("first");
    assert_eq!(
        grid(&mut conn, "SELECT id FROM replaced ORDER BY id").await,
        strings(&[&["10"], &["11"]])
    );
    let batch = id_batch(&sink, &[12]);
    sink.write_batch(&batch).await.expect("second");
    assert_eq!(
        grid(&mut conn, "SELECT id FROM replaced ORDER BY id").await,
        strings(&[&["10"], &["11"], &["12"]]),
        "the second flush must not truncate again"
    );
}

async fn a_failed_truncating_flush_rolls_the_delete_back(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE guarded (id BIGINT NOT NULL PRIMARY KEY)",
    )
    .await;
    exec(&mut conn, "INSERT INTO guarded VALUES (1), (2)").await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"g\"\ntable \"guarded\"\ntruncate_before_first_write #true\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n",
    );
    let batch = id_batch(&sink, &[7, 7]);
    sink.write_batch(&batch)
        .await
        .expect_err("a duplicate inside the batch");
    assert_eq!(
        grid(&mut conn, "SELECT id FROM guarded ORDER BY id").await,
        strings(&[&["1"], &["2"]]),
        "the delete is part of the failed transaction"
    );
    let batch = id_batch(&sink, &[7, 8]);
    sink.write_batch(&batch)
        .await
        .expect("the first write that lands still truncates");
    assert_eq!(
        grid(&mut conn, "SELECT id FROM guarded ORDER BY id").await,
        strings(&[&["7"], &["8"]])
    );
}

async fn first_write_truncates_only_once_across_a_reconnect(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE reconnected (id BIGINT NOT NULL PRIMARY KEY)",
    )
    .await;
    exec(&mut conn, "INSERT INTO reconnected VALUES (100)").await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"rc\"\ntable \"reconnected\"\ntruncate_before_first_write #true\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n",
    );
    let batch = id_batch(&sink, &[1]);
    sink.write_batch(&batch).await.expect("first");

    // Kill every other connection, the sink's included.
    let me: u64 = conn
        .query_first("SELECT CONNECTION_ID()")
        .await
        .expect("id")
        .expect("row");
    let ids: Vec<u64> = conn
        .query("SELECT ID FROM information_schema.PROCESSLIST")
        .await
        .expect("processlist");
    for id in ids.into_iter().filter(|id| *id != me) {
        let _ = conn.query_drop(format!("KILL {id}")).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let batch = id_batch(&sink, &[2]);
    sink.write_batch(&batch).await.expect("the sink reconnects");
    assert_eq!(
        grid(&mut conn, "SELECT id FROM reconnected ORDER BY id").await,
        strings(&[&["1"], &["2"]]),
        "a reconnect must not truncate the rows the first flush wrote"
    );
}

async fn a_missing_table_is_a_configuration_error_naming_it(server: &Server) {
    let mut sink = common::sink(
        &server.dsn(),
        "name \"m\"\ntable \"no_such_table\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n",
    );
    let batch = id_batch(&sink, &[1]);
    let err = sink.write_batch(&batch).await.expect_err("no table");
    assert_eq!(err.category(), "configuration");
    assert!(
        err.message().contains("'saci.no_such_table'"),
        "{}",
        err.message()
    );
    assert!(
        err.message().contains("does not exist"),
        "{}",
        err.message()
    );
}

async fn strict_mode_refuses_an_out_of_range_value_and_writes_nothing(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE tiny (id BIGINT NOT NULL PRIMARY KEY, v TINYINT)",
    )
    .await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"st\"\ntable \"tiny\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"v\" type=\"int32\"\n",
    );
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1_i64, 2])),
        Arc::new(Int32Array::from(vec![100, 300])),
    ];
    let batch = RecordBatch::try_new(sink.schema(), columns).expect("batch");
    let err = sink
        .write_batch(&batch)
        .await
        .expect_err("300 does not fit a TINYINT");
    assert!(
        err.message()
            .starts_with("MariadbSink: INSERT into 'saci.tiny' failed: Out of range value"),
        "{}",
        err.message()
    );
    assert_eq!(
        count(&mut conn, "tiny").await,
        0,
        "the in-range row is rolled back too"
    );
}

async fn a_value_finer_than_the_column_fraction_is_refused(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE coarse (id BIGINT NOT NULL PRIMARY KEY, at DATETIME(0))",
    )
    .await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"f\"\ntable \"coarse\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"at\" type=\"timestamp_micros\"\n",
    );
    let write = |sink: &MariadbSink, id: i64, micros: i64| -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(TimestampMicrosecondArray::from(vec![micros])),
        ];
        RecordBatch::try_new(sink.schema(), columns).expect("batch")
    };
    let whole = write(&sink, 1, 1_700_000_000_000_000);
    sink.write_batch(&whole)
        .await
        .expect("whole seconds fit DATETIME(0)");
    let fine = write(&sink, 2, 1_700_000_000_500_000);
    let err = sink
        .write_batch(&fine)
        .await
        .expect_err("half a second does not");
    assert!(
        err.message().contains("fraction digits beyond the 0 "),
        "{}",
        err.message()
    );
    assert_eq!(
        grid(&mut conn, "SELECT id, CAST(at AS CHAR) FROM coarse").await,
        strings(&[&["1", "2023-11-14 22:13:20"]])
    );
}

async fn a_table_name_with_a_backtick_is_written_not_injected(server: &Server) {
    let mut conn = server.conn().await;
    exec(&mut conn, "CREATE TABLE victim (id BIGINT)").await;
    exec(&mut conn, "INSERT INTO victim VALUES (1)").await;
    exec(
        &mut conn,
        "CREATE TABLE `t``; DROP TABLE victim; --` (`c``; -- x` BIGINT NOT NULL)",
    )
    .await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"bt\"\ntable \"t`; DROP TABLE victim; --\"\n\
         schema_fields \"c`; -- x\" type=\"int64\" nullable=#false\n",
    );
    let batch = id_batch(&sink, &[42]);
    sink.write_batch(&batch)
        .await
        .expect("the name is quoted, not interpreted");
    assert_eq!(
        grid(
            &mut conn,
            "SELECT `c``; -- x` FROM `t``; DROP TABLE victim; --`"
        )
        .await,
        strings(&[&["42"]])
    );
    assert_eq!(
        count(&mut conn, "victim").await,
        1,
        "the payload must not have run"
    );
}

async fn chunking_stays_under_the_placeholder_limit(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE wide (id BIGINT NOT NULL PRIMARY KEY, a INT, b INT, c INT, d INT)",
    )
    .await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"w\"\ntable \"wide\"\nchunk_rows 100000\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"a\" type=\"int32\"\nschema_fields \"b\" type=\"int32\"\n\
         schema_fields \"c\" type=\"int32\"\nschema_fields \"d\" type=\"int32\"\n",
    );
    // 13 108 rows of 5 columns is 65 540 placeholders, 5 over the server limit,
    // so an uncapped chunk would be refused.
    let rows = 13_108_usize;
    let ids: Vec<i64> = (0..rows as i64).collect();
    let ints = |n: i32| Arc::new(Int32Array::from(vec![n; rows])) as ArrayRef;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)),
        ints(1),
        ints(2),
        ints(3),
        ints(4),
    ];
    let batch = RecordBatch::try_new(sink.schema(), columns).expect("batch");
    sink.write_batch(&batch)
        .await
        .expect("split into two statements");
    assert_eq!(count(&mut conn, "wide").await, rows as i64);
}

async fn an_altered_column_type_between_flushes_is_written_with_the_new_type(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE altered (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(10) CHARACTER SET latin1)",
    )
    .await;
    let mut sink = id_value_sink(server, "altered", "upsert", "conflict_columns \"id\"\n");
    let batch = id_value_batch(&sink, &[(1, "abc")]);
    sink.write_batch(&batch).await.expect("latin1 text");

    // MySQL reprepares a cached statement against the new definition with the
    // old parameter state; the sink keeps no statement across flushes.
    exec(
        &mut conn,
        "ALTER TABLE altered MODIFY v VARCHAR(10) CHARACTER SET utf8mb4",
    )
    .await;
    let batch = id_value_batch(&sink, &[(2, "\u{1F600}")]);
    sink.write_batch(&batch)
        .await
        .expect("an emoji fits the altered column");
    assert_eq!(
        grid(&mut conn, "SELECT id, v FROM altered ORDER BY id").await,
        strings(&[&["1", "abc"], &["2", "\u{1F600}"]])
    );
}

/// The service's `RetryingSink` re-drives the batch whose `write_batch` failed.
/// A flush that failed must not leave that batch buffered, or the retry
/// buffers it a second time and the table gets every row twice.
async fn a_batch_the_caller_retries_is_written_once(server: &Server) {
    let mut conn = server.conn().await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"rt\"\ntable \"retried_once\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n",
    );
    let batch = id_batch(&sink, &[1, 2]);
    sink.write_batch(&batch)
        .await
        .expect_err("the table does not exist yet");
    assert_eq!(sink.pending_rows(), Some(0), "a refused batch is not held");

    exec(&mut conn, "CREATE TABLE retried_once (id BIGINT NOT NULL)").await;
    sink.write_batch(&batch).await.expect("the retry");
    sink.finish().await.expect("nothing left to write");
    assert_eq!(
        grid(&mut conn, "SELECT id FROM retried_once ORDER BY id").await,
        strings(&[&["1"], &["2"]]),
        "each row is written once"
    );
}

/// A decimal key is bound as exact text. A `DELETE` by it must name one row: two
/// keys that differ past the 15th digit must not compare equal as doubles.
async fn a_decimal_delete_key_matches_exactly(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE dkeys (k DECIMAL(38,0) NOT NULL PRIMARY KEY, v VARCHAR(10))",
    )
    .await;
    exec(
        &mut conn,
        "INSERT INTO dkeys VALUES (12345678901234567890123456789012345678, 'a'), \
         (12345678901234567890123456789012345679, 'b')",
    )
    .await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"dk\"\ntable \"dkeys\"\nwrite_mode \"apply_changes\"\nconflict_columns \"k\"\n\
         schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
         schema_fields \"k\" type=\"decimal128\" precision=38 scale=0 nullable=#false\n\
         schema_fields \"v\" type=\"utf8\"\n",
    );
    let keys = Decimal128Array::from(vec![12345678901234567890123456789012345678_i128])
        .with_precision_and_scale(38, 0)
        .expect("decimal");
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["D"])),
        Arc::new(keys),
        Arc::new(StringArray::from(vec![None::<&str>])),
    ];
    let batch = RecordBatch::try_new(sink.schema(), columns).expect("batch");
    sink.write_batch(&batch).await.expect("delete one key");
    assert_eq!(
        grid(&mut conn, "SELECT CAST(k AS CHAR), v FROM dkeys").await,
        strings(&[&["12345678901234567890123456789012345679", "b"]]),
        "only the named key goes"
    );
}

/// One statement larger than the server's `max_allowed_packet` is refused and
/// the connection with it, whatever `chunk_rows` says.
async fn a_statement_over_max_allowed_packet_is_split(server: &Server) {
    let mut conn = server.conn().await;
    exec(&mut conn, "SET GLOBAL max_allowed_packet = 1048576").await;
    exec(
        &mut conn,
        "CREATE TABLE blobs (id BIGINT NOT NULL PRIMARY KEY, b LONGBLOB)",
    )
    .await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"mp\"\ntable \"blobs\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"b\" type=\"binary\"\n",
    );
    // 20 rows of 200 000 bytes: 4 MB in one `chunk_rows 1000` statement, four
    // times what the server accepts, and each row alone well under it.
    let payload = vec![7_u8; 200_000];
    let rows = 20_usize;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
        Arc::new(BinaryArray::from_iter_values(
            (0..rows).map(|_| payload.as_slice()),
        )),
    ];
    let batch = RecordBatch::try_new(sink.schema(), columns).expect("batch");
    sink.write_batch(&batch)
        .await
        .expect("the rows go out in statements the server accepts");
    assert_eq!(count(&mut conn, "blobs").await, rows as i64);
}

/// With no `dedupe_order_column` a key repeated inside one flush applies in
/// batch order: the last row wins an upsert, the first survives
/// `ignore_conflicts`.
async fn a_repeated_key_applies_in_batch_order_without_a_dedupe_column(server: &Server) {
    let mut conn = server.conn().await;
    for table in ["order_upsert", "order_ignore"] {
        exec(
            &mut conn,
            &format!("CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(10))"),
        )
        .await;
    }
    for (table, mode, winner) in [
        ("order_upsert", "upsert", "second"),
        ("order_ignore", "ignore_conflicts", "first"),
    ] {
        let mut sink = id_value_sink(server, table, mode, "conflict_columns \"id\"\n");
        let batch = id_value_batch(&sink, &[(1, "first"), (1, "second")]);
        sink.write_batch(&batch).await.expect("write");
        assert_eq!(
            grid(&mut conn, &format!("SELECT id, v FROM {table}")).await,
            strings(&[&["1", winner]]),
            "{mode}"
        );
    }
}

/// The first and last values each temporal type holds, one microsecond either
/// side of the epoch, and the widest and a negative decimal.
async fn the_edges_of_every_temporal_and_decimal_type_round_trip(server: &Server) {
    let mut conn = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE edges (id BIGINT NOT NULL PRIMARY KEY, d DATE, dt DATETIME(6), \
         ts TIMESTAMP(6) NULL, t TIME(6), dec_a DECIMAL(38,0), dneg DECIMAL(10,4))",
    )
    .await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"e\"\ntable \"edges\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"d\" type=\"date32\"\n\
         schema_fields \"dt\" type=\"timestamp_micros\"\n\
         schema_fields \"ts\" type=\"timestamp_micros_utc\"\n\
         schema_fields \"t\" type=\"time64_micros\"\n\
         schema_fields \"dec_a\" type=\"decimal128\" precision=38 scale=0\n\
         schema_fields \"dneg\" type=\"decimal128\" precision=10 scale=4\n",
    );
    let widest = 99_999_999_999_999_999_999_999_999_999_999_999_999_i128;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
        // 0000-01-01, 9999-12-31, 1969-12-31
        Arc::new(Date32Array::from(vec![-719_528, 2_932_896, -1])),
        Arc::new(TimestampMicrosecondArray::from(vec![
            -62_167_219_200_000_000_i64,
            253_402_300_799_999_999,
            -1,
        ])),
        Arc::new(
            TimestampMicrosecondArray::from(vec![
                Some(1_000_000_i64),
                Some(2_147_483_647_000_000),
                None,
            ])
            .with_timezone("UTC"),
        ),
        Arc::new(Time64MicrosecondArray::from(vec![0_i64, 86_399_999_999, 1])),
        Arc::new(
            Decimal128Array::from(vec![widest, -widest, 0])
                .with_precision_and_scale(38, 0)
                .expect("decimal"),
        ),
        Arc::new(
            Decimal128Array::from(vec![-123_456_i128, -1, 0])
                .with_precision_and_scale(10, 4)
                .expect("decimal"),
        ),
    ];
    let batch = RecordBatch::try_new(sink.schema(), columns).expect("batch");
    sink.write_batch(&batch).await.expect("write");
    assert_eq!(
        grid(
            &mut conn,
            "SELECT id, CAST(d AS CHAR), CAST(dt AS CHAR), CAST(ts AS CHAR), CAST(t AS CHAR), \
             CAST(dec_a AS CHAR), CAST(dneg AS CHAR) FROM edges ORDER BY id",
        )
        .await,
        strings(&[
            &[
                "1",
                "0000-01-01",
                "0000-01-01 00:00:00.000000",
                "1970-01-01 00:00:01.000000",
                "00:00:00.000000",
                "99999999999999999999999999999999999999",
                "-12.3456",
            ],
            &[
                "2",
                "9999-12-31",
                "9999-12-31 23:59:59.999999",
                "2038-01-19 03:14:07.000000",
                "23:59:59.999999",
                "-99999999999999999999999999999999999999",
                "-0.0001",
            ],
            &[
                "3",
                "1969-12-31",
                "1969-12-31 23:59:59.999999",
                "NULL",
                "00:00:00.000001",
                "0",
                "0.0000",
            ],
        ])
    );
}

/// The server drops the sink's connection while its `INSERT` waits on a table
/// lock. The caller is told the batch failed and re-drives it, which must land
/// the rows once, on a fresh session.
async fn a_connection_lost_mid_flush_is_recovered_by_a_retry(server: &Server) {
    let mut conn = server.conn().await;
    let mut admin = server.conn().await;
    exec(&mut conn, "CREATE TABLE lost (id BIGINT NOT NULL)").await;
    let mut sink = common::sink(
        &server.dsn(),
        "name \"l\"\ntable \"lost\"\nschema_fields \"id\" type=\"int64\" nullable=#false\n",
    );
    let first = id_batch(&sink, &[1]);
    sink.write_batch(&first).await.expect("opens the session");

    exec(&mut conn, "LOCK TABLES lost WRITE").await;
    let batch = id_batch(&sink, &[2, 3]);
    let redriven = batch.clone();
    let blocked = tokio::spawn(async move {
        let outcome = sink.write_batch(&batch).await;
        (sink, outcome)
    });

    // Wait until the INSERT is parked on the lock, then drop its connection.
    let mut killed = false;
    for _ in 0..100 {
        let waiting: Vec<u64> = admin
            .query(
                "SELECT ID FROM information_schema.PROCESSLIST \
                 WHERE INFO LIKE 'INSERT INTO%' ORDER BY ID",
            )
            .await
            .expect("processlist");
        for id in &waiting {
            exec(&mut admin, &format!("KILL {id}")).await;
            killed = true;
        }
        if killed {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(killed, "the INSERT never reached the server");

    let (mut sink, outcome) = blocked.await.expect("the write task");
    outcome.expect_err("the connection was dropped mid-flush");
    exec(&mut conn, "UNLOCK TABLES").await;
    assert_eq!(sink.pending_rows(), Some(0));

    sink.write_batch(&redriven).await.expect("the retry");
    assert_eq!(
        grid(&mut conn, "SELECT id FROM lost ORDER BY id").await,
        strings(&[&["1"], &["2"], &["3"]]),
        "each row once"
    );
}

/// How long a dropped call runs first, in microseconds: from not at all to long
/// enough to finish, so the drop lands in the connect, the catalog reads, the
/// transaction and the commit in turn.
const DROP_AFTER_MICROS: [u64; 14] = [
    0, 100, 200, 300, 500, 800, 1_200, 2_000, 3_000, 5_000, 8_000, 13_000, 21_000, 40_000,
];

/// A runner that `select!`s against a cancel token drops the call it was
/// awaiting. Dropped while its `INSERT` waits on a table lock (so mid-statement
/// and mid-transaction), the connection must not be reused: the next call
/// would read the dropped call's reply, or implicitly commit its transaction.
async fn a_write_dropped_mid_statement_is_redriven_once(server: &Server) {
    let mut conn = server.conn().await;
    let mut admin = server.conn().await;
    exec(
        &mut conn,
        "CREATE TABLE dropped (id BIGINT NOT NULL, v VARCHAR(10))",
    )
    .await;
    let mut sink = id_value_sink(server, "dropped", "append", "");
    let first = id_value_batch(&sink, &[(1, "a")]);
    sink.write_batch(&first).await.expect("opens the session");

    exec(&mut conn, "LOCK TABLES dropped WRITE").await;
    let batch = id_value_batch(&sink, &[(2, "b"), (3, "c")]);
    {
        let write = sink.write_batch(&batch);
        tokio::pin!(write);
        let parked = async {
            loop {
                let waiting: Vec<u64> = admin
                    .query(
                        "SELECT ID FROM information_schema.PROCESSLIST \
                         WHERE INFO LIKE 'INSERT INTO%dropped%'",
                    )
                    .await
                    .expect("processlist");
                if !waiting.is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        };
        tokio::select! {
            outcome = &mut write => panic!("the INSERT cannot finish under the lock: {outcome:?}"),
            () = parked => {}
        }
        // `write` is dropped here, with its transaction open on the server.
    }
    exec(&mut conn, "UNLOCK TABLES").await;

    sink.write_batch(&batch).await.expect("the re-drive");
    sink.finish().await.expect("nothing left to write");
    assert_eq!(sink.pending_rows(), Some(0));
    assert_eq!(
        grid(&mut conn, "SELECT id, v FROM dropped ORDER BY id").await,
        strings(&[&["1", "a"], &["2", "b"], &["3", "c"]]),
        "each row once: the dropped call's transaction is not committed by the next one"
    );
}

/// `table` rows as `id=v` text, in key order.
async fn pairs(conn: &mut Conn, table: &str) -> Vec<String> {
    grid(conn, &format!("SELECT id, v FROM {table} ORDER BY id"))
        .await
        .into_iter()
        .map(|row| format!("{}={}", row[0], row[1]))
        .collect()
}

/// Drop `write_batch` after each interval in turn, the sink warm and cold, with
/// rows an earlier call accepted waiting in its buffer. The caller re-drives
/// the batch it dropped, so the table must end with every row exactly once,
/// the accepted ones included, and no call may fail or panic on the way.
async fn a_dropped_flush_at_any_point_loses_nothing_and_repeats_nothing(server: &Server) {
    let mut conn = server.conn().await;
    let mut n = 0;
    for primed in [false, true] {
        for micros in DROP_AFTER_MICROS {
            n += 1;
            let table = format!("sweep_flush_{n}");
            let at = format!("primed={primed} after {micros} us");
            exec(
                &mut conn,
                &format!("CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(10))"),
            )
            .await;
            let mut sink = id_value_sink(
                server,
                &table,
                "upsert",
                "conflict_columns \"id\"\nflush_rows 5\nchunk_rows 2\n",
            );
            let mut expected = Vec::new();
            if primed {
                let warm = id_value_batch(
                    &sink,
                    &[(100, "w"), (101, "w"), (102, "w"), (103, "w"), (104, "w")],
                );
                sink.write_batch(&warm).await.expect("warms the session");
                expected.extend((100..105).map(|id| format!("{id}=w")));
            }
            let accepted = id_value_batch(&sink, &[(1, "a"), (2, "b")]);
            sink.write_batch(&accepted).await.expect("buffered");
            let own = id_value_batch(&sink, &[(3, "c"), (4, "d"), (5, "e")]);
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
            let mut want: Vec<String> = ["1=a", "2=b", "3=c", "4=d", "5=e"]
                .iter()
                .map(ToString::to_string)
                .collect();
            want.extend(expected);
            assert_eq!(pairs(&mut conn, &table).await, want, "{at}");
        }
    }
}

/// The same sweep over `finish`: a dropped one keeps what it held, and a second
/// `finish` writes it.
async fn a_dropped_finish_at_any_point_loses_nothing(server: &Server) {
    let mut conn = server.conn().await;
    let mut n = 0;
    for primed in [false, true] {
        for micros in DROP_AFTER_MICROS {
            n += 1;
            let table = format!("sweep_finish_{n}");
            let at = format!("primed={primed} after {micros} us");
            exec(
                &mut conn,
                &format!("CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(10))"),
            )
            .await;
            let mut sink = id_value_sink(
                server,
                &table,
                "upsert",
                "conflict_columns \"id\"\nflush_rows 100\nchunk_rows 1\n",
            );
            let mut want = Vec::new();
            if primed {
                let warm = id_value_batch(&sink, &[(100, "w")]);
                sink.write_batch(&warm).await.expect("buffered");
                sink.finish().await.expect("warms the session");
                want.push("100=w".to_string());
            }
            let held = id_value_batch(&sink, &[(1, "a"), (2, "b"), (3, "c")]);
            sink.write_batch(&held).await.expect("buffered");
            let outcome =
                tokio::time::timeout(std::time::Duration::from_micros(micros), sink.finish()).await;
            if let Ok(result) = outcome {
                result.unwrap_or_else(|e| panic!("{at}: {e}"));
            }
            sink.finish()
                .await
                .unwrap_or_else(|e| panic!("{at}: the second finish: {e}"));
            assert_eq!(sink.pending_rows(), Some(0), "{at}");
            want.extend(["1=a", "2=b", "3=c"].iter().map(ToString::to_string));
            want.sort();
            let mut got = pairs(&mut conn, &table).await;
            got.sort();
            assert_eq!(got, want, "{at}");
        }
    }
}

/// A dropped first flush may or may not have committed its `DELETE`, so the
/// sink cannot know the target was cleared: the re-driven write clears it again
/// and the table ends with the new rows once and none of the old.
async fn a_dropped_truncating_flush_still_truncates_once(server: &Server) {
    let mut conn = server.conn().await;
    for (n, micros) in DROP_AFTER_MICROS.into_iter().enumerate() {
        let table = format!("sweep_truncate_{n}");
        let at = format!("after {micros} us");
        exec(
            &mut conn,
            &format!("CREATE TABLE {table} (id BIGINT NOT NULL, v VARCHAR(10))"),
        )
        .await;
        exec(
            &mut conn,
            &format!("INSERT INTO {table} VALUES (900, 'old'), (901, 'old')"),
        )
        .await;
        let mut sink = id_value_sink(
            server,
            &table,
            "append",
            "truncate_before_first_write #true\n",
        );
        let own = id_value_batch(&sink, &[(1, "a"), (2, "b")]);
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
        assert_eq!(pairs(&mut conn, &table).await, ["1=a", "2=b"], "{at}");
    }
}
