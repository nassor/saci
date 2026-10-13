//! `polling` against a real MariaDB and a real MySQL server.
//!
//! Soft-skips a flavour whose container cannot start; see `common::try_start`.

mod common;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use common::Server;
use mysql_async::prelude::Queryable;
use saci_core::io::source::Source;

common::on_every_server!(
    polling_batches_the_table_then_resumes_from_the_persisted_offset,
    a_new_source_resumes_from_the_offset_table,
    initial_now_skips_existing_rows,
    a_tiebreak_column_keeps_a_non_unique_cursor_lossless,
    a_where_clause_narrows_every_query,
    max_batches_per_cycle_ends_the_cycle_early_and_resumes,
    a_bigint_cursor_above_two_to_the_53_resumes_exactly,
    a_bigint_unsigned_cursor_above_two_to_the_63_resumes_exactly,
    a_datetime_cursor_resumes_exactly,
    a_declared_column_the_table_lacks_names_itself,
    a_precreated_offset_table_needs_no_create_privilege,
    request_batch_rows_changes_the_next_limit,
    a_table_altered_between_cycles_is_read_with_its_new_shape,
    a_null_tiebreak_value_is_an_error_not_a_skipped_run,
    a_next_batch_dropped_during_session_setup_still_resumes_from_the_stored_offset,
    every_cursor_type_resumes_from_the_stored_offset_at_its_extremes,
    a_polled_auto_increment_cursor_does_not_skip_a_late_commit,
    a_cursor_that_is_not_auto_increment_is_never_held_back,
    gap_timeout_zero_reads_past_a_hole_at_once,
    a_where_clause_with_the_hold_off_reads_past_a_hole,
    a_where_clause_over_an_auto_increment_cursor_still_holds_back_a_late_commit,
    a_where_clause_that_drops_whole_windows_still_advances,
    a_where_clause_and_a_tiebreak_over_a_non_unique_cursor_keep_every_tie,
    a_deleted_row_hole_is_released_at_once_when_innodb_trx_is_readable,
    a_deleted_row_hole_waits_the_timeout_without_innodb_trx,
    a_filtered_tie_run_cut_by_a_batch_is_finished_with_no_later_number,
    a_writer_waiting_on_a_lock_still_holds_a_hole_open,
);

const PRICES_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"label\" type=\"utf8\"
";

fn i64_column(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column_by_name(name)
                .expect("column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column");
            (0..column.len())
                .map(|row| column.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    i64_column(batches, "id")
}

fn sizes(batches: &[RecordBatch]) -> Vec<usize> {
    batches.iter().map(RecordBatch::num_rows).collect()
}

/// `INSERT INTO prices VALUES (n, 'p<n>'), ...` for `from..=to`.
fn insert_prices(from: i64, to: i64) -> String {
    let values = (from..=to)
        .map(|n| format!("({n}, 'p{n}')"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO prices VALUES {values}")
}

async fn stored_cursor(server: &Server, name: &str) -> Option<String> {
    let mut conn = server.conn().await;
    conn.exec_first(
        "SELECT cursor_value FROM saci_source_offsets WHERE source_name = ?",
        (name,),
    )
    .await
    .expect("read the offset row")
}

async fn prices(server: &Server, rows: i64) {
    let mut conn = server.conn().await;
    conn.query_drop("CREATE TABLE prices (id BIGINT PRIMARY KEY, label VARCHAR(20))")
        .await
        .expect("create");
    conn.query_drop(insert_prices(1, rows)).await.expect("seed");
}

async fn polling_batches_the_table_then_resumes_from_the_persisted_offset(server: &Server) {
    prices(server, 10).await;
    let mut conn = server.conn().await;

    let mut source = common::source(
        &server.dsn(),
        &format!(
            "name \"prices\"\nbatch_rows 4\n\n\
             mode kind=\"polling\" table=\"prices\" cursor_column=\"id\"\n{PRICES_FIELDS}"
        ),
    );

    let batches = common::drain(&mut source).await;
    assert_eq!(
        sizes(&batches),
        vec![4, 4, 2],
        "batch_rows = 4 over 10 rows"
    );
    assert_eq!(ids(&batches), (1..=10).collect::<Vec<_>>());
    let labels = batches[0]
        .column_by_name("label")
        .expect("label")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8");
    assert_eq!(labels.value(0), "p1");

    conn.query_drop(insert_prices(11, 13))
        .await
        .expect("insert more");
    let batches = common::drain(&mut source).await;
    assert_eq!(ids(&batches), vec![11, 12, 13]);

    assert_eq!(
        stored_cursor(server, "prices").await.as_deref(),
        Some("10"),
        "the offset is committed one cycle late"
    );
}

async fn a_new_source_resumes_from_the_offset_table(server: &Server) {
    prices(server, 5).await;
    let body = format!(
        "name \"prices\"\nbatch_rows 100\n\n\
         mode kind=\"polling\" table=\"prices\" cursor_column=\"id\"\n{PRICES_FIELDS}"
    );

    let mut first = common::source(&server.dsn(), &body);
    assert_eq!(ids(&common::drain(&mut first).await), vec![1, 2, 3, 4, 5]);
    // The second cycle commits the first one's offset.
    assert!(common::drain(&mut first).await.is_empty());
    drop(first);

    let mut conn = server.conn().await;
    conn.query_drop(insert_prices(6, 8))
        .await
        .expect("insert more");

    let mut second = common::source(&server.dsn(), &body);
    assert_eq!(
        ids(&common::drain(&mut second).await),
        vec![6, 7, 8],
        "a fresh instance of the same source starts at the stored offset"
    );
}

async fn initial_now_skips_existing_rows(server: &Server) {
    prices(server, 5).await;
    let mut conn = server.conn().await;
    let body = "name \"prices\"\nbatch_rows 100\n\n\
         mode kind=\"polling\" table=\"prices\" cursor_column=\"id\" initial=\"now\"\n";

    let mut source = common::source(&server.dsn(), &format!("{body}{PRICES_FIELDS}"));
    assert!(
        common::drain(&mut source).await.is_empty(),
        "nothing is new yet"
    );

    conn.query_drop(insert_prices(6, 7)).await.expect("insert");
    assert_eq!(ids(&common::drain(&mut source).await), vec![6, 7]);

    // "now" was persisted by the cycle after the first, so a restart does not
    // re-evaluate it and skip what arrived while it was down.
    assert!(common::drain(&mut source).await.is_empty());
    drop(source);
    conn.query_drop(insert_prices(8, 8)).await.expect("insert");
    let mut again = common::source(&server.dsn(), &format!("{body}{PRICES_FIELDS}"));
    assert_eq!(ids(&common::drain(&mut again).await), vec![8]);
}

async fn a_tiebreak_column_keeps_a_non_unique_cursor_lossless(server: &Server) {
    let mut conn = server.conn().await;
    conn.query_drop(
        "CREATE TABLE events (id BIGINT PRIMARY KEY, grp INT NOT NULL, KEY k_grp_id (grp, id))",
    )
    .await
    .expect("create");
    // Five rows share each of the first two cursor values, so a batch boundary
    // of 4 lands inside a run of equal cursors twice.
    let values = (1..=12)
        .map(|n| {
            format!(
                "({n}, {})",
                if n <= 5 {
                    1
                } else if n <= 10 {
                    2
                } else {
                    3
                }
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    conn.query_drop(format!("INSERT INTO events VALUES {values}"))
        .await
        .expect("seed");

    let mut source = common::source(
        &server.dsn(),
        "name \"events\"\nbatch_rows 4\n\n\
         mode kind=\"polling\" table=\"events\" cursor_column=\"grp\" tiebreak_column=\"id\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"grp\" type=\"int32\" nullable=#false\n",
    );
    let batches = common::drain(&mut source).await;
    assert_eq!(sizes(&batches), vec![4, 4, 4]);
    assert_eq!(
        ids(&batches),
        (1..=12).collect::<Vec<_>>(),
        "no row skipped or repeated"
    );

    conn.query_drop("INSERT INTO events VALUES (13, 3), (14, 4)")
        .await
        .expect("insert");
    assert_eq!(ids(&common::drain(&mut source).await), vec![13, 14]);
}

async fn a_where_clause_narrows_every_query(server: &Server) {
    prices(server, 9).await;
    let mut conn = server.conn().await;
    conn.query_drop("UPDATE prices SET label = 'skip' WHERE id % 2 = 0")
        .await
        .expect("mark");

    let mut source = common::source(
        &server.dsn(),
        &format!(
            "name \"prices\"\nbatch_rows 2\n\n\
             mode kind=\"polling\" table=\"prices\" cursor_column=\"id\" \
             where_clause=\"label <> 'skip'\"\n{PRICES_FIELDS}"
        ),
    );
    let batches = common::drain(&mut source).await;
    assert_eq!(ids(&batches), vec![1, 3, 5, 7, 9]);
    assert_eq!(
        sizes(&batches),
        vec![2, 2, 1],
        "the resumed query keeps the filter too"
    );
}

async fn max_batches_per_cycle_ends_the_cycle_early_and_resumes(server: &Server) {
    prices(server, 10).await;
    let mut source = common::source(
        &server.dsn(),
        &format!(
            "name \"prices\"\nbatch_rows 2\nmax_batches_per_cycle 2\n\n\
             mode kind=\"polling\" table=\"prices\" cursor_column=\"id\"\n{PRICES_FIELDS}"
        ),
    );
    let mut cycles = Vec::new();
    for _ in 0..6 {
        let batches = common::drain(&mut source).await;
        if batches.is_empty() {
            break;
        }
        cycles.push(ids(&batches));
    }
    assert_eq!(
        cycles,
        vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8], vec![9, 10]],
        "each cycle stops at two batches and the next resumes after it"
    );
}

async fn a_bigint_cursor_above_two_to_the_53_resumes_exactly(server: &Server) {
    // Above 2^53 a double cannot tell neighbours apart.
    let base: i64 = 9_007_199_254_740_992;
    let mut conn = server.conn().await;
    conn.query_drop("CREATE TABLE big (id BIGINT PRIMARY KEY, label VARCHAR(20))")
        .await
        .expect("create");
    let values = (1..=6)
        .map(|n| format!("({}, 'b{n}')", base + n))
        .collect::<Vec<_>>()
        .join(", ");
    conn.query_drop(format!("INSERT INTO big VALUES {values}"))
        .await
        .expect("seed");

    let body = format!(
        "name \"big\"\nbatch_rows 2\nmax_batches_per_cycle 1\n\n\
         mode kind=\"polling\" table=\"big\" cursor_column=\"id\"\n{PRICES_FIELDS}"
    );
    let mut first = common::source(&server.dsn(), &body);
    assert_eq!(
        ids(&common::drain(&mut first).await),
        vec![base + 1, base + 2]
    );
    // The next cycle commits the first one's last row before it reads on.
    assert_eq!(
        ids(&common::drain(&mut first).await),
        vec![base + 3, base + 4]
    );
    assert_eq!(
        stored_cursor(server, "big").await,
        Some((base + 2).to_string()),
        "the offset text is the exact value"
    );
    drop(first);

    let mut second = common::source(&server.dsn(), &body);
    assert_eq!(
        ids(&common::drain(&mut second).await),
        vec![base + 3, base + 4],
        "a new instance resumes after exactly base + 2"
    );
}

async fn a_bigint_unsigned_cursor_above_two_to_the_63_resumes_exactly(server: &Server) {
    // MySQL types `CAST(? AS UNSIGNED)` over a text parameter as signed, so
    // this cursor must bind as an unsigned integer.
    let base: u64 = 9_223_372_036_854_775_808;
    let mut conn = server.conn().await;
    conn.query_drop("CREATE TABLE ubig (id BIGINT UNSIGNED PRIMARY KEY, label VARCHAR(20))")
        .await
        .expect("create");
    let values = (1..=6)
        .map(|n| format!("({}, 'u{n}')", base + n))
        .collect::<Vec<_>>()
        .join(", ");
    conn.query_drop(format!(
        "INSERT INTO ubig VALUES {values}, ({}, 'top')",
        u64::MAX
    ))
    .await
    .expect("seed");

    let body = "name \"ubig\"\nbatch_rows 2\nmax_batches_per_cycle 1\n\n\
         mode kind=\"polling\" table=\"ubig\" cursor_column=\"id\"\n\
         schema_fields \"id\" type=\"uint64\" nullable=#false\n";
    let read = |batches: &[RecordBatch]| -> Vec<u64> {
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name("id")
                    .expect("id")
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("uint64");
                (0..column.len())
                    .map(|row| column.value(row))
                    .collect::<Vec<_>>()
            })
            .collect()
    };

    let mut first = common::source(&server.dsn(), body);
    assert_eq!(
        read(&common::drain(&mut first).await),
        vec![base + 1, base + 2]
    );
    assert_eq!(
        read(&common::drain(&mut first).await),
        vec![base + 3, base + 4]
    );
    drop(first);

    let mut second = common::source(&server.dsn(), body);
    assert_eq!(
        read(&common::drain(&mut second).await),
        vec![base + 3, base + 4]
    );
    assert_eq!(
        read(&common::drain(&mut second).await),
        vec![base + 5, base + 6]
    );
    assert_eq!(read(&common::drain(&mut second).await), vec![u64::MAX]);
}

async fn a_datetime_cursor_resumes_exactly(server: &Server) {
    let mut conn = server.conn().await;
    conn.query_drop("CREATE TABLE ticks (id BIGINT PRIMARY KEY, at DATETIME(6) NOT NULL)")
        .await
        .expect("create");
    // Neighbours one microsecond apart: a cursor that lost the fraction would
    // repeat or skip rows.
    conn.query_drop(
        "INSERT INTO ticks VALUES \
         (1, '2024-01-30 10:20:30.123456'), (2, '2024-01-30 10:20:30.123457'), \
         (3, '2024-01-30 10:20:30.123458'), (4, '2024-01-30 10:20:31.000000'), \
         (5, '2024-01-30 10:20:31.000001')",
    )
    .await
    .expect("seed");

    let body = "name \"ticks\"\nbatch_rows 2\nmax_batches_per_cycle 1\n\n\
         mode kind=\"polling\" table=\"ticks\" cursor_column=\"at\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"at\" type=\"timestamp_micros\" nullable=#false\n";
    let mut first = common::source(&server.dsn(), body);
    assert_eq!(ids(&common::drain(&mut first).await), vec![1, 2]);
    assert_eq!(ids(&common::drain(&mut first).await), vec![3, 4]);
    assert_eq!(
        stored_cursor(server, "ticks").await.as_deref(),
        Some("2024-01-30 10:20:30.123457")
    );
    drop(first);

    let mut second = common::source(&server.dsn(), body);
    assert_eq!(ids(&common::drain(&mut second).await), vec![3, 4]);
    assert_eq!(ids(&common::drain(&mut second).await), vec![5]);
}

async fn a_declared_column_the_table_lacks_names_itself(server: &Server) {
    prices(server, 2).await;
    let mut source = common::source(
        &server.dsn(),
        "name \"prices\"\nbatch_rows 10\n\n\
         mode kind=\"polling\" table=\"prices\" cursor_column=\"id\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"colour\" type=\"utf8\"\n",
    );
    let err = source
        .next_batch()
        .await
        .expect_err("an undeclared column fails the read");
    let message = err.message();
    assert!(message.contains("colour"), "{message}");
    assert!(message.contains("MariadbSource"), "{message}");
    assert!(
        !message.contains("root:saci"),
        "the DSN never reaches an error: {message}"
    );
}

async fn a_precreated_offset_table_needs_no_create_privilege(server: &Server) {
    prices(server, 3).await;
    let mut root = server.conn().await;
    root.query_drop(
        "CREATE TABLE saci_source_offsets (source_name VARCHAR(191) NOT NULL PRIMARY KEY, \
         cursor_value TEXT NOT NULL, tiebreak_value TEXT NULL, \
         updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) \
         ON UPDATE CURRENT_TIMESTAMP(6))",
    )
    .await
    .expect("create offsets");
    root.query_drop("CREATE USER 'reader'@'%' IDENTIFIED BY 'pw'")
        .await
        .expect("create user");
    root.query_drop("GRANT SELECT, INSERT, UPDATE ON saci.* TO 'reader'@'%'")
        .await
        .expect("grant");

    let mut source = common::source(
        &server.dsn_as("reader", "pw"),
        &format!(
            "name \"prices\"\nbatch_rows 2\n\n\
             mode kind=\"polling\" table=\"prices\" cursor_column=\"id\"\n{PRICES_FIELDS}"
        ),
    );
    assert_eq!(ids(&common::drain(&mut source).await), vec![1, 2, 3]);
    assert!(common::drain(&mut source).await.is_empty());
    assert_eq!(stored_cursor(server, "prices").await.as_deref(), Some("3"));
}

async fn request_batch_rows_changes_the_next_limit(server: &Server) {
    prices(server, 12).await;
    let mut source = common::source(
        &server.dsn(),
        &format!(
            "name \"prices\"\nbatch_rows 100\n\n\
             mode kind=\"polling\" table=\"prices\" cursor_column=\"id\"\n{PRICES_FIELDS}"
        ),
    );
    source.request_batch_rows(5);
    let first = source.next_batch().await.expect("batch").expect("rows");
    assert_eq!(first.num_rows(), 5);
    // The hint lands on a source that already ran a query: the very next one
    // asks for the new size.
    source.request_batch_rows(3);
    let second = source.next_batch().await.expect("batch").expect("rows");
    assert_eq!(second.num_rows(), 3);
}

async fn a_table_altered_between_cycles_is_read_with_its_new_shape(server: &Server) {
    // MySQL 8.4 and 9 answer a cached prepared statement over a redefined table
    // with stale values, so the source keeps no statement cache.
    let mut conn = server.conn().await;
    conn.query_drop("CREATE TABLE shifty (id BIGINT PRIMARY KEY, a INT, b VARCHAR(10))")
        .await
        .expect("create");
    conn.query_drop("INSERT INTO shifty VALUES (1, 10, 'x'), (2, 20, 'y')")
        .await
        .expect("seed");
    let mut source = common::source(
        &server.dsn(),
        "name \"shifty\"\nbatch_rows 10\n\n\
         mode kind=\"polling\" table=\"shifty\" cursor_column=\"id\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"a\" type=\"int64\"\n\
         schema_fields \"b\" type=\"utf8\"\n",
    );
    assert_eq!(ids(&common::drain(&mut source).await), vec![1, 2]);

    // Same statement text over a table that is dropped and rebuilt with
    // another layout: `b` moves ahead of `a` and `a` widens to BIGINT.
    conn.query_drop("DROP TABLE shifty").await.expect("drop");
    conn.query_drop("CREATE TABLE shifty (id BIGINT PRIMARY KEY, b VARCHAR(10), a BIGINT)")
        .await
        .expect("recreate");
    conn.query_drop("INSERT INTO shifty VALUES (3, 'z', 5000000000)")
        .await
        .expect("insert");
    let batches = common::drain(&mut source).await;
    assert_eq!(ids(&batches), vec![3]);
    assert_eq!(i64_column(&batches, "a"), vec![5_000_000_000]);
    assert_eq!(
        batches[0]
            .column_by_name("b")
            .expect("b")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8")
            .value(0),
        "z"
    );
}

/// A tiebreak value that is NULL cannot be resumed from: `tiebreak > ?` matches
/// nothing, and an offset without a tiebreak would continue at the next cursor
/// value and skip the rest of the run. It is an error, as a NULL cursor is.
async fn a_null_tiebreak_value_is_an_error_not_a_skipped_run(server: &Server) {
    let mut conn = server.conn().await;
    conn.query_drop(
        "CREATE TABLE nullties (id BIGINT PRIMARY KEY, grp INT NOT NULL, seq INT NULL, \
         KEY k_grp_seq (grp, seq))",
    )
    .await
    .expect("create");
    conn.query_drop("INSERT INTO nullties VALUES (1, 1, NULL), (2, 1, 5), (3, 1, 6), (4, 2, 1)")
        .await
        .expect("seed");

    let mut source = common::source(
        &server.dsn(),
        "name \"nullties\"\nbatch_rows 1\n\n\
         mode kind=\"polling\" table=\"nullties\" cursor_column=\"grp\" tiebreak_column=\"seq\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"grp\" type=\"int32\" nullable=#false\n\
         schema_fields \"seq\" type=\"int32\"\n",
    );
    let err = source
        .next_batch()
        .await
        .expect_err("rows 2 and 3 would be skipped");
    assert!(
        err.message().contains("tiebreak column 'seq'") && err.message().contains("NULL"),
        "{}",
        err.message()
    );
}

/// A `next_batch` the host drops while the session is still being set up (the
/// stream runner does it on cancellation) must not leave a connection that is
/// taken for ready: the next call would read from the start of the table
/// instead of the stored offset.
async fn a_next_batch_dropped_during_session_setup_still_resumes_from_the_stored_offset(
    server: &Server,
) {
    prices(server, 6).await;
    let body = format!(
        "name \"cancelled\"\nbatch_rows 3\nmax_batches_per_cycle 1\n\n\
         mode kind=\"polling\" table=\"prices\" cursor_column=\"id\"\n{PRICES_FIELDS}"
    );
    // The second cycle stores 3 and reads 4..=6.
    let mut seeded = common::source(&server.dsn(), &body);
    assert_eq!(ids(&common::drain(&mut seeded).await), vec![1, 2, 3]);
    assert_eq!(ids(&common::drain(&mut seeded).await), vec![4, 5, 6]);
    drop(seeded);
    assert_eq!(
        stored_cursor(server, "cancelled").await.as_deref(),
        Some("3")
    );

    for micros in (300..40_000).step_by(700) {
        let mut source = common::source(&server.dsn(), &body);
        let cancelled = tokio::time::timeout(
            std::time::Duration::from_micros(micros),
            source.next_batch(),
        )
        .await;
        let batch = match cancelled {
            Ok(done) => done,
            Err(_elapsed) => source.next_batch().await,
        };
        let batch = batch
            .unwrap_or_else(|e| panic!("after a cancel at {micros}us: {}", e.message()))
            .expect("rows");
        assert_eq!(
            ids(&[batch]),
            vec![4, 5, 6],
            "after a cancel at {micros}us the stored offset still applies"
        );
    }
}

/// Six rows whose cursor order is `id` order, read two at a time: a restart
/// after the second cycle resumes from the stored offset and delivers the
/// third and fourth rows again, then the last two. `mode` is the `mode` line
/// after `kind="polling"`.
async fn resumes_in_pairs(
    server: &Server,
    table: &str,
    create: &str,
    insert: &str,
    mode: &str,
    fields: &str,
) {
    let mut conn = server.conn().await;
    conn.query_drop(create).await.expect("create");
    conn.query_drop(insert).await.expect("seed");
    let body = format!(
        "name \"{table}\"\nbatch_rows 2\nmax_batches_per_cycle 1\n\n\
         mode kind=\"polling\" table=\"{table}\" {mode}\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n{fields}"
    );
    let mut first = common::source(&server.dsn(), &body);
    assert_eq!(ids(&common::drain(&mut first).await), vec![1, 2], "{table}");
    assert_eq!(ids(&common::drain(&mut first).await), vec![3, 4], "{table}");
    drop(first);

    let mut second = common::source(&server.dsn(), &body);
    assert_eq!(
        ids(&common::drain(&mut second).await),
        vec![3, 4],
        "{table}: resumed from the stored offset"
    );
    assert_eq!(
        ids(&common::drain(&mut second).await),
        vec![5, 6],
        "{table}"
    );
    assert!(common::drain(&mut second).await.is_empty(), "{table}");
}

/// Timestamp, date, signed small integer and text cursors, at their ends.
async fn every_cursor_type_resumes_from_the_stored_offset_at_its_extremes(server: &Server) {
    resumes_in_pairs(
        server,
        "ts_cursor",
        "CREATE TABLE ts_cursor (id BIGINT PRIMARY KEY, at TIMESTAMP(6) NOT NULL DEFAULT '2000-01-01 00:00:01')",
        "INSERT INTO ts_cursor VALUES (1, '1970-01-01 00:00:01.000000'), \
         (2, '2024-01-30 10:20:30.123456'), (3, '2024-01-30 10:20:30.123457'), \
         (4, '2024-01-30 10:20:31.000000'), (5, '2024-01-30 10:20:31.000001'), \
         (6, '2038-01-19 03:14:07.999999')",
        "cursor_column=\"at\"",
        "schema_fields \"at\" type=\"timestamp_micros_utc\" nullable=#false\n",
    )
    .await;
    resumes_in_pairs(
        server,
        "date_cursor",
        "CREATE TABLE date_cursor (id BIGINT PRIMARY KEY, d DATE NOT NULL)",
        "INSERT INTO date_cursor VALUES (1, '0001-01-01'), (2, '1970-01-01'), \
         (3, '1999-12-31'), (4, '2000-02-29'), (5, '2024-01-30'), (6, '9999-12-31')",
        "cursor_column=\"d\"",
        "schema_fields \"d\" type=\"date32\" nullable=#false\n",
    )
    .await;
    resumes_in_pairs(
        server,
        "tiny_cursor",
        "CREATE TABLE tiny_cursor (id BIGINT PRIMARY KEY, n TINYINT NOT NULL)",
        "INSERT INTO tiny_cursor VALUES (1, -128), (2, -2), (3, -1), (4, 0), (5, 1), (6, 127)",
        "cursor_column=\"n\"",
        "schema_fields \"n\" type=\"int8\" nullable=#false\n",
    )
    .await;
    resumes_in_pairs(
        server,
        "text_cursor",
        "CREATE TABLE text_cursor (id BIGINT PRIMARY KEY, name VARCHAR(20) NOT NULL, \
         KEY k_name_id (name, id))",
        // Case and accent variants tie under the default collations, which the
        // id then orders.
        "INSERT INTO text_cursor VALUES (1, 'apple'), (2, 'Apple'), (3, 'apple'), \
         (4, 'banana'), (5, 'Banana'), (6, 'cherry')",
        "cursor_column=\"name\" tiebreak_column=\"id\"",
        "schema_fields \"name\" type=\"utf8\" nullable=#false\n",
    )
    .await;
}

// ------------------------------------------------- commit order and sequence holes

async fn events(server: &Server) {
    let mut conn = server.conn().await;
    conn.query_drop(
        "CREATE TABLE events (id BIGINT AUTO_INCREMENT PRIMARY KEY, label VARCHAR(20))",
    )
    .await
    .expect("create");
}

fn events_source(server: &Server, mode_extra: &str) -> saci_connector_mariadb::MariadbSource {
    common::source(
        &server.dsn(),
        &format!(
            "name \"events\"\nbatch_rows 100\n\n\
             mode kind=\"polling\" table=\"events\" cursor_column=\"id\" {mode_extra}\n\
             {PRICES_FIELDS}"
        ),
    )
}

/// Transaction A takes an `AUTO_INCREMENT` value and stays open, B takes the
/// next and commits, a drain reads B's row, and A commits. The value is handed
/// out when the row is written, so A's row shows up behind a cursor that has
/// already passed it.
async fn a_polled_auto_increment_cursor_does_not_skip_a_late_commit(server: &Server) {
    events(server).await;
    let mut conn = server.conn().await;
    let mut source = events_source(server, "");
    conn.query_drop("INSERT INTO events (label) VALUES ('first')")
        .await
        .expect("first");
    assert_eq!(ids(&common::drain(&mut source).await), vec![1]);

    let mut a =
        common::open_transaction(server, "INSERT INTO events (label) VALUES ('slow')").await;
    conn.query_drop("INSERT INTO events (label) VALUES ('fast')")
        .await
        .expect("fast");
    let before = common::drain(&mut source).await;

    a.query_drop("COMMIT").await.expect("commit");
    let after = common::drain(&mut source).await;

    let all: Vec<RecordBatch> = before.iter().chain(&after).cloned().collect();
    assert_eq!(
        ids(&all),
        vec![2, 3],
        "the slow writer's row, committed after the fast one's, must arrive"
    );
    assert!(
        before.is_empty(),
        "a row behind an open hole is held back, not handed over: {:?}",
        ids(&before)
    );
}

/// A cursor the server does not number is not a sequence, so a jump in it is
/// not a hole and nothing waits.
async fn a_cursor_that_is_not_auto_increment_is_never_held_back(server: &Server) {
    let mut conn = server.conn().await;
    conn.query_drop("CREATE TABLE sparse (id BIGINT PRIMARY KEY, label VARCHAR(20))")
        .await
        .expect("create");
    conn.query_drop("INSERT INTO sparse VALUES (1, 'a'), (5, 'b'), (9, 'c')")
        .await
        .expect("seed");
    let mut source = common::source(
        &server.dsn(),
        &format!(
            "name \"sparse\"\nbatch_rows 100\n\n\
             mode kind=\"polling\" table=\"sparse\" cursor_column=\"id\"\n{PRICES_FIELDS}"
        ),
    );
    assert_eq!(ids(&common::drain(&mut source).await), vec![1, 5, 9]);
}

/// `gap_timeout_ms 0` is the opt-out: rows go out past a hole at once, and a
/// row that commits later is not delivered.
async fn gap_timeout_zero_reads_past_a_hole_at_once(server: &Server) {
    events(server).await;
    let mut conn = server.conn().await;
    let mut source = events_source(server, "gap_timeout_ms=0");
    conn.query_drop("INSERT INTO events (label) VALUES ('first')")
        .await
        .expect("first");
    assert_eq!(ids(&common::drain(&mut source).await), vec![1]);

    let mut a =
        common::open_transaction(server, "INSERT INTO events (label) VALUES ('slow')").await;
    conn.query_drop("INSERT INTO events (label) VALUES ('fast')")
        .await
        .expect("fast");
    assert_eq!(ids(&common::drain(&mut source).await), vec![3]);
    a.query_drop("COMMIT").await.expect("commit");
    assert!(common::drain(&mut source).await.is_empty());
}

/// The hold off reads past holes whatever the filter: nothing is checked.
async fn a_where_clause_with_the_hold_off_reads_past_a_hole(server: &Server) {
    events(server).await;
    let mut conn = server.conn().await;
    conn.query_drop("INSERT INTO events (label) VALUES ('keep'), ('drop'), ('keep')")
        .await
        .expect("seed");
    let mut open = events_source(server, "where_clause=\"label = 'keep'\" gap_timeout_ms=0");
    assert_eq!(ids(&common::drain(&mut open).await), vec![1, 3]);
}

/// A filter must not hide a hole: holes are found on the unfiltered numbers,
/// and a row the filter drops is a number, not a hole.
async fn a_where_clause_over_an_auto_increment_cursor_still_holds_back_a_late_commit(
    server: &Server,
) {
    events(server).await;
    let mut conn = server.conn().await;
    let mut source = events_source(server, "where_clause=\"label = 'keep'\"");
    conn.query_drop("INSERT INTO events (label) VALUES ('keep')")
        .await
        .expect("first");
    assert_eq!(ids(&common::drain(&mut source).await), vec![1]);

    let mut a =
        common::open_transaction(server, "INSERT INTO events (label) VALUES ('keep')").await;
    conn.query_drop("INSERT INTO events (label) VALUES ('keep')")
        .await
        .expect("fast");
    conn.query_drop("INSERT INTO events (label) VALUES ('drop')")
        .await
        .expect("dropped by the filter");
    let before = common::drain(&mut source).await;

    a.query_drop("COMMIT").await.expect("commit");
    let after = common::drain(&mut source).await;

    let all: Vec<RecordBatch> = before.iter().chain(&after).cloned().collect();
    assert_eq!(
        ids(&all),
        vec![2, 3],
        "the slow writer's row must arrive, and the filtered one must not"
    );
    assert!(
        before.is_empty(),
        "a row behind an open hole is held back: {:?}",
        ids(&before)
    );

    conn.query_drop("INSERT INTO events (label) VALUES ('drop'), ('keep')")
        .await
        .expect("a dropped number and a kept one");
    assert_eq!(
        ids(&common::drain(&mut source).await),
        vec![6],
        "a number the filter drops is not a hole, so nothing waits"
    );
}

/// Every number in the first windows fails the filter. The offset must move
/// past them, or the source would read the same window forever.
async fn a_where_clause_that_drops_whole_windows_still_advances(server: &Server) {
    events(server).await;
    let mut conn = server.conn().await;
    let dropped = vec!["('drop')"; 600].join(", ");
    conn.query_drop(format!("INSERT INTO events (label) VALUES {dropped}"))
        .await
        .expect("dropped rows");
    conn.query_drop("INSERT INTO events (label) VALUES ('keep')")
        .await
        .expect("kept row");

    let mut source = common::source(
        &server.dsn(),
        &format!(
            "name \"events\"\nbatch_rows 10\n\n\
             mode kind=\"polling\" table=\"events\" cursor_column=\"id\" \
             where_clause=\"label = 'keep'\"\n{PRICES_FIELDS}"
        ),
    );
    assert_eq!(ids(&common::drain(&mut source).await), vec![601]);
    assert!(common::drain(&mut source).await.is_empty());
    assert_eq!(
        stored_cursor(server, "events").await.as_deref(),
        Some("601")
    );
}

/// A non-unique `AUTO_INCREMENT` cursor with a tiebreak: equal numbers are
/// not holes, and a filter that drops one row of a run keeps the others.
async fn a_where_clause_and_a_tiebreak_over_a_non_unique_cursor_keep_every_tie(server: &Server) {
    let mut conn = server.conn().await;
    conn.query_drop(
        "CREATE TABLE ties (id BIGINT NOT NULL AUTO_INCREMENT, n INT NOT NULL, \
         label VARCHAR(20), KEY (id, n))",
    )
    .await
    .expect("create");
    conn.query_drop(
        "INSERT INTO ties (id, n, label) VALUES (1, 1, 'keep'), (1, 2, 'keep'), \
         (1, 3, 'drop'), (2, 1, 'keep'), (2, 2, 'keep'), (3, 1, 'keep')",
    )
    .await
    .expect("seed");

    let mut source = common::source(
        &server.dsn(),
        "name \"ties\"\nbatch_rows 2\n\n\
         mode kind=\"polling\" table=\"ties\" cursor_column=\"id\" tiebreak_column=\"n\" \
         where_clause=\"label = 'keep'\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"n\" type=\"int32\" nullable=#false\n\
         schema_fields \"label\" type=\"utf8\"\n",
    );
    let batches = common::drain(&mut source).await;
    assert_eq!(ids(&batches), vec![1, 1, 2, 2, 3]);
    assert_eq!(sizes(&batches), vec![2, 2, 1]);
    assert!(common::drain(&mut source).await.is_empty());
}

/// Rows deleted before the source read them leave permanent holes. With
/// `INNODB_TRX` readable, no open transaction can fill them, so they are
/// released at once however long `gap_timeout_ms` is.
async fn a_deleted_row_hole_is_released_at_once_when_innodb_trx_is_readable(server: &Server) {
    events(server).await;
    let mut conn = server.conn().await;
    conn.query_drop("INSERT INTO events (label) VALUES ('keep'), ('keep'), ('keep')")
        .await
        .expect("seed");
    conn.query_drop("DELETE FROM events WHERE id = 2")
        .await
        .expect("delete");

    let mut plain = events_source(server, "gap_timeout_ms=30000");
    assert_eq!(ids(&common::drain(&mut plain).await), vec![1, 3]);

    let mut filtered = common::source(
        &server.dsn(),
        &format!(
            "name \"events_filtered\"\nbatch_rows 100\n\n\
             mode kind=\"polling\" table=\"events\" cursor_column=\"id\" \
             where_clause=\"label = 'keep'\" gap_timeout_ms=30000\n{PRICES_FIELDS}"
        ),
    );
    assert_eq!(ids(&common::drain(&mut filtered).await), vec![1, 3]);
}

/// Without the `PROCESS` privilege `INNODB_TRX` is refused, and the source
/// falls back to the timeout alone: rows before the hole go out, the rest wait.
async fn a_deleted_row_hole_waits_the_timeout_without_innodb_trx(server: &Server) {
    events(server).await;
    let mut conn = server.conn().await;
    conn.query_drop("INSERT INTO events (label) VALUES ('keep'), ('keep'), ('keep')")
        .await
        .expect("seed");
    conn.query_drop("DELETE FROM events WHERE id = 2")
        .await
        .expect("delete");
    let dsn = common::user_without_process(server, "plain").await;

    let mut plain = common::source(
        &dsn,
        &format!(
            "name \"events\"\nbatch_rows 100\n\n\
             mode kind=\"polling\" table=\"events\" cursor_column=\"id\" \
             gap_timeout_ms=1500\n{PRICES_FIELDS}"
        ),
    );
    let mut filtered = common::source(
        &dsn,
        &format!(
            "name \"events_filtered\"\nbatch_rows 100\n\n\
             mode kind=\"polling\" table=\"events\" cursor_column=\"id\" \
             where_clause=\"label = 'keep'\" gap_timeout_ms=1500\n{PRICES_FIELDS}"
        ),
    );
    assert_eq!(ids(&common::drain(&mut plain).await), vec![1]);
    assert_eq!(ids(&common::drain(&mut filtered).await), vec![1]);
    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
    assert_eq!(ids(&common::drain(&mut plain).await), vec![3]);
    assert_eq!(ids(&common::drain(&mut filtered).await), vec![3]);
}

/// The last rows of the table are a run of equal numbers cut by `batch_rows`,
/// and no higher number exists to move the window on. The rest of the run must
/// still come out.
async fn a_filtered_tie_run_cut_by_a_batch_is_finished_with_no_later_number(server: &Server) {
    let mut conn = server.conn().await;
    conn.query_drop(
        "CREATE TABLE ties_tail (id BIGINT NOT NULL AUTO_INCREMENT, n INT NOT NULL, \
         label VARCHAR(20), KEY (id, n))",
    )
    .await
    .expect("create");
    conn.query_drop(
        "INSERT INTO ties_tail (id, n, label) VALUES (1, 1, 'keep'), (1, 2, 'keep'), \
         (1, 3, 'keep'), (1, 4, 'drop')",
    )
    .await
    .expect("seed");

    let mut source = common::source(
        &server.dsn(),
        "name \"ties_tail\"\nbatch_rows 2\n\n\
         mode kind=\"polling\" table=\"ties_tail\" cursor_column=\"id\" tiebreak_column=\"n\" \
         where_clause=\"label = 'keep'\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"n\" type=\"int64\" nullable=#false\n\
         schema_fields \"label\" type=\"utf8\"\n",
    );
    let batches = common::drain(&mut source).await;
    assert_eq!(i64_column(&batches, "n"), vec![1, 2, 3]);
    assert_eq!(sizes(&batches), vec![2, 1]);
    assert!(common::drain(&mut source).await.is_empty());
}

/// An insert waiting on a lock has taken its number and written no row, so it
/// shows as `LOCK WAIT` with no modified rows. It owns a hole all the same: the
/// rows after the hole wait, and its row arrives in order when the lock clears.
async fn a_writer_waiting_on_a_lock_still_holds_a_hole_open(server: &Server) {
    events(server).await;
    let mut conn = server.conn().await;
    conn.query_drop("INSERT INTO events (label) VALUES ('first')")
        .await
        .expect("first");
    conn.query_drop("INSERT INTO events (id, label) VALUES (100, 'later')")
        .await
        .expect("later");

    // M holds the gap between 1 and 100 and has modified nothing, so it is no
    // blocker. W inserts into that gap and waits for it.
    let mut m = common::open_transaction(
        server,
        "SELECT id FROM events WHERE id BETWEEN 2 AND 10 FOR UPDATE",
    )
    .await;
    let mut w = server.conn().await;
    let writer = tokio::spawn(async move {
        w.query_drop("INSERT INTO events (id, label) VALUES (50, 'late')")
            .await
            .expect("the insert goes through once the gap is free");
    });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(!writer.is_finished(), "the insert waits on the gap lock");

    let mut source = events_source(server, "gap_timeout_ms=30000");
    let before = common::drain(&mut source).await;
    m.query_drop("COMMIT").await.expect("release the gap");
    writer.await.expect("the writer finishes");
    let after = common::drain(&mut source).await;

    let all: Vec<RecordBatch> = before.iter().chain(&after).cloned().collect();
    assert_eq!(
        ids(&all),
        vec![1, 50, 100],
        "the waiting insert's row arrives"
    );
    assert_eq!(ids(&before), vec![1], "the row after the hole waits for it");
}
