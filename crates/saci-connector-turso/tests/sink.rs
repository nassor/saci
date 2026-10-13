//! The sink's write modes.

mod common;

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use saci_connector_turso::{TursoSink, TursoSinkConfig};
use saci_core::io::sink::Sink;

fn sink(path: &str, mode: &str, conflict: Option<&str>) -> TursoSink {
    let conflict = match conflict {
        Some(column) => format!("conflict_columns \"{column}\"\n"),
        None => String::new(),
    };
    sink_with(path, mode, &conflict)
}

/// A sink over `enriched` with `extra` config lines.
fn sink_with(path: &str, mode: &str, extra: &str) -> TursoSink {
    let body = format!(
        "name \"enriched\"\n\
         table \"enriched\"\n\
         write_mode \"{mode}\"\n\
         {extra}\
         connection path=\"{}\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"total\" type=\"float64\" nullable=#true\n",
        common::kdl_path(path)
    );
    let config: TursoSinkConfig = common::config_from_kdl(&body);
    TursoSink::new(config).expect("sink builds")
}

fn concurrent_sink(path: &str) -> TursoSink {
    let body = format!(
        "name \"enriched\"\n\
         table \"enriched\"\n\
         write_mode \"append\"\n\
         transaction \"concurrent\"\n\
         connection path=\"{}\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"total\" type=\"float64\" nullable=#true\n",
        common::kdl_path(path)
    );
    let config: TursoSinkConfig = common::config_from_kdl(&body);
    TursoSink::new(config).expect("sink builds")
}

fn batch(rows: &[(i64, f64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("total", DataType::Float64, true),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
    ));
    let totals: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|(_, total)| *total).collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, vec![ids, totals]).expect("batch")
}

async fn create(path: &std::path::Path) {
    let conn = common::connect(path).await;
    common::exec(
        &conn,
        "CREATE TABLE enriched (id INTEGER PRIMARY KEY, total REAL)",
    )
    .await;
}

async fn totals(path: &std::path::Path) -> Vec<(i64, f64)> {
    let conn = common::connect(path).await;
    let mut rows = conn
        .query("SELECT id, total FROM enriched ORDER BY id", ())
        .await
        .expect("query");
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.expect("step") {
        let id = match row.get_value(0).expect("id") {
            turso::Value::Integer(i) => i,
            other => panic!("id: {other:?}"),
        };
        let total = match row.get_value(1).expect("total") {
            turso::Value::Real(f) => f,
            turso::Value::Integer(i) => i as f64,
            other => panic!("total: {other:?}"),
        };
        out.push((id, total));
    }
    out
}

#[tokio::test]
async fn append_writes_every_row() {
    let db = common::temp_db();
    create(&db.path).await;
    let mut sink = sink(&db.path_str(), "append", None);
    sink.write_batch(&batch(&[(1, 1.0), (2, 2.0)]))
        .await
        .expect("write");
    sink.finish().await.expect("finish");
    drop(sink);
    assert_eq!(totals(&db.path).await, vec![(1, 1.0), (2, 2.0)]);
}

#[tokio::test]
async fn concurrent_transactions_write_under_mvcc() {
    // `transaction "concurrent"` turns MVCC on for the sink's connection and
    // flushes between BEGIN CONCURRENT and COMMIT. The write succeeding at all
    // is the proof MVCC engaged: the mode is set before the first flush.
    let db = common::temp_db();
    create(&db.path).await;
    let mut sink = concurrent_sink(&db.path_str());
    sink.write_batch(&batch(&[(1, 1.0), (2, 2.0)]))
        .await
        .expect("write");
    sink.finish().await.expect("finish");
    drop(sink);
    assert_eq!(totals(&db.path).await, vec![(1, 1.0), (2, 2.0)]);
}

#[tokio::test]
async fn finish_without_a_batch_is_a_no_op() {
    // A sink that never wrote opens no database; `finish` must still succeed
    // for the default (push-enabled) sync config.
    let db = common::temp_db();
    create(&db.path).await;
    let mut sink = sink(&db.path_str(), "append", None);
    sink.finish().await.expect("an empty sink finishes");
    assert_eq!(totals(&db.path).await, Vec::new());
}

#[tokio::test]
async fn upsert_replaces_a_conflicting_row() {
    let db = common::temp_db();
    create(&db.path).await;
    for total in [1.0, 9.0] {
        let mut sink = sink(&db.path_str(), "upsert", Some("id"));
        sink.write_batch(&batch(&[(1, total)]))
            .await
            .expect("write");
        sink.finish().await.expect("finish");
        drop(sink);
    }
    assert_eq!(totals(&db.path).await, vec![(1, 9.0)]);
}

#[tokio::test]
async fn ignore_conflicts_keeps_the_first_row() {
    let db = common::temp_db();
    create(&db.path).await;
    for total in [1.0, 9.0] {
        let mut sink = sink(&db.path_str(), "ignore_conflicts", Some("id"));
        sink.write_batch(&batch(&[(1, total)]))
            .await
            .expect("write");
        sink.finish().await.expect("finish");
        drop(sink);
    }
    assert_eq!(totals(&db.path).await, vec![(1, 1.0)]);
}

#[tokio::test]
async fn upsert_without_a_unique_index_is_refused() {
    let db = common::temp_db();
    let conn = common::connect(&db.path).await;
    common::exec(&conn, "CREATE TABLE enriched (id INTEGER, total REAL)").await;
    drop(conn);

    let mut sink = sink(&db.path_str(), "upsert", Some("id"));
    let error = sink
        .write_batch(&batch(&[(1, 1.0)]))
        .await
        .expect_err("no unique index on id");
    assert!(
        error.message().contains("UNIQUE"),
        "error should name the missing constraint, got: {}",
        error.message()
    );
}

// ------------------------------------------------------ retry and re-drive

async fn create_keyless(path: &std::path::Path) {
    let conn = common::connect(path).await;
    common::exec(&conn, "CREATE TABLE enriched (id INTEGER, total REAL)").await;
}

/// Another connection holding the database's write lock, so a sink's
/// transaction fails with a busy error until it is released.
struct WriteLock {
    conn: turso::Connection,
}

impl WriteLock {
    async fn take(path: &std::path::Path) -> Self {
        let conn = common::connect(path).await;
        common::exec(&conn, "BEGIN IMMEDIATE").await;
        common::exec(&conn, "INSERT INTO enriched VALUES (-1, -1.0)").await;
        Self { conn }
    }

    async fn release(self) {
        common::exec(&self.conn, "ROLLBACK").await;
    }
}

/// The service's `RetryingSink` re-drives the batch whose `write_batch` failed.
/// A flush that failed must not leave that batch buffered, or the retry
/// buffers it a second time and a table without a key gets every row twice.
#[tokio::test]
async fn a_batch_the_caller_retries_is_written_once() {
    let db = common::temp_db();
    create_keyless(&db.path).await;
    let mut sink = sink_with(&db.path_str(), "append", "chunk_rows 2\n");
    // An empty batch opens the database before the lock goes on.
    sink.write_batch(&batch(&[])).await.expect("opens");
    let own = batch(&[(1, 1.0), (2, 2.0)]);

    let lock = WriteLock::take(&db.path).await;
    sink.write_batch(&own)
        .await
        .expect_err("the database is locked");
    assert_eq!(sink.pending_rows(), Some(0), "a refused batch is not held");
    lock.release().await;

    sink.write_batch(&own).await.expect("the retry");
    sink.finish().await.expect("nothing left to write");
    assert_eq!(
        totals(&db.path).await,
        vec![(1, 1.0), (2, 2.0)],
        "each row is written once"
    );
}

#[tokio::test]
async fn a_failed_flush_keeps_the_rows_an_earlier_call_accepted() {
    let db = common::temp_db();
    create_keyless(&db.path).await;
    let mut sink = sink_with(&db.path_str(), "append", "chunk_rows 3\n");
    sink.write_batch(&batch(&[(10, 10.0)]))
        .await
        .expect("buffered");

    let own = batch(&[(1, 1.0), (2, 2.0)]);
    let lock = WriteLock::take(&db.path).await;
    sink.write_batch(&own)
        .await
        .expect_err("the database is locked");
    assert_eq!(
        sink.pending_rows(),
        Some(1),
        "the accepted row stays; the refused batch is the caller's to re-drive"
    );
    lock.release().await;

    sink.write_batch(&own).await.expect("the re-driven batch");
    assert_eq!(sink.pending_rows(), Some(0));
    assert_eq!(totals(&db.path).await, vec![(1, 1.0), (2, 2.0), (10, 10.0)]);
}

#[tokio::test]
async fn a_failed_finish_keeps_its_rows_for_the_next_attempt() {
    let db = common::temp_db();
    create_keyless(&db.path).await;
    let mut sink = sink_with(&db.path_str(), "append", "chunk_rows 100\n");
    sink.write_batch(&batch(&[(1, 1.0), (2, 2.0)]))
        .await
        .expect("buffered");

    let lock = WriteLock::take(&db.path).await;
    sink.finish().await.expect_err("the database is locked");
    assert_eq!(sink.pending_rows(), Some(2));
    lock.release().await;

    sink.finish().await.expect("the buffered rows are retried");
    assert_eq!(sink.pending_rows(), Some(0));
    assert_eq!(totals(&db.path).await, vec![(1, 1.0), (2, 2.0)]);
}

/// The `DELETE` is part of the first flush's transaction, so a flush that fails
/// leaves the old rows where they were, and the first one that lands still
/// clears them.
#[tokio::test]
async fn a_failed_truncating_flush_leaves_the_old_rows_in_place() {
    let db = common::temp_db();
    create(&db.path).await;
    let seed = common::connect(&db.path).await;
    common::exec(&seed, "INSERT INTO enriched VALUES (900, 1.0), (901, 1.0)").await;
    drop(seed);
    let mut sink = sink_with(
        &db.path_str(),
        "append",
        "chunk_rows 2\ntruncate_before_first_write #true\n",
    );

    sink.write_batch(&batch(&[(7, 1.0), (7, 2.0)]))
        .await
        .expect_err("a duplicate key inside the batch");
    assert_eq!(
        totals(&db.path).await,
        vec![(900, 1.0), (901, 1.0)],
        "the delete is rolled back with the rows"
    );

    sink.write_batch(&batch(&[(7, 1.0), (8, 2.0)]))
        .await
        .expect("the first write that lands still truncates");
    assert_eq!(totals(&db.path).await, vec![(7, 1.0), (8, 2.0)]);
}

/// Once a flush has committed the table is the run's own, and a later flush
/// must not clear it.
#[tokio::test]
async fn first_write_truncates_only_once() {
    let db = common::temp_db();
    create(&db.path).await;
    let seed = common::connect(&db.path).await;
    common::exec(&seed, "INSERT INTO enriched VALUES (900, 1.0)").await;
    drop(seed);
    let mut sink = sink_with(
        &db.path_str(),
        "append",
        "chunk_rows 1\ntruncate_before_first_write #true\n",
    );

    sink.write_batch(&batch(&[(1, 1.0)])).await.expect("first");
    sink.write_batch(&batch(&[(2, 2.0)])).await.expect("second");
    assert_eq!(totals(&db.path).await, vec![(1, 1.0), (2, 2.0)]);
}
