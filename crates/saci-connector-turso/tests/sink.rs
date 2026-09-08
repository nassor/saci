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
    let body = format!(
        "name \"enriched\"\n\
         table \"enriched\"\n\
         write_mode \"{mode}\"\n\
         {conflict}\
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
