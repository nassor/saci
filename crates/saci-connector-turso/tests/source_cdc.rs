//! The `cdc` read strategy.

mod common;

use arrow_array::{Array, Int64Array, StringArray};
use saci_connector_turso::{TursoSource, TursoSourceConfig};
use saci_core::io::source::Source;

fn source(path: &str, retention: &str) -> TursoSource {
    let body = format!(
        "name \"changes\"\n\
         batch_rows 1024\n\
         connection path=\"{}\"\n\
         mode kind=\"cdc\" table=\"orders\" cdc_table=\"turso_cdc\" retention=\"{retention}\"\n\
         schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
         schema_fields \"__change_id\" type=\"int64\" nullable=#false\n\
         schema_fields \"__txn_id\" type=\"int64\" nullable=#false\n\
         schema_fields \"id\" type=\"int64\" nullable=#true\n\
         schema_fields \"total\" type=\"float64\" nullable=#true\n",
        common::kdl_path(path)
    );
    let config: TursoSourceConfig = common::config_from_kdl(&body);
    TursoSource::new(config).expect("source builds")
}

/// Collect `(op, id)` pairs over one full drain.
async fn collect(source: &mut TursoSource) -> Vec<(String, Option<i64>)> {
    let mut out = Vec::new();
    while let Some(batch) = source.next_batch().await.expect("next_batch") {
        let ops = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("__op column");
        let ids = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let txns = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("__txn_id column");
        for row in 0..batch.num_rows() {
            assert!(
                !Array::is_null(txns, row),
                "every change carries a transaction id"
            );
            let id = if Array::is_null(ids, row) {
                None
            } else {
                Some(ids.value(row))
            };
            out.push((ops.value(row).to_string(), id));
        }
    }
    out
}

/// Make four changes through a capture-enabled connection, then drop it.
async fn seed(path: &std::path::Path) {
    let conn = common::connect(path).await;
    common::exec(
        &conn,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)",
    )
    .await;
    common::exec(&conn, "PRAGMA capture_data_changes_conn('full')").await;
    common::exec(&conn, "INSERT INTO orders (id, total) VALUES (1, 1.5)").await;
    common::exec(&conn, "INSERT INTO orders (id, total) VALUES (2, 2.5)").await;
    common::exec(&conn, "UPDATE orders SET total = 9.5 WHERE id = 1").await;
    common::exec(&conn, "DELETE FROM orders WHERE id = 2").await;
}

#[tokio::test]
async fn cdc_reads_inserts_updates_and_deletes() {
    let db = common::temp_db();
    seed(&db.path).await;

    let mut src = source(&db.path_str(), "keep");
    assert_eq!(
        collect(&mut src).await,
        vec![
            ("I".to_string(), Some(1)),
            ("I".to_string(), Some(2)),
            ("U".to_string(), Some(1)),
            ("D".to_string(), Some(2)),
        ]
    );
    // The change cursor is durable: a second cycle sees nothing new.
    assert_eq!(collect(&mut src).await.len(), 0);
}

#[tokio::test]
async fn delete_acked_prunes_the_change_table() {
    let db = common::temp_db();
    seed(&db.path).await;

    let mut src = source(&db.path_str(), "delete_acked");
    assert_eq!(collect(&mut src).await.len(), 4);
    // The next cycle commits the last position and prunes behind it.
    assert_eq!(collect(&mut src).await.len(), 0);
    drop(src);

    let conn = common::connect(&db.path).await;
    assert_eq!(
        common::scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM turso_cdc WHERE table_name = 'orders'"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn cdc_without_a_change_table_is_a_loud_error() {
    let db = common::temp_db();
    let conn = common::connect(&db.path).await;
    common::exec(
        &conn,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)",
    )
    .await;
    drop(conn);

    let mut src = source(&db.path_str(), "keep");
    let error = src.next_batch().await.expect_err("no change table exists");
    assert!(
        error.message().contains("capture_data_changes_conn"),
        "error should name the pragma, got: {}",
        error.message()
    );
}
