//! The `polling` read strategy.

mod common;

use saci_connector_turso::{TursoSource, TursoSourceConfig};

fn source(path: &str, batch_rows: usize) -> TursoSource {
    let body = format!(
        "name \"orders\"\n\
         batch_rows {batch_rows}\n\
         connection path=\"{}\"\n\
         mode kind=\"polling\" table=\"orders\" cursor_column=\"id\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"total\" type=\"float64\" nullable=#true\n",
        common::kdl_path(path)
    );
    let config: TursoSourceConfig = common::config_from_kdl(&body);
    TursoSource::new(config).expect("source builds")
}

async fn seed(path: &std::path::Path, ids: std::ops::RangeInclusive<i64>) {
    let conn = common::connect(path).await;
    for id in ids {
        common::exec(
            &conn,
            &format!("INSERT INTO orders (id, total) VALUES ({id}, {id}.5)"),
        )
        .await;
    }
}

#[tokio::test]
async fn polling_resumes_from_its_durable_offset() {
    let db = common::temp_db();
    let conn = common::connect(&db.path).await;
    common::exec(
        &conn,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)",
    )
    .await;
    drop(conn);
    seed(&db.path, 1..=5).await;

    // One cycle reads every row, however the batches fall.
    let mut first = source(&db.path_str(), 2);
    assert_eq!(common::drain(&mut first).await, 5);
    // A second cycle is caught up.
    assert_eq!(common::drain(&mut first).await, 0);
    drop(first);

    // The committed cursor is durable.
    let conn = common::connect(&db.path).await;
    assert_eq!(
        common::scalar_text(
            &conn,
            "SELECT cursor_value FROM saci_source_offsets WHERE source_name = 'orders'"
        )
        .await,
        "5"
    );
    drop(conn);

    seed(&db.path, 6..=8).await;

    // A fresh instance resumes past the five rows already delivered.
    let mut second = source(&db.path_str(), 2);
    assert_eq!(common::drain(&mut second).await, 3);
}

#[tokio::test]
async fn polling_rejects_an_empty_offset_table_row_gracefully() {
    let db = common::temp_db();
    let conn = common::connect(&db.path).await;
    common::exec(
        &conn,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)",
    )
    .await;
    drop(conn);

    // No rows at all: the first cycle is simply empty.
    let mut src = source(&db.path_str(), 4);
    assert_eq!(common::drain(&mut src).await, 0);
}
