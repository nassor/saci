//! The `dump` read strategy.

mod common;

use arrow_array::{Array, Int64Array, RecordBatch};
use saci_connector_turso::{TursoSource, TursoSourceConfig};

fn source(path: &str, batch_rows: usize) -> TursoSource {
    let body = format!(
        "name \"orders\"\n\
         batch_rows {batch_rows}\n\
         connection path=\"{}\"\n\
         mode kind=\"dump\" table=\"orders\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"total\" type=\"float64\" nullable=#true\n",
        common::kdl_path(path)
    );
    let config: TursoSourceConfig = common::config_from_kdl(&body);
    TursoSource::new(config).expect("source builds")
}

fn source_capped(path: &str, batch_rows: usize, max_batches_per_cycle: usize) -> TursoSource {
    let body = format!(
        "name \"orders\"\n\
         batch_rows {batch_rows}\n\
         max_batches_per_cycle {max_batches_per_cycle}\n\
         connection path=\"{}\"\n\
         mode kind=\"dump\" table=\"orders\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"total\" type=\"float64\" nullable=#true\n",
        common::kdl_path(path)
    );
    let config: TursoSourceConfig = common::config_from_kdl(&body);
    TursoSource::new(config).expect("source builds")
}

async fn seed_five(db: &common::TestDb) {
    let conn = common::connect(&db.path).await;
    common::exec(
        &conn,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)",
    )
    .await;
    for id in 1..=5 {
        common::exec(
            &conn,
            &format!("INSERT INTO orders (id, total) VALUES ({id}, {id}.5)"),
        )
        .await;
    }
}

#[tokio::test]
async fn dump_reads_the_whole_table_every_cycle() {
    let db = common::temp_db();
    seed_five(&db).await;

    let mut src = source(&db.path_str(), 2);
    assert_eq!(common::drain(&mut src).await, 5);
    // A dump has no cursor: the next cycle re-reads the same five rows.
    assert_eq!(common::drain(&mut src).await, 5);
}

#[tokio::test]
async fn dump_respects_max_batches_per_cycle() {
    use saci_core::io::source::Source;

    let db = common::temp_db();
    seed_five(&db).await;

    // One batch per cycle: the runner gets control back after every batch, and
    // the scan resumes where it stopped rather than restarting.
    let mut src = source_capped(&db.path_str(), 2, 1);
    let first = src.next_batch().await.expect("first batch").expect("rows");
    assert_eq!(first.num_rows(), 2);
    assert!(
        src.next_batch().await.expect("cycle end").is_none(),
        "the cap ends the cycle after one batch"
    );
    let second = src.next_batch().await.expect("second batch").expect("rows");
    assert_eq!(second.num_rows(), 2);
    assert_eq!(ids(&second), vec![3, 4], "the next cycle resumes at row 3");
}

fn ids(batch: &RecordBatch) -> Vec<i64> {
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column");
    (0..column.len()).map(|row| column.value(row)).collect()
}
