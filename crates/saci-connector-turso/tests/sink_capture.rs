//! A capturing sink's writes are visible to a `cdc` source.

mod common;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use saci_connector_turso::{TursoSink, TursoSinkConfig, TursoSource, TursoSourceConfig};
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;

fn sink(path: &str) -> TursoSink {
    let body = format!(
        "name \"writer\"\n\
         table \"enriched\"\n\
         write_mode \"append\"\n\
         connection path=\"{}\"\n\
         capture mode=\"full\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"total\" type=\"float64\" nullable=#true\n",
        common::kdl_path(path)
    );
    let config: TursoSinkConfig = common::config_from_kdl(&body);
    TursoSink::new(config).expect("sink builds")
}

fn source(path: &str) -> TursoSource {
    let body = format!(
        "name \"reader\"\n\
         batch_rows 1024\n\
         connection path=\"{}\"\n\
         mode kind=\"cdc\" table=\"enriched\"\n\
         schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
         schema_fields \"id\" type=\"int64\" nullable=#true\n\
         schema_fields \"total\" type=\"float64\" nullable=#true\n",
        common::kdl_path(path)
    );
    let config: TursoSourceConfig = common::config_from_kdl(&body);
    TursoSource::new(config).expect("source builds")
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

#[tokio::test]
async fn a_capturing_sink_feeds_a_cdc_source() {
    let db = common::temp_db();
    let conn = common::connect(&db.path).await;
    common::exec(
        &conn,
        "CREATE TABLE enriched (id INTEGER PRIMARY KEY, total REAL)",
    )
    .await;
    drop(conn);

    // The sink enables capture on its own connection, so its writes land in
    // the change table.
    let mut sink = sink(&db.path_str());
    sink.write_batch(&batch(&[(1, 1.5), (2, 2.5)]))
        .await
        .expect("write");
    sink.finish().await.expect("finish");
    drop(sink);

    let mut src = source(&db.path_str());
    let mut ops = Vec::new();
    let mut ids = Vec::new();
    while let Some(batch) = src.next_batch().await.expect("next_batch") {
        let op_column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("__op");
        let id_column = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        for row in 0..batch.num_rows() {
            ops.push(op_column.value(row).to_string());
            ids.push(if Array::is_null(id_column, row) {
                None
            } else {
                Some(id_column.value(row))
            });
        }
    }
    assert_eq!(ops, vec!["I", "I"]);
    assert_eq!(ids, vec![Some(1), Some(2)]);
}
