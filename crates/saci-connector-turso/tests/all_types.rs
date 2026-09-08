//! Every declared type round-trips through a sink and back through a source.

mod common;

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float64Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use saci_connector_turso::{TursoSink, TursoSinkConfig, TursoSource, TursoSourceConfig};
use saci_core::io::{sink::Sink, source::Source};

const FIELDS: &str = "\
schema_fields \"id\" type=\"int64\" nullable=#false\n\
schema_fields \"i\" type=\"int64\" nullable=#true\n\
schema_fields \"f\" type=\"float64\" nullable=#true\n\
schema_fields \"s\" type=\"utf8\" nullable=#true\n\
schema_fields \"b\" type=\"bool\" nullable=#true\n\
schema_fields \"bin\" type=\"binary\" nullable=#true\n\
schema_fields \"d\" type=\"decimal128\" precision=10 scale=2 nullable=#true\n";

fn sink(path: &str) -> TursoSink {
    let body = format!(
        "name \"roundtrip\"\ntable \"alltypes\"\nwrite_mode \"append\"\n\
         connection path=\"{}\"\n{FIELDS}",
        common::kdl_path(path)
    );
    let config: TursoSinkConfig = common::config_from_kdl(&body);
    TursoSink::new(config).expect("sink builds")
}

fn source(path: &str) -> TursoSource {
    let body = format!(
        "name \"roundtrip\"\nbatch_rows 1024\n\
         connection path=\"{}\"\nmode kind=\"dump\" table=\"alltypes\"\n{FIELDS}",
        common::kdl_path(path)
    );
    let config: TursoSourceConfig = common::config_from_kdl(&body);
    TursoSource::new(config).expect("source builds")
}

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("i", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Boolean, true),
        Field::new("bin", DataType::Binary, true),
        Field::new("d", DataType::Decimal128(10, 2), true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1])),
        Arc::new(Int64Array::from(vec![42])),
        Arc::new(Float64Array::from(vec![3.5])),
        Arc::new(StringArray::from(vec!["hi"])),
        Arc::new(BooleanArray::from(vec![true])),
        Arc::new(BinaryArray::from_vec(vec![&[1u8, 2, 3][..]])),
        Arc::new(
            Decimal128Array::from(vec![12345i128])
                .with_precision_and_scale(10, 2)
                .expect("decimal"),
        ),
    ];
    RecordBatch::try_new(schema, columns).expect("batch")
}

#[tokio::test]
async fn every_declared_type_round_trips() {
    let db = common::temp_db();
    let conn = common::connect(&db.path).await;
    common::exec(
        &conn,
        "CREATE TABLE alltypes (id INTEGER PRIMARY KEY, i INTEGER, f REAL, s TEXT, \
         b INTEGER, bin BLOB, d TEXT)",
    )
    .await;
    drop(conn);

    let mut sink = sink(&db.path_str());
    sink.write_batch(&batch()).await.expect("write");
    sink.finish().await.expect("finish");
    drop(sink);

    let mut source = source(&db.path_str());
    let read = source
        .next_batch()
        .await
        .expect("next_batch")
        .expect("one batch");

    let id = read
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let i = read
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let f = read
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let s = read
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let b = read
        .column(4)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let bin = read
        .column(5)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let d = read
        .column(6)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();

    assert_eq!(id.value(0), 1);
    assert_eq!(i.value(0), 42);
    assert_eq!(f.value(0), 3.5);
    assert_eq!(s.value(0), "hi");
    assert!(b.value(0));
    assert_eq!(bin.value(0).to_vec(), vec![1u8, 2, 3]);
    assert_eq!(d.value(0), 12345);
    assert_eq!(d.precision(), 10);
    assert_eq!(d.scale(), 2);
}
