//! One transport, three formats: a [`FileSink`] write followed by a
//! [`FileSource`] read for csv, ndjson and parquet.
//!
//! This is what the connector-versus-transformer split buys. The connector code
//! under test is identical in all three cases; only the `Arc<dyn Transformer>`
//! differs.

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use tempfile::TempDir;

use saci_connector_file::{FileSink, FileSource};
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use saci_transformer::Transformer;
use saci_transformer_csv::CsvTransformer;
use saci_transformer_ndjson::NdjsonTransformer;
use saci_transformer_parquet::ParquetTransformer;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Float64, false),
    ]))
}

fn batch(rows: i64) -> RecordBatch {
    batch_range(0, rows)
}

/// Rows carrying ids `start..end`, so two writes into one file are told apart.
fn batch_range(start: i64, end: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(start..end)),
            Arc::new(Float64Array::from_iter_values(
                (start..end).map(|i| i as f64 * 1.5),
            )),
        ],
    )
    .expect("batch")
}

/// Drain every batch `path` holds, read through `transformer`.
async fn read_back(
    path: &std::path::Path,
    transformer: Arc<dyn Transformer>,
    declared: Option<Arc<Schema>>,
) -> Vec<RecordBatch> {
    let mut source = FileSource::open_async(path, transformer, declared)
        .await
        .expect("source opens");

    let mut batches = Vec::new();
    while let Some(batch) = source.next_batch().await.expect("read") {
        batches.push(batch);
    }
    batches
}

/// Write `rows` rows in `format`, read them back, and return what came back.
async fn round_trip(
    file_name: &str,
    transformer: Arc<dyn Transformer>,
    declared_on_read: bool,
    rows: i64,
) -> Vec<RecordBatch> {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join(file_name);

    let mut sink = FileSink::create(&path, Arc::clone(&transformer), schema()).expect("sink opens");
    sink.write_batch(&batch(rows)).await.expect("write");
    sink.finish().await.expect("finish");

    let declared = declared_on_read.then(schema);
    read_back(&path, transformer, declared).await
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn first_value(batches: &[RecordBatch], row: usize) -> f64 {
    batches[0]
        .column_by_name("val")
        .expect("val column")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float column")
        .value(row)
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int column")
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn csv_round_trips_through_one_file_connector() {
    let batches = round_trip("out.csv", Arc::new(CsvTransformer::new(true)), true, 64).await;
    assert_eq!(total_rows(&batches), 64);
    assert!((first_value(&batches, 3) - 4.5).abs() < 1e-9);
}

#[tokio::test]
async fn ndjson_round_trips_through_one_file_connector() {
    let batches = round_trip(
        "out.ndjson",
        Arc::new(NdjsonTransformer::default()),
        true,
        64,
    )
    .await;
    assert_eq!(total_rows(&batches), 64);
    assert!((first_value(&batches, 3) - 4.5).abs() < 1e-9);
}

#[tokio::test]
async fn parquet_round_trips_through_one_file_connector() {
    // Parquet carries its own schema, so nothing is declared on the read.
    let batches = round_trip(
        "out.parquet",
        Arc::new(ParquetTransformer::new()),
        false,
        64,
    )
    .await;
    assert_eq!(total_rows(&batches), 64);
    assert!((first_value(&batches, 3) - 4.5).abs() < 1e-9);
}

#[tokio::test]
async fn only_parquet_reports_estimated_rows() {
    let dir = TempDir::new().expect("temp dir");

    let parquet_path = dir.path().join("rows.parquet");
    let mut sink = FileSink::create(&parquet_path, Arc::new(ParquetTransformer::new()), schema())
        .expect("sink opens");
    sink.write_batch(&batch(42)).await.expect("write");
    sink.finish().await.expect("finish");
    let parquet = FileSource::open(&parquet_path, Arc::new(ParquetTransformer::new()), None)
        .expect("source opens");
    assert_eq!(parquet.estimated_rows(), Some(42));

    let csv_path = dir.path().join("rows.csv");
    let mut sink = FileSink::create(&csv_path, Arc::new(CsvTransformer::new(true)), schema())
        .expect("sink opens");
    sink.write_batch(&batch(42)).await.expect("write");
    sink.finish().await.expect("finish");
    let csv = FileSource::open(
        &csv_path,
        Arc::new(CsvTransformer::new(true)),
        Some(schema()),
    )
    .expect("source opens");
    assert_eq!(csv.estimated_rows(), None);
}

#[tokio::test]
async fn a_write_after_finish_is_an_error_naming_the_sink() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("closed.parquet");

    let mut sink =
        FileSink::create(&path, Arc::new(ParquetTransformer::new()), schema()).expect("sink opens");
    sink.write_batch(&batch(1)).await.expect("write");
    sink.finish().await.expect("finish");

    let Err(err) = sink.write_batch(&batch(1)).await else {
        panic!("a finished sink has no writer left");
    };
    assert_eq!(err.message(), "FileSink: write_batch called after finish");

    // A second finish is a no-op, which is what makes `run_with_io`'s
    // unconditional finish on every call safe.
    sink.finish().await.expect("finish is idempotent");
}

#[tokio::test]
async fn opening_a_missing_file_names_the_path() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("absent.csv");
    let Err(err) = FileSource::open(&path, Arc::new(CsvTransformer::new(true)), Some(schema()))
    else {
        panic!("a missing file must not open");
    };
    assert!(
        err.message().starts_with("FileSource: cannot open"),
        "got: {err}"
    );
    assert!(err.message().contains("absent.csv"), "got: {err}");
}

#[tokio::test]
async fn appending_to_an_existing_file_preserves_prior_rows() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("appended.ndjson");
    let transformer: Arc<dyn Transformer> = Arc::new(NdjsonTransformer::default());

    let mut first =
        FileSink::create(&path, Arc::clone(&transformer), schema()).expect("sink opens");
    first.write_batch(&batch_range(1, 4)).await.expect("write");
    first.finish().await.expect("finish");

    // A second sink on the same path: the default open mode keeps what the
    // first one wrote.
    let mut second =
        FileSink::create(&path, Arc::clone(&transformer), schema()).expect("sink opens");
    second.write_batch(&batch_range(4, 7)).await.expect("write");
    second.finish().await.expect("finish");

    let batches = read_back(&path, transformer, Some(schema())).await;
    assert_eq!(ids(&batches), vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn truncating_mode_replaces_the_file() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("replaced.ndjson");
    let transformer: Arc<dyn Transformer> = Arc::new(NdjsonTransformer::default());

    let mut first =
        FileSink::create(&path, Arc::clone(&transformer), schema()).expect("sink opens");
    first.write_batch(&batch_range(1, 4)).await.expect("write");
    first.finish().await.expect("finish");

    let mut second =
        FileSink::create_truncating(&path, Arc::clone(&transformer), schema()).expect("sink opens");
    second.write_batch(&batch_range(4, 7)).await.expect("write");
    second.finish().await.expect("finish");

    let batches = read_back(&path, transformer, Some(schema())).await;
    assert_eq!(ids(&batches), vec![4, 5, 6]);
}

#[tokio::test]
async fn a_sink_that_cannot_open_its_path_names_the_mode() {
    let dir = TempDir::new().expect("temp dir");
    // A directory is not a file either mode can open, on Windows or on Unix.
    let path = dir.path().to_path_buf();
    let transformer: Arc<dyn Transformer> = Arc::new(NdjsonTransformer::default());

    let Err(err) = FileSink::create(&path, Arc::clone(&transformer), schema()) else {
        panic!("a directory must not open as a sink");
    };
    assert!(
        err.message().starts_with("FileSink: cannot open"),
        "got: {err}"
    );
    assert!(err.message().contains("for append"), "got: {err}");

    let Err(err) = FileSink::create_truncating(&path, transformer, schema()) else {
        panic!("a directory must not open as a sink");
    };
    assert!(
        err.message().starts_with("FileSink: cannot create"),
        "got: {err}"
    );
}
