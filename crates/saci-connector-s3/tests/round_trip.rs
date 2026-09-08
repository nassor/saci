//! Integration tests against a real RustFS S3-compatible server.
//!
//! Every test opens with `let Some(s3) = common::try_start().await else { return; };`
//! so the suite soft-skips when no Docker daemon is reachable.

mod common;

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures_util::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;

use saci_connector_s3::{Flush, S3Sink, S3SinkConfig, S3Source, S3SourceConfig, SchemaFrom};
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use saci_transformer_csv::CsvTransformer;
use saci_transformer_parquet::ParquetTransformer;

use common::S3Container;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn batch(id: i64, name: &str) -> RecordBatch {
    let id_col: ArrayRef = Arc::new(Int64Array::from(vec![id]));
    let name_col: ArrayRef = Arc::new(StringArray::from(vec![name]));
    RecordBatch::try_new(schema(), vec![id_col, name_col]).expect("batch builds")
}

fn sink_config(s3: &S3Container, prefix: &str, suffix: &str, flush: Flush) -> S3SinkConfig {
    S3SinkConfig {
        connection: s3.connection(),
        prefix: prefix.to_string(),
        suffix: suffix.to_string(),
        flush,
        schema_fields: Vec::new(),
    }
}

fn source_config(s3: &S3Container, prefix: &str, schema_from: SchemaFrom) -> S3SourceConfig {
    S3SourceConfig {
        connection: s3.connection(),
        prefix: prefix.to_string(),
        schema_from,
        schema_fields: Vec::new(),
    }
}

/// Keys under `prefix`, in the service's own order (which the source sorts).
async fn list_keys(s3: &S3Container, prefix: &str) -> Vec<String> {
    let mut stream = s3.store().list(Some(&Path::from(prefix)));
    let mut keys = Vec::new();
    while let Some(meta) = stream.next().await {
        keys.push(meta.expect("list succeeds").location.to_string());
    }
    keys
}

/// Matches `^orders/\d{8}T\d{6}\.\d{3}Z-\d{6}\.csv$`: a fixed-width UTC
/// timestamp, a dash, and the six-digit flush counter.
fn matches_key_shape(stem: &str) -> bool {
    let bytes = stem.as_bytes();
    bytes.len() == 27
        && bytes[0..8].iter().all(u8::is_ascii_digit)
        && bytes[8] == b'T'
        && bytes[9..15].iter().all(u8::is_ascii_digit)
        && bytes[15] == b'.'
        && bytes[16..19].iter().all(u8::is_ascii_digit)
        && bytes[19] == b'Z'
        && bytes[20] == b'-'
        && bytes[21..27].iter().all(u8::is_ascii_digit)
}

async fn rows_of(src: &mut S3Source) -> Vec<(i64, String)> {
    let mut rows = Vec::new();
    while let Some(batch) = src.next_batch().await.expect("read succeeds") {
        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let name = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        for i in 0..batch.num_rows() {
            rows.push((id.value(i), name.value(i).to_string()));
        }
    }
    rows
}

#[tokio::test]
async fn an_s3_sink_and_source_round_trip_csv() {
    let Some(s3) = common::try_start().await else {
        return;
    };

    let mut sink = S3Sink::new(
        sink_config(
            &s3,
            "orders",
            ".csv",
            Flush {
                max_rows: 1,
                max_bytes: 0,
                max_age_ms: 0,
            },
        ),
        schema(),
        Arc::new(CsvTransformer::new(false)),
    )
    .expect("sink builds");
    for (id, name) in [(1, "one"), (2, "two"), (3, "three")] {
        sink.write_batch(&batch(id, name))
            .await
            .expect("batch writes");
    }
    sink.finish().await.expect("finish");

    let keys = list_keys(&s3, "orders").await;
    assert_eq!(keys.len(), 3, "one object per row with max_rows 1");
    for key in &keys {
        let stem = key
            .strip_prefix("orders/")
            .expect("key under the prefix")
            .strip_suffix(".csv")
            .expect("csv suffix");
        assert!(
            matches_key_shape(stem),
            "key '{key}' matches the documented shape"
        );
    }
    // The timestamp prefix makes key order upload order.
    assert!(
        keys.windows(2).all(|w| w[0] < w[1]),
        "keys are time-ordered"
    );

    let mut src = S3Source::new(
        source_config(&s3, "orders", SchemaFrom::Config),
        schema(),
        Arc::new(CsvTransformer::new(false)),
    )
    .expect("source builds");
    assert_eq!(
        rows_of(&mut src).await,
        vec![
            (1, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string())
        ]
    );
}

#[tokio::test]
async fn the_sink_writes_an_object_once_max_bytes_accumulates() {
    let Some(s3) = common::try_start().await else {
        return;
    };

    let mut sink = S3Sink::new(
        sink_config(
            &s3,
            "out",
            "",
            Flush {
                max_rows: 0,
                max_bytes: 1,
                max_age_ms: 0,
            },
        ),
        schema(),
        Arc::new(CsvTransformer::new(false)),
    )
    .expect("sink builds");
    sink.write_batch(&batch(1, "one"))
        .await
        .expect("batch writes");
    sink.write_batch(&batch(2, "two"))
        .await
        .expect("batch writes");
    // No finish: a one-byte ceiling makes every batch its own object.
    assert_eq!(list_keys(&s3, "out").await.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_sink_writes_an_object_after_max_age_ms_without_a_batch() {
    let Some(s3) = common::try_start().await else {
        return;
    };

    let mut sink = S3Sink::new(
        sink_config(
            &s3,
            "aged",
            "",
            Flush {
                max_rows: 0,
                max_bytes: 0,
                max_age_ms: 500,
            },
        ),
        schema(),
        Arc::new(CsvTransformer::new(false)),
    )
    .expect("sink builds");
    sink.write_batch(&batch(7, "seven"))
        .await
        .expect("batch writes");
    // No further call and no finish: the ticker must fire on its own. The
    // multi-thread flavor lets the ticker task run while this future sleeps.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let keys = list_keys(&s3, "aged").await;
    assert_eq!(
        keys.len(),
        1,
        "the ticker wrote the object without any further call"
    );

    let mut src = S3Source::new(
        source_config(&s3, "aged", SchemaFrom::Config),
        schema(),
        Arc::new(CsvTransformer::new(false)),
    )
    .expect("source builds");
    assert_eq!(rows_of(&mut src).await, vec![(7, "seven".to_string())]);
}

#[tokio::test]
async fn all_thresholds_at_zero_writes_one_object_at_finish() {
    let Some(s3) = common::try_start().await else {
        return;
    };

    let mut sink = S3Sink::new(
        sink_config(
            &s3,
            "all",
            "",
            Flush {
                max_rows: 0,
                max_bytes: 0,
                max_age_ms: 0,
            },
        ),
        schema(),
        Arc::new(CsvTransformer::new(false)),
    )
    .expect("sink builds");
    for (id, name) in [(1, "one"), (2, "two"), (3, "three")] {
        sink.write_batch(&batch(id, name))
            .await
            .expect("batch writes");
    }
    assert!(
        list_keys(&s3, "all").await.is_empty(),
        "nothing before finish"
    );
    sink.finish().await.expect("finish");
    let keys = list_keys(&s3, "all").await;
    assert_eq!(keys.len(), 1, "exactly one object at finish");

    let mut src = S3Source::new(
        source_config(&s3, "all", SchemaFrom::Config),
        schema(),
        Arc::new(CsvTransformer::new(false)),
    )
    .expect("source builds");
    assert_eq!(
        rows_of(&mut src).await,
        vec![
            (1, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string())
        ]
    );
}

#[tokio::test]
async fn a_parquet_object_reads_back_under_both_schema_sources() {
    let Some(s3) = common::try_start().await else {
        return;
    };

    let mut sink = S3Sink::new(
        sink_config(
            &s3,
            "pq",
            ".parquet",
            Flush {
                max_rows: 0,
                max_bytes: 0,
                max_age_ms: 0,
            },
        ),
        schema(),
        Arc::new(ParquetTransformer::new()),
    )
    .expect("sink builds");
    sink.write_batch(&batch(4, "four"))
        .await
        .expect("batch writes");
    sink.finish().await.expect("finish");

    // Parquet carries its own schema, so the object is the schema source and
    // the rows round-trip field for field.
    let mut src = S3Source::new(
        source_config(&s3, "pq", SchemaFrom::Object),
        schema(),
        Arc::new(ParquetTransformer::new()),
    )
    .expect("source builds");
    assert_eq!(rows_of(&mut src).await, vec![(4, "four".to_string())]);

    // The same object under `schema_from "config"`: the declared schema is a
    // projection target rather than a promise the object has to match, so this
    // reads back the same rows.
    let mut projected = S3Source::new(
        source_config(&s3, "pq", SchemaFrom::Config),
        schema(),
        Arc::new(ParquetTransformer::new()),
    )
    .expect("source builds");
    assert_eq!(rows_of(&mut projected).await, vec![(4, "four".to_string())]);
}

#[tokio::test]
async fn a_source_over_an_empty_prefix_reports_eof() {
    let Some(s3) = common::try_start().await else {
        return;
    };

    let mut src = S3Source::new(
        source_config(&s3, "empty-prefix", SchemaFrom::Config),
        schema(),
        Arc::new(CsvTransformer::new(false)),
    )
    .expect("source builds");
    assert!(src.next_batch().await.expect("listing succeeds").is_none());
}
