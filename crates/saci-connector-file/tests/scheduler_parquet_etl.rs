// A file source and sink carrying the parquet transformer around a
// two-system pipeline.

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;

use saci_connector_file::{FileSink, FileSource};
use saci_core::SaciError;
use saci_core::dataset::Dataset;
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use saci_core::pipeline::Pipeline;
use saci_core::system::{ParallelSystem, SystemMeta, WriteSet};
use saci_transformer_parquet::ParquetTransformer;

struct ScaleSystem;

#[async_trait]
impl ParallelSystem for ScaleSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("scale_price")
            .read("Trade", "price")
            .write("Trade", "price")
    }
    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let batch = pipeline
            .batch_for("Trade")
            .ok_or_else(|| SaciError::generic("Trade component not found"))?;
        let prices = batch
            .column_by_name("price")
            .ok_or_else(|| SaciError::generic("price column missing"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| SaciError::generic("price is not Float64"))?;
        let scaled: Float64Array =
            Float64Array::from_iter_values(prices.values().iter().map(|&p| p * 1.1));
        Ok(WriteSet::new().put("Trade", "price", Arc::new(scaled)))
    }
}

struct RoundSystem;

#[async_trait]
impl ParallelSystem for RoundSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("round_price")
            .read("Trade", "price")
            .write("Trade", "rounded")
    }
    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let batch = pipeline
            .batch_for("Trade")
            .ok_or_else(|| SaciError::generic("Trade component not found"))?;
        let prices = batch
            .column_by_name("price")
            .ok_or_else(|| SaciError::generic("price column missing"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| SaciError::generic("price is not Float64"))?;
        let rounded: Int64Array =
            Int64Array::from_iter_values(prices.values().iter().map(|&p| p.round() as i64));
        Ok(WriteSet::new().put("Trade", "rounded", Arc::new(rounded)))
    }
}

fn trade_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("rounded", DataType::Int64, false),
    ]))
}

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
    ]))
}

async fn generate_input(path: &std::path::Path, n: usize) -> Result<(), SaciError> {
    let schema = input_schema();
    let ids = Arc::new(Int64Array::from_iter_values(0..n as i64));
    let prices = Arc::new(Float64Array::from_iter_values(
        (0..n).map(|i| (i as f64) * 0.01 + 1.0),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, prices])
        .map_err(|e| SaciError::generic(format!("{e}")))?;
    let mut sink = FileSink::create(path, Arc::new(ParquetTransformer::new()), schema)?;
    sink.write_batch(&batch).await?;
    sink.finish().await
}

struct PaddedParquetSource {
    inner: FileSource,
    full_schema: Arc<Schema>,
}

#[async_trait]
impl Source for PaddedParquetSource {
    fn schema(&self) -> Arc<Schema> {
        self.full_schema.clone()
    }
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        match self.inner.next_batch().await? {
            None => Ok(None),
            Some(batch) => {
                let n = batch.num_rows();
                let rounded: Arc<dyn arrow_array::Array> =
                    Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(0i64, n)));
                let id_col = batch.column_by_name("id").unwrap().clone();
                let price_col = batch.column_by_name("price").unwrap().clone();
                let padded = RecordBatch::try_new(
                    self.full_schema.clone(),
                    vec![id_col, price_col, rounded],
                )
                .map_err(|e| SaciError::generic(format!("{e}")))?;
                Ok(Some(padded))
            }
        }
    }
    fn estimated_rows(&self) -> Option<usize> {
        self.inner.estimated_rows()
    }
}

#[tokio::test]
async fn test_parquet_etl_pipeline_reads_scales_rounds_and_writes() {
    let dir = tempfile::tempdir().unwrap();
    let input_path = dir.path().join("input.parquet");
    let output_path = dir.path().join("output.parquet");

    generate_input(&input_path, 100).await.unwrap();

    let full_schema = trade_schema();
    let mut pipeline = Pipeline::new("parquet-etl");
    pipeline
        .data_mut()
        .register_raw_component("Trade", full_schema.clone());

    let inner_src = FileSource::open_async(&input_path, Arc::new(ParquetTransformer::new()), None)
        .await
        .unwrap();
    let src = PaddedParquetSource {
        inner: inner_src,
        full_schema: full_schema.clone(),
    };
    let sink = FileSink::create(
        &output_path,
        Arc::new(ParquetTransformer::new()),
        full_schema.clone(),
    )
    .unwrap();

    pipeline
        .add_parallel_system(ScaleSystem)
        .add_parallel_system(RoundSystem)
        .add_source("Trade", src)
        .add_sink("Trade", sink);

    pipeline.run_with_io().await.unwrap();

    assert_eq!(
        pipeline.data().rows(),
        100,
        "all 100 rows should be present"
    );

    let mut out_src =
        FileSource::open_async(&output_path, Arc::new(ParquetTransformer::new()), None)
            .await
            .unwrap();
    let batch = out_src
        .next_batch()
        .await
        .unwrap()
        .expect("output batch should exist");
    assert!(batch.num_rows() > 0);

    let prices = batch
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let rounded = batch
        .column_by_name("rounded")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // First row: input price = 1.0 * 1.1 = 1.1 → rounded = 1.
    let scaled_price = prices.value(0);
    assert!(
        (scaled_price - 1.1).abs() < 1e-9,
        "price should be scaled by 1.1: got {scaled_price}"
    );
    assert_eq!(rounded.value(0), 1, "rounded value should be 1");
}

#[tokio::test]
async fn test_parquet_etl_pipeline_runs_with_1000_rows() {
    let dir = tempfile::tempdir().unwrap();
    let input_path = dir.path().join("input_large.parquet");
    let output_path = dir.path().join("output_large.parquet");

    generate_input(&input_path, 1_000).await.unwrap();

    let full_schema = trade_schema();
    let mut pipeline = Pipeline::new("parquet-etl-large");
    pipeline
        .data_mut()
        .register_raw_component("Trade", full_schema.clone());

    let inner_src = FileSource::open_async(&input_path, Arc::new(ParquetTransformer::new()), None)
        .await
        .unwrap();
    let src = PaddedParquetSource {
        inner: inner_src,
        full_schema: full_schema.clone(),
    };
    let sink = FileSink::create(
        &output_path,
        Arc::new(ParquetTransformer::new()),
        full_schema.clone(),
    )
    .unwrap();

    pipeline
        .add_parallel_system(ScaleSystem)
        .add_parallel_system(RoundSystem)
        .add_source("Trade", src)
        .add_sink("Trade", sink);

    pipeline.run_with_io().await.unwrap();

    assert_eq!(pipeline.data().rows(), 1_000);
}
