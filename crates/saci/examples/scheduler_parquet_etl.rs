//! End-to-end Parquet ETL through the Source/Sink layer.
//!
//! Generates a synthetic 10 000-row `input.parquet` in a temp directory, reads
//! it with a [`FileSource`] carrying a [`ParquetCodec`] through
//! [`Pipeline::run`], scales `price` by 1.1 and derives a `rounded`
//! column with two [`ParallelSystem`]s, writes the result with a [`FileSink`],
//! then reads it back and prints five rows.

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use saci::codec::parquet::ParquetCodec;
use saci::connector::file::{FileSink, FileSource};
use saci::prelude::*;

/// Schema with `id` (Int64), `price` (Float64), `rounded` (Int64).
fn trade_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("rounded", DataType::Int64, false),
    ]))
}

/// Input schema. The pipeline adds the `rounded` column.
fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
    ]))
}

/// Multiply `price` by 1.1.
struct ScaleSystem;

#[async_trait]
impl ParallelSystem for ScaleSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("scale_price")
            .reads_named("Trade", "price")
            .writes_named("Trade", "price")
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let batch = pipeline.batch_named("Trade")?;
        let prices = batch
            .column_by_name("price")
            .ok_or_else(|| SaciError::generic("ScaleSystem: price column missing"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| SaciError::generic("ScaleSystem: price is not Float64"))?;

        let scaled: Float64Array =
            Float64Array::from_iter_values(prices.values().iter().map(|&p| p * 1.1));

        Ok(WriteSet::new().put_named("Trade", "price", Arc::new(scaled)))
    }
}

/// Round `price` into a new `rounded` (Int64) column.
struct RoundSystem;

#[async_trait]
impl ParallelSystem for RoundSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("round_price")
            .reads_named("Trade", "price")
            .writes_named("Trade", "rounded")
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let batch = pipeline.batch_named("Trade")?;
        let prices = batch
            .column_by_name("price")
            .ok_or_else(|| SaciError::generic("RoundSystem: price column missing"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| SaciError::generic("RoundSystem: price is not Float64"))?;

        let rounded: Int64Array =
            Int64Array::from_iter_values(prices.values().iter().map(|&p| p.round() as i64));

        Ok(WriteSet::new().put_named("Trade", "rounded", Arc::new(rounded)))
    }
}

async fn generate_input(path: &std::path::Path, n: usize) -> Result<(), SaciError> {
    let schema = input_schema();
    let ids = Arc::new(Int64Array::from_iter_values(0..n as i64));
    let prices = Arc::new(Float64Array::from_iter_values(
        (0..n).map(|i| (i as f64) * 0.01 + 1.0),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, prices])
        .map_err(|e| SaciError::generic(format!("generate_input: {e}")))?;

    let mut sink = FileSink::create(path, Arc::new(ParquetCodec::new()), schema)?;
    sink.write_batch(&batch).await?;
    sink.finish().await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let input_path = dir.path().join("input.parquet");
    let output_path = dir.path().join("output.parquet");

    println!("Generating 10 000-row input.parquet …");
    generate_input(&input_path, 10_000).await?;

    let full_schema = trade_schema();

    // The Parquet file holds only `id` and `price`, so the source pads a
    // zero-filled `rounded` column to match the registered Trade schema.
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
                    .map_err(|e| SaciError::generic(format!("PaddedParquetSource: {e}")))?;
                    Ok(Some(padded))
                }
            }
        }

        fn estimated_rows(&self) -> Option<usize> {
            self.inner.estimated_rows()
        }
    }

    let inner_src =
        FileSource::open_async(&input_path, Arc::new(ParquetCodec::new()), None).await?;
    let src = PaddedParquetSource {
        inner: inner_src,
        full_schema: full_schema.clone(),
    };
    let sink = FileSink::create(
        &output_path,
        Arc::new(ParquetCodec::new()),
        full_schema.clone(),
    )?;

    let mut pipeline = Pipeline::new("parquet-etl")
        .schema("Trade", full_schema.clone())
        .parallel_system(ScaleSystem)
        .parallel_system(RoundSystem)
        .source("Trade", src)
        .sink("Trade", sink);

    println!("Running pipeline (scale + round) …");
    pipeline.run().await?;
    println!(
        "Pipeline complete. {} rows processed.",
        pipeline.data().rows()
    );

    println!("\nFirst 5 rows of output.parquet:");
    println!("{:<8} {:<14} {:<10}", "id", "price", "rounded");
    println!("{}", "-".repeat(34));

    let mut out_src =
        FileSource::open_async(&output_path, Arc::new(ParquetCodec::new()), None).await?;
    let mut printed = 0usize;
    'outer: while let Some(batch) = out_src.next_batch().await? {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
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

        for i in 0..batch.num_rows() {
            if printed >= 5 {
                break 'outer;
            }
            println!(
                "{:<8} {:<14.4} {:<10}",
                ids.value(i),
                prices.value(i),
                rounded.value(i)
            );
            printed += 1;
        }
    }

    // `dir` is removed when it drops.
    println!("\nTemp files cleaned up.");
    Ok(())
}
