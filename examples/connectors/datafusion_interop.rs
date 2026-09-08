//! DataFusion interop: SQL query results ingested into a SACI pipeline.
//!
//! [`DataFusionSource`] runs a SQL query against a DataFusion
//! [`SessionContext`] and streams the results into a SACI [`Dataset`], which a
//! [`Pipeline`] then aggregates.
//!
//! ```bash
//! cargo run -p saci-connector-datafusion --example datafusion_interop
//! ```

use std::sync::Arc;

use arrow_array::{Float64Array, Int32Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use saci_connector_datafusion::DataFusionSource;
use saci_core::dataset::Dataset;
use saci_core::io::source::{Source, drain_into_dataset};
use saci_core::{Pipeline, SaciError, System, SystemMeta};

fn sales_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("product_id", DataType::Int32, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("unit_price", DataType::Float64, false),
    ]))
}

fn make_sales_batch() -> arrow_array::RecordBatch {
    let schema = sales_schema();
    let product_ids: Arc<dyn arrow_array::Array> =
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
    let regions: Arc<dyn arrow_array::Array> = Arc::new(StringArray::from(vec![
        "north", "south", "north", "east", "west", "north", "south", "east", "west", "north",
    ]));
    let quantities: Arc<dyn arrow_array::Array> =
        Arc::new(Int32Array::from(vec![10, 5, 20, 8, 15, 12, 3, 7, 18, 9]));
    let unit_prices: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(vec![
        9.99, 24.99, 4.99, 49.99, 14.99, 9.99, 24.99, 49.99, 14.99, 9.99,
    ]));
    arrow_array::RecordBatch::try_new(schema, vec![product_ids, regions, quantities, unit_prices])
        .expect("make_sales_batch failed")
}

struct TotalRevenue(f64);
struct RegionFilter(String);

/// Compute total revenue = SUM(quantity * unit_price) over the ingested batch.
struct ComputeRevenue;

#[async_trait]
impl System for ComputeRevenue {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("compute_revenue")
            .read("sales", "quantity")
            .read("sales", "unit_price")
            .write_resource::<TotalRevenue>()
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), SaciError> {
        let filter = pipeline
            .get_resource::<RegionFilter>()
            .map(|r| r.0.clone())
            .unwrap_or_default();

        let batch = pipeline
            .batch_for("sales")
            .ok_or_else(|| SaciError::generic("sales component not found"))?;

        let qty_col = batch
            .column_by_name("quantity")
            .ok_or_else(|| SaciError::generic("quantity column missing"))?
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("quantity is Int32");

        let price_col = batch
            .column_by_name("unit_price")
            .ok_or_else(|| SaciError::generic("unit_price column missing"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("unit_price is Float64");

        let region_col = batch
            .column_by_name("region")
            .ok_or_else(|| SaciError::generic("region column missing"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region is Utf8");

        let revenue: f64 = (0..qty_col.len())
            .filter(|&i| filter.is_empty() || region_col.value(i) == filter)
            .map(|i| qty_col.value(i) as f64 * price_col.value(i))
            .sum();

        pipeline.insert_resource(TotalRevenue(revenue));
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    let batch = make_sales_batch();
    println!("Registered {} rows of sales data", batch.num_rows());

    let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    ctx.register_table("sales_raw", Arc::new(provider))?;

    let sql = "SELECT product_id, region, quantity, unit_price \
               FROM sales_raw \
               WHERE region = 'north'";
    println!("SQL: {sql}");

    let mut src = DataFusionSource::from_sql(&ctx, sql).await?;
    println!("Result schema: {:?}", src.schema());

    let src_schema = src.schema();
    let mut dataset = Dataset::new();
    dataset.register_raw_component("sales", src_schema);

    let rows = drain_into_dataset(&mut src, &mut dataset, "sales").await?;
    println!("Ingested {rows} rows into SACI Dataset");

    // The SQL already filtered by region, so this matches every ingested row.
    dataset.insert_resource(RegionFilter("north".to_string()));

    let mut pipeline = Pipeline::new("datafusion");
    *pipeline.data_mut() = dataset;
    pipeline.add_system(ComputeRevenue);
    pipeline.run().await?;

    let revenue = pipeline
        .data()
        .get_resource::<TotalRevenue>()
        .map(|r| r.0)
        .unwrap_or(0.0);
    println!("Total revenue (north region): ${revenue:.2}");

    // 409.49 = 10*9.99 + 20*4.99 + 12*9.99 + 9*9.99, the four north rows.
    println!("Expected: $409.49");
    assert!(
        (revenue - 409.49).abs() < 0.01,
        "revenue mismatch: got {revenue:.2}"
    );
    println!("Correctness check passed.");

    Ok(())
}
