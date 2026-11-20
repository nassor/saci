//! The smallest complete SACI pipeline: seed five orders, convert and flag them
//! in one stage, then summarise.
//!
//! `ConvertCurrency` and `FlagExpress` write disjoint fields, so the
//! field-level DAG puts them in the same stage:
//!
//! ```text
//! Stage 0:  [SeedOrders]                    writes every field
//! Stage 1:  [ConvertCurrency, FlagExpress]  concurrent, "usd_amount", "express"
//! Stage 2:  [summary]                       reads every field
//! ```
//!
//! ```bash
//! cargo run -p saci --example first_pipeline
//! ```

use std::sync::Arc;

use arrow_array::{BooleanArray, Float64Array};
use saci::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Component, Serialize, Deserialize, Clone, Debug)]
struct Order {
    id: u64,
    currency: String,
    amount: f64,
    usd_amount: f64,
    express: bool,
}

/// USD-base exchange rates, held as a resource rather than a column.
struct FxRates {
    eur: f64,
    gbp: f64,
    jpy: f64,
}

impl FxRates {
    fn rate_for(&self, currency: &str) -> f64 {
        match currency {
            "EUR" => self.eur,
            "GBP" => self.gbp,
            "JPY" => self.jpy,
            // "USD" is the reporting currency, and an unknown code converts
            // one to one rather than collapsing the row to zero.
            _ => 1.0,
        }
    }
}

/// What the summary system leaves behind for `main` to read.
struct Summary {
    rows: usize,
    express: usize,
    total_usd: f64,
}

struct SeedOrders;

#[async_trait]
impl System for SeedOrders {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("seed").writes_all::<Order>()
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        let orders = vec![
            Order {
                id: 1,
                currency: "EUR".into(),
                amount: 120.00,
                usd_amount: 0.0,
                express: false,
            },
            Order {
                id: 2,
                currency: "USD".into(),
                amount: 4300.00,
                usd_amount: 0.0,
                express: false,
            },
            Order {
                id: 3,
                currency: "GBP".into(),
                amount: 75.50,
                usd_amount: 0.0,
                express: false,
            },
            Order {
                id: 4,
                currency: "JPY".into(),
                amount: 900000.00,
                usd_amount: 0.0,
                express: false,
            },
            Order {
                id: 5,
                currency: "EUR".into(),
                amount: 12.00,
                usd_amount: 0.0,
                express: false,
            },
        ];

        let n = orders.len();
        data.append::<Order>(&orders)?;
        println!("seeded {n} orders");
        Ok(())
    }
}

struct ConvertCurrency;

#[async_trait]
impl ParallelSystem for ConvertCurrency {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("convert")
            .reads(Order::AMOUNT)
            .reads(Order::CURRENCY)
            .writes(Order::USD_AMOUNT)
            .reads_resource::<FxRates>()
    }

    async fn run(&self, data: &Dataset) -> Result<WriteSet, SaciError> {
        let rates = data.resource::<FxRates>()?;

        let orders = data.view::<Order>()?;
        let amount = orders.f64(Order::AMOUNT)?;
        let currency = orders.str(Order::CURRENCY)?;
        let values: Vec<f64> = (0..orders.len())
            .map(|i| amount.value(i) * rates.rate_for(currency.value(i)))
            .collect();

        Ok(WriteSet::new().put(Order::USD_AMOUNT, Arc::new(Float64Array::from(values))))
    }
}

struct FlagExpress;

#[async_trait]
impl ParallelSystem for FlagExpress {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("express")
            .reads(Order::AMOUNT)
            .writes(Order::EXPRESS)
    }

    async fn run(&self, data: &Dataset) -> Result<WriteSet, SaciError> {
        let orders = data.view::<Order>()?;
        let amount = orders.f64(Order::AMOUNT)?;
        let flags: Vec<bool> = (0..orders.len())
            .map(|i| amount.value(i) > 1000.0)
            .collect();

        Ok(WriteSet::new().put(Order::EXPRESS, Arc::new(BooleanArray::from(flags))))
    }
}

fn make_summary_system() -> impl System {
    system_fn(
        SystemMeta::new("summary")
            .reads(Order::ID)
            .reads(Order::CURRENCY)
            .reads(Order::AMOUNT)
            .reads(Order::USD_AMOUNT)
            .reads(Order::EXPRESS)
            .writes_resource::<Summary>(),
        |data| {
            let rows;
            let mut express = 0usize;
            let mut total_usd = 0.0f64;

            {
                let orders = data.view::<Order>()?;
                rows = orders.len();

                let id = orders.u64(Order::ID)?;
                let currency = orders.str(Order::CURRENCY)?;
                let amount = orders.f64(Order::AMOUNT)?;
                let usd = orders.f64(Order::USD_AMOUNT)?;
                let is_express = orders.bool(Order::EXPRESS)?;

                for i in 0..rows {
                    if is_express.value(i) {
                        express += 1;
                    }
                    total_usd += usd.value(i);
                    println!(
                        "order {}  {:>10.2} {} -> {:>10.2} USD  express={}",
                        id.value(i),
                        amount.value(i),
                        currency.value(i),
                        usd.value(i),
                        is_express.value(i)
                    );
                }
            }

            data.insert_resource(Summary {
                rows,
                express,
                total_usd,
            });

            Ok(())
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), SaciError> {
    let mut pipeline = Pipeline::new("first-pipeline")
        .component::<Order>()
        .resource(FxRates {
            eur: 1.08,
            gbp: 1.27,
            jpy: 0.0067,
        })
        .system(SeedOrders)
        .parallel_system(ConvertCurrency)
        .parallel_system(FlagExpress)
        .system(make_summary_system());

    pipeline.run().await?;

    println!("stages: {:?}", pipeline.plan()?.to_vec());

    let summary = pipeline.data().resource::<Summary>()?;
    println!(
        "{} orders, {} express, {:.2} USD total",
        summary.rows, summary.express, summary.total_usd
    );

    Ok(())
}
