//! Synthetic order generator.
//!
//! [`run_generator`] runs as a background task on the bootstrap node. Every
//! `interval` it produces a batch of `Order` rows, serialises them to Arrow
//! IPC, and registers the batch through the cluster store.
//!
//! On a non-leader node `register_master_batch` fails; the generator logs a
//! warning and skips the round. Only the leader produces new work.

use std::sync::Arc;
use std::time::Duration;

use rand::RngExt;

use saci_service::Dataset;

use crate::components::Order;
use crate::store::FulfillmentStore;

const CURRENCIES: &[&str] = &["USD", "EUR", "GBP", "JPY", "CAD"];
const REGIONS: &[&str] = &["us-east", "us-west", "eu-west", "ap-south"];

/// Arrow schema version; bump it when the `Order` schema changes.
pub const SCHEMA_ID: u32 = 1;

/// Background task: generate and register synthetic `Order` batches until the
/// process is killed. `node_id` only annotates traces.
pub async fn run_generator(store: Arc<FulfillmentStore>, node_id: u64, interval: Duration) {
    let mut batch_counter: u64 = 0;

    loop {
        let orders = generate_orders(300, 500);
        let row_count = orders.len() as u32;

        match serialise_orders(&orders) {
            Ok(ipc) => {
                match store
                    .register_batch(
                        batch_counter,
                        "Order".to_string(),
                        SCHEMA_ID,
                        ipc,
                        row_count,
                    )
                    .await
                {
                    Ok(()) => {
                        #[cfg(feature = "tracing")]
                        tracing::info!(
                            node_id,
                            batch_id = batch_counter,
                            rows = row_count,
                            "generator: registered batch"
                        );
                        batch_counter += 1;
                    }
                    Err(e) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            node_id,
                            batch_id = batch_counter,
                            error = %e,
                            "generator: skipping batch (not leader or cluster error)"
                        );
                    }
                }
            }
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::error!(node_id, error = %e, "generator: failed to serialise orders");
            }
        }

        tokio::time::sleep(interval).await;
    }
}

/// Generate between `min_rows` and `max_rows` synthetic `Order` rows.
fn generate_orders(min_rows: usize, max_rows: usize) -> Vec<Order> {
    let mut rng = rand::rng();
    let count = rng.random_range(min_rows..=max_rows);
    (0..count).map(|_| random_order(&mut rng)).collect()
}

fn random_order(rng: &mut impl rand::Rng) -> Order {
    let product_idx = rng.random_range(1u32..=20u32);
    let product_id = format!("P{product_idx:03}");

    let customer_idx = rng.random_range(100u32..=999u32);
    let customer_id = format!("C{customer_idx}");

    let currency = CURRENCIES[rng.random_range(0..CURRENCIES.len())];
    let region = REGIONS[rng.random_range(0..REGIONS.len())];
    let quantity = rng.random_range(1i64..=10i64);
    let amount_original = rng.random_range(10.0f64..=5000.0f64);

    Order {
        id: uuid::Uuid::now_v7().to_string(),
        customer_id,
        product_id,
        quantity,
        amount_original,
        currency: currency.to_string(),
        amount_usd: 0.0,
        region: region.to_string(),
        fraud_score: 0.0,
        tax_rate: 0.0,
        tax_amount: 0.0,
        validation_status: "pending".to_string(),
        inventory_status: "pending".to_string(),
        processing_status: "pending".to_string(),
    }
}

/// Serialise a batch of `Order` rows to Arrow IPC bytes via a `Dataset`.
fn serialise_orders(orders: &[Order]) -> saci_service::SaciResult<Vec<u8>> {
    let mut dataset = Dataset::new();
    dataset.register_component::<Order>()?;
    dataset.append::<Order>(orders)?;

    let mut buf = Vec::new();
    dataset.write_ipc(&mut buf)?;
    Ok(buf)
}
