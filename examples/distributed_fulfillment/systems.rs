//! Processing systems for the order fulfillment pipeline.
//!
//! The field-level `SystemMeta` declarations resolve to four stages, and each
//! system's own doc comment names the fields it reads and writes:
//!
//! ```text
//! Stage 0  ValidateOrder, DetectFraud, ConvertCurrency  (parallel)
//! Stage 1  CheckInventory                              needs validation_status
//! Stage 2  ApproveOrder, ComputeTax                     (parallel)
//! Stage 3  GenerateInvoice                              appends Invoice rows
//! ```

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::{ArrayRef, Float64Array, RecordBatch, StringArray};
use async_trait::async_trait;

use saci_service::SaciError;
use saci_service::SystemConfig;
use saci_service::dataset::Dataset;
use saci_service::pipeline::Pipeline;
use saci_service::system::{ParallelSystem, System, SystemMeta, WriteSet};

use crate::components::{Invoice, Order};
use crate::resources::{FxRateTable, InventoryCatalog, NodeId, TaxRateTable};

fn node_id(pipeline: &Dataset) -> u64 {
    pipeline.get_resource::<NodeId>().map(|n| n.0).unwrap_or(0)
}

fn now_rfc3339() -> String {
    use std::fmt::Write as _;
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Epoch seconds; sub-second precision is not needed.
    let mut s = String::new();
    let _ = write!(s, "{secs}");
    s
}

/// Stage 0: marks an order "valid" when amount and quantity are both positive.
/// Writes `Order.validation_status` as a `WriteSet`.
pub struct ValidateOrderSystem;

#[async_trait]
impl ParallelSystem for ValidateOrderSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("validate_order")
            .reads(Order::AMOUNT_ORIGINAL)
            .reads(Order::QUANTITY)
            .writes(Order::VALIDATION_STATUS)
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let orders = pipeline.view::<Order>()?;
        let n = orders.len();
        let nid = node_id(pipeline);

        let amount_col = orders.f64("amount_original")?;
        let qty_col = orders.i64("quantity")?;

        let statuses: Vec<&str> = (0..n)
            .map(|i| {
                if amount_col.value(i) > 0.0 && qty_col.value(i) > 0 {
                    "valid"
                } else {
                    "invalid"
                }
            })
            .collect();

        let valid_count = statuses.iter().filter(|&&s| s == "valid").count();

        #[cfg(feature = "tracing")]
        tracing::info!(
            node_id = nid,
            stage = 0,
            system = "validate_order",
            rows = n,
            valid = valid_count,
            invalid = n - valid_count,
            "stage 0: order validation complete"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = nid;

        let arr: ArrayRef = Arc::new(StringArray::from(statuses));
        Ok(WriteSet::new().put("Order", "validation_status", arr))
    }
}

/// Stage 0: scores fraud risk from order amount and customer ID. Writes
/// `Order.fraud_score` in [0.0, 1.0].
pub struct DetectFraudSystem;

#[async_trait]
impl ParallelSystem for DetectFraudSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("detect_fraud")
            .reads(Order::AMOUNT_ORIGINAL)
            .reads(Order::CUSTOMER_ID)
            .writes(Order::FRAUD_SCORE)
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let orders = pipeline.view::<Order>()?;
        let n = orders.len();
        let nid = node_id(pipeline);

        let amount_col = orders.f64("amount_original")?;
        let cust_col = orders.str("customer_id")?;

        // Sample heuristic: orders over $2000 and customers in C800 to C900
        // score higher.
        let scores: Vec<f64> = (0..n)
            .map(|i| {
                let amount = amount_col.value(i);
                let cid = cust_col.value(i);
                let customer_num: u32 = cid.trim_start_matches('C').parse().unwrap_or(0);
                let mut score = 0.0f64;
                if amount > 2000.0 {
                    score += 0.35;
                }
                if (800..=900).contains(&customer_num) {
                    score += 0.40;
                }
                if amount > 4000.0 {
                    score += 0.20;
                }
                score.min(1.0)
            })
            .collect();

        let high_risk = scores.iter().filter(|&&s| s > 0.6).count();

        #[cfg(feature = "tracing")]
        tracing::info!(
            node_id = nid,
            stage = 0,
            system = "detect_fraud",
            rows = n,
            high_risk,
            "stage 0: fraud detection complete"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = nid;

        let arr: ArrayRef = Arc::new(Float64Array::from(scores));
        Ok(WriteSet::new().put("Order", "fraud_score", arr))
    }
}

/// Stage 0: converts `amount_original` to USD through the read-only
/// `FxRateTable` resource. Writes `Order.amount_usd`.
pub struct ConvertCurrencySystem;

#[async_trait]
impl ParallelSystem for ConvertCurrencySystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("convert_currency")
            .reads(Order::AMOUNT_ORIGINAL)
            .reads(Order::CURRENCY)
            .read_resource::<FxRateTable>()
            .writes(Order::AMOUNT_USD)
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let rates = pipeline
            .get_resource::<FxRateTable>()
            .ok_or_else(|| SaciError::generic("FxRateTable resource missing"))?;
        let orders = pipeline.view::<Order>()?;
        let n = orders.len();
        let nid = node_id(pipeline);

        let amount_col = orders.f64("amount_original")?;
        let currency_col = orders.str("currency")?;

        let usd_amounts: Vec<f64> = (0..n)
            .map(|i| {
                let rate = rates.rate(currency_col.value(i));
                amount_col.value(i) * rate
            })
            .collect();

        let total_usd: f64 = usd_amounts.iter().sum();

        #[cfg(feature = "tracing")]
        tracing::info!(
            node_id = nid,
            stage = 0,
            system = "convert_currency",
            rows = n,
            total_usd = format!("{total_usd:.2}"),
            "stage 0: currency conversion complete"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = nid;

        let arr: ArrayRef = Arc::new(Float64Array::from(usd_amounts));
        Ok(WriteSet::new().put("Order", "amount_usd", arr))
    }
}

/// Stage 1: checks availability against the `InventoryCatalog` resource and
/// writes `Order.inventory_status`; orders that failed validation stay
/// "pending". A sequential `System`, because `replace_batch` needs
/// `&mut Dataset`.
pub struct CheckInventorySystem;

#[async_trait]
impl System for CheckInventorySystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("check_inventory")
            .reads(Order::PRODUCT_ID)
            .reads(Order::QUANTITY)
            .reads(Order::VALIDATION_STATUS)
            .read_resource::<InventoryCatalog>()
            .writes(Order::INVENTORY_STATUS)
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), SaciError> {
        let catalog = pipeline
            .get_resource::<InventoryCatalog>()
            .ok_or_else(|| SaciError::generic("InventoryCatalog resource missing"))?;

        // Copy the columns out so `replace_batch` can take `&mut Dataset` once
        // the view is dropped.
        let (n, nid, product_ids, quantities, validation_statuses, batch_for_rebuild) = {
            let orders = pipeline.view::<Order>()?;
            let n = orders.len();
            let nid = node_id(pipeline);
            let product_ids: Vec<String> = orders
                .str("product_id")?
                .iter()
                .map(|v| v.unwrap_or("").to_owned())
                .collect();
            let quantities: Vec<i64> = orders.i64("quantity")?.values().to_vec();
            let validation_statuses: Vec<String> = orders
                .str("validation_status")?
                .iter()
                .map(|v| v.unwrap_or("").to_owned())
                .collect();
            let batch = orders.batch().clone();
            (n, nid, product_ids, quantities, validation_statuses, batch)
        };

        let mut statuses: Vec<String> = Vec::with_capacity(n);
        let mut available_count = 0usize;
        let mut low_stock_count = 0usize;
        let mut out_of_stock_count = 0usize;

        for i in 0..n {
            if validation_statuses[i] != "valid" {
                statuses.push("pending".into());
                continue;
            }
            let product = product_ids[i].as_str();
            let qty_needed = quantities[i];
            let available = catalog.available(product);
            let status = if available >= qty_needed {
                available_count += 1;
                "available"
            } else if available > 0 {
                low_stock_count += 1;
                "low_stock"
            } else {
                out_of_stock_count += 1;
                "out_of_stock"
            };
            statuses.push(status.into());
        }

        #[cfg(feature = "tracing")]
        tracing::info!(
            node_id = nid,
            stage = 1,
            system = "check_inventory",
            rows = n,
            available = available_count,
            low_stock = low_stock_count,
            out_of_stock = out_of_stock_count,
            "stage 1: inventory check complete"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = nid;

        let schema = batch_for_rebuild.schema();
        let new_status_arr: ArrayRef = Arc::new(StringArray::from(
            statuses.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        ));
        let inv_idx = schema.index_of("inventory_status").unwrap();
        let new_cols: Vec<ArrayRef> = (0..schema.fields().len())
            .map(|i| {
                if i == inv_idx {
                    new_status_arr.clone()
                } else {
                    batch_for_rebuild.column(i).clone()
                }
            })
            .collect();
        let new_batch = RecordBatch::try_new(schema, new_cols)
            .map_err(|e| SaciError::generic(format!("CheckInventory: rebuild batch: {e}")))?;
        pipeline.replace_batch::<Order>(new_batch)?;
        Ok(())
    }
}

/// Stage 2: approves or rejects each order from its fraud score, inventory
/// status, and validation status. Writes `Order.processing_status`.
pub struct ApproveOrderSystem;

#[async_trait]
impl ParallelSystem for ApproveOrderSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("approve_order")
            .reads(Order::FRAUD_SCORE)
            .reads(Order::INVENTORY_STATUS)
            .reads(Order::VALIDATION_STATUS)
            .writes(Order::PROCESSING_STATUS)
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let orders = pipeline.view::<Order>()?;
        let n = orders.len();
        let nid = node_id(pipeline);

        let fraud_col = orders.f64("fraud_score")?;
        let inventory_col = orders.str("inventory_status")?;
        let validation_col = orders.str("validation_status")?;

        let mut approved_count = 0usize;
        let statuses: Vec<&str> = (0..n)
            .map(|i| {
                let is_valid = validation_col.value(i) == "valid";
                let has_stock = matches!(inventory_col.value(i), "available" | "low_stock");
                let low_fraud = fraud_col.value(i) < 0.7;

                if is_valid && has_stock && low_fraud {
                    approved_count += 1;
                    "approved"
                } else {
                    "rejected"
                }
            })
            .collect();

        let rejected_count = n - approved_count;

        #[cfg(feature = "tracing")]
        tracing::info!(
            node_id = nid,
            stage = 2,
            system = "approve_order",
            rows = n,
            approved = approved_count,
            rejected = rejected_count,
            "stage 2: order approval complete"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = nid;

        let arr: ArrayRef = Arc::new(StringArray::from(statuses));
        Ok(WriteSet::new().put("Order", "processing_status", arr))
    }
}

/// Stage 2: computes regional tax on `amount_usd` through the `TaxRateTable`
/// resource. Writes `Order.tax_rate` and `Order.tax_amount`, which are disjoint
/// from ApproveOrderSystem's write, so the two run concurrently.
pub struct ComputeTaxSystem;

#[async_trait]
impl ParallelSystem for ComputeTaxSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("compute_tax")
            .reads(Order::AMOUNT_USD)
            .reads(Order::REGION)
            .read_resource::<TaxRateTable>()
            .writes(Order::TAX_RATE)
            .writes(Order::TAX_AMOUNT)
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let tax_table = pipeline
            .get_resource::<TaxRateTable>()
            .ok_or_else(|| SaciError::generic("TaxRateTable resource missing"))?;
        let orders = pipeline.view::<Order>()?;
        let n = orders.len();
        let nid = node_id(pipeline);

        let amount_col = orders.f64("amount_usd")?;
        let region_col = orders.str("region")?;

        let mut rates = Vec::with_capacity(n);
        let mut amounts = Vec::with_capacity(n);
        let mut total_tax = 0.0f64;

        for i in 0..n {
            let rate = tax_table.rate(region_col.value(i));
            let tax = amount_col.value(i) * rate;
            rates.push(rate);
            amounts.push(tax);
            total_tax += tax;
        }

        #[cfg(feature = "tracing")]
        tracing::info!(
            node_id = nid,
            stage = 2,
            system = "compute_tax",
            rows = n,
            total_tax = format!("{total_tax:.2}"),
            "stage 2: tax computation complete"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = nid;

        let rate_arr: ArrayRef = Arc::new(Float64Array::from(rates));
        let amount_arr: ArrayRef = Arc::new(Float64Array::from(amounts));
        Ok(WriteSet::new().put("Order", "tax_rate", rate_arr).put(
            "Order",
            "tax_amount",
            amount_arr,
        ))
    }
}

/// Stage 3: appends one `Invoice` row per order. Rejected orders get a
/// "skipped" invoice. Retries with a fixed backoff.
pub struct GenerateInvoiceSystem;

impl GenerateInvoiceSystem {
    fn config() -> SystemConfig {
        SystemConfig::new().with_fixed_retry(3, Duration::from_millis(50))
    }
}

#[async_trait]
impl System for GenerateInvoiceSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("generate_invoice")
            .reads(Order::ID)
            .reads(Order::AMOUNT_USD)
            .reads(Order::TAX_RATE)
            .reads(Order::TAX_AMOUNT)
            .reads(Order::PROCESSING_STATUS)
            .write_component("Invoice")
    }

    fn config(&self) -> SystemConfig {
        Self::config()
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), SaciError> {
        // Copy the columns out so `append` can take `&mut Dataset` once the
        // view is dropped.
        let (n, nid, id_col, amount_col, tax_rate_col, tax_amount_col, status_col) = {
            let orders = pipeline.view::<Order>()?;
            (
                orders.len(),
                node_id(pipeline),
                orders
                    .str("id")?
                    .iter()
                    .map(|v| v.unwrap_or("").to_owned())
                    .collect::<Vec<_>>(),
                orders.f64("amount_usd")?.values().to_vec(),
                orders.f64("tax_rate")?.values().to_vec(),
                orders.f64("tax_amount")?.values().to_vec(),
                orders
                    .str("processing_status")?
                    .iter()
                    .map(|v| v.unwrap_or("").to_owned())
                    .collect::<Vec<_>>(),
            )
        };

        let issued_at = now_rfc3339();
        let mut invoices = Vec::with_capacity(n);
        let mut issued_count = 0usize;

        for i in 0..n {
            let processing_status = status_col[i].as_str();
            let subtotal = amount_col[i];
            let tax_amount = tax_amount_col[i];
            let (inv_status, total) = if processing_status == "approved" {
                issued_count += 1;
                ("issued", subtotal + tax_amount)
            } else {
                ("skipped", 0.0)
            };
            invoices.push(Invoice {
                order_id: id_col[i].clone(),
                subtotal,
                tax_rate: tax_rate_col[i],
                tax_amount,
                total,
                issued_at: issued_at.clone(),
                status: inv_status.to_string(),
            });
        }

        #[cfg(feature = "tracing")]
        tracing::info!(
            node_id = nid,
            stage = 3,
            system = "generate_invoice",
            rows = n,
            issued = issued_count,
            skipped = n - issued_count,
            "stage 3: invoice generation complete"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = nid;

        pipeline.append::<Invoice>(&invoices)?;
        Ok(())
    }
}

/// Assemble the fulfillment pipeline. Its seven systems resolve to four stages
/// from their `SystemMeta` declarations.
pub fn build_pipeline() -> Pipeline {
    let mut pipeline = Pipeline::new("fulfillment");

    pipeline.add_parallel_system(ValidateOrderSystem);
    pipeline.add_parallel_system(DetectFraudSystem);
    pipeline.add_parallel_system(ConvertCurrencySystem);

    pipeline.add_system(CheckInventorySystem);

    pipeline.add_parallel_system(ApproveOrderSystem);
    pipeline.add_parallel_system(ComputeTaxSystem);

    pipeline.add_system(GenerateInvoiceSystem);

    pipeline
}
