//! Non-columnar pipeline resources for the fulfillment pipeline.
//!
//! Resources are singleton Rust values stored in the `Pipeline` by TypeId.
//! They hold lookup tables and configuration shared between systems, outside
//! the Arrow columns and outside the field-granular DAG scheduler.

use std::collections::HashMap;

/// Currency-to-USD conversion rates, USD as base. Read by
/// `ConvertCurrencySystem` in Stage 0 and never mutated after construction.
pub struct FxRateTable(pub HashMap<String, f64>);

impl Default for FxRateTable {
    fn default() -> Self {
        let mut m = HashMap::new();
        m.insert("USD".into(), 1.00);
        m.insert("EUR".into(), 1.08);
        m.insert("GBP".into(), 1.27);
        m.insert("JPY".into(), 0.0067);
        m.insert("CAD".into(), 0.74);
        Self(m)
    }
}

impl FxRateTable {
    /// Return the USD rate for `currency`, defaulting to 1.0 for unknowns.
    pub fn rate(&self, currency: &str) -> f64 {
        *self.0.get(currency).unwrap_or(&1.0)
    }
}

/// Regional tax rates as fractions, not percentages. Read by `ComputeTaxSystem`
/// in Stage 2 and never mutated after construction.
pub struct TaxRateTable(pub HashMap<String, f64>);

impl Default for TaxRateTable {
    fn default() -> Self {
        let mut m = HashMap::new();
        m.insert("us-east".into(), 0.08);
        m.insert("us-west".into(), 0.095);
        m.insert("eu-west".into(), 0.20);
        m.insert("ap-south".into(), 0.18);
        Self(m)
    }
}

impl TaxRateTable {
    /// Return the tax rate for `region`, defaulting to 0.10 for unknowns.
    pub fn rate(&self, region: &str) -> f64 {
        *self.0.get(region).unwrap_or(&0.10)
    }
}

/// Available stock per product ID, read by `CheckInventorySystem` in Stage 1.
/// A real deployment would pull this snapshot from an inventory service.
pub struct InventoryCatalog(pub HashMap<String, i64>);

impl Default for InventoryCatalog {
    fn default() -> Self {
        let mut m = HashMap::new();
        // P001 to P010: well stocked.
        for i in 1..=10u32 {
            m.insert(format!("P{i:03}"), 1000i64);
        }
        // P011 to P015: low stock.
        for i in 11..=15u32 {
            m.insert(format!("P{i:03}"), 5i64);
        }
        // P016 to P020: out of stock.
        for i in 16..=20u32 {
            m.insert(format!("P{i:03}"), 0i64);
        }
        Self(m)
    }
}

impl InventoryCatalog {
    /// Return available quantity for `product_id`, 0 for unknown products.
    pub fn available(&self, product_id: &str) -> i64 {
        *self.0.get(product_id).unwrap_or(&0)
    }
}

/// Identifies the cluster node processing the current batch. Injected by the
/// pipeline factory, read by systems to annotate trace logs.
pub struct NodeId(pub u64);
