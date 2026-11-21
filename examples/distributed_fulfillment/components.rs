//! Arrow-columnar component definitions for the fulfillment pipeline.
//!
//! `Order` carries the generator's input fields plus the pipeline-computed
//! ones, which start at defaults and are filled stage by stage. `Invoice`
//! starts empty and gains rows in Stage 3.

use serde::{Deserialize, Serialize};

use saci::Component;

/// A customer order flowing through the fulfillment pipeline. The generator
/// writes the input fields; each remaining field records the system and stage
/// that fills it.
#[derive(Component, Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    /// Unique order ID (UUID string).
    pub id: String,
    pub customer_id: String,
    pub product_id: String,
    /// Number of units ordered.
    pub quantity: i64,
    /// Original amount in the native currency.
    pub amount_original: f64,
    /// ISO currency code ("USD", "EUR", "GBP", "JPY", "CAD").
    pub currency: String,
    /// Amount converted to USD. Filled by ConvertCurrencySystem (Stage 0).
    pub amount_usd: f64,
    /// Geographic region ("us-east", "us-west", "eu-west", "ap-south").
    pub region: String,
    /// Fraud probability score [0.0, 1.0]. Filled by DetectFraudSystem (Stage 0).
    pub fraud_score: f64,
    /// Applicable tax rate. Filled by ComputeTaxSystem (Stage 2).
    pub tax_rate: f64,
    /// Tax amount in USD. Filled by ComputeTaxSystem (Stage 2).
    pub tax_amount: f64,
    /// "pending" | "valid" | "invalid". Filled by ValidateOrderSystem (Stage 0).
    pub validation_status: String,
    /// "pending" | "available" | "low_stock" | "out_of_stock".
    /// Filled by CheckInventorySystem (Stage 1).
    pub inventory_status: String,
    /// "pending" | "approved" | "rejected".
    /// Filled by ApproveOrderSystem (Stage 2).
    pub processing_status: String,
}

/// A generated invoice. `GenerateInvoiceSystem` appends one row per order in
/// Stage 3; rejected orders get `status = "skipped"`.
#[derive(Component, Debug, Clone, Serialize, Deserialize)]
pub struct Invoice {
    /// Matches the originating `Order.id`.
    pub order_id: String,
    /// Pre-tax total in USD (`Order.amount_usd`).
    pub subtotal: f64,
    /// Tax rate applied (from `Order.tax_rate`).
    pub tax_rate: f64,
    /// Tax amount in USD (from `Order.tax_amount`).
    pub tax_amount: f64,
    /// Grand total including tax.
    pub total: f64,
    /// RFC 3339 issue timestamp.
    pub issued_at: String,
    /// "issued" (approved orders) | "skipped" (rejected orders).
    pub status: String,
}
