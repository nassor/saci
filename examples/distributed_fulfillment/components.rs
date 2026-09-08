//! Arrow-columnar component definitions for the fulfillment pipeline.
//!
//! `Order` carries the generator's input fields plus the pipeline-computed
//! ones, which start at defaults and are filled stage by stage. `Invoice`
//! starts empty and gains rows in Stage 3.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use saci_service::component::Component;
use saci_service::system::FieldRef;

/// A customer order flowing through the fulfillment pipeline. The generator
/// writes the input fields; each remaining field records the system and stage
/// that fills it.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl Component for Order {
    fn name() -> &'static str {
        "Order"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("product_id", DataType::Utf8, false),
            Field::new("quantity", DataType::Int64, false),
            Field::new("amount_original", DataType::Float64, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("amount_usd", DataType::Float64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("fraud_score", DataType::Float64, false),
            Field::new("tax_rate", DataType::Float64, false),
            Field::new("tax_amount", DataType::Float64, false),
            Field::new("validation_status", DataType::Utf8, false),
            Field::new("inventory_status", DataType::Utf8, false),
            Field::new("processing_status", DataType::Utf8, false),
        ]))
    }
}

impl Order {
    pub const ID: FieldRef<Order> = FieldRef::new("id");
    pub const CUSTOMER_ID: FieldRef<Order> = FieldRef::new("customer_id");
    pub const PRODUCT_ID: FieldRef<Order> = FieldRef::new("product_id");
    pub const QUANTITY: FieldRef<Order> = FieldRef::new("quantity");
    pub const AMOUNT_ORIGINAL: FieldRef<Order> = FieldRef::new("amount_original");
    pub const CURRENCY: FieldRef<Order> = FieldRef::new("currency");
    pub const AMOUNT_USD: FieldRef<Order> = FieldRef::new("amount_usd");
    pub const REGION: FieldRef<Order> = FieldRef::new("region");
    pub const FRAUD_SCORE: FieldRef<Order> = FieldRef::new("fraud_score");
    pub const TAX_RATE: FieldRef<Order> = FieldRef::new("tax_rate");
    pub const TAX_AMOUNT: FieldRef<Order> = FieldRef::new("tax_amount");
    pub const VALIDATION_STATUS: FieldRef<Order> = FieldRef::new("validation_status");
    pub const INVENTORY_STATUS: FieldRef<Order> = FieldRef::new("inventory_status");
    pub const PROCESSING_STATUS: FieldRef<Order> = FieldRef::new("processing_status");
}

/// A generated invoice. `GenerateInvoiceSystem` appends one row per order in
/// Stage 3; rejected orders get `status = "skipped"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl Component for Invoice {
    fn name() -> &'static str {
        "Invoice"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, false),
            Field::new("subtotal", DataType::Float64, false),
            Field::new("tax_rate", DataType::Float64, false),
            Field::new("tax_amount", DataType::Float64, false),
            Field::new("total", DataType::Float64, false),
            Field::new("issued_at", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]))
    }
}
