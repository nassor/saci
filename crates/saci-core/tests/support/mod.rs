//! Fixtures shared by the `scheduler_etl*` integration tests: the component, the
//! FX table, the report resource, and the seed rows.
//!
//! Each test binary compiles its own copy, so items a given test never touches
//! would otherwise warn.
#![allow(dead_code)]

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use saci_core::component::Component;
use saci_core::system::FieldRef;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Transaction {
    pub id: u64,
    pub amount: f64,
    pub currency: String,
    pub valid: bool,
    pub usd_amount: f64,
}

impl Component for Transaction {
    fn name() -> &'static str {
        "Transaction"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("valid", DataType::Boolean, false),
            Field::new("usd_amount", DataType::Float64, false),
        ]))
    }
}

impl Transaction {
    pub const AMOUNT: FieldRef<Transaction> = FieldRef::new("amount");
    pub const CURRENCY: FieldRef<Transaction> = FieldRef::new("currency");
    pub const VALID: FieldRef<Transaction> = FieldRef::new("valid");
    pub const USD_AMOUNT: FieldRef<Transaction> = FieldRef::new("usd_amount");
}

pub struct FxRates {
    pub eur: f64,
    pub gbp: f64,
    pub jpy: f64,
    pub cad: f64,
}

impl FxRates {
    pub fn rate_for(&self, currency: &str) -> f64 {
        match currency {
            "USD" => 1.0,
            "EUR" => self.eur,
            "GBP" => self.gbp,
            "JPY" => self.jpy,
            "CAD" => self.cad,
            _ => 1.0,
        }
    }
}

pub struct Report {
    pub total_rows: usize,
    pub valid_count: usize,
    pub rejected_count: usize,
    pub total_usd: f64,
}

/// The nine seed transactions both ETL tests ingest.
///
/// `ValidateSystem` rejects `id` 1004 (negative) and 1008 (zero). The assertions
/// in each test depend on these exact values.
pub fn seed_transactions() -> Vec<Transaction> {
    vec![
        Transaction {
            id: 1001,
            amount: 1500.00,
            currency: "USD".into(),
            valid: false,
            usd_amount: 0.0,
        },
        Transaction {
            id: 1002,
            amount: 2300.50,
            currency: "EUR".into(),
            valid: false,
            usd_amount: 0.0,
        },
        Transaction {
            id: 1003,
            amount: 750.00,
            currency: "GBP".into(),
            valid: false,
            usd_amount: 0.0,
        },
        Transaction {
            id: 1004,
            amount: -50.00,
            currency: "USD".into(),
            valid: false,
            usd_amount: 0.0,
        },
        Transaction {
            id: 1005,
            amount: 5000.00,
            currency: "JPY".into(),
            valid: false,
            usd_amount: 0.0,
        },
        Transaction {
            id: 1006,
            amount: 320.75,
            currency: "EUR".into(),
            valid: false,
            usd_amount: 0.0,
        },
        Transaction {
            id: 1007,
            amount: 1200.00,
            currency: "GBP".into(),
            valid: false,
            usd_amount: 0.0,
        },
        Transaction {
            id: 1008,
            amount: 0.00,
            currency: "USD".into(),
            valid: false,
            usd_amount: 0.0,
        },
        Transaction {
            id: 1009,
            amount: 680.00,
            currency: "CAD".into(),
            valid: false,
            usd_amount: 0.0,
        },
    ]
}
