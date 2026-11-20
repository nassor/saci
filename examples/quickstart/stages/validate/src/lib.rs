//! Quick Start stage 1: validates each `Order` against `min_amount`.
//!
//! Built for `wasm32-wasip3`:
//!
//! ```bash
//! cargo build --release -p quickstart-validate-wasm --target wasm32-wasip3
//! ```

#![deny(missing_docs)]

use saci::processor::prelude::*;

/// One card authorisation row matching the Quick Start `Order` schema.
#[derive(Component, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct Order {
    /// Unique authorisation identifier.
    pub id: i64,
    /// Issuing region code.
    pub region: String,
    /// ISO currency code.
    pub currency: String,
    /// Authorisation amount in original currency.
    pub amount: f64,
    /// Whether the authorisation clears the `min_amount` check.
    pub valid: bool,
    /// USD-converted amount (reserved for downstream stages).
    pub usd_amount: f64,
    /// Formatted USD amount (reserved for downstream stages).
    pub usd_amount_display: String,
    /// Fraud risk score (reserved for downstream stages).
    pub risk_score: f64,
    /// Escalation flag (reserved for downstream stages).
    pub flagged: bool,
    /// Settlement fee computed downstream.
    pub fee: f64,
    /// Settlement disposition written downstream (`0` settled, `1` held, `2` rejected).
    pub review_tier: i64,
    /// Settlement label (reserved for downstream stages).
    pub settlement: String,
}

/// Mark `row.valid` according to the host-configured `min_amount` threshold.
#[transform(component = Order)]
pub fn validate(row: &mut Order, config: &Config) -> Result<()> {
    let min_amount: f64 = config.get("min_amount", 0.0)?;
    row.valid = row.amount > min_amount;
    if !row.valid {
        metric("validate.invalid_rows", 1.0);
    }
    Ok(())
}

/// Export the Quick Start validation pipeline as a `saci:pipeline@0.4.0` component.
#[processor(name = "quickstart-validate")]
pub fn build() -> Pipeline {
    Pipeline::new("quickstart-validate")
        .component::<Order>()
        .system(validate_system())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(id: i64, amount: f64) -> Order {
        Order {
            id,
            region: "emea".to_string(),
            currency: "USD".to_string(),
            amount,
            valid: false,
            usd_amount: 0.0,
            usd_amount_display: String::new(),
            risk_score: 0.0,
            flagged: false,
            fee: 0.0,
            review_tier: 0,
            settlement: String::new(),
        }
    }

    #[test]
    fn validates_orders_against_threshold() {
        let mut data = Dataset::new();
        data.register::<Order>().expect("register Order");
        data.append(&[sample(1, 0.0), sample(2, 25.0)])
            .expect("append");

        pollster::block_on(build().run_on(&mut data)).expect("run");

        let rows = <Order as Component>::from_record_batch(
            data.batch::<Order>().expect("Order registered"),
        )
        .expect("decode");
        assert!(!rows[0].valid);
        assert!(rows[1].valid);
    }

    #[test]
    fn order_schema_has_twelve_fields() {
        assert_eq!(<Order as Component>::schema().fields().len(), 12);
    }
}
