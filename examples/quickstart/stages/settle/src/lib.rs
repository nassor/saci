//! Quick Start stage 2: assigns settlement `fee` and `review_tier`.
//!
//! Built for `wasm32-wasip3`:
//!
//! ```bash
//! cargo build --release -p quickstart-settle-wasm --target wasm32-wasip3
//! ```

#![deny(missing_docs)]

use saci::processor::prelude::*;

const TIER_SETTLED: i64 = 0;
const TIER_HOLD: i64 = 1;
const TIER_REJECTED: i64 = 2;

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
    /// Validation verdict written by stage 1 (`validate`).
    pub valid: bool,
    /// USD-converted amount (reserved for downstream stages).
    pub usd_amount: f64,
    /// Formatted USD amount (reserved for downstream stages).
    pub usd_amount_display: String,
    /// Fraud risk score (reserved for downstream stages).
    pub risk_score: f64,
    /// Escalation flag (reserved for downstream stages).
    pub flagged: bool,
    /// Computed settlement fee in cents, rounded to two decimal places.
    pub fee: f64,
    /// Settlement disposition (`0` settled, `1` held, `2` rejected).
    pub review_tier: i64,
    /// Settlement label (reserved for downstream stages).
    pub settlement: String,
}

/// Compute `fee` and `review_tier` for one order using `fee_bps`, `fee_fixed`,
/// and `hold_above` configuration keys.
#[transform(component = Order)]
pub fn settle(row: &mut Order, config: &Config) -> Result<()> {
    let fee_bps: f64 = config.get("fee_bps", 290.0)?;
    let fee_fixed: f64 = config.get("fee_fixed", 0.30)?;
    let hold_above: f64 = config.get("hold_above", 1000.0)?;

    if !row.valid {
        row.fee = 0.0;
        row.review_tier = TIER_REJECTED;
        metric("settle.rejected_rows", 1.0);
        return Ok(());
    }

    row.fee = ((row.amount * fee_bps / 10_000.0 + fee_fixed) * 100.0).round() / 100.0;
    if row.amount > hold_above {
        row.review_tier = TIER_HOLD;
        metric("settle.hold_rows", 1.0);
    } else {
        row.review_tier = TIER_SETTLED;
    }
    Ok(())
}

/// Export the Quick Start settlement pipeline as a `saci:pipeline@0.4.0` component.
#[processor(name = "quickstart-settle")]
pub fn build() -> Pipeline {
    Pipeline::new("quickstart-settle")
        .component::<Order>()
        .system(settle_system())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(id: i64, amount: f64, valid: bool) -> Order {
        Order {
            id,
            region: "amer".to_string(),
            currency: "USD".to_string(),
            amount,
            valid,
            usd_amount: 0.0,
            usd_amount_display: String::new(),
            risk_score: 0.0,
            flagged: false,
            fee: 99.0,
            review_tier: 99,
            settlement: String::new(),
        }
    }

    #[test]
    fn settles_holds_and_rejects_orders() {
        let mut data = Dataset::new();
        data.register::<Order>().expect("register Order");
        data.append(&[
            sample(1, 0.25, false),
            sample(2, 100.0, true),
            sample(3, 2000.0, true),
        ])
        .expect("append");

        pollster::block_on(build().run_on(&mut data)).expect("run");

        let rows = <Order as Component>::from_record_batch(
            data.batch::<Order>().expect("Order registered"),
        )
        .expect("decode");

        assert_eq!(rows[0].fee, 0.0);
        assert_eq!(rows[0].review_tier, TIER_REJECTED);

        assert_eq!(rows[1].fee, 3.20);
        assert_eq!(rows[1].review_tier, TIER_SETTLED);

        assert_eq!(rows[2].fee, 58.30);
        assert_eq!(rows[2].review_tier, TIER_HOLD);
    }

    #[test]
    fn order_schema_has_twelve_fields() {
        assert_eq!(<Order as Component>::schema().fields().len(), 12);
    }
}
