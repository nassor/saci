//! Stateful settlement processor compiled to `wasm32-wasip3`.
//!
//! Reads the order columns, maps `valid` and `review_tier` to a `settlement`
//! outcome string (`REJECTED`, `HOLD`, `REVIEW`, or `SETTLED`), and maintains
//! a cross-batch [`Ledger`] of settled volume net of fees.
//!
//! Writing a `Utf8` column (`settlement`) rewrites the offsets buffer, values
//! buffer, and RecordBatch metadata via `saci::processor` and `arrow-rs`.
//!
//! # Shape
//!
//! One row struct and two functions. `#[derive(Component)]` traces [`Order`]'s
//! Arrow schema from the type, `#[transform]` and `#[fold]` wrap the two
//! functions in systems, and `#[processor]` emits the WIT world, the guest
//! exports and the host-io bridges. Nothing here is target-gated: every
//! wasm32-only item lives inside the `#[processor]` expansion.
//!
//! # DAG shape
//!
//! Two systems in two stages, derived from declared access rather than ordered
//! by hand:
//!
//! ```text
//! stage 0:  settle  reads Order            writes Order
//! stage 1:  ledger  reads Order            writes ProcessorState<Ledger>
//! ```
//!
//! `ledger` reads what `settle` writes, so the scheduler is forced to sequence
//! them.
//!
//! # Build
//!
//! ```bash
//! cargo build --release -p settle-wasm --target wasm32-wasip3
//! ```
//!
//! The component lands in `target/wasm32-wasip3/release/settle_wasm.wasm`.

#![deny(missing_docs)]

use saci::processor::prelude::*;

/// Settlement outcome for an invalid row.
const REJECTED: &str = "REJECTED";
/// Settlement outcome for review tier 2 (flagged for manual hold).
const HOLD: &str = "HOLD";
/// Settlement outcome for review tier 1 (manual review).
const REVIEW: &str = "REVIEW";
/// Settlement outcome for a clean row; the only one the ledger counts.
const SETTLED: &str = "SETTLED";

/// Review tier assigned to a flagged row.
const TIER_HOLD: i64 = 2;
/// Review tier assigned to a row flagged for review.
const TIER_REVIEW: i64 = 1;
/// Review tier for a clean row.
const TIER_CLEAR: i64 = 0;

/// One order row matching the shared `Order` component schema.
///
/// Field order is load-bearing because it determines the schema fingerprint
/// and the fixed buffer layout.
///
/// | #  | field                  | Arrow type | written by       |
/// |----|------------------------|------------|------------------|
/// | 0  | `id`                   | `Int64`    | input only       |
/// | 1  | `region`               | `Utf8`     | input only       |
/// | 2  | `currency`             | `Utf8`     | input only       |
/// | 3  | `amount`               | `Float64`  | input only       |
/// | 4  | `valid`                | `Boolean`  | Go stage         |
/// | 5  | `usd_amount`           | `Float64`  | Python stage     |
/// | 6  | `usd_amount_display`   | `Utf8`     | Python stage     |
/// | 7  | `risk_score`           | `Float64`  | TypeScript stage |
/// | 8  | `flagged`              | `Boolean`  | TypeScript stage |
/// | 9  | `fee`                  | `Float64`  | Kotlin stage     |
/// | 10 | `review_tier`          | `Int64`    | C# stage         |
/// | 11 | `settlement`           | `Utf8`     | Rust stage       |
#[derive(Component, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct Order {
    /// Stable row identity. Input only.
    pub id: i64,
    /// Originating region (`emea` / `apac` / `amer`). Input only, read by the
    /// **Kotlin** stage to pick a fee rate.
    pub region: String,
    /// ISO currency code of `amount`. Input only.
    pub currency: String,
    /// Order amount in `currency`. Input only.
    pub amount: f64,
    /// `amount > min_amount`. Written by the **Go** stage.
    pub valid: bool,
    /// `amount` converted to USD, or `0.0` when invalid. Written by the
    /// **Python** stage.
    pub usd_amount: f64,
    /// `usd_amount` formatted for display. Written by the **Python** stage,
    /// which is the one non-Rust stage with a variable-length writer.
    pub usd_amount_display: String,
    /// `usd_amount / risk_threshold`. Written by the **TypeScript** stage.
    pub risk_score: f64,
    /// `risk_score >= 1.0`. Written by the **TypeScript** stage.
    pub flagged: bool,
    /// `usd_amount` times the region's rate, or `0.0` when invalid. Written by
    /// the **Kotlin** stage.
    pub fee: f64,
    /// `0` clear, `1` manual review, `2` escalated. Written by the **C#**
    /// stage.
    pub review_tier: i64,
    /// `REJECTED` / `HOLD` / `REVIEW` / `SETTLED`. Written by this stage.
    pub settlement: String,
}

/// Running total of settled orders, carried across `run-batch` calls.
///
/// This is the *state* component, not a batch component: it lives in a
/// `ProcessorState<Ledger>` resource, is never registered on the batch dataset,
/// and therefore never appears in the output IPC. `#[processor(state = Ledger)]`
/// serialises it into `run-result.checkpoint` after every batch and restores it
/// from `prior` before the next one. `Default` is the cold start: zero rows
/// settled, zero volume.
#[derive(Component, serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Ledger {
    /// Rows that reached `SETTLED`, summed over every batch so far.
    pub settled_count: i64,
    /// Net USD volume of those rows, `usd_amount - fee`, summed over every
    /// batch so far.
    pub settled_usd: f64,
}

/// Assign `settlement` from the validity flag and the review tier upstream
/// wrote.
///
/// `!valid → REJECTED`, tier 2 → `HOLD`, tier 1 → `REVIEW`, tier 0 →
/// `SETTLED`. Rejection wins over the tier: a row the Go stage rejected never
/// had its amount converted, so its `risk_score` and therefore its tier are
/// meaningless. Any other tier value is a codec fault upstream, not a row to
/// settle by default, so it fails the batch.
///
/// # Errors
///
/// Returns an error, failing the whole batch, when `review_tier` carries a
/// value no stage assigns.
#[transform(component = Order)]
pub fn settle(row: &mut Order) -> Result<()> {
    row.settlement = if !row.valid {
        REJECTED.to_string()
    } else {
        match row.review_tier {
            TIER_HOLD => HOLD.to_string(),
            TIER_REVIEW => REVIEW.to_string(),
            TIER_CLEAR => SETTLED.to_string(),
            other => {
                return Err(
                    format!("settle: row {} carries unknown review_tier {other}", row.id).into(),
                );
            }
        }
    };
    Ok(())
}

/// Accumulate settled volume, net of fees, into the cross-batch [`Ledger`] and
/// report the batch's settlement mix.
///
/// Writes no column. It reads `Order`, which [`settle`] writes, and that places
/// it in a second stage: the sequencing comes from declared access, not from an
/// ordering call.
///
/// The `settle.*_rows` counts are reported here rather than from [`settle`]
/// because they are batch totals and a `#[transform]` sees one row at a time.
/// This is the stage's one batch-level hook, so it is where the numbers exist.
///
/// # Errors
///
/// Infallible today; the signature keeps the `#[fold]` contract.
#[fold(reads = Order, state = Ledger)]
pub fn ledger(rows: &[Order], state: &mut Ledger) -> Result<()> {
    let mut held = 0usize;
    let mut reviewed = 0usize;
    let mut rejected = 0usize;
    let mut batch_count = 0i64;
    let mut batch_usd = 0.0f64;

    for row in rows {
        match row.settlement.as_str() {
            HOLD => held += 1,
            REVIEW => reviewed += 1,
            REJECTED => rejected += 1,
            SETTLED => {
                batch_count += 1;
                batch_usd += row.usd_amount - row.fee;
            }
            _ => {}
        }
    }

    state.settled_count += batch_count;
    state.settled_usd += batch_usd;

    metric("settle.held_rows", held as f64);
    metric("settle.review_rows", reviewed as f64);
    metric("settle.rejected_rows", rejected as f64);
    log(
        "settle",
        &format!(
            "{held} held, {reviewed} in review, {rejected} rejected out of {}",
            rows.len()
        ),
    );

    metric("settle.settled_usd_total", state.settled_usd);
    metric("settle.settled_count_total", state.settled_count as f64);
    log(
        "ledger",
        &format!(
            "batch settled {batch_count} rows / {batch_usd:.2} USD net; \
             lifetime {} rows / {:.2} USD net",
            state.settled_count, state.settled_usd
        ),
    );
    Ok(())
}

/// Build the settle pipeline.
///
/// Registers only `Order`. `Ledger` is state and must not be registered on the
/// batch dataset: `Dataset`'s IPC format requires every registered component to
/// hold exactly the dataset's row count, while ledger rows are independent of
/// batch rows.
#[processor(name = "settle", state = Ledger)]
pub fn build() -> Pipeline {
    Pipeline::new("settle")
        .component::<Order>()
        .system(settle_system())
        .system(ledger_system())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pollster::block_on;
    use saci::processor::arrow_schema::{DataType, Field, Schema};

    fn order(id: i64, valid: bool, review_tier: i64) -> Order {
        Order {
            id,
            region: "emea".to_string(),
            currency: "EUR".to_string(),
            amount: 100.0,
            valid,
            usd_amount: 110.0,
            usd_amount_display: "110.00".to_string(),
            risk_score: 0.5,
            flagged: false,
            fee: 10.0,
            review_tier,
            settlement: String::new(),
        }
    }

    #[test]
    fn the_order_schema_is_the_twelve_column_cross_language_contract() {
        let expected = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("valid", DataType::Boolean, false),
            Field::new("usd_amount", DataType::Float64, false),
            Field::new("usd_amount_display", DataType::Utf8, false),
            Field::new("risk_score", DataType::Float64, false),
            Field::new("flagged", DataType::Boolean, false),
            Field::new("fee", DataType::Float64, false),
            Field::new("review_tier", DataType::Int64, false),
            Field::new("settlement", DataType::Utf8, false),
        ]);
        assert_eq!(*<Order as Component>::schema(), expected);
    }

    /// The value every stage's `describe()` reports, and the one the host gates
    /// checkpoint compatibility on. It is the FNV-1a of the component name, the
    /// `u32` version little-endian, and each field name in declaration order,
    /// so any reordering, rename or added column moves it.
    #[test]
    fn the_schema_fingerprint_is_the_agreed_cross_language_value() {
        let mut registry = SchemaRegistry::new();
        registry.register::<Order>();
        // The same 8-char lowercase hex `describe()` puts in
        // `pipeline-descriptor.schema_fingerprint`.
        assert_eq!(format!("{:08x}", registry.fingerprint()), "f6405a7b");
    }

    #[test]
    fn the_ledger_schema_matches_the_persisted_checkpoint_layout() {
        let expected = Schema::new(vec![
            Field::new("settled_count", DataType::Int64, false),
            Field::new("settled_usd", DataType::Float64, false),
        ]);
        assert_eq!(*<Ledger as Component>::schema(), expected);
        assert_eq!(<Ledger as Component>::name(), "Ledger");
    }

    #[test]
    fn the_settlement_cascade_puts_rejection_ahead_of_the_tier() {
        // An invalid row never had its amount converted, so its tier is
        // meaningless even when it says "escalate".
        let mut row = order(1, false, TIER_HOLD);
        settle(&mut row).expect("settle");
        assert_eq!(row.settlement, REJECTED);
    }

    #[test]
    fn each_review_tier_maps_to_its_outcome() {
        for (tier, expected) in [
            (TIER_HOLD, HOLD),
            (TIER_REVIEW, REVIEW),
            (TIER_CLEAR, SETTLED),
        ] {
            let mut row = order(1, true, tier);
            settle(&mut row).expect("settle");
            assert_eq!(row.settlement, expected, "tier {tier}");
        }
    }

    #[test]
    fn an_unknown_review_tier_fails_the_batch() {
        let mut row = order(7, true, 9);
        let err = settle(&mut row).expect_err("tier 9 is a codec fault upstream");
        assert!(err.message().contains("unknown review_tier 9"), "{err:?}");
        assert!(err.message().contains("row 7"), "{err:?}");
    }

    #[test]
    fn the_ledger_counts_only_settled_rows_net_of_fees() {
        let rows = vec![
            Order {
                settlement: SETTLED.to_string(),
                usd_amount: 110.0,
                fee: 10.0,
                ..order(1, true, TIER_CLEAR)
            },
            Order {
                settlement: SETTLED.to_string(),
                usd_amount: 40.0,
                fee: 5.0,
                ..order(2, true, TIER_CLEAR)
            },
            Order {
                settlement: HOLD.to_string(),
                usd_amount: 999.0,
                fee: 1.0,
                ..order(3, true, TIER_HOLD)
            },
            Order {
                settlement: REJECTED.to_string(),
                ..order(4, false, TIER_CLEAR)
            },
        ];

        let mut state = Ledger::default();
        ledger(&rows, &mut state).expect("ledger");
        assert_eq!(state.settled_count, 2);
        assert_eq!(state.settled_usd, 135.0);

        // A second batch accumulates rather than replacing.
        ledger(&rows, &mut state).expect("ledger");
        assert_eq!(state.settled_count, 4);
        assert_eq!(state.settled_usd, 270.0);
    }

    #[test]
    fn the_pipeline_registers_only_order_and_sequences_the_two_systems() {
        let pipeline = build();
        assert_eq!(pipeline.name(), "settle");
        assert!(pipeline.data().schemas().contains("Order"));
        assert!(
            !pipeline.data().schemas().contains("Ledger"),
            "state must not be a batch component"
        );
    }

    /// The stage's whole host-side path: the real `build()` pipeline, both
    /// generated systems, an Arrow batch in and out, and the state resource the
    /// `#[processor]` expansion restores on the wasm side.
    #[test]
    fn the_pipeline_settles_a_batch_and_folds_the_ledger() {
        let mut data = Dataset::new();
        data.register::<Order>().expect("register Order");
        data.append(&[
            Order {
                usd_amount: 110.0,
                fee: 10.0,
                ..order(1, true, TIER_CLEAR)
            },
            Order {
                usd_amount: 40.0,
                fee: 5.0,
                ..order(2, true, TIER_CLEAR)
            },
            order(3, true, TIER_HOLD),
            order(4, true, TIER_REVIEW),
            order(5, false, TIER_CLEAR),
        ])
        .expect("append rows");
        data.insert_resource(saci::processor::ProcessorState::<Ledger>::default());

        block_on(build().run_on(&mut data)).expect("run the pipeline");

        let settled = <Order as Component>::from_record_batch(
            data.batch::<Order>().expect("Order registered"),
        )
        .expect("decode rows");
        let outcomes: Vec<&str> = settled.iter().map(|r| r.settlement.as_str()).collect();
        assert_eq!(outcomes, [SETTLED, SETTLED, HOLD, REVIEW, REJECTED]);

        // `ledger` runs in a later stage than `settle`, so it saw the written
        // column rather than the empty one the batch arrived with.
        let state = data
            .get_resource::<saci::processor::ProcessorState<Ledger>>()
            .expect("state resource");
        assert_eq!(
            state.rows,
            vec![Ledger {
                settled_count: 2,
                settled_usd: 135.0,
            }]
        );
    }
}
