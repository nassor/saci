//! Order classification as a WebAssembly processor.
//!
//! The first stage of `examples/integrity/`: the `ingest` workflow feeds this
//! component two streams, `Order` rows off Kafka and `Catalog` rows off a
//! file. Catalog rows never leave the component; they accumulate in its
//! checkpoint state so a catalog batch read once enriches every later order
//! batch. Order rows come out as `ClassifiedOrder`, each carrying the joined
//! tax flag, the derived line total, the branch the batch routes to and an
//! FNV-1a checksum over all of it, which the example's verifier recomputes
//! independently.
//!
//! Routing is per batch, which is why the publisher packs one priority per
//! Kafka message. The [`RouteDecision`] names two branches: the priority
//! branch (`express` or `standard`, one HTTP sink each) and the constant
//! `all` branch feeding the channel bridge into the `settle` workflow.
//! `WorkflowSpec::validate` requires every outbound link of a node to be
//! labelled or none of them to be, so the bridge link carries a label too.
//!
//! # Build
//!
//! ```bash
//! cargo build --release -p integrity-classify-wasm --target wasm32-wasip2
//! ```
//!
//! The output component lands at
//! `target/wasm32-wasip2/release/integrity_classify_wasm.wasm`.

#![deny(missing_docs)]

// The bindings are generated in place from `crates/saci-processor/wit`. The
// module and the `export_pipeline!` invocation below are gated on
// `target_arch = "wasm32"`: the expansion emits canonical ABI intrinsics and the
// `component-type` custom section, neither of which the host target can link.
#[cfg(target_arch = "wasm32")]
#[allow(warnings)]
mod bindings {
    wit_bindgen::generate!({
        path: "../../../../crates/saci-processor/wit",
        world: "saci-pipeline",
        generate_all,
    });
}

use std::sync::Arc;

use saci_processor::arrow_array::{
    BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use saci_processor::arrow_schema::{DataType, Field, FieldRef, Schema};
use saci_processor::prelude::*;
use saci_processor::{ProcessorState, RouteDecision};

/// Branch every non-express batch routes to.
pub const BRANCH_STANDARD: &str = "standard";
/// Branch an `express` batch routes to.
pub const BRANCH_EXPRESS: &str = "express";
/// Branch every order batch routes to in addition to its priority branch, so
/// the channel bridge into `settle` sees the whole stream.
pub const BRANCH_ALL: &str = "all";

/// 64-bit FNV-1a over `bytes`, reinterpreted as `i64`.
///
/// Duplicated in each of this example's three processor crates on purpose: a
/// wasm processor component is self-contained, and a shared crate would be a
/// fourth workspace member for twelve lines.
pub fn fnv1a64(bytes: &[u8]) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash as i64
}

/// Round to two decimals the way the verifier does, so a money value survives
/// the round trip through its own text form bit for bit.
fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

/// One order as the Kafka source decodes it.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Order {
    /// Caller-assigned identity, unique across the run.
    pub order_id: i64,
    /// Catalog key this order is joined on.
    pub sku: String,
    /// Units ordered.
    pub qty: i32,
    /// Price of one unit, before tax.
    pub unit_price: f64,
    /// Grouping key the `settle` workflow windows on.
    pub region: String,
    /// `"express"` or anything else; selects the branch for the whole batch.
    pub priority: String,
    /// Event time in milliseconds since the epoch.
    pub event_ms: i64,
}

impl Component for Order {
    fn name() -> &'static str {
        "Order"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("sku", DataType::Utf8, false),
            Field::new("qty", DataType::Int32, false),
            Field::new("unit_price", DataType::Float64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("priority", DataType::Utf8, false),
            Field::new("event_ms", DataType::Int64, false),
        ]))
    }
}

/// One catalog entry as the file source decodes it. Deliberately carries no
/// timestamp: it is reference data, not a stream, which is what makes the
/// `ingest` workflow's fan-in heterogeneous.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Catalog {
    /// Join key.
    pub sku: String,
    /// Human-readable product name.
    pub name: String,
    /// Whether an order line for this sku is taxed.
    pub taxable: bool,
    /// Shipping weight; carried for realism, unused by the join.
    pub weight_kg: f64,
}

impl Component for Catalog {
    fn name() -> &'static str {
        "Catalog"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("sku", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("taxable", DataType::Boolean, false),
            Field::new("weight_kg", DataType::Float64, false),
        ]))
    }
}

/// One classified order: the input row plus everything derived from it.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct ClassifiedOrder {
    /// Carried through from [`Order`].
    pub order_id: i64,
    /// Carried through from [`Order`].
    pub sku: String,
    /// Carried through from [`Order`].
    pub region: String,
    /// Carried through from [`Order`].
    pub priority: String,
    /// Carried through from [`Order`].
    pub qty: i32,
    /// Carried through from [`Order`].
    pub unit_price: f64,
    /// `qty * unit_price`, times 1.20 when the sku is taxable, rounded to two
    /// decimals.
    pub line_total: f64,
    /// The catalog's tax flag for this sku; `false` when the sku has not been
    /// seen in a catalog batch yet.
    pub taxable: bool,
    /// The branch this batch routed to: [`BRANCH_EXPRESS`] or
    /// [`BRANCH_STANDARD`].
    pub branch: String,
    /// FNV-1a over the row's canonical text form.
    pub checksum: i64,
    /// Carried through from [`Order`].
    pub event_ms: i64,
}

impl Component for ClassifiedOrder {
    fn name() -> &'static str {
        "ClassifiedOrder"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("sku", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("priority", DataType::Utf8, false),
            Field::new("qty", DataType::Int32, false),
            Field::new("unit_price", DataType::Float64, false),
            Field::new("line_total", DataType::Float64, false),
            Field::new("taxable", DataType::Boolean, false),
            Field::new("branch", DataType::Utf8, false),
            Field::new("checksum", DataType::Int64, false),
            Field::new("event_ms", DataType::Int64, false),
        ]))
    }
}

/// One remembered catalog row.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct CatalogEntry {
    /// Join key.
    pub sku: String,
    /// The tax flag orders for this sku inherit.
    pub taxable: bool,
}

/// The processor's cross-batch state: the catalog accumulated so far.
///
/// A `Vec` rather than a map, because the checkpoint is Arrow rows and the
/// demo catalog is a few dozen skus: a linear scan per order row costs less
/// than the map would cost to serialise.
#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct CatalogState {
    /// One entry per sku seen, in first-seen order.
    pub entries: Vec<CatalogEntry>,
}

impl Component for CatalogState {
    fn name() -> &'static str {
        "CatalogState"
    }
    fn schema() -> Arc<Schema> {
        // Traced from a sample so the nested `entries: Vec<CatalogEntry>`
        // column matches serde_arrow's encoding exactly; a hand-written nested
        // schema would drift from it and fail the state checkpoint round trip.
        use serde_arrow::schema::{SchemaLike as _, TracingOptions};
        let sample = CatalogState {
            entries: vec![CatalogEntry {
                sku: "sample".to_string(),
                taxable: true,
            }],
        };
        let fields = Vec::<FieldRef>::from_samples(&[sample], TracingOptions::default())
            .expect("CatalogState schema traces from a sample");
        Arc::new(Schema::new(fields))
    }
}

/// Report a metric through `host-io::metric`; a no-op outside wasm, where the
/// host bindings do not exist.
#[cfg(target_arch = "wasm32")]
fn report_metric(name: &str, value: f64) {
    crate::bindings::saci::pipeline::host_io::metric(name, value);
}

#[cfg(not(target_arch = "wasm32"))]
fn report_metric(_name: &str, _value: f64) {}

/// The canonical text a [`ClassifiedOrder`]'s checksum is taken over.
///
/// The verifier rebuilds this string from the row it received and hashes it
/// again, so every format specifier here is part of the example's contract.
pub fn classified_checksum_input(row: &ClassifiedOrder) -> String {
    format!(
        "{}|{}|{}|{}|{}|{:.4}|{:.2}|{}|{}|{}",
        row.order_id,
        row.sku,
        row.region,
        row.priority,
        row.qty,
        row.unit_price,
        row.line_total,
        row.taxable,
        row.branch,
        row.event_ms
    )
}

/// Fold the batch's catalog rows into the state, upserting by sku.
fn absorb_catalog(batch: &RecordBatch, state: &mut CatalogState) -> Result<(), SaciError> {
    let n = batch.num_rows();
    if n == 0 {
        return Ok(());
    }
    let sku = column::<StringArray>(batch, "sku")?;
    let taxable = column::<BooleanArray>(batch, "taxable")?;
    for i in 0..n {
        let key = sku.value(i);
        let flag = taxable.value(i);
        match state.entries.iter_mut().find(|e| e.sku == key) {
            Some(entry) => entry.taxable = flag,
            None => state.entries.push(CatalogEntry {
                sku: key.to_string(),
                taxable: flag,
            }),
        }
    }
    Ok(())
}

/// Downcast the named column, naming the component and the column on failure.
fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T, SaciError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| SaciError::generic(format!("classify: column '{name}' missing: {e}")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| SaciError::generic(format!("classify: column '{name}' has the wrong type")))
}

/// Join the batch's orders against the accumulated catalog, emit one
/// [`ClassifiedOrder`] per order row, and name the branches the batch routes
/// to.
///
/// A batch carrying only catalog rows produces no output and an empty
/// [`RouteDecision`], so nothing is delivered downstream for it.
pub fn classify_impl(data: &mut Dataset) -> Result<(), SaciError> {
    let catalog = data
        .columns::<Catalog>()
        .ok_or_else(|| SaciError::generic("classify: Catalog component missing"))?
        .clone();
    let orders = data
        .columns::<Order>()
        .ok_or_else(|| SaciError::generic("classify: Order component missing"))?
        .clone();

    let state = data
        .get_resource_mut::<ProcessorState<CatalogState>>()
        .ok_or_else(|| {
            SaciError::generic("classify: ProcessorState<CatalogState> resource missing")
        })?;
    // The SDK serialises the resource as rows of the state component; exactly
    // one row carries the whole catalog.
    if state.rows.is_empty() {
        state.rows.push(CatalogState::default());
    }
    let catalog_state = &mut state.rows[0];
    absorb_catalog(&catalog, catalog_state)?;

    let n = orders.num_rows();
    if n == 0 {
        let known = catalog_state.entries.len();
        // An empty decision routes nowhere: a catalog-only batch must not
        // reach a sink, and must not warn about a branch no link carries.
        data.insert_resource(RouteDecision(Vec::new()));
        report_metric("classify.catalog_skus", known as f64);
        return Ok(());
    }

    let order_id = column::<Int64Array>(&orders, "order_id")?;
    let sku = column::<StringArray>(&orders, "sku")?;
    let qty = column::<Int32Array>(&orders, "qty")?;
    let unit_price = column::<Float64Array>(&orders, "unit_price")?;
    let region = column::<StringArray>(&orders, "region")?;
    let priority = column::<StringArray>(&orders, "priority")?;
    let event_ms = column::<Int64Array>(&orders, "event_ms")?;

    let mut rows: Vec<ClassifiedOrder> = Vec::with_capacity(n);
    let mut unknown_skus = 0u64;
    // Routing is per batch, so `branch` is one decision for the whole batch,
    // taken from the first row. Deriving it per row would let a batch that
    // mixed priorities carry a `branch` naming a sink it was never delivered
    // to; the KDL keeps one Kafka message equal to one batch precisely so
    // that cannot happen, and this makes the column agree with the
    // `RouteDecision` even if it ever did.
    let batch_branch = if priority.value(0) == BRANCH_EXPRESS {
        BRANCH_EXPRESS
    } else {
        BRANCH_STANDARD
    };
    for i in 0..n {
        let key = sku.value(i);
        let taxable = match catalog_state.entries.iter().find(|e| e.sku == key) {
            Some(entry) => entry.taxable,
            None => {
                unknown_skus += 1;
                false
            }
        };
        let gross = f64::from(qty.value(i)) * unit_price.value(i);
        let line_total = round2(if taxable { gross * 1.20 } else { gross });
        let mut row = ClassifiedOrder {
            order_id: order_id.value(i),
            sku: key.to_string(),
            region: region.value(i).to_string(),
            priority: priority.value(i).to_string(),
            qty: qty.value(i),
            unit_price: unit_price.value(i),
            line_total,
            taxable,
            branch: batch_branch.to_string(),
            checksum: 0,
            event_ms: event_ms.value(i),
        };
        row.checksum = fnv1a64(classified_checksum_input(&row).as_bytes());
        rows.push(row);
    }

    // The `all` branch is the bridge into `settle` and fires for every order
    // batch, alongside the priority branch that selects one HTTP sink.
    let known = catalog_state.entries.len();
    // The borrow through `state` ends here (NLL), before `data.append` below.

    data.append::<ClassifiedOrder>(&rows)?;
    data.insert_resource(RouteDecision(vec![
        batch_branch.to_string(),
        BRANCH_ALL.to_string(),
    ]));
    report_metric("classify.catalog_skus", known as f64);
    report_metric("classify.unknown_skus", unknown_skus as f64);
    Ok(())
}

/// Build the classification pipeline.
///
/// Called lazily by the `export_pipeline!` macro on the first call to any WIT
/// export, and constructed exactly once per component instance.
pub fn build() -> Pipeline {
    let mut pipeline = Pipeline::new("integrity-classify");
    pipeline
        .data
        .register_component::<Catalog>()
        .expect("register Catalog");
    pipeline
        .data
        .register_component::<Order>()
        .expect("register Order");
    pipeline
        .data
        .register_component::<ClassifiedOrder>()
        .expect("register ClassifiedOrder");
    pipeline.add_system(system_fn(
        SystemMeta::new("classify")
            .read_component("Order")
            .read_component("Catalog")
            .write_component("ClassifiedOrder"),
        classify_impl,
    ));
    pipeline
}

#[cfg(target_arch = "wasm32")]
saci_processor::export_pipeline!(build, state = CatalogState);

#[cfg(test)]
mod tests {
    use super::*;
    use saci_processor::__rt::{ProcessorStateSpec, Stateful};

    fn dataset_with_state() -> Dataset {
        let mut data = Dataset::new();
        data.register_component::<Catalog>().unwrap();
        data.register_component::<Order>().unwrap();
        data.register_component::<ClassifiedOrder>().unwrap();
        <Stateful<CatalogState> as ProcessorStateSpec>::restore(&mut data, None).unwrap();
        data
    }

    fn catalog(sku: &str, taxable: bool) -> Catalog {
        Catalog {
            sku: sku.to_string(),
            name: format!("{sku} widget"),
            taxable,
            weight_kg: 1.5,
        }
    }

    fn order(order_id: i64, sku: &str, qty: i32, unit_price: f64, priority: &str) -> Order {
        Order {
            order_id,
            sku: sku.to_string(),
            qty,
            unit_price,
            region: "eu".to_string(),
            priority: priority.to_string(),
            event_ms: 1_700_000_000_000,
        }
    }

    /// The emitted rows, read back off the dataset the way a sink's
    /// transformer would.
    fn emitted(data: &Dataset) -> Vec<(i64, f64, bool, String, i64)> {
        let batch = data
            .batch_for("ClassifiedOrder")
            .expect("ClassifiedOrder batch");
        let id = column::<Int64Array>(batch, "order_id").unwrap();
        let total = column::<Float64Array>(batch, "line_total").unwrap();
        let taxable = column::<BooleanArray>(batch, "taxable").unwrap();
        let branch = column::<StringArray>(batch, "branch").unwrap();
        let checksum = column::<Int64Array>(batch, "checksum").unwrap();
        (0..batch.num_rows())
            .map(|i| {
                (
                    id.value(i),
                    total.value(i),
                    taxable.value(i),
                    branch.value(i).to_string(),
                    checksum.value(i),
                )
            })
            .collect()
    }

    fn routes(data: &Dataset) -> Option<Vec<String>> {
        data.get_resource::<RouteDecision>().map(|r| r.0.clone())
    }

    /// One workflow item, as the stream runner drives it: a fresh dataset, the
    /// previous item's checkpoint restored, one batch, then the next
    /// checkpoint.
    fn run_batch(
        prior: Option<&[u8]>,
        catalog_rows: &[Catalog],
        order_rows: &[Order],
    ) -> (Dataset, Option<Vec<u8>>) {
        let mut data = dataset_with_state();
        <Stateful<CatalogState> as ProcessorStateSpec>::restore(&mut data, prior).unwrap();
        data.append::<Catalog>(catalog_rows).unwrap();
        data.append::<Order>(order_rows).unwrap();
        classify_impl(&mut data).unwrap();
        let next = <Stateful<CatalogState> as ProcessorStateSpec>::capture(&data).unwrap();
        (data, next)
    }

    #[test]
    fn a_catalog_only_batch_emits_nothing_and_routes_nowhere() {
        let (data, blob) = run_batch(None, &[catalog("sku-1", true)], &[]);
        assert!(emitted(&data).is_empty());
        assert_eq!(
            routes(&data),
            Some(Vec::new()),
            "an empty decision keeps a catalog batch out of every sink"
        );
        assert!(blob.is_some(), "the catalog survives in the checkpoint");
    }

    #[test]
    fn a_catalog_batch_taxes_orders_in_later_batches() {
        let (_, blob) = run_batch(None, &[catalog("sku-1", true)], &[]);
        let (data, _) = run_batch(
            blob.as_deref(),
            &[],
            &[order(1, "sku-1", 3, 10.0, "standard")],
        );
        let rows = emitted(&data);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, 36.0, "3 * 10.00 * 1.20");
        assert!(rows[0].2, "the catalog row survived the checkpoint");
    }

    #[test]
    fn an_unknown_sku_is_untaxed() {
        let (data, _) = run_batch(None, &[], &[order(1, "sku-missing", 3, 10.0, "standard")]);
        let rows = emitted(&data);
        assert_eq!(rows[0].1, 30.0, "no catalog row means no tax");
        assert!(!rows[0].2);
    }

    #[test]
    fn a_later_catalog_batch_overrides_an_earlier_tax_flag() {
        let (_, blob) = run_batch(None, &[catalog("sku-1", true)], &[]);
        let (_, blob) = run_batch(blob.as_deref(), &[catalog("sku-1", false)], &[]);
        let (data, _) = run_batch(
            blob.as_deref(),
            &[],
            &[order(1, "sku-1", 2, 5.0, "standard")],
        );
        assert_eq!(emitted(&data)[0].1, 10.0);
        assert!(!emitted(&data)[0].2);
    }

    #[test]
    fn line_total_rounds_to_two_decimals() {
        let (_, blob) = run_batch(None, &[catalog("sku-1", true)], &[]);
        // 1 * 0.07 * 1.20 = 0.084 in exact arithmetic, 0.08 after rounding.
        let (data, _) = run_batch(
            blob.as_deref(),
            &[],
            &[order(1, "sku-1", 1, 0.07, "standard")],
        );
        assert_eq!(emitted(&data)[0].1, 0.08);
    }

    #[test]
    fn an_express_batch_routes_to_express_and_all() {
        let (data, _) = run_batch(None, &[], &[order(1, "sku-1", 1, 1.0, "express")]);
        assert_eq!(
            routes(&data),
            Some(vec!["express".to_string(), "all".to_string()]),
            "the priority branch plus the bridge branch"
        );
        assert_eq!(emitted(&data)[0].3, "express");
    }

    #[test]
    fn every_row_carries_the_branch_the_batch_routed_to() {
        // The KDL keeps one Kafka message equal to one batch, so a mixed
        // batch should not occur; if it ever does, `branch` must still name
        // where the rows actually went rather than where each row's own
        // priority would have sent it.
        let (data, _) = run_batch(
            None,
            &[],
            &[
                order(1, "sku-1", 1, 1.0, "express"),
                order(2, "sku-1", 1, 1.0, "standard"),
            ],
        );
        let branches: Vec<String> = emitted(&data).into_iter().map(|row| row.3).collect();
        assert_eq!(branches, vec!["express".to_string(), "express".to_string()]);
        assert_eq!(
            routes(&data),
            Some(vec!["express".to_string(), "all".to_string()])
        );
    }

    #[test]
    fn an_unrecognised_priority_routes_to_standard() {
        let (data, _) = run_batch(None, &[], &[order(1, "sku-1", 1, 1.0, "whenever")]);
        assert_eq!(
            routes(&data),
            Some(vec!["standard".to_string(), "all".to_string()])
        );
        assert_eq!(emitted(&data)[0].3, "standard");
    }

    #[test]
    fn the_checksum_covers_every_derived_field() {
        let (data, _) = run_batch(None, &[], &[order(7, "sku-1", 2, 3.5, "express")]);
        let rows = emitted(&data);
        let expected = fnv1a64(
            classified_checksum_input(&ClassifiedOrder {
                order_id: 7,
                sku: "sku-1".to_string(),
                region: "eu".to_string(),
                priority: "express".to_string(),
                qty: 2,
                unit_price: 3.5,
                line_total: 7.0,
                taxable: false,
                branch: "express".to_string(),
                checksum: 0,
                event_ms: 1_700_000_000_000,
            })
            .as_bytes(),
        );
        assert_eq!(rows[0].4, expected);
    }

    #[test]
    fn fnv1a64_matches_the_reference_vector() {
        // The canonical FNV-1a 64 test vector: "a" hashes to 0xaf63dc4c8601ec8c.
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8cu64 as i64);
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325u64 as i64);
    }
}
