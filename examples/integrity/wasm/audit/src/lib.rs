//! CDC audit verification as a WebAssembly processor.
//!
//! The third stage of `examples/integrity/`, and the only one that is
//! stateless: the `audit` workflow reads `public.order_audit` through
//! PostgreSQL logical replication and this component turns each change into an
//! `AuditVerdict`.
//!
//! The input carries one reserved column, `__op`, which `cdc_logical` fills
//! from the change stream rather than from the tuple (`"I"`, `"U"` or `"D"`).
//! The verdict renames it to `op`, so the CDC metadata reaches the sink under
//! a name a consumer can read, and folds it into both the `ok` predicate and
//! the checksum.
//!
//! # Build
//!
//! ```bash
//! cargo build --release -p integrity-audit-wasm --target wasm32-wasip2
//! ```
//!
//! The output component lands at
//! `target/wasm32-wasip2/release/integrity_audit_wasm.wasm`.

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

use saci_processor::arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
use saci_processor::arrow_schema::{DataType, Field, Schema};
use saci_processor::prelude::*;

/// The statuses an audit row may legitimately carry, in workflow order.
pub const VALID_STATUSES: [&str; 4] = ["placed", "picked", "shipped", "settled"];
/// The change-stream operations a verdict accepts. A `"D"` row is reported
/// with `ok = false` rather than dropped, so a delete is still visible at the
/// sink.
pub const VALID_OPS: [&str; 2] = ["I", "U"];

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

/// One change to `public.order_audit`, as `cdc_logical` decodes it.
///
/// `__op` is a reserved field name the connector fills from the `pgoutput`
/// message instead of from the tuple; the remaining columns are the table's
/// own. They stay non-nullable only because the table is
/// `REPLICA IDENTITY FULL`, which is what makes a `"D"` change carry every
/// column rather than the key alone.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct AuditRow {
    /// `"I"`, `"U"` or `"D"`.
    #[serde(rename = "__op")]
    pub op: String,
    /// Primary key of the audit table.
    pub audit_id: i64,
    /// The order this change describes.
    pub order_id: i64,
    /// Order status after the change.
    pub status: String,
    /// Monotonic revision within one order.
    pub revision: i32,
    /// When the change happened, milliseconds since the epoch.
    pub changed_ms: i64,
}

impl Component for AuditRow {
    fn name() -> &'static str {
        "AuditRow"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("__op", DataType::Utf8, false),
            Field::new("audit_id", DataType::Int64, false),
            Field::new("order_id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("revision", DataType::Int32, false),
            Field::new("changed_ms", DataType::Int64, false),
        ]))
    }
}

/// One verdict on an audit change.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct AuditVerdict {
    /// Carried through from [`AuditRow`].
    pub audit_id: i64,
    /// Carried through from [`AuditRow`].
    pub order_id: i64,
    /// [`AuditRow::op`], renamed out of the reserved namespace.
    pub op: String,
    /// Carried through from [`AuditRow`].
    pub status: String,
    /// Carried through from [`AuditRow`].
    pub revision: i32,
    /// Carried through from [`AuditRow`].
    pub changed_ms: i64,
    /// Whether the change is one this pipeline considers well formed.
    pub ok: bool,
    /// FNV-1a over the row's canonical text form.
    pub checksum: i64,
}

impl Component for AuditVerdict {
    fn name() -> &'static str {
        "AuditVerdict"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("audit_id", DataType::Int64, false),
            Field::new("order_id", DataType::Int64, false),
            Field::new("op", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("revision", DataType::Int32, false),
            Field::new("changed_ms", DataType::Int64, false),
            Field::new("ok", DataType::Boolean, false),
            Field::new("checksum", DataType::Int64, false),
        ]))
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

/// Whether a change is well formed: a known status, a positive revision, and
/// an operation that writes a row.
pub fn verdict_ok(op: &str, status: &str, revision: i32) -> bool {
    VALID_OPS.contains(&op) && VALID_STATUSES.contains(&status) && revision >= 1
}

/// The canonical text an [`AuditVerdict`]'s checksum is taken over.
///
/// The verifier rebuilds this string from the row it received and hashes it
/// again, so every separator here is part of the example's contract.
pub fn verdict_checksum_input(row: &AuditVerdict) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        row.audit_id, row.order_id, row.op, row.status, row.revision, row.changed_ms, row.ok
    )
}

/// Downcast the named column, naming the column on failure.
fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T, SaciError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| SaciError::generic(format!("audit: column '{name}' missing: {e}")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| SaciError::generic(format!("audit: column '{name}' has the wrong type")))
}

/// Turn every audit change in the batch into a verdict.
pub fn verify_impl(data: &mut Dataset) -> Result<(), SaciError> {
    let rows = data
        .columns::<AuditRow>()
        .ok_or_else(|| SaciError::generic("audit: AuditRow component missing"))?
        .clone();
    let n = rows.num_rows();
    if n == 0 {
        return Ok(());
    }

    let op = column::<StringArray>(&rows, "__op")?;
    let audit_id = column::<Int64Array>(&rows, "audit_id")?;
    let order_id = column::<Int64Array>(&rows, "order_id")?;
    let status = column::<StringArray>(&rows, "status")?;
    let revision = column::<Int32Array>(&rows, "revision")?;
    let changed_ms = column::<Int64Array>(&rows, "changed_ms")?;

    let mut verdicts: Vec<AuditVerdict> = Vec::with_capacity(n);
    let mut rejected = 0u64;
    for i in 0..n {
        let ok = verdict_ok(op.value(i), status.value(i), revision.value(i));
        if !ok {
            rejected += 1;
        }
        let mut verdict = AuditVerdict {
            audit_id: audit_id.value(i),
            order_id: order_id.value(i),
            op: op.value(i).to_string(),
            status: status.value(i).to_string(),
            revision: revision.value(i),
            changed_ms: changed_ms.value(i),
            ok,
            checksum: 0,
        };
        verdict.checksum = fnv1a64(verdict_checksum_input(&verdict).as_bytes());
        verdicts.push(verdict);
    }

    data.append::<AuditVerdict>(&verdicts)?;
    report_metric("audit.rejected", rejected as f64);
    Ok(())
}

/// Build the audit verification pipeline.
///
/// Called lazily by the `export_pipeline!` macro on the first call to any WIT
/// export, and constructed exactly once per component instance.
pub fn build() -> Pipeline {
    let mut pipeline = Pipeline::new("integrity-audit");
    pipeline
        .data
        .register_component::<AuditRow>()
        .expect("register AuditRow");
    pipeline
        .data
        .register_component::<AuditVerdict>()
        .expect("register AuditVerdict");
    pipeline.add_system(system_fn(
        SystemMeta::new("verify_audit")
            .read_component("AuditRow")
            .write_component("AuditVerdict"),
        verify_impl,
    ));
    pipeline
}

#[cfg(target_arch = "wasm32")]
saci_processor::export_pipeline!(build);

#[cfg(test)]
mod tests {
    use super::*;
    use saci_processor::arrow_array::BooleanArray;

    fn row(op: &str, audit_id: i64, status: &str, revision: i32) -> AuditRow {
        AuditRow {
            op: op.to_string(),
            audit_id,
            order_id: 100 + audit_id,
            status: status.to_string(),
            revision,
            changed_ms: 1_700_000_000_000 + audit_id,
        }
    }

    /// One emitted verdict: audit id, op, ok, checksum.
    type Verdict = (i64, String, bool, i64);

    fn run(rows: &[AuditRow]) -> Vec<Verdict> {
        let mut data = Dataset::new();
        data.register_component::<AuditRow>().unwrap();
        data.register_component::<AuditVerdict>().unwrap();
        data.append::<AuditRow>(rows).unwrap();
        verify_impl(&mut data).unwrap();
        let batch = data.batch_for("AuditVerdict").expect("AuditVerdict batch");
        let audit_id = column::<Int64Array>(batch, "audit_id").unwrap();
        let op = column::<StringArray>(batch, "op").unwrap();
        let ok = column::<BooleanArray>(batch, "ok").unwrap();
        let checksum = column::<Int64Array>(batch, "checksum").unwrap();
        (0..batch.num_rows())
            .map(|i| {
                (
                    audit_id.value(i),
                    op.value(i).to_string(),
                    ok.value(i),
                    checksum.value(i),
                )
            })
            .collect()
    }

    #[test]
    fn an_insert_of_a_known_status_is_ok() {
        let out = run(&[row("I", 1, "placed", 1)]);
        assert_eq!(out.len(), 1);
        assert!(out[0].2);
        assert_eq!(out[0].1, "I", "__op is renamed to op on the way out");
    }

    #[test]
    fn an_update_is_ok_too() {
        assert!(run(&[row("U", 1, "shipped", 3)])[0].2);
    }

    #[test]
    fn a_delete_is_reported_rather_than_dropped() {
        let out = run(&[row("D", 1, "settled", 4)]);
        assert_eq!(out.len(), 1, "the row still reaches the sink");
        assert!(!out[0].2);
    }

    #[test]
    fn an_unknown_status_is_not_ok() {
        assert!(!run(&[row("I", 1, "teleported", 1)])[0].2);
    }

    #[test]
    fn revision_zero_is_not_ok() {
        assert!(!run(&[row("I", 1, "placed", 0)])[0].2);
        assert!(!run(&[row("I", 1, "placed", -1)])[0].2);
        assert!(run(&[row("I", 1, "placed", 1)])[0].2, "1 is the floor");
    }

    #[test]
    fn every_valid_status_is_accepted() {
        for status in VALID_STATUSES {
            assert!(run(&[row("I", 1, status, 1)])[0].2, "{status}");
        }
    }

    #[test]
    fn the_checksum_covers_op_and_the_verdict() {
        let out = run(&[row("I", 7, "picked", 2)]);
        let expected = fnv1a64(b"7|107|I|picked|2|1700000000007|true");
        assert_eq!(out[0].3, expected);

        // The same row rejected by its op hashes differently, so a verdict
        // cannot be swapped for another row's.
        let rejected = run(&[row("D", 7, "picked", 2)]);
        assert_ne!(rejected[0].3, expected);
    }

    #[test]
    fn an_empty_batch_emits_nothing() {
        assert!(run(&[]).is_empty());
    }

    #[test]
    fn fnv1a64_matches_the_reference_vector() {
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8cu64 as i64);
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325u64 as i64);
    }
}
