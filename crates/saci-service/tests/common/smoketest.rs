//! Shared fixture for the tests that drive the `saci-processor-smoketest`
//! WebAssembly component: host-side component mirrors, the artifact path, the
//! runtime loader and a seeded dataset.
//!
//! Used by `wasm_roundtrip.rs`, `processor_metrics.rs`, `workflow_branching.rs`,
//! `workflow_metrics.rs` and `workflow_dag.rs`, and by `connector_matrix.rs` for the
//! artifact path alone. They live in separate test binaries because
//! `processor_metrics` and `workflow_metrics` each install a process-global meter
//! provider, which must be in place before any `run_on` builds the instruments. Each
//! binary uses a subset of this module.
#![allow(dead_code, reason = "each test binary uses a subset of the fixture")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use saci_core::runtime::PipelineRuntime as _;
use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::wasm::{WasmEngine, WasmPipelineRuntime};
use serde::{Deserialize, Serialize};

/// Host-side mirror of the `Ping` component declared in
/// `crates/saci-processor-smoketest/src/lib.rs`.
///
/// Both definitions must share the same name and field shape. The
/// schema-fingerprint check in `wasm_roundtrip.rs` is what enforces it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Ping {
    pub seq: u64,
}

impl Component for Ping {
    fn name() -> &'static str {
        "Ping"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "seq",
            DataType::UInt64,
            false,
        )]))
    }
}

/// Host-side mirror of the smoketest's state component.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Counter {
    pub count: u64,
}

impl Component for Counter {
    fn name() -> &'static str {
        "Counter"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))
    }
}

/// Where the release build leaves the smoketest artifact.
///
/// `rustc` links a `wasm32-wasip2` cdylib into a Component Model component
/// itself, so `cargo build --target wasm32-wasip2` writes the finished component
/// here with no intermediate core module and no preview1 adapter step.
pub fn smoketest_wasm_path() -> PathBuf {
    // CARGO_MANIFEST_DIR points at crates/saci-service, so the workspace root is
    // two levels up.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root above crates/saci-service");
    workspace_root
        .join("target")
        .join("wasm32-wasip2")
        .join("release")
        .join("saci_processor_smoketest.wasm")
}

/// Load the smoketest component with `config` injected as
/// `[pipeline.wasm.config]` would.
pub fn load_runtime(config: HashMap<String, String>) -> WasmPipelineRuntime {
    let wasm_path = smoketest_wasm_path();
    assert!(
        wasm_path.exists(),
        "smoketest .wasm not found at {}; \
         run `cargo build --release -p saci-processor-smoketest --target wasm32-wasip2` first",
        wasm_path.display()
    );

    let wasm_bytes =
        std::fs::read(&wasm_path).unwrap_or_else(|e| panic!("read smoketest wasm: {e}"));

    let engine = WasmEngine::new().expect("WasmEngine init");
    WasmPipelineRuntime::from_bytes(
        engine,
        "smoketest",
        &wasm_bytes,
        config,
        // 60 epoch ticks of 100 ms = 6 s, plenty for an identity round-trip.
        60,
    )
    .expect("WasmPipelineRuntime::from_bytes")
}

/// Seed a dataset from the runtime's own template so every component the
/// processor declares is registered, then fill the data plane with `rows` `Ping`
/// values.
pub fn seeded_dataset(runtime: &WasmPipelineRuntime, rows: usize) -> Dataset {
    let mut dataset = runtime.template_dataset();
    let pings: Vec<Ping> = (0..rows).map(|i| Ping { seq: i as u64 }).collect();
    dataset.append::<Ping>(&pings).expect("append Ping rows");
    dataset
}
