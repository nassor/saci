//! The two processor-target gates CI runs on every push.
//!
//! Both exist to catch drift that a host-only build cannot see: `saci` has
//! to keep compiling for `wasm32-wasip3` without its default features, and
//! the `arrow-ipc` pin has to keep meaning the same bytes on both sides of
//! the component boundary.

use crate::sh::{Ctx, Result};

const CHECK_USAGE: &str = "usage: cargo xtask check-wasm-processor";
const ROUNDTRIP_USAGE: &str = "usage: cargo xtask processor-ipc-roundtrip";

/// Check that `saci` still compiles for the processor target.
///
/// A processor build has no tokio and no rayon: it runs the system DAG through
/// the `pollster` sync executor, which only the `processor` feature selects. A
/// host-only test run cannot catch a dependency that crept into that path.
///
/// The crate is checked with `--no-default-features --features processor`,
/// and a failure ends the task with cargo's own exit code. That combination is
/// a documented promise: `crates/saci/src/lib.rs` and AGENTS.md hand a
/// processor author exactly it, because the default `engine` implies
/// `runtime` (tokio and rayon), and tokio cannot target wasm32-wasip3. Nothing
/// in a host build compiles the crate that way, so a feature edge leaking
/// tokio back in would falsify the instruction silently.
///
/// It installs the `wasm32-wasip3` target first, as
/// [`processor_ipc_roundtrip`] does.
pub fn check_wasm_processor(args: &[String]) -> Result<()> {
    let ctx = Ctx::new("check-wasm-processor");
    if ctx.no_options(args, CHECK_USAGE)? {
        return Ok(());
    }

    ctx.cmd("rustup")?
        .args(["target", "add", "wasm32-wasip3"])
        .run()?;

    ctx.log("checking saci for wasm32-wasip3...");
    ctx.cargo()?
        .args([
            "check",
            "--manifest-path",
            "crates/saci/Cargo.toml",
            "--target",
            "wasm32-wasip3",
            "--no-default-features",
            "--features",
            "processor",
        ])
        .run()?;

    ctx.log("PASS: saci builds for wasm32-wasip3");
    Ok(())
}

/// Drive the host to processor Arrow IPC round-trip regression test.
///
/// It catches `arrow-ipc` version drift between the host and the processor
/// SDK before drift can corrupt checkpoints in production.
///
/// Steps:
///
/// 1. Ensure the `wasm32-wasip3` toolchain target is installed.
/// 2. Build the `saci-processor-smoketest` component, release profile.
/// 3. Run the host-side `wasm_roundtrip` integration test, which loads the
///    `.wasm` through `WasmPipelineRuntime`, drives a `RecordBatch` through
///    `run-batch`, and asserts byte-exact IPC equality on the round-trip.
///
/// `rustc` links a `wasm32-wasip3` cdylib into a Component Model component
/// itself, so the artifact under `target/wasm32-wasip3/release/` is the finished
/// component: no preview1 core module and no adapter step in between.
///
/// The fixture is deliberately trivial: one component, a single u64 field, zero
/// systems, so the pipeline is an identity. Any byte difference between the
/// before and after IPC snapshots therefore means `arrow-ipc` drift, not a
/// processor logic bug.
///
/// Cold runs are slow: the first `wasm32-wasip3` build of `arrow-ipc`,
/// `saci` and the `wit-bindgen` generator. CI should cache `target/` and
/// `~/.cargo/registry` between runs.
///
/// Exit codes: 3 build produced no artifact.
pub fn processor_ipc_roundtrip(args: &[String]) -> Result<()> {
    let ctx = Ctx::with_pins("processor-ipc-roundtrip", "crates/saci/PINS.md");
    if ctx.no_options(args, ROUNDTRIP_USAGE)? {
        return Ok(());
    }

    ctx.log(format!("repo: {}", ctx.root().display()));

    ctx.log("ensuring wasm32-wasip3 target is installed...");
    ctx.cmd("rustup")?
        .args(["target", "add", "wasm32-wasip3"])
        .run()?;

    // Release profile: the test asserts the canonical release output path.
    ctx.log("building saci-processor-smoketest (release)...");
    ctx.cargo()?
        .args([
            "build",
            "--release",
            "-p",
            "saci-processor-smoketest",
            "--target",
            "wasm32-wasip3",
        ])
        .run()?;

    let artifact = ctx.path("target/wasm32-wasip3/release/saci_processor_smoketest.wasm");
    ctx.expect_artifact(&artifact, 3)?;
    ctx.log(format!("smoketest built: {} bytes", ctx.size(&artifact)?));

    // The test name and crate are pinned so later `saci` test additions do
    // not silently join this gate.
    ctx.log("running host-side round-trip test...");
    ctx.cargo()?
        .args([
            "test",
            "--test",
            "wasm_roundtrip",
            "-p",
            "saci",
            "--features",
            "wasm-host",
            "--",
            "--nocapture",
        ])
        .run()?;

    ctx.log("PASS");
    Ok(())
}
