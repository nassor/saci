//! Build the two Quick Start processor components into
//! `examples/quickstart/build/`.
//!
//! - `validate.wasm` (`quickstart-validate-wasm`), reads `amount` and writes
//!   `valid`
//! - `settle.wasm` (`quickstart-settle-wasm`), reads `valid` and `amount`,
//!   writes `fee` and `review_tier`
//!
//! Steps:
//!
//! 1. Ensure the `wasm32-wasip3` Rust target is installed and `wasm-tools`
//!    (pinned in `crates/saci/PINS.md`) is available.
//! 2. Build `quickstart-validate-wasm` and `quickstart-settle-wasm` for
//!    `wasm32-wasip3` in release mode.
//! 3. Collect both into `examples/quickstart/build/` as `validate.wasm` and
//!    `settle.wasm` and validate each one with `wasm-tools`.
//!
//! Exit codes:
//!
//! | code | prerequisite               |
//! |------|----------------------------|
//! | 3    | wasm-tools                 |
//! | 8    | build produced no artifact |

use std::path::Path;

use crate::sh::{Ctx, Result};
use crate::wasm;

const USAGE: &str = "usage: cargo xtask quickstart";

/// Exit code for a build that produced no artifact.
const NO_ARTIFACT: u8 = 8;

pub fn run(args: &[String]) -> Result<()> {
    let ctx = Ctx::with_pins("quickstart", "crates/saci/PINS.md");
    if ctx.no_options(args, USAGE)? {
        return Ok(());
    }

    ctx.log(format!("repo: {}", ctx.root().display()));
    ctx.require("wasm-tools", 3, wasm::WASM_TOOLS_HINT)?;

    ctx.log("ensuring wasm32-wasip3 target is installed...");
    ctx.cmd("rustup")?
        .args(["target", "add", "wasm32-wasip3"])
        .run()?;

    ctx.log(
        "building quickstart-validate-wasm and quickstart-settle-wasm (wasm32-wasip3, release)...",
    );
    ctx.cargo()?
        .args([
            "build",
            "--release",
            "-p",
            "quickstart-validate-wasm",
            "-p",
            "quickstart-settle-wasm",
            "--target",
            "wasm32-wasip3",
        ])
        .run()?;

    let build_dir = ctx.path("examples/quickstart/build");
    ctx.remove_dir(&build_dir)?;
    ctx.ensure_dir(&build_dir)?;
    collect(
        &ctx,
        &ctx.path("target/wasm32-wasip3/release/quickstart_validate_wasm.wasm"),
        &build_dir,
        "validate.wasm",
    )?;
    collect(
        &ctx,
        &ctx.path("target/wasm32-wasip3/release/quickstart_settle_wasm.wasm"),
        &build_dir,
        "settle.wasm",
    )?;

    wasm::validate_dir(&ctx, &build_dir, NO_ARTIFACT)?;

    ctx.log(format!("PASS: components in {}", build_dir.display()));

    // Emit a variables-bearing copy of the Quick Start config so it can be run
    // without exporting an env var: the registry entry injects the `variables`
    // block the config's placeholders resolve against. The components above
    // are exactly what the copy references.
    let entry = crate::examples::by_name("quickstart").expect("registered");
    let emitted = ctx.path("target/xtask/quickstart.kdl");
    crate::examples::inject(&ctx, entry, &emitted)?;
    ctx.log(format!("emitted {} (self-contained)", emitted.display()));

    ctx.log("next: docker compose -f examples/quickstart/docker-compose.yml up -d");
    ctx.log(serve_command_hint(entry.features));
    Ok(())
}

/// The `serve` line `run` prints once the components are built.
///
/// A named function rather than a `format!` inline in [`run`], because
/// everything above that call invokes `wasm-tools` and `cargo --target`:
/// keeping formatting separate makes the spacing testable on its own.
///
/// `features` comes from the `quickstart` registry entry, so the printed
/// command matches what `build_service` compiled.
fn serve_command_hint(features: &[&str]) -> String {
    format!(
        "      cargo run -p saci {}-- serve \
         -c examples/quickstart/quickstart.kdl",
        crate::examples::feature_flag(features)
    )
}

/// Copy a built component into the build directory under its published name.
fn collect(ctx: &Ctx, from: &Path, build_dir: &Path, name: &str) -> Result<()> {
    ctx.expect_artifact(from, NO_ARTIFACT)?;
    let to = build_dir.join(name);
    ctx.copy(from, &to)?;
    ctx.log(format!("collected {name} ({} bytes)", ctx.size(&to)?));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An entry needing nothing beyond the `server` bundle still names it,
    /// with one space before the separator.
    ///
    /// `feature_flag` carries its own trailing space, so the template has
    /// none before `--`. Interpolating a bare `join(",")` here instead
    /// renders `--features  -- serve` for an empty list: cargo reads the `--`
    /// as the value of `--features` and the command dies.
    #[test]
    fn an_entry_needing_no_extra_features_names_the_server_bundle() {
        let line = serve_command_hint(&[]);
        assert!(
            line.contains("cargo run -p saci --features server -- serve"),
            "{line}"
        );
    }

    /// The real entry's shape: one `--features` with the list, then `--`.
    #[test]
    fn an_entry_with_features_names_them_before_the_separator() {
        let line = serve_command_hint(&["connector-nats", "connector-postgresql"]);
        assert!(
            line.contains(
                "cargo run -p saci --features server,connector-nats,connector-postgresql -- serve"
            ),
            "{line}"
        );
    }
}
