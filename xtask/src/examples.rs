//! The runnable example configurations, and how to build each one's artifacts.
//!
//! One registry entry per config under `examples/` that `cargo xtask validate`
//! checks and `cargo xtask demo` runs. An entry carries everything a command
//! needs that is not in the config file itself:
//!
//! - the `saci-service` features the config needs beyond the default bundle:
//!   the processor host it loads from, plus every connector it names that
//!   needs an installed broker, database or object store, all of which are
//!   opt-in,
//! - a step that builds the processor components the config loads,
//! - the names a `variables` block must resolve so the config is
//!   self-contained. A committed example config uses `${NAME:-default}`
//!   placeholders; the one value a default cannot express cross-platform is
//!   the native plugin library path (`lib*.so` is the Linux default, which is
//!   wrong on Windows), so plugin configs resolve `SACI_PLUGIN_LIB` here. Every
//!   config gets an isolated `SACI_DATA_DIR` (and `SACI_OUT_DIR` where used) so
//!   runs do not collide. Service URLs keep their in-file defaults.
//!
//! This is the part "before the variables feature" made awkward: an xtask had
//! to export OS env vars into the child `saci-service` process, process-global
//! and fragile, to make a config resolve. Injecting a `variables` block into a
//! copy keeps the run deterministic and environment-free.

use std::env::consts::{DLL_PREFIX, DLL_SUFFIX, EXE_SUFFIX};
use std::path::{Path, PathBuf};

use crate::sh::{Ctx, Result};

/// How one injected config variable's value is derived from the context.
pub type VariableValue = fn(&Ctx) -> String;

/// One runnable example config.
pub struct Example {
    /// Stable id, used by `cargo xtask validate --only` and `cargo xtask demo`.
    pub name: &'static str,
    /// Path to the committed KDL config, relative to the repository root.
    pub config: &'static str,
    /// Extra `saci-service` features beyond the default bundle.
    pub features: &'static [&'static str],
    /// Build the processor components this config loads.
    pub build: fn(&Ctx) -> Result<()>,
    /// `(name, value)` pairs an injected `variables` block resolves, so the
    /// config needs no OS env export. The value is computed from the context.
    pub variables: &'static [(&'static str, VariableValue)],
    /// Whether `serve` returns on its own (`run_mode kind="one_shot"`). A
    /// streaming config runs until Ctrl-C, so `demo` prints its run commands
    /// rather than blocking.
    pub one_shot: bool,
}

/// An isolated per-run data directory, so FileSinks and checkpoints do not
/// collide across runs. Forward slashes keep the value valid KDL on Windows.
fn data_dir(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("saci-xtask-{name}"));
    // FileSinks open their output file at build time, so the directory must
    // already exist; create it before the value is injected.
    let _ = std::fs::create_dir_all(&dir);
    dir.to_string_lossy().replace('\\', "/")
}

/// The platform-correct native plugin library path, relative to the
/// repository root (every command runs from there). Demo plugins are built in
/// release like their wasm twins; the smoketest fixture stays debug because
/// the plugin tests resolve it from the test binary's own profile dir.
fn plugin_lib(name: &str, release: bool) -> String {
    format!(
        "target/{}/{DLL_PREFIX}{name}{DLL_SUFFIX}",
        if release { "release" } else { "debug" }
    )
}

/// Build one processor component for `wasm32-wasip2`.
fn cargo_wasm(ctx: &Ctx, pkg: &str) -> Result<()> {
    ctx.cargo()?
        .args(["build", "--release", "-p", pkg, "--target", "wasm32-wasip2"])
        .run()
}

/// Build one native plugin cdylib.
fn cargo_plugin(ctx: &Ctx, pkg: &str) -> Result<()> {
    ctx.cargo()?.args(["build", "--release", "-p", pkg]).run()
}

/// Build one native plugin cdylib in the dev profile (test fixtures and the
/// standalone_plugin demo resolve their artifact from `target/debug`).
fn cargo_plugin_debug(ctx: &Ctx, pkg: &str) -> Result<()> {
    ctx.cargo()?.args(["build", "-p", pkg]).run()
}

fn build_order_processing_wasm(ctx: &Ctx) -> Result<()> {
    cargo_wasm(ctx, "order-processing-wasm")
}

fn build_branching(ctx: &Ctx) -> Result<()> {
    cargo_wasm(ctx, "branching-wasm")?;
    cargo_plugin(ctx, "branching-plugin")
}

fn build_windowing_tumbling(ctx: &Ctx) -> Result<()> {
    cargo_wasm(ctx, "windowing-tumbling-wasm")?;
    cargo_plugin(ctx, "windowing-tumbling-plugin")
}

fn build_windowing_sliding(ctx: &Ctx) -> Result<()> {
    cargo_wasm(ctx, "windowing-sliding-wasm")
}

fn build_windowing_session(ctx: &Ctx) -> Result<()> {
    cargo_wasm(ctx, "windowing-session-wasm")
}

/// The integrity demo's three processor components. Both of its registry
/// entries name this: the second build is a no-op once the first has run.
fn build_integrity(ctx: &Ctx) -> Result<()> {
    cargo_wasm(ctx, "integrity-classify-wasm")?;
    cargo_wasm(ctx, "integrity-aggregate-wasm")?;
    cargo_wasm(ctx, "integrity-audit-wasm")
}

/// The committed catalog file the integrity demo's `FileSource` reads.
///
/// `FileSource` opens its path when the factory is built, `validate`
/// included, so a generated throwaway would work for the gate and then
/// disagree with the publisher, which predicts every `taxable` flag from the
/// same file. One committed file, named here and defaulted in the config, is
/// what keeps the three readers in agreement. Relative to the repository
/// root, which is where every command runs.
fn integrity_catalog(_ctx: &Ctx) -> String {
    "examples/integrity/catalog.csv".to_string()
}

fn build_saci_plugin_smoketest(ctx: &Ctx) -> Result<()> {
    cargo_plugin_debug(ctx, "saci-plugin-smoketest")
}

fn build_quickstart(_ctx: &Ctx) -> Result<()> {
    crate::quickstart::run(&[])
}

fn build_polyglot(_ctx: &Ctx) -> Result<()> {
    crate::polyglot::run(&[])
}

/// Every runnable example config, in the order `cargo xtask validate` checks
/// them (cheapest builds first).
pub const EXAMPLES: &[Example] = &[
    Example {
        name: "standalone_wasm",
        config: "examples/configs/standalone_wasm.kdl",
        features: &[],
        build: build_order_processing_wasm,
        variables: &[("SACI_DATA_DIR", |_| data_dir("standalone_wasm"))],
        one_shot: true,
    },
    Example {
        name: "standalone_plugin",
        config: "examples/configs/standalone_plugin.kdl",
        features: &["plugin"],
        build: build_saci_plugin_smoketest,
        variables: &[
            ("SACI_PLUGIN_LIB", |_| {
                plugin_lib("saci_plugin_smoketest", false)
            }),
            ("SACI_DATA_DIR", |_| data_dir("standalone_plugin")),
        ],
        one_shot: true,
    },
    Example {
        name: "branching",
        config: "examples/branching/branching.kdl",
        // A NatsSource fanned out to FileSinks by both processor hosts.
        features: &["plugin", "connector-nats"],
        build: build_branching,
        variables: &[
            ("SACI_PLUGIN_LIB", |_| plugin_lib("branching_plugin", true)),
            ("SACI_OUT_DIR", |_| data_dir("branching")),
            ("SACI_DATA_DIR", |_| data_dir("branching")),
        ],
        one_shot: false,
    },
    Example {
        name: "windowing_tumbling",
        config: "examples/windowing/tumbling/tumbling.kdl",
        // Two NatsSources feeding two PostgresSinks, one per processor host.
        features: &["plugin", "connector-nats", "connector-postgresql"],
        build: build_windowing_tumbling,
        variables: &[
            ("SACI_PLUGIN_LIB", |_| {
                plugin_lib("windowing_tumbling_plugin", true)
            }),
            ("SACI_DATA_DIR", |_| data_dir("windowing-tumbling")),
        ],
        one_shot: false,
    },
    Example {
        name: "windowing_sliding",
        config: "examples/windowing/sliding/sliding.kdl",
        // Two NatsSources feeding a PostgresSink.
        features: &["connector-nats", "connector-postgresql"],
        build: build_windowing_sliding,
        variables: &[("SACI_DATA_DIR", |_| data_dir("windowing-sliding"))],
        one_shot: false,
    },
    Example {
        name: "windowing_session",
        config: "examples/windowing/session/session.kdl",
        // Two NatsSources feeding a PostgresSink.
        features: &["connector-nats", "connector-postgresql"],
        build: build_windowing_session,
        variables: &[("SACI_DATA_DIR", |_| data_dir("windowing-session"))],
        one_shot: false,
    },
    Example {
        name: "quickstart",
        config: "examples/quickstart/quickstart.kdl",
        // A NatsSource feeding a PostgresSink.
        features: &["connector-nats", "connector-postgresql"],
        build: build_quickstart,
        variables: &[("SACI_DATA_DIR", |_| data_dir("quickstart"))],
        one_shot: false,
    },
    Example {
        name: "standalone_polyglot",
        config: "examples/configs/standalone_polyglot.kdl",
        features: &[],
        build: build_polyglot,
        variables: &[("SACI_DATA_DIR", |_| data_dir("standalone_polyglot"))],
        one_shot: true,
    },
    // Two entries, one example: `Example` carries one config, and this demo
    // runs two `saci-service` processes. `run_mode` is a whole-config
    // property, and the stream half's live sources and the CDC half's
    // cycle-EOF source need opposite runners. Registering both is what puts
    // each config under `cargo xtask validate`.
    Example {
        name: "integrity",
        config: "examples/integrity/integrity.kdl",
        // The config's `orders_kafka` and `payments_nats` sources.
        features: &["connector-kafka", "connector-nats"],
        build: build_integrity,
        variables: &[
            ("SACI_DATA_DIR", |_| data_dir("integrity")),
            ("SACI_CATALOG_FILE", integrity_catalog),
        ],
        one_shot: false,
    },
    Example {
        name: "integrity_audit",
        config: "examples/integrity/integrity_audit.kdl",
        // Its only source is a PostgreSQL logical-decoding CDC reader.
        features: &["connector-postgresql"],
        build: build_integrity,
        variables: &[("SACI_AUDIT_DATA_DIR", |_| data_dir("integrity-audit"))],
        one_shot: false,
    },
];

/// Look an example up by its stable id.
pub fn by_name(name: &str) -> Option<&'static Example> {
    EXAMPLES.iter().find(|ex| ex.name == name)
}

/// The `saci-service` binary `validate`/`demo` run, assuming it has been built.
pub fn service_binary(ctx: &Ctx) -> PathBuf {
    // `EXE_SUFFIX` is ".exe"; `with_extension` would double the dot, so the
    // suffix is appended whole.
    ctx.root()
        .join(format!("target/debug/saci-service{EXE_SUFFIX}"))
}

/// Write a copy of `config` with a `variables { ... }` block prepended,
/// resolving each `(name, value)` pair. The copy goes to `out` and needs no
/// OS env export to load.
pub fn inject_variables(
    ctx: &Ctx,
    config: &Path,
    pairs: &[(&str, String)],
    out: &Path,
) -> Result<()> {
    let text = ctx.read(config)?;
    let mut vars = String::new();
    for (name, value) in pairs {
        vars.push_str(&format!("    {name} \"{value}\"\n"));
    }
    ctx.write(out, &format!("variables {{\n{vars}}}\n\n{text}"))?;
    Ok(())
}

/// Write a copy of an example's config with a `variables { ... }` block
/// prepended, resolving every name in its registry entry. The copy goes to
/// `out` and needs no OS env export to load.
pub fn inject(ctx: &Ctx, ex: &Example, out: &Path) -> Result<()> {
    let pairs: Vec<(&str, String)> = ex
        .variables
        .iter()
        .map(|(name, value)| (*name, value(ctx)))
        .collect();
    inject_variables(ctx, Path::new(ex.config), &pairs, out)
}

/// Build `saci-service` with the features `selected` examples need, once.
pub fn build_service(ctx: &Ctx, selected: &[&Example]) -> Result<()> {
    let mut features: Vec<&str> = Vec::new();
    for ex in selected {
        for feature in ex.features {
            if !features.contains(feature) {
                features.push(feature);
            }
        }
    }
    let mut build = ctx
        .cargo()?
        .args(["build", "-p", "saci-service", "--bin", "saci-service"]);
    if !features.is_empty() {
        build = build.args(["--features", &features.join(",")]);
    }
    build.run()
}

/// The `--features` argument a printed `cargo run` line needs, with its
/// trailing space, or an empty string when the default bundle covers the
/// entry.
///
/// Lives beside [`build_service`] because the two answer the same question
/// from one source, `Example::features`: that one compiles the binary, this
/// one prints the command a reader is meant to be able to paste. Both have
/// to treat an entry needing nothing beyond the default bundle as "no
/// `--features` at all" rather than as an empty flag, which `cargo` rejects.
pub fn feature_flag(features: &[&str]) -> String {
    if features.is_empty() {
        String::new()
    } else {
        format!("--features {} ", features.join(","))
    }
}

/// Build `saci-service` with every feature, for the config-only pass over
/// every `examples/configs/*.kdl` file: that pass exercises every built-in
/// connector's factory, not just the ones the registry's buildable entries
/// need.
pub fn build_service_all_features(ctx: &Ctx) -> Result<()> {
    ctx.cargo()?
        .args([
            "build",
            "-p",
            "saci-service",
            "--bin",
            "saci-service",
            "--all-features",
        ])
        .run()
}
