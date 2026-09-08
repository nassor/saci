//! `cargo xtask validate`: gate every runnable example config through
//! `saci-service validate`, then every `examples/configs/*.kdl` file through
//! its `--connectors-only` mode.
//!
//! Each config's registry entry (`examples.rs`) carries the `variables` block
//! that resolves its `${...}` placeholders, and this task injects it into a
//! temp copy, so every example validates against an environment that declares
//! nothing and no per-config `export VAR=...` lines are needed in the shell.
//!
//! `saci-service validate` opens no connector connections (every factory
//! builds synchronously), so a config that references NATS, PostgreSQL or
//! Kafka still validates without those services running.
//!
//! ## Pass 1: build and run, the registry (`examples.rs`)
//!
//! Per selected config:
//!
//! 1. Build the processor components it loads (wasm components, native
//!    plugins; the quickstart and polyglot builds run their own tasks). A
//!    config whose toolchains are missing is recorded as failed and does not
//!    stop the rest.
//! 2. Build `saci-service` once with the union of features the configs whose
//!    artifacts built need: each entry names the processor host it loads from
//!    and every opt-in connector its config declares.
//! 3. Write a temp copy with the entry's `variables` block and run
//!    `saci-service validate --config <copy>`.
//!
//! This is the only pass that checks a workflow's `link`s end to end:
//! matching components and field-for-field identical schemas, which needs
//! the real processor built.
//!
//! ## Pass 2: config-only, every `examples/configs/*.kdl` file
//!
//! Most templates under `examples/configs/` name a `pipelines/*.wasm`
//! placeholder that does not exist in the repository, so pass 1's build gate
//! cannot load them and they are not in the registry above.
//! `saci-service validate --connectors-only` builds every declared source,
//! sink and transformer with no processor artifact needed: every built-in
//! connector's factory parses and validates its own `deny_unknown_fields`
//! config synchronously, with no network or broker connection, so a
//! misspelled or misplaced key still fails this pass.
//!
//! Files are discovered by listing `examples/configs/`, not from a
//! hand-maintained list, so a new file there is covered the day it lands.
//! `--only` narrows pass 1 only; pass 2 always covers every file. The two
//! files that declare a `${VAR}` placeholder with no default
//! (`CONFIG_ONLY_VARIABLES` below) get the same `variables`-injection
//! treatment as the registry above; every other file's own
//! `${NAME:-default}` fallback needs nothing injected.
//!
//! Exit codes:
//!
//! | code | meaning                                                        |
//! |------|----------------------------------------------------------------|
//! | 1    | an artifact build, or a `saci-service validate` run in either pass, failed |
//! | 2    | `--only` named an unknown config                              |

use std::path::PathBuf;

use crate::examples::{
    EXAMPLES, Example, build_service, build_service_all_features, inject, inject_variables,
    service_binary,
};
use crate::sh::{Ctx, Result};

const USAGE: &str = "usage: cargo xtask validate [--only=name1,name2,...]";

/// What `-h` prints.
const HELP: &str = "\
usage: cargo xtask validate [--only=LIST]

  --only=LIST   Validate only the named example configs, a comma-separated
                subset of:

                standalone_wasm   load the Rust order-processing component
                standalone_plugin load the native plugin smoketest
                branching         load both routers, wasm and plugin
                windowing_tumbling load both tumbling processors
                windowing_sliding load the sliding processor
                windowing_session load the session processor
                quickstart        load the Go + C# Quick Start components
                standalone_polyglot  load the Python stage
                integrity         load the integrity demo's stream half
                integrity_audit   load the integrity demo's PostgreSQL CDC half

                The default is every config above. Processor artifacts are
                built first, then `saci-service validate` runs once per config
                against a temp copy carrying its `variables` block, so no
                OS env vars need exporting. Unaffected by --only: every
                examples/configs/*.kdl file also gets a --connectors-only
                pass, needing no processor artifact.";

/// `${VAR}` placeholders with no default that the config-only pass must
/// resolve to parse at all; every other `examples/configs/*.kdl` file's own
/// `${NAME:-default}` fallback needs nothing injected. Matched against the
/// file's own name, not its path.
const CONFIG_ONLY_VARIABLES: &[(&str, &[(&str, &str)])] = &[
    ("cluster.kdl", &[("SACI_NODE_ID", "1")]),
    (
        "postgresql.kdl",
        &[("SACI_PG_DSN", "postgres://saci@localhost:5432/app")],
    ),
    // The synced-remote and encryption snippets in turso.kdl are comments, but
    // `${VAR}` substitution runs over the whole file text.
    (
        "turso.kdl",
        &[
            ("SACI_TURSO_URL", "libsql://example.turso.io"),
            ("SACI_TURSO_TOKEN", "example-token"),
            ("SACI_TURSO_KEY", "0"),
        ],
    ),
];

/// Every `examples/configs/*.kdl` file, sorted for a deterministic run order.
fn config_files(ctx: &Ctx) -> Result<Vec<PathBuf>> {
    let dir = ctx.path("examples/configs");
    let entries = std::fs::read_dir(&dir)
        .map_err(|e| ctx.error(1, &[&format!("reading {}: {e}", dir.display())]))?;
    let mut files: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|e| ctx.error(1, &[&format!("reading {}: {e}", dir.display())]))?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "kdl") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

pub fn run(args: &[String]) -> Result<()> {
    let ctx = Ctx::new("validate");

    let mut only: Option<Vec<String>> = None;
    for arg in args {
        if let Some(list) = arg.strip_prefix("--only=") {
            only = Some(list.split(',').map(str::to_owned).collect());
            continue;
        }
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{HELP}");
                return Ok(());
            }
            other => return ctx.fail(1, &[&format!("unknown argument '{other}'"), USAGE]),
        }
    }

    let selected: Vec<&Example> = match &only {
        None => EXAMPLES.iter().collect(),
        Some(names) => {
            let mut found = Vec::new();
            for name in names {
                match crate::examples::by_name(name) {
                    Some(ex) => found.push(ex),
                    None => {
                        return ctx.fail(2, &[&format!("unknown example '{name}'"), HELP]);
                    }
                }
            }
            found
        }
    };

    // Build every selected config's artifacts, recording (not aborting on) a
    // failure: a config whose toolchains are missing still lets the rest
    // validate. Configs that built then get validated below.
    let mut build_failures: Vec<&'static str> = Vec::new();
    let mut buildable: Vec<&Example> = Vec::new();
    for ex in &selected {
        ctx.log(format!("building artifacts for '{0}'", ex.name));
        match (ex.build)(&ctx) {
            Ok(()) => buildable.push(ex),
            Err(e) => {
                ctx.log(format!(
                    "'{}' artifact build failed: {}",
                    ex.name, e.message
                ));
                build_failures.push(ex.name);
            }
        }
    }

    if buildable.is_empty() {
        return ctx.fail(
            1,
            &[&format!(
                "no example configs had their artifacts built; failing: {}",
                build_failures.join(", ")
            )],
        );
    }

    ctx.log("building saci-service");
    build_service(&ctx, &buildable)?;
    let binary = service_binary(&ctx);

    let tmp = std::env::temp_dir();
    let mut validate_failures: Vec<&'static str> = Vec::new();
    for ex in &buildable {
        let copy = tmp.join(format!("saci-xtask-{0}.kdl", ex.name));
        inject(&ctx, ex, &copy)?;
        ctx.log(format!(
            "validating '{0}' ({1})",
            ex.name,
            PathBuf::from(ex.config).display()
        ));
        match ctx.run_exe(&binary, &["validate", "--config", &copy.to_string_lossy()]) {
            Ok(()) => {}
            Err(e) => {
                ctx.log(format!("'{}' failed to validate: {}", ex.name, e.message));
                validate_failures.push(ex.name);
            }
        }
    }

    let passed = buildable.len() - validate_failures.len();

    // Pass 2: config-only, every examples/configs/*.kdl file. Independent of
    // --only and of pass 1 above: it needs no processor artifact, so every
    // file runs regardless of what pass 1 selected or found.
    ctx.log("building saci-service (--all-features, for the config-only pass)");
    build_service_all_features(&ctx)?;
    let all_features_binary = service_binary(&ctx);
    let configs = config_files(&ctx)?;
    let mut config_only_failures: Vec<String> = Vec::new();
    for path in &configs {
        let file_name = path
            .file_name()
            .expect("a listed directory entry has a file name")
            .to_string_lossy()
            .into_owned();
        let vars: &[(&str, &str)] = CONFIG_ONLY_VARIABLES
            .iter()
            .find(|entry| entry.0 == file_name.as_str())
            .map_or(&[][..], |entry| entry.1);
        let pairs: Vec<(&str, String)> = vars
            .iter()
            .map(|&(name, value)| (name, value.to_string()))
            .collect();
        let copy = tmp.join(format!("saci-xtask-configonly-{file_name}"));
        inject_variables(&ctx, path, &pairs, &copy)?;
        ctx.log(format!(
            "validating (config-only) 'examples/configs/{file_name}'"
        ));
        match ctx.run_exe(
            &all_features_binary,
            &[
                "validate",
                "--config",
                &copy.to_string_lossy(),
                "--connectors-only",
            ],
        ) {
            Ok(()) => {}
            Err(e) => {
                ctx.log(format!(
                    "'{file_name}' failed config-only validation: {}",
                    e.message
                ));
                config_only_failures.push(file_name);
            }
        }
    }
    let config_only_passed = configs.len() - config_only_failures.len();

    if validate_failures.is_empty() && config_only_failures.is_empty() {
        ctx.log(format!("PASS: all {passed} example configs validated"));
        ctx.log(format!(
            "PASS: all {config_only_passed} examples/configs/*.kdl files config-only validated"
        ));
        return Ok(());
    }

    let mut summary: Vec<String> = Vec::new();
    if !validate_failures.is_empty() {
        summary.push(format!(
            "{passed} of {} example configs validated; {} failed: {}",
            buildable.len(),
            validate_failures.len(),
            validate_failures.join(", ")
        ));
    }
    if !config_only_failures.is_empty() {
        summary.push(format!(
            "{config_only_passed} of {} examples/configs/*.kdl files config-only validated; \
             {} failed: {}",
            configs.len(),
            config_only_failures.len(),
            config_only_failures.join(", ")
        ));
    }
    let summary_lines: Vec<&str> = summary.iter().map(String::as_str).collect();
    ctx.fail(1, &summary_lines)
}
