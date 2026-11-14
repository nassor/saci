//! Build a `saci-service` binary that carries only the connectors, byte
//! formats, processor hosts and extra features named on the command line.
//!
//! Every part of the `saci` crate sits behind a feature, so a custom server is
//! `cargo build -p saci --bin saci-service --no-default-features --features
//! service,...`. This task spells that list from short names and checks each
//! one against the `[features]` table of `crates/saci/Cargo.toml` before cargo
//! starts, so a typo lists the names that exist instead of failing inside a
//! build. It builds the release profile and copies the binary out of
//! `target/release`, where the next build with other features would replace it.
//!
//! ```text
//! cargo xtask server --connectors=kafka,postgresql --transformers=ndjson --processors=wasm
//! cargo xtask server --connectors=file --transformers=csv,parquet --features=windows,mimalloc --out=dist/saci-service
//! ```
//!
//! | code | meaning                                 |
//! |------|-----------------------------------------|
//! | 1    | unknown argument or unknown name        |
//! | 5    | the build produced no binary            |
//!
//! A failed build exits with cargo's own code.

use std::env::consts::EXE_SUFFIX;
use std::path::PathBuf;

use crate::sh::{Ctx, Result};

const USAGE: &str = "\
usage: cargo xtask server [options]

Build a saci-service binary with only the parts named here. `service` is always on.

  --connectors=LIST    connectors, e.g. file,kafka,postgresql
  --transformers=LIST  byte formats, e.g. csv,ndjson,parquet
  --processors=LIST    processor hosts: wasm, plugin
  --features=LIST      any other saci feature, e.g. windows,mimalloc,service-cluster
  --out=PATH           where the binary is copied, relative to the repository root
                       (default target/server/saci-service)
  --list               print the names each option accepts";

/// Exit code for a build that left no binary behind.
const NO_ARTIFACT: u8 = 5;

/// The manifest whose `[features]` table every name is checked against.
const MANIFEST: &str = "crates/saci/Cargo.toml";

/// Processor host short names and the feature each selects.
const PROCESSORS: [(&str, &str); 2] = [("wasm", "wasm-host"), ("plugin", "plugin-host")];

/// What the command line asked for, each a comma-separated list.
#[derive(Default)]
struct Selection<'a> {
    connectors: &'a str,
    transformers: &'a str,
    processors: &'a str,
    features: &'a str,
}

pub fn run(args: &[String]) -> Result<()> {
    let ctx = Ctx::new("server");

    let mut selection = Selection::default();
    let mut out = None;
    let mut list = false;
    for arg in args {
        let arg = arg.as_str();
        if let Some(value) = arg.strip_prefix("--connectors=") {
            selection.connectors = value;
        } else if let Some(value) = arg.strip_prefix("--transformers=") {
            selection.transformers = value;
        } else if let Some(value) = arg.strip_prefix("--processors=") {
            selection.processors = value;
        } else if let Some(value) = arg.strip_prefix("--features=") {
            selection.features = value;
        } else if let Some(value) = arg.strip_prefix("--out=") {
            out = Some(PathBuf::from(value));
        } else if arg == "--list" {
            list = true;
        } else if arg == "-h" || arg == "--help" {
            println!("{USAGE}");
            return Ok(());
        } else {
            return ctx.fail(1, &[&format!("unknown argument '{arg}'"), USAGE]);
        }
    }

    let manifest = ctx.read(&ctx.path(MANIFEST))?;
    let known = feature_names(&manifest);
    if list {
        print_names(&known);
        return Ok(());
    }
    let features = cargo_features(&known, &selection).map_err(|e| ctx.error(1, &[&e]))?;
    let features = features.join(",");

    ctx.log(format!("features: {features}"));
    ctx.cargo()?
        .args(["build", "--release", "-p", "saci", "--bin", "saci-service"])
        .args(["--no-default-features", "--features", &features])
        .run()?;

    let built = ctx.path(&format!("target/release/saci-service{EXE_SUFFIX}"));
    ctx.expect_artifact(&built, NO_ARTIFACT)?;
    let out = out.unwrap_or_else(|| ctx.path(&format!("target/server/saci-service{EXE_SUFFIX}")));
    ctx.copy(&built, &out)?;
    ctx.log(format!(
        "built {} ({} bytes)",
        out.display(),
        ctx.size(&out)?
    ));
    Ok(())
}

/// The keys of a manifest's `[features]` table.
///
/// A line scan, not a TOML parse: a key starts a line, while the continuation
/// lines of a multi-line feature list are indented and comments start with `#`.
fn feature_names(manifest: &str) -> Vec<&str> {
    manifest
        .lines()
        .skip_while(|line| line.trim() != "[features]")
        .skip(1)
        .take_while(|line| !line.starts_with('['))
        .filter(|line| !line.starts_with(|c: char| c.is_whitespace() || c == '#'))
        .filter_map(|line| line.split_once('=').map(|(key, _)| key.trim()))
        .filter(|key| !key.is_empty())
        .collect()
}

/// The `--features` list for a selection: `service`, then each named part's
/// feature in the order given, duplicates dropped.
fn cargo_features(known: &[&str], selection: &Selection) -> Result<Vec<String>, String> {
    let mut features = vec!["service".to_owned()];
    let mut push = |feature: String| {
        if !features.contains(&feature) {
            features.push(feature);
        }
    };
    for (list, prefix, what) in [
        (selection.connectors, "connector-", "connector"),
        (selection.transformers, "transformer-", "transformer"),
    ] {
        for name in items(list) {
            let feature = format!("{prefix}{name}");
            if name == "contract" || !known.contains(&feature.as_str()) {
                let names = prefixed(known, prefix).join(", ");
                return Err(format!("unknown {what} '{name}'; known: {names}"));
            }
            push(feature);
        }
    }
    for name in items(selection.processors) {
        let Some((_, feature)) = PROCESSORS.iter().find(|(short, _)| *short == name) else {
            return Err(format!(
                "unknown processor host '{name}'; known: wasm, plugin"
            ));
        };
        push((*feature).to_owned());
    }
    for name in items(selection.features) {
        if !known.contains(&name) {
            return Err(format!(
                "unknown feature '{name}'; `cargo xtask server --list` prints every name"
            ));
        }
        push(name.to_owned());
    }
    Ok(features)
}

/// The non-empty entries of a comma-separated list.
fn items(list: &str) -> impl Iterator<Item = &str> {
    list.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
}

/// The short names behind every `<prefix><name>` feature, contracts excluded.
fn prefixed<'a>(known: &[&'a str], prefix: &str) -> Vec<&'a str> {
    known
        .iter()
        .filter_map(|key| key.strip_prefix(prefix))
        .filter(|name| *name != "contract")
        .collect()
}

fn print_names(known: &[&str]) {
    let other: Vec<&str> = known
        .iter()
        .copied()
        .filter(|key| {
            !key.starts_with("connector-")
                && !key.starts_with("transformer-")
                && !PROCESSORS.iter().any(|(_, feature)| feature == key)
                && !matches!(*key, "default" | "service")
        })
        .collect();
    println!("connectors:   {}", prefixed(known, "connector-").join(", "));
    println!(
        "transformers: {}",
        prefixed(known, "transformer-").join(", ")
    );
    println!("processors:   wasm, plugin");
    println!("features:     {}", other.join(", "));
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = include_str!("../../crates/saci/Cargo.toml");

    /// The scan finds keys across single- and multi-line entries and never
    /// mistakes a list element or a comment for a key.
    #[test]
    fn feature_names_reads_the_saci_manifest() {
        let known = feature_names(MANIFEST);
        for key in [
            "default",
            "server",
            "connector-http",
            "connector-kafka",
            "transformer-csv",
            "wasm-host",
            "mimalloc",
        ] {
            assert!(known.contains(&key), "{key} missing from {known:?}");
        }
        assert!(
            known
                .iter()
                .all(|key| key.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')),
            "{known:?}"
        );
    }

    #[test]
    fn cargo_features_maps_short_names_in_order() {
        let known = feature_names(MANIFEST);
        let selection = Selection {
            connectors: "kafka, file,kafka",
            transformers: "csv",
            processors: "wasm,plugin",
            features: "windows",
        };
        assert_eq!(
            cargo_features(&known, &selection).unwrap(),
            [
                "service",
                "connector-kafka",
                "connector-file",
                "transformer-csv",
                "wasm-host",
                "plugin-host",
                "windows",
            ]
        );
    }

    /// An unknown or internal name fails before cargo runs, naming what exists.
    #[test]
    fn cargo_features_rejects_unknown_names() {
        let known = feature_names(MANIFEST);
        for selection in [
            Selection {
                connectors: "kafak",
                ..Selection::default()
            },
            Selection {
                connectors: "contract",
                ..Selection::default()
            },
            Selection {
                transformers: "xml",
                ..Selection::default()
            },
            Selection {
                processors: "python",
                ..Selection::default()
            },
            Selection {
                features: "connector-nope",
                ..Selection::default()
            },
        ] {
            let err = cargo_features(&known, &selection).unwrap_err();
            assert!(err.starts_with("unknown "), "{err}");
        }
        let err = cargo_features(
            &known,
            &Selection {
                connectors: "kafak",
                ..Selection::default()
            },
        )
        .unwrap_err();
        assert!(err.contains("kafka") && !err.contains("contract"), "{err}");
    }
}
