//! `cargo xtask demo <name>`: run an example pipeline end to end.
//!
//! For a `one_shot` config this builds its artifacts and the service, writes a
//! `variables`-bearing copy of the config into `target/xtask/<name>.kdl`, and
//! runs `saci-service serve` against it to completion. The FileSinks truncate
//! on build, so the run is repeatable and self-contained.
//!
//! A streaming config (branching, the three windowing modes, quickstart) has
//! no natural end:
//! `serve` runs until Ctrl-C against live NATS/PostgreSQL services. For those
//! the command builds the artifacts and writes the same ready-to-run config,
//! then prints the exact `serve` + publisher invocations instead of blocking.
//! The injected config needs no OS env vars; only the external services do.
//!
//! Exit codes:
//!
//! | code | meaning                                    |
//! |------|--------------------------------------------|
//! | 1    | a build or the `serve` run failed          |
//! | 2    | no `<name>` given or it is unknown         |

use std::path::Path;

use crate::examples::{Example, build_service, by_name, feature_flag, inject, service_binary};
use crate::sh::{Ctx, Result};

const USAGE: &str = "usage: cargo xtask demo <name>";

const HELP: &str = "\
usage: cargo xtask demo <name>

  <name>  one of:
            standalone_wasm    one_shot: runs serve and exits
            standalone_plugin  one_shot: runs serve and exits
            branching          streaming: prints the run commands
            windowing_tumbling streaming: prints the run commands
            windowing_sliding  streaming: prints the run commands
            windowing_session  streaming: prints the run commands
            quickstart         streaming: prints the run commands
            standalone_polyglot  one_shot: runs serve and exits
            integrity          streaming: prints the run commands
            integrity_audit    streaming: prints the run commands

  A one_shot demo builds the config's artifacts, writes
  target/xtask/<name>.kdl (a variables-bearing copy), and runs
  `saci-service serve` against it. A streaming demo builds the artifacts and
  prints the serve + publisher commands, which need live NATS/PostgreSQL.";

pub fn run(args: &[String]) -> Result<()> {
    let ctx = Ctx::new("demo");

    let name = match args {
        [name] if name == "-h" || name == "--help" => {
            println!("{HELP}");
            return Ok(());
        }
        [name] => name,
        [] => return ctx.fail(2, &["no <name> given", USAGE]),
        _ => return ctx.fail(2, &["too many arguments", USAGE]),
    };

    let Some(ex) = crate::examples::by_name(name) else {
        return ctx.fail(2, &[&format!("unknown example '{name}'"), HELP]);
    };

    ctx.log(format!("building artifacts for '{name}'"));
    (ex.build)(&ctx)?;

    ctx.log("building saci-service");
    build_service(&ctx, &[ex])?;
    let binary = service_binary(&ctx);

    let out = ctx.root().join("target/xtask").join(format!("{name}.kdl"));
    inject(&ctx, ex, &out)?;
    ctx.log(format!("config written to {}", out.display()));

    if ex.one_shot {
        run_one_shot(&ctx, &binary, ex, &out)?;
    } else {
        print_commands(ex);
    }
    Ok(())
}

fn run_one_shot(ctx: &Ctx, binary: &Path, ex: &Example, config: &Path) -> Result<()> {
    ctx.log(format!("running `saci-service serve` for '{0}'", ex.name));
    ctx.run_exe(binary, &["serve", "--config", &config.to_string_lossy()])?;
    ctx.log(format!("PASS: '{0}' ran to completion", ex.name));
    Ok(())
}

/// The two registry entries the integrity demo spans, which share one printed
/// sequence instead of each getting a `serve` + publisher pair.
const INTEGRITY: [&str; 2] = ["integrity", "integrity_audit"];

/// One streaming demo's publisher: the `[[example]]` target that feeds its
/// config, the features that target needs, and the arguments this demo runs
/// it with.
///
/// `features` is the target's `required-features` in
/// `crates/saci-service/Cargo.toml`: every publisher reaches `async-nats`
/// through `saci-connector-nats`, so each target names that connector
/// feature. Cargo skips a target whose required features are off, silently
/// and with a green exit, so a printed command missing the flag publishes
/// nothing at all; the test module below pins every row against the manifest
/// so the two cannot drift apart.
struct Publisher {
    /// The registry entry whose `serve` line this publisher feeds.
    example: &'static str,
    /// The `[[example]]` target name.
    target: &'static str,
    /// The features that target declares as `required-features`.
    features: &'static [&'static str],
    /// Everything after the `--` separator.
    args: &'static str,
}

impl Publisher {
    /// The `cargo run` line [`print_commands`] prints for this publisher.
    ///
    /// Built with [`feature_flag`], the helper the `serve` line above it
    /// already uses, so both lines in one printed pair select their features
    /// the same way.
    fn command(&self) -> String {
        format!(
            "cargo run -p saci-service --example {0} {1}-- {2}",
            self.target,
            feature_flag(self.features),
            self.args
        )
    }
}

/// The publisher every streaming registry entry outside [`INTEGRITY`] needs,
/// one row per entry.
///
/// Two windowing modes share `windowed_publish` with the same arguments, and
/// they still get a row each: the key is the registry entry a reader named on
/// the command line, not the target it happens to resolve to.
const PUBLISHERS: &[Publisher] = &[
    Publisher {
        example: "branching",
        target: "branching_publish",
        features: &["connector-nats"],
        args: "--rate 50",
    },
    Publisher {
        example: "windowing_tumbling",
        target: "windowed_publish",
        features: &["connector-nats"],
        args: "--rate 20 --ts-step-ms 2000",
    },
    Publisher {
        example: "windowing_sliding",
        target: "windowed_publish",
        features: &["connector-nats"],
        args: "--rate 20 --ts-step-ms 2000",
    },
    Publisher {
        example: "windowing_session",
        target: "windowed_publish",
        features: &["connector-nats"],
        // The gap flags put a silence in every symbol's stream at the same
        // instant, so a session never outlives its burst.
        args: "--rate 20 --ts-step-ms 2000 --gap-every 20 --gap-ms 30000",
    },
    Publisher {
        example: "quickstart",
        target: "quickstart_publish",
        features: &["connector-nats"],
        args: "--count 5000 --rate 500",
    },
];

/// Look a publisher up by the registry entry it feeds.
fn publisher(example: &str) -> Option<&'static Publisher> {
    PUBLISHERS.iter().find(|entry| entry.example == example)
}

/// Print the `serve` + publisher invocations a streaming config needs, with
/// the already-written variables-bearing config.
fn print_commands(ex: &Example) {
    // The integrity demo is two `saci-service` processes over one publisher,
    // so either of its entries prints the whole sequence rather than half of
    // it. Both configs are written by `run`, so whichever entry was named,
    // the other's copy may not exist yet: the printed lines use the
    // committed configs and their in-file `${VAR:-default}` values.
    if INTEGRITY.contains(&ex.name) {
        let stream = by_name("integrity").expect("EXAMPLES declares it, in examples.rs");
        let audit = by_name("integrity_audit").expect("EXAMPLES declares it, in examples.rs");
        println!("[demo] Start the stack, then run, in this order:");
        println!("[demo]   docker compose -f examples/integrity/docker-compose.yml up -d");
        println!("[demo]   cargo run -p saci-service --example integrity_check");
        println!(
            "[demo]   cargo run -p saci-service {}-- serve \
             -c examples/integrity/integrity.kdl",
            feature_flag(stream.features)
        );
        println!(
            "[demo]   cargo run -p saci-service {}-- serve \
             -c examples/integrity/integrity_audit.kdl",
            feature_flag(audit.features)
        );
        println!(
            "[demo] The publisher serves the verification endpoint on 127.0.0.1:9099 and \
             waits for both services; the compose stack must be up before it runs."
        );
        println!(
            "[demo] The order is enforced: the publisher's startup reset deletes the Kafka \
             topic, its consumer group, the JetStream messages, the audit table and the \
             replication slot, so it exits 2 rather than run while either control plane \
             answers. Stop both services before re-running it."
        );
        return;
    }
    let Some(publish) = publisher(ex.name) else {
        unreachable!("streaming config {} has no publisher", ex.name);
    };
    println!("[demo] Start the services, then run:");
    match ex.name {
        "branching" => {
            println!("[demo]   docker run -d --name saci-nats -p 4222:4222 nats:2.11-alpine");
        }
        // One compose file serves all three windowing modes, so the path is
        // not this entry's own name.
        name if name.starts_with("windowing_") => {
            println!("[demo]   docker compose -f examples/windowing/docker-compose.yml up -d");
        }
        _ => {
            println!(
                "[demo]   docker compose -f examples/{0}/docker-compose.yml up -d",
                ex.name
            );
        }
    }
    println!(
        "[demo]   cargo run -p saci-service {0}-- serve -c target/xtask/{1}.kdl",
        feature_flag(ex.features),
        ex.name
    );
    println!("[demo]   {}", publish.command());
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{INTEGRITY, PUBLISHERS, publisher};
    use crate::examples::EXAMPLES;

    /// `crates/saci-service/Cargo.toml`, read at compile time.
    ///
    /// Same mechanism as `crates/saci-service/tests/feature_bundles.rs`,
    /// which reads its own crate's manifest this way and parses the one
    /// section it needs with a line classifier: no TOML dependency, and
    /// `xtask` keeps its empty `[dependencies]`. The path crosses into a
    /// sibling crate, which is what buys the check its teeth: cargo tracks an
    /// included file, so editing a `required-features` line recompiles this
    /// test, and moving the manifest breaks the build here instead of leaving
    /// a check that reads a file nobody writes any more. Reading it at run
    /// time from the repository root would turn both of those into a silent
    /// pass or a late panic.
    const SERVICE_MANIFEST: &str = include_str!("../../crates/saci-service/Cargo.toml");

    /// Every `[[example]]` target in the manifest, as
    /// `(name, required-features)` in manifest order.
    ///
    /// A `[` line opens a table: `[[example]]` starts a target and anything
    /// else ends the run of them. Inside one, a `#` line is a comment (three
    /// of the four publishers carry one), `name` and `required-features` are
    /// the two keys this needs, and a line starting with `"` extends the
    /// array the preceding key opened, so a wrapped list reads the same as a
    /// single-line one. `path`, `harness` and anything else is skipped.
    ///
    /// # Panics
    ///
    /// If the manifest declares no `[[example]]` target, or a target's
    /// `required-features` appears before its `name`.
    fn example_targets() -> Vec<(&'static str, Vec<&'static str>)> {
        let mut targets: Vec<(&str, Vec<&str>)> = Vec::new();
        let mut in_example = false;
        for line in SERVICE_MANIFEST.lines() {
            let line = line.trim();
            if line.starts_with('[') {
                in_example = line == "[[example]]";
                continue;
            }
            if !in_example || line.is_empty() || line.starts_with('#') {
                continue;
            }
            if line.starts_with('"') {
                open(&mut targets).1.extend(quoted(line));
                continue;
            }
            let Some((key, rest)) = line.split_once('=') else {
                continue;
            };
            match key.trim() {
                "name" => targets.push((
                    quoted(rest)
                        .next()
                        .expect("an [[example]] target's name is a quoted string"),
                    Vec::new(),
                )),
                "required-features" => open(&mut targets).1.extend(quoted(rest)),
                _ => {}
            }
        }
        assert!(
            !targets.is_empty(),
            "crates/saci-service/Cargo.toml declares no `[[example]]` target, so the parser in \
             this module found nothing to compare the publisher table against"
        );
        targets
    }

    /// The target whose keys are being read.
    ///
    /// # Panics
    ///
    /// If no `name` key has been seen yet.
    fn open<'a>(
        targets: &'a mut Vec<(&'static str, Vec<&'static str>)>,
    ) -> &'a mut (&'static str, Vec<&'static str>) {
        targets
            .last_mut()
            .expect("an [[example]] target names itself before it lists required-features")
    }

    /// Every double-quoted substring of `line`, in order.
    fn quoted(line: &'static str) -> impl Iterator<Item = &'static str> {
        line.split('"').skip(1).step_by(2)
    }

    /// A publisher's feature list is exactly the `required-features` its
    /// `[[example]]` target declares.
    ///
    /// Cargo skips a target whose required features are off without a word
    /// and with exit code 0, so the printed line has to carry the flag or the
    /// reader runs nothing and sees no error. Set equality fails in both
    /// directions: a row naming a feature the target no longer requires, and
    /// a target that gained one no row carries.
    #[test]
    fn every_publisher_names_the_features_its_example_target_requires() {
        let targets = example_targets();
        for entry in PUBLISHERS {
            let declared = targets
                .iter()
                .find(|(name, _)| *name == entry.target)
                .unwrap_or_else(|| {
                    panic!(
                        "crates/saci-service/Cargo.toml declares no `[[example]]` target named \
                         `{}`, so the `{}` demo prints a command cargo cannot run",
                        entry.target, entry.example
                    )
                });
            assert_eq!(
                entry.features.iter().copied().collect::<BTreeSet<_>>(),
                declared.1.iter().copied().collect::<BTreeSet<_>>(),
                "the `{}` demo prints its publisher's `--features` from this table, and \
                 `{}`'s `required-features` in crates/saci-service/Cargo.toml is what cargo \
                 needs to build that target at all",
                entry.example,
                entry.target,
            );
        }
    }

    /// Every streaming registry entry is printable: it has a publisher row,
    /// or it is one of the two the integrity sequence covers.
    ///
    /// `print_commands` panics on an entry that is neither, and the only way
    /// to reach that is `cargo xtask demo <name>` after registering a new
    /// streaming config, by which point the artifacts have already been
    /// built.
    #[test]
    fn every_streaming_entry_has_a_publisher_or_belongs_to_the_integrity_pair() {
        for ex in EXAMPLES.iter().filter(|ex| !ex.one_shot) {
            assert!(
                INTEGRITY.contains(&ex.name) || publisher(ex.name).is_some(),
                "streaming entry `{}` has no row in PUBLISHERS, so `cargo xtask demo {0}` \
                 panics after building its artifacts",
                ex.name
            );
        }
    }

    /// The rendered line names the target, then the features, then `--`.
    ///
    /// `feature_flag` carries its own trailing space, which is what keeps the
    /// separator from fusing onto the feature list.
    #[test]
    fn a_publisher_line_separates_its_features_from_its_arguments() {
        let entry = publisher("branching").expect("PUBLISHERS declares it");
        assert_eq!(
            entry.command(),
            "cargo run -p saci-service --example branching_publish --features connector-nats \
             -- --rate 50"
        );
    }
}
