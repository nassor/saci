//! SACI benchmark harness.
//!
//! Reproducible criterion runs: fixed compiler flags, fixed sample size,
//! optional CPU-affinity pinning, and A/B comparison against a saved baseline.
//!
//! Pinning matters on a dual-CCD part such as the Ryzen 9 9950X3D, where only
//! one die carries 3D V-cache. The scheduler can migrate workers across dies
//! and change L3 residency wholesale between runs. Pin for A/B comparison;
//! take published figures unpinned, since that is what a deployment gets.
//!
//! Every refusal here exits 1: an unknown bench or option, a compile that
//! failed, a harness path cargo did not print, a missing `taskset`, or an
//! `--affinity` request the platform cannot honour. A benchmark that runs and
//! fails exits with the harness's own code.

use std::env;
use std::path::Path;

use crate::sh::{Cmd, Ctx, Result};

const USAGE: &str = "\
usage: cargo xtask bench <bench> [options] [-- <extra criterion args>]

  <bench>              one of: tpch_q6 tpch_q1 parallelism_compute parallelism
                       pipeline batch_vs_stream ipc_checkpoint vs_datafusion_q6
                       wasm_roundtrip
  --save NAME          store this run as criterion baseline NAME
  --baseline NAME      compare against baseline NAME; prints % change and p-value
  --affinity MASK      hex CPU affinity mask, e.g. FFFF for logical CPUs 0-15.
                       Omit for all CPUs. Inherited by rayon/tokio workers.
  --filter REGEX       only run benchmarks whose id matches REGEX
  --samples N          criterion sample size (default 10)
  --threads N          size the rayon/tokio pools to N. Pair with --affinity:
                       num_cpus reports the machine, not the affinity mask.
  --features LIST      extra cargo features, comma-separated
  --build-only         compile and exit; always do this before a timing run";

/// Each benchmark, the package whose `benches/` holds it, and the features that
/// package needs for it to compile.
const BENCHES: &[(&str, &str, &str)] = &[
    ("tpch_q6", "saci-core", ""),
    ("tpch_q1", "saci-core", ""),
    ("parallelism", "saci-core", ""),
    ("parallelism_compute", "saci-core", ""),
    ("pipeline", "saci-core", ""),
    ("ipc_checkpoint", "saci-core", ""),
    ("batch_vs_stream", "saci-core", "io"),
    ("vs_datafusion_q6", "saci-connector-datafusion", ""),
    // Needs the smoketest component on disk first:
    // `cargo build --release -p saci-processor-smoketest --target wasm32-wasip2`.
    ("wasm_roundtrip", "saci-service", "service,wasm"),
];

/// The published numbers are taken with these flags; changing them invalidates
/// existing comparisons. A caller who exported `RUSTFLAGS` keeps theirs
/// untouched, because a deliberate override is the one reason to measure
/// something else.
const RUSTFLAGS: &str = "-C target-cpu=native -C opt-level=3 -C codegen-units=1";

/// Install line for the Linux pinning tool.
const TASKSET_HINT: &str = "part of util-linux: apt install util-linux";

pub fn run(args: &[String]) -> Result<()> {
    let ctx = Ctx::new("bench");
    let Some(opts) = parse(&ctx, args)? else {
        return Ok(());
    };

    let Some(&(_, pkg, mapped)) = BENCHES.iter().find(|(name, ..)| *name == opts.bench) else {
        println!("{USAGE}");
        return ctx.fail(1, &[&format!("unknown bench '{}'", opts.bench)]);
    };

    // `--features` adds to the mapped list rather than replacing it: the mapped
    // features are what the benchmark needs to compile at all.
    let mut features = mapped.to_owned();
    if let Some(extra) = opts.features.as_deref() {
        if !features.is_empty() {
            features.push(',');
        }
        features.push_str(extra);
    }

    // Compile as its own step, always. A criterion run that shares the machine
    // with rustc measures rustc.
    let mut build = ctx
        .cargo()?
        .args(["bench", "-p", pkg, "--bench", opts.bench.as_str()]);
    if !features.is_empty() {
        build = build.args(["--features", features.as_str()]);
    }
    build = build.arg("--no-run");
    if env::var_os("RUSTFLAGS").is_none() {
        build = build.env("RUSTFLAGS", RUSTFLAGS);
    }

    // The log is captured because the harness path has to be read back out of
    // it, and printed in full either way.
    let (ok, log) = build.output_merged()?;
    println!("{}", log.trim_end_matches('\n'));
    if !ok {
        return ctx.fail(1, &[&format!("compiling bench '{}' failed", opts.bench)]);
    }
    if opts.build_only {
        return Ok(());
    }

    // Run the harness directly rather than through cargo, so an affinity mask
    // applies to the benchmark process itself instead of to a cargo parent that
    // would spawn it unpinned.
    //
    // The path comes from cargo's own `Executable benches/<name>.rs (<path>)`
    // line, printed on every --no-run invocation. Globbing `deps/<bench>-*` for
    // the newest mtime is unsafe: cargo's metadata hash encodes profile, feature
    // and RUSTFLAGS settings, so binaries built under different configurations
    // coexist in that directory, and a build cargo considers fresh does not
    // touch the artefact's mtime. Fail loudly rather than guess.
    let Some(exe) = harness_path(&log, &opts.bench).filter(|path| Path::new(path).is_file()) else {
        return ctx.fail(
            1,
            &[
                &format!(
                    "could not read an Executable path for bench '{}' out of cargo's output",
                    opts.bench
                ),
                "refusing to guess: a stale binary would produce a silently wrong measurement",
            ],
        );
    };

    let argv = criterion_args(&opts);
    let threads = opts.threads.as_deref();
    println!(
        "== {}  exe={exe}  samples={}  affinity={}  threads={} {}{}",
        opts.bench,
        opts.samples,
        opts.affinity.as_deref().unwrap_or("all"),
        threads.unwrap_or("default"),
        opts.save
            .as_ref()
            .map_or(String::new(), |s| format!("save={s}")),
        opts.baseline
            .as_ref()
            .map_or(String::new(), |b| format!("baseline={b}")),
    );

    match opts.affinity.as_deref() {
        None => with_threads(ctx.cmd(exe)?, threads).args(&argv).run(),
        Some(mask) => pinned(&ctx, exe, &argv, mask, threads),
    }
}

/// One parsed invocation. Every value criterion or cargo receives verbatim is
/// kept as the caller spelled it.
struct Opts {
    bench: String,
    save: Option<String>,
    baseline: Option<String>,
    affinity: Option<String>,
    filter: Option<String>,
    samples: String,
    threads: Option<String>,
    features: Option<String>,
    build_only: bool,
    extra: Vec<String>,
}

/// Parse the command line. `None` means help was printed and there is nothing
/// left to do.
fn parse(ctx: &Ctx, args: &[String]) -> Result<Option<Opts>> {
    let mut rest = args.iter();
    let Some(bench) = rest.next() else {
        println!("{USAGE}");
        return ctx.fail(1, &["a bench name is required"]);
    };
    if matches!(bench.as_str(), "-h" | "--help") {
        println!("{USAGE}");
        return Ok(None);
    }

    let mut opts = Opts {
        bench: bench.clone(),
        save: None,
        baseline: None,
        affinity: None,
        filter: None,
        samples: "10".to_owned(),
        threads: None,
        features: None,
        build_only: false,
        extra: Vec::new(),
    };
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--save" => opts.save = present(value(ctx, &mut rest, "--save")?),
            "--baseline" => opts.baseline = present(value(ctx, &mut rest, "--baseline")?),
            "--affinity" => opts.affinity = present(value(ctx, &mut rest, "--affinity")?),
            "--filter" => opts.filter = present(value(ctx, &mut rest, "--filter")?),
            "--samples" => opts.samples = value(ctx, &mut rest, "--samples")?,
            "--threads" => opts.threads = present(value(ctx, &mut rest, "--threads")?),
            "--features" => opts.features = present(value(ctx, &mut rest, "--features")?),
            "--build-only" => opts.build_only = true,
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(None);
            }
            "--" => {
                opts.extra = rest.cloned().collect();
                break;
            }
            other => {
                println!("{USAGE}");
                return ctx.fail(1, &[&format!("unknown option '{other}'")]);
            }
        }
    }
    Ok(Some(opts))
}

/// The value that follows an option.
fn value(ctx: &Ctx, args: &mut std::slice::Iter<'_, String>, flag: &str) -> Result<String> {
    match args.next() {
        Some(value) => Ok(value.clone()),
        None => {
            println!("{USAGE}");
            ctx.fail(1, &[&format!("{flag} requires a value")])
        }
    }
}

/// An empty value is no value: `--save ''` leaves the option unset instead of
/// handing criterion an empty baseline name.
fn present(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

/// The harness path out of cargo's `Executable benches/<name>.rs (<path>)`
/// line: the last such line wins, and the path is everything between the
/// parenthesis that line opens and the closing one, so a checkout directory
/// holding its own parentheses still parses. A Windows path needs no separator
/// rewriting, there being no shell between cargo's output and the process that
/// runs it.
fn harness_path<'log>(log: &'log str, bench: &str) -> Option<&'log str> {
    let needle = format!("{bench}.rs (");
    log.lines()
        .filter_map(|line| line.split_once(&needle)?.1.trim_end().strip_suffix(')'))
        .next_back()
}

/// The criterion argv: the fixed flags, then the baselines, then the filter
/// regex, then whatever followed `--`.
fn criterion_args(opts: &Opts) -> Vec<String> {
    let mut argv = vec![
        "--bench".to_owned(),
        "--sample-size".to_owned(),
        opts.samples.clone(),
        "--noplot".to_owned(),
    ];
    if let Some(save) = &opts.save {
        argv.push("--save-baseline".to_owned());
        argv.push(save.clone());
    }
    if let Some(baseline) = &opts.baseline {
        argv.push("--baseline".to_owned());
        argv.push(baseline.clone());
    }
    if let Some(filter) = &opts.filter {
        argv.push(filter.clone());
    }
    argv.extend(opts.extra.iter().cloned());
    argv
}

/// Size the worker pools to the pinned set.
///
/// `num_cpus::get()` reports the machine, not the process affinity mask, so a
/// pinned run would otherwise spin up one rayon worker per machine CPU and
/// oversubscribe the pinned set.
fn with_threads(cmd: Cmd, threads: Option<&str>) -> Cmd {
    match threads {
        Some(n) => cmd
            .env("RAYON_NUM_THREADS", n)
            .env("TOKIO_WORKER_THREADS", n),
        None => cmd,
    }
}

/// Launch the harness with `mask` applied to the process.
///
/// The mask has to be on the benchmark process, not on a parent, so each
/// platform gets the mechanism that binds the process itself.
fn pinned(ctx: &Ctx, exe: &str, argv: &[String], mask: &str, threads: Option<&str>) -> Result<()> {
    match env::consts::OS {
        // Windows applies a process affinity mask to every thread the process
        // owns, existing and future, so setting it right after launch still
        // binds the rayon and tokio pools, which are created lazily long after
        // this returns. Setting the mask at creation time is a Win32 call this
        // runner will not hand-roll through FFI, so PowerShell makes it.
        //
        // Start-Process inherits this child's environment, so the pool sizes
        // reach the harness, and it resolves a relative -FilePath against its
        // own working directory: the path joined onto the repository root is
        // native and absolute, so there is nothing left for it to resolve.
        "windows" => {
            let exe = ctx.root().join(exe);
            let list = argv
                .iter()
                .map(|arg| quote(arg.as_str()))
                .collect::<Vec<String>>()
                .join(",");
            let script = format!(
                "$ErrorActionPreference='Stop'\n\
                 $p = Start-Process -FilePath {} -ArgumentList @({list}) -PassThru -NoNewWindow\n\
                 $p.ProcessorAffinity = [System.IntPtr]0x{mask}\n\
                 $p.WaitForExit()\n\
                 exit $p.ExitCode",
                quote(&exe.to_string_lossy()),
            );
            with_threads(ctx.cmd("powershell")?, threads)
                .args(["-NoProfile", "-Command", script.as_str()])
                .run()
        }
        // Linux sets the mask before the harness starts, and a thread inherits
        // its creator's, so the pools are bound before they exist.
        "linux" => {
            ctx.require("taskset", 1, TASKSET_HINT)?;
            with_threads(ctx.cmd("taskset")?, threads)
                .arg(mask)
                .arg(exe)
                .args(argv)
                .run()
        }
        other => ctx.fail(
            1,
            &[
                &format!("--affinity is not supported on {other}"),
                "refusing to run: the measurement would silently be of an unpinned process",
            ],
        ),
    }
}

/// A PowerShell single-quoted string, in which the only escape is a doubled
/// quote.
fn quote(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cargo prints one `Executable` line per built target, and a rebuilt
    /// harness appends another: the last one is the binary that was just built.
    #[test]
    fn harness_path_takes_the_last_executable_line() {
        let log = "   Compiling saci-core v0.1.0\n\
             Executable benches/tpch_q6.rs (target/release/deps/tpch_q6-aaaa.exe)\n\
             Executable benches/tpch_q6.rs (target/release/deps/tpch_q6-bbbb.exe)\n";
        assert_eq!(
            harness_path(log, "tpch_q6"),
            Some("target/release/deps/tpch_q6-bbbb.exe")
        );
    }

    /// A checkout path may itself hold parentheses, so the path is what the
    /// last `(` opens, not the first.
    #[test]
    fn harness_path_reads_the_final_parentheses() {
        let log = "     Executable benches/pipeline.rs (C:\\src (2)\\deps\\pipeline-1.exe)";
        assert_eq!(
            harness_path(log, "pipeline"),
            Some("C:\\src (2)\\deps\\pipeline-1.exe")
        );
    }

    /// Another bench's line is not this bench's harness, and a build that
    /// printed none must not be guessed at.
    #[test]
    fn harness_path_ignores_other_benches() {
        let log = "     Executable benches/tpch_q1.rs (target/release/deps/tpch_q1-1.exe)";
        assert_eq!(harness_path(log, "tpch_q6"), None);
        assert_eq!(harness_path("", "tpch_q6"), None);
    }

    /// PowerShell has one escape inside a single-quoted string.
    #[test]
    fn quote_doubles_single_quotes() {
        assert_eq!(quote("no'match"), "'no''match'");
    }
}
