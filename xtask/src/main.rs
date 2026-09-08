//! `cargo xtask`: the repository's build, benchmark and release tasks.
//!
//! One runner, three platforms. Every task here drives a foreign toolchain
//! (Go, .NET, Gradle, npm, wasm-tools, wasm-bindgen, Tailwind)
//! or cargo itself, and cargo is the one prerequisite all of them already have,
//! so a Rust runner asks for nothing the task did not already need. A shell
//! script asks for bash, awk, sed and coreutils, which a Windows machine has
//! none of: `bash` there resolves to the WSL launcher, which fails before
//! reading the first line of the script.
//!
//! Each command's module header documents its steps and its exit codes, so a
//! failure names the missing prerequisite instead of dying inside a compiler.

mod bench;
mod ci;
mod demo;
mod examples;
mod generated;
mod pack;
mod plugins;
mod polyglot;
mod quickstart;
mod sh;
mod ui;
mod validate;
mod wasm;

use std::env;
use std::process::ExitCode;

const USAGE: &str = "\
cargo xtask <command> [options]

Commands
  quickstart                Build the two Quick Start processor components
  polyglot [--only=LIST]    Build the six polyglot processor components
  plugins [--only=LIST]     Build the two native plugin fixtures
  ui                        Rebuild the /ui dashboard bundle
  bench <name> [options]    Run a criterion benchmark through the harness
  pack-sdk                  Pack the five saci-sdk packages
  check-wasm-processor      cargo check saci-core and the facade for wasm32-wasip2
  processor-ipc-roundtrip   Host-to-processor Arrow IPC round-trip gate
  validate [--only=LIST]    Validate every runnable example config
  demo <name>               Build and run an example pipeline

`cargo xtask <command> --help` prints that command's own usage.";

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let Some((command, rest)) = args.split_first() else {
        eprintln!("{USAGE}");
        return ExitCode::from(1);
    };

    // Every task addresses paths from the repository root. `Cmd` sets a child's
    // working directory itself, but a relative path handed *to* a toolchain
    // resolves against this process's, so both have to agree.
    let root = sh::repo_root();
    if let Err(e) = env::set_current_dir(&root) {
        eprintln!("xtask: entering {}: {e}", root.display());
        return ExitCode::from(1);
    }

    let result = match command.as_str() {
        "quickstart" => quickstart::run(rest),
        "polyglot" => polyglot::run(rest),
        "plugins" => plugins::run(rest),
        "ui" => ui::run(rest),
        "bench" => bench::run(rest),
        "pack-sdk" => pack::run(rest),
        "check-wasm-processor" => ci::check_wasm_processor(rest),
        "processor-ipc-roundtrip" => ci::processor_ipc_roundtrip(rest),
        "validate" => validate::run(rest),
        "demo" => demo::run(rest),
        "-h" | "--help" | "help" => {
            println!("{USAGE}");
            Ok(())
        }
        other => {
            eprintln!("xtask: unknown command '{other}'\n\n{USAGE}");
            return ExitCode::from(1);
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{}", e.message);
            ExitCode::from(e.code)
        }
    }
}
