//! Build the two native plugin fixtures: shared libraries the host loads with
//! dlopen, not WebAssembly components.
//!
//! - `target/debug/<pre>saci_plugin_smoketest<suf>`, Rust, the host test fixture
//! - `examples/plugins/settle-go/<pre>settle_go<suf>`, Go, the cross-language
//!   proof
//!
//! `<pre>` and `<suf>` are the platform's shared library prefix and suffix,
//! read from `std::env::consts::DLL_PREFIX` and `DLL_SUFFIX`, so both artifacts
//! match what a Rust test resolves through those same constants and either path
//! is built the same way.
//!
//! Steps:
//!
//! 1. `cargo build -p saci-plugin-smoketest`.
//! 2. Regenerate `examples/polyglot/generated/` with the `polyglot_schema_emit`
//!    emitter and copy `schema_gen.go` into the Go plugin, rewriting its package
//!    clause. The Go plugin embeds the `Order` schema bytes and fingerprint as
//!    constants, so they must come from that emitter. (The six polyglot stages
//!    derive their own schemas and no longer consume these constants; the Quick
//!    Start and the plugin builds still do.)
//! 3. `go build -buildmode=c-shared` the Go plugin, in place.
//!
//! Toolchain versions are pinned in `examples/plugins/settle-go/PINS.md`. Every
//! check runs before any work and has its own exit code, so a CI failure names
//! the missing tool instead of dying inside a compiler:
//!
//! | code | prerequisite               |
//! |------|----------------------------|
//! | 2    | cargo                      |
//! | 3    | Go                         |
//! | 4    | a C compiler for cgo       |
//! | 5    | build produced no artifact |
//!
//! A contributor with only one toolchain can build one plugin:
//!
//! - `cargo xtask plugins --only=rust`
//! - `cargo xtask plugins --only=go`
//!
//! `--only=go` still needs cargo, because step 2 regenerates the schema
//! constants the Go plugin compiles in.

use std::env::consts::{DLL_PREFIX, DLL_SUFFIX};
use std::path::Path;

use crate::generated;
use crate::sh::{Ctx, Result, which};

const USAGE: &str = "usage: cargo xtask plugins [--only=rust,go]";

/// Exit code for a build that produced no artifact.
const NO_ARTIFACT: u8 = 5;

pub fn run(args: &[String]) -> Result<()> {
    let ctx = Ctx::with_pins("plugins", "examples/plugins/settle-go/PINS.md");

    let mut only = "rust,go";
    for arg in args {
        if let Some(list) = arg.strip_prefix("--only=") {
            only = list;
            continue;
        }
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            other => return ctx.fail(1, &[&format!("unknown argument '{other}'"), USAGE]),
        }
    }
    let build_rust = wants(only, "rust");
    let build_go = wants(only, "go");

    // The platform's shared library naming. Windows is the one target without
    // the `lib` prefix.
    let rust_artifact = ctx
        .path("target/debug")
        .join(format!("{DLL_PREFIX}saci_plugin_smoketest{DLL_SUFFIX}"));
    let go_plugin = ctx.path("examples/plugins/settle-go");
    let go_lib = format!("{DLL_PREFIX}settle_go{DLL_SUFFIX}");
    let go_artifact = go_plugin.join(&go_lib);

    ctx.log(format!("repo: {}", ctx.root().display()));
    ctx.log(format!("plugins: {only}"));

    // Every toolchain check first. Step 2 needs cargo whichever plugin was asked
    // for, so cargo is unconditional.
    ctx.require("cargo", 2, "https://rustup.rs")?;
    if build_go {
        ctx.require("go", 3, "https://go.dev/dl/ (1.25 or newer)")?;
        require_cgo(&ctx)?;
    }

    if build_rust {
        ctx.log("building Rust plugin fixture (saci-plugin-smoketest)...");
        ctx.cargo()?
            .args(["build", "-p", "saci-plugin-smoketest"])
            .run()?;
        artifact(&ctx, &rust_artifact)?;
    }

    generated::emit(
        &ctx,
        "schema constants",
        &["schema_gen.go", "order_fingerprint.txt"],
        NO_ARTIFACT,
    )?;

    if build_go {
        // The generated file names the package the WASM stage's binding
        // directory needs. A c-shared plugin is package main, so the clause is
        // rewritten and nothing else is.
        ctx.log("copying Order schema constants into the Go plugin...");
        let generated = ctx.read(&generated::dir(&ctx).join("schema_gen.go"))?;
        ctx.write(
            &go_plugin.join("schema_gen.go"),
            &as_package_main(&generated),
        )?;

        ctx.log("building Go plugin (settle-go)...");
        ctx.cmd("go")?
            .dir(&go_plugin)
            .env("CGO_ENABLED", "1")
            .args(["build", "-buildmode=c-shared", "-o"])
            .arg(&go_lib)
            .arg(".")
            .run()?;
        artifact(&ctx, &go_artifact)?;
    }

    ctx.log("PASS");
    if build_rust {
        ctx.log(format!("rust: {}", rust_artifact.display()));
    }
    if build_go {
        ctx.log(format!("go:   {}", go_artifact.display()));
    }

    // The smoketest plugin is what `standalone_plugin.kdl` loads, so a Rust
    // build also emits a variables-bearing copy of that config (the registry
    // entry resolves the platform's plugin library path) that needs no env
    // export to validate or run.
    if build_rust {
        let entry = crate::examples::by_name("standalone_plugin").expect("registered");
        let emitted = ctx.path("target/xtask/standalone_plugin.kdl");
        crate::examples::inject(&ctx, entry, &emitted)?;
        ctx.log(format!("emitted {} (self-contained)", emitted.display()));
    }

    Ok(())
}

/// Whether `--only` asked for a plugin, by comma-separated list membership.
fn wants(only: &str, plugin: &str) -> bool {
    only.split(',').any(|item| item == plugin)
}

/// Refuse to start when cgo has no C compiler.
///
/// cgo shells out to the compiler `go env CC` names, and `-buildmode=c-shared`
/// cannot work without it. Checking here turns "gcc: executable file not found"
/// into a named failure, because that message reads like a broken Go toolchain
/// rather than a missing system package.
fn require_cgo(ctx: &Ctx) -> Result<()> {
    // Both streams are captured so a `go env` that fails says nothing on the
    // way out; its output is only trusted when it succeeded.
    let (queried, reported) = ctx.cmd("go")?.args(["env", "CC"]).output_merged()?;
    let named = if queried { reported.trim() } else { "" };
    let cc = if named.is_empty() { "cc" } else { named };
    if which(cc).is_some() {
        return Ok(());
    }

    let missing = format!("no C compiler for cgo: '{cc}' is not on PATH");
    ctx.fail(
        4,
        &[
            &missing,
            "the Go plugin is a cgo shared library and cannot build without one",
            "install with: Linux 'apt install build-essential',",
            "              macOS 'xcode-select --install',",
            "              Windows mingw-w64 via 'winget install -e --id MSYS2.MSYS2'",
            "              then 'pacman -S mingw-w64-x86_64-gcc' and put its bin/ on PATH",
            "see examples/plugins/settle-go/PINS.md for the pinned versions",
        ],
    )
}

/// The emitted `schema_gen.go` with its package clause rewritten to
/// `package main`.
///
/// A line rewrite, not a parse: the plugin compiles the emitted schema bytes
/// and fingerprint in as constants, so every other byte has to survive the
/// copy unchanged.
fn as_package_main(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    for line in source.split_inclusive('\n') {
        if line.starts_with("package ") {
            out.push_str("package main");
            if line.ends_with('\n') {
                out.push('\n');
            }
        } else {
            out.push_str(line);
        }
    }
    out
}

/// Report a built shared library, or refuse to continue when the build left
/// none behind.
fn artifact(ctx: &Ctx, path: &Path) -> Result<()> {
    ctx.expect_artifact(path, NO_ARTIFACT)?;
    let name = match path.file_name() {
        Some(name) => name.to_string_lossy(),
        None => path.as_os_str().to_string_lossy(),
    };
    ctx.log(format!("built {name} ({} bytes)", ctx.size(path)?));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only the package clause changes: the schema bytes and the fingerprint
    /// below it are what the plugin compiles in.
    #[test]
    fn as_package_main_rewrites_only_the_clause() {
        let source = "package export_saci_pipeline_pipeline\n\nconst Fingerprint = \"8c0a76ff\"\n";
        assert_eq!(
            as_package_main(source),
            "package main\n\nconst Fingerprint = \"8c0a76ff\"\n"
        );
    }

    /// The clause is a line, not a word: an identifier that merely starts with
    /// `package` stays put, and a file without a trailing newline keeps that.
    #[test]
    fn as_package_main_matches_whole_lines() {
        assert_eq!(as_package_main("packageName := 1"), "packageName := 1");
        assert_eq!(as_package_main("package x"), "package main");
    }

    /// `--only` is a set of names, so a prefix is not a member.
    #[test]
    fn wants_matches_whole_names() {
        assert!(wants("rust,go", "go"));
        assert!(wants("go", "go"));
        assert!(!wants("rust", "go"));
        assert!(!wants("gopher", "go"));
    }
}
