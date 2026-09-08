//! Build the two Quick Start processor components into
//! `examples/quickstart/build/`.
//!
//! - `validate-go.wasm`, Go, reads `amount` and writes `valid`
//! - `settle-cs.wasm`, C#, reads `valid` and `amount`, writes `fee` and
//!   `review_tier`
//!
//! The Go stage is `examples/polyglot/stages/go-validate`, reused unmodified: a
//! built component does not care which pipeline loads it, only that the
//! pipeline's declared components match what its `describe()` reports. The C#
//! stage is `examples/quickstart/stages/csharp-settle`, written for this
//! tutorial.
//!
//! This is deliberately not a mode of the `polyglot` task: the six-language
//! example needs Python, TypeScript, Kotlin, Gradle and jco as well, and a
//! tutorial that demands five toolchains to show two stages is not a quick
//! start.
//!
//! Steps:
//!
//! 1. Regenerate `examples/polyglot/generated/` with the `polyglot_schema_emit`
//!    emitter. The C# stage embeds the emitted `SchemaGen.cs`, with the schema
//!    bytes and the fingerprint as constants, so running this first means the
//!    tutorial needs no prior `polyglot` build.
//! 2. Build the Go stage with componentize-go. It is the polyglot stage, which
//!    declares its own `Order` schema through the Go SDK and compiles in nothing
//!    generated.
//! 3. Copy `SchemaGen.cs` into the C# stage and build it with dotnet.
//! 4. Collect both into `examples/quickstart/build/` and validate each one.
//!
//! Toolchain versions are pinned in `examples/polyglot/PINS.md`. Every tool is
//! checked before any work, and the exit codes name which one is missing,
//! matching the `polyglot` task's numbering:
//!
//! | code | prerequisite               |
//! |------|----------------------------|
//! | 3    | wasm-tools                 |
//! | 4    | Go                         |
//! | 5    | componentize-go            |
//! | 8    | build produced no artifact |
//! | 11   | dotnet                     |

use std::path::Path;

use crate::sh::{Ctx, Result};
use crate::{generated, wasm};

const USAGE: &str = "usage: cargo xtask quickstart";

/// Path to the canonical WIT package, relative to a stage directory. Both stage
/// directories sit four levels below the repository root, and a toolchain
/// invoked from inside one resolves the path against that directory.
const WIT_DIR_REL: &str = "../../../../crates/saci-processor/wit";

/// Exit code for a build that produced no artifact.
const NO_ARTIFACT: u8 = 8;

pub fn run(args: &[String]) -> Result<()> {
    let ctx = Ctx::with_pins("quickstart", "examples/polyglot/PINS.md");
    if ctx.no_options(args, USAGE)? {
        return Ok(());
    }

    ctx.log(format!("repo: {}", ctx.root().display()));

    // Both toolchains and the validator first, before the emit and the two
    // builds: a machine missing dotnet would otherwise pay for the Go stage
    // and fail anyway.
    ctx.require("go", 4, "https://go.dev/dl/ (1.25.5 or newer)")?;
    ctx.require(
        "componentize-go",
        5,
        "go install github.com/bytecodealliance/componentize-go@v0.4.1",
    )?;
    ctx.require(
        "dotnet",
        11,
        "https://dotnet.microsoft.com/download/dotnet/10.0 (SDK 10)",
    )?;
    ctx.require("wasm-tools", 3, wasm::WASM_TOOLS_HINT)?;

    generated::emit(
        &ctx,
        "schema constants",
        &["SchemaGen.cs", "order_fingerprint.txt"],
        NO_ARTIFACT,
    )?;

    let go_artifact = build_go(&ctx)?;
    let cs_artifact = build_csharp(&ctx)?;

    let build_dir = ctx.path("examples/quickstart/build");
    ctx.ensure_dir(&build_dir)?;
    collect(&ctx, &go_artifact, &build_dir, "validate-go.wasm")?;
    collect(&ctx, &cs_artifact, &build_dir, "settle-cs.wasm")?;

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
/// everything above that call shells out to Go and dotnet: a test that
/// reached the literal would need both toolchains on `PATH`. Here the
/// spacing is testable on its own.
///
/// `features` comes from the `quickstart` registry entry, so the printed
/// command matches what `build_service` compiled.
fn serve_command_hint(features: &[&str]) -> String {
    format!(
        "      cargo run -p saci-service {}-- serve \
         -c examples/quickstart/quickstart.kdl",
        crate::examples::feature_flag(features)
    )
}

/// The Go stage, built with componentize-go.
fn build_go(ctx: &Ctx) -> Result<std::path::PathBuf> {
    ctx.log("building the Go stage (validate)...");

    let stage = ctx.path("examples/polyglot/stages/go-validate");

    // `bindings` rewrites go.mod to `module wit_component`. That module name is
    // dictated by componentize-go, and it regenerates go.mod from a fixed
    // template, so the SDK dependency (which now also carries the codec) has to
    // be re-declared afterwards.
    ctx.cmd("componentize-go")?
        .dir(&stage)
        .args([
            "-d",
            WIT_DIR_REL,
            "-w",
            "saci-pipeline",
            "bindings",
            "--format",
        ])
        .run()?;
    ctx.cmd("go")?
        .dir(&stage)
        .args([
            "mod",
            "edit",
            "-require=github.com/nassor/saci/packages/saci-sdk-go@v0.0.0",
            "-replace=github.com/nassor/saci/packages/saci-sdk-go=../../../../packages/saci-sdk-go",
        ])
        .run()?;
    ctx.cmd("componentize-go")?
        .dir(&stage)
        .args([
            "-d",
            WIT_DIR_REL,
            "-w",
            "saci-pipeline",
            "build",
            "-o",
            "validate-go.wasm",
        ])
        .run()?;

    Ok(stage.join("validate-go.wasm"))
}

/// The C# stage, built with dotnet.
fn build_csharp(ctx: &Ctx) -> Result<std::path::PathBuf> {
    ctx.log("building the C# stage (settle)...");

    let stage = ctx.path("examples/quickstart/stages/csharp-settle");
    ctx.copy(
        &generated::dir(ctx).join("SchemaGen.cs"),
        &stage.join("SchemaGen.cs"),
    )?;

    // `dotnet build` is the whole story: componentize-dotnet publishes as part
    // of the build and the NativeAOT link step embeds the component type, so
    // the output is already a component. The first run downloads wasi-sdk into
    // ~/.wasi-sdk/, which is slow and about 535 MB.
    ctx.cmd("dotnet")?
        .dir(&stage)
        .args(["build", "-c", "Release", "--nologo"])
        .run()?;

    Ok(stage.join("bin/Release/net10.0/wasi-wasm/publish/settle-cs.wasm"))
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

    /// An entry needing nothing beyond the default bundle prints a runnable
    /// command, not an empty flag.
    ///
    /// `feature_flag` carries its own trailing space, so the template has
    /// none before `--`. Interpolating a bare `join(",")` here instead, which
    /// is what this file did, renders `--features  -- serve`: cargo reads the
    /// `--` as the value of `--features` and the command dies. The quickstart
    /// entry names two connectors today, so nothing else would catch the
    /// regression if it came back.
    #[test]
    fn an_entry_needing_no_extra_features_prints_no_features_flag() {
        let line = serve_command_hint(&[]);
        assert!(!line.contains("--features"), "{line}");
        assert!(
            line.contains("cargo run -p saci-service -- serve"),
            "one space between the package and the separator: {line}"
        );
    }

    /// The real entry's shape: one `--features` with the list, then `--`.
    #[test]
    fn an_entry_with_features_names_them_before_the_separator() {
        let line = serve_command_hint(&["connector-nats", "connector-postgresql"]);
        assert!(
            line.contains(
                "cargo run -p saci-service --features connector-nats,connector-postgresql -- serve"
            ),
            "{line}"
        );
    }
}
