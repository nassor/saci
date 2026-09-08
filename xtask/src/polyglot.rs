//! Build the six polyglot processor components into `examples/polyglot/build/`.
//!
//! One SACI workload, six languages, one WIT world. Each stage is a separate
//! WebAssembly component exporting `saci:pipeline@0.3.0`:
//!
//! | component          | language   | writes                                  |
//! |--------------------|------------|-----------------------------------------|
//! | `validate-go.wasm` | Go         | `valid`                                 |
//! | `enrich-py.wasm`   | Python     | `usd_amount`                            |
//! | `score-ts.wasm`    | TypeScript | `risk_score`, `flagged`                 |
//! | `fee-kt.wasm`      | Kotlin     | `fee`                                   |
//! | `tier-cs.wasm`     | C#         | `review_tier`                           |
//! | `settle-rs.wasm`   | Rust       | `settlement`, plus a cross-batch ledger |
//!
//! Steps:
//!
//! 1. Build each stage with its own toolchain. Each stage declares the `Order`
//!    schema in its own language through its SDK, so there is nothing to emit
//!    and no generated constants to copy in first.
//! 2. Collect the six artifacts into `examples/polyglot/build/`.
//! 3. Validate each one with wasm-tools and confirm it exports
//!    `saci:pipeline@0.3.0`.
//!
//! `--only` narrows step 1 to the stages a contributor has toolchains for. The
//! six independent declarations are held in agreement by the fingerprint check
//! the host runs at load time, not by a shared generated file.
//!
//! Toolchain versions are pinned in `examples/polyglot/PINS.md`. Every tool the
//! requested stages need is checked before any work, so a machine missing one
//! toolchain hears about it in a second rather than after the other stages have
//! compiled. Each check has its own exit code:
//!
//! | code | prerequisite               |
//! |------|----------------------------|
//! | 3    | wasm-tools                 |
//! | 4    | Go                         |
//! | 5    | componentize-go            |
//! | 6    | componentize-py            |
//! | 7    | Node/npm                   |
//! | 8    | build produced no artifact |
//! | 9    | Gradle                     |
//! | 10   | wit-bindgen                |
//! | 11   | dotnet                     |
//! | 12   | curl                       |
//!
//! The Rust stage needs no check of its own: `cargo build --target
//! wasm32-wasip2` is plain cargo, already present by definition.

use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::sh::{Ctx, Result};
use crate::{generated, wasm};

const USAGE: &str = "usage: cargo xtask polyglot [--only=rust,go,python,ts,kotlin,csharp]";

/// What `-h` prints. `--only` is the one flag, and a contributor holding a
/// single toolchain needs its accepted values.
const HELP: &str = "\
usage: cargo xtask polyglot [--only=LIST]

  --only=LIST   Build only the named stages. LIST is a comma-separated subset
                of rust,go,python,ts,kotlin,csharp and defaults to all six, so
                a contributor with one toolchain can build one stage:

                  cargo xtask polyglot --only=rust
                  cargo xtask polyglot --only=go,ts
                  cargo xtask polyglot --only=kotlin,csharp";

/// Path to the canonical WIT package, relative to a stage directory. Every
/// stage sits four levels below the repository root, and a toolchain invoked
/// from inside one resolves the path against that directory.
const WIT_DIR_REL: &str = "../../../../crates/saci-processor/wit";

/// The same WIT package from the repository root, for the tools that run there:
/// wit-bindgen and `wasm-tools component embed`.
const WIT_DIR: &str = "crates/saci-processor/wit";

/// Exit code for a build that produced no artifact.
const NO_ARTIFACT: u8 = 8;

/// The default stage list: all six.
const ALL_STAGES: &str = "rust,go,python,ts,kotlin,csharp";

/// Kotlin's toolchain emits a WASI preview 1 core module, so componentizing it
/// needs the preview 1 adapter that ships with the wasmtime the workspace pins.
const WASMTIME_TAG: &str = "v48.0.1";

const GO_HINT: &str = "https://go.dev/dl/ (1.25.5 or newer)";
const COMPONENTIZE_GO_HINT: &str = "go install github.com/bytecodealliance/componentize-go@v0.4.1";
const COMPONENTIZE_PY_HINT: &str = "pip install componentize-py==0.25.0";
const NODE_HINT: &str = "https://nodejs.org/ (24.12 or newer)";
const GRADLE_HINT: &str = "https://gradle.org/install/ (8.14.4 or newer, on JDK 21)";
const WIT_BINDGEN_HINT: &str =
    "cargo install wit-bindgen-cli --git https://github.com/Kotlin/wit-bindgen --branch kotlin";
const DOTNET_HINT: &str = "https://dotnet.microsoft.com/download/dotnet/10.0 (SDK 10)";

pub fn run(args: &[String]) -> Result<()> {
    let ctx = Ctx::with_pins("polyglot", "examples/polyglot/PINS.md");

    let mut only = ALL_STAGES;
    for arg in args {
        if let Some(list) = arg.strip_prefix("--only=") {
            only = list;
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

    ctx.log(format!("repo: {}", ctx.root().display()));
    ctx.log(format!("stages: {only}"));

    require_toolchains(&ctx, only)?;

    // Each stage hands back the path it built and nothing is copied until all of
    // them are through, so a missing toolchain or a failed compile leaves the
    // build directory holding whatever last succeeded rather than a half-built
    // set of six.
    let rust = wants(only, "rust").then(|| build_rust(&ctx)).transpose()?;
    let go = wants(only, "go").then(|| build_go(&ctx)).transpose()?;
    let python = wants(only, "python")
        .then(|| build_python(&ctx))
        .transpose()?;
    let ts = wants(only, "ts").then(|| build_ts(&ctx)).transpose()?;
    let kotlin = wants(only, "kotlin")
        .then(|| build_kotlin(&ctx))
        .transpose()?;
    let csharp = wants(only, "csharp")
        .then(|| build_csharp(&ctx))
        .transpose()?;

    let build_dir = ctx.path("examples/polyglot/build");
    ctx.ensure_dir(&build_dir)?;
    for (artifact, name) in [
        (go, "validate-go.wasm"),
        (python, "enrich-py.wasm"),
        (ts, "score-ts.wasm"),
        (kotlin, "fee-kt.wasm"),
        (csharp, "tier-cs.wasm"),
        (rust, "settle-rs.wasm"),
    ] {
        if let Some(path) = artifact {
            collect(&ctx, &path, &build_dir, name)?;
        }
    }

    wasm::validate_dir(&ctx, &build_dir, NO_ARTIFACT)?;

    ctx.log(format!("PASS: components in {}", build_dir.display()));

    // The Python stage is what `standalone_polyglot.kdl` loads, so the build
    // also emits a variables-bearing copy of that config that needs no env
    // export to validate or run.
    let entry = crate::examples::by_name("standalone_polyglot").expect("registered");
    let emitted = ctx.path("target/xtask/standalone_polyglot.kdl");
    crate::examples::inject(&ctx, entry, &emitted)?;
    ctx.log(format!("emitted {} (self-contained)", emitted.display()));

    ctx.log("next: cargo run -p saci-service --features wasm,tracing --example polyglot_orders");
    Ok(())
}

/// Whether `--only` asked for a stage, by comma-separated list membership.
fn wants(only: &str, stage: &str) -> bool {
    only.split(',').any(|name| name == stage)
}

/// Every prerequisite the requested stages need, checked before any work.
///
/// A stage that checks its own toolchain does it after the earlier stages have
/// already compiled, and Kotlin is fifth of six: a machine without Gradle would
/// otherwise pay for the Rust, Go, Python and TypeScript builds and fail
/// anyway. wasm-tools is unconditional, because every run validates what it
/// collected.
fn require_toolchains(ctx: &Ctx, only: &str) -> Result<()> {
    ctx.require("wasm-tools", 3, wasm::WASM_TOOLS_HINT)?;
    // No check for the Rust stage: it is plain `cargo build`.
    if wants(only, "go") {
        ctx.require("go", 4, GO_HINT)?;
        ctx.require("componentize-go", 5, COMPONENTIZE_GO_HINT)?;
    }
    if wants(only, "python") {
        ctx.require("componentize-py", 6, COMPONENTIZE_PY_HINT)?;
    }
    if wants(only, "ts") {
        ctx.require("node", 7, NODE_HINT)?;
        ctx.require("npm", 7, "ships with Node")?;
    }
    if wants(only, "kotlin") {
        ctx.require("gradle", 9, GRADLE_HINT)?;
        ctx.require("wit-bindgen", 10, WIT_BINDGEN_HINT)?;
        // curl is the one prerequisite that depends on state: it is needed only
        // when the cached adapter is absent.
        if !wasi_adapter(ctx).is_file() {
            ctx.require("curl", 12, Ctx::CURL_HINT)?;
        }
    }
    if wants(only, "csharp") {
        ctx.require("dotnet", 11, DOTNET_HINT)?;
    }
    Ok(())
}

/// The Rust stage. Plain `cargo build`: `rustc` links a `wasm32-wasip2` cdylib
/// straight into a Component Model component, so no componentizer is involved.
fn build_rust(ctx: &Ctx) -> Result<PathBuf> {
    ctx.log("building Rust stage (settle)...");
    ctx.cargo()?
        .args([
            "build",
            "--release",
            "-p",
            "polyglot-settle-wasm",
            "--target",
            "wasm32-wasip2",
        ])
        .run()?;

    Ok(ctx.path("target/wasm32-wasip2/release/polyglot_settle_wasm.wasm"))
}

/// The Go stage, built with componentize-go.
fn build_go(ctx: &Ctx) -> Result<PathBuf> {
    ctx.log("building Go stage (validate)...");

    let stage = ctx.path("examples/polyglot/stages/go-validate");

    // `bindings` rewrites go.mod to `module wit_component`. That module name is
    // dictated by componentize-go.
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
    // `bindings` regenerates go.mod from a fixed template, `module wit_component`
    // plus one require, and drops everything else. `build` never touches the
    // file, so the SDK dependency (which now also carries the codec) is
    // re-declared here.
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

/// The Python stage, built with componentize-py.
fn build_python(ctx: &Ctx) -> Result<PathBuf> {
    ctx.log("building Python stage (enrich)...");

    let stage = ctx.path("examples/polyglot/stages/python-enrich");

    // No `bindings` call here. Its output is type-checker stubs for editors:
    // `componentize` regenerates the real bindings itself and never reads them
    // off disk. It is also not idempotent, so a second run fails with "Cannot
    // create a file when that file already exists". See PINS.md for generating
    // the stubs by hand.
    //
    // No --stub-wasi: the bundled CPython needs the real WASI imports, which the
    // host supplies via wasmtime_wasi::p2::add_to_linker_sync. `-p` names every
    // directory componentize-py resolves imports from, and it defaults to `.`,
    // so the SDK's `src` is the one directory named here. Resolution happens
    // once, during the pre-init snapshot: the component has no runtime
    // filesystem, so the path cannot be supplied later.
    ctx.cmd("componentize-py")?
        .dir(&stage)
        .args([
            "-d",
            WIT_DIR_REL,
            "-w",
            "saci-pipeline",
            "componentize",
            "app",
            "-p",
            ".",
        ])
        .arg("-p")
        .arg(ctx.path("packages/saci-sdk-py/src"))
        .args(["-o", "enrich-py.wasm"])
        .run()?;

    Ok(stage.join("enrich-py.wasm"))
}

/// The TypeScript stage, built with jco.
fn build_ts(ctx: &Ctx) -> Result<PathBuf> {
    ctx.log("building TypeScript stage (score)...");

    let stage = ctx.path("examples/polyglot/stages/ts-score");

    // The stage links the SDK with `file:`, so `dist/` has to exist before
    // `npm ci` resolves it, and jco bundles the codec through the SDK's
    // realpath.
    let sdk = ctx.path("packages/saci-sdk-ts");
    npm_install(ctx, &sdk)?;
    ctx.cmd("npm")?
        .dir(&sdk)
        .args(["run", "--silent", "build"])
        .run()?;

    npm_install(ctx, &stage)?;
    // jco writes the WIT world's TypeScript declarations that score.ts and
    // wit.d.ts type themselves against, then type-checks before emitting.
    // componentize itself never reads a type: it strips them.
    ctx.cmd("npm")?
        .dir(&stage)
        .args(["run", "--silent", "typecheck"])
        .run()?;

    // `jco componentize` bundles a TypeScript entrypoint on its own, so
    // `--bundle` is implied: StarlingMonkey's loader cannot resolve the
    // `@nassor/saci-sdk` import at wizer time.
    //
    // `--disable fetch-event` on top of `--disable http` is what drops the
    // `wasi:http/types` import. Without it the component fails to instantiate
    // against the host, which links WASI but not wasi:http: "component imports
    // instance `wasi:http/types@0.2.10`, but a matching implementation was not
    // found in the linker".
    //
    // Clocks, random and stdio stay enabled (the default): disabling clocks
    // makes Date.now() return garbage, and this stage reports timing in
    // run-metrics.
    ctx.cmd("npx")?
        .dir(&stage)
        .args([
            "jco",
            "componentize",
            "score.ts",
            "--wit",
            WIT_DIR_REL,
            "--world-name",
            "saci-pipeline",
            "--disable",
            "http",
            "--disable",
            "fetch-event",
            "-o",
            "score-ts.wasm",
        ])
        .run()?;

    Ok(stage.join("score-ts.wasm"))
}

/// The Kotlin stage, built with wit-bindgen, Gradle and wasm-tools.
fn build_kotlin(ctx: &Ctx) -> Result<PathBuf> {
    // Fetched once into the gitignored generated/ directory, so a checkout
    // carries no prebuilt binary.
    let adapter = wasi_adapter(ctx);
    if !adapter.is_file() {
        ctx.log(format!(
            "fetching the wasmtime {WASMTIME_TAG} WASI preview 1 reactor adapter..."
        ));
        let url = format!(
            "https://github.com/bytecodealliance/wasmtime/releases/download/\
             {WASMTIME_TAG}/wasi_snapshot_preview1.reactor.wasm"
        );
        ctx.download(&url, &adapter, 12, 12)?;
    }

    ctx.log("building Kotlin stage (fee)...");
    let stage = ctx.path("examples/polyglot/stages/kotlin-fee");

    // The stage resolves `io.github.nassor:saci-sdk-kt` and its KSP processor
    // from mavenLocal(), so both have to be published there before Gradle
    // configures the stage.
    ctx.cmd("gradle")?
        .dir(&ctx.path("packages/saci-sdk-kt"))
        .args(["--quiet", "--console=plain", "publishToMavenLocal"])
        .run()?;
    ctx.cmd("gradle")?
        .dir(&ctx.path("packages/saci-sdk-kt-ksp"))
        .args(["--quiet", "--console=plain", "publishToMavenLocal"])
        .run()?;

    // Kotlin has no Gradle-native WIT step and no Gradle-native componentizer,
    // so three tools run around the compile:
    //   1. JetBrains' wit-bindgen fork writes the bindings. `--kotlin-imports`
    //      names the package the generated trampoline resolves `PipelineImpl`
    //      from, which is why the processor object lives in `impl`.
    //   2. Gradle produces a core wasm module, not a component.
    //   3. `component embed` attaches the world's component type and
    //      `component new` wraps the module, with the preview 1 adapter covering
    //      the clock and random imports the Kotlin runtime links against.
    let wit_dir = ctx.path(WIT_DIR);
    ctx.cmd("wit-bindgen")?
        .args(["kotlin", "--kotlin-imports", "impl.*"])
        .arg(&wit_dir)
        .arg("--out-dir")
        .arg(stage.join("src/wasmWasiMain/kotlin/bindings"))
        .run()?;
    ctx.cmd("gradle")?
        .dir(&stage)
        .args([
            "--quiet",
            "--console=plain",
            "compileProductionExecutableKotlinWasmWasiOptimize",
        ])
        .run()?;

    let out = stage.join("build/compileSync/wasmWasi/main/productionExecutable/optimized");
    ctx.cmd("wasm-tools")?
        .args(["component", "embed"])
        .arg(&wit_dir)
        .arg(out.join("fee-kt.wasm"))
        .arg("-o")
        .arg(out.join("fee-kt-embedded.wasm"))
        .run()?;

    let mut adapt = OsString::from("wasi_snapshot_preview1=");
    adapt.push(&adapter);
    let artifact = stage.join("fee-kt.wasm");
    ctx.cmd("wasm-tools")?
        .args(["component", "new"])
        .arg(out.join("fee-kt-embedded.wasm"))
        .arg("--adapt")
        .arg(adapt)
        .arg("-o")
        .arg(&artifact)
        .run()?;

    Ok(artifact)
}

/// The C# stage, built with dotnet.
fn build_csharp(ctx: &Ctx) -> Result<PathBuf> {
    ctx.log("building C# stage (tier)...");

    let stage = ctx.path("examples/polyglot/stages/csharp-tier");

    // `dotnet build` is the whole story: componentize-dotnet publishes as part
    // of the build and the NativeAOT link step embeds the component type, so
    // the output is already a component. The first run downloads wasi-sdk into
    // ~/.wasi-sdk/, which is slow and about 535 MB.
    ctx.cmd("dotnet")?
        .dir(&stage)
        .args(["build", "-c", "Release", "--nologo"])
        .run()?;

    Ok(stage.join("bin/Release/net10.0/wasi-wasm/publish/tier-cs.wasm"))
}

/// The WASI preview 1 reactor adapter, with `SACI_WASI_ADAPTER` overriding where
/// it is read from.
///
/// The override exists for an offline or air-gapped build, which supplies its
/// own copy instead of the one the Kotlin stage downloads.
fn wasi_adapter(ctx: &Ctx) -> PathBuf {
    match env::var_os("SACI_WASI_ADAPTER") {
        Some(path) if !path.is_empty() => PathBuf::from(path),
        _ => generated::dir(ctx).join("wasi_snapshot_preview1.reactor.wasm"),
    }
}

/// Install a package's dependencies, reproducibly when a lockfile is present.
///
/// `npm` is a `.cmd` shim on Windows, which [`Ctx::cmd`] resolves to its full
/// name so the child process can be created at all.
fn npm_install(ctx: &Ctx, dir: &Path) -> Result<()> {
    let mode = if dir.join("package-lock.json").is_file() {
        "ci"
    } else {
        "install"
    };
    ctx.cmd("npm")?.dir(dir).arg(mode).run()
}

/// Copy a built component into the build directory under its published name.
fn collect(ctx: &Ctx, from: &Path, build_dir: &Path, name: &str) -> Result<()> {
    ctx.expect_artifact(from, NO_ARTIFACT)?;
    let to = build_dir.join(name);
    ctx.copy(from, &to)?;
    ctx.log(format!("collected {name} ({} bytes)", ctx.size(&to)?));
    Ok(())
}
