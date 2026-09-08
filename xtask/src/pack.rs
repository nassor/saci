//! Pack the five `saci-sdk` packages into `target/arrow-ipc-dist/`.
//!
//! One package per language, one version. The version lives in `packages/VERSION`
//! and every manifest that carries one must match it:
//!
//! - `packages/saci-sdk-py/pyproject.toml`
//! - `packages/saci-sdk-ts/package.json`
//! - `packages/saci-sdk-kt/build.gradle.kts`
//! - `packages/saci-sdk-kt-ksp/build.gradle.kts`
//! - `packages/saci-sdk-cs/Saci.Sdk.csproj`
//!
//! Go has no manifest version: its version is the git tag
//! `packages/saci-sdk-go/v<version>`, so this task only builds it. The Kotlin
//! stage's two dependency lines also pin the version and are checked too.
//!
//! Artifacts:
//!
//! | file                            | what                                    |
//! |---------------------------------|-----------------------------------------|
//! | `saci_sdk-<v>-py3-none-any.whl` | Python wheel                            |
//! | `saci_sdk-<v>.tar.gz`           | Python sdist                            |
//! | `nassor-saci-sdk-<v>.tgz`       | npm tarball                             |
//! | `Saci.Sdk.<v>.nupkg`            | NuGet package                           |
//! | `saci-sdk-maven-<v>.tar.gz`     | the Pages Maven repository, as of `<v>` |
//!
//! The Kotlin step is the one that writes into the working tree: it publishes
//! into `docs/static/maven/`, which Zola copies verbatim into the built site.
//! Those files are the released Maven repository and are committed.
//!
//! Each check below has its own exit code so a CI failure names the cause:
//!
//! | code | cause                    |
//! |------|--------------------------|
//! | 2    | version mismatch         |
//! | 3    | Go                       |
//! | 4    | Python                   |
//! | 5    | `python -m build` failed |
//! | 6    | Node/npm                 |
//! | 7    | dotnet                   |
//! | 8    | Gradle                   |
//! | 9    | an artifact is missing   |

use std::fs;
use std::path::Path;

use crate::sh::{Ctx, Error, Result};

const USAGE: &str = "usage: cargo xtask pack-sdk";

/// Exit code for an artifact a pack step did not produce.
const MISSING_ARTIFACT: u8 = 9;

pub fn run(args: &[String]) -> Result<()> {
    let ctx = Ctx::with_pins("pack-sdk", "examples/polyglot/PINS.md");
    if ctx.no_options(args, USAGE)? {
        return Ok(());
    }

    let packages = ctx.path("packages");
    let dist = ctx.path("target/arrow-ipc-dist");
    let maven = ctx.path("docs/static/maven");

    let version = ctx.read(&packages.join("VERSION"))?.trim().to_owned();
    ctx.log(format!("version: {version}"));

    assert_versions(&ctx, &version)?;

    // Every toolchain next, before anything is built, packed or deleted: a
    // missing dotnet or gradle is worth naming now rather than after the Go
    // build and the Python and npm packs have already run.
    ctx.require("go", 3, "https://go.dev/dl/ (1.25.5 or newer)")?;
    ctx.require(
        "python",
        4,
        "https://www.python.org/downloads/ (3.10 or newer)",
    )?;
    ctx.require("npm", 6, "ships with Node")?;
    ctx.require(
        "dotnet",
        7,
        "https://dotnet.microsoft.com/download/dotnet/10.0 (SDK 10)",
    )?;
    ctx.require(
        "gradle",
        8,
        "https://gradle.org/install/ (8.14.4 or newer, on JDK 21)",
    )?;

    // Rebuilt from scratch, so the listing at the end is this version's
    // artifacts and not what an earlier run left behind.
    ctx.remove_dir(&dist)?;
    ctx.ensure_dir(&dist)?;

    build_go(&ctx, &packages)?;

    pack_python(&ctx, &packages, &dist)?;
    pack_npm(&ctx, &packages, &dist)?;
    pack_nuget(&ctx, &packages, &dist)?;
    publish_maven(&ctx, &packages, &version)?;

    expect_artifacts(&ctx, &dist, &version)?;

    ctx.log(format!("PASS: artifacts in {}", dist.display()));
    for (name, bytes) in listing(&ctx, &dist)? {
        ctx.log(format!("  {name} ({bytes} bytes)"));
    }
    ctx.log(format!("maven repository: {}", maven.display()));
    ctx.log("commit docs/static/maven/** with the release: Zola copies it verbatim");
    Ok(())
}

/// Refuse to pack when a manifest has drifted from `packages/VERSION`.
///
/// One version, five SDK packages plus the Kotlin stage that consumes two of
/// them. A manifest that drifts ships a package whose coordinate disagrees with
/// the release tag, which no consumer can diagnose. Each format spells the
/// declaration its own way, so the check is the exact substring, not a parse:
/// the spelling is the thing worth pinning.
fn assert_versions(ctx: &Ctx, version: &str) -> Result<()> {
    let declarations = [
        // SDK manifests
        (
            "packages/saci-sdk-py/pyproject.toml",
            format!("version = \"{version}\""),
        ),
        (
            "packages/saci-sdk-ts/package.json",
            format!("\"version\": \"{version}\""),
        ),
        (
            "packages/saci-sdk-kt/build.gradle.kts",
            format!("version = \"{version}\""),
        ),
        (
            "packages/saci-sdk-kt-ksp/build.gradle.kts",
            format!("version = \"{version}\""),
        ),
        (
            "packages/saci-sdk-cs/Saci.Sdk.csproj",
            format!("<Version>{version}</Version>"),
        ),
        // SDK dependency coordinate (KSP -> runtime)
        (
            "packages/saci-sdk-kt-ksp/build.gradle.kts",
            format!("implementation(\"io.github.nassor:saci-sdk-kt:{version}\")"),
        ),
        // Kotlin stage dependency coordinates
        (
            "examples/polyglot/stages/kotlin-fee/build.gradle.kts",
            format!("implementation(\"io.github.nassor:saci-sdk-kt:{version}\")"),
        ),
        (
            "examples/polyglot/stages/kotlin-fee/build.gradle.kts",
            format!("add(\"kspWasmWasi\", \"io.github.nassor:saci-sdk-kt-ksp:{version}\")"),
        ),
    ];
    for (rel, declaration) in &declarations {
        let manifest = ctx.path(rel);
        if !ctx.read(&manifest)?.contains(declaration.as_str()) {
            return ctx.fail(
                2,
                &[
                    &format!("{} does not declare version {version}", manifest.display()),
                    &format!("expected to find: {declaration}"),
                    "packages/VERSION is the source of truth",
                ],
            );
        }
    }
    ctx.log(format!(
        "all {} version declarations match {version}",
        declarations.len()
    ));
    Ok(())
}

/// Compile the Go package. There is nothing to pack: a Go module is consumed
/// from its git tag, so the build is only a check that the tagged tree builds.
fn build_go(ctx: &Ctx, packages: &Path) -> Result<()> {
    ctx.log("building Go package (source-only distribution, nothing to pack)...");
    ctx.cmd("go")?
        .dir(&packages.join("saci-sdk-go"))
        .args(["build", "./..."])
        .run()
}

/// The Python wheel and sdist.
fn pack_python(ctx: &Ctx, packages: &Path, dist: &Path) -> Result<()> {
    ctx.log("packing Python wheel and sdist...");
    let packed = ctx
        .cmd("python")?
        .args(["-m", "build", "--outdir"])
        .arg(dist)
        .arg(packages.join("saci-sdk-py"))
        .status()?;
    if !packed {
        // `build` is a distribution of its own, not part of python, so its
        // absence is the likely cause here and gets its own code and hint.
        return ctx.fail(
            5,
            &["python -m build failed", "install with: pip install build"],
        );
    }
    Ok(())
}

/// The npm tarball.
fn pack_npm(ctx: &Ctx, packages: &Path, dist: &Path) -> Result<()> {
    ctx.log("packing npm tarball...");
    let package = packages.join("saci-sdk-ts");

    // `npm ci` installs exactly what the lockfile pins, which is what a release
    // pack wants, but it refuses to run without one, so a checkout carrying no
    // lockfile gets the resolving install instead.
    let install = if package.join("package-lock.json").is_file() {
        "ci"
    } else {
        "install"
    };
    ctx.cmd("npm")?.dir(&package).arg(install).run()?;
    ctx.cmd("npm")?
        .dir(&package)
        .args(["run", "--silent", "build"])
        .run()?;
    ctx.cmd("npm")?
        .dir(&package)
        .args(["pack", "--pack-destination"])
        .arg(dist)
        .run()
}

/// The NuGet package.
fn pack_nuget(ctx: &Ctx, packages: &Path, dist: &Path) -> Result<()> {
    ctx.log("packing NuGet package...");
    ctx.cmd("dotnet")?
        .arg("pack")
        .arg(packages.join("saci-sdk-cs"))
        .args(["-c", "Release", "-o"])
        .arg(dist)
        .arg("--nologo")
        .run()
}

/// Publish the Kotlin packages into `docs/static/maven/` and archive them.
fn publish_maven(ctx: &Ctx, packages: &Path, version: &str) -> Result<()> {
    ctx.log("publishing the Kotlin packages into docs/static/maven/...");

    // saci-sdk-kt-ksp's build.gradle.kts resolves `io.github.nassor:saci-sdk-kt`
    // from mavenLocal() at configure time (see that file), so this task must
    // populate it itself rather than relying on `cargo xtask polyglot` (which
    // publishes there too, for its own unrelated stage build) having already
    // run in the same environment: `pack-sdk` is run standalone in CI and
    // must be self-sufficient.
    ctx.cmd("gradle")?
        .arg("-p")
        .arg(packages.join("saci-sdk-kt"))
        .args(["--quiet", "--console=plain", "publishToMavenLocal"])
        .run()?;

    for project in ["saci-sdk-kt", "saci-sdk-kt-ksp"] {
        ctx.cmd("gradle")?
            .arg("-p")
            .arg(packages.join(project))
            .args([
                "--quiet",
                "--console=plain",
                "publishAllPublicationsToPagesRepository",
            ])
            .run()?;
    }

    // The Kotlin publications are four Maven modules, `saci-sdk-kt` plus one per
    // target plus the JVM-only KSP processor, and the version list each carries
    // lives in a maven-metadata.xml above the version directory. The asset is
    // therefore the whole repository rather than one version directory:
    // anything less does not resolve.
    //
    // tar stays a subprocess because it ships with Windows 10 and later, macOS
    // and Linux, so calling it costs no dependency. Its paths stay relative: an
    // absolute Windows path's drive letter reads as a remote host. `Cmd` runs at
    // the repository root, which is what they resolve against.
    ctx.cmd("tar")?
        .arg("-czf")
        .arg(format!("target/arrow-ipc-dist/{}", maven_archive(version)))
        .args(["-C", "docs/static", "maven"])
        .run()
}

/// Refuse to report a pass when a step wrote no artifact. A toolchain that
/// succeeds and produces nothing is otherwise a green release with an empty
/// asset list.
fn expect_artifacts(ctx: &Ctx, dist: &Path, version: &str) -> Result<()> {
    for name in [
        format!("saci_sdk-{version}-py3-none-any.whl"),
        format!("saci_sdk-{version}.tar.gz"),
        format!("nassor-saci-sdk-{version}.tgz"),
        format!("Saci.Sdk.{version}.nupkg"),
        maven_archive(version),
    ] {
        let artifact = dist.join(&name);
        if !artifact.is_file() {
            return ctx.fail(
                MISSING_ARTIFACT,
                &[&format!("{} was not produced", artifact.display())],
            );
        }
    }
    Ok(())
}

/// The Maven repository tarball's name, written by one step and checked by
/// another.
fn maven_archive(version: &str) -> String {
    format!("saci-sdk-maven-{version}.tar.gz")
}

/// Every file in `dir`, name and size, ordered by name. A shell glob arrived
/// sorted; a directory read does not, so the order is imposed here.
fn listing(ctx: &Ctx, dir: &Path) -> Result<Vec<(String, u64)>> {
    let entries = fs::read_dir(dir).map_err(|e| read_error(ctx, dir, &e))?;
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| read_error(ctx, dir, &e))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        files.push((name, ctx.size(&path)?));
    }
    files.sort();
    Ok(files)
}

/// A directory read that failed, worded the way `Ctx`'s own file helpers word
/// theirs.
fn read_error(ctx: &Ctx, dir: &Path, e: &std::io::Error) -> Error {
    ctx.error(1, &[&format!("reading {}: {e}", dir.display())])
}
