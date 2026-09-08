//! Build the saci-service dashboard directly into
//! `crates/saci-service/assets/ui/`.
//!
//! The dashboard is a client-side-rendered Leptos app compiled to
//! `wasm32-unknown-unknown`. `crates/saci-service/src/service/inspector_api.rs`
//! embeds the output with `include_str!`/`include_bytes!`, so the four
//! filenames are a contract:
//!
//! | file             | origin                                       |
//! |------------------|----------------------------------------------|
//! | `index.html`     | hand written, never generated, not touched   |
//! | `app.js`         | wasm-bindgen glue                            |
//! | `app_bg.wasm`    | the module itself                            |
//! | `app.css`        | Tailwind output                              |
//!
//! `crates/saci-service/assets/ui/` is the one committed home for all four,
//! the same way the conformance vectors and the benchmark SVGs are committed,
//! so `cargo build -p saci-service` never needs the wasm toolchain. It lives
//! under `saci-service`'s own directory rather than `saci-service-ui`'s,
//! because `cargo package`/`publish` never includes files outside the
//! package being packaged: an `include_str!` reaching into `saci-service-ui`
//! (itself excluded from the workspace) would silently drop out of a
//! published tarball.
//!
//! `saci-service-ui` is excluded from the workspace (it only builds for
//! `wasm32-unknown-unknown`), so `cargo fmt --all` and
//! `cargo clippy --all-targets` do not reach it. This task runs both gates
//! itself.
//!
//! Steps:
//!
//! 1. fmt and clippy the UI crate.
//! 2. `cargo build --release --target wasm32-unknown-unknown`.
//! 3. wasm-bindgen `--target web`, at the exact version the lock file
//!    resolves, writing straight into `crates/saci-service/assets/ui/`.
//! 4. Tailwind, using the standalone binary (no node anywhere in this repo),
//!    writing `crates/saci-service/assets/ui/app.css` directly.
//!
//! Exit codes name the missing prerequisite:
//!
//! | code | prerequisite                                   |
//! |------|------------------------------------------------|
//! | 2    | `wasm32-unknown-unknown` rustup target         |
//! | 3    | wasm-bindgen-cli                               |
//! | 4    | Tailwind binary                                |
//! | 5    | build produced no artifact                     |
//! | 6    | curl                                           |
//! | 7    | unsupported platform for the Tailwind download |

use std::env;
use std::env::consts::{ARCH, OS};
use std::path::{Path, PathBuf};

use crate::sh::{Ctx, Result, which};

const USAGE: &str = "usage: cargo xtask ui";

/// The UI crate's manifest. Every cargo call here names it: the crate is
/// outside the workspace, so `-p saci-service-ui` does not reach it.
const MANIFEST: &str = "crates/saci-service-ui/Cargo.toml";

/// The only target the dashboard compiles for.
const TARGET: &str = "wasm32-unknown-unknown";

/// The pinned Tailwind release, and where its standalone binaries are
/// published.
const TAILWIND_VERSION: &str = "v4.3.3";
const RELEASES: &str = "https://github.com/tailwindlabs/tailwindcss/releases";

/// Exit code for a step that produced no artifact.
const NO_ARTIFACT: u8 = 5;

pub fn run(args: &[String]) -> Result<()> {
    let ctx = Ctx::new("ui");
    if ctx.no_options(args, USAGE)? {
        return Ok(());
    }

    let ui_dir = ctx.path("crates/saci-service-ui");
    let out = ctx.path("crates/saci-service/assets/ui");

    // The target check comes first: without it the missing prerequisite
    // surfaces as a cargo error about a core library it cannot find. A missing
    // rustup is the same failure, so it carries the same code.
    ctx.require("rustup", 2, "https://rustup.rs")?;
    let (ok, installed) = ctx
        .cmd("rustup")?
        .args(["target", "list", "--installed"])
        .output_status()?;
    if !ok || !installed.lines().any(|line| line.trim() == TARGET) {
        return ctx.fail(
            2,
            &[
                &format!("{TARGET} target missing"),
                &format!("run: rustup target add {TARGET}"),
            ],
        );
    }

    ctx.log("fmt and clippy (the workspace gates do not reach this crate)");
    ctx.cargo()?
        .args(["fmt", "--manifest-path", MANIFEST, "--", "--check"])
        .run()?;
    ctx.cargo()?
        .args([
            "clippy",
            "--manifest-path",
            MANIFEST,
            "--target",
            TARGET,
            "--",
            "-D",
            "warnings",
        ])
        .run()?;

    check_wasm_bindgen(&ctx, &ui_dir)?;

    ctx.log("building the wasm module (release)");
    ctx.cargo()?
        .args([
            "build",
            "--manifest-path",
            MANIFEST,
            "--release",
            "--target",
            TARGET,
        ])
        .run()?;

    let wasm_in = ui_dir.join("target/wasm32-unknown-unknown/release/saci_service_ui.wasm");
    ctx.expect_artifact(&wasm_in, NO_ARTIFACT)?;

    ctx.log("running wasm-bindgen --target web");
    ctx.ensure_dir(&out)?;
    // --no-typescript suppresses app.d.ts and app_bg.wasm.d.ts; --out-name app
    // fixes the two filenames the server embeds. No wasm-opt pass: nothing else
    // in this repository's toolchain installs it, and --target web output is
    // what ships. --out-dir writes straight into the committed vendored
    // location: there is no separate build-scratch directory to copy from.
    ctx.cmd("wasm-bindgen")?
        .arg(&wasm_in)
        .args(["--target", "web", "--no-typescript", "--out-dir"])
        .arg(&out)
        .args(["--out-name", "app"])
        .run()?;

    for name in ["app.js", "app_bg.wasm"] {
        if !out.join(name).is_file() {
            return ctx.fail(
                NO_ARTIFACT,
                &[&format!("wasm-bindgen did not produce assets/ui/{name}")],
            );
        }
    }

    tailwind(&ctx, &ui_dir, &out)?;

    let css = out.join("app.css");
    if !css.is_file() || ctx.size(&css)? == 0 {
        return ctx.fail(
            NO_ARTIFACT,
            &["Tailwind did not produce a non-empty assets/ui/app.css"],
        );
    }

    ctx.log("crates/saci-service/assets/ui/:");
    for name in ["index.html", "app.js", "app_bg.wasm", "app.css"] {
        ctx.log(format!("  {name} ({} bytes)", ctx.size(&out.join(name))?));
    }
    ctx.log("PASS: commit crates/saci-service/assets/ui/");
    Ok(())
}

/// Gate the wasm-bindgen CLI on the version the lock file resolves to.
fn check_wasm_bindgen(ctx: &Ctx, ui_dir: &Path) -> Result<()> {
    // wasm-bindgen-cli must match the wasm-bindgen crate exactly. A mismatch is
    // a hard runtime panic in the browser ("schema versions must exactly
    // match"), not a warning, so the version comes from the lock file rather
    // than a constant here.
    //
    // The lock file is read as committed, with no refresh of its own: `cargo
    // generate-lockfile` ignores the existing lock and re-resolves every
    // dependency to the newest version the local registry index has cached,
    // which silently drags this crate onto a newer wasm-bindgen (and
    // whatever it pulls in) the moment that version exists in a
    // contributor's cache, whether or not anything here actually needs it.
    // The `fmt` and `clippy` steps just above already invoke `cargo` against
    // this manifest, and a plain (non-`--locked`) cargo invocation performs
    // Cargo's ordinary targeted resolution: it keeps every existing pin that
    // still satisfies `Cargo.toml` and only touches entries a real
    // manifest change requires, so by the time this runs the lock already
    // reflects any edit worth reflecting.
    let lock = ui_dir.join("Cargo.lock");
    let text = ctx.read(&lock).unwrap_or_default();
    let Some(resolved) = locked_version(&text, "wasm-bindgen") else {
        return ctx.fail(
            3,
            &[&format!(
                "could not read the resolved wasm-bindgen version from {}",
                lock.display()
            )],
        );
    };
    ctx.log(format!("wasm-bindgen crate resolves to {resolved}"));

    let install = format!("cargo install wasm-bindgen-cli --version {resolved} --locked");
    ctx.require("wasm-bindgen", 3, &install)?;

    let (ok, reported) = ctx.cmd("wasm-bindgen")?.arg("--version").output_status()?;
    if !ok {
        return ctx.fail(
            3,
            &[
                "wasm-bindgen --version failed",
                &format!("run: {install} --force"),
            ],
        );
    }
    // The CLI prints `wasm-bindgen <version>`.
    let have = reported.split_whitespace().nth(1).unwrap_or_default();
    if have != resolved {
        return ctx.fail(
            3,
            &[
                &format!("wasm-bindgen CLI is {have} but the crate resolves to {resolved}"),
                &format!("run: {install} --force"),
            ],
        );
    }
    Ok(())
}

/// The version `name` resolves to in a `Cargo.lock`.
///
/// A `[[package]]` entry writes its `name` before its `version`, so the first
/// `version` line after the matching `name` line belongs to that entry. The
/// dependency lists that also mention the crate are indented, which the exact
/// line match rules out.
fn locked_version(lock: &str, name: &str) -> Option<String> {
    let entry = format!("name = \"{name}\"");
    let mut lines = lock.lines();
    lines.find(|line| *line == entry)?;
    lines
        .find_map(|line| line.strip_prefix("version = \""))
        .and_then(|rest| rest.strip_suffix('"'))
        .map(str::to_owned)
}

/// Compile the stylesheet with the Tailwind standalone binary.
fn tailwind(ctx: &Ctx, ui_dir: &Path, out: &Path) -> Result<()> {
    // Tailwind's standalone binaries need no node. The download is cached in
    // the gitignored .tools/ directory next to the crate.
    let tools = ui_dir.join(".tools");
    // `std::env::consts` replaces `uname -s`-`uname -m`: it reports the OS and
    // architecture this binary was compiled for, so a Windows host cannot
    // present itself as MINGW64_NT, MSYS_NT or a bare Windows_NT depending on
    // which shell asked.
    let (asset, bin) = match (OS, ARCH) {
        ("linux", "x86_64") => ("tailwindcss-linux-x64", "tailwindcss"),
        ("linux", "aarch64") => ("tailwindcss-linux-arm64", "tailwindcss"),
        ("macos", "x86_64") => ("tailwindcss-macos-x64", "tailwindcss"),
        ("macos", "aarch64") => ("tailwindcss-macos-arm64", "tailwindcss"),
        ("windows", "x86_64") => ("tailwindcss-windows-x64.exe", "tailwindcss.exe"),
        _ => {
            return ctx.fail(
                7,
                &[
                    &format!("no Tailwind standalone binary known for {OS}-{ARCH}"),
                    &format!("download one from {RELEASES}/tag/{TAILWIND_VERSION}"),
                    &format!("and put it at {}", tools.join("tailwindcss").display()),
                ],
            );
        }
    };

    let tw = match env::var_os("SACI_TAILWIND").filter(|path| !path.is_empty()) {
        Some(path) => PathBuf::from(path),
        None => tools.join(bin),
    };
    let Some(tw) = tw.to_str().map(str::to_owned) else {
        return ctx.fail(
            1,
            &[&format!("Tailwind path is not UTF-8: {}", tw.display())],
        );
    };

    // `which` is the executable test, the way `[[ -x ]]` was: a cached entry
    // that cannot be run is fetched again rather than handed to the OS.
    if which(&tw).is_none() {
        ctx.ensure_dir(&tools)?;
        let url = format!("{RELEASES}/download/{TAILWIND_VERSION}/{asset}");
        ctx.log(format!("downloading Tailwind {TAILWIND_VERSION} ({asset})"));
        ctx.download(&url, Path::new(&tw), 6, 4)?;
        ctx.make_executable(Path::new(&tw))?;
    }

    ctx.log("running Tailwind");
    let Some(css_out) = out.join("app.css").to_str().map(str::to_owned) else {
        return ctx.fail(1, &["assets/ui/app.css path is not UTF-8"]);
    };
    // --cwd matters: Tailwind's automatic class detection scans from the CLI's
    // working directory, not from the CSS file's directory, so an invocation
    // from the repo root would otherwise scan the whole tree. `-o` is an
    // absolute path since the destination sits outside `ui_dir`.
    ctx.cmd(&tw)?
        .arg("--cwd")
        .arg(ui_dir)
        .args(["-i", "style/input.css", "-o", &css_out, "--minify"])
        .run()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The version belongs to the entry that named the crate, and a dependency
    /// list mentioning it is indented, so an exact line match skips it.
    #[test]
    fn locked_version_reads_the_named_entry() {
        let lock = "\
[[package]]
name = \"leptos\"
version = \"0.8.11\"
dependencies = [
 \"wasm-bindgen\",
]

[[package]]
name = \"wasm-bindgen\"
version = \"0.2.127\"
";
        assert_eq!(
            locked_version(lock, "wasm-bindgen").as_deref(),
            Some("0.2.127")
        );
    }

    /// A longer crate name is a different crate, and an absent one is not a
    /// version to guess at.
    #[test]
    fn locked_version_needs_an_exact_name() {
        let lock = "[[package]]\nname = \"wasm-bindgen-futures\"\nversion = \"0.4.50\"\n";
        assert_eq!(locked_version(lock, "wasm-bindgen"), None);
    }
}
