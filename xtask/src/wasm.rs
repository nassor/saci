//! Component validation, shared by the `quickstart` and `polyglot` builds.

use std::path::{Path, PathBuf};

use crate::sh::{Ctx, Result};

/// The interface every processor component must export.
const EXPORT: &str = "saci:pipeline/pipeline@0.3.0";

/// The pinned validator, whose install line is the same wherever it is needed.
pub const WASM_TOOLS_HINT: &str = "cargo install wasm-tools --locked --version 1.246.2";

/// Validate every `.wasm` in `dir` and confirm each one exports the pipeline
/// world.
///
/// A component that validates but exports something else instantiates against
/// nothing: the host resolves the export by name, so this is what turns WIT
/// drift into a build failure rather than a runtime one. `missing` is the exit
/// code for that case; a validator that is absent exits 3.
pub fn validate_dir(ctx: &Ctx, dir: &Path, missing: u8) -> Result<()> {
    ctx.require("wasm-tools", 3, WASM_TOOLS_HINT)?;

    let entries = std::fs::read_dir(dir)
        .map_err(|e| ctx.error(1, &[&format!("reading {}: {e}", dir.display())]))?;
    let mut components: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "wasm"))
        .collect();
    // Directory order is arbitrary, unlike the shell glob this replaces, and a
    // build log that reorders itself between runs is a diff nobody can read.
    components.sort();

    for path in &components {
        let name = path
            .file_name()
            .unwrap_or(path.as_os_str())
            .to_string_lossy()
            .into_owned();

        ctx.cmd("wasm-tools")?
            .args(["validate", "--features", "component-model"])
            .arg(path)
            .run()?;

        let (ok, wit) = ctx
            .cmd("wasm-tools")?
            .args(["component", "wit"])
            .arg(path)
            .output_status()?;
        if !ok || !wit.contains(EXPORT) {
            return ctx.fail(missing, &[&format!("{name} does not export {EXPORT}")]);
        }
        ctx.log(format!("validated {name}: exports {EXPORT}"));
    }
    Ok(())
}
