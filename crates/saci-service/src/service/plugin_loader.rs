//! # Native Plugin Loader
//!
//! Resolves a [`PluginSpec`] to a [`NativePluginRuntime`] at service startup.
//!
//! This loader resolves a path, not bytes. The dynamic linker opens a file by
//! name, so there is no counterpart here to the `ModuleResolver` trait in
//! `service::loader`, which returns bytes so tests can serve a WebAssembly
//! fixture from memory. The library file is read once, and only when the spec
//! pins a `sha3_256` digest.
//!
//! [`NativePluginRuntime::open`] calls the plugin's `describe` eagerly, so a
//! manifest error, a schema fingerprint mismatch, or an ABI version mismatch
//! surfaces at startup rather than at the first batch.

use std::path::{Path, PathBuf};

use saci_core::SaciResult;
use saci_core::error::SaciError;

use crate::plugin::NativePluginRuntime;
use crate::service::config::PluginSpec;
use crate::service::digest::verify_sha3_256;

/// Resolve `library` against `base_dir`.
///
/// An absolute `library` ignores `base_dir`, matching `LocalModuleResolver` on
/// the WebAssembly path.
fn resolve_library_path(library: &str, base_dir: Option<&Path>) -> PathBuf {
    let p = Path::new(library);
    match base_dir {
        Some(base) if !p.is_absolute() => base.join(p),
        _ => p.to_path_buf(),
    }
}

/// Show `path` as an absolute path for an error message.
///
/// Falls back to the path as written when the current directory is
/// unavailable, because a diagnostic must never fail its own formatting.
fn for_display(path: &Path) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Load the shared library named by `spec`.
///
/// The plugin's manifest names the runtime, so this takes no pipeline name,
/// unlike `PipelineRuntimeLoader::load` on the WebAssembly path.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when the resolved library file does not
/// exist, cannot be read, or fails its `sha3_256` digest check. Errors raised
/// while opening the library, reading its manifest, or checking its schema
/// fingerprint propagate from [`NativePluginRuntime::open`].
pub fn load_plugin_runtime(
    spec: &PluginSpec,
    base_dir: Option<&Path>,
) -> SaciResult<NativePluginRuntime> {
    let library = spec.library.as_deref().ok_or_else(|| {
        SaciError::configuration(format!(
            "plugin node '{}' declares no 'library'; supply a runtime through \
             ServiceBuilder::with_runtime instead",
            spec.id
        ))
    })?;
    let path = resolve_library_path(library, base_dir);

    if !path.exists() {
        return Err(SaciError::configuration(format!(
            "plugin library '{}' does not exist",
            for_display(&path).display()
        )));
    }

    if let Some(expected) = spec.sha3_256.as_deref() {
        let bytes = std::fs::read(&path).map_err(|e| {
            SaciError::configuration(format!(
                "reading plugin library '{}': {e}",
                for_display(&path).display()
            ))
        })?;
        let artifact = format!("plugin library '{}'", for_display(&path).display());
        verify_sha3_256(&artifact, &bytes, expected)?;
    }

    NativePluginRuntime::open(&path, spec.config.clone())
}

#[cfg(all(test, feature = "service", feature = "plugin"))]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn spec(library: &str, sha3_256: Option<&str>) -> PluginSpec {
        PluginSpec {
            id: "p".to_string(),
            name: None,
            library: Some(library.to_string()),
            sha3_256: sha3_256.map(str::to_string),
            config: HashMap::new(),
            window: None,
        }
    }

    #[test]
    fn test_no_library_declared_is_a_configuration_error() {
        let missing = PluginSpec {
            id: "p".to_string(),
            name: None,
            library: None,
            sha3_256: None,
            config: HashMap::new(),
            window: None,
        };
        let err = load_plugin_runtime(&missing, None).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.to_string().contains("'p'"), "{err}");
        assert!(err.to_string().contains("with_runtime"), "{err}");
    }

    #[test]
    fn test_relative_library_resolves_against_base_dir() {
        let path = resolve_library_path("libplugin.so", Some(Path::new("/opt/saci/plugins")));
        assert_eq!(path, Path::new("/opt/saci/plugins").join("libplugin.so"));
    }

    #[test]
    fn test_absolute_library_ignores_base_dir() {
        let absolute = std::path::absolute("libplugin.so").expect("absolute");
        let path = resolve_library_path(
            absolute.to_str().expect("utf-8 path"),
            Some(Path::new("/some/other/dir")),
        );
        assert_eq!(path, absolute);
    }

    #[test]
    fn test_missing_library_names_absolute_path() {
        let err = load_plugin_runtime(&spec("no_such_plugin.so", None), None).unwrap_err();
        assert_eq!(err.category(), "configuration");
        let msg = err.to_string();
        assert!(msg.contains("does not exist"), "{msg}");
        let expected = std::path::absolute("no_such_plugin.so").expect("absolute");
        assert!(msg.contains(&expected.display().to_string()), "{msg}");
    }

    #[test]
    fn test_digest_mismatch_names_both_digests() {
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(b"not a shared library").expect("write");
        let path = file.path().to_str().expect("utf-8 path").to_string();

        let err = load_plugin_runtime(&spec(&path, Some("deadbeef")), None).unwrap_err();
        assert_eq!(err.category(), "configuration");
        let msg = err.to_string();
        assert!(msg.contains("SHA3-256 mismatch"), "{msg}");
        assert!(msg.contains("deadbeef"), "{msg}");
        assert!(
            msg.contains(&crate::service::digest::sha3_256_hex(
                b"not a shared library"
            )),
            "{msg}"
        );
    }
}
