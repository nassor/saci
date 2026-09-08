//! # Pipeline Runtime Loader
//!
//! Resolves a [`WasmSpec`] to a [`WasmPipelineRuntime`] at service startup.
//!
//! It reads the `.wasm` bytes through a [`ModuleResolver`], verifies the
//! optional digest before JIT compilation, compiles and instantiates the
//! component via [`WasmPipelineRuntime::from_bytes`], then calls `describe()`
//! eagerly to warm the component-name cache and surface processor errors at
//! startup rather than at the first batch.

use std::collections::HashMap;
use std::path::Path;

use saci_core::SaciResult;
use saci_core::error::SaciError;

use crate::service::config::WasmSpec;
use crate::service::digest::verify_sha3_256;
use crate::wasm::{WasmEngine, WasmPipelineRuntime};

/// Reads raw WASM bytes given a module path string.
///
/// The default implementation ([`LocalModuleResolver`]) reads from the local
/// filesystem. Tests can substitute a resolver that serves fixtures from memory.
pub trait ModuleResolver: Send + Sync {
    /// Return the raw WASM component bytes for `module_path`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if the bytes cannot be read.
    fn resolve(&self, module_path: &str) -> SaciResult<Vec<u8>>;
}

/// Reads WASM bytes from the local filesystem.
pub struct LocalModuleResolver {
    /// Optional base directory. When set, relative `module_path` values are
    /// resolved against it. Absolute paths ignore `base_dir`.
    pub base_dir: Option<std::path::PathBuf>,
}

impl LocalModuleResolver {
    /// Create a resolver with no base directory (paths are used as-is).
    pub fn new() -> Self {
        Self { base_dir: None }
    }

    /// Create a resolver that resolves relative paths under `base_dir`.
    pub fn with_base_dir(base_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            base_dir: Some(base_dir.into()),
        }
    }

    fn full_path(&self, module_path: &str) -> std::path::PathBuf {
        let p = Path::new(module_path);
        if p.is_absolute() {
            p.to_path_buf()
        } else if let Some(ref base) = self.base_dir {
            base.join(p)
        } else {
            p.to_path_buf()
        }
    }
}

impl Default for LocalModuleResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl ModuleResolver for LocalModuleResolver {
    fn resolve(&self, module_path: &str) -> SaciResult<Vec<u8>> {
        let path = self.full_path(module_path);
        std::fs::read(&path).map_err(|e| {
            SaciError::configuration(format!("reading wasm module '{}': {e}", path.display()))
        })
    }
}

/// Loads a [`WasmPipelineRuntime`] from a [`WasmSpec`] using the supplied
/// engine and module resolver.
///
/// ```no_run
/// # #[cfg(all(feature = "service", feature = "wasm"))]
/// # {
/// use saci_service::service::config::WasmSpec;
/// use saci_service::service::loader::{LocalModuleResolver, PipelineRuntimeLoader};
/// use saci_service::wasm::WasmEngine;
///
/// let engine = WasmEngine::new().unwrap();
/// let resolver = LocalModuleResolver::with_base_dir("/opt/saci/pipelines");
/// let loader = PipelineRuntimeLoader::new(engine, resolver);
///
/// // WasmSpec comes from a declared workflow's `wasm` list (deserialized from config).
/// # let spec = WasmSpec { id: "my-pipeline".into(), name: None,
/// #     module: Some("transform.wasm".into()), sha3_256: None,
/// #     config: Default::default(),
/// #     window: None };
/// let runtime = loader.load("my-pipeline", &spec).unwrap();
/// # }
/// ```
pub struct PipelineRuntimeLoader<R = LocalModuleResolver> {
    engine: WasmEngine,
    resolver: R,
    /// Epoch ticks before a single WASM call is interrupted.
    epoch_deadline: u64,
}

impl<R: ModuleResolver> PipelineRuntimeLoader<R> {
    /// Default epoch deadline (100 ticks × 100 ms/tick = 10 s).
    const DEFAULT_EPOCH_DEADLINE: u64 = 100;

    /// Create a loader with the given engine and resolver.
    pub fn new(engine: WasmEngine, resolver: R) -> Self {
        Self {
            engine,
            resolver,
            epoch_deadline: Self::DEFAULT_EPOCH_DEADLINE,
        }
    }

    /// Override the epoch deadline (in ticks) for timeout enforcement.
    pub fn with_epoch_deadline(mut self, ticks: u64) -> Self {
        self.epoch_deadline = ticks;
        self
    }

    /// Resolve, verify, compile, and describe the WASM module.
    ///
    /// The pipeline name identifies the runtime in logs and metrics, and is
    /// usually derived from the `[pipeline]` table in the config.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for IO / digest / compile failures.
    /// Returns [`SaciError::SystemExecution`] if the processor's `describe()` call
    /// fails on the first instantiation.
    pub fn load(&self, pipeline_name: &str, spec: &WasmSpec) -> SaciResult<WasmPipelineRuntime> {
        let module = spec.module.as_deref().ok_or_else(|| {
            SaciError::configuration(format!(
                "wasm node '{pipeline_name}' declares no 'module'; supply a runtime through \
                 ServiceBuilder::with_runtime instead"
            ))
        })?;
        let bytes = self.resolver.resolve(module)?;

        if let Some(ref expected) = spec.sha3_256 {
            verify_sha3_256("wasm module", &bytes, expected)?;
        }

        let config: HashMap<String, String> = spec.config.clone();
        let runtime = WasmPipelineRuntime::from_bytes(
            self.engine.clone(),
            pipeline_name.to_string(),
            &bytes,
            config,
            self.epoch_deadline,
        )?;

        // describe() eagerly, to populate the component-name cache and surface
        // processor errors at startup.
        runtime.describe().map_err(|e| {
            SaciError::configuration(format!("wasm module '{module}' describe() failed: {e}"))
        })?;

        Ok(runtime)
    }
}

#[cfg(all(test, feature = "service", feature = "wasm"))]
mod tests {
    use super::*;

    struct InMemoryResolver {
        bytes: Vec<u8>,
    }
    impl InMemoryResolver {
        fn new(bytes: Vec<u8>) -> Self {
            Self { bytes }
        }
    }
    impl ModuleResolver for InMemoryResolver {
        fn resolve(&self, _module_path: &str) -> SaciResult<Vec<u8>> {
            Ok(self.bytes.clone())
        }
    }

    struct FailingResolver;
    impl ModuleResolver for FailingResolver {
        fn resolve(&self, module_path: &str) -> SaciResult<Vec<u8>> {
            Err(SaciError::configuration(format!(
                "simulated IO failure for {module_path}"
            )))
        }
    }

    #[tokio::test]
    async fn test_io_failure_propagates() {
        let engine = WasmEngine::new().expect("engine");
        let loader = PipelineRuntimeLoader::new(engine, FailingResolver);
        let spec = WasmSpec {
            id: "test".to_string(),
            name: None,
            module: Some("missing.wasm".to_string()),
            sha3_256: None,
            config: HashMap::new(),
            window: None,
        };
        let err = loader.load("test", &spec).err().expect("expected error");
        assert!(
            err.to_string().contains("simulated IO failure"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_no_module_declared_is_a_configuration_error() {
        let engine = WasmEngine::new().expect("engine");
        let loader = PipelineRuntimeLoader::new(engine, FailingResolver);
        let spec = WasmSpec {
            id: "p".to_string(),
            name: None,
            module: None,
            sha3_256: None,
            config: HashMap::new(),
            window: None,
        };
        let err = loader.load("p", &spec).err().expect("expected error");
        assert_eq!(err.category(), "configuration");
        assert!(err.to_string().contains("'p'"), "unexpected error: {err}");
        assert!(
            err.to_string().contains("with_runtime"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_invalid_wasm_bytes_rejected() {
        let engine = WasmEngine::new().expect("engine");
        let loader =
            PipelineRuntimeLoader::new(engine, InMemoryResolver::new(b"not-wasm".to_vec()));
        let spec = WasmSpec {
            id: "test".to_string(),
            name: None,
            module: Some("bad.wasm".to_string()),
            sha3_256: None,
            config: HashMap::new(),
            window: None,
        };
        let err = loader.load("test", &spec).err().expect("expected error");
        assert!(
            err.to_string().contains("wasm compile error") || err.category() == "configuration",
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_local_resolver_missing_file() {
        let resolver = LocalModuleResolver::new();
        let err = resolver
            .resolve("/nonexistent/path/pipeline.wasm")
            .unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.to_string().contains("reading wasm module"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_local_resolver_base_dir() {
        use std::io::Write;
        use tempfile::NamedTempFile;
        let mut f = NamedTempFile::new().expect("tempfile");
        f.write_all(b"dummy").expect("write");
        let dir = f.path().parent().unwrap().to_path_buf();
        let filename = f.path().file_name().unwrap().to_str().unwrap().to_string();

        let resolver = LocalModuleResolver::with_base_dir(&dir);
        let bytes = resolver.resolve(&filename).expect("resolve");
        assert_eq!(bytes, b"dummy");
    }

    #[test]
    fn test_local_resolver_absolute_path_ignores_base_dir() {
        use std::io::Write;
        use tempfile::NamedTempFile;
        let mut f = NamedTempFile::new().expect("tempfile");
        f.write_all(b"abs").expect("write");
        let abs_path = f.path().to_str().unwrap().to_string();

        let resolver = LocalModuleResolver::with_base_dir("/some/other/dir");
        let bytes = resolver.resolve(&abs_path).expect("resolve");
        assert_eq!(bytes, b"abs");
    }
}
