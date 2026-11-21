use async_trait::async_trait;

use crate::dataset::Dataset;
use crate::error::SaciError;

use super::meta::SystemMeta;
use super::write_set::WriteSet;
use super::{ParallelSystem, System};

/// Type alias for the closure stored inside [`FnSystem`].
type SystemClosure = dyn Fn(&mut Dataset) -> Result<(), SaciError> + Send + Sync;

/// Type alias for the closure stored inside [`FnParallelSystem`].
type ParallelClosure = dyn Fn(&Dataset) -> Result<WriteSet, SaciError> + Send + Sync;

/// A [`System`] implementation backed by a closure.
///
/// Created by [`system_fn`], which also supplies `run_sync` so the pipeline
/// skips the async state machine.
struct FnSystem {
    meta: SystemMeta,
    f: Box<SystemClosure>,
}

#[async_trait]
impl System for FnSystem {
    fn meta(&self) -> SystemMeta {
        self.meta.clone()
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        (self.f)(data)
    }

    fn run_sync(&self, data: &mut Dataset) -> Option<Result<(), SaciError>> {
        Some((self.f)(data))
    }
}

/// A [`ParallelSystem`] implementation backed by a closure, created by
/// [`parallel_fn`].
struct FnParallelSystem {
    meta: SystemMeta,
    f: Box<ParallelClosure>,
}

#[async_trait]
impl ParallelSystem for FnParallelSystem {
    fn meta(&self) -> SystemMeta {
        self.meta.clone()
    }

    async fn run(&self, data: &Dataset) -> Result<WriteSet, SaciError> {
        (self.f)(data)
    }

    fn run_sync(&self, data: &Dataset) -> Option<Result<WriteSet, SaciError>> {
        Some((self.f)(data))
    }
}

/// Create a [`System`] from a closure, with no dedicated struct.
///
/// The closure takes `&mut Dataset`, returns `Result<(), SaciError>` and must be
/// `Send + Sync + 'static`. The returned system implements
/// [`System::run_sync`], so the pipeline runs the closure without building an
/// async state machine.
///
/// # Example
///
/// ```rust
/// use saci::{SystemMeta, system_fn};
///
/// let sys = system_fn(SystemMeta::new("noop").reads_named("Count", "n"), |_data| Ok(()));
/// ```
pub fn system_fn(
    meta: SystemMeta,
    f: impl Fn(&mut Dataset) -> Result<(), SaciError> + Send + Sync + 'static,
) -> impl System {
    FnSystem {
        meta,
        f: Box::new(f),
    }
}

/// Create a [`ParallelSystem`] from a closure over an immutable dataset view.
///
/// The returned system implements [`ParallelSystem::run_sync`], so the
/// pipeline calls the closure directly on a blocking thread.
///
/// # Example
///
/// ```rust
/// use saci::{SystemMeta, WriteSet, parallel_fn};
///
/// let sys = parallel_fn(SystemMeta::new("noop"), |_data| Ok(WriteSet::new()));
/// ```
pub fn parallel_fn(
    meta: SystemMeta,
    f: impl Fn(&Dataset) -> Result<WriteSet, SaciError> + Send + Sync + 'static,
) -> impl ParallelSystem {
    FnParallelSystem {
        meta,
        f: Box::new(f),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::RetryMode;

    #[test]
    fn test_system_fn_preserves_meta() {
        let sys = system_fn(
            SystemMeta::new("my_fn").reads_named("Order", "id"),
            |_world| Ok(()),
        );
        let meta = sys.meta();
        assert_eq!(meta.name(), "my_fn");
        assert_eq!(meta.reads.len(), 1);
        assert_eq!(meta.reads[0].field, "id");
    }

    #[cfg(feature = "engine")]
    #[tokio::test]
    async fn test_system_fn_runs_closure() {
        let mut data = Dataset::new();
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter_clone = counter.clone();

        let sys = system_fn(SystemMeta::new("counter_fn"), move |_data| {
            counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        });

        sys.run(&mut data).await.unwrap();
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn test_system_fn_run_sync_returns_some() {
        let mut data = Dataset::new();
        let sys = system_fn(SystemMeta::new("sync_fn"), |_data| Ok(()));
        assert!(matches!(sys.run_sync(&mut data), Some(Ok(()))));
    }

    #[test]
    fn test_system_fn_default_retry_is_none() {
        let sys = system_fn(SystemMeta::new("cfg"), |_world| Ok(()));
        assert!(matches!(sys.retry(), RetryMode::None));
    }

    #[test]
    fn test_parallel_fn_run_sync_returns_the_write_set() {
        let data = Dataset::new();
        let sys = parallel_fn(SystemMeta::new("par"), |_data| Ok(WriteSet::new()));
        assert!(matches!(sys.run_sync(&data), Some(Ok(ws)) if ws.is_empty()));
    }
}
