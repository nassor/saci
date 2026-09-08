use async_trait::async_trait;

use crate::dataset::Dataset;
use crate::error::SaciError;

use super::System;
use super::meta::SystemMeta;

/// Type alias for the closure stored inside [`FnSystem`].
type SystemClosure = dyn Fn(&mut Dataset) -> Result<(), SaciError> + Send + Sync;

/// A [`System`] implementation backed by a closure.
///
/// Created by [`system_fn`], which also supplies `run_sync` so the pipeline
/// skips the async state machine.
pub struct FnSystem {
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
/// #
/// # {
/// use std::sync::Arc;
/// use arrow_schema::{DataType, Field, Schema};
/// use saci_core::component::Component;
/// use saci_core::system::{SystemMeta, system_fn};
/// use saci_core::SaciError;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Count { n: u64 }
/// impl Component for Count {
///     fn name() -> &'static str { "Count" }
///     fn schema() -> Arc<Schema> {
///         Arc::new(Schema::new(vec![Field::new("n", DataType::UInt64, false)]))
///     }
/// }
///
/// let sys = system_fn(
///     SystemMeta::new("noop").read("Count", "n"),
///     |_world| Ok(()),
/// );
/// # }
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

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "runtime")]
    use crate::component::Component;
    #[cfg(feature = "runtime")]
    use crate::dataset::Dataset;
    use crate::retry::SystemConfig;
    #[cfg(feature = "runtime")]
    use arrow_schema::{DataType, Field, Schema};
    #[cfg(feature = "runtime")]
    use serde::{Deserialize, Serialize};
    #[cfg(feature = "runtime")]
    use std::sync::Arc;

    #[cfg(feature = "runtime")]
    #[derive(Serialize, Deserialize)]
    struct Order {
        id: u64,
        total: f64,
    }

    #[cfg(feature = "runtime")]
    impl Component for Order {
        fn name() -> &'static str {
            "Order"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("total", DataType::Float64, false),
            ]))
        }
    }

    #[test]
    fn test_system_fn_preserves_meta() {
        let sys = system_fn(
            SystemMeta::new("my_fn").read("Order", "id"),
            |_world| Ok(()),
        );
        let meta = sys.meta();
        assert_eq!(meta.name, "my_fn");
        assert_eq!(meta.reads.len(), 1);
        assert_eq!(meta.reads[0].field, "id");
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_system_fn_runs_closure() {
        let mut data = Dataset::new();
        data.register_component::<Order>().unwrap();
        let orders: Vec<Order> = (0..3)
            .map(|i| Order {
                id: i,
                total: i as f64,
            })
            .collect();
        data.append::<Order>(&orders).unwrap();

        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter_clone = counter.clone();

        let sys = system_fn(
            SystemMeta::new("counter_fn").read("Order", "id"),
            move |_data| {
                counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            },
        );

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
    fn test_system_fn_default_config() {
        let sys = system_fn(SystemMeta::new("cfg"), |_world| Ok(()));
        assert_eq!(
            sys.config().retry_mode.max_attempts(),
            SystemConfig::default().retry_mode.max_attempts()
        );
    }
}
