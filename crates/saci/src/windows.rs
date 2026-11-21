//! Flink-style windowed aggregation over columnar Arrow data.
//!
//! Implements tumbling, keyed, session, and sliding windows. It also adds
//! streaming semantics: watermark tracking, allowed lateness, late-data
//! re-firing, and the dropped rows beyond the lateness budget. The window
//! geometry, [`WindowSpec`](crate::window::WindowSpec), lives in
//! [`crate::window`], outside this feature.

#![cfg(feature = "windows")]

mod accumulator;
mod aggregate;
mod function;
mod hash;
mod result;
mod session;
mod sliding;
mod system;
pub(crate) mod time;
mod watermark;
pub use accumulator::{CURRENT_ACCUMULATOR_VERSION, WindowAccumulator, migrate_to_current};
pub use function::{ProcessWindowFn, ReduceAggregate, WindowContext, WindowFunction};
pub use hash::compute_key_hash as key_hash;
pub use result::WindowResults;
pub use system::WindowedSystem;
pub use watermark::{WatermarkState, WindowWatermark};
