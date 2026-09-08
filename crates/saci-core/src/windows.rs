//! Flink-style windowed aggregation over columnar Arrow data.
//!
//! Implements tumbling, keyed, session, and sliding windows. It also adds
//! streaming semantics: watermark tracking, allowed lateness, late-data
//! re-firing, and side-output routing for data beyond the lateness budget.

#![cfg(feature = "windows")]

pub mod accumulator;
mod aggregate;
mod builder;
pub mod function;
pub mod hash;
pub mod result;
pub mod session;
mod sliding;
pub mod system;
pub mod time;
pub mod watermark;

pub use accumulator::{CURRENT_ACCUMULATOR_VERSION, WindowAccumulator};
pub use function::{ProcessWindowFn, ReduceAggregate, WindowContext, WindowFunction};
pub use result::{DroppedLate, SideOutput, WindowResults};
pub use system::{WindowedSystem, WindowedSystemBuilder};
pub use watermark::{WatermarkState, WindowWatermark};

/// The geometry enum, whose home is outside this feature: a host parses a
/// `window` declaration in every build, and only the engine below needs the
/// feature. Re-exported here so windowing code reads one path.
pub use crate::window_spec::WindowSpec;
