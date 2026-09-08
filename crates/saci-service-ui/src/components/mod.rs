//! The four tab views and the pieces they share.

mod dlq;
mod flow;
mod graph;
mod legend;
pub mod logs;
mod traces;

pub use dlq::DlqView;
pub use graph::PipelinesView;
pub use legend::Swatch;
pub use logs::LogsView;
pub use traces::TracesView;
