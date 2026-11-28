//! Relational, analytical, and stream-processing Arrow operators.
//!
//! All operators are Tokio-free so they run identically inside native [`Pipeline`](crate::Pipeline)
//! workflows, WebAssembly (`wasm32-wasip3`) processors, native plugins, and
//! declarative `saci-service` pipelines.

pub mod aggregate;
pub mod deduplicate;
pub mod expr;
pub mod group_by;
pub mod hash;
pub mod join;
pub mod quality;
pub mod route;
pub mod sort;
pub mod sql;
pub mod time;
pub mod unnest;
#[cfg(feature = "windows")]
pub mod window_agg;

pub use aggregate::{AggregateOp, AggregateSpec};
pub use deduplicate::{DeduplicateState, DeduplicateSystem, KeepPolicy};
pub use expr::{
    BinaryOp, Expr, FilterSystem, ProjectItem, ProjectSystem, ScalarFunc, ScalarValue, UnaryOp,
};
pub use group_by::GroupBySystem;
pub use join::{
    IntervalJoinState, IntervalJoinSystem, JoinKey, JoinKind, JoinSystem, LookupJoinState,
    LookupJoinSystem,
};
pub use quality::{
    DataQualityState, DataQualitySystem, QualityFailureAction, QualityRule, QualitySplit,
};
pub use route::{BranchPayloads, BranchRule, RouteMode, RouteSystem};
pub use sort::{SortKey, SortSystem, TopNSystem};
pub use sql::{
    SqlExpr, SqlJoinClause, SqlOrderItem, SqlQuery, SqlSelectItem, SqlSystem, SqlTableRef,
};
pub use unnest::UnnestSystem;
#[cfg(feature = "windows")]
pub use window_agg::{WindowAggregateState, WindowAggregateSystem, WindowColumns};
