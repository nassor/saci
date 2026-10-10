//! `saci-connector-clickhouse`: a ClickHouse [`Source`] and [`Sink`] for SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! Both halves are declarative: the Arrow schema is written in the service
//! configuration as `schema_fields`, not introspected at build time, because
//! [`SourceFactory::build`] and [`SinkFactory::build`] are synchronous and
//! open no connection. That keeps `saci-service validate` database-free.
//!
//! [`SourceFactory::build`]: saci_connector::factory::SourceFactory::build
//! [`SinkFactory::build`]: saci_connector::factory::SinkFactory::build
//!
//! # Source modes
//!
//! One `mode` block inside the source's `config` selects the read strategy:
//!
//! - `kind="polling"`: incremental query over a live table, ordered by a cursor
//!   column (`WHERE cursor > last_val ORDER BY cursor LIMIT batch_rows`).
//! - `kind="dump"`: paginated full table scan (`LIMIT batch_rows OFFSET offset`).
//! - `kind="query"`: arbitrary `SELECT` query.
//!
//! ClickHouse returns query results in native `ArrowStream` format, which is
//! decoded directly into Arrow [`RecordBatch`](arrow_array::RecordBatch)es.
//!
//! # Sink write modes
//!
//! `ClickhouseSink` streams Arrow IPC batches via HTTP `INSERT INTO table FORMAT ArrowStream`.
//!
//! - `write_mode="append"`: appends batches directly into the target table.
//! - `truncate_before_first_write=#true`: executes `TRUNCATE TABLE` on the first write.
//!
//! # Delivery semantics
//!
//! At-least-once delivery: queries resume from the last known cursor, and writes
//! stream batches progressively.
//!
//! # Cargo features
//!
//! - `tls` (default): rustls-backed TLS with WebPKI roots.
//! - `tracing`: emit `tracing` spans and events.

#![deny(missing_docs)]

pub mod config;
pub mod connection;
pub mod factory;
pub mod sink;
pub mod source;

pub use config::{
    ClickhouseConnectionConfig, ClickhouseFieldType, ClickhouseSinkConfig, ClickhouseSourceConfig,
    CompressionConfig, DumpMode, FieldSpec, PollingMode, QueryMode, SourceMode, WriteMode,
};
pub use factory::{ClickhouseSinkFactory, ClickhouseSourceFactory};
pub use sink::ClickhouseSink;
pub use source::ClickhouseSource;
