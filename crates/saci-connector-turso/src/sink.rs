//! [`TursoSink`]: one prepared `INSERT` per target, one transaction per flush.
//!
//! [`TursoSink::new`] opens no connection, mirroring
//! [`TursoSource::new`](crate::source::TursoSource::new): the first
//! [`Sink::write_batch`] opens the database, connects, optionally turns MVCC on
//! or enables change capture, verifies the target table's real columns against
//! the declared schema, and prepares the insert.
//!
//! Rows are buffered as received batches and flushed in one transaction once
//! `chunk_rows` rows are pending, so a pipeline iteration lands atomically
//! downstream.
//!
//! # Write modes
//!
//! `write_mode = "append"` inserts directly; `"upsert"` and
//! `"ignore_conflicts"` add `ON CONFLICT (…)`, whose target SQLite resolves when
//! the statement is prepared. A `conflict_columns` set with no matching
//! `PRIMARY KEY` or `UNIQUE` constraint is therefore refused as the sink
//! connects, naming the columns.
//!
//! # Transaction modes
//!
//! `transaction = "deferred"` (the default) and `"immediate"` map to the two
//! ordinary `BEGIN` forms. `"concurrent"` is the MVCC path: the sink sets
//! `journal_mode = 'mvcc'` on its connection and flushes between
//! `BEGIN CONCURRENT` and `COMMIT`, so several connections write at once, and a
//! write-write conflict is retried up to `conflict_retries` times. The engine
//! makes MVCC and change capture mutually exclusive, so a `capture` block and
//! `transaction "concurrent"` cannot be combined.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;

use crate::config::{TransactionMode, TursoSinkConfig, WriteMode};
use crate::connection::Db;
use crate::metrics::Instruments;
use crate::types::encode_value;

/// The engine's change-capture pragma.
const CDC_PRAGMA: &str = "capture_data_changes_conn";

/// A Turso [`Sink`].
pub struct TursoSink {
    config: TursoSinkConfig,
    schema: Arc<Schema>,
    db: Option<Db>,
    conn: Option<turso::Connection>,
    statement: Option<turso::Statement>,
    buffered: Vec<RecordBatch>,
    buffered_rows: usize,
    truncated: bool,
    metrics: Instruments,
}

/// The result of one flush attempt.
enum FlushOutcome {
    /// Every row committed.
    Done,
    /// A write-write conflict; the whole flush may be retried.
    Conflict(SaciError),
    /// Anything else, including an encode failure.
    Fatal(SaciError),
}

impl TursoSink {
    /// Validate `config` and build the Arrow schema. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for any invalid key.
    pub fn new(config: TursoSinkConfig) -> Result<Self, SaciError> {
        config.validate()?;
        let fields = config
            .schema_fields
            .iter()
            .map(|f| f.to_arrow_field())
            .collect::<Result<Vec<_>, _>>()?;
        let schema = Arc::new(Schema::new(fields));
        let metrics = Instruments::sink(&config.name);
        Ok(Self {
            config,
            schema,
            db: None,
            conn: None,
            statement: None,
            buffered: Vec::new(),
            buffered_rows: 0,
            truncated: false,
            metrics,
        })
    }

    /// The connector's name, for error prefixes.
    fn what(&self) -> String {
        format!("sink '{}'", self.config.name)
    }

    /// Open the database, connect, set MVCC and capture, verify and prepare,
    /// once.
    async fn ensure_open(&mut self) -> Result<(), SaciError> {
        if self.conn.is_some() {
            return Ok(());
        }
        let what = self.what();
        let db = Db::open(&self.config.connection, &what).await?;
        let conn = db.connect(&self.config.connection, &what).await?;
        if self.config.transaction == TransactionMode::Concurrent {
            conn.pragma_update("journal_mode", "'mvcc'")
                .await
                .map_err(|e| SaciError::generic(format!("{what}: enabling MVCC: {e}")))?;
        }
        if let Some(capture) = &self.config.capture {
            let pragma = match &capture.table {
                Some(table) => {
                    format!("PRAGMA {CDC_PRAGMA}('{},{}')", capture.mode.as_str(), table)
                }
                None => format!("PRAGMA {CDC_PRAGMA}('{}')", capture.mode.as_str()),
            };
            conn.execute(&pragma, ())
                .await
                .map_err(|e| SaciError::generic(format!("{what}: enabling change capture: {e}")))?;
        }
        verify_target(&conn, &self.config, &what).await?;
        let sql = build_insert_sql(&self.config);
        // `ON CONFLICT (…)` names a conflict target, and SQLite resolves it at
        // prepare time: an unmatched target fails here, which is the check that
        // the columns carry a unique index.
        let statement = conn.prepare(&sql).await.map_err(|e| {
            if self.config.write_mode == WriteMode::Append {
                SaciError::generic(format!("{what}: preparing the insert: {e}"))
            } else {
                SaciError::configuration(format!(
                    "{what}: conflict_columns {:?} do not match a PRIMARY KEY or UNIQUE \
                     constraint on '{}': {e}",
                    self.config.conflict_columns, self.config.table
                ))
            }
        })?;
        self.db = Some(db);
        self.conn = Some(conn);
        self.statement = Some(statement);
        Ok(())
    }

    /// Commit the buffered rows, retrying a `concurrent` conflict.
    async fn flush(&mut self) -> Result<(), SaciError> {
        if self.buffered_rows == 0 {
            return Ok(());
        }
        let what = self.what();
        let attempts = if self.config.transaction == TransactionMode::Concurrent {
            self.config.conflict_retries.max(1)
        } else {
            1
        };
        let mut last = None;
        for _ in 0..attempts {
            match self.flush_once(&what).await {
                FlushOutcome::Done => {
                    let rows = self.buffered_rows as u64;
                    self.buffered.clear();
                    self.buffered_rows = 0;
                    self.metrics.sink_flush(rows);
                    return Ok(());
                }
                FlushOutcome::Conflict(error) => {
                    last = Some(error);
                    tokio::task::yield_now().await;
                }
                FlushOutcome::Fatal(error) => return Err(error),
            }
        }
        Err(last.unwrap_or_else(|| {
            SaciError::generic(format!(
                "{what}: concurrent write gave up after {} attempts",
                self.config.conflict_retries.max(1)
            ))
        }))
    }

    /// One transaction's worth of writes, leaving the buffer untouched so the
    /// caller can retry it.
    async fn flush_once(&mut self, what: &str) -> FlushOutcome {
        let fields = &self.config.schema_fields;
        let table = self.config.table.clone();
        let begin = self.config.transaction.begin_sql();
        let conn = self.conn.as_ref().expect("opened");
        if let Err(e) = conn.execute(begin, ()).await {
            return classify(what, "beginning a transaction", e, &table);
        }
        let statement = self.statement.as_mut().expect("prepared");
        for batch in &self.buffered {
            for row in 0..batch.num_rows() {
                let mut values = Vec::with_capacity(fields.len());
                for (idx, field) in fields.iter().enumerate() {
                    match encode_value(field, batch.column(idx), row) {
                        Ok(value) => values.push(value),
                        Err(e) => {
                            let _ = conn.execute("ROLLBACK", ()).await;
                            return FlushOutcome::Fatal(SaciError::generic(format!("{what}: {e}")));
                        }
                    }
                }
                if let Err(e) = statement.execute(turso::params_from_iter(values)).await {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return classify(what, "writing", e, &table);
                }
            }
        }
        if let Err(e) = conn.execute("COMMIT", ()).await {
            let _ = conn.execute("ROLLBACK", ()).await;
            return classify(what, "committing", e, &table);
        }
        FlushOutcome::Done
    }
}

#[async_trait]
impl Sink for TursoSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.ensure_open().await?;
        if self.config.truncate_before_first_write && !self.truncated {
            let what = self.what();
            let sql = format!("DELETE FROM {}", self.config.table);
            self.conn
                .as_ref()
                .expect("opened")
                .execute(&sql, ())
                .await
                .map_err(|e| {
                    SaciError::generic(format!("{what}: truncating '{}': {e}", self.config.table))
                })?;
            self.truncated = true;
        }
        if batch.num_rows() > 0 {
            self.buffered_rows += batch.num_rows();
            self.buffered.push(batch.clone());
        }
        if self.buffered_rows >= self.config.chunk_rows {
            self.flush().await?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        self.flush().await?;
        // A sink that received no batch never opened the database, so there is
        // nothing to push.
        if self.config.sync.push_after_write
            && let Some(db) = self.db.as_ref()
        {
            let what = self.what();
            db.push(&self.config.connection, &what).await?;
        }
        Ok(())
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn pending_rows(&self) -> Option<usize> {
        Some(self.buffered_rows)
    }
}

/// Classify an engine error the transaction ran into.
fn classify(what: &str, context: &str, error: turso::Error, table: &str) -> FlushOutcome {
    let conflict = matches!(
        &error,
        turso::Error::Busy(_) | turso::Error::BusySnapshot(_)
    ) || matches!(
        &error,
        turso::Error::Error(message)
            if message.contains("conflict") || message.contains("busy")
    );
    let wrapped = SaciError::generic(format!("{what}: {context} into '{table}': {error}"));
    if conflict {
        FlushOutcome::Conflict(wrapped)
    } else {
        FlushOutcome::Fatal(wrapped)
    }
}

/// Refuse a target the sink cannot write into.
async fn verify_target(
    conn: &turso::Connection,
    config: &TursoSinkConfig,
    what: &str,
) -> Result<(), SaciError> {
    let sql = format!("PRAGMA table_info({})", config.table);
    let mut rows = conn
        .query(&sql, ())
        .await
        .map_err(|e| engine(what, "reading the target's columns", e))?;
    let mut columns = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(|e| engine(what, "reading the target's columns", e))?
    {
        let name = row
            .get_value(1)
            .map_err(|e| engine(what, "reading the target's columns", e))?;
        if let Some(name) = name.as_text() {
            columns.push(name.clone());
        }
    }
    if columns.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: table '{}' has no columns; does it exist?",
            config.table
        )));
    }
    for field in &config.schema_fields {
        if !columns.iter().any(|c| c == &field.name) {
            return Err(SaciError::configuration(format!(
                "{what}: table '{}' has no column '{}'",
                config.table, field.name
            )));
        }
    }
    Ok(())
}

/// The insert statement for the configured write mode.
fn build_insert_sql(config: &TursoSinkConfig) -> String {
    let columns = config
        .schema_fields
        .iter()
        .map(|f| f.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = (1..=config.schema_fields.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        "INSERT INTO {} ({columns}) VALUES ({placeholders})",
        config.table
    );
    match config.write_mode {
        WriteMode::Append => {}
        WriteMode::IgnoreConflicts => {
            sql.push_str(&format!(
                " ON CONFLICT ({}) DO NOTHING",
                config.conflict_columns.join(", ")
            ));
        }
        WriteMode::Upsert => {
            let assignments = config
                .schema_fields
                .iter()
                .filter(|f| !config.conflict_columns.contains(&f.name))
                .map(|f| format!("{} = excluded.{}", f.name, f.name))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(
                " ON CONFLICT ({}) DO UPDATE SET {assignments}",
                config.conflict_columns.join(", ")
            ));
        }
    }
    sql
}

/// Wrap an engine error.
fn engine(what: &str, context: &str, error: turso::Error) -> SaciError {
    SaciError::generic(format!("{what}: {context}: {error}"))
}
