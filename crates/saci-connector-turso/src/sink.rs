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
//!
//! # Failure
//!
//! A failed flush rolls back whole, `truncate_before_first_write`'s `DELETE`
//! included, because that `DELETE` is the first statement of the first flush's
//! transaction. The batch whose [`Sink::write_batch`] call failed is dropped from
//! the buffer: the caller was told it failed and re-drives it (the service's
//! retry wrapper does), and keeping it too would write its rows twice. Rows an
//! earlier call accepted under `chunk_rows`, and everything a failed
//! [`Sink::finish`] held, stay buffered for the next flush. A run that writes no
//! row never flushes and leaves the table alone.
//!
//! # Cancellation
//!
//! A call can be dropped before it returns (a runner `select!`s on a cancel
//! token, a caller wraps it in `timeout`). The `turso` client's futures return
//! `Pending` only while the engine waits on IO, and a `concurrent` flush also
//! yields between conflict retries, so that is where a drop lands. The buffer is
//! only cleared once `COMMIT` returns, so a dropped flush loses no row. `recover`,
//! run first thing in every call, takes a dropped `write_batch`'s own batch back
//! out as if it had failed and rolls back a transaction the dropped flush left
//! open on the connection, which would refuse the next `BEGIN`. A dropped
//! `finish` keeps what it held. No flush has returned, so
//! `truncate_before_first_write` deletes again, in the same transaction as the
//! rows. If the dropped flush did commit, the re-drive writes its rows a second
//! time: harmless for `upsert` and `ignore_conflicts`, and the same duplicate an
//! `append` retry after a lost `COMMIT` makes. `pending_rows` still counts a
//! dropped call's rows until the next call.

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
    /// Rows of the batch the running `write_batch` pushed, set only while its
    /// flush runs. Still set at the next call means that call was dropped.
    own_rows: usize,
    /// Set once a flush committed. It never resets: the one-shot `DELETE` runs
    /// inside the flush's transaction until a commit has carried it.
    first_write_done: bool,
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
            own_rows: 0,
            first_write_done: false,
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

    /// Take back what a call dropped before it returned left behind.
    ///
    /// A dropped `write_batch` never told its caller whether the batch landed;
    /// like a failed one it is taken out of the buffer, because the caller is
    /// expected to re-drive it. A dropped `finish`, and the rows earlier calls
    /// accepted, stay buffered. Nothing marks `first_write_done` for a flush
    /// that did not return, so `truncate_before_first_write` clears the target
    /// again, which is right whether or not the dropped flush committed: the
    /// `DELETE` and the rows share one transaction.
    ///
    /// A flush dropped between `BEGIN` and `COMMIT` leaves its connection inside
    /// the transaction, and the next `BEGIN` would be refused. The engine's own
    /// autocommit flag says so, so the abandoned transaction is rolled back
    /// here, and its half-run `INSERT` is reset first.
    async fn recover(&mut self) -> Result<(), SaciError> {
        if self.own_rows > 0 {
            self.buffered.pop();
            self.buffered_rows -= self.own_rows;
            self.own_rows = 0;
        }
        if let Some(conn) = &self.conn
            && matches!(conn.is_autocommit(), Ok(false))
        {
            if let Some(statement) = &self.statement {
                let _ = statement.reset();
            }
            conn.execute("ROLLBACK", ())
                .await
                .map_err(|e| engine(&self.what(), "rolling back an abandoned transaction", e))?;
        }
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
                    self.first_write_done = true;
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
    ///
    /// Until a flush has committed, the transaction opens with the `DELETE`
    /// `truncate_before_first_write` asks for, so the old rows and the new ones
    /// change together or not at all.
    async fn flush_once(&mut self, what: &str) -> FlushOutcome {
        let fields = &self.config.schema_fields;
        let table = self.config.table.clone();
        let begin = self.config.transaction.begin_sql();
        let truncate = self.config.truncate_before_first_write && !self.first_write_done;
        let conn = self.conn.as_ref().expect("opened");
        if let Err(e) = conn.execute(begin, ()).await {
            return classify(what, "beginning a transaction on", e, &table);
        }
        if truncate {
            let sql = format!("DELETE FROM {table}");
            if let Err(e) = conn.execute(&sql, ()).await {
                let _ = conn.execute("ROLLBACK", ()).await;
                return classify(what, "truncating", e, &table);
            }
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
                    return classify(what, "writing into", e, &table);
                }
            }
        }
        if let Err(e) = conn.execute("COMMIT", ()).await {
            let _ = conn.execute("ROLLBACK", ()).await;
            return classify(what, "committing to", e, &table);
        }
        FlushOutcome::Done
    }
}

#[async_trait]
impl Sink for TursoSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.recover().await?;
        self.ensure_open().await?;
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let rows = batch.num_rows();
        self.buffered_rows += rows;
        self.buffered.push(batch.clone());
        if self.buffered_rows >= self.config.chunk_rows {
            // Dropped inside the flush, this call leaves `own_rows` set, and the
            // next call takes the batch back out (`recover`).
            self.own_rows = rows;
            let flushed = self.flush().await;
            self.own_rows = 0;
            if let Err(e) = flushed {
                // The caller is told this batch failed and re-drives it (the
                // service's retry wrapper does), so it is not kept: holding it
                // too would write its rows twice. Rows an earlier call accepted
                // stay buffered for the next write or `finish`.
                self.buffered.pop();
                self.buffered_rows -= rows;
                return Err(e);
            }
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        self.recover().await?;
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
    let wrapped = SaciError::generic(format!("{what}: {context} '{table}': {error}"));
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Float64Array, Int64Array};
    use arrow_schema::{DataType, Field};
    use saci_connector::from_kdl_str;

    /// A database file with one keyless table, and a sink over it.
    async fn sink_over_new_table(dir: &tempfile::TempDir, extra: &str) -> TursoSink {
        let path = dir
            .path()
            .join("sink.db")
            .to_string_lossy()
            .replace('\\', "/");
        let db = turso::Builder::new_local(&path)
            .build()
            .await
            .expect("opens the database");
        let conn = db.connect().expect("connects");
        conn.execute("CREATE TABLE enriched (id INTEGER, total REAL)", ())
            .await
            .expect("creates the table");
        drop(conn);
        drop(db);

        let kdl = format!(
            "name \"enriched\"\n\
             table \"enriched\"\n\
             {extra}\
             connection path=\"{path}\"\n\
             schema_fields \"id\" type=\"int64\" nullable=#false\n\
             schema_fields \"total\" type=\"float64\" nullable=#true\n"
        );
        let config: TursoSinkConfig =
            serde_json::from_value(from_kdl_str(&kdl).expect("kdl parses")).expect("config");
        TursoSink::new(config).expect("sink builds")
    }

    fn rows(rows: &[(i64, f64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("total", DataType::Float64, true),
        ]));
        let ids: ArrayRef = Arc::new(Int64Array::from(
            rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        ));
        let totals: ArrayRef = Arc::new(Float64Array::from(
            rows.iter().map(|(_, total)| *total).collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, vec![ids, totals]).expect("batch")
    }

    async fn stored(sink: &TursoSink) -> Vec<(i64, f64)> {
        let mut cursor = sink
            .conn
            .as_ref()
            .expect("opened")
            .query("SELECT id, total FROM enriched ORDER BY id", ())
            .await
            .expect("query");
        let mut out = Vec::new();
        while let Some(row) = cursor.next().await.expect("step") {
            let (Ok(turso::Value::Integer(id)), Ok(turso::Value::Real(total))) =
                (row.get_value(0), row.get_value(1))
            else {
                panic!("unexpected column types");
            };
            out.push((id, total));
        }
        out
    }

    /// What a flush dropped after its first `INSERT` leaves on the connection:
    /// an open transaction holding one row.
    async fn leave_a_transaction_open(sink: &mut TursoSink) {
        sink.conn
            .as_ref()
            .expect("opened")
            .execute("BEGIN", ())
            .await
            .expect("begin");
        sink.statement
            .as_mut()
            .expect("prepared")
            .execute(turso::params_from_iter([
                turso::Value::Integer(99),
                turso::Value::Real(9.0),
            ]))
            .await
            .expect("insert");
        assert!(
            !sink
                .conn
                .as_ref()
                .expect("opened")
                .is_autocommit()
                .expect("flag")
        );
    }

    #[tokio::test]
    async fn a_transaction_a_dropped_flush_left_open_is_rolled_back_by_the_next_call() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut sink = sink_over_new_table(&dir, "chunk_rows 2\n").await;
        sink.ensure_open().await.expect("opens");
        leave_a_transaction_open(&mut sink).await;
        // The dropped call's own batch, still buffered with the marker set.
        sink.buffered.push(rows(&[(1, 1.0)]));
        sink.buffered_rows = 1;
        sink.own_rows = 1;

        sink.write_batch(&rows(&[(1, 1.0), (2, 2.0)]))
            .await
            .expect("the re-drive");
        sink.finish().await.expect("nothing left to write");
        assert_eq!(
            stored(&sink).await,
            vec![(1, 1.0), (2, 2.0)],
            "the abandoned row is gone and the dropped call's batch is written once"
        );
    }

    #[tokio::test]
    async fn a_finish_after_a_dropped_flush_writes_what_the_buffer_held() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut sink = sink_over_new_table(&dir, "chunk_rows 100\n").await;
        sink.write_batch(&rows(&[(1, 1.0), (2, 2.0)]))
            .await
            .expect("buffered");
        leave_a_transaction_open(&mut sink).await;

        sink.finish().await.expect("the dropped finish is redone");
        assert_eq!(stored(&sink).await, vec![(1, 1.0), (2, 2.0)]);
    }

    #[tokio::test]
    async fn a_write_dropped_inside_its_flush_is_taken_back_by_the_next_call() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut sink = sink_over_new_table(&dir, "chunk_rows 3\n").await;
        sink.write_batch(&rows(&[(1, 1.0)]))
            .await
            .expect("buffered");
        // The state a `write_batch` dropped inside its flush leaves.
        sink.buffered.push(rows(&[(2, 2.0), (3, 3.0)]));
        sink.buffered_rows += 2;
        sink.own_rows = 2;

        sink.recover().await.expect("recovers");
        assert_eq!(sink.own_rows, 0);
        assert_eq!(sink.buffered.len(), 1, "the dropped call's batch is gone");
        assert_eq!(sink.pending_rows(), Some(1), "the accepted row stays");
    }

    #[tokio::test]
    async fn recovering_a_sink_that_nothing_interrupted_changes_nothing() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut sink = sink_over_new_table(&dir, "").await;
        sink.recover().await.expect("a sink that never opened");
        assert_eq!(sink.pending_rows(), Some(0));

        sink.write_batch(&rows(&[(1, 1.0)]))
            .await
            .expect("buffered");
        sink.recover().await.expect("an idle connection");
        assert_eq!(sink.pending_rows(), Some(1));
        assert!(
            sink.conn
                .as_ref()
                .expect("opened")
                .is_autocommit()
                .expect("flag")
        );
    }
}
