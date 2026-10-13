//! [`ClickhouseSink`]: ClickHouse [`Sink`] implementation with Arrow IPC streaming.
//!
//! A flush is one `INSERT` per chunk of `chunk_rows` rows. ClickHouse commits
//! each `INSERT` on its own, so a flush that fails on its third chunk has
//! already written the first two, and `TRUNCATE` is not transactional either.
//! The sections below say how the sink keeps a retry of such a flush from
//! repeating work.
//!
//! # Failure
//!
//! A failed [`Sink::write_batch`] does not keep its own batch. The service's
//! retry wrapper hands the same batch back, and keeping it too would buffer its
//! rows twice. Rows an earlier call accepted under `flush_rows`, and everything a
//! failed [`Sink::finish`] held, stay buffered. The buffer is cleared only once
//! every chunk has been written, so a retry rebuilds the same chunks from the
//! same rows.
//!
//! # Deduplication
//!
//! Chunks the server confirmed are not sent again. A chunk whose outcome is
//! unknown, because its request failed or the sink stopped waiting for it, is
//! sent again under the same `insert_deduplication_token`. A table that
//! deduplicates drops that second copy: a replicated `MergeTree` does by
//! default, and a plain one does once `non_replicated_deduplication_window` is
//! above zero. On any other table the chunk is written twice.
//!
//! The token names this sink, the flush, and the chunk's position and content,
//! so a retry of one chunk matches only itself. Two flushes of identical rows
//! are both kept, where the table's own content-based deduplication would drop
//! the second, and a chunk rebuilt from different rows is never mistaken for the
//! one before it. The sink sets the token on every `INSERT`, replacing one given
//! in `connection.settings`. The server needs to be ClickHouse 22.2 or later,
//! and the table's window has to cover the flushes between the failure and the
//! retry.
//!
//! # Truncate
//!
//! `truncate_before_first_write` runs `TRUNCATE TABLE` at the start of every
//! attempt of the first flush, until one completes. A first flush that failed
//! after its truncate and some chunks leaves them in the table, and the retry
//! clears them and writes the whole flush again. Each attempt starts a new
//! deduplication scope, so state from before the truncate never suppresses the
//! retry. A request given up on at the `query_timeout_ms` bound (see
//! Cancellation) can still commit after the retry's `TRUNCATE`, and that chunk
//! then sits beside the retry's copy. Once the first flush has completed the
//! table is the run's own and is never truncated again. A run that writes no row
//! never flushes and leaves the table alone.
//!
//! # Cancellation
//!
//! A call can be dropped before it returns (a runner `select!`s on a cancel
//! token, a caller wraps it in `timeout`). The buffer is cleared only after the
//! last chunk returns, so a dropped flush loses no row, and `recover`, run first
//! thing in every call, takes a dropped `write_batch`'s own batch back out as if
//! it had failed. A dropped `finish` keeps what it held.
//!
//! A request the dropped call had sent keeps running on the server and may
//! commit. Every request runs in a task the sink owns, and `recover` waits for
//! the one left behind, up to `query_timeout_ms`, before anything else goes out.
//! A chunk that landed is recorded and not sent again, and a `TRUNCATE` cannot
//! run after the rows of the retry. A request still unanswered at the bound is
//! given up on, its outcome stays unknown, and the token above covers it.
//! `pending_rows` still counts a dropped call's rows until the next call.

use std::collections::hash_map::{DefaultHasher, RandomState};
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_cast::CastOptions;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;
use async_trait::async_trait;
use saci_core::error::SaciError;
use saci_core::io::cast_batch;
use saci_core::io::sink::Sink;
use tokio::task::JoinHandle;

use crate::config::ClickhouseSinkConfig;
use crate::connection::{build_client, quote_qualified};

/// The server setting that names an `INSERT` for deduplication, available from
/// ClickHouse 22.2.
const DEDUP_TOKEN_SETTING: &str = "insert_deduplication_token";

/// A request a flush sent and has not seen answered.
///
/// It runs in a task the sink owns, so dropping the call that awaits it does
/// not stop it, and the next call finds it here.
struct InFlight {
    /// Position and fingerprint of the chunk the request carries, `None` for the
    /// `TRUNCATE`.
    chunk: Option<(usize, u64)>,
    handle: JoinHandle<Result<(), SaciError>>,
}

/// A ClickHouse [`Sink`] bulk-loading batches via native Arrow IPC streaming.
pub struct ClickhouseSink {
    config: ClickhouseSinkConfig,
    schema: Arc<Schema>,
    client: clickhouse::Client,
    buffer: Vec<RecordBatch>,
    buffered_rows: usize,
    /// Rows of the batch the running `write_batch` pushed, set only while its
    /// flush runs. Still set at the next call means that call was dropped.
    own_rows: usize,
    /// Set once a flush completed. `truncate_before_first_write` clears the
    /// target at the start of every flush until then.
    first_write_done: bool,
    /// Tells this sink's deduplication tokens from those of every other sink
    /// and run that wrote the table.
    run: u64,
    /// Tells one flush's tokens from the next. Advances when a flush completes
    /// and when an attempt truncates the target.
    scope: u64,
    /// Fingerprints of the chunks of the flush in progress that the server
    /// confirmed, in chunk order. A retry that rebuilds the same chunks skips
    /// these.
    landed: Vec<u64>,
    /// The request a dropped call left running, until the next call settles it.
    in_flight: Option<InFlight>,
}

impl ClickhouseSink {
    /// Creates a new [`ClickhouseSink`] from configuration.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if configuration validation fails
    /// or Arrow schema cannot be constructed.
    pub fn new(config: ClickhouseSinkConfig) -> Result<Self, SaciError> {
        config.validate()?;

        let fields = config
            .schema_fields
            .iter()
            .map(|f| f.to_arrow_field())
            .collect::<Result<Vec<_>, _>>()?;
        let schema = Arc::new(Schema::new(fields));
        let client = build_client(&config.connection)?;

        Ok(Self {
            config,
            schema,
            client,
            buffer: Vec::new(),
            buffered_rows: 0,
            own_rows: 0,
            first_write_done: false,
            run: new_run_id(),
            scope: 0,
            landed: Vec::new(),
            in_flight: None,
        })
    }

    /// Take back what a call dropped before it returned left behind.
    ///
    /// A dropped `write_batch` never told its caller whether the batch landed;
    /// like a failed one it is taken out of the buffer, because the caller is
    /// expected to re-drive it. A dropped `finish`, and the rows earlier calls
    /// accepted, stay buffered. A dropped first flush leaves `first_write_done`
    /// unset, so the re-drive truncates again, which is right whether or not the
    /// dropped `TRUNCATE` ran.
    ///
    /// The request the dropped call was waiting for is still running on the
    /// server and may commit. It runs in a task the sink owns, so it is waited
    /// for here: that turns an unknown outcome into a known one, and keeps the
    /// request from landing after something the next call sends, a `TRUNCATE`
    /// included. The wait is bounded by `query_timeout_ms`. A request still
    /// unanswered by then is given up on and its outcome stays unknown, which the
    /// deduplication token covers.
    async fn recover(&mut self) {
        if self.own_rows > 0 {
            self.buffer.pop();
            self.buffered_rows -= self.own_rows;
            self.own_rows = 0;
        }
        if self.in_flight.is_none() {
            return;
        }
        let bound = self.config.connection.query_timeout_ms;
        let answered = if bound == 0 {
            // An error only means the chunk is not recorded as landed.
            let _ = self.settle().await;
            true
        } else {
            tokio::time::timeout(Duration::from_millis(bound), self.settle())
                .await
                .is_ok()
        };
        if !answered && let Some(flight) = self.in_flight.take() {
            flight.handle.abort();
        }
    }

    /// Waits for the request in flight, if any, and takes it. A chunk the server
    /// confirmed is recorded as landed.
    ///
    /// Dropping this future leaves the request in place for the next call.
    async fn settle(&mut self) -> Result<(), SaciError> {
        let Some(flight) = self.in_flight.as_mut() else {
            return Ok(());
        };
        let joined = (&mut flight.handle).await;
        // Nothing awaits between the answer and the record, so a drop cannot
        // separate them.
        let taken = self.in_flight.take();
        let result = joined.unwrap_or_else(|e| {
            Err(SaciError::generic(format!(
                "clickhouse sink: request task failed: {e}"
            )))
        });
        if result.is_ok()
            && let Some(InFlight {
                chunk: Some((index, fingerprint)),
                ..
            }) = taken
            && self.landed.len() == index
        {
            self.landed.push(fingerprint);
        }
        result
    }

    /// Starts a deduplication scope with nothing written in it yet.
    fn begin_scope(&mut self) {
        self.scope += 1;
        self.landed.clear();
    }

    /// Flushes any buffered rows to ClickHouse.
    ///
    /// The buffer is cleared only after the last chunk returns, so an error or
    /// a dropped call anywhere before that leaves every row in it.
    async fn flush(&mut self) -> Result<(), SaciError> {
        if self.buffered_rows == 0 {
            return Ok(());
        }

        let quoted_table = quote_qualified(&self.config.table);

        if self.config.truncate_before_first_write && !self.first_write_done {
            // The scope changes before the request goes out: nothing sent
            // before the `TRUNCATE` may count as written or share its tokens
            // afterwards.
            self.begin_scope();
            self.in_flight = Some(InFlight {
                chunk: None,
                handle: spawn_truncate(self.client.clone(), quoted_table.clone()),
            });
            self.settle().await?;
        }

        // One INSERT per chunk, so a large buffer becomes several requests of at
        // most `chunk_rows` rows rather than one unbounded stream.
        let chunks = split_chunks(&self.schema, &self.buffer, self.config.chunk_rows)?;
        let insert_sql = format!("INSERT INTO {quoted_table} FORMAT ArrowStream");
        for (index, chunk) in chunks.iter().enumerate() {
            let encoded = encode_chunk(&self.schema, chunk)?;
            let fingerprint = fingerprint(&encoded);
            // A retry rebuilds the chunks of the failed flush from the same
            // rows, so a chunk the server already confirmed matches what is
            // recorded at its position. Anything after a mismatch belongs to
            // rows that are no longer buffered.
            if self.landed.get(index) == Some(&fingerprint) {
                continue;
            }
            self.landed.truncate(index);

            let token = dedup_token(self.run, self.scope, index, fingerprint);
            self.in_flight = Some(InFlight {
                chunk: Some((index, fingerprint)),
                handle: spawn_insert(self.client.clone(), insert_sql.clone(), token, encoded),
            });
            self.settle().await?;
        }

        self.buffer.clear();
        self.buffered_rows = 0;
        self.first_write_done = true;
        self.begin_scope();
        Ok(())
    }
}

/// Sends `TRUNCATE TABLE` in a task, so dropping the call that awaits it leaves
/// it running.
fn spawn_truncate(client: clickhouse::Client, table: String) -> JoinHandle<Result<(), SaciError>> {
    tokio::spawn(async move {
        client
            .query(&format!("TRUNCATE TABLE {table}"))
            .execute()
            .await
            .map_err(|e| {
                SaciError::generic(format!(
                    "clickhouse sink truncate table '{table}' error: {e}"
                ))
            })
    })
}

/// Sends one chunk as an `INSERT` in a task, so dropping the call that awaits
/// it leaves it running.
fn spawn_insert(
    client: clickhouse::Client,
    sql: String,
    token: String,
    encoded: Vec<u8>,
) -> JoinHandle<Result<(), SaciError>> {
    tokio::spawn(async move {
        let mut insert = client
            .insert_formatted_with(sql)
            .with_setting(DEDUP_TOKEN_SETTING, token);
        insert
            .send(bytes::Bytes::from(encoded))
            .await
            .map_err(|e| SaciError::generic(format!("clickhouse sink: stream send error: {e}")))?;
        insert
            .end()
            .await
            .map_err(|e| SaciError::generic(format!("clickhouse sink: stream commit error: {e}")))
    })
}

/// Encodes one chunk as a complete Arrow IPC stream.
fn encode_chunk(schema: &Arc<Schema>, chunk: &RecordBatch) -> Result<Vec<u8>, SaciError> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema).map_err(|e| {
            SaciError::generic(format!("clickhouse sink: stream writer init error: {e}"))
        })?;
        writer.write(chunk).map_err(|e| {
            SaciError::generic(format!("clickhouse sink: stream encode error: {e}"))
        })?;
        writer.finish().map_err(|e| {
            SaciError::generic(format!("clickhouse sink: stream finish error: {e}"))
        })?;
    }
    Ok(buf)
}

/// A hash of `bytes` that is the same for the same bytes anywhere in this
/// process. Nothing persists it, so the algorithm may change between releases.
fn fingerprint(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(bytes);
    hasher.finish()
}

/// A value no other sink instance shares, drawn from the keys the standard
/// library seeds its hash maps with.
fn new_run_id() -> u64 {
    RandomState::new().build_hasher().finish()
}

/// The deduplication token for chunk `index` of the flush in `scope`.
///
/// The content hash is part of it so that the token can only ever match the
/// same rows: a retry that rebuilt the chunk from different rows gets a new
/// token instead of being dropped as a duplicate.
fn dedup_token(run: u64, scope: u64, index: usize, fingerprint: u64) -> String {
    format!("saci-{run:016x}-{scope}-{index}-{fingerprint:016x}")
}

/// Splits `batches` into chunks of at most `limit` rows, `0` meaning no limit.
///
/// `RecordBatch::slice` shares its arrays with the batch it came from, so a
/// chunk copies no column data; only the final concatenation into one batch per
/// chunk allocates. Every chunk carries `schema`'s row order, so the row order
/// across chunks is the order of `batches`.
fn split_chunks(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
    limit: usize,
) -> Result<Vec<RecordBatch>, SaciError> {
    let mut chunks = Vec::new();
    let mut pending: Vec<RecordBatch> = Vec::new();
    let mut pending_rows = 0usize;

    for batch in batches {
        let mut offset = 0;
        while offset < batch.num_rows() {
            let room = if limit == 0 {
                batch.num_rows() - offset
            } else {
                limit - pending_rows
            };
            let take = room.min(batch.num_rows() - offset);
            pending.push(batch.slice(offset, take));
            pending_rows += take;
            offset += take;

            if limit != 0 && pending_rows == limit {
                chunks.push(concat_batches(schema, &pending).map_err(|e| {
                    SaciError::generic(format!("clickhouse sink: batch concatenation error: {e}"))
                })?);
                pending.clear();
                pending_rows = 0;
            }
        }
    }

    if !pending.is_empty() {
        chunks.push(concat_batches(schema, &pending).map_err(|e| {
            SaciError::generic(format!("clickhouse sink: batch concatenation error: {e}"))
        })?);
    }

    Ok(chunks)
}

#[async_trait]
impl Sink for ClickhouseSink {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.recover().await;
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let casted = cast_batch(batch, &self.schema, &CastOptions::default())?;
        let rows = casted.num_rows();
        self.buffered_rows += rows;
        self.buffer.push(casted);

        if self.config.flush_rows == 0 || self.buffered_rows >= self.config.flush_rows {
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
                self.buffer.pop();
                self.buffered_rows -= rows;
                return Err(e);
            }
        }

        Ok(())
    }

    fn pending_rows(&self) -> Option<usize> {
        Some(self.buffered_rows)
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        self.recover().await;
        self.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClickhouseConnectionConfig, ClickhouseFieldType, FieldSpec, WriteMode};
    use std::collections::HashMap;

    #[tokio::test]
    async fn sink_initialization_and_buffering() {
        let config = ClickhouseSinkConfig {
            name: "out".to_string(),
            connection: ClickhouseConnectionConfig {
                url: "http://localhost:8123".to_string(),
                user: None,
                password: None,
                database: "default".to_string(),
                query_timeout_ms: 30000,
                compression: crate::config::CompressionConfig::Lz4,
                settings: HashMap::new(),
            },
            table: "events".to_string(),
            schema_fields: vec![FieldSpec {
                name: "id".to_string(),
                ty: ClickhouseFieldType::Int64,
                nullable: false,
                precision: None,
                scale: None,
            }],
            write_mode: WriteMode::Append,
            chunk_rows: 1000,
            flush_rows: 50,
            truncate_before_first_write: false,
        };

        let mut sink = ClickhouseSink::new(config).expect("creates sink");
        assert_eq!(sink.pending_rows(), Some(0));
        let schema = sink.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");

        // An empty batch writes nothing
        let empty = RecordBatch::new_empty(Arc::clone(&schema));
        sink.write_batch(&empty)
            .await
            .expect("empty batch writes ok");
        assert_eq!(sink.pending_rows(), Some(0));
    }

    #[tokio::test]
    async fn sink_buffers_rows_up_to_flush_rows() {
        use arrow_array::Int64Array;
        let config = ClickhouseSinkConfig {
            name: "out".to_string(),
            connection: ClickhouseConnectionConfig {
                url: "http://localhost:8123".to_string(),
                user: None,
                password: None,
                database: "default".to_string(),
                query_timeout_ms: 30000,
                compression: crate::config::CompressionConfig::None,
                settings: HashMap::new(),
            },
            table: "events".to_string(),
            schema_fields: vec![FieldSpec {
                name: "id".to_string(),
                ty: ClickhouseFieldType::Int64,
                nullable: false,
                precision: None,
                scale: None,
            }],
            write_mode: WriteMode::Append,
            chunk_rows: 1000,
            flush_rows: 50,
            truncate_before_first_write: false,
        };

        let mut sink = ClickhouseSink::new(config).expect("creates sink");
        let schema = sink.schema();

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .expect("batch1");
        sink.write_batch(&batch1).await.expect("writes batch1");
        assert_eq!(sink.pending_rows(), Some(5));

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![6, 7, 8]))],
        )
        .expect("batch2");
        sink.write_batch(&batch2).await.expect("writes batch2");
        assert_eq!(sink.pending_rows(), Some(8));
    }

    #[tokio::test]
    async fn sink_casts_compatible_types_in_batch() {
        use arrow_array::Int32Array;
        use arrow_schema::{DataType, Field};

        let config = ClickhouseSinkConfig {
            name: "out".to_string(),
            connection: ClickhouseConnectionConfig {
                url: "http://localhost:8123".to_string(),
                user: None,
                password: None,
                database: "default".to_string(),
                query_timeout_ms: 30000,
                compression: crate::config::CompressionConfig::None,
                settings: HashMap::new(),
            },
            table: "events".to_string(),
            schema_fields: vec![FieldSpec {
                name: "id".to_string(),
                ty: ClickhouseFieldType::Int64,
                nullable: false,
                precision: None,
                scale: None,
            }],
            write_mode: WriteMode::Append,
            chunk_rows: 1000,
            flush_rows: 100,
            truncate_before_first_write: false,
        };

        let mut sink = ClickhouseSink::new(config).expect("creates sink");
        let int32_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            int32_schema,
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )
        .expect("int32 batch");

        sink.write_batch(&batch)
            .await
            .expect("casts and buffers batch");
        assert_eq!(sink.pending_rows(), Some(3));
    }

    #[tokio::test]
    async fn sink_rejects_incompatible_schema() {
        use arrow_array::StringArray;
        use arrow_schema::{DataType, Field};

        let config = ClickhouseSinkConfig {
            name: "out".to_string(),
            connection: ClickhouseConnectionConfig {
                url: "http://localhost:8123".to_string(),
                user: None,
                password: None,
                database: "default".to_string(),
                query_timeout_ms: 30000,
                compression: crate::config::CompressionConfig::None,
                settings: HashMap::new(),
            },
            table: "events".to_string(),
            schema_fields: vec![FieldSpec {
                name: "id".to_string(),
                ty: ClickhouseFieldType::Int64,
                nullable: false,
                precision: None,
                scale: None,
            }],
            write_mode: WriteMode::Append,
            chunk_rows: 1000,
            flush_rows: 100,
            truncate_before_first_write: false,
        };

        let mut sink = ClickhouseSink::new(config).expect("creates sink");
        // Missing "id" field
        let bad_schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Utf8,
            false,
        )]));
        let batch =
            RecordBatch::try_new(bad_schema, vec![Arc::new(StringArray::from(vec!["hello"]))])
                .expect("bad batch");

        assert!(sink.write_batch(&batch).await.is_err());
    }

    #[test]
    fn split_chunks_caps_rows_per_chunk() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from((0_i64..10).collect::<Vec<_>>()))],
        )
        .expect("batch");

        // Ten rows, three per chunk: 3 + 3 + 3 + 1, in row order.
        let chunks = split_chunks(&schema, std::slice::from_ref(&batch), 3).expect("split");
        assert_eq!(
            chunks.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            vec![3, 3, 3, 1]
        );
        let ids: Vec<i64> = chunks
            .iter()
            .flat_map(|chunk| {
                let col = chunk
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 column");
                (0..chunk.num_rows())
                    .map(|i| col.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ids, (0_i64..10).collect::<Vec<_>>());

        // `0` means no limit: one chunk.
        let chunks = split_chunks(&schema, std::slice::from_ref(&batch), 0).expect("split");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 10);

        // A chunk spans a batch boundary.
        let chunks =
            split_chunks(&schema, &[batch.slice(0, 4), batch.slice(4, 6)], 3).expect("split");
        assert_eq!(
            chunks.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            vec![3, 3, 3, 1]
        );
    }

    /// A server that accepts every connection and hangs up at once, so the
    /// request in flight fails the way a dropped network would.
    async fn hanging_up_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });
        url
    }

    /// A server that accepts connections and never answers, so a flush parks in
    /// its request until the test drops it.
    async fn silent_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });
        url
    }

    fn sink_at(url: &str, flush_rows: usize) -> ClickhouseSink {
        let config = ClickhouseSinkConfig {
            name: "out".to_string(),
            connection: ClickhouseConnectionConfig {
                url: url.to_string(),
                user: None,
                password: None,
                database: "default".to_string(),
                query_timeout_ms: 200,
                compression: crate::config::CompressionConfig::None,
                settings: HashMap::new(),
            },
            table: "events".to_string(),
            schema_fields: vec![FieldSpec {
                name: "id".to_string(),
                ty: ClickhouseFieldType::Int64,
                nullable: false,
                precision: None,
                scale: None,
            }],
            write_mode: WriteMode::Append,
            chunk_rows: 1000,
            flush_rows,
            truncate_before_first_write: false,
        };
        ClickhouseSink::new(config).expect("creates sink")
    }

    fn rows(ids: &[i64]) -> RecordBatch {
        use arrow_array::Int64Array;
        let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids.to_vec()))]).expect("batch")
    }

    #[tokio::test]
    async fn a_failed_flush_does_not_keep_the_batch_that_triggered_it() {
        let mut sink = sink_at(&hanging_up_server().await, 2);
        sink.write_batch(&rows(&[1])).await.expect("buffered");

        sink.write_batch(&rows(&[2]))
            .await
            .expect_err("the server hangs up");
        assert_eq!(sink.own_rows, 0);
        assert_eq!(sink.buffer.len(), 1, "the caller's batch is not kept");
        assert_eq!(sink.pending_rows(), Some(1), "the accepted row stays");
    }

    #[tokio::test]
    async fn a_failed_finish_keeps_every_row() {
        let mut sink = sink_at(&hanging_up_server().await, 10);
        sink.write_batch(&rows(&[1, 2])).await.expect("buffered");

        sink.finish().await.expect_err("the server hangs up");
        assert_eq!(sink.pending_rows(), Some(2));
        assert_eq!(sink.buffer.len(), 1);
    }

    #[tokio::test]
    async fn a_write_dropped_inside_its_flush_is_taken_back_by_the_next_call() {
        let mut sink = sink_at(&silent_server().await, 2);
        sink.write_batch(&rows(&[1])).await.expect("buffered");

        let own = rows(&[2]);
        let dropped = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            sink.write_batch(&own),
        )
        .await;
        assert!(dropped.is_err(), "the server never answers");
        assert_eq!(sink.own_rows, 1);
        assert_eq!(sink.buffer.len(), 2, "both batches are still buffered");

        assert!(sink.in_flight.is_some(), "the request is still out");

        sink.recover().await;
        assert!(sink.in_flight.is_none(), "given up on after the timeout");
        assert_eq!(sink.own_rows, 0);
        assert_eq!(sink.buffer.len(), 1, "the dropped call's batch is gone");
        assert_eq!(sink.pending_rows(), Some(1), "the accepted row stays");
    }

    #[tokio::test]
    async fn a_finish_dropped_inside_its_flush_keeps_every_row() {
        let mut sink = sink_at(&silent_server().await, 10);
        sink.write_batch(&rows(&[1])).await.expect("buffered");

        let dropped =
            tokio::time::timeout(std::time::Duration::from_millis(100), sink.finish()).await;
        assert!(dropped.is_err(), "the server never answers");
        assert!(sink.in_flight.is_some(), "the request is still out");
        sink.recover().await;
        assert!(sink.in_flight.is_none(), "given up on after the timeout");
        assert_eq!(sink.pending_rows(), Some(1));
        assert_eq!(sink.buffer.len(), 1);
    }

    #[tokio::test]
    async fn recovering_a_sink_that_nothing_interrupted_changes_nothing() {
        let mut sink = sink_at("http://127.0.0.1:1", 10);
        sink.recover().await;
        assert_eq!(sink.pending_rows(), Some(0));
        assert!(sink.in_flight.is_none());
    }

    #[test]
    fn a_new_scope_forgets_the_chunks_of_the_one_before() {
        let mut sink = sink_at("http://127.0.0.1:1", 10);
        sink.landed = vec![1, 2];
        let before = sink.scope;
        sink.begin_scope();
        assert!(sink.landed.is_empty());
        assert_eq!(sink.scope, before + 1);
    }

    #[test]
    fn a_chunk_has_one_token_that_changes_with_anything_that_makes_it_another_write() {
        let schema = rows(&[1]).schema();
        let encode = |ids: &[i64]| encode_chunk(&schema, &rows(ids)).expect("encodes");

        let one = encode(&[1, 2]);
        assert_eq!(one, encode(&[1, 2]), "the encoding is deterministic");
        let print = fingerprint(&one);
        assert_eq!(print, fingerprint(&encode(&[1, 2])));
        assert_ne!(
            print,
            fingerprint(&encode(&[1, 3])),
            "other rows, other print"
        );

        let token = dedup_token(7, 3, 0, print);
        assert_eq!(token, dedup_token(7, 3, 0, print));
        assert_ne!(token, dedup_token(8, 3, 0, print), "another sink");
        assert_ne!(token, dedup_token(7, 4, 0, print), "another flush");
        assert_ne!(
            token,
            dedup_token(7, 3, 1, print),
            "another chunk of the flush"
        );
        assert_ne!(
            token,
            dedup_token(7, 3, 0, fingerprint(&encode(&[1, 3]))),
            "other rows"
        );
    }

    #[test]
    fn two_sinks_draw_different_run_ids() {
        assert_ne!(new_run_id(), new_run_id());
    }

    /// A retry compares each rebuilt chunk with the one the server confirmed at
    /// its position, so the chunks must depend on the rows and not on how the
    /// buffer cut them into batches.
    #[test]
    fn the_same_rows_make_the_same_chunks_however_the_batches_were_cut() {
        let all = rows(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let schema = all.schema();
        let prints = |batches: &[RecordBatch]| -> Vec<u64> {
            split_chunks(&schema, batches, 3)
                .expect("split")
                .iter()
                .map(|chunk| fingerprint(&encode_chunk(&schema, chunk).expect("encodes")))
                .collect()
        };

        let whole = prints(std::slice::from_ref(&all));
        assert_eq!(whole.len(), 4);
        assert_eq!(whole, prints(&[all.slice(0, 4), all.slice(4, 6)]));
        assert_eq!(
            whole,
            prints(&[all.slice(0, 1), all.slice(1, 8), all.slice(9, 1)])
        );
    }
}
