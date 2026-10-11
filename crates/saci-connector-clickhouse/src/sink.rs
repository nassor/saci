//! [`ClickhouseSink`]: ClickHouse [`Sink`] implementation with Arrow IPC streaming.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_cast::CastOptions;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;
use async_trait::async_trait;
use saci_core::error::SaciError;
use saci_core::io::cast_batch;
use saci_core::io::sink::Sink;

use crate::config::ClickhouseSinkConfig;
use crate::connection::{build_client, quote_qualified};

/// A ClickHouse [`Sink`] bulk-loading batches via native Arrow IPC streaming.
pub struct ClickhouseSink {
    config: ClickhouseSinkConfig,
    schema: Arc<Schema>,
    client: clickhouse::Client,
    buffer: Vec<RecordBatch>,
    buffered_rows: usize,
    first_write_done: bool,
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
            first_write_done: false,
        })
    }

    /// Flushes any buffered rows to ClickHouse.
    async fn flush(&mut self) -> Result<(), SaciError> {
        if self.buffered_rows == 0 {
            return Ok(());
        }

        let quoted_table = quote_qualified(&self.config.table);

        if !self.first_write_done {
            if self.config.truncate_before_first_write {
                let truncate_sql = format!("TRUNCATE TABLE {quoted_table}");
                self.client
                    .query(&truncate_sql)
                    .execute()
                    .await
                    .map_err(|e| {
                        SaciError::generic(format!(
                            "clickhouse sink truncate table '{quoted_table}' error: {e}"
                        ))
                    })?;
            }
            self.first_write_done = true;
        }

        // One INSERT per chunk, so a large buffer becomes several requests of at
        // most `chunk_rows` rows rather than one unbounded stream.
        for chunk in split_chunks(&self.schema, &self.buffer, self.config.chunk_rows)? {
            let mut buf = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buf, &self.schema).map_err(|e| {
                    SaciError::generic(format!("clickhouse sink: stream writer init error: {e}"))
                })?;
                writer.write(&chunk).map_err(|e| {
                    SaciError::generic(format!("clickhouse sink: stream encode error: {e}"))
                })?;
                writer.finish().map_err(|e| {
                    SaciError::generic(format!("clickhouse sink: stream finish error: {e}"))
                })?;
            }

            let insert_sql = format!("INSERT INTO {quoted_table} FORMAT ArrowStream");
            let mut insert = self.client.insert_formatted_with(insert_sql);
            insert.send(bytes::Bytes::from(buf)).await.map_err(|e| {
                SaciError::generic(format!("clickhouse sink: stream send error: {e}"))
            })?;
            insert.end().await.map_err(|e| {
                SaciError::generic(format!("clickhouse sink: stream commit error: {e}"))
            })?;
        }

        self.buffer.clear();
        self.buffered_rows = 0;
        Ok(())
    }
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
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let casted = cast_batch(batch, &self.schema, &CastOptions::default())?;
        self.buffered_rows += casted.num_rows();
        self.buffer.push(casted);

        if self.config.flush_rows == 0 || self.buffered_rows >= self.config.flush_rows {
            self.flush().await?;
        }

        Ok(())
    }

    fn pending_rows(&self) -> Option<usize> {
        Some(self.buffered_rows)
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
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
}
