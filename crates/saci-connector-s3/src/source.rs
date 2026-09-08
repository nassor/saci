//! [`S3Source`]: an S3 object stream read through a transformer.
//!
//! The source lists its prefix once, then walks the listing one object at a
//! time. Each object is streamed into an unnamed tempfile, one dedicated OS
//! thread drives the transformer's [`BatchReader`](saci_transformer::BatchReader)
//! over that file, and [`Source::next_batch`] awaits the bounded channel the
//! thread feeds. EOF is the listing exhausted; there is no re-list/tail mode,
//! so this is a finite source every run mode can drive.

use std::collections::VecDeque;
use std::io::Seek;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Fields, Schema};
use async_trait::async_trait;
use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_transformer::Transformer;

use crate::config::{S3SourceConfig, SchemaFrom};

/// Batches the reader thread may queue before it blocks. Matches
/// `saci-connector-file`'s.
const CHANNEL_CAPACITY: usize = 4;

/// S3 [`Source`]. The transformer decodes; this type owns the object client,
/// the spool file, the reader thread and the channel.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
///
/// use arrow_schema::Schema;
/// use saci_connector_s3::{S3ConnectionConfig, S3Source, S3SourceConfig};
/// use saci_transformer_csv::CsvTransformer;
///
/// let config = S3SourceConfig {
///     connection: S3ConnectionConfig {
///         bucket: "orders".to_string(),
///         endpoint: Some("http://127.0.0.1:9000".to_string()),
///         access_key_id: Some("key".to_string()),
///         secret_access_key: Some("secret".to_string()),
///         allow_http: true,
///         ..Default::default()
///     },
///     prefix: "incoming".to_string(),
///     schema_from: saci_connector_s3::SchemaFrom::Config,
///     schema_fields: Vec::new(),
/// };
/// let src = S3Source::new(
///     config,
///     Arc::new(Schema::empty()),
///     Arc::new(CsvTransformer::new(true)),
/// )
/// .unwrap();
/// ```
pub struct S3Source {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    transformer: Arc<dyn Transformer>,
    declared: Arc<Schema>,
    schema_from: SchemaFrom,
    /// `None` until the first `next_batch` lists the prefix.
    pending: Option<VecDeque<Path>>,
    /// Batches from the object currently being decoded.
    current: Option<mpsc::Receiver<Result<RecordBatch, SaciError>>>,
}

impl S3Source {
    /// Synchronous and opens no connection, matching `KafkaSource::new`: the
    /// first request happens inside `next_batch`.
    ///
    /// `schema` is the declared Arrow schema the config named, whatever
    /// `schema_from` says; what the transformer receives depends on
    /// [`SchemaFrom`] (`Config` hands it over to project the object onto,
    /// `Object` hides it and the object's own schema must then match field for
    /// field).
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the connection settings are
    /// invalid. No request is made.
    pub fn new(
        config: S3SourceConfig,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, SaciError> {
        let store = config.connection.build_store("S3Source")?;
        Ok(Self {
            store,
            prefix: Path::from(config.prefix),
            transformer,
            declared: schema,
            schema_from: config.schema_from,
            pending: None,
            current: None,
        })
    }
}

#[async_trait]
impl Source for S3Source {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.declared)
    }

    /// One batch off the object at the front of the listing, or `Ok(None)`
    /// once that listing is exhausted. The first call lists the prefix.
    ///
    /// # Errors
    ///
    /// A location leaves the listing only when its object's batch stream ends
    /// cleanly, so every other outcome leaves it at the front: a listing
    /// error, a download or spool error, a reader the object's bytes refuse,
    /// the `schema_from="object"` schema check, and a decode error raised part
    /// way through the object. A caller's retry therefore re-attempts that
    /// same object rather than advancing past it.
    ///
    /// A failure before the object's first batch replays nothing. A failure
    /// after some of its batches have been handed over replays those batches:
    /// the retry reopens the object at its start, so the rows already
    /// delivered arrive a second time. That duplication is what this engine's
    /// at-least-once delivery allows, and it is the price of not dropping the
    /// rest of the object in silence.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        // 1. First call: list the prefix once, in location order. object_store
        //    documents no ordering guarantee, so the sort is load-bearing.
        if self.pending.is_none() {
            let mut locations = Vec::new();
            let mut stream = self.store.list(Some(&self.prefix));
            while let Some(meta) = stream.next().await {
                let meta = meta.map_err(|e| {
                    SaciError::generic(format!("S3Source: listing {}: {e}", self.prefix))
                })?;
                // A directory marker some services write as a zero-byte object.
                if meta.size == 0 {
                    continue;
                }
                locations.push(meta.location);
            }
            locations.sort_unstable();
            self.pending = Some(locations.into());
        }

        loop {
            // 2. Drain the object currently being decoded before opening the
            //    next one. Only a clean end of that object's stream (`None`)
            //    pops its location from `pending`: an error leaves the
            //    location at the front so a caller's retry re-attempts the
            //    same object instead of silently skipping past it.
            if let Some(rx) = &mut self.current {
                match rx.recv().await {
                    Some(Ok(batch)) => return Ok(Some(batch)),
                    Some(Err(e)) => {
                        self.current = None;
                        return Err(e);
                    }
                    None => {
                        self.current = None;
                        self.pending
                            .as_mut()
                            .expect("pending was just set")
                            .pop_front();
                    }
                }
            }

            // 3. Next object, or EOF once the listing is exhausted. Peeked,
            //    not popped: opening or decoding it below may still fail, and
            //    step 2 above is the only place that removes a location, once
            //    it is fully and successfully drained.
            let Some(location) = self
                .pending
                .as_ref()
                .expect("pending was just set")
                .front()
                .cloned()
            else {
                return Ok(None);
            };

            // 4. Stream the object into an unnamed tempfile: the OS reclaims it
            //    on close, and the object never sits whole in memory.
            let result = self
                .store
                .get(&location)
                .await
                .map_err(|e| SaciError::generic(format!("S3Source: get {location}: {e}")))?;
            let mut stream = result.into_stream();
            let mut file = tokio::fs::File::from_std(
                tempfile::tempfile()
                    .map_err(|e| SaciError::generic(format!("S3Source: spool file: {e}")))?,
            );
            let mut size = 0usize;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| {
                    SaciError::generic(format!("S3Source: download {location}: {e}"))
                })?;
                size += chunk.len();
                file.write_all(&chunk)
                    .await
                    .map_err(|e| SaciError::generic(format!("S3Source: spool write: {e}")))?;
            }
            file.flush()
                .await
                .map_err(|e| SaciError::generic(format!("S3Source: spool flush: {e}")))?;
            let mut spool = file.into_std().await;
            spool
                .rewind()
                .map_err(|e| SaciError::generic(format!("S3Source: spool rewind: {e}")))?;

            #[cfg(feature = "tracing")]
            tracing::info!(key = %location, bytes = size, "S3Source: object spooled");
            #[cfg(not(feature = "tracing"))]
            let _ = (&location, size);

            // 5. Open the reader off the executor — a parquet footer read is
            //    disk IO — and check the object's own schema against the config
            //    when the config asked the object to be the schema source.
            let declared_arg = match self.schema_from {
                SchemaFrom::Config => Some(Arc::clone(&self.declared)),
                SchemaFrom::Object => None,
            };
            let transformer = Arc::clone(&self.transformer);
            let (mut reader, object_schema) = tokio::task::spawn_blocking(move || {
                let reader = transformer.open_reader(spool, declared_arg)?;
                let schema = reader.schema();
                Ok::<_, SaciError>((reader, schema))
            })
            .await
            .map_err(|e| SaciError::generic(format!("S3Source: spawn_blocking panic: {e}")))??;
            if self.schema_from == SchemaFrom::Object
                && object_schema.fields() != self.declared.fields()
            {
                return Err(SaciError::configuration(format!(
                    "S3Source: object '{location}' carries schema [{}] but the config declared [{}]",
                    render_fields(object_schema.fields()),
                    render_fields(self.declared.fields()),
                )));
            }

            // 6. Decode off the executor, feeding a bounded channel; the
            //    executor never blocks on the disk. The loop above drains the
            //    receiver the object just opened.
            let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
            std::thread::spawn(move || {
                loop {
                    match reader.next_batch() {
                        Ok(None) => break,
                        Ok(Some(batch)) => {
                            // A send error means the source was dropped
                            // (pipeline aborted), so this thread has nowhere
                            // left to go.
                            if tx.blocking_send(Ok(batch)).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(Err(e));
                            break;
                        }
                    }
                }
            });
            self.current = Some(rx);
        }
    }
}

/// `Fields`'s own `Debug` is unreadable in an error a config author has to act
/// on; this renders `name: data_type` pairs.
fn render_fields(fields: &Fields) -> String {
    fields
        .iter()
        .map(|f| format!("{}: {}", f.name(), f.data_type()))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use arrow_schema::Schema;
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    use saci_transformer_parquet::ParquetTransformer;

    use super::*;

    /// A permanently unreadable object (invalid Parquet bytes) must fail
    /// every attempt, never a clean EOF: `next_batch` peeks the front of
    /// `pending` rather than popping it, and only removes a location once
    /// its object has been fully and successfully drained, so a caller
    /// retrying after an error re-attempts the same object instead of
    /// silently advancing past it.
    ///
    /// This covers a failure raised before the object's first batch, where
    /// the retry replays nothing. It says nothing about a decode error part
    /// way through an object, whose retry reopens it from the start and
    /// re-emits the batches already delivered.
    #[tokio::test]
    async fn a_source_whose_object_fails_to_open_reports_an_error_not_eof() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let location = Path::from("prefix/broken.parquet");
        store
            .put(
                &location,
                PutPayload::from(b"not a parquet file".as_slice()),
            )
            .await
            .expect("seed put succeeds");

        let mut source = S3Source {
            store,
            prefix: Path::from("prefix"),
            transformer: Arc::new(ParquetTransformer::new()),
            declared: Arc::new(Schema::empty()),
            // `Object` so the malformed bytes fail inside the Parquet
            // reader builder itself, not the earlier configuration check
            // on the object's own schema.
            schema_from: SchemaFrom::Object,
            pending: None,
            current: None,
        };

        source
            .next_batch()
            .await
            .expect_err("a malformed object must not silently succeed");
        source.next_batch().await.expect_err(
            "retrying after a failed attempt must surface an error again, not a clean EOF",
        );
    }
}
