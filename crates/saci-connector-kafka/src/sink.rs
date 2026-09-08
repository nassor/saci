//! [`KafkaSink`]: a Kafka topic producer [`Sink`].

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, RecordBatch};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::Schema;
use async_trait::async_trait;
use futures_util::future::join_all;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_transformer::{MessageShape, Transformer};

use crate::admin::ensure_topics;
use crate::config::{KafkaSinkConfig, client_config};

/// Kafka [`Sink`]: the resolved format's [`MessageShape`] decides whether a
/// batch becomes one message per row or one message in total.
///
/// The producer connects lazily: [`new`](Self::new) only builds and validates
/// the `FutureProducer`, so `saci-service validate` stays broker-free. The
/// first [`write_batch`](Sink::write_batch) call provisions the topic (unless
/// opted out).
pub struct KafkaSink {
    producer: FutureProducer,
    /// Kept so the admin client used for topic provisioning shares the same
    /// bootstrap servers and properties as the producer.
    client_config: ClientConfig,
    topic: String,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
    /// Cached from the transformer at construction, where the capability check
    /// already proved it is `Some`.
    shape: MessageShape,
    cfg: KafkaSinkConfig,
    /// Set once the topic has been provisioned, so it happens exactly once.
    provisioned: bool,
}

impl KafkaSink {
    /// Validate the config and create the producer. Opens no connection:
    /// librdkafka connects lazily in the background on first use.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `cfg` fails validation, the
    /// format has no message codec, `key_field` is set against a format that
    /// emits one message per batch, or librdkafka rejects a property in
    /// `cfg.properties`.
    pub fn new(
        cfg: KafkaSinkConfig,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, SaciError> {
        cfg.validate()?;

        // Both capability checks happen here rather than in `validate`: only
        // the resolved transformer knows its message shape.
        let format = transformer.format();
        let Some(shape) = transformer.message_shape() else {
            return Err(SaciError::configuration(format!(
                "KafkaSink: format '{format}' has no message codec"
            )));
        };
        if shape == MessageShape::PerBatch && cfg.key_field.is_some() {
            return Err(SaciError::configuration(format!(
                "KafkaSink config: 'key_field' needs a row-per-message format; '{format}' emits \
                 one message per batch"
            )));
        }

        let client_config = client_config(&cfg.brokers, &[], &cfg.properties);
        let producer: FutureProducer = client_config.create().map_err(|e| {
            SaciError::configuration(format!("KafkaSink: cannot create producer: {e}"))
        })?;
        let topic = cfg.topic.clone();
        Ok(Self {
            producer,
            client_config,
            topic,
            schema,
            transformer,
            shape,
            cfg,
            provisioned: false,
        })
    }

    async fn ensure_provisioned(&mut self) -> Result<(), SaciError> {
        if self.provisioned {
            return Ok(());
        }
        let topics = std::slice::from_ref(&self.topic);
        ensure_topics(&self.client_config, topics, &self.cfg.provision).await?;
        self.provisioned = true;
        Ok(())
    }

    /// Render `field` for every row as a message key, `None` for a null cell.
    fn render_keys(batch: &RecordBatch, field: &str) -> Result<Vec<Option<String>>, SaciError> {
        let column = batch.column_by_name(field).ok_or_else(|| {
            SaciError::generic(format!(
                "KafkaSink: key_field '{field}' is not a column in the batch"
            ))
        })?;
        let formatter = ArrayFormatter::try_new(column.as_ref(), &FormatOptions::default())
            .map_err(|e| {
                SaciError::generic(format!("KafkaSink: formatting key_field '{field}': {e}"))
            })?;
        Ok((0..column.len())
            .map(|i| {
                if column.is_null(i) {
                    None
                } else {
                    Some(formatter.value(i).to_string())
                }
            })
            .collect())
    }

    /// Which rows are tombstones: every column other than `key_field` is
    /// null, so the row names a key and carries no value for it.
    ///
    /// Decided once per batch, and short-circuited by the first value column
    /// with no nulls in it at all — one row of real data anywhere in that
    /// column proves no row of the batch can be a delete marker.
    fn tombstone_rows(batch: &RecordBatch, key_field: &str) -> Vec<bool> {
        let mut rows = vec![true; batch.num_rows()];
        for (index, field) in batch.schema_ref().fields().iter().enumerate() {
            if field.name() == key_field {
                continue;
            }
            let column = batch.column(index);
            if column.null_count() == 0 {
                rows.fill(false);
                return rows;
            }
            for (row, tombstone) in rows.iter_mut().enumerate() {
                *tombstone &= column.is_null(row);
            }
        }
        rows
    }

    /// The record one row is produced as. A tombstone carries its key and a
    /// NULL payload, which is what tells a compacted topic the key is gone;
    /// every other row carries the encoded payload.
    fn record<'a>(
        topic: &'a str,
        payload: &'a [u8],
        key: Option<&'a str>,
        tombstone: bool,
    ) -> FutureRecord<'a, str, [u8]> {
        let mut record: FutureRecord<'a, str, [u8]> = FutureRecord::to(topic);
        if !tombstone {
            record = record.payload(payload);
        }
        if let Some(key) = key {
            record = record.key(key);
        }
        record
    }

    /// Encode the batch and publish every payload. Every message is sent, then
    /// every delivery is awaited concurrently: that is the durability boundary,
    /// and it also bounds the producer queue without extra config.
    async fn publish(&self, batch: &RecordBatch) -> Result<(), SaciError> {
        let payloads = self.transformer.encode_messages(batch)?;
        if self.shape == MessageShape::PerRow && payloads.len() != batch.num_rows() {
            return Err(SaciError::generic(format!(
                "KafkaSink: format '{}' produced {} messages for {} rows",
                self.transformer.format(),
                payloads.len(),
                batch.num_rows()
            )));
        }

        let keys = match &self.cfg.key_field {
            None => None,
            Some(field) => Some(Self::render_keys(batch, field)?),
        };
        // Tombstones need a key to delete, so `validate` pairs the two keys
        // and this reads both or neither.
        let tombstones = match (&self.cfg.key_field, self.cfg.tombstones) {
            (Some(field), true) => Some(Self::tombstone_rows(batch, field)),
            _ => None,
        };

        let flush_timeout = Duration::from_millis(self.cfg.flush_timeout_ms);
        let mut sends = Vec::with_capacity(payloads.len());
        for (i, payload) in payloads.iter().enumerate() {
            let record = Self::record(
                &self.topic,
                payload.as_slice(),
                keys.as_ref().and_then(|k| k[i].as_deref()),
                tombstones.as_ref().is_some_and(|t| t[i]),
            );
            sends.push(self.producer.send(record, Timeout::After(flush_timeout)));
        }

        for result in join_all(sends).await {
            result.map_err(|(e, _)| {
                SaciError::generic(format!(
                    "KafkaSink: delivery to '{}' failed: {e}",
                    self.topic
                ))
            })?;
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.ensure_provisioned().await?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.publish(batch).await
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        let producer = self.producer.clone();
        let timeout = Duration::from_millis(self.cfg.flush_timeout_ms);
        // `Producer::flush` is a blocking call (it polls librdkafka's queue in
        // a loop up to `timeout`), so it must not run on the async executor.
        tokio::task::spawn_blocking(move || producer.flush(Timeout::After(timeout)))
            .await
            .map_err(|e| SaciError::generic(format!("KafkaSink: flush task panicked: {e}")))?
            .map_err(|e| SaciError::generic(format!("KafkaSink: flush failed: {e}")))
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};

    /// Two rows keyed by `id`: the first carries a value, the second carries
    /// nothing but its key, which is what a delete looks like on the wire.
    fn batch(values: [Option<&str>; 2]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(values.to_vec())),
            ],
        )
        .expect("two columns of two rows")
    }

    #[test]
    fn a_row_whose_value_columns_are_all_null_is_a_tombstone() {
        let rows = KafkaSink::tombstone_rows(&batch([Some("kept"), None]), "id");
        assert_eq!(rows, vec![false, true]);
    }

    #[test]
    fn a_value_column_with_no_nulls_leaves_every_row_alone() {
        let rows = KafkaSink::tombstone_rows(&batch([Some("a"), Some("b")]), "id");
        assert_eq!(rows, vec![false, false]);
    }

    #[test]
    fn a_null_in_the_key_column_alone_is_not_a_tombstone() {
        // `label` is the key here, so `id` is the only value column and it is
        // fully populated: neither row deletes anything.
        let rows = KafkaSink::tombstone_rows(&batch([Some("a"), None]), "label");
        assert_eq!(rows, vec![false, false]);
    }

    #[test]
    fn a_tombstone_row_is_produced_with_a_null_payload_and_its_key() {
        let record = KafkaSink::record("orders", b"{\"id\":2}", Some("2"), true);
        assert!(record.payload.is_none(), "a delete marker carries no value");
        assert_eq!(record.key, Some("2"));
        assert_eq!(record.topic, "orders");
    }

    #[test]
    fn a_row_that_is_not_a_tombstone_is_produced_unchanged() {
        let payload = b"{\"id\":1,\"label\":\"kept\"}";
        let record = KafkaSink::record("orders", payload, Some("1"), false);
        assert_eq!(record.payload, Some(payload.as_slice()));
        assert_eq!(record.key, Some("1"));
    }

    #[test]
    fn a_keyless_sink_still_produces_its_payload() {
        let payload = b"{\"id\":1}";
        let record = KafkaSink::record("orders", payload, None, false);
        assert_eq!(record.payload, Some(payload.as_slice()));
        assert_eq!(record.key, None);
    }
}
