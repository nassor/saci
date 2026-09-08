//! [`ChannelRegistry`]: the name-keyed [`ChannelBridge`] pairing a
//! `ChannelSink` in one workflow with a `ChannelSource` in another over one
//! shared `tokio::sync::mpsc` pair.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use tokio::sync::mpsc;

use saci_connector::ChannelBridge;
use saci_core::error::SaciError;
use saci_core::io::{sink::Sink, source::Source};

use crate::{ChannelSink, ChannelSource};

/// One channel name's pairing state.
///
/// Whichever half is built first creates the `mpsc` pair and stores both
/// ends; the second half `take`s its end out. Once both halves exist, the
/// registry holds no live endpoint for this name: the sink is the channel's
/// only `Sender`, so the producer workflow finishing (dropping its
/// `ChannelSink`) closes the channel and the consumer's `ChannelSource` sees
/// a legitimate EOF.
#[derive(Default)]
struct Entry {
    sender: Option<mpsc::Sender<RecordBatch>>,
    receiver: Option<mpsc::Receiver<RecordBatch>>,
    schema: Option<Arc<Schema>>,
    buffer: usize,
    sink_built: bool,
    source_built: bool,
}

/// Name-keyed registry resolving each channel name's `ChannelSink` and
/// `ChannelSource` to the two ends of one shared `mpsc` pair.
///
/// Register one instance with [`ServiceBuilder::with_channel_bridge`]
/// (`saci-service`, `connector-channel` registers a default instance
/// automatically); the channel factories reach it through
/// [`ConnectorContext::channel_bridge`](saci_connector::ConnectorContext::channel_bridge).
///
/// [`ServiceBuilder::with_channel_bridge`]: https://docs.rs/saci-service/latest/saci_service/service/builder/struct.ServiceBuilder.html#method.with_channel_bridge
#[derive(Default)]
pub struct ChannelRegistry {
    entries: Mutex<HashMap<String, Entry>>,
}

fn schema_mismatch(name: &str) -> SaciError {
    SaciError::configuration(format!(
        "channel '{name}': the paired ChannelSource and ChannelSink declare different schemas"
    ))
}

fn buffer_mismatch(name: &str, buffer: usize, other: usize) -> SaciError {
    SaciError::configuration(format!(
        "channel '{name}': buffer {buffer} differs from the paired half's buffer {other}"
    ))
}

impl ChannelBridge for ChannelRegistry {
    fn sink(
        &self,
        name: &str,
        schema: Arc<Schema>,
        buffer: usize,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = entries.entry(name.to_string()).or_default();
        if entry.sink_built {
            return Err(SaciError::configuration(format!(
                "channel '{name}': more than one ChannelSink declared"
            )));
        }
        if entry.sender.is_none() && entry.receiver.is_none() {
            // Nobody has claimed this name yet: create the pair.
            let (tx, rx) = mpsc::channel(buffer);
            entry.sender = Some(tx);
            entry.receiver = Some(rx);
            entry.schema = Some(schema.clone());
            entry.buffer = buffer;
        } else {
            // The source half already created the pair.
            if entry.schema.as_ref() != Some(&schema) {
                return Err(schema_mismatch(name));
            }
            if entry.buffer != buffer {
                return Err(buffer_mismatch(name, buffer, entry.buffer));
            }
        }
        let tx = entry
            .sender
            .take()
            .expect("sender present for the sink half");
        entry.sink_built = true;
        Ok(Box::new(ChannelSink::from_sender(schema, buffer, tx)))
    }

    fn source(
        &self,
        name: &str,
        schema: Arc<Schema>,
        buffer: usize,
    ) -> Result<Box<dyn Source>, SaciError> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = entries.entry(name.to_string()).or_default();
        if entry.source_built {
            return Err(SaciError::configuration(format!(
                "channel '{name}': more than one ChannelSource declared"
            )));
        }
        if entry.sender.is_none() && entry.receiver.is_none() {
            // Nobody has claimed this name yet: create the pair.
            let (tx, rx) = mpsc::channel(buffer);
            entry.sender = Some(tx);
            entry.receiver = Some(rx);
            entry.schema = Some(schema.clone());
            entry.buffer = buffer;
        } else {
            // The sink half already created the pair.
            if entry.schema.as_ref() != Some(&schema) {
                return Err(schema_mismatch(name));
            }
            if entry.buffer != buffer {
                return Err(buffer_mismatch(name, buffer, entry.buffer));
            }
        }
        let rx = entry
            .receiver
            .take()
            .expect("receiver present for the source half");
        entry.source_built = true;
        Ok(Box::new(ChannelSource::from_receiver(schema, rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch(schema: Arc<Schema>) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn sink_first_then_source_pairs_and_carries_data() {
        let registry = ChannelRegistry::default();
        let mut sink = registry.sink("bridge", schema(), 4).expect("sink half");
        let mut source = registry.source("bridge", schema(), 4).expect("source half");

        sink.write_batch(&batch(schema())).await.expect("write");
        drop(sink);

        let received = source.next_batch().await.expect("recv").expect("a batch");
        assert_eq!(received.num_rows(), 3);
        assert!(
            source.next_batch().await.expect("recv").is_none(),
            "EOF after sink drop"
        );
    }

    #[tokio::test]
    async fn source_first_then_sink_pairs_and_carries_data() {
        let registry = ChannelRegistry::default();
        let mut source = registry.source("bridge", schema(), 4).expect("source half");
        let mut sink = registry.sink("bridge", schema(), 4).expect("sink half");

        sink.write_batch(&batch(schema())).await.expect("write");
        drop(sink);

        let received = source.next_batch().await.expect("recv").expect("a batch");
        assert_eq!(received.num_rows(), 3);
    }

    #[test]
    fn a_second_sink_for_the_same_name_is_a_configuration_error() {
        let registry = ChannelRegistry::default();
        let _first = registry.sink("bridge", schema(), 4).expect("first sink");
        let err = registry
            .sink("bridge", schema(), 4)
            .err()
            .expect("a second ChannelSink must be rejected");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("more than one ChannelSink declared"));
    }

    #[test]
    fn a_second_source_for_the_same_name_is_a_configuration_error() {
        let registry = ChannelRegistry::default();
        let _first = registry
            .source("bridge", schema(), 4)
            .expect("first source");
        let err = registry
            .source("bridge", schema(), 4)
            .err()
            .expect("a second ChannelSource must be rejected");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message()
                .contains("more than one ChannelSource declared")
        );
    }

    #[test]
    fn mismatched_schema_between_the_two_halves_is_a_configuration_error() {
        let registry = ChannelRegistry::default();
        let other_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let _sink = registry.sink("bridge", schema(), 4).expect("sink half");
        let err = registry
            .source("bridge", other_schema, 4)
            .err()
            .expect("mismatched schema must be rejected");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("declare different schemas"));
    }

    #[test]
    fn mismatched_buffer_between_the_two_halves_is_a_configuration_error() {
        let registry = ChannelRegistry::default();
        let _sink = registry.sink("bridge", schema(), 4).expect("sink half");
        let err = registry
            .source("bridge", schema(), 8)
            .err()
            .expect("mismatched buffer must be rejected");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("buffer 8 differs from"));
    }

    #[test]
    fn distinct_names_do_not_interact() {
        let registry = ChannelRegistry::default();
        let _a_sink = registry.sink("a", schema(), 4).expect("a sink");
        let _b_sink = registry.sink("b", schema(), 4).expect("b sink");
        let _a_source = registry.source("a", schema(), 4).expect("a source");
        let _b_source = registry.source("b", schema(), 4).expect("b source");
    }
}
