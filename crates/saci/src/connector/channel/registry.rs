//! [`ChannelRegistry`]: the name-keyed [`ChannelBridge`] pairing a
//! `ChannelSink` in one workflow with a `ChannelSource` in another over one
//! shared `tokio::sync::mpsc` pair.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use tokio::sync::mpsc;

use crate::connector::ChannelBridge;
use crate::error::SaciError;
use crate::io::{Sink, Source};

use crate::connector::channel::{ChannelSink, ChannelSource};

/// Shared pairing slot for one named channel across workflow restarts.
///
/// Tracks whether each half is currently alive via [`sink_active`] and
/// [`source_active`], and exchanges replacement `mpsc` endpoints when either
/// workflow is rebuilt after dropping its previous half.
pub(crate) struct ChannelSlot {
    schema: Arc<Schema>,
    buffer: usize,
    pub(crate) sink_active: AtomicBool,
    pub(crate) source_active: AtomicBool,
    pub(crate) pending_tx: Mutex<Option<mpsc::Sender<RecordBatch>>>,
    pub(crate) pending_rx: Mutex<Option<mpsc::Receiver<RecordBatch>>>,
}

impl ChannelSlot {
    fn new(schema: Arc<Schema>, buffer: usize) -> Self {
        Self {
            schema,
            buffer,
            sink_active: AtomicBool::new(false),
            source_active: AtomicBool::new(false),
            pending_tx: Mutex::new(None),
            pending_rx: Mutex::new(None),
        }
    }

    pub(crate) fn take_pending_tx(&self) -> Option<mpsc::Sender<RecordBatch>> {
        self.pending_tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    pub(crate) fn take_pending_rx(&self) -> Option<mpsc::Receiver<RecordBatch>> {
        self.pending_rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

/// Name-keyed registry resolving each channel name's `ChannelSink` and
/// `ChannelSource` to the two ends of one shared `mpsc` pair.
///
/// Register one instance with [`ServiceBuilder::channels`]
/// (under the service, `connector-channel` registers a default instance
/// automatically); the channel factories reach it through
/// [`ConnectorContext::channels`](crate::connector::ConnectorContext::channels).
///
/// [`ServiceBuilder::channels`]: crate::service::ServiceBuilder::channels
#[derive(Default)]
pub struct ChannelRegistry {
    entries: Mutex<HashMap<String, Arc<ChannelSlot>>>,
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
        let slot = {
            let mut entries = self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            entries
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(ChannelSlot::new(schema.clone(), buffer)))
                .clone()
        };
        if slot.schema != schema {
            return Err(schema_mismatch(name));
        }
        if slot.buffer != buffer {
            return Err(buffer_mismatch(name, buffer, slot.buffer));
        }
        if slot
            .sink_active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(SaciError::configuration(format!(
                "channel '{name}': more than one ChannelSink declared"
            )));
        }

        let tx = match slot.take_pending_tx() {
            Some(tx) => tx,
            None => {
                let (tx, rx) = mpsc::channel(buffer);
                *slot
                    .pending_rx
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(rx);
                tx
            }
        };
        Ok(Box::new(ChannelSink::with_slot(schema, buffer, tx, slot)))
    }

    fn source(
        &self,
        name: &str,
        schema: Arc<Schema>,
        buffer: usize,
    ) -> Result<Box<dyn Source>, SaciError> {
        let slot = {
            let mut entries = self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            entries
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(ChannelSlot::new(schema.clone(), buffer)))
                .clone()
        };
        if slot.schema != schema {
            return Err(schema_mismatch(name));
        }
        if slot.buffer != buffer {
            return Err(buffer_mismatch(name, buffer, slot.buffer));
        }
        if slot
            .source_active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(SaciError::configuration(format!(
                "channel '{name}': more than one ChannelSource declared"
            )));
        }

        let rx = match slot.take_pending_rx() {
            Some(rx) => rx,
            None => {
                let (tx, rx) = mpsc::channel(buffer);
                *slot
                    .pending_tx
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(tx);
                rx
            }
        };
        Ok(Box::new(ChannelSource::with_slot(schema, rx, slot)))
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
