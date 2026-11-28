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

/// Current channel halves and monotonic generation for one named slot.
pub(crate) struct SlotEndpoints {
    /// Active sender half routing batches to the current (or pending) receiver.
    pub(crate) tx: mpsc::Sender<RecordBatch>,
    /// Receiver waiting for the next `ChannelSource` when the sink was created first or rebuilt.
    pub(crate) unclaimed_rx: Option<mpsc::Receiver<RecordBatch>>,
    /// Monotonic generation incremented whenever a fresh `(tx, rx)` pair is installed.
    pub(crate) generation: u64,
}

fn closed_sender() -> mpsc::Sender<RecordBatch> {
    let (tx, _rx) = mpsc::channel(1);
    tx
}

/// Shared pairing slot for one named channel across workflow restarts.
///
/// Tracks whether each half is currently alive via `sink_active` and
/// `source_active`, and synchronizes replacement `(tx, rx)` pairs via a
/// monotonic generation under `endpoints`.
pub(crate) struct ChannelSlot {
    schema: Arc<Schema>,
    buffer: usize,
    pub(crate) sink_active: AtomicBool,
    pub(crate) source_active: AtomicBool,
    pub(crate) endpoints: Mutex<SlotEndpoints>,
}

impl ChannelSlot {
    fn new(schema: Arc<Schema>, buffer: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer);
        Self {
            schema,
            buffer,
            sink_active: AtomicBool::new(false),
            source_active: AtomicBool::new(false),
            endpoints: Mutex::new(SlotEndpoints {
                tx,
                unclaimed_rx: Some(rx),
                generation: 1,
            }),
        }
    }

    /// Take a pending replacement receiver installed for a generation newer than `current_gen`.
    pub(crate) fn take_newer_rx(
        &self,
        current_gen: &mut u64,
    ) -> Option<mpsc::Receiver<RecordBatch>> {
        let mut ep = self
            .endpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if ep.generation > *current_gen
            && let Some(rx) = ep.unclaimed_rx.take()
        {
            *current_gen = ep.generation;
            return Some(rx);
        }
        None
    }

    /// Refresh a sink's sender if a newer source installed a fresh channel generation
    /// or the previous receiver closed and needs a fresh channel pair.
    pub(crate) fn refresh_tx(
        &self,
        tx: &mut mpsc::Sender<RecordBatch>,
        current_gen: &mut u64,
    ) -> bool {
        let mut ep = self
            .endpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if ep.generation > *current_gen && !ep.tx.is_closed() {
            *tx = ep.tx.clone();
            *current_gen = ep.generation;
            return true;
        }
        if ep.tx.is_closed() && ep.unclaimed_rx.is_none() {
            let (new_tx, new_rx) = mpsc::channel(self.buffer);
            ep.tx = new_tx.clone();
            ep.unclaimed_rx = Some(new_rx);
            ep.generation += 1;
            *tx = new_tx;
            *current_gen = ep.generation;
            return true;
        }
        false
    }

    /// Called on `ChannelSink::drop`: release the slot's sender reference if no newer
    /// generation has already replaced it so the active receiver observes clean EOF.
    pub(crate) fn release_sink(&self, generation: u64) {
        {
            let mut ep = self
                .endpoints
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if ep.generation == generation && ep.unclaimed_rx.is_none() {
                ep.tx = closed_sender();
            }
        }
        self.sink_active.store(false, Ordering::SeqCst);
    }

    /// Called on `ChannelSource::drop`: release the active flag cleanly.
    pub(crate) fn release_source(&self, generation: u64) {
        {
            let mut ep = self
                .endpoints
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if ep.generation == generation {
                ep.unclaimed_rx = None;
            }
        }
        self.source_active.store(false, Ordering::SeqCst);
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

        let (tx, generation) = {
            let mut ep = slot
                .endpoints
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if ep.tx.is_closed() && ep.unclaimed_rx.is_none() {
                let (new_tx, new_rx) = mpsc::channel(buffer);
                ep.tx = new_tx.clone();
                ep.unclaimed_rx = Some(new_rx);
                ep.generation += 1;
                (new_tx, ep.generation)
            } else {
                (ep.tx.clone(), ep.generation)
            }
        };
        Ok(Box::new(ChannelSink::with_slot(
            schema, buffer, tx, slot, generation,
        )))
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

        let (rx, generation) = {
            let mut ep = slot
                .endpoints
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(rx) = ep.unclaimed_rx.take() {
                if !ep.tx.is_closed() {
                    (rx, ep.generation)
                } else {
                    let (new_tx, new_rx) = mpsc::channel(buffer);
                    ep.tx = new_tx;
                    ep.unclaimed_rx = None;
                    ep.generation += 1;
                    (new_rx, ep.generation)
                }
            } else {
                let (new_tx, new_rx) = mpsc::channel(buffer);
                ep.tx = new_tx;
                ep.unclaimed_rx = None;
                ep.generation += 1;
                (new_rx, ep.generation)
            }
        };
        Ok(Box::new(ChannelSource::with_slot(
            schema, rx, slot, generation,
        )))
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

    #[tokio::test]
    async fn channel_restarts_preserve_flow_across_single_and_dual_rebuilds() {
        let registry = ChannelRegistry::default();

        // 1. Initial sink + source pair.
        let mut sink = registry.sink("bridge", schema(), 4).expect("sink1");
        let mut source = registry.source("bridge", schema(), 4).expect("source1");
        sink.write_batch(&batch(schema())).await.unwrap();
        assert_eq!(source.next_batch().await.unwrap().unwrap().num_rows(), 3);

        // 2. Sink-only restart while source remains alive.
        drop(sink);
        assert!(
            source.next_batch().await.unwrap().is_none(),
            "old source sees EOF while sink is down"
        );
        let mut sink2 = registry.sink("bridge", schema(), 4).expect("sink2");
        sink2.write_batch(&batch(schema())).await.unwrap();
        assert_eq!(
            source.next_batch().await.unwrap().unwrap().num_rows(),
            3,
            "existing source swaps seamlessly to restarted sink"
        );

        // 3. Source-only restart while sink2 remains alive.
        drop(source);
        let mut source2 = registry.source("bridge", schema(), 4).expect("source2");
        sink2.write_batch(&batch(schema())).await.unwrap();
        assert_eq!(
            source2.next_batch().await.unwrap().unwrap().num_rows(),
            3,
            "restarted source receives batches from live sink2"
        );

        // 4. Simultaneous dual rebuild where new source builds before old sink drops.
        let sink3 = registry.sink("bridge", schema(), 4);
        assert!(sink3.is_err(), "cannot create sink3 before sink2 drops");
        drop(source2);
        let mut source3 = registry.source("bridge", schema(), 4).expect("source3");
        drop(sink2);
        let mut sink3 = registry.sink("bridge", schema(), 4).expect("sink3");
        sink3.write_batch(&batch(schema())).await.unwrap();
        assert_eq!(source3.next_batch().await.unwrap().unwrap().num_rows(), 3);
    }
}
