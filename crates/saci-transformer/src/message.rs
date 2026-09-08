//! The message surface: [`MessageShape`] and [`MessageDecoder`].
//!
//! A message transport carries discrete payloads rather than one stream with an
//! end: a TCP frame, a Kafka record. Decoding a window of them into one
//! [`RecordBatch`] is the transformer's job; deciding
//! what a window is remains the connector's.

use arrow_array::RecordBatch;

use saci_core::error::SaciError;

/// How [`Transformer::encode_messages`](crate::Transformer::encode_messages)
/// splits a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageShape {
    /// One message per row. A transport may key each message off a column.
    PerRow,
    /// One message per batch.
    PerBatch,
}

/// Decode a window of discrete message payloads into one batch.
///
/// `flush` resets the decoder, so one decoder serves a connection or a
/// consumer for its whole life.
pub trait MessageDecoder: Send {
    /// Feed one payload. Errors name the payload that failed, and the caller
    /// adds its own transport coordinates.
    ///
    /// # Errors
    ///
    /// Returns the format's decode error for this payload.
    fn push(&mut self, payload: &[u8]) -> Result<(), SaciError>;

    /// Produce the batch for everything pushed since the last flush, `None`
    /// when that was no rows, then reset.
    ///
    /// # Errors
    ///
    /// Returns the format's error from finishing the window, such as a failed
    /// concatenation of per-payload batches.
    fn flush(&mut self) -> Result<Option<RecordBatch>, SaciError>;
}
