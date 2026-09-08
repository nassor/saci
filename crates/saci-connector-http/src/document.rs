//! Turning one whole document body into batches, shared by the read halves.
//!
//! The transformer's stream read surface takes a `std::fs::File` because
//! parquet reads its footer before any row group, so a body has to land
//! somewhere seekable before it can be decoded. It lands in an unnamed temp
//! file the OS reclaims when the last handle closes, written chunk by chunk as
//! the body arrives: a 2 GiB response never exists in memory, and
//! `max_body_bytes` stops one before the disk fills.
//!
//! Opening the reader and draining it are blocking calls (a parquet footer read
//! is a seek), so both happen inside a `spawn_blocking` the caller owns.

use std::io::{Seek, Write};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Fields, Schema};
use tokio::sync::mpsc;

use saci_core::error::SaciError;
use saci_transformer::{BatchReader, Transformer};

use crate::client::Body;

/// Batches the reader thread may queue before it blocks. Matches
/// `saci-connector-file`'s.
pub(crate) const CHANNEL_CAPACITY: usize = 4;

/// Why spooling one body stopped, which is what the caller labels its metric
/// with: the cap is the peer sending too much, a failed read is the transport,
/// and a failed write is this host's disk.
pub(crate) enum SpoolError {
    /// The body, announced or discovered, exceeds `max_body_bytes`.
    OverCap(SaciError),
    /// The body stopped or failed mid-read.
    Transport(SaciError),
    /// The temp file could not be created, written or rewound.
    Spool(SaciError),
}

impl SpoolError {
    /// The `saci_http_request_errors_total` kind this counts as, `None` for a
    /// local write failure, which is neither a size nor a transport fault.
    pub(crate) fn kind(&self) -> Option<&'static str> {
        match self {
            Self::OverCap(_) => Some("size"),
            Self::Transport(_) => Some("transport"),
            Self::Spool(_) => None,
        }
    }

    /// The error to report, whichever side it came from.
    pub(crate) fn into_error(self) -> SaciError {
        match self {
            Self::OverCap(e) | Self::Transport(e) | Self::Spool(e) => e,
        }
    }
}

/// Spool a whole response body to a temp file, chunk by chunk.
///
/// `source` names where the body came from, a url or a peer, so the cap error
/// says which one overran. `cap` is `max_body_bytes`; a `content-length` above
/// it is refused before the first chunk is read, and a body that overruns it
/// without announcing a length is refused as soon as it does.
///
/// # Errors
///
/// Returns [`SpoolError`], which names whether the body exceeded `cap`, failed
/// mid-read, or could not be written to the temp file.
pub(crate) async fn spool(
    what: &str,
    source: &str,
    announced: Option<u64>,
    body: &mut Body,
    cap: Option<usize>,
) -> Result<(std::fs::File, u64), SpoolError> {
    if let (Some(cap), Some(announced)) = (cap, announced)
        && announced > cap as u64
    {
        return Err(SpoolError::OverCap(over_cap(what, source, cap)));
    }

    let file = tempfile::tempfile()
        .map_err(|e| SpoolError::Spool(SaciError::generic(format!("{what}: spool file: {e}"))))?;
    let mut spool = tokio::fs::File::from_std(file);
    let mut written: u64 = 0;
    // A body that stops short of its `content-length` reports the
    // transport's own wording, which names neither this connector nor what
    // was being read.
    let failed_read = |e: SaciError| {
        SpoolError::Transport(SaciError::generic(format!(
            "{what}: cannot read {source}: {}",
            e.message()
        )))
    };
    while let Some(chunk) = body.next_chunk().await.map_err(failed_read)? {
        written += chunk.len() as u64;
        if let Some(cap) = cap
            && written > cap as u64
        {
            return Err(SpoolError::OverCap(over_cap(what, source, cap)));
        }
        tokio::io::AsyncWriteExt::write_all(&mut spool, &chunk)
            .await
            .map_err(|e| {
                SpoolError::Spool(SaciError::generic(format!("{what}: spool write: {e}")))
            })?;
    }
    let mut file = spool.into_std().await;
    file.rewind()
        .map_err(|e| SpoolError::Spool(SaciError::generic(format!("{what}: spool rewind: {e}"))))?;
    Ok((file, written))
}

/// The same thing for a body already in memory, which is what the push source
/// has once it has collected a request under its own cap.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when the temp file cannot be created,
/// written or rewound.
pub(crate) fn spool_bytes(what: &str, body: &[u8]) -> Result<std::fs::File, SaciError> {
    let mut file =
        tempfile::tempfile().map_err(|e| SaciError::generic(format!("{what}: spool file: {e}")))?;
    file.write_all(body)
        .map_err(|e| SaciError::generic(format!("{what}: spool write: {e}")))?;
    file.rewind()
        .map_err(|e| SaciError::generic(format!("{what}: spool rewind: {e}")))?;
    Ok(file)
}

/// The one wording a body over `max_body_bytes` carries, whichever half read
/// it and whether the length was announced or discovered.
fn over_cap(what: &str, source: &str, cap: usize) -> SaciError {
    SaciError::generic(format!(
        "{what}: body from {source} exceeds max_body_bytes ({cap})"
    ))
}

/// Open the format's reader over a spooled body and settle its schema.
///
/// `declared` is what the format is handed, already resolved from the
/// `schema_from` rule: the declaration verbatim, or nothing so a
/// self-describing format reads its own. `expected` is what the graph was
/// validated against; when `check` is set, the schema the body turned out to
/// carry must equal it field for field.
///
/// Blocking: call inside `spawn_blocking`.
///
/// # Errors
///
/// Returns the format's own error when the body cannot be read, and
/// [`SaciError::Configuration`] when `check` is set and the body's schema
/// differs from `expected`.
pub(crate) fn open(
    what: &str,
    source: &str,
    transformer: &Arc<dyn Transformer>,
    spool: std::fs::File,
    declared: Option<Arc<Schema>>,
    check: bool,
    expected: &Schema,
) -> Result<Box<dyn BatchReader>, SaciError> {
    let reader = transformer.open_reader(spool, declared)?;
    if check {
        let carried = reader.schema();
        if carried.fields() != expected.fields() {
            return Err(SaciError::configuration(format!(
                "{what}: body from {source} carries schema [{}] but the config declared [{}]",
                render_fields(carried.fields()),
                render_fields(expected.fields()),
            )));
        }
    }
    Ok(reader)
}

/// Drain `reader` on a dedicated OS thread, pushing batches down `tx`.
///
/// One thread per document, not per batch: the decode is blocking and the
/// channel is bounded, so the executor never waits on it and the reader stops
/// once the consumer falls behind.
pub(crate) fn spawn_reader(
    mut reader: Box<dyn BatchReader>,
    tx: mpsc::Sender<Result<RecordBatch, SaciError>>,
) {
    std::thread::spawn(move || {
        loop {
            match reader.next_batch() {
                Ok(None) => break,
                Ok(Some(batch)) => {
                    // A send error means the consumer is gone (pipeline
                    // aborted), so this thread has nowhere left to go.
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
}

/// `Fields`'s own `Debug` is unreadable in an error a config author has to act
/// on; this renders `name: data_type` pairs.
pub(crate) fn render_fields(fields: &Fields) -> String {
    fields
        .iter()
        .map(|f| format!("{}: {}", f.name(), f.data_type()))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field};

    use super::*;

    #[test]
    fn fields_render_as_name_and_type_pairs() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]);
        assert_eq!(render_fields(schema.fields()), "id: Int64, label: Utf8");
    }

    #[test]
    fn an_in_memory_body_spools_to_a_rewound_file() {
        let mut file = spool_bytes("HttpPushSource", b"id\n1\n").expect("spool");
        let mut read = String::new();
        std::io::Read::read_to_string(&mut file, &mut read).expect("read back");
        assert_eq!(read, "id\n1\n", "the handle is rewound, not at the end");
    }
}
