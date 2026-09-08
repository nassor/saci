//! Arrow IPC serialisation for [`Dataset`]: the bytes that cross the host and
//! processor boundary, and the bytes a checkpoint holds.
//!
//! The payload is not one Arrow IPC stream. It is a sequence of
//! length-prefixed streams closed by a zero sentinel:
//!
//! ```text
//! saci_stream := segment* terminator
//! segment    := u32le len ++ arrow_ipc_stream[len]
//! terminator := u32le 0
//! ```
//!
//! One segment per registered component, ordered by component name, then the
//! `__alive` bitmap segment, then the terminator. Each segment's Arrow schema
//! carries `__saci_component` naming the component and `__saci_schema_version`
//! holding its decimal `u32` version; the `__alive` segment carries only the
//! former.
//!
//! The byte level specification, down to flatbuffer field ids and per type
//! buffer layouts, is the `Host to processor wire format` page of the docs site.

use std::io::{Read, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_buffer::builder::BooleanBufferBuilder;
use arrow_schema::{DataType, Field, Schema};

use crate::component::Component;
use crate::error::SaciError;

use super::{
    ALIVE_BATCH_NAME, COMPONENT_NAME_KEY, Dataset, SCHEMA_VERSION_KEY, batch_to_ipc_bytes,
    intern_component_name, ipc_stream_to_batch,
};

impl Dataset {
    /// Stamp a component's identity onto a copy of its batch schema.
    ///
    /// `__saci_component` is what the reader dispatches on, and
    /// `__saci_schema_version` is what a processor compares against its own
    /// registration, so a segment without them is unreadable.
    fn annotate_batch(
        batch: &RecordBatch,
        name: &str,
        version: u32,
    ) -> Result<RecordBatch, SaciError> {
        let mut meta = batch.schema_ref().metadata().clone();
        meta.insert(COMPONENT_NAME_KEY.to_string(), name.to_string());
        meta.insert(SCHEMA_VERSION_KEY.to_string(), version.to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            batch
                .schema_ref()
                .fields()
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            meta,
        ));
        RecordBatch::try_new(schema, batch.columns().to_vec())
            .map_err(|e| SaciError::generic(format!("IPC rebuild batch error: {e}")))
    }

    /// Build the trailing `__alive` segment, length prefix included.
    ///
    /// This is the only producer of that segment: the liveness bitmap is a
    /// single non-nullable `Boolean` column named `alive`, and its length is
    /// what every component's row count is validated against on read.
    fn alive_ipc_bytes(&self) -> Result<Vec<u8>, SaciError> {
        let alive_array: ArrayRef = Arc::new(BooleanArray::new(self.alive.finish_cloned(), None));
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("alive", DataType::Boolean, false)],
            [(COMPONENT_NAME_KEY.to_string(), ALIVE_BATCH_NAME.to_string())]
                .into_iter()
                .collect(),
        ));
        let batch = RecordBatch::try_new(schema, vec![alive_array])
            .map_err(|e| SaciError::generic(format!("IPC alive batch error: {e}")))?;
        let seg = batch_to_ipc_bytes(&batch)?;
        let mut out = Vec::with_capacity(4 + seg.len());
        out.extend_from_slice(&(seg.len() as u32).to_le_bytes());
        out.extend_from_slice(&seg);
        Ok(out)
    }

    /// Serialise the whole dataset to an Arrow IPC stream.
    ///
    /// Each component is written as one `RecordBatch` with a metadata entry
    /// `__saci_component = <name>`. The alive bitmap is written last as a
    /// pseudo-batch under the `__alive` key.
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` on any Arrow IPC writer error.
    pub fn write_ipc<W: Write>(&self, writer: &mut W) -> Result<(), SaciError> {
        let mut names: Vec<&&str> = self.components.keys().collect();
        names.sort();

        for name in names {
            let batch = self.get_or_build_merged(name);
            let version = self.schemas.get_version(name).unwrap_or(1);
            let annotated = Self::annotate_batch(batch, name, version)?;
            let segment = batch_to_ipc_bytes(&annotated)?;
            let len = segment.len() as u32;
            writer
                .write_all(&len.to_le_bytes())
                .map_err(|e| SaciError::generic(format!("IPC write len: {e}")))?;
            writer
                .write_all(&segment)
                .map_err(|e| SaciError::generic(format!("IPC write segment: {e}")))?;
        }

        let alive_seg = self.alive_ipc_bytes()?;
        writer
            .write_all(&alive_seg)
            .map_err(|e| SaciError::generic(format!("IPC write alive: {e}")))?;

        writer
            .write_all(&0u32.to_le_bytes())
            .map_err(|e| SaciError::generic(format!("IPC write sentinel: {e}")))?;

        Ok(())
    }

    /// Serialise a single component's `RecordBatch` to an Arrow IPC stream.
    ///
    /// Produces the same length-prefixed framing as
    /// [`write_ipc`](Self::write_ipc), but writes only the named component, the
    /// alive bitmap and the terminating sentinel.
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` if the component is not registered or if
    /// any Arrow IPC write fails.
    pub fn write_component_ipc<C: Component>(&self, writer: &mut Vec<u8>) -> Result<(), SaciError> {
        let name = C::name();
        if !self.schemas.contains(name) {
            return Err(SaciError::generic(format!(
                "Dataset::write_component_ipc: component '{name}' is not registered"
            )));
        }

        let batch = self.get_or_build_merged(name);
        let version = self.schemas.get_version(name).unwrap_or(1);
        let annotated = Self::annotate_batch(batch, name, version)?;
        let segment = batch_to_ipc_bytes(&annotated)?;
        let len = segment.len() as u32;
        writer.extend_from_slice(&len.to_le_bytes());
        writer.extend_from_slice(&segment);

        writer.extend_from_slice(&self.alive_ipc_bytes()?);
        writer.extend_from_slice(&0u32.to_le_bytes());

        Ok(())
    }

    /// Reconstruct a `Dataset` from an IPC stream produced by [`write_ipc`](Self::write_ipc).
    ///
    /// The stream is untrusted: it may be operator-supplied, a processor's
    /// `run-batch` output, or a checkpoint read back from storage. Arrow-rs
    /// itself panics rather than erroring on some malformed buffer spans
    /// (documented on `Buffer::slice_with_length`'s `# Panics`), so the
    /// decode runs behind [`catch_unwind`] and turns any such panic into an
    /// `Err` instead of taking down the caller.
    ///
    /// A component may hold fewer rows than the dataset's row count — a
    /// windowing processor's output reduces N input rows to M result rows —
    /// but never more: the `__alive` bitmap is the dataset's row bound, and a
    /// component longer than it is corruption.
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` if the stream is malformed, truncated,
    /// contains batches without the `__saci_component` metadata key, decodes to
    /// a buffer whose declared span exceeds its message body, or holds a
    /// component with more rows than the alive bitmap.
    pub fn read_ipc<R: Read>(reader: &mut R) -> Result<Self, SaciError> {
        let mut dataset = Self::new();
        let mut alive_count: u32 = 0;

        loop {
            let mut len_buf = [0u8; 4];
            reader
                .read_exact(&mut len_buf)
                .map_err(|e| SaciError::generic(format!("IPC read len: {e}")))?;
            let len = u32::from_le_bytes(len_buf);

            if len == 0 {
                break;
            }

            // Decode straight out of the framed sub-reader. Materialising the
            // segment into a `Vec` first copies the body twice, because
            // `arrow-ipc`'s `MessageReader` copies it into an Arrow-owned
            // buffer regardless. `read_ipc` is generic over `R: Read` with no
            // `Seek`, so `take` is the only way to bound the sub-reader.
            //
            // `AssertUnwindSafe` is sound here: a panic mid-decode is fatal to
            // this whole `read_ipc` call (we return `Err` immediately, we do
            // not keep looping), so nothing observes `limited`, `reader` or
            // the partially built `dataset` in a state a panic left
            // inconsistent.
            let mut limited = (&mut *reader).take(u64::from(len));
            let batch = match catch_unwind(AssertUnwindSafe(|| ipc_stream_to_batch(&mut limited))) {
                Ok(result) => result?,
                Err(payload) => {
                    return Err(SaciError::generic(format!(
                        "IPC decode panicked: {}",
                        panic_payload_message(&*payload)
                    )));
                }
            };

            // The Arrow reader stops on the first record-batch message and
            // leaves the 8-byte end-of-stream marker unread. Drain whatever is
            // left of the segment, or the next iteration reads its length
            // prefix at the wrong offset. A short drain means the stream ended
            // inside the frame, which is a truncation error.
            let unread = limited.limit();
            if unread > 0 {
                let drained = std::io::copy(&mut limited, &mut std::io::sink())
                    .map_err(|e| SaciError::generic(format!("IPC read segment tail: {e}")))?;
                if drained != unread {
                    return Err(SaciError::generic(format!(
                        "IPC read segment: stream ended {} bytes inside a {len}-byte frame",
                        unread - drained
                    )));
                }
            }

            let name_str = batch
                .schema_ref()
                .metadata()
                .get(COMPONENT_NAME_KEY)
                .ok_or_else(|| {
                    SaciError::generic(format!(
                        "IPC batch missing '{}' metadata key",
                        COMPONENT_NAME_KEY
                    ))
                })?
                .clone();

            if name_str == ALIVE_BATCH_NAME {
                alive_count += 1;
                if alive_count > 1 {
                    return Err(SaciError::generic(
                        "IPC stream contains more than one '__alive' batch; stream is corrupted",
                    ));
                }

                let alive_col = batch.column(0);
                let bool_arr = alive_col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| SaciError::generic("IPC alive column is not BooleanArray"))?;
                let n = bool_arr.len();
                let mut builder = BooleanBufferBuilder::new(n);
                let mut dead = 0usize;
                for i in 0..n {
                    let v = bool_arr.value(i);
                    builder.append(v);
                    if !v {
                        dead += 1;
                    }
                }
                dataset.row_count = n;
                dataset.live_count = n - dead;
                dataset.alive = builder;
                dataset.dead_count = dead;
            } else {
                let on_disk_version: u32 = batch
                    .schema_ref()
                    .metadata()
                    .get(SCHEMA_VERSION_KEY)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1);

                let mut meta = batch.schema_ref().metadata().clone();
                meta.remove(COMPONENT_NAME_KEY);
                meta.remove(SCHEMA_VERSION_KEY);
                let clean_schema = Arc::new(Schema::new_with_metadata(
                    batch
                        .schema_ref()
                        .fields()
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>(),
                    meta,
                ));
                let clean_batch =
                    RecordBatch::try_new(clean_schema.clone(), batch.columns().to_vec())
                        .map_err(|e| SaciError::generic(format!("IPC clean batch error: {e}")))?;

                let static_name = intern_component_name(&name_str);

                dataset
                    .schemas
                    .register_raw(static_name, clean_schema, on_disk_version);
                dataset.components.insert(static_name, vec![clean_batch]);
            }
        }

        if alive_count == 0 {
            return Err(SaciError::generic(
                "IPC stream contains no '__alive' batch; stream is corrupted or incomplete",
            ));
        }

        // A component may hold *fewer* rows than the dataset's row count: that
        // is exactly what a windowing processor outputs, where N input rows
        // reduce to M result rows in a separate component, and it matches the
        // in-memory model, where `append` grows components independently. A
        // component with MORE rows than the alive bitmap is still corruption:
        // the bitmap is the dataset's row bound and no component may exceed it.
        for (name, chunks) in &dataset.components {
            let component_rows: usize = chunks.iter().map(|b| b.num_rows()).sum();
            if component_rows > dataset.row_count {
                return Err(SaciError::generic(format!(
                    "IPC component '{name}' has {component_rows} rows but alive bitmap has {}; \
                     stream is corrupted",
                    dataset.row_count
                )));
            }
        }

        Ok(dataset)
    }
}

/// Stringify a caught panic payload for inclusion in an error message.
///
/// `panic!`, `.unwrap()` and `.expect()` all payload a `&str` or `String`; the
/// same idiom is used at the `saci-plugin` ABI boundary.
fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::UInt64Array;
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::{ALIVE_BATCH_NAME, COMPONENT_NAME_KEY, Dataset, batch_to_ipc_bytes};
    use crate::row::Row;

    #[derive(Serialize, Deserialize)]
    struct IpcOrder {
        id: u64,
        total: f64,
    }

    impl Component for IpcOrder {
        fn name() -> &'static str {
            "IpcOrder"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("total", DataType::Float64, false),
            ]))
        }
    }

    #[test]
    fn test_ipc_round_trip() {
        let mut ds = Dataset::new();
        ds.register_component::<IpcOrder>().unwrap();
        for i in 0..500usize {
            ds.append::<IpcOrder>(&[IpcOrder {
                id: i as u64,
                total: i as f64,
            }])
            .unwrap();
        }
        ds.mark_dead(Row::new(1));
        ds.mark_dead(Row::new(3));

        let mut buf: Vec<u8> = Vec::new();
        ds.write_ipc(&mut buf).unwrap();

        let mut cursor = std::io::Cursor::new(&buf);
        let restored = Dataset::read_ipc(&mut cursor).unwrap();

        assert_eq!(restored.rows(), 500);
        assert_eq!(restored.live_rows(), 498);
    }

    fn build_ipc_buffer(
        component_rows: usize,
        alive_rows: Option<usize>,
        duplicate_alive: bool,
    ) -> Vec<u8> {
        use arrow_array::{ArrayRef, BooleanArray, Float64Array, RecordBatch};
        use arrow_schema::Schema;

        let mut buf: Vec<u8> = Vec::new();
        let ids: Vec<u64> = (0..component_rows as u64).collect();
        let totals: Vec<f64> = (0..component_rows).map(|i| i as f64).collect();
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("total", DataType::Float64, false),
            ],
            [(COMPONENT_NAME_KEY.to_string(), "IpcOrder".to_string())]
                .into_iter()
                .collect(),
        ));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(ids)) as ArrayRef,
                Arc::new(Float64Array::from(totals)) as ArrayRef,
            ],
        )
        .unwrap();
        let seg = batch_to_ipc_bytes(&batch).unwrap();
        buf.extend_from_slice(&(seg.len() as u32).to_le_bytes());
        buf.extend_from_slice(&seg);

        let write_alive = |out: &mut Vec<u8>, n: usize| {
            let alive_array: ArrayRef = Arc::new(BooleanArray::from(vec![true; n]));
            let alive_schema = Arc::new(Schema::new_with_metadata(
                vec![Field::new("alive", DataType::Boolean, false)],
                [(COMPONENT_NAME_KEY.to_string(), ALIVE_BATCH_NAME.to_string())]
                    .into_iter()
                    .collect(),
            ));
            let alive_batch = RecordBatch::try_new(alive_schema, vec![alive_array]).unwrap();
            let seg = batch_to_ipc_bytes(&alive_batch).unwrap();
            out.extend_from_slice(&(seg.len() as u32).to_le_bytes());
            out.extend_from_slice(&seg);
        };

        if let Some(n) = alive_rows {
            write_alive(&mut buf, n);
            if duplicate_alive {
                write_alive(&mut buf, n);
            }
        }

        buf.extend_from_slice(&0u32.to_le_bytes());
        buf
    }

    #[test]
    fn test_read_ipc_missing_alive_returns_error() {
        let buf = build_ipc_buffer(10, None, false);
        let mut cursor = std::io::Cursor::new(&buf);
        let result = Dataset::read_ipc(&mut cursor);
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("__alive"));
    }

    #[test]
    fn test_read_ipc_duplicate_alive_returns_error() {
        let buf = build_ipc_buffer(10, Some(10), true);
        let mut cursor = std::io::Cursor::new(&buf);
        let result = Dataset::read_ipc(&mut cursor);
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("__alive"));
    }

    #[test]
    fn test_read_ipc_row_count_mismatch_returns_error() {
        // A component with MORE rows than the alive bitmap is corruption: the
        // bitmap is the dataset's row bound.
        let buf = build_ipc_buffer(10, Some(5), false);
        let mut cursor = std::io::Cursor::new(&buf);
        let result = Dataset::read_ipc(&mut cursor);
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("IpcOrder"));
    }

    #[test]
    fn test_read_ipc_shorter_component_is_accepted() {
        // A component with FEWER rows than the alive bitmap is a results
        // component: exactly what a windowing processor outputs when N input
        // rows reduce to M aggregate rows. The component keeps its own length;
        // the dataset's row count is the maximum.
        let buf = build_ipc_buffer(3, Some(10), false);
        let mut cursor = std::io::Cursor::new(&buf);
        let restored = Dataset::read_ipc(&mut cursor).expect("shorter component decodes");
        assert_eq!(restored.rows(), 10);
        let orders = restored
            .batch_for("IpcOrder")
            .expect("IpcOrder component present");
        assert_eq!(orders.num_rows(), 3);
    }

    /// A length prefix that promises more bytes than the stream holds must be
    /// an error, not a short batch. A body that ends early surfaces as an Arrow
    /// read failure; a frame that outruns the stream after the batch surfaces
    /// from the tail drain.
    #[test]
    fn test_read_ipc_truncated_segment_returns_error() {
        let buf = build_ipc_buffer(10, Some(10), false);
        // Keep the 4-byte length prefix, drop the last 32 bytes of the body.
        let truncated = &buf[..buf.len() - 32];
        let mut cursor = std::io::Cursor::new(truncated);
        let result = Dataset::read_ipc(&mut cursor);
        assert!(result.is_err(), "truncated stream must not decode");
        let msg = result.err().unwrap().to_string();
        assert!(msg.contains("IPC"), "unexpected error: {msg}");
    }

    /// Truncation inside the *first* segment's body, where no later structural
    /// check can mask the framing error.
    #[test]
    fn test_read_ipc_truncated_first_segment_returns_error() {
        let buf = build_ipc_buffer(10, Some(10), false);
        let first_len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
        let truncated = &buf[..4 + first_len / 2];
        let mut cursor = std::io::Cursor::new(truncated);
        let result = Dataset::read_ipc(&mut cursor);
        assert!(result.is_err(), "truncated first segment must not decode");
        let msg = result.err().unwrap().to_string();
        assert!(msg.contains("IPC"), "unexpected error: {msg}");
    }

    /// Several component segments plus the alive segment in one stream. The
    /// decoder leaves each segment's end-of-stream marker unread, so a wrong
    /// tail drain would read the second length prefix at the wrong offset and
    /// garble every segment after the first.
    #[test]
    fn test_read_ipc_multiple_component_segments() {
        #[derive(Serialize, Deserialize)]
        struct IpcWide {
            a: u64,
            b: u64,
            c: f64,
        }
        impl Component for IpcWide {
            fn name() -> &'static str {
                "IpcWide"
            }
            fn schema() -> Arc<Schema> {
                Arc::new(Schema::new(vec![
                    Field::new("a", DataType::UInt64, false),
                    Field::new("b", DataType::UInt64, false),
                    Field::new("c", DataType::Float64, false),
                ]))
            }
        }

        let mut ds = Dataset::new();
        ds.register_component::<IpcOrder>().unwrap();
        ds.register_component::<IpcWide>().unwrap();
        for i in 0..300usize {
            ds.append::<IpcOrder>(&[IpcOrder {
                id: i as u64,
                total: i as f64 * 1.5,
            }])
            .unwrap();
            ds.append::<IpcWide>(&[IpcWide {
                a: i as u64,
                b: (i * 2) as u64,
                c: i as f64,
            }])
            .unwrap();
        }
        ds.mark_dead(Row::new(7));

        let mut buf: Vec<u8> = Vec::new();
        ds.write_ipc(&mut buf).unwrap();

        let mut cursor = std::io::Cursor::new(&buf);
        let restored = Dataset::read_ipc(&mut cursor).unwrap();
        assert_eq!(restored.rows(), 300);
        assert_eq!(restored.live_rows(), 299);

        // Both components survived with their values intact.
        let orders = restored.column::<IpcOrder>("total").unwrap();
        let wide = restored.column::<IpcWide>("b").unwrap();
        assert_eq!(orders.len(), 300);
        assert_eq!(wide.len(), 300);
    }

    #[test]
    fn test_ipc_intern_on_repeated_reload() {
        let mut ds = Dataset::new();
        ds.register_component::<IpcOrder>().unwrap();
        ds.append::<IpcOrder>(&[IpcOrder { id: 0, total: 0.0 }])
            .unwrap();

        let mut buf: Vec<u8> = Vec::new();
        ds.write_ipc(&mut buf).unwrap();

        let mut cursor1 = std::io::Cursor::new(&buf);
        let restored1 = Dataset::read_ipc(&mut cursor1).unwrap();
        let name1: &'static str = restored1.components.keys().next().unwrap();

        let mut cursor2 = std::io::Cursor::new(&buf);
        let restored2 = Dataset::read_ipc(&mut cursor2).unwrap();
        let name2: &'static str = restored2.components.keys().next().unwrap();

        assert!(std::ptr::eq(name1, name2));
    }

    /// Reads a `.saci` vector from the committed cross-language conformance
    /// corpus. Regenerate the corpus with
    /// `cargo run -p saci-service --features conformance --example conformance_vectors -- emit`
    /// after any wire-format change; see `packages/arrow-ipc-conformance/README.md`.
    fn conformance_vector(name: &str) -> Vec<u8> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../packages/arrow-ipc-conformance/vectors")
            .join(name);
        std::fs::read(&path)
            .unwrap_or_else(|e| panic!("read conformance vector {}: {e}", path.display()))
    }

    /// A buffer whose declared span reaches past its message body must be
    /// refused, not fed to `arrow-buffer`'s `Buffer::slice_with_length`, whose
    /// `# Panics` contract asserts `offset + length <= self.length`.
    #[test]
    fn test_read_ipc_buffer_overruns_body_returns_error() {
        let bytes = conformance_vector("buffer_overruns_body.saci");
        let result = Dataset::read_ipc(&mut &bytes[..]);
        assert!(
            result.is_err(),
            "an oversized buffer span must error, not panic"
        );
    }

    /// Same failure mode as `buffer_overruns_body`, reached through a negative
    /// length that wraps to a huge `usize` offset instead of an outsized one.
    #[test]
    fn test_read_ipc_negative_buffer_length_returns_error() {
        let bytes = conformance_vector("negative_buffer_length.saci");
        let result = Dataset::read_ipc(&mut &bytes[..]);
        assert!(
            result.is_err(),
            "a negative buffer length must error, not panic"
        );
    }
}
