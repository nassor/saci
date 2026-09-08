//! The [`Transformer`] trait and the [`unsupported`] error every capability a
//! format lacks returns.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;

use saci_core::error::SaciError;

use crate::batch::{BatchReader, BatchWriter};
use crate::message::{MessageDecoder, MessageShape};

/// A byte format. Connectors move bytes; transformers give them meaning.
///
/// A transformer has two surfaces and need not implement both. The stream
/// surface ([`open_reader`](Self::open_reader),
/// [`open_writer`](Self::open_writer)) is what the file connector uses. The
/// message surface ([`open_message_decoder`](Self::open_message_decoder),
/// [`encode_messages`](Self::encode_messages)) is what TCP and Kafka use. Every
/// method that a format does not support returns [`unsupported`], so a mismatch
/// is a configuration error naming the format and the capability.
pub trait Transformer: Send + Sync + 'static {
    /// The name a `format` key selects this transformer with.
    fn format(&self) -> &'static str;

    /// Open a reader over `input`.
    ///
    /// `input` is a `File` rather than a `Read` because Parquet reads its
    /// footer before any row group. `declared` is the schema the config named,
    /// `None` when it named none: a schemaless format rejects `None` unless it
    /// can infer, and a self-describing one treats `Some` as a projection
    /// target. A projection returns exactly those columns, one batch per
    /// source batch; a declared column the source does not carry is the
    /// format's own error.
    ///
    /// # Errors
    ///
    /// Returns [`unsupported`] when the format has no stream read surface, or
    /// the format's own error when the handle cannot be read.
    fn open_reader(
        &self,
        input: std::fs::File,
        declared: Option<Arc<Schema>>,
    ) -> Result<Box<dyn BatchReader>, SaciError> {
        let _ = (input, declared);
        Err(unsupported(self.format(), "reading a byte stream"))
    }

    /// Open a writer over `output`. Buffering is the format's business: the
    /// handle arrives unbuffered.
    ///
    /// # Errors
    ///
    /// Returns [`unsupported`] when the format has no stream write surface, or
    /// the format's own error when the writer cannot be initialised.
    fn open_writer(
        &self,
        output: Box<dyn std::io::Write + Send>,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn BatchWriter>, SaciError> {
        let _ = (output, schema);
        Err(unsupported(self.format(), "writing a byte stream"))
    }

    /// Start decoding discrete message payloads against `schema`.
    ///
    /// `schema` is required rather than optional, unlike
    /// [`open_reader`](Self::open_reader)'s `declared`: the host reads
    /// `Source::schema()` at build time, before a payload can arrive, so a
    /// message transport has no source schema to fall back on. A payload is
    /// projected onto `schema` the way a stream is.
    ///
    /// # Errors
    ///
    /// Returns [`unsupported`] when the format has no message decoder, or the
    /// format's own error when the decoder cannot be built.
    fn open_message_decoder(
        &self,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn MessageDecoder>, SaciError> {
        let _ = schema;
        Err(unsupported(self.format(), "decoding discrete messages"))
    }

    /// Encode one batch into the payloads to publish, in row order when
    /// [`message_shape`](Self::message_shape) is [`MessageShape::PerRow`].
    ///
    /// # Errors
    ///
    /// Returns [`unsupported`] when the format has no message encoder, or the
    /// format's own encode error.
    fn encode_messages(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SaciError> {
        let _ = batch;
        Err(unsupported(self.format(), "encoding discrete messages"))
    }

    /// How this transformer splits a batch into messages, `None` when it has
    /// no message surface.
    ///
    /// This is a declaration about the other two message methods, and an
    /// implementor must keep it true: return `Some` only when both
    /// [`open_message_decoder`](Self::open_message_decoder) and
    /// [`encode_messages`](Self::encode_messages) are implemented, `None` only
    /// when neither is. One answer serves both directions, so a transformer
    /// that implements exactly one of the two has no honest declaration to
    /// make.
    ///
    /// Every message connector — the Kafka, NATS and TCP sources and sinks —
    /// refuses its config at build time when this returns `None`, before a
    /// socket is opened or a batch written. So a `Some` that outruns the
    /// methods turns a configuration error into a first-batch failure with the
    /// pipeline already live, and a `None` that understates them refuses a
    /// config that would have worked.
    ///
    /// `message_shape_agrees_with_the_message_surface`
    /// (`crates/saci-service/tests/connector_matrix.rs`) probes both methods on
    /// every registered transformer and fails on any disagreement.
    fn message_shape(&self) -> Option<MessageShape> {
        None
    }
}

/// The error a transformer returns for a capability it does not have.
pub fn unsupported(format: &str, capability: &str) -> SaciError {
    SaciError::configuration(format!("format '{format}' does not support {capability}"))
}

#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};

    use super::*;

    /// A transformer with neither surface: every capability falls through to
    /// the defaulted `unsupported` error.
    struct BareTransformer;

    impl Transformer for BareTransformer {
        fn format(&self) -> &'static str {
            "bare"
        }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![1i64]))]).expect("batch")
    }

    #[test]
    fn unsupported_is_a_configuration_error_naming_format_and_capability() {
        let err = unsupported("csv", "decoding discrete messages");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'csv'"), "got: {err}");
        assert!(
            err.message().contains("decoding discrete messages"),
            "got: {err}"
        );
    }

    #[test]
    fn a_transformer_with_no_surfaces_reports_every_capability_unsupported() {
        let t = BareTransformer;
        assert_eq!(t.message_shape(), None);

        let Err(writer_err) = t.open_writer(Box::new(Vec::new()), schema()) else {
            panic!("a format with no stream write surface must not open a writer");
        };
        assert_eq!(writer_err.category(), "configuration");
        assert!(
            writer_err.message().contains("writing a byte stream"),
            "got: {writer_err}"
        );

        let Err(decoder_err) = t.open_message_decoder(schema()) else {
            panic!("a format with no message decoder must not open one");
        };
        assert!(
            decoder_err.message().contains("decoding discrete messages"),
            "got: {decoder_err}"
        );

        let encode_err = t.encode_messages(&batch()).expect_err("no message encoder");
        assert!(
            encode_err.message().contains("encoding discrete messages"),
            "got: {encode_err}"
        );

        for err in [&writer_err, &decoder_err, &encode_err] {
            assert!(err.message().contains("'bare'"), "got: {err}");
        }
    }

    #[test]
    fn the_defaulted_reader_reports_unsupported() {
        let file = tempfile::tempfile().expect("temp file");
        let Err(err) = BareTransformer.open_reader(file, None) else {
            panic!("a format with no stream read surface must not open a reader");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("reading a byte stream"),
            "got: {err}"
        );
    }
}
