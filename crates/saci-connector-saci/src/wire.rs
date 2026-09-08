//! The frame format a `saci` sink writes and a `saci` source reads.
//!
//! Every frame is a `u32` big-endian body length followed by that many body
//! bytes. The body's first byte is the frame kind. A `str` below is a `u16`
//! big-endian byte length followed by UTF-8 bytes.
//!
//! ```text
//! 1 HELLO  : u8 version (=1), str service, str workflow, str sink,
//!            u32 BE schema_len, schema bytes (an Arrow IPC Schema message)
//! 2 ACCEPT : (empty)
//! 3 REJECT : str reason
//! 4 DATA   : str traceparent (length 0 = none), then Arrow IPC stream bytes
//!            for one batch (the schema message rides on the session's first
//!            DATA frame, then dictionary and record batch messages)
//! ```
//!
//! The module is public because the protocol is: a foreign producer, or a test
//! harness standing in for one half, builds frames with it. A `Data` frame's
//! IPC bytes are a slice of the body [`Buffer`], so decoding copies nothing.

use std::sync::Arc;

use arrow_buffer::Buffer;
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::Schema;
use saci_connector::NodeIdentity;
use saci_core::error::SaciError;

/// The only protocol version this crate speaks.
pub const PROTOCOL_VERSION: u8 = 1;

const KIND_HELLO: u8 = 1;
const KIND_ACCEPT: u8 = 2;
const KIND_REJECT: u8 = 3;
const KIND_DATA: u8 = 4;

/// One decoded frame.
#[derive(Debug)]
pub enum Frame {
    /// A sink naming itself and the schema it will send.
    Hello {
        /// The protocol version the sender speaks.
        ///
        /// Always [`PROTOCOL_VERSION`] from [`encode_frame`]; a body carrying
        /// anything else reaches the source through [`hello_version`] first,
        /// so an unsupported version is refused by version rather than by
        /// whatever its unknown layout decodes to.
        version: u8,
        /// Which service, workflow and sink node is calling.
        identity: NodeIdentity,
        /// The schema every batch of this session carries.
        schema: Arc<Schema>,
    },
    /// The source took the session.
    Accept,
    /// The source refused the session, and why.
    Reject {
        /// Text a human reads in the sink's error.
        reason: String,
    },
    /// One batch, with the sender's trace context when it had one.
    Data {
        /// A W3C `traceparent`, or `None` when the sender exported no span.
        traceparent: Option<String>,
        /// Arrow IPC stream bytes, a zero-copy slice of the decoded body.
        ipc: Buffer,
    },
}

impl Frame {
    /// The kind byte this frame is encoded with, for an error naming what
    /// arrived where something else was expected.
    pub const fn kind(&self) -> u8 {
        match self {
            Self::Hello { .. } => KIND_HELLO,
            Self::Accept => KIND_ACCEPT,
            Self::Reject { .. } => KIND_REJECT,
            Self::Data { .. } => KIND_DATA,
        }
    }
}

/// Encode `schema` as an Arrow IPC Schema message flatbuffer.
fn schema_bytes(schema: &Schema) -> Vec<u8> {
    IpcDataGenerator::default()
        .schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut DictionaryTracker::new(false),
            &IpcWriteOptions::default(),
        )
        .ipc_message
}

/// Append a `u16` length and the UTF-8 bytes of `s`.
fn put_str(out: &mut Vec<u8>, s: &str) -> Result<(), SaciError> {
    let len = u16::try_from(s.len()).map_err(|_| {
        SaciError::generic(format!(
            "saci wire: a string of {} bytes does not fit the u16 length prefix",
            s.len()
        ))
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(s.as_bytes());
    Ok(())
}

/// Prefix `out[4..]` with its own length, which the caller reserved.
fn finish_length(out: &mut [u8], start: usize) -> Result<(), SaciError> {
    let body = out.len() - start - 4;
    let len = u32::try_from(body).map_err(|_| {
        SaciError::generic(format!(
            "saci wire: a frame of {body} bytes does not fit the u32 length prefix"
        ))
    })?;
    out[start..start + 4].copy_from_slice(&len.to_be_bytes());
    Ok(())
}

/// Append one whole frame, length prefix included, to `out`.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when a string or the whole body exceeds its
/// length prefix.
pub fn encode_frame(out: &mut Vec<u8>, frame: &Frame) -> Result<(), SaciError> {
    let start = out.len();
    out.extend_from_slice(&[0u8; 4]);
    match frame {
        Frame::Hello {
            version,
            identity,
            schema,
        } => {
            out.push(KIND_HELLO);
            out.push(*version);
            put_str(out, &identity.service)?;
            put_str(out, &identity.workflow)?;
            put_str(out, &identity.node)?;
            let bytes = schema_bytes(schema);
            let len = u32::try_from(bytes.len()).map_err(|_| {
                SaciError::generic(format!(
                    "saci wire: a frame of {} bytes does not fit the u32 length prefix",
                    bytes.len()
                ))
            })?;
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(&bytes);
        }
        Frame::Accept => out.push(KIND_ACCEPT),
        Frame::Reject { reason } => {
            out.push(KIND_REJECT);
            put_str(out, reason)?;
        }
        Frame::Data { traceparent, ipc } => {
            out.push(KIND_DATA);
            put_str(out, traceparent.as_deref().unwrap_or(""))?;
            out.extend_from_slice(ipc);
        }
    }
    finish_length(out, start)
}

/// The length prefix plus the DATA prelude for a body whose IPC part is
/// `ipc_len` bytes.
///
/// The caller writes the IPC buffers straight after it, so a batch is never
/// staged into a second contiguous buffer on its way to the socket.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when `traceparent` or the whole body exceeds
/// its length prefix.
pub fn data_header(traceparent: Option<&str>, ipc_len: usize) -> Result<Vec<u8>, SaciError> {
    let tp = traceparent.unwrap_or("");
    let mut out = Vec::with_capacity(4 + 1 + 2 + tp.len());
    out.extend_from_slice(&[0u8; 4]);
    out.push(KIND_DATA);
    put_str(&mut out, tp)?;
    let body = out.len() - 4 + ipc_len;
    let len = u32::try_from(body).map_err(|_| {
        SaciError::generic(format!(
            "saci wire: a frame of {body} bytes does not fit the u32 length prefix"
        ))
    })?;
    out[..4].copy_from_slice(&len.to_be_bytes());
    Ok(out)
}

/// Read a `u16`-prefixed string starting at `at`, advancing it past both.
fn take_str(body: &[u8], at: &mut usize, field: &str) -> Result<String, SaciError> {
    let len = take_u16(body, at)? as usize;
    let end = at.checked_add(len).filter(|end| *end <= body.len());
    let Some(end) = end else {
        return Err(truncated());
    };
    let s = std::str::from_utf8(&body[*at..end])
        .map_err(|_| SaciError::generic(format!("saci wire: invalid UTF-8 in {field}")))?
        .to_string();
    *at = end;
    Ok(s)
}

fn take_u16(body: &[u8], at: &mut usize) -> Result<u16, SaciError> {
    if *at + 2 > body.len() {
        return Err(truncated());
    }
    let v = u16::from_be_bytes([body[*at], body[*at + 1]]);
    *at += 2;
    Ok(v)
}

fn take_u32(body: &[u8], at: &mut usize) -> Result<u32, SaciError> {
    if *at + 4 > body.len() {
        return Err(truncated());
    }
    let v = u32::from_be_bytes([body[*at], body[*at + 1], body[*at + 2], body[*at + 3]]);
    *at += 4;
    Ok(v)
}

fn truncated() -> SaciError {
    SaciError::generic("saci wire: truncated frame")
}

/// Decode one body: the bytes after the `u32` length prefix.
///
/// [`Frame::Data`]'s `ipc` is a zero-copy slice of `body`.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] for a body that ends early, names an unknown
/// kind, carries a non-UTF-8 string, or carries schema bytes Arrow refuses.
pub fn decode_frame(body: Buffer) -> Result<Frame, SaciError> {
    let bytes = body.as_slice();
    let Some((&kind, _)) = bytes.split_first() else {
        return Err(truncated());
    };
    let mut at = 1usize;
    match kind {
        KIND_HELLO => {
            if at >= bytes.len() {
                return Err(truncated());
            }
            let version = bytes[at];
            at += 1;
            let identity = NodeIdentity {
                service: take_str(bytes, &mut at, "the hello's service")?,
                workflow: take_str(bytes, &mut at, "the hello's workflow")?,
                node: take_str(bytes, &mut at, "the hello's sink")?,
            };
            let schema_len = take_u32(bytes, &mut at)? as usize;
            let end = at.checked_add(schema_len).filter(|end| *end <= bytes.len());
            let Some(end) = end else {
                return Err(truncated());
            };
            let schema = arrow_ipc::convert::try_schema_from_flatbuffer_bytes(&bytes[at..end])
                .map_err(|e| SaciError::generic(format!("saci wire: bad schema: {e}")))?;
            Ok(Frame::Hello {
                version,
                identity,
                schema: Arc::new(schema),
            })
        }
        KIND_ACCEPT => Ok(Frame::Accept),
        KIND_REJECT => Ok(Frame::Reject {
            reason: take_str(bytes, &mut at, "the reject's reason")?,
        }),
        KIND_DATA => {
            let tp = take_str(bytes, &mut at, "the data frame's traceparent")?;
            Ok(Frame::Data {
                traceparent: (!tp.is_empty()).then_some(tp),
                ipc: body.slice(at),
            })
        }
        k => Err(SaciError::generic(format!(
            "saci wire: unknown frame kind {k}"
        ))),
    }
}

/// The protocol version a HELLO body announces, without decoding the rest.
///
/// `None` when `body` is not a hello frame, or is too short to carry a
/// version. The source reads this before it agrees to anything, so a peer
/// speaking a future version is refused by version rather than by whatever
/// its unknown layout decodes to.
pub fn hello_version(body: &[u8]) -> Option<u8> {
    match body {
        [KIND_HELLO, version, ..] => Some(*version),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_ipc::reader::StreamDecoder;
    use arrow_ipc::writer::StreamEncoder;
    use arrow_schema::{DataType, Field};

    fn identity() -> NodeIdentity {
        NodeIdentity {
            service: "svc-a".to_string(),
            workflow: "ticks".to_string(),
            node: "out".to_string(),
        }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    /// Encode one frame and hand back its body, the way a reader that already
    /// consumed the length prefix sees it.
    fn round_trip(frame: &Frame) -> Frame {
        let mut buf = Vec::new();
        encode_frame(&mut buf, frame).expect("encode");
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(len, buf.len() - 4, "the prefix must cover the body exactly");
        decode_frame(Buffer::from(buf[4..].to_vec())).expect("decode")
    }

    #[test]
    fn a_hello_round_trips_with_its_identity_and_schema() {
        let decoded = round_trip(&Frame::Hello {
            version: PROTOCOL_VERSION,
            identity: identity(),
            schema: schema(),
        });
        match decoded {
            Frame::Hello {
                version,
                identity: got,
                schema: got_schema,
            } => {
                assert_eq!(version, PROTOCOL_VERSION);
                assert_eq!(got, identity());
                assert_eq!(got_schema.fields(), schema().fields());
            }
            other => panic!("expected a hello, got {other:?}"),
        }
    }

    #[test]
    fn accept_and_reject_round_trip() {
        assert!(matches!(round_trip(&Frame::Accept), Frame::Accept));
        match round_trip(&Frame::Reject {
            reason: "schema mismatch".to_string(),
        }) {
            Frame::Reject { reason } => assert_eq!(reason, "schema mismatch"),
            other => panic!("expected a reject, got {other:?}"),
        }
    }

    #[test]
    fn a_data_frame_round_trips_to_an_equal_batch() {
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
            .expect("batch");
        let mut encoder = StreamEncoder::try_new(&schema()).expect("encoder");
        let ipc: Vec<u8> = encoder
            .encode(&batch)
            .expect("encode batch")
            .iter()
            .flat_map(|b| b.as_slice().to_vec())
            .collect();

        let decoded = round_trip(&Frame::Data {
            traceparent: Some(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
            ),
            ipc: Buffer::from(ipc),
        });
        let Frame::Data { traceparent, ipc } = decoded else {
            panic!("expected a data frame");
        };
        assert_eq!(
            traceparent.as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );

        let mut decoder = StreamDecoder::new();
        let mut buf = ipc;
        let mut out = Vec::new();
        while !buf.is_empty() {
            if let Some(b) = decoder.decode(&mut buf).expect("decode ipc") {
                out.push(b);
            }
        }
        assert_eq!(out, vec![batch]);
    }

    /// An absent traceparent is an empty string on the wire, not a flag.
    #[test]
    fn a_data_frame_without_a_traceparent_decodes_to_none() {
        let decoded = round_trip(&Frame::Data {
            traceparent: None,
            ipc: Buffer::from(vec![7u8, 8, 9]),
        });
        match decoded {
            Frame::Data { traceparent, ipc } => {
                assert!(traceparent.is_none());
                assert_eq!(ipc.as_slice(), &[7, 8, 9]);
            }
            other => panic!("expected a data frame, got {other:?}"),
        }
    }

    #[test]
    fn a_data_header_frames_the_same_body_as_encode_frame() {
        let ipc = vec![1u8, 2, 3, 4];
        let mut whole = Vec::new();
        encode_frame(
            &mut whole,
            &Frame::Data {
                traceparent: Some("tp".to_string()),
                ipc: Buffer::from(ipc.clone()),
            },
        )
        .expect("encode");
        let mut split = data_header(Some("tp"), ipc.len()).expect("header");
        split.extend_from_slice(&ipc);
        assert_eq!(whole, split);
    }

    #[test]
    fn a_truncated_body_is_named_as_truncated() {
        let err = decode_frame(Buffer::from(vec![KIND_REJECT, 0])).expect_err("must fail");
        assert_eq!(err.message(), "saci wire: truncated frame");
        let err = decode_frame(Buffer::from(Vec::<u8>::new())).expect_err("must fail");
        assert_eq!(err.message(), "saci wire: truncated frame");
    }

    #[test]
    fn an_unknown_kind_is_named_with_its_byte() {
        let err = decode_frame(Buffer::from(vec![99u8])).expect_err("must fail");
        assert_eq!(err.message(), "saci wire: unknown frame kind 99");
    }

    #[test]
    fn schema_bytes_arrow_refuses_are_named_as_a_bad_schema() {
        let mut body = vec![KIND_HELLO, PROTOCOL_VERSION];
        put_str(&mut body, "svc").expect("service");
        put_str(&mut body, "w").expect("workflow");
        put_str(&mut body, "out").expect("node");
        body.extend_from_slice(&3u32.to_be_bytes());
        body.extend_from_slice(&[0xff, 0xff, 0xff]);
        let err = decode_frame(Buffer::from(body)).expect_err("must fail");
        assert!(
            err.message().starts_with("saci wire: bad schema: "),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_utf8_string_names_its_field() {
        let mut body = vec![KIND_REJECT];
        body.extend_from_slice(&2u16.to_be_bytes());
        body.extend_from_slice(&[0xff, 0xfe]);
        let err = decode_frame(Buffer::from(body)).expect_err("must fail");
        assert_eq!(
            err.message(),
            "saci wire: invalid UTF-8 in the reject's reason"
        );
    }

    /// The reject reason is the one field a caller can make arbitrarily long,
    /// and its length prefix is a `u16`.
    #[test]
    fn a_string_past_the_u16_prefix_is_refused() {
        let err = encode_frame(
            &mut Vec::new(),
            &Frame::Reject {
                reason: "x".repeat(70_000),
            },
        )
        .expect_err("must fail");
        assert!(
            err.message().contains("does not fit the u16 length prefix"),
            "got: {err}"
        );
    }

    /// A hello whose declared schema length runs past the body it arrived in
    /// is truncated, not a schema Arrow gets to see.
    #[test]
    fn a_hello_whose_schema_length_overruns_the_body_is_truncated() {
        let mut body = vec![KIND_HELLO, PROTOCOL_VERSION];
        put_str(&mut body, "svc").expect("service");
        put_str(&mut body, "w").expect("workflow");
        put_str(&mut body, "out").expect("node");
        body.extend_from_slice(&64u32.to_be_bytes());
        body.extend_from_slice(&[0u8; 8]);
        let err = decode_frame(Buffer::from(body)).expect_err("must fail");
        assert_eq!(err.message(), "saci wire: truncated frame");
    }

    #[test]
    fn a_hello_carrying_only_its_kind_byte_is_truncated() {
        let err = decode_frame(Buffer::from(vec![KIND_HELLO])).expect_err("must fail");
        assert_eq!(err.message(), "saci wire: truncated frame");
    }

    #[test]
    fn a_hello_announces_its_version_before_the_rest_is_decoded() {
        let mut buf = Vec::new();
        encode_frame(
            &mut buf,
            &Frame::Hello {
                version: PROTOCOL_VERSION,
                identity: identity(),
                schema: schema(),
            },
        )
        .expect("encode");
        assert_eq!(hello_version(&buf[4..]), Some(PROTOCOL_VERSION));
        assert_eq!(hello_version(&[KIND_ACCEPT]), None);
        assert_eq!(
            decode_frame(Buffer::from(buf[4..].to_vec()))
                .expect("decode")
                .kind(),
            KIND_HELLO
        );
    }
}
