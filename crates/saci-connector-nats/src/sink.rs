//! [`NatsSink`]: a core NATS subject publisher or a JetStream publisher
//! [`Sink`].
//!
//! The resolved format's [`MessageShape`] decides whether a batch becomes one
//! message per row or one message in total. Only a row-per-message format can
//! honour `subject_field`, `header_fields` and `message_id_field`.
//!
//! # Delivery semantics
//!
//! A JetStream publish is acknowledged by the stream, and `write_batch` waits
//! for every ack, so a returned `write_batch` means the stream has the rows.
//! `mode.atomic_batch` makes one batch all-or-nothing instead of merely
//! acknowledged message by message. Core NATS has no per-message ack;
//! `flush_every_batch` waits for the server to acknowledge the whole write
//! instead, which is the strongest boundary the protocol offers.

use std::future::IntoFuture;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::{self, context::PublishAckFuture};
use async_nats::{Client, HeaderMap, HeaderName, HeaderValue, Subject, header};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::join_all;

use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_transformer::{MessageShape, Transformer};

use crate::config::{NatsSinkConfig, SinkMode};
use crate::connect::{connect, jetstream_context};
use crate::provision::resolve_stream;
use crate::render::render_column;

const WHAT: &str = "NatsSink";

/// JetStream refuses an atomic batch larger than the server's
/// `max_batch_size`, which defaults to 1000 messages.
const MAX_ATOMIC_BATCH_MESSAGES: usize = 1_000;

/// Everything the first `write_batch` opened.
enum Started {
    Core {
        client: Client,
    },
    /// The context owns its own `Client`, so the connection lives as long as
    /// the context does and `Context::client` hands a clone back when a core
    /// publish is needed.
    Jetstream {
        context: jetstream::Context,
    },
}

/// NATS [`Sink`]: one publish per encoded message.
///
/// Connects lazily: [`new`](Self::new) validates the config and opens nothing,
/// so `saci-service validate` stays broker-free. The first
/// [`write_batch`](Sink::write_batch) connects and, in JetStream mode, resolves
/// the stream.
pub struct NatsSink {
    cfg: NatsSinkConfig,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
    /// Cached from the transformer at construction, where the capability check
    /// already proved it is `Some`.
    shape: MessageShape,
    /// The subject a `PerBatch` format always uses, and the fallback when a
    /// rendered `subject_field` cell is null.
    default_subject: Subject,
    /// The `[headers]` table, plus `Nats-Expected-Stream` when configured.
    /// Parsed once: `HeaderMap::insert` panics on an illegal name or value.
    static_headers: HeaderMap,
    /// `header_fields`, with each header name already parsed.
    header_columns: Vec<(HeaderName, String)>,
    /// The stream sequence of the last message this sink's own ack confirmed,
    /// which `expected_last_sequence` sends on the next batch. `None` until the
    /// first confirmed batch, when the header would say nothing.
    last_sequence: Option<u64>,
    state: Option<Started>,
}

impl NatsSink {
    /// Validate the config and build the sink. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `cfg` fails validation, the
    /// format has no message codec, or a per-row key is set against a format
    /// that emits one message per batch.
    pub fn new(
        cfg: NatsSinkConfig,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, SaciError> {
        cfg.validate()?;

        // Both capability checks happen here rather than in `validate`: only
        // the resolved transformer knows its message shape.
        let format = transformer.format();
        let Some(shape) = transformer.message_shape() else {
            return Err(SaciError::configuration(format!(
                "{WHAT}: format '{format}' has no message codec"
            )));
        };

        let (subject, headers, header_fields, message_id_field, expected_stream) = match &cfg.mode {
            SinkMode::Core(core) => (
                &core.subject,
                &core.headers,
                &core.header_fields,
                None,
                None,
            ),
            SinkMode::Jetstream(js) => (
                &js.subject,
                &js.headers,
                &js.header_fields,
                js.message_id_field.as_deref(),
                js.expected_stream.then_some(js.stream.as_str()),
            ),
        };

        if shape == MessageShape::PerBatch
            && let Some(key) = [
                ("mode.subject_field", subject_field(&cfg.mode).is_some()),
                ("mode.header_fields", !header_fields.is_empty()),
                ("mode.message_id_field", message_id_field.is_some()),
            ]
            .into_iter()
            .find_map(|(key, set)| set.then_some(key))
        {
            return Err(SaciError::configuration(format!(
                "{WHAT} config: '{key}' needs a row-per-message format; '{format}' emits one \
                 message per batch"
            )));
        }

        // Every name and value here was proved legal by `cfg.validate`, so the
        // panicking `insert` cannot fire; the parses below keep that local.
        let mut static_headers = HeaderMap::new();
        for (name, value) in headers {
            static_headers.insert(header_name(name)?, header_value(value, name)?);
        }
        if let Some(stream) = expected_stream {
            static_headers.insert(
                header::NATS_EXPECTED_STREAM,
                header_value(stream, "mode.stream")?,
            );
        }
        let header_columns = header_fields
            .iter()
            .map(|(name, column)| Ok((header_name(name)?, column.clone())))
            .collect::<Result<Vec<_>, SaciError>>()?;

        let default_subject = Subject::from(subject.as_str());
        Ok(Self {
            cfg,
            schema,
            transformer,
            shape,
            default_subject,
            static_headers,
            header_columns,
            last_sequence: None,
            state: None,
        })
    }

    async fn ensure_started(&mut self) -> Result<(), SaciError> {
        if self.state.is_some() {
            return Ok(());
        }
        let client = connect(&self.cfg.connection, WHAT).await?;
        let started = match &self.cfg.mode {
            SinkMode::Core(_) => Started::Core { client },
            SinkMode::Jetstream(js) => {
                let context = jetstream_context(
                    client,
                    js.domain.as_deref(),
                    js.api_prefix.as_deref(),
                    Duration::from_millis(js.api_timeout_ms),
                    Duration::from_millis(js.ack_timeout_ms),
                    js.max_ack_inflight,
                    js.backpressure_on_inflight,
                );
                // Even with `create = false` this fetches the stream, so a
                // stream typo is a startup error rather than a black hole:
                // JetStream answers an unmatched subject with `no responders`.
                resolve_stream(
                    &context,
                    &js.stream,
                    &js.stream_provision,
                    // What this sink publishes to is the right subject set for a
                    // stream it creates.
                    std::slice::from_ref(&js.subject),
                    WHAT,
                )
                .await?;
                Started::Jetstream { context }
            }
        };
        self.state = Some(started);
        Ok(())
    }

    /// Encode the batch and publish every payload.
    async fn publish(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        let payloads = self.transformer.encode_messages(batch)?;
        if self.shape == MessageShape::PerRow && payloads.len() != batch.num_rows() {
            return Err(SaciError::generic(format!(
                "{WHAT}: format '{}' produced {} messages for {} rows",
                self.transformer.format(),
                payloads.len(),
                batch.num_rows()
            )));
        }

        // Rendered once per batch, not once per message.
        let subjects = match subject_field(&self.cfg.mode) {
            None => None,
            Some(field) => Some(render_column(batch, field, WHAT, "subject_field")?),
        };
        let message_ids = match message_id_field(&self.cfg.mode) {
            None => None,
            Some(field) => Some(render_column(batch, field, WHAT, "message_id_field")?),
        };
        let mut header_values = Vec::with_capacity(self.header_columns.len());
        for (name, column) in &self.header_columns {
            header_values.push((name, render_column(batch, column, WHAT, "header_fields")?));
        }

        match &self.cfg.mode {
            SinkMode::Core(core) => {
                let Some(Started::Core { client }) = &self.state else {
                    return Err(not_started());
                };
                let reply = core.reply_subject.as_deref().map(Subject::from);
                for (i, payload) in payloads.into_iter().enumerate() {
                    let subject = self.subject_at(subjects.as_deref(), i);
                    let headers = self.headers_at(&header_values, message_ids.as_deref(), i)?;
                    let payload = Bytes::from(payload);
                    let sent = match (&reply, headers) {
                        (None, None) => client.publish(subject.clone(), payload).await,
                        (None, Some(headers)) => {
                            client
                                .publish_with_headers(subject.clone(), headers, payload)
                                .await
                        }
                        (Some(reply), None) => {
                            client
                                .publish_with_reply(subject.clone(), reply.clone(), payload)
                                .await
                        }
                        (Some(reply), Some(headers)) => {
                            client
                                .publish_with_reply_and_headers(
                                    subject.clone(),
                                    reply.clone(),
                                    headers,
                                    payload,
                                )
                                .await
                        }
                    };
                    sent.map_err(|e| {
                        SaciError::generic(format!("{WHAT}: publish to '{subject}' failed: {e}"))
                    })?;
                }
                if core.flush_every_batch {
                    flush(client, core.flush_timeout_ms).await?;
                }
            }
            SinkMode::Jetstream(js) => {
                let Some(Started::Jetstream { context }) = &self.state else {
                    return Err(not_started());
                };
                if js.atomic_batch && payloads.len() > MAX_ATOMIC_BATCH_MESSAGES {
                    return Err(SaciError::generic(format!(
                        "{WHAT}: atomic batch of {} messages exceeds the JetStream limit of \
                         {MAX_ATOMIC_BATCH_MESSAGES}; nothing was sent",
                        payloads.len()
                    )));
                }
                if js.atomic_batch && payloads.len() > 1 {
                    // All-or-nothing: every message but the last is a core
                    // publish with no reply subject, because the server answers
                    // a batch's non-committing messages with a zero-byte ack
                    // that `send_publish` cannot parse. The last message is a
                    // request whose JSON pub-ack is the only proof the batch
                    // committed; the commit marker stores it like the rest.
                    let client = context.client();
                    let batch_id = client.new_inbox();
                    let n = payloads.len();
                    for (i, payload) in payloads.into_iter().enumerate() {
                        let subject = self.subject_at(subjects.as_deref(), i);
                        let mut headers = self
                            .headers_at(&header_values, message_ids.as_deref(), i)?
                            .unwrap_or_default();
                        headers.insert(
                            header::NATS_BATCH_ID,
                            header_value(&batch_id, "mode.atomic_batch batch id")?,
                        );
                        headers.insert(
                            header::NATS_BATCH_SEQUENCE,
                            HeaderValue::from((i + 1) as u64),
                        );
                        if i == 0
                            && js.expected_last_sequence
                            && let Some(seq) = self.last_sequence
                        {
                            headers.insert(
                                header::NATS_EXPECTED_LAST_SEQUENCE,
                                HeaderValue::from(seq),
                            );
                        }
                        if i + 1 == n {
                            headers.insert(
                                header::NATS_BATCH_COMMIT,
                                header_value(header::NATS_BATCH_COMMIT_FINAL, "batch commit")?,
                            );
                            let message = PublishMessage::build()
                                .payload(Bytes::from(payload))
                                .headers(headers);
                            let ack = context
                                .send_publish(subject.clone(), message)
                                .await
                                .map_err(|e| {
                                    SaciError::generic(format!(
                                        "{WHAT}: publish to '{subject}' failed: {e}"
                                    ))
                                })?;
                            let mut pending = vec![(subject, ack)];
                            self.last_sequence = drain_acks(&mut pending).await?;
                        } else {
                            client
                                .publish_with_headers(
                                    subject.clone(),
                                    headers,
                                    Bytes::from(payload),
                                )
                                .await
                                .map_err(|e| {
                                    SaciError::generic(format!(
                                        "{WHAT}: publish to '{subject}' failed: {e}"
                                    ))
                                })?;
                        }
                    }
                    return Ok(());
                }
                // Window the awaited acks at `max_ack_inflight`, so a batch
                // larger than the client's permit pool cannot deadlock on
                // `send_publish`'s `acquire_owned`: the permit for message n is
                // only returned once message n - max_ack_inflight has been
                // acknowledged.
                let window = js.max_ack_inflight;
                let mut pending: Vec<(Subject, PublishAckFuture)> = Vec::new();
                for (i, payload) in payloads.into_iter().enumerate() {
                    let subject = self.subject_at(subjects.as_deref(), i);
                    let mut message = PublishMessage::build().payload(Bytes::from(payload));
                    if let Some(headers) =
                        self.headers_at(&header_values, message_ids.as_deref(), i)?
                    {
                        message = message.headers(headers);
                    }
                    if js.expected_last_sequence
                        && i == 0
                        && let Some(seq) = self.last_sequence
                    {
                        message = message.expected_last_sequence(seq);
                    }
                    let ack = context
                        .send_publish(subject.clone(), message)
                        .await
                        .map_err(|e| {
                            SaciError::generic(format!(
                                "{WHAT}: publish to '{subject}' failed: {e}"
                            ))
                        })?;
                    pending.push((subject, ack));
                    if pending.len() >= window
                        && let Some(last) = drain_acks(&mut pending).await?
                    {
                        self.last_sequence = Some(last);
                    }
                }
                if let Some(last) = drain_acks(&mut pending).await? {
                    self.last_sequence = Some(last);
                }
            }
        }
        Ok(())
    }

    /// The subject for message `i`: the rendered cell when it is not null, else
    /// the configured subject. A `PerBatch` format always takes the latter,
    /// because `subject_field` is refused for it.
    fn subject_at(&self, subjects: Option<&[Option<String>]>, i: usize) -> Subject {
        match subjects
            .and_then(|rendered| rendered.get(i))
            .and_then(Option::as_deref)
        {
            Some(cell) => Subject::from(cell),
            None => self.default_subject.clone(),
        }
    }

    /// The headers for message `i`, `None` when there are none to send.
    ///
    /// With no per-row headers the static map is cloned, or skipped entirely
    /// when it is empty, so the common case allocates nothing.
    fn headers_at(
        &self,
        header_values: &[(&HeaderName, Vec<Option<String>>)],
        message_ids: Option<&[Option<String>]>,
        i: usize,
    ) -> Result<Option<HeaderMap>, SaciError> {
        let dynamic_id = message_ids
            .and_then(|rendered| rendered.get(i))
            .and_then(Option::as_deref);
        if header_values.is_empty() && dynamic_id.is_none() {
            return Ok(if self.static_headers.is_empty() {
                None
            } else {
                Some(self.static_headers.clone())
            });
        }
        let mut headers = self.static_headers.clone();
        for (name, rendered) in header_values {
            if let Some(cell) = rendered.get(i).and_then(Option::as_deref) {
                headers.insert((*name).clone(), header_value(cell, name.as_ref())?);
            }
        }
        if let Some(id) = dynamic_id {
            headers.insert(header::NATS_MESSAGE_ID, header_value(id, "message_id")?);
        }
        Ok(Some(headers))
    }
}

/// `subject_field`, whichever mode is configured.
fn subject_field(mode: &SinkMode) -> Option<&str> {
    match mode {
        SinkMode::Core(core) => core.subject_field.as_deref(),
        SinkMode::Jetstream(js) => js.subject_field.as_deref(),
    }
}

/// `message_id_field`, which only JetStream has.
fn message_id_field(mode: &SinkMode) -> Option<&str> {
    match mode {
        SinkMode::Core(_) => None,
        SinkMode::Jetstream(js) => js.message_id_field.as_deref(),
    }
}

fn header_name(name: &str) -> Result<HeaderName, SaciError> {
    HeaderName::from_str(name).map_err(|e| {
        SaciError::configuration(format!(
            "{WHAT} config: '{name}' is not a legal NATS header name: {e}"
        ))
    })
}

/// A rendered cell reaches this too, so an illegal value from the data is an
/// error rather than the panic `HeaderMap::insert` would raise.
fn header_value(value: &str, key: &str) -> Result<HeaderValue, SaciError> {
    HeaderValue::from_str(value).map_err(|e| {
        SaciError::generic(format!(
            "{WHAT}: '{key}' value is not a legal NATS header value: {e}"
        ))
    })
}

async fn flush(client: &Client, timeout_ms: u64) -> Result<(), SaciError> {
    let timeout = Duration::from_millis(timeout_ms);
    match tokio::time::timeout(timeout, client.flush()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(SaciError::generic(format!("{WHAT}: flush failed: {e}"))),
        Err(_elapsed) => Err(SaciError::generic(format!(
            "{WHAT}: flush timed out after {timeout_ms} ms"
        ))),
    }
}

fn not_started() -> SaciError {
    SaciError::generic(format!("{WHAT}: publish before start"))
}

/// Await every pending JetStream publish ack, returning the stream sequence of
/// the last one, or `None` when nothing was pending.
///
/// The windowing caller uses this to bound in-flight publishes at
/// `max_ack_inflight`: each `send_publish` takes one of the context's ack
/// permits and only returns it when its ack arrives, so publishing without
/// draining in a window would deadlock once the pool is exhausted. `None` is
/// what a batch whose size is an exact multiple of the window leaves behind,
/// and it must not be read as sequence 0.
async fn drain_acks(
    pending: &mut Vec<(Subject, PublishAckFuture)>,
) -> Result<Option<u64>, SaciError> {
    let (subjects, futures): (Vec<_>, Vec<_>) = std::mem::take(pending).into_iter().unzip();
    let results = join_all(futures.into_iter().map(IntoFuture::into_future)).await;
    let mut last = None;
    for (subject, result) in subjects.into_iter().zip(results) {
        let ack = result.map_err(|e| {
            SaciError::generic(format!(
                "{WHAT}: publish to '{subject}' was not acknowledged: {e}"
            ))
        })?;
        last = Some(ack.sequence);
    }
    Ok(last)
}

#[async_trait]
impl Sink for NatsSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.ensure_started().await?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.publish(batch).await
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        match (&self.state, &self.cfg.mode) {
            // Core NATS has no per-message ack, so one last flush is the only
            // durability boundary left.
            (Some(Started::Core { client }), SinkMode::Core(core)) => {
                flush(client, core.flush_timeout_ms).await
            }
            // A JetStream `write_batch` already awaited every ack it opened, so
            // there is nothing left to drain here.
            _ => Ok(()),
        }
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}
