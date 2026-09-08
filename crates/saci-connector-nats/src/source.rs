//! [`NatsSource`]: a core NATS subscription or a JetStream pull consumer
//! [`Source`].
//!
//! Live by default: with `stop_at_end = false`,
//! [`next_batch`](Source::next_batch) blocks on the first message and never
//! returns `Ok(None)`, exactly like a live TCP ingestion source idling for a
//! connection. Setting `stop_at_end = true` makes it usable from the batch run
//! modes.
//!
//! # Delivery semantics
//!
//! JetStream is at-least-once. `Source` has no ack hook, so the acks for one
//! batch are sent at the start of the next `next_batch` call, not when the batch
//! is handed over; a crash between the two redelivers that batch.
//!
//! Core NATS is at-most-once and has no ack at all: a message consumed while the
//! pipeline later fails is gone. A `queue_group` spreads one core subject across
//! several SACI instances.
//!
//! A window the format cannot decode is reported rather than skipped, and
//! nothing in it is acknowledged, so JetStream redelivers it. The same
//! payload fails the same way, so `mode.max_decode_attempts` bounds that:
//! on the last permitted delivery the offending message is terminated and the
//! error names the stream sequence it retired, which is what keeps one poison
//! message from being either swallowed in silence or redelivered forever.
//!
//! [`next_batch`](Source::next_batch) is cancel-safe either way: the collected
//! window lives on the source rather than in the future, and a message's ack
//! travels beside it until the window is handed over. Dropping that future — as
//! `run_stream`'s one-second source prime does — therefore loses no message
//! that core NATS would never send again, and acknowledges none that no caller
//! has seen.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::message::{AckKind, Acker};
use async_nats::{Client, Subject, Subscriber};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::{SelectAll, select_all};
use tokio::time::{Instant, timeout_at};

use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_transformer::Transformer;

use crate::config::{AckPolicyConfig, NatsSourceConfig, SourceMode};
use crate::connect::{connect, jetstream_context};
use crate::provision::resolve_stream;

const WHAT: &str = "NatsSource";

/// How long to wait between a `stop_at_end` window that came back empty and
/// the next attempt, while the server still reports messages outstanding for
/// this consumer.
const DRAIN_CONFIRM_BACKOFF: Duration = Duration::from_millis(50);

/// How a collected JetStream message is acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckMode {
    /// `ack_policy = "none"`: nothing is acknowledged, so no `Acker` is kept.
    Never,
    /// One `+ACK` per message, not waited for.
    Single,
    /// One `+ACK` per message, confirmed by the server.
    Double,
}

/// Where one collected payload came from, for a decode error's coordinates.
enum Origin {
    /// The core subject it arrived on.
    Subject(Subject),
    /// Its sequence in the stream.
    Sequence(u64),
}

/// One collected message: its payload, enough to name it in an error, and the
/// acknowledgement that belongs to it.
struct Received {
    origin: Origin,
    payload: Bytes,
    /// This message's `Acker`, kept beside the payload rather than on the
    /// source's pending list so a window nobody received cannot be
    /// acknowledged. `None` in core mode, which has no acks at all, and under
    /// `ack_policy = "none"`.
    ack: Option<Acker>,
    /// The server's own count of delivery attempts for this message, 1 on a
    /// first delivery. This is what bounds redelivery of a payload the format
    /// cannot decode. 0 in core mode, which never redelivers anything.
    delivered: u32,
}

/// A window the format rejected, and which message it blames.
struct DecodeFailure {
    /// The message the decoder rejected, as an index into the window. `None`
    /// when the failure belongs to the window as a whole: opening the decoder,
    /// flushing it, or a window that decoded no rows at all.
    index: Option<usize>,
    error: SaciError,
}

/// What the pull consumer needs per window, resolved once at start.
struct PullWindow {
    /// Stream name, for a decode error's coordinates.
    stream: String,
    /// Consumer name, same. Empty for an unnamed ephemeral consumer.
    consumer: String,
    /// How long a `stop_at_end` pull request stays open server-side.
    expires: Duration,
    /// Byte ceiling on one window. 0 leaves the library default.
    max_bytes: usize,
    /// Idle heartbeat interval. 0 leaves the library default.
    heartbeat: Duration,
}

/// Everything the first `next_batch` opened.
enum Started {
    Core {
        /// Held so the connection outlives an explicit unsubscribe.
        _client: Client,
        /// One subscriber per comma-separated subject, merged into one stream.
        subscribers: SelectAll<Subscriber>,
    },
    /// The consumer owns a JetStream context, which owns its own `Client` clone,
    /// so the connection lives as long as the consumer does. Boxed because a
    /// `PullConsumer` carries the server's whole consumer info.
    Jetstream {
        consumer: Box<PullConsumer>,
        window: PullWindow,
    },
}

/// NATS [`Source`]: one `RecordBatch` per collected window of messages.
///
/// Connects lazily: [`new`](Self::new) validates the config and opens nothing,
/// so `saci-service validate` stays broker-free. The first
/// [`next_batch`](Source::next_batch) connects, provisions the stream when
/// asked to, and creates the subscription or consumer. The stream runner
/// primes every source's first poll at start, so a fan-in source's
/// subscription is open before the rotation blocks on any one of them; core
/// NATS drops a message published with no subscriber, so without that prime
/// the first message on a second fan-in subject is lost.
pub struct NatsSource {
    /// Effective fetch size for the next collected window. `batch_size` from
    /// config seeds this, but a host that sends
    /// [`Source::request_batch_rows`] overwrites it before the first poll, not
    /// after some initial one: under `saci-service`'s flow control the
    /// configured value never reaches the server. It stays authoritative when
    /// flow control is disabled for this source, and when the source feeds a
    /// windowed node, which `saci-service` never hints.
    cfg: NatsSourceConfig,
    schema: Arc<Schema>,
    /// The payload codec. A decoder is opened per window rather than held for
    /// the source's life: `Source` is `Send + Sync`, and a decoder need only be
    /// `Send`.
    transformer: Arc<dyn Transformer>,
    /// How a JetStream message is acknowledged. [`AckMode::Never`] in core mode,
    /// which has no acks at all.
    ack_mode: AckMode,
    state: Option<Started>,
    /// The window collected but not yet handed to the caller, each message
    /// still carrying its own `Acker`. It lives here rather than in
    /// [`next_batch`](Source::next_batch)'s future so dropping that future —
    /// as `run_stream`'s one-second source prime does — neither loses the
    /// messages, which core NATS would never redeliver, nor acknowledges
    /// them: an `Acker` reaches `pending_acks` only when its message reaches
    /// the caller.
    buffer: Vec<Received>,
    /// Ackers for the batch already handed to the caller, sent at the start of
    /// the next call. This is what makes JetStream delivery at-least-once.
    pending_acks: Vec<Acker>,
    /// Delivery attempts spent on one message before the source terminates it
    /// as undecodable. 0 disables the bound, which is what core mode gets:
    /// core NATS redelivers nothing, so no window can come back.
    max_decode_attempts: u32,
    /// JetStream's own count of messages still waiting for this consumer.
    last_pending: Option<usize>,
}

impl NatsSource {
    /// Validate the config and build the source. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `cfg` fails validation or
    /// `transformer` has no message codec.
    pub fn new(
        cfg: NatsSourceConfig,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, SaciError> {
        cfg.validate()?;
        if transformer.message_shape().is_none() {
            return Err(SaciError::configuration(format!(
                "{WHAT}: format '{}' has no message codec",
                transformer.format()
            )));
        }
        let ack_mode = match &cfg.mode {
            SourceMode::Core(_) => AckMode::Never,
            SourceMode::Jetstream(js) => match (js.ack_policy, js.double_ack) {
                (AckPolicyConfig::None, _) => AckMode::Never,
                (_, true) => AckMode::Double,
                (_, false) => AckMode::Single,
            },
        };
        let max_decode_attempts = match &cfg.mode {
            // Core NATS has no ack and no redelivery, so an undecodable
            // window is reported once and gone; there is nothing to bound.
            SourceMode::Core(_) => 0,
            SourceMode::Jetstream(js) => {
                // A `max_deliver` the operator set is the server's own
                // ceiling, and the server retires the message silently once
                // it is reached. Clamping to it keeps the last attempt this
                // source's, so the message is named rather than dropped
                // behind an advisory nobody here subscribes to.
                let server_ceiling = u32::try_from(js.max_deliver).unwrap_or(u32::MAX);
                if js.max_deliver > 0 {
                    js.max_decode_attempts.min(server_ceiling)
                } else {
                    js.max_decode_attempts
                }
            }
        };
        Ok(Self {
            cfg,
            schema,
            transformer,
            ack_mode,
            state: None,
            buffer: Vec::new(),
            pending_acks: Vec::new(),
            max_decode_attempts,
            last_pending: None,
        })
    }

    async fn ensure_started(&mut self) -> Result<(), SaciError> {
        if self.state.is_some() {
            return Ok(());
        }
        let client = connect(&self.cfg.connection, WHAT).await?;
        let started = match &self.cfg.mode {
            SourceMode::Core(core) => {
                let mut subscribers = Vec::new();
                for subject in core.subject.split(',').map(str::trim) {
                    let subject = Subject::from(subject);
                    let subscriber = match &core.queue_group {
                        Some(group) => client.queue_subscribe(subject.clone(), group.clone()).await,
                        None => client.subscribe(subject.clone()).await,
                    };
                    subscribers.push(subscriber.map_err(|e| {
                        SaciError::generic(format!("{WHAT}: cannot subscribe to '{subject}': {e}"))
                    })?);
                }
                Started::Core {
                    _client: client,
                    // A single subject stays a one-element merge, so the
                    // collect loop has one code path.
                    subscribers: select_all(subscribers),
                }
            }
            SourceMode::Jetstream(js) => {
                let context = jetstream_context(
                    client,
                    js.domain.as_deref(),
                    js.api_prefix.as_deref(),
                    Duration::from_millis(js.api_timeout_ms),
                    // The source publishes nothing, so the ack-inflight
                    // settings the sink exposes are irrelevant here and keep
                    // the client's own defaults.
                    Duration::from_secs(30),
                    5_000,
                    true,
                );
                let stream = resolve_stream(
                    &context,
                    &js.stream,
                    &js.stream_provision,
                    // What this source reads is the right subject set for a
                    // stream it creates.
                    &js.filter_subjects,
                    WHAT,
                )
                .await?;
                let config = js.to_consumer_config()?;
                let name = config.name.clone().unwrap_or_default();
                let consumer: PullConsumer = match &js.durable_name {
                    // A durable consumer is resumed if it already exists, which
                    // is what makes a restart continue where it left off.
                    Some(durable) => stream.get_or_create_consumer(durable, config).await,
                    None => stream.create_consumer(config).await,
                }
                .map_err(|e| {
                    SaciError::generic(format!(
                        "{WHAT}: cannot create consumer on stream '{}': {e}",
                        js.stream
                    ))
                })?;
                Started::Jetstream {
                    consumer: Box::new(consumer),
                    window: PullWindow {
                        stream: js.stream.clone(),
                        consumer: name,
                        expires: Duration::from_millis(js.fetch_expires_ms),
                        max_bytes: js.fetch_max_bytes,
                        heartbeat: Duration::from_millis(js.heartbeat_ms),
                    },
                }
            }
        };
        self.state = Some(started);
        Ok(())
    }

    /// Acknowledge the previous batch, if one is pending. Acknowledging here,
    /// at the start of the next call rather than when the batch was produced,
    /// is what makes delivery at-least-once.
    async fn flush_pending_acks(&mut self) -> Result<(), SaciError> {
        if self.ack_mode == AckMode::Never {
            self.pending_acks.clear();
            return Ok(());
        }
        for acker in self.pending_acks.drain(..) {
            let result = if self.ack_mode == AckMode::Double {
                acker.double_ack().await
            } else {
                acker.ack().await
            };
            result.map_err(|e| SaciError::generic(format!("{WHAT}: ack failed: {e}")))?;
        }
        Ok(())
    }

    /// Fill [`self.buffer`](Self::buffer) with a window, resuming whatever a
    /// cancelled call left in it.
    async fn collect(&mut self) -> Result<(), SaciError> {
        let Self {
            cfg,
            ack_mode,
            state,
            buffer,
            last_pending,
            ..
        } = self;
        let state = state
            .as_mut()
            .ok_or_else(|| SaciError::generic(format!("{WHAT}: collect before start")))?;
        match state {
            Started::Core { subscribers, .. } => collect_core(subscribers, cfg, buffer).await,
            Started::Jetstream {
                consumer, window, ..
            } => {
                collect_jetstream(
                    consumer.as_mut(),
                    window,
                    cfg,
                    *ack_mode,
                    buffer,
                    last_pending,
                )
                .await
            }
        }
    }

    /// Feed a whole window through one decoder and take the batch out.
    ///
    /// Each push names the message it failed on, keeping per-message
    /// attribution; the transformer names the format that rejected it. That
    /// attribution is also what [`retire_exhausted`](Self::retire_exhausted)
    /// bounds redelivery by, so the failure carries the offending message's
    /// index rather than only its label.
    fn decode_window(&self, received: &[Received]) -> Result<RecordBatch, DecodeFailure> {
        let whole_window = |error| DecodeFailure { index: None, error };
        let mut decoder = self
            .transformer
            .open_message_decoder(Arc::clone(&self.schema))
            .map_err(whole_window)?;
        for (index, message) in received.iter().enumerate() {
            decoder.push(&message.payload).map_err(|e| DecodeFailure {
                index: Some(index),
                error: SaciError::generic(format!(
                    "{WHAT}: decode failed for {}: {e}",
                    self.origin_label(&message.origin)
                )),
            })?;
        }
        decoder.flush().map_err(whole_window)?.ok_or_else(|| {
            whole_window(SaciError::generic(format!(
                "{WHAT}: window decoded no rows"
            )))
        })
    }

    /// Terminate the message a decode failure blames, once the server has
    /// delivered it as often as `max_decode_attempts` allows, and name it in
    /// the returned error.
    ///
    /// An undecodable window is never acknowledged, so the server redelivers
    /// it after each `ack_wait` and the same payload fails the same way: with
    /// no bound the consumer never advances past it. On the last permitted
    /// attempt this sends `+TERM` for that one message, which stops its
    /// redelivery without acknowledging it, and reports which stream sequence
    /// was retired. Every other message in the window keeps its own `Acker`,
    /// which is dropped here, so those come back on the next window as usual.
    ///
    /// Before the bound is reached, and whenever the failure belongs to no one
    /// message, the decode error is returned as it stands and the window is
    /// redelivered.
    async fn retire_exhausted(&self, window: Vec<Received>, failure: DecodeFailure) -> SaciError {
        if self.max_decode_attempts == 0 {
            return failure.error;
        }
        // A failure the decoder could not attribute still belongs to a
        // one-message window: there is nothing else in it to blame.
        let index = failure
            .index
            .or_else(|| (window.len() == 1).then_some(0usize));
        let Some(message) = index.and_then(|index| window.get(index)) else {
            return failure.error;
        };
        if message.delivered < self.max_decode_attempts {
            return failure.error;
        }
        let Some(acker) = message.ack.as_ref() else {
            return failure.error;
        };
        let terminated = if self.ack_mode == AckMode::Double {
            acker.double_ack_with(AckKind::Term).await
        } else {
            acker.ack_with(AckKind::Term).await
        };
        let label = self.origin_label(&message.origin);
        match terminated {
            Ok(()) => SaciError::generic(format!(
                "{WHAT}: {label} failed to decode on all {} delivery attempt(s) \
                 'mode.max_decode_attempts' allows and has been terminated, so the consumer can \
                 advance; the row(s) it carried are lost: {}",
                self.max_decode_attempts,
                failure.error.message()
            )),
            Err(e) => SaciError::generic(format!(
                "{WHAT}: {label} failed to decode on all {} delivery attempt(s) \
                 'mode.max_decode_attempts' allows and could not be terminated ({e}), so the \
                 server will redeliver it: {}",
                self.max_decode_attempts,
                failure.error.message()
            )),
        }
    }

    /// Where a message came from, in words. Built only on the error path, so a
    /// clean window allocates nothing for it.
    fn origin_label(&self, origin: &Origin) -> String {
        match (origin, &self.state) {
            (Origin::Subject(subject), _) => format!("subject '{subject}'"),
            (
                Origin::Sequence(sequence),
                Some(Started::Jetstream {
                    window:
                        PullWindow {
                            stream, consumer, ..
                        },
                    ..
                }),
            ) => format!("{stream}[{consumer}]@{sequence}"),
            (Origin::Sequence(sequence), _) => format!("stream sequence {sequence}"),
        }
    }
}

/// Collect a window off the merged core subscription into `buffer`, resuming
/// whatever a cancelled call left in it.
///
/// A live source blocks on its first message with no deadline, then keeps taking
/// messages until `batch_size` or one `poll_timeout_ms` window; a message a
/// cancelled call already took satisfies that blocking phase. A `stop_at_end`
/// source bounds every receive by that window: core NATS has no end-of-stream
/// signal, so "the window elapsed with nothing on it" is the only EOF a
/// subscription can offer.
async fn collect_core(
    subscribers: &mut SelectAll<Subscriber>,
    cfg: &NatsSourceConfig,
    buffer: &mut Vec<Received>,
) -> Result<(), SaciError> {
    if !cfg.stop_at_end && buffer.is_empty() {
        match subscribers.next().await {
            Some(message) => buffer.push(received_core(message)),
            None => return Err(closed_error()),
        }
    }

    let deadline = Instant::now() + Duration::from_millis(cfg.poll_timeout_ms);
    while buffer.len() < cfg.batch_size {
        match timeout_at(deadline, subscribers.next()).await {
            Ok(Some(message)) => buffer.push(received_core(message)),
            // Every subscription ended, which the held client makes impossible
            // short of an explicit unsubscribe.
            Ok(None) if buffer.is_empty() => return Err(closed_error()),
            Ok(None) => break,
            Err(_elapsed) => break,
        }
    }
    Ok(())
}

fn received_core(message: async_nats::Message) -> Received {
    Received {
        origin: Origin::Subject(message.subject),
        payload: message.payload,
        ack: None,
        // Core NATS delivers a message once or not at all.
        delivered: 0,
    }
}

fn closed_error() -> SaciError {
    SaciError::generic(format!(
        "{WHAT}: every subscription closed while the connection was still held"
    ))
}

/// What the server says this consumer still owes.
struct Outstanding {
    /// Messages waiting to be delivered.
    pending: u64,
    /// Messages the server counts as delivered but unacknowledged. Its own
    /// pull request may have been answered into a window the caller never
    /// read, so these are not necessarily in anyone's hands.
    ack_pending: usize,
    /// The consumer's effective `ack_wait`, which is how long until an
    /// unacknowledged message is redelivered.
    ack_wait: Duration,
}

impl Outstanding {
    async fn read(consumer: &mut PullConsumer, window: &PullWindow) -> Result<Self, SaciError> {
        let info = consumer.info().await.map_err(|e| {
            SaciError::generic(format!(
                "{WHAT}: cannot read consumer info on stream '{}': {e}",
                window.stream
            ))
        })?;
        Ok(Self {
            pending: info.num_pending,
            ack_pending: info.num_ack_pending,
            ack_wait: info.config.ack_wait,
        })
    }

    fn is_drained(&self) -> bool {
        self.pending == 0 && self.ack_pending == 0
    }

    /// How long to keep asking before giving up on a stream that will not
    /// hand over what it says it holds. A message waiting to be delivered
    /// arrives on the next request, so `poll_timeout_ms` covers it; one the
    /// server counts as delivered comes back only when `ack_wait` expires, so
    /// that window is added rather than assumed.
    fn budget(&self, cfg: &NatsSourceConfig) -> Duration {
        let poll = Duration::from_millis(cfg.poll_timeout_ms);
        if self.ack_pending == 0 {
            poll
        } else {
            poll + self.ack_wait
        }
    }
}

/// Collect a window off the pull consumer into `buffer`, resuming whatever a
/// cancelled call left in it. Each message keeps its own `Acker`, so a window
/// this returns has been acknowledged by nobody yet.
///
/// `stop_at_end` uses `fetch`, which sets `no_wait` and returns only what is
/// already there. An empty window is not EOF on its own: a consumer that has
/// not started serving yet, and one whose delivered messages went into a
/// window nobody read, both answer exactly like a drained stream does. The
/// server's own counters are what tell them apart, so EOF is reported only
/// once it agrees this consumer has nothing [`Outstanding`]; until then the
/// window is asked again, for [`Outstanding::budget`] at most.
///
/// A live source uses `batch`, which waits up to `poll_timeout_ms` per request
/// and loops until a window carries at least one message.
async fn collect_jetstream(
    consumer: &mut PullConsumer,
    window: &PullWindow,
    cfg: &NatsSourceConfig,
    ack_mode: AckMode,
    buffer: &mut Vec<Received>,
    last_pending: &mut Option<usize>,
) -> Result<(), SaciError> {
    // What a cancelled call left behind goes over as it stands: a bounded
    // source that asked again would pay another whole `fetch_expires_ms`
    // window before handing over messages already in hand.
    if cfg.stop_at_end && !buffer.is_empty() {
        return Ok(());
    }
    let mut confirm_deadline: Option<Instant> = None;
    loop {
        {
            // What this window still has room for, so a window resumed after a
            // cancel asks for the rest of `batch_size` rather than for another
            // whole one. Never zero: a request for no messages is not a
            // request, and the `stop_at_end` path above already returned a
            // full carried window.
            let room = cfg.batch_size.saturating_sub(buffer.len()).max(1);
            let batch = if cfg.stop_at_end {
                let mut builder = consumer.fetch().max_messages(room).expires(window.expires);
                if window.max_bytes > 0 {
                    builder = builder.max_bytes(window.max_bytes);
                }
                if !window.heartbeat.is_zero() {
                    builder = builder.heartbeat(window.heartbeat);
                }
                builder.messages().await
            } else {
                let mut builder = consumer
                    .batch()
                    .max_messages(room)
                    .expires(Duration::from_millis(cfg.poll_timeout_ms));
                if window.max_bytes > 0 {
                    builder = builder.max_bytes(window.max_bytes);
                }
                if !window.heartbeat.is_zero() {
                    builder = builder.heartbeat(window.heartbeat);
                }
                builder.messages().await
            }
            .map_err(|e| {
                SaciError::generic(format!(
                    "{WHAT}: pull request on stream '{}' failed: {e}",
                    window.stream
                ))
            })?;

            let mut batch = std::pin::pin!(batch);
            while let Some(item) = batch.next().await {
                let message = item.map_err(|e| {
                    SaciError::generic(format!(
                        "{WHAT}: pull on stream '{}' failed: {e}",
                        window.stream
                    ))
                })?;
                // `info` parses the reply subject, with no round-trip.
                let (sequence, pending, delivered) = {
                    let info = message.info().map_err(|e| {
                        SaciError::generic(format!(
                            "{WHAT}: message on stream '{}' carries no JetStream metadata: {e}",
                            window.stream
                        ))
                    })?;
                    (info.stream_sequence, info.pending, info.delivered)
                };
                *last_pending = Some(usize::try_from(pending).unwrap_or(usize::MAX));

                let (payload, ack) = if ack_mode == AckMode::Never {
                    (message.message.payload, None)
                } else {
                    let (message, acker) = message.split();
                    (message.payload, Some(acker))
                };
                buffer.push(Received {
                    origin: Origin::Sequence(sequence),
                    payload,
                    ack,
                    delivered: u32::try_from(delivered).unwrap_or(u32::MAX),
                });
            }
        }

        if !buffer.is_empty() {
            return Ok(());
        }
        if !cfg.stop_at_end {
            // `batch` completing empty means the window expired with nothing
            // on the stream, so a live source asks again.
            continue;
        }

        let outstanding = Outstanding::read(consumer, window).await?;
        *last_pending = Some(usize::try_from(outstanding.pending).unwrap_or(usize::MAX));
        if outstanding.is_drained() {
            return Ok(());
        }
        let deadline =
            *confirm_deadline.get_or_insert_with(|| Instant::now() + outstanding.budget(cfg));
        if Instant::now() >= deadline {
            return Err(SaciError::generic(format!(
                "{WHAT}: stream '{}' reports {} message(s) waiting and {} delivered but \
                 unacknowledged for this consumer, and none of them arrived; refusing to \
                 report end of stream",
                window.stream, outstanding.pending, outstanding.ack_pending
            )));
        }
        tokio::time::sleep(DRAIN_CONFIRM_BACKOFF).await;
    }
}

#[async_trait]
impl Source for NatsSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Cancel-safe: the collected window lives on the source, and a message's
    /// `Acker` joins `pending_acks` only once that message is on its way to
    /// the caller, so a dropped future neither loses a window nor
    /// acknowledges one.
    ///
    /// # Errors
    ///
    /// A window the format cannot decode is reported rather than skipped, and
    /// its acks are dropped, so JetStream redelivers it. That redelivery is
    /// bounded by `mode.max_decode_attempts`: on the last permitted attempt
    /// the source terminates the offending message and the error names the
    /// stream sequence it retired.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.ensure_started().await?;
        self.flush_pending_acks().await?;

        self.collect().await?;
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let window = std::mem::take(&mut self.buffer);
        let failure = match self.decode_window(&window) {
            // Handed over, so this window's acks become the ones the next
            // call sends.
            Ok(batch) => {
                self.pending_acks
                    .extend(window.into_iter().filter_map(|message| message.ack));
                return Ok(Some(batch));
            }
            Err(failure) => failure,
        };
        // Every ack in the window is dropped, so the server redelivers what it
        // never saw acknowledged — except a message that has now used up its
        // decode attempts, which is terminated so the consumer can advance.
        Err(self.retire_exhausted(window, failure).await)
    }

    /// JetStream's own count of messages still waiting for this consumer, which
    /// is exactly the progress hint the trait asks for. Core NATS has no such
    /// number.
    fn estimated_rows(&self) -> Option<usize> {
        self.last_pending
    }

    /// A plain field write: sets `cfg.batch_size`, which `collect` reads
    /// fresh off `self.cfg` on every call, so it takes effect on the very
    /// next `next_batch`. No reconnect, no re-subscription. `saci-service`
    /// sends the hint ahead of the first poll, so `batch_size` in config is
    /// not a starting size; it is authoritative only where the host sends no
    /// hint, meaning flow control disabled for this source or a source
    /// feeding a windowed node.
    fn request_batch_rows(&mut self, rows: usize) {
        self.cfg.batch_size = rows.max(1);
    }
}
