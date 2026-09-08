//! Framing for the two streaming read modes: newline-delimited bytes, and
//! `text/event-stream`.
//!
//! A live body arrives in chunks that have nothing to do with message
//! boundaries, so a framer holds the tail of the last chunk and hands out whole
//! frames only. Framing is transport, the way the TCP connector's length prefix
//! is; what a frame's bytes mean is the transformer's.
//!
//! `max_body_bytes` caps one frame. A chunk is whatever the transport handed
//! over, and hyper's HTTP/1 read buffer hands back a few hundred kilobytes of
//! small complete lines at a time, so a frame past the cap is [`OverCap`]
//! whether it arrived whole inside one chunk or grew across several, and a tail
//! that has already outgrown the cap is refused before its terminator arrives.
//!
//! [`EventFramer`] follows the WHATWG `text/event-stream` rules: any of `\r\n`,
//! `\n` or `\r` ends a line, a line starting with `:` is a comment, a line
//! `field: value` strips one leading space from the value, and a blank line
//! dispatches whatever `data` lines came before it, joined with `\n`. Only
//! `event`, `data`, `id` and `retry` are read; every other field name is
//! ignored, as the specification requires. `retry` takes effect where it is
//! processed and the last event ID buffer is applied at dispatch, before the
//! empty-data check, so both outlive the block that carried them and a block
//! with no `data:` still sets them.

use std::time::Duration;

/// One `text/event-stream` event, as dispatched by a blank line.
///
/// `id` and `retry` belong to the stream, so [`EventFramer::last_id`] and
/// [`EventFramer::take_retry`] carry them.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Event {
    /// The `event:` field, `None` when the stream named none.
    pub(crate) event: Option<String>,
    /// Every `data:` line of this event, joined with `\n`.
    pub(crate) data: Vec<u8>,
}

/// A frame longer than the configured cap, refused instead of framed.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OverCap;

/// Newline-delimited framing: one frame per `\n`-terminated line.
///
/// A trailing `\r` is stripped, so a server writing CRLF lines frames the same
/// as one writing LF. A partial last line is held until its newline arrives,
/// and dropped if the body ends without one: half a json object is not a frame.
pub(crate) struct LineFramer {
    buffer: Vec<u8>,
    /// The longest line to frame, `None` for no limit. A line past it is a
    /// protocol failure rather than unbounded memory.
    cap: Option<usize>,
}

impl LineFramer {
    /// A framer that frames lines of at most `cap` bytes.
    pub(crate) fn new(cap: Option<usize>) -> Self {
        Self {
            buffer: Vec::new(),
            cap,
        }
    }

    /// Add a chunk of body bytes.
    pub(crate) fn push(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
    }

    /// Whether what is left after draining has outgrown the cap.
    ///
    /// Evaluated once every whole line is drained, so the bytes measured are
    /// one unterminated line that can only grow.
    pub(crate) fn over_cap(&self) -> bool {
        self.cap.is_some_and(|cap| self.buffer.len() > cap)
    }

    /// The next whole line, or `None` when the buffer holds no newline yet.
    ///
    /// # Errors
    ///
    /// Returns [`OverCap`] when the next line is longer than the cap, however
    /// many chunks it arrived in.
    pub(crate) fn next(&mut self) -> Result<Option<Vec<u8>>, OverCap> {
        let Some(at) = self.buffer.iter().position(|b| *b == b'\n') else {
            return Ok(None);
        };
        if self.cap.is_some_and(|cap| at > cap) {
            return Err(OverCap);
        }
        let mut line: Vec<u8> = self.buffer.drain(..=at).collect();
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        Ok(Some(line))
    }
}

/// `text/event-stream` framing: one frame per dispatched event.
pub(crate) struct EventFramer {
    buffer: Vec<u8>,
    cap: Option<usize>,
    /// The event being accumulated: the name and data seen since the last
    /// blank line.
    pending: Event,
    /// Whether any `data:` line reached `pending`. An event with no data
    /// dispatches nothing, so this is not `data.is_empty()`: a `data:` line
    /// with an empty value still dispatches.
    has_data: bool,
    /// The last event ID buffer, which an `id:` field sets and which outlives
    /// the block that set it, as the specification's does.
    id: Option<String>,
    /// The id a dispatch applied, which is what a reconnect echoes. An empty
    /// string is a real value: it clears the id rather than echoing it.
    last_id: Option<String>,
    /// The reconnect delay a `retry:` field asked for, until the reader takes
    /// it.
    retry: Option<Duration>,
}

impl EventFramer {
    /// A framer that dispatches events carrying at most `cap` bytes of data.
    pub(crate) fn new(cap: Option<usize>) -> Self {
        Self {
            buffer: Vec::new(),
            cap,
            pending: Event::default(),
            has_data: false,
            id: None,
            last_id: None,
            retry: None,
        }
    }

    /// Add a chunk of body bytes.
    pub(crate) fn push(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
    }

    /// Whether what is left after draining has outgrown the cap.
    ///
    /// The undispatched event's data plus the residual tail: both are bytes of
    /// the event being accumulated, and neither can shrink.
    pub(crate) fn over_cap(&self) -> bool {
        self.cap
            .is_some_and(|cap| self.buffer.len() + self.pending.data.len() > cap)
    }

    /// The reconnect delay the stream last asked for, cleared by this read.
    pub(crate) fn take_retry(&mut self) -> Option<Duration> {
        self.retry.take()
    }

    /// The last event id a dispatch applied, `None` until one did.
    pub(crate) fn last_id(&self) -> Option<&str> {
        self.last_id.as_deref()
    }

    /// The next dispatched event, or `None` when no blank line has arrived.
    ///
    /// # Errors
    ///
    /// Returns [`OverCap`] when the accumulated `data` of one event is longer
    /// than the cap.
    pub(crate) fn next(&mut self) -> Result<Option<Event>, OverCap> {
        while let Some(line) = self.next_line() {
            if line.is_empty() {
                // The specification applies the last event ID buffer at
                // dispatch, before the empty-data check, so a block carrying
                // only an `id:` still names the id a reconnect echoes.
                self.last_id.clone_from(&self.id);
                if !self.has_data {
                    // A field block with no data at all dispatches nothing,
                    // but it does reset the buffer: an `event:` name alone
                    // does not attach to the next block.
                    self.pending = Event::default();
                    continue;
                }
                self.has_data = false;
                return Ok(Some(std::mem::take(&mut self.pending)));
            }
            if line.first() == Some(&b':') {
                continue;
            }
            let (field, value) = split_field(&line);
            match field.as_slice() {
                b"event" => {
                    self.pending.event = Some(String::from_utf8_lossy(value).into_owned());
                }
                b"data" => {
                    if self.has_data {
                        self.pending.data.push(b'\n');
                    }
                    self.pending.data.extend_from_slice(value);
                    self.has_data = true;
                    if self.cap.is_some_and(|cap| self.pending.data.len() > cap) {
                        return Err(OverCap);
                    }
                }
                // A NUL in an id is ignored per the specification.
                b"id" if !value.contains(&0) => {
                    self.id = Some(String::from_utf8_lossy(value).into_owned());
                }
                b"retry" => {
                    // A reconnection time takes effect where it is processed.
                    if let Ok(text) = std::str::from_utf8(value)
                        && !text.is_empty()
                        && text.bytes().all(|b| b.is_ascii_digit())
                        && let Ok(ms) = text.parse::<u64>()
                    {
                        self.retry = Some(Duration::from_millis(ms));
                    }
                }
                _ => {}
            }
        }
        Ok(None)
    }

    /// One line, ended by `\r\n`, `\n` or `\r`.
    ///
    /// A buffer ending in a lone `\r` yields nothing: the next chunk may make
    /// it a `\r\n`, and splitting there would invent an extra blank line.
    fn next_line(&mut self) -> Option<Vec<u8>> {
        let at = self
            .buffer
            .iter()
            .position(|b| *b == b'\n' || *b == b'\r')?;
        if self.buffer[at] == b'\r' && at + 1 == self.buffer.len() {
            return None;
        }
        let skip = if self.buffer[at] == b'\r' && self.buffer.get(at + 1) == Some(&b'\n') {
            2
        } else {
            1
        };
        let line: Vec<u8> = self.buffer.drain(..at).collect();
        self.buffer.drain(..skip);
        Some(line)
    }
}

/// Split one `field: value` line, stripping a single leading space from the
/// value. A line with no colon is a field name with an empty value.
fn split_field(line: &[u8]) -> (Vec<u8>, &[u8]) {
    match line.iter().position(|b| *b == b':') {
        Some(at) => {
            let value = &line[at + 1..];
            let value = value.strip_prefix(b" ").unwrap_or(value);
            (line[..at].to_vec(), value)
        }
        None => (line.to_vec(), &[]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(framer: &mut LineFramer) -> Vec<String> {
        let mut out = Vec::new();
        while let Some(line) = framer.next().expect("within the cap") {
            out.push(String::from_utf8(line).expect("utf-8"));
        }
        out
    }

    #[test]
    fn a_partial_line_waits_for_its_newline() {
        let mut framer = LineFramer::new(None);
        framer.push(b"{\"id\":1}\n{\"id\"");
        assert_eq!(lines(&mut framer), vec!["{\"id\":1}"]);
        framer.push(b":2}\n");
        assert_eq!(lines(&mut framer), vec!["{\"id\":2}"]);
    }

    #[test]
    fn crlf_lines_lose_the_carriage_return() {
        let mut framer = LineFramer::new(None);
        framer.push(b"a\r\nb\n");
        assert_eq!(lines(&mut framer), vec!["a", "b"]);
    }

    #[test]
    fn an_empty_line_is_still_a_frame() {
        let mut framer = LineFramer::new(None);
        framer.push(b"\n");
        assert_eq!(lines(&mut framer), vec![""]);
    }

    #[test]
    fn an_unterminated_tail_over_the_cap_is_refused() {
        let mut framer = LineFramer::new(Some(4));
        framer.push(b"abcd");
        assert!(!framer.over_cap(), "exactly the cap is within it");
        framer.push(b"e");
        assert!(framer.over_cap());
    }

    /// One chunk holds whatever the transport handed over, so a chunk of many
    /// small lines is not a frame over the cap.
    #[test]
    fn many_small_lines_in_one_chunk_are_all_framed() {
        let mut framer = LineFramer::new(Some(8));
        framer.push(b"one\ntwo\nthree\nfour\n");
        assert_eq!(lines(&mut framer), vec!["one", "two", "three", "four"]);
        assert!(!framer.over_cap(), "nothing is left to measure");
    }

    #[test]
    fn a_line_over_the_cap_inside_one_chunk_is_refused() {
        let mut framer = LineFramer::new(Some(4));
        framer.push(b"abcdefgh\nshort\n");
        assert_eq!(framer.next(), Err(OverCap));
    }

    #[test]
    fn a_line_of_exactly_the_cap_is_framed() {
        let mut framer = LineFramer::new(Some(4));
        framer.push(b"abcd\n");
        assert_eq!(lines(&mut framer), vec!["abcd"]);
    }

    fn events(framer: &mut EventFramer) -> Vec<Event> {
        let mut out = Vec::new();
        while let Some(event) = framer.next().expect("within the cap") {
            out.push(event);
        }
        out
    }

    #[test]
    fn a_comment_line_is_ignored() {
        let mut framer = EventFramer::new(None);
        framer.push(b": keepalive\ndata: one\n\n");
        let got = events(&mut framer);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].data, b"one");
        assert_eq!(got[0].event, None);
    }

    #[test]
    fn several_data_lines_join_with_a_newline() {
        let mut framer = EventFramer::new(None);
        framer.push(b"event: order\ndata: {\"a\":1,\ndata:  \"b\":2}\n\n");
        let got = events(&mut framer);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].event.as_deref(), Some("order"));
        assert_eq!(
            String::from_utf8_lossy(&got[0].data),
            "{\"a\":1,\n \"b\":2}",
            "only the first colon splits, and only one space is stripped"
        );
    }

    /// A field name is everything before the colon, so a line indented to
    /// look like a continuation is a field nothing reads.
    #[test]
    fn an_indented_data_line_is_a_different_field() {
        let mut framer = EventFramer::new(None);
        framer.push(b"data: one\n data: two\n\n");
        assert_eq!(events(&mut framer)[0].data, b"one");
    }

    #[test]
    fn id_and_retry_are_read_off_the_stream() {
        let mut framer = EventFramer::new(None);
        framer.push(b"id: 7\nretry: 50\ndata: x\n\n");
        assert_eq!(events(&mut framer).len(), 1);
        assert_eq!(framer.last_id(), Some("7"));
        assert_eq!(framer.take_retry(), Some(Duration::from_millis(50)));
        assert_eq!(framer.take_retry(), None, "the reader takes it once");
    }

    #[test]
    fn a_non_numeric_retry_is_ignored() {
        let mut framer = EventFramer::new(None);
        framer.push(b"retry: soon\ndata: x\n\n");
        assert_eq!(events(&mut framer).len(), 1);
        assert_eq!(framer.take_retry(), None);
    }

    /// A reconnection time takes effect where it is processed, so a block with
    /// no `data:` still sets it.
    #[test]
    fn a_retry_block_with_no_data_is_still_applied() {
        let mut framer = EventFramer::new(None);
        framer.push(b"retry: 5000\n\n");
        assert!(events(&mut framer).is_empty(), "no data, no event");
        assert_eq!(framer.take_retry(), Some(Duration::from_millis(5000)));
    }

    /// The last event ID buffer is applied at dispatch, before the empty-data
    /// check, so an id-only block names the id a reconnect echoes.
    #[test]
    fn an_id_block_with_no_data_is_still_remembered() {
        let mut framer = EventFramer::new(None);
        framer.push(b"id: 42\n\n");
        assert!(events(&mut framer).is_empty(), "no data, no event");
        assert_eq!(framer.last_id(), Some("42"));
    }

    /// An empty `id:` value is a real answer: it clears the id, and a
    /// reconnect sends no `last-event-id` at all.
    #[test]
    fn an_empty_id_clears_the_last_id() {
        let mut framer = EventFramer::new(None);
        framer.push(b"id: 42\ndata: x\n\n");
        assert_eq!(events(&mut framer).len(), 1);
        assert_eq!(framer.last_id(), Some("42"));

        framer.push(b"id:\ndata: y\n\n");
        assert_eq!(events(&mut framer).len(), 1);
        assert_eq!(framer.last_id(), Some(""));
    }

    /// The buffer outlives the block that set it, so an event naming no id
    /// keeps the one before it.
    #[test]
    fn the_last_id_persists_across_an_event_that_names_none() {
        let mut framer = EventFramer::new(None);
        framer.push(b"id: 9\ndata: x\n\ndata: y\n\n");
        assert_eq!(events(&mut framer).len(), 2);
        assert_eq!(framer.last_id(), Some("9"));
    }

    #[test]
    fn cr_only_line_endings_frame_the_same() {
        let mut framer = EventFramer::new(None);
        framer.push(b"data: x\r\rdata: y\r\r");
        let got = events(&mut framer);
        assert_eq!(got.len(), 1, "the last CR may still become a CRLF");
        assert_eq!(got[0].data, b"x");
        framer.push(b"\n");
        assert_eq!(events(&mut framer)[0].data, b"y");
    }

    /// A lone trailing `\r` may still become `\r\n`, so splitting on it would
    /// invent a blank line and dispatch early.
    #[test]
    fn a_trailing_carriage_return_waits_for_the_next_chunk() {
        let mut framer = EventFramer::new(None);
        framer.push(b"data: x\r");
        assert!(events(&mut framer).is_empty());
        framer.push(b"\n\n");
        assert_eq!(events(&mut framer).len(), 1);
    }

    /// An `event:` with no `data:` dispatches nothing, and does not leak its
    /// name into the block that follows.
    #[test]
    fn an_event_with_no_data_dispatches_nothing() {
        let mut framer = EventFramer::new(None);
        framer.push(b"event: ping\n\ndata: y\n\n");
        let got = events(&mut framer);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].event, None);
        assert_eq!(got[0].data, b"y");
    }

    /// A `data:` with an empty value is still an event, which is why the
    /// dispatch rule cannot be "the data buffer is non-empty".
    #[test]
    fn an_empty_data_value_still_dispatches() {
        let mut framer = EventFramer::new(None);
        framer.push(b"data:\n\n");
        let got = events(&mut framer);
        assert_eq!(got.len(), 1);
        assert!(got[0].data.is_empty());
    }

    #[test]
    fn an_unknown_field_is_ignored() {
        let mut framer = EventFramer::new(None);
        framer.push(b"comment: hello\ndata: z\n\n");
        assert_eq!(events(&mut framer)[0].data, b"z");
    }

    /// The cap is one event's data, so a chunk of several small events is
    /// framed whatever its own size is.
    #[test]
    fn many_small_events_in_one_chunk_are_all_dispatched() {
        let mut framer = EventFramer::new(Some(8));
        framer.push(b"data: one\n\ndata: two\n\ndata: three\n\n");
        let got = events(&mut framer);
        assert_eq!(got.len(), 3);
        assert!(!framer.over_cap(), "nothing is left to measure");
    }

    #[test]
    fn an_event_over_the_cap_inside_one_chunk_is_refused() {
        let mut framer = EventFramer::new(Some(4));
        framer.push(b"data: abcdefgh\n\n");
        assert_eq!(framer.next(), Err(OverCap));
    }

    /// Data accumulated across `data:` lines is one frame, so the cap applies
    /// to their total.
    #[test]
    fn data_lines_that_together_pass_the_cap_are_refused() {
        let mut framer = EventFramer::new(Some(6));
        framer.push(b"data: abc\ndata: def\n\n");
        assert_eq!(framer.next(), Err(OverCap));
    }

    /// An undispatched tail is measured raw, field name included: it is held
    /// memory, and the event it belongs to can only grow.
    #[test]
    fn an_undispatched_event_over_the_cap_is_refused() {
        let mut framer = EventFramer::new(Some(8));
        framer.push(b"data: ab");
        assert_eq!(framer.next(), Ok(None));
        assert!(!framer.over_cap(), "the tail is still within the cap");
        framer.push(b"cdef");
        assert_eq!(framer.next(), Ok(None));
        assert!(framer.over_cap());
    }
}
