//! [`TimeBoundedBuffer`]: the ring buffer every inspector signal lands in.
//!
//! Two independent bounds, both enforced on every push:
//!
//! - **Time**: entries older than `ttl` are dropped, so the buffer holds a
//!   moving window rather than a growing log.
//! - **Capacity**: `max_len` caps the entry count whatever the arrival rate, so
//!   a burst cannot exhaust memory between two TTL drains. Capacity evictions
//!   are counted and reported, because silently shortening the window would
//!   make the dashboard lie about its own retention.
//!
//! `std::sync::RwLock` around a `VecDeque`, no new dependency and no background
//! task: the drains ride along on the write the pusher already holds the lock
//! for. A poisoned lock is recovered with
//! [`PoisonError::into_inner`](std::sync::PoisonError::into_inner) rather than
//! unwrapped, because a panic in one consumer must not disable telemetry for
//! the rest of the process.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

/// A shared, time- and capacity-bounded queue of `T`.
///
/// Cloning shares the storage: every clone reads and writes the same buffer.
#[derive(Debug)]
pub struct TimeBoundedBuffer<T> {
    storage: Arc<RwLock<VecDeque<(Instant, T)>>>,
    ttl: Duration,
    max_len: usize,
    dropped: Arc<AtomicU64>,
}

impl<T> Clone for TimeBoundedBuffer<T> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            ttl: self.ttl,
            max_len: self.max_len,
            dropped: Arc::clone(&self.dropped),
        }
    }
}

impl<T: Clone> TimeBoundedBuffer<T> {
    /// Create an empty buffer retaining entries for `ttl`, at most `max_len` of
    /// them.
    ///
    /// `max_len` is clamped to at least 1: a zero-capacity buffer would drop
    /// every push and report the whole stream as evicted, which is a
    /// misconfiguration the caller cannot see in the data.
    pub fn new(ttl: Duration, max_len: usize) -> Self {
        Self {
            storage: Arc::new(RwLock::new(VecDeque::with_capacity(max_len.min(1024)))),
            ttl,
            max_len: max_len.max(1),
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Append `item`, then enforce both bounds.
    ///
    /// One write-lock acquisition covers the push and both drains. The TTL
    /// drain runs first so an idle buffer does not report capacity evictions
    /// for entries that had already expired.
    pub fn push(&self, item: T) {
        let now = Instant::now();
        let mut guard = self.storage.write().unwrap_or_else(PoisonError::into_inner);

        while let Some((at, _)) = guard.front() {
            if now.duration_since(*at) > self.ttl {
                guard.pop_front();
            } else {
                break;
            }
        }

        guard.push_back((now, item));

        let mut evicted = 0u64;
        while guard.len() > self.max_len {
            guard.pop_front();
            evicted += 1;
        }
        drop(guard);

        if evicted > 0 {
            self.dropped.fetch_add(evicted, Ordering::Relaxed);
        }
    }

    /// Every entry still inside the TTL window, oldest first.
    ///
    /// Read-only: expired entries are filtered out of the result but stay in
    /// storage until the next [`push`](Self::push) drains them, so a reader
    /// never blocks a writer behind a write lock.
    pub fn read_recent(&self) -> Vec<T> {
        let now = Instant::now();
        let guard = self.storage.read().unwrap_or_else(PoisonError::into_inner);
        guard
            .iter()
            .filter(|(at, _)| now.duration_since(*at) <= self.ttl)
            .map(|(_, item)| item.clone())
            .collect()
    }

    /// The newest `n` entries inside the TTL window, newest first.
    pub fn read_last(&self, n: usize) -> Vec<T> {
        let now = Instant::now();
        let guard = self.storage.read().unwrap_or_else(PoisonError::into_inner);
        guard
            .iter()
            .rev()
            .filter(|(at, _)| now.duration_since(*at) <= self.ttl)
            .take(n)
            .map(|(_, item)| item.clone())
            .collect()
    }

    /// Entries currently in storage, expired ones included.
    pub fn len(&self) -> usize {
        self.storage
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Whether storage holds no entries at all.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many entries have been evicted by the capacity bound.
    ///
    /// TTL expiry is not counted: it is the buffer working as configured, while
    /// a capacity eviction means the configured window was not honoured.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_zero_retains_only_the_newest_entry() {
        let buffer: TimeBoundedBuffer<u32> = TimeBoundedBuffer::new(Duration::ZERO, 128);
        buffer.push(1);
        std::thread::sleep(Duration::from_millis(2));
        buffer.push(2);

        // The push drained everything older than the (zero) window, so only
        // the entry pushed in this call survives.
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.dropped(), 0, "TTL expiry is not a capacity eviction");
    }

    #[test]
    fn capacity_bound_keeps_the_newest_and_counts_evictions() {
        let buffer: TimeBoundedBuffer<u32> = TimeBoundedBuffer::new(Duration::from_secs(60), 3);
        for value in 1..=4 {
            buffer.push(value);
        }

        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.read_recent(), vec![2, 3, 4]);
        assert_eq!(buffer.dropped(), 1);
    }

    #[test]
    fn read_last_returns_newest_first() {
        let buffer: TimeBoundedBuffer<u32> = TimeBoundedBuffer::new(Duration::from_secs(60), 128);
        for value in 1..=5 {
            buffer.push(value);
        }

        assert_eq!(buffer.read_last(2), vec![5, 4]);
        assert_eq!(buffer.read_last(99).len(), 5);
    }

    #[test]
    fn clones_share_one_storage() {
        let buffer: TimeBoundedBuffer<u32> = TimeBoundedBuffer::new(Duration::from_secs(60), 128);
        let clone = buffer.clone();
        clone.push(7);

        assert_eq!(buffer.read_recent(), vec![7]);
    }

    #[test]
    fn zero_capacity_is_clamped_rather_than_dropping_everything() {
        let buffer: TimeBoundedBuffer<u32> = TimeBoundedBuffer::new(Duration::from_secs(60), 0);
        buffer.push(1);

        assert_eq!(buffer.read_recent(), vec![1]);
        assert_eq!(buffer.dropped(), 0);
    }
}
