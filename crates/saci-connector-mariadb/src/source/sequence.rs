//! Holes in an `AUTO_INCREMENT` cursor.
//!
//! The server hands out an `AUTO_INCREMENT` value when a row is written, not
//! when its transaction commits. If transaction A takes 5 and stays open while
//! B takes 6 and commits, a reader that has read 6 would move its cursor past 5
//! and never see A's row. A cursor source ([`CursorReader`]) reads `cursor > ?`,
//! so it cannot go back, and `delete_acked` would prune the row too.
//!
//! The reader therefore treats a gap in the numbers it reads as a transaction
//! that may still commit. Rows are handed over only up to the first gap; the
//! gap is waited for and, if a row fills it, delivered in order. A gap that
//! nothing can fill any more is passed and the rows after it go out.
//!
//! # When a hole is passed
//!
//! A hole can only be filled by a transaction that took its number before the
//! row after the hole took its own, so the filler is already running when the
//! reader first sees the hole. Two sources of evidence say when that is over:
//!
//! - `INNODB_TRX`, when the user may read it (the `PROCESS` privilege). After
//!   the read that first shows a hole, the reader records which transactions
//!   that have modified rows, or are waiting on a lock, are running. The list
//!   must be newer than the read: the server rebuilds the table only after 0.1
//!   seconds without a read, so an older one could predate the writer that owns
//!   the hole. The reader asks inside a transaction of its own, with a number in
//!   the statement, and accepts the list only when it shows that statement
//!   running ([`fresh_writers`]). A hole it cannot get a current list for is
//!   left to the timeout.
//!   Before each later read it drops the recorded transactions that have ended. A hole
//!   whose set is empty cannot be filled: every transaction that could have
//!   filled it has finished, and the read that follows the check still shows it
//!   empty. The hole is passed at once ([`Outcome::Settled`]), whatever
//!   `gap_timeout_ms` says. While a recorded transaction is still running the
//!   hole is held, past `gap_timeout_ms`, until `gap_max_wait_ms` ends the wait
//!   ([`Outcome::Capped`]). The cap exists because the transactions cannot be
//!   narrowed to the ones that touch this table: an unrelated transaction left
//!   open would otherwise stop the source for good.
//! - Time alone, when `INNODB_TRX` is not readable. The hole is passed once it
//!   is still empty `gap_timeout_ms` after the reader first saw it
//!   ([`Outcome::TimedOut`]). If no transaction stays open between taking a
//!   number and committing for longer than that, the filler has committed by
//!   then. The clock is the reader's own, so it owes nothing to row timestamps
//!   (a statement's `CURRENT_TIMESTAMP` is its start, not the number's
//!   allocation) or to the server's clock.
//!
//! A hole that is passed on time or on the cap is a risk the operator can see:
//! a row that commits into it later is not delivered.
//!
//! # What counts as a hole
//!
//! Consecutive values the server generates differ by `auto_increment_increment`,
//! the step. A jump of more than one step between neighbouring rows is a hole;
//! equal values (a non-unique cursor with a tiebreak) are not. Before the first
//! delivery the previous value is 0, which the first value of every sequence is
//! within one step of. The check reads the global step, so it holds for a
//! server that runs a step above 1; a writer that sets a session step of its own
//! shows holes that are not.
//!
//! # State
//!
//! Each open hole is keyed by the first row after it, so every hole in a read
//! has its own clock and its own set of transactions, and a window with several
//! waits for the slowest, not their sum. State is in memory: a restarted reader
//! has none, the offset (never past an open hole) is all that survives, and each
//! hole is looked at again from the start.
//!
//! [`CursorReader`]: super::cursor::CursorReader

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use saci_core::error::SaciError;

use crate::connection::{TableRef, mysql_detail};

/// A run of numbers no row carries, between two rows that do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Hole {
    /// The last row before the hole: the delivered cursor, or a row of the batch.
    pub(crate) last: i128,
    /// The first row after it.
    pub(crate) next: i128,
}

/// Why a hole was passed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// No transaction that could fill it is still running.
    Settled,
    /// Still empty after `gap_timeout_ms`, with no transaction list to consult.
    TimedOut,
    /// A transaction that could fill it was still running at `gap_max_wait_ms`.
    Capped,
}

impl Outcome {
    /// The metric label.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Outcome::Settled => "settled",
            Outcome::TimedOut => "timeout",
            Outcome::Capped => "cap",
        }
    }
}

/// A hole the delivered rows pass over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Passed {
    pub(crate) hole: Hole,
    pub(crate) outcome: Outcome,
}

/// What the reader may hand over of the rows it read.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Release {
    /// How many leading rows are safe to deliver.
    pub(crate) deliver: usize,
    /// Holes the delivered rows pass over.
    pub(crate) passed: Vec<Passed>,
    /// Holes in this read that have no set of transactions yet: new ones, and
    /// ones the reader could not record transactions for. They are tried again.
    pub(crate) unattached: Vec<i128>,
}

/// One open hole.
#[derive(Debug, Clone)]
struct Open {
    /// When the reader first saw it.
    first: Instant,
    /// The ids of the transactions that could still fill it, once recorded.
    /// `None` until the reader has asked, and when it cannot.
    blockers: Option<HashSet<u64>>,
}

/// The open holes of one reader.
#[derive(Debug)]
pub(crate) struct Holes {
    timeout: Duration,
    max_wait: Option<Duration>,
    /// By the first row after the hole.
    seen: HashMap<i128, Open>,
}

impl Holes {
    /// A tracker that passes a hole with no evidence after `timeout`, and one a
    /// running transaction could fill after `max_wait` (`None` waits for the
    /// transaction without limit).
    pub(crate) fn new(timeout: Duration, max_wait: Option<Duration>) -> Self {
        Self {
            timeout,
            max_wait,
            seen: HashMap::new(),
        }
    }

    /// Whether any hole is open.
    pub(crate) fn tracking(&self) -> bool {
        !self.seen.is_empty()
    }

    /// Drop from every hole's set the transactions that are no longer in
    /// `active`. Call it before the read the next [`release`](Self::release)
    /// judges, so that a hole left empty is empty in that read too.
    pub(crate) fn trim(&mut self, active: &HashSet<u64>) {
        for open in self.seen.values_mut() {
            if let Some(blockers) = &mut open.blockers {
                blockers.retain(|id| active.contains(id));
            }
        }
    }

    /// Record `active`, the transactions running after the read that first
    /// showed the holes `keys`, as the ones that could fill them.
    pub(crate) fn attach(&mut self, keys: &[i128], active: &HashSet<u64>) {
        for key in keys {
            if let Some(open) = self.seen.get_mut(key)
                && open.blockers.is_none()
            {
                open.blockers = Some(active.clone());
            }
        }
    }

    /// Whether any of the holes `keys` has an empty set: nothing can fill it.
    pub(crate) fn settled(&self, keys: &[i128]) -> bool {
        keys.iter().any(|key| {
            self.seen
                .get(key)
                .and_then(|open| open.blockers.as_ref())
                .is_some_and(HashSet::is_empty)
        })
    }

    /// Go back to time alone for every hole, after the transaction list stopped
    /// being readable.
    pub(crate) fn forget_evidence(&mut self) {
        for open in self.seen.values_mut() {
            open.blockers = None;
        }
    }

    /// Whether `open` may be passed at `now`, and why.
    fn verdict(&self, open: &Open, now: Instant) -> Option<Outcome> {
        let age = now.saturating_duration_since(open.first);
        match &open.blockers {
            Some(blockers) if blockers.is_empty() => Some(Outcome::Settled),
            Some(_) => self
                .max_wait
                .filter(|max_wait| age >= *max_wait)
                .map(|_| Outcome::Capped),
            None => (age >= self.timeout).then_some(Outcome::TimedOut),
        }
    }

    /// Which of `values`, the cursors of the rows just read in order, may be
    /// delivered, given that `last` was delivered before them.
    ///
    /// A hole at or before a row keeps that row and every later one back until
    /// the hole may be passed. Every hole in `values` has its clock and its
    /// transactions kept, held back or not; a hole no longer in `values` (a row
    /// filled it, or the window moved) is forgotten.
    pub(crate) fn release(
        &mut self,
        now: Instant,
        step: i128,
        last: Option<i128>,
        values: &[i128],
    ) -> Release {
        let mut known = std::mem::take(&mut self.seen);
        let mut deliver = values.len();
        let mut passed = Vec::new();
        let mut unattached = Vec::new();
        let mut previous = last.unwrap_or(0);
        for (index, &value) in values.iter().enumerate() {
            if value - previous > step {
                let open = known.remove(&value).unwrap_or(Open {
                    first: now,
                    blockers: None,
                });
                if open.blockers.is_none() {
                    unattached.push(value);
                }
                if deliver == values.len() {
                    match self.verdict(&open, now) {
                        Some(outcome) => passed.push(Passed {
                            hole: Hole {
                                last: previous,
                                next: value,
                            },
                            outcome,
                        }),
                        None => deliver = index,
                    }
                }
                self.seen.insert(value, open);
            }
            previous = value;
        }
        Release {
            deliver,
            passed,
            unattached,
        }
    }
}

/// The step of `column` of `table` when it is an `AUTO_INCREMENT` column, and
/// `None` when it is not a sequence the server numbers.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when the catalog or the server variable
/// cannot be read.
pub(crate) async fn step_of(
    conn: &mut Conn,
    what: &str,
    table: &TableRef,
    column: &str,
) -> Result<Option<i128>, SaciError> {
    let fail = |e: mysql_async::Error| {
        SaciError::generic(format!(
            "{what}: cannot read the AUTO_INCREMENT settings of '{table}': {}",
            mysql_detail(&e)
        ))
    };
    let extra: Option<Option<String>> = conn
        .exec_first(
            "SELECT EXTRA FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
            (table.schema.as_str(), table.name.as_str(), column),
        )
        .await
        .map_err(fail)?;
    if !extra
        .flatten()
        .is_some_and(|extra| extra.to_ascii_lowercase().contains("auto_increment"))
    {
        return Ok(None);
    }
    let step: Option<u64> = conn
        .query_first("SELECT @@GLOBAL.auto_increment_increment")
        .await
        .map_err(fail)?;
    Ok(Some(i128::from(step.unwrap_or(1).max(1))))
}

/// Whether the connecting user can read `INNODB_TRX`. Both servers refuse a user
/// without the `PROCESS` privilege with error 1227.
///
/// # Errors
///
/// Returns the server's error when the table cannot be read.
pub(crate) async fn trx_readable(conn: &mut Conn) -> Result<(), mysql_async::Error> {
    conn.query_drop("SELECT COUNT(*) FROM information_schema.INNODB_TRX")
        .await
}

/// The ids of the running transactions that have modified rows or are waiting on
/// a lock, other than this session's own. An insert takes its number before it
/// writes the row, and can wait on a lock (a duplicate key check, a gap lock)
/// in between: it shows as `LOCK WAIT` with no modified rows, and the hole
/// is still its. A transaction that has done neither cannot be what a hole
/// waits for.
///
/// # Errors
///
/// Returns the server's error when the table cannot be read.
pub(crate) async fn active_writers(conn: &mut Conn) -> Result<HashSet<u64>, mysql_async::Error> {
    let ids: Vec<u64> = conn
        .query(
            "SELECT trx_id FROM information_schema.INNODB_TRX \
             WHERE (trx_rows_modified > 0 OR trx_state = 'LOCK WAIT') \
             AND NOT (trx_mysql_thread_id <=> CONNECTION_ID())",
        )
        .await?;
    Ok(ids.into_iter().collect())
}

/// The same list as [`active_writers`], proved current. The server rebuilds the
/// table only when it has gone 0.1 seconds without a read, so a reader that
/// asks right after another reader (a monitor polling faster than that keeps it
/// so) is served a list from before the writer it is looking for started.
///
/// The proof is the reader's own statement. This opens a transaction, and reads
/// the table with a statement that carries a number no earlier statement had.
/// A transaction lists the statement it is running, so a list that shows that
/// number was built while the statement ran, after everything the reader had
/// seen before. The reader's earlier transactions, still in an older list, do
/// not carry it. `None` is a list that does not show it.
///
/// # Errors
///
/// Returns the server's error when a statement fails.
pub(crate) async fn fresh_writers(
    conn: &mut Conn,
    table: &TableRef,
) -> Result<Option<HashSet<u64>>, mysql_async::Error> {
    use rand::RngExt;

    conn.query_drop("START TRANSACTION").await?;
    let nonce: u64 = rand::rng().random();
    let listed = canary_list(conn, table, nonce).await;
    // Ended whatever the list said; a dead connection ends it by itself.
    let ended = conn.query_drop("ROLLBACK").await;
    let listed = listed?;
    ended?;
    Ok(listed)
}

/// The body of [`fresh_writers`], inside the reader's own transaction.
async fn canary_list(
    conn: &mut Conn,
    table: &TableRef,
    nonce: u64,
) -> Result<Option<HashSet<u64>>, mysql_async::Error> {
    // A consistent read of any InnoDB table registers the transaction.
    conn.query_drop(format!("SELECT 1 FROM {} LIMIT 1", table.quoted()))
        .await?;
    let rows: Vec<(u64, u64, u64)> = conn.query(canary_sql(nonce)).await?;
    Ok(canary_writers(rows))
}

/// The statement behind [`canary_list`]: each listed transaction that has
/// modified rows or waits on a lock, and any of this session's own, as its id,
/// whether it is this session's, and whether it is running this statement.
fn canary_sql(nonce: u64) -> String {
    format!(
        "SELECT trx_id, (trx_mysql_thread_id <=> CONNECTION_ID()), \
         COALESCE(trx_mysql_thread_id <=> CONNECTION_ID() \
         AND INSTR(trx_query, 'saci-canary-{nonce}') > 0, 0) \
         FROM information_schema.INNODB_TRX \
         WHERE trx_rows_modified > 0 OR trx_state = 'LOCK WAIT' \
         OR (trx_mysql_thread_id <=> CONNECTION_ID()) /* saci-canary-{nonce} */"
    )
}

/// The writers of a list from [`canary_sql`] when it shows the statement that
/// asked for it, and `None` when it does not. This session's own transactions
/// are never writers.
fn canary_writers(rows: Vec<(u64, u64, u64)>) -> Option<HashSet<u64>> {
    if !rows.iter().any(|(_, _, running)| *running != 0) {
        return None;
    }
    Some(
        rows.into_iter()
            .filter(|(_, own, _)| *own == 0)
            .map(|(id, _, _)| id)
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(10);
    const MAX_WAIT: Duration = Duration::from_secs(60);

    fn holes() -> (Holes, Instant) {
        (Holes::new(TIMEOUT, Some(MAX_WAIT)), Instant::now())
    }

    fn after(start: Instant, secs: u64) -> Instant {
        start + Duration::from_secs(secs)
    }

    fn ids(ids: &[u64]) -> HashSet<u64> {
        ids.iter().copied().collect()
    }

    fn timed_out(last: i128, next: i128) -> Passed {
        Passed {
            hole: Hole { last, next },
            outcome: Outcome::TimedOut,
        }
    }

    #[test]
    fn contiguous_rows_are_all_delivered_and_leave_no_clock() {
        let (mut holes, t0) = holes();
        let release = holes.release(t0, 1, Some(4), &[5, 6, 7]);
        assert_eq!(
            release,
            Release {
                deliver: 3,
                passed: Vec::new(),
                unattached: Vec::new()
            }
        );
        assert!(!holes.tracking());
    }

    #[test]
    fn a_hole_at_the_front_holds_every_row_back() {
        let (mut holes, t0) = holes();
        let release = holes.release(t0, 1, Some(4), &[6, 7]);
        assert_eq!(release.deliver, 0);
        assert!(release.passed.is_empty());
        assert_eq!(release.unattached, vec![6]);
    }

    #[test]
    fn rows_before_a_hole_in_the_window_are_delivered() {
        let (mut holes, t0) = holes();
        let release = holes.release(t0, 1, Some(4), &[5, 6, 9, 10]);
        assert_eq!(release.deliver, 2);
        assert_eq!(release.unattached, vec![9]);
        assert_eq!(holes.seen.len(), 1);
        assert!(holes.seen.contains_key(&9));
    }

    #[test]
    fn nothing_before_the_first_delivery_may_start_past_the_first_number() {
        let (mut holes, t0) = holes();
        assert_eq!(holes.release(t0, 1, None, &[1, 2]).deliver, 2);
        assert_eq!(holes.release(t0, 1, None, &[2, 3]).deliver, 0);
        assert_eq!(holes.release(t0, 3, None, &[2, 5]).deliver, 2);
        assert_eq!(holes.release(t0, 3, None, &[4]).deliver, 0);
    }

    #[test]
    fn with_no_evidence_a_hole_is_passed_once_it_is_older_than_the_timeout() {
        let (mut holes, t0) = holes();
        assert_eq!(holes.release(t0, 1, Some(4), &[6, 7]).deliver, 0);
        let again = holes.release(after(t0, 9), 1, Some(4), &[6, 7]);
        assert_eq!(again.deliver, 0);
        assert_eq!(
            again.unattached,
            vec![6],
            "with no evidence the hole stays unattached"
        );
        let release = holes.release(after(t0, 10), 1, Some(4), &[6, 7]);
        assert_eq!(release.deliver, 2);
        assert_eq!(release.passed, vec![timed_out(4, 6)]);
    }

    #[test]
    fn a_hole_the_late_row_fills_is_delivered_and_forgotten() {
        let (mut holes, t0) = holes();
        assert_eq!(holes.release(t0, 1, Some(4), &[6]).deliver, 0);
        let release = holes.release(after(t0, 1), 1, Some(4), &[5, 6]);
        assert_eq!(release.deliver, 2);
        assert!(release.passed.is_empty());
        assert!(!holes.tracking());
    }

    #[test]
    fn holes_in_one_window_run_their_clocks_together() {
        let (mut holes, t0) = holes();
        assert_eq!(holes.release(t0, 1, Some(0), &[2, 3, 5, 6]).deliver, 0);
        // Both holes were first seen at t0, so both are old at t0 + 10.
        let release = holes.release(after(t0, 10), 1, Some(0), &[2, 3, 5, 6]);
        assert_eq!(release.deliver, 4);
        assert_eq!(release.passed, vec![timed_out(0, 2), timed_out(3, 5)]);
    }

    #[test]
    fn a_younger_hole_behind_an_expired_one_still_holds_its_rows() {
        let (mut holes, t0) = holes();
        assert_eq!(holes.release(t0, 1, Some(0), &[2, 3]).deliver, 0);
        // At t0 + 10 the first hole is old; a second one appears in the window now.
        let release = holes.release(after(t0, 10), 1, Some(0), &[2, 3, 5, 6]);
        assert_eq!(
            release.deliver, 2,
            "the second hole's clock starts at t0 + 10"
        );
        assert_eq!(release.passed, vec![timed_out(0, 2)]);
        // The hole at the front is now the one at 5.
        assert_eq!(holes.release(after(t0, 11), 1, Some(3), &[5, 6]).deliver, 0);
        assert_eq!(holes.release(after(t0, 20), 1, Some(3), &[5, 6]).deliver, 2);
    }

    #[test]
    fn a_step_above_one_is_not_a_hole_but_a_missing_step_is() {
        let (mut holes, t0) = holes();
        assert_eq!(holes.release(t0, 3, Some(2), &[5, 8, 11]).deliver, 3);
        // 11 is missing between 8 and 14.
        assert_eq!(holes.release(t0, 3, Some(2), &[5, 8, 14]).deliver, 2);
    }

    #[test]
    fn equal_cursor_values_under_a_tiebreak_are_not_holes() {
        let (mut holes, t0) = holes();
        assert_eq!(holes.release(t0, 1, Some(5), &[5, 5, 6, 6]).deliver, 4);
    }

    #[test]
    fn an_empty_window_forgets_every_clock() {
        let (mut holes, t0) = holes();
        assert_eq!(holes.release(t0, 1, Some(4), &[6]).deliver, 0);
        assert_eq!(holes.release(t0, 1, Some(4), &[]).deliver, 0);
        assert!(!holes.tracking());
    }

    #[test]
    fn a_cursor_near_the_top_of_the_unsigned_range_does_not_overflow() {
        let (mut holes, t0) = holes();
        let top = i128::from(u64::MAX);
        assert_eq!(
            holes.release(t0, 1, Some(top - 2), &[top - 1, top]).deliver,
            2
        );
        assert_eq!(holes.release(t0, 1, Some(top - 3), &[top - 1]).deliver, 0);
    }

    #[test]
    fn a_hole_no_running_transaction_can_fill_is_passed_at_once() {
        let (mut holes, t0) = holes();
        let first = holes.release(t0, 1, Some(4), &[6, 7]);
        assert_eq!(first.deliver, 0);
        holes.attach(&first.unattached, &ids(&[]));
        assert!(holes.settled(&first.unattached));

        let release = holes.release(t0, 1, Some(4), &[6, 7]);
        assert_eq!(release.deliver, 2, "no waiting for gap_timeout_ms");
        assert_eq!(
            release.passed,
            vec![Passed {
                hole: Hole { last: 4, next: 6 },
                outcome: Outcome::Settled
            }]
        );
    }

    #[test]
    fn a_hole_is_held_past_the_timeout_while_a_recorded_transaction_runs() {
        let (mut holes, t0) = holes();
        let first = holes.release(t0, 1, Some(4), &[6]);
        holes.attach(&first.unattached, &ids(&[41, 42]));
        assert!(!holes.settled(&first.unattached));

        // Past gap_timeout_ms, with 42 still running.
        holes.trim(&ids(&[42, 99]));
        assert_eq!(holes.release(after(t0, 30), 1, Some(4), &[6]).deliver, 0);

        // 42 ends; the hole is empty by the next read.
        holes.trim(&ids(&[99]));
        let release = holes.release(after(t0, 31), 1, Some(4), &[6]);
        assert_eq!(release.deliver, 1);
        assert_eq!(release.passed[0].outcome, Outcome::Settled);
    }

    #[test]
    fn a_transaction_that_started_after_the_sighting_never_blocks() {
        let (mut holes, t0) = holes();
        let first = holes.release(t0, 1, Some(4), &[6]);
        holes.attach(&first.unattached, &ids(&[41]));
        // 77 is new in the next probe; it is not in the hole's set and never will be.
        holes.trim(&ids(&[41, 77]));
        holes.trim(&ids(&[77]));
        assert_eq!(holes.release(t0, 1, Some(4), &[6]).deliver, 1);
    }

    #[test]
    fn the_cap_ends_the_wait_for_a_transaction_that_never_finishes() {
        let (mut holes, t0) = holes();
        let first = holes.release(t0, 1, Some(4), &[6]);
        holes.attach(&first.unattached, &ids(&[41]));
        assert_eq!(holes.release(after(t0, 59), 1, Some(4), &[6]).deliver, 0);
        let release = holes.release(after(t0, 60), 1, Some(4), &[6]);
        assert_eq!(release.deliver, 1);
        assert_eq!(release.passed[0].outcome, Outcome::Capped);
    }

    #[test]
    fn with_no_cap_a_running_transaction_is_waited_for_without_limit() {
        let mut holes = Holes::new(TIMEOUT, None);
        let t0 = Instant::now();
        let first = holes.release(t0, 1, Some(4), &[6]);
        holes.attach(&first.unattached, &ids(&[41]));
        assert_eq!(
            holes.release(after(t0, 100_000), 1, Some(4), &[6]).deliver,
            0
        );
    }

    #[test]
    fn a_hole_is_not_passed_before_its_transactions_are_recorded() {
        let (mut holes, t0) = holes();
        assert_eq!(holes.release(t0, 1, Some(4), &[6]).deliver, 0);
        // Recording happens right after the read; until then only time speaks,
        // and a fresh hole is as old as the read.
        assert_eq!(holes.release(t0, 1, Some(4), &[6]).deliver, 0);
    }

    #[test]
    fn losing_the_transaction_list_goes_back_to_the_timeout() {
        let (mut holes, t0) = holes();
        let first = holes.release(t0, 1, Some(4), &[6]);
        holes.attach(&first.unattached, &ids(&[41]));
        holes.forget_evidence();
        assert_eq!(holes.release(after(t0, 9), 1, Some(4), &[6]).deliver, 0);
        let release = holes.release(after(t0, 10), 1, Some(4), &[6]);
        assert_eq!(release.passed, vec![timed_out(4, 6)]);
    }

    #[test]
    fn each_hole_has_its_own_set_of_transactions() {
        let (mut holes, t0) = holes();
        let first = holes.release(t0, 1, Some(0), &[2, 3, 5, 6]);
        assert_eq!(first.unattached, vec![2, 5]);
        holes.attach(&[2], &ids(&[]));
        holes.attach(&[5], &ids(&[7]));
        let release = holes.release(t0, 1, Some(0), &[2, 3, 5, 6]);
        assert_eq!(
            release.deliver, 2,
            "the second hole still has a transaction"
        );
        assert_eq!(release.passed.len(), 1);
    }
}

#[cfg(test)]
mod canary_tests {
    use super::*;

    #[test]
    fn the_probe_statement_carries_its_own_number_in_its_text_and_its_filter() {
        let sql = canary_sql(77);
        assert!(sql.contains("INSTR(trx_query, 'saci-canary-77')"), "{sql}");
        assert!(
            sql.contains("COALESCE(trx_mysql_thread_id <=> CONNECTION_ID() AND INSTR"),
            "an idle transaction has no query, and the column must not be NULL: {sql}"
        );
        assert!(sql.ends_with("/* saci-canary-77 */"), "{sql}");
        assert!(sql.contains("trx_state = 'LOCK WAIT'"), "{sql}");
    }

    #[test]
    fn a_list_that_does_not_show_the_running_statement_is_stale() {
        // The reader's own earlier transaction is listed, running something else.
        assert_eq!(canary_writers(vec![(0, 1, 0), (41, 0, 0)]), None);
        assert_eq!(canary_writers(Vec::new()), None);
    }

    #[test]
    fn a_list_that_shows_the_statement_yields_the_other_writers_only() {
        let rows = vec![(0, 1, 1), (41, 0, 0), (42, 0, 0), (9, 1, 0)];
        assert_eq!(canary_writers(rows), Some(HashSet::from([41, 42])));
        assert_eq!(canary_writers(vec![(0, 1, 1)]), Some(HashSet::new()));
    }
}
