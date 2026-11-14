//! A MySQL GTID set: parsed from the server's text form, grown one transaction
//! at a time, rendered canonically for the offset table, and converted to the
//! `Sid` blocks that `COM_BINLOG_DUMP_GTID` carries.
//!
//! # Text form
//!
//! `uuid:1-5:7:tag:1-3,uuid2:1-9`. After the UUID, a segment starting with
//! `[a-z_]` is a tag (MySQL 8.4+) that applies to the numeric segments after
//! it; a numeric segment is a transaction number `n` or an inclusive range
//! `n-m`. Whitespace, newlines included, is ignored: `@@global.gtid_executed`
//! comes back wrapped at one UUID per line.
//!
//! # Intervals
//!
//! Intervals are held inclusive and merged (overlapping and adjacent ranges
//! collapse), so equal sets compare and render equal. The wire format is
//! half-open: [`GtidSet::to_sids`] adds one to each end, as `Sid::from_str`
//! does.

use std::collections::BTreeMap;
use std::fmt::{self, Write as _};
use std::str::FromStr;

use mysql_common::packets::{GnoInterval, Sid, Tag};

/// The largest transaction number MySQL assigns (`2^63 - 1`).
const MAX_GNO: u64 = i64::MAX as u64;

/// One UUID's transactions under one tag; `None` is the untagged group.
type Key = ([u8; 16], Option<Tag<'static>>);

/// A set of executed transactions, grouped by server UUID and tag.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GtidSet {
    /// Inclusive `(start, end)` intervals per group, sorted, disjoint and
    /// non-adjacent.
    groups: BTreeMap<Key, Vec<(u64, u64)>>,
}

impl GtidSet {
    /// True when the set holds no transaction.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Adds one transaction `uuid[:tag]:gno`.
    ///
    /// # Errors
    ///
    /// A `gno` of 0 or above `2^63 - 1`, or a tag that is not `[a-z_][a-z0-9_]{0,31}`.
    pub(crate) fn insert(
        &mut self,
        uuid: [u8; 16],
        tag: Option<&str>,
        gno: u64,
    ) -> Result<(), String> {
        check_gno(gno)?;
        let tag = tag.map(check_tag).transpose()?;
        add_range(self.groups.entry((uuid, tag)).or_default(), gno, gno);
        Ok(())
    }

    /// Whether every transaction of this set is also in `other`.
    pub(crate) fn is_subset_of(&self, other: &GtidSet) -> bool {
        self.groups.iter().all(|(group, intervals)| {
            other.groups.get(group).is_some_and(|theirs| {
                // `theirs` is sorted, disjoint and non-adjacent, so an interval
                // of this set lies inside one of theirs or not at all.
                intervals.iter().all(|&(start, end)| {
                    let at = theirs.partition_point(|&(_, last)| last < start);
                    theirs
                        .get(at)
                        .is_some_and(|&(first, last)| first <= start && end <= last)
                })
            })
        })
    }

    /// How many transactions the set holds: of two sets where one contains the
    /// other, the larger has the greater count.
    pub(crate) fn transactions(&self) -> u128 {
        self.groups
            .values()
            .flatten()
            .map(|&(first, last)| u128::from(last - first) + 1)
            .sum()
    }

    /// The set as `Sid` blocks for a binlog dump request, one per UUID and
    /// tag, with exclusive interval ends.
    pub(crate) fn to_sids(&self) -> Vec<Sid<'static>> {
        self.groups
            .iter()
            .map(|((uuid, tag), intervals)| {
                let sid = Sid::new(*uuid).with_intervals(
                    intervals
                        .iter()
                        .map(|&(start, end)| GnoInterval::new(start, end + 1))
                        .collect(),
                );
                match tag {
                    Some(tag) => sid.with_tag(tag.clone()),
                    None => sid,
                }
            })
            .collect()
    }
}

/// `uuid[:tag]:gno` for one transaction, the form the `__gtid` column carries.
pub(crate) fn format_gtid(uuid: &[u8; 16], tag: Option<&str>, gno: u64) -> String {
    let mut out = format_uuid(uuid);
    if let Some(tag) = tag.filter(|t| !t.is_empty()) {
        let _ = write!(out, ":{tag}");
    }
    let _ = write!(out, ":{gno}");
    out
}

/// Lower-case hyphenated UUID text (`8-4-4-4-12`).
pub(crate) fn format_uuid(uuid: &[u8; 16]) -> String {
    let mut out = String::with_capacity(36);
    for (i, byte) in uuid.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        let _ = write!(out, "{byte:02x}");
    }
    out
}

impl fmt::Display for GtidSet {
    /// Canonical text: lower-case UUIDs ascending, each UUID's untagged group
    /// first then its tags sorted, groups of one UUID joined into one entry
    /// (`uuid:1-5:7:tag:1-3`), entries joined by `,`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut previous: Option<&[u8; 16]> = None;
        for ((uuid, tag), intervals) in &self.groups {
            if previous != Some(uuid) {
                if previous.is_some() {
                    f.write_char(',')?;
                }
                f.write_str(&format_uuid(uuid))?;
                previous = Some(uuid);
            }
            if let Some(tag) = tag {
                write!(f, ":{tag}")?;
            }
            for &(start, end) in intervals {
                f.write_char(':')?;
                if start == end {
                    write!(f, "{start}")?;
                } else {
                    write!(f, "{start}-{end}")?;
                }
            }
        }
        Ok(())
    }
}

impl FromStr for GtidSet {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        let mut set = GtidSet::default();
        if compact.is_empty() {
            return Ok(set);
        }
        for entry in compact.split(',') {
            parse_entry(&mut set, entry)
                .map_err(|e| format!("invalid GTID set '{compact}': {e}"))?;
        }
        Ok(set)
    }
}

/// Parses one `uuid:...` entry into `set`.
fn parse_entry(set: &mut GtidSet, entry: &str) -> Result<(), String> {
    let mut segments = entry.split(':');
    let uuid = parse_uuid(segments.next().unwrap_or_default())?;
    let mut tag: Option<Tag<'static>> = None;
    // Whether the current group has received an interval yet.
    let mut interval_seen = true;
    let mut any = false;
    for segment in segments {
        any = true;
        let first = segment.chars().next();
        match first {
            None => return Err(format!("empty segment in entry '{entry}'")),
            Some(c) if c.is_ascii_lowercase() || c == '_' => {
                if !interval_seen {
                    return Err(format!(
                        "tag '{}' has no transactions",
                        tag.as_ref().map_or("", Tag::as_str)
                    ));
                }
                tag = Some(check_tag(segment)?);
                interval_seen = false;
            }
            Some(_) => {
                let (start, end) = parse_interval(segment)?;
                let group = set.groups.entry((uuid, tag.clone())).or_default();
                add_range(group, start, end);
                interval_seen = true;
            }
        }
    }
    if !any {
        return Err(format!("entry '{entry}' has no transaction numbers"));
    }
    if !interval_seen {
        return Err(format!(
            "tag '{}' has no transactions",
            tag.as_ref().map_or("", Tag::as_str)
        ));
    }
    Ok(())
}

/// `n` or `n-m`, both inclusive and within `1..=MAX_GNO`.
fn parse_interval(segment: &str) -> Result<(u64, u64), String> {
    let number = |s: &str| {
        s.parse::<u64>()
            .map_err(|_| format!("'{segment}' is not a transaction number or range"))
            .and_then(|n| check_gno(n).map(|()| n))
    };
    match segment.split_once('-') {
        None => number(segment).map(|n| (n, n)),
        Some((start, end)) => {
            let (start, end) = (number(start)?, number(end)?);
            if start > end {
                return Err(format!("range '{segment}' ends before it starts"));
            }
            Ok((start, end))
        }
    }
}

fn check_gno(gno: u64) -> Result<(), String> {
    if gno == 0 || gno > MAX_GNO {
        Err(format!("transaction number {gno} is outside 1..={MAX_GNO}"))
    } else {
        Ok(())
    }
}

fn check_tag(tag: &str) -> Result<Tag<'static>, String> {
    Tag::new(tag.to_owned()).map_err(|e| format!("tag '{tag}': {e}"))
}

/// A UUID as 32 hex digits, hyphenated or not, in either case.
fn parse_uuid(text: &str) -> Result<[u8; 16], String> {
    let bad = || format!("'{text}' is not a UUID");
    let digits: Vec<u8> = text.bytes().filter(|&b| b != b'-').collect();
    if digits.len() != 32 || text.len() - digits.len() > 4 {
        return Err(bad());
    }
    if text.contains('-') {
        // Hyphens, when present, sit at the canonical 8-4-4-4-12 offsets.
        let hyphens: Vec<usize> = text.match_indices('-').map(|(i, _)| i).collect();
        if hyphens != [8, 13, 18, 23] {
            return Err(bad());
        }
    }
    let mut out = [0u8; 16];
    for (byte, [hi, lo]) in out.iter_mut().zip(digits.as_chunks::<2>().0) {
        let hi = hex_value(*hi).ok_or_else(bad)?;
        let lo = hex_value(*lo).ok_or_else(bad)?;
        *byte = hi << 4 | lo;
    }
    Ok(out)
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Inserts the inclusive range `start..=end` into a sorted, disjoint,
/// non-adjacent interval list, merging whatever it touches.
fn add_range(intervals: &mut Vec<(u64, u64)>, start: u64, end: u64) {
    // Both bounds are at most `MAX_GNO`, so `+ 1` cannot overflow.
    let first = intervals.partition_point(|&(_, e)| e + 1 < start);
    let (mut lo, mut hi) = (start, end);
    let mut last = first;
    while last < intervals.len() && intervals[last].0 <= hi + 1 {
        lo = lo.min(intervals[last].0);
        hi = hi.max(intervals[last].1);
        last += 1;
    }
    intervals.splice(first..last, [(lo, hi)]);
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const B: &str = "00000000-0000-0000-0000-000000000001";

    fn uuid(text: &str) -> [u8; 16] {
        parse_uuid(text).expect("uuid")
    }

    fn set(text: &str) -> GtidSet {
        text.parse().expect("gtid set")
    }

    #[test]
    fn an_empty_text_is_the_empty_set() {
        for text in ["", "  ", "\n\t "] {
            let parsed = set(text);
            assert!(parsed.is_empty());
            assert_eq!(parsed.to_string(), "");
            assert!(parsed.to_sids().is_empty());
        }
    }

    #[test]
    fn a_canonical_set_round_trips() {
        for text in [
            format!("{A}:1"),
            format!("{A}:1-5"),
            format!("{A}:1-5:7"),
            format!("{B}:1-3,{A}:1-5:7-9"),
            format!("{B}:2:4:6,{A}:1-5"),
        ] {
            let parsed = set(&text);
            assert_eq!(parsed.to_string(), text);
            assert_eq!(set(&parsed.to_string()), parsed);
        }
    }

    #[test]
    fn entries_render_by_uuid_whatever_their_input_order() {
        assert_eq!(
            set(&format!("{A}:1-5:7-9,{B}:1-3")).to_string(),
            format!("{B}:1-3,{A}:1-5:7-9")
        );
    }

    #[test]
    fn whitespace_and_newlines_are_ignored() {
        let wrapped = format!("{A}:1-5:7,\n{B}:1-3\n");
        let spaced = format!("  {A} : 1-5 : 7 ,\r\n  {B}:1 - 3  ");
        assert_eq!(set(&wrapped), set(&spaced));
        assert_eq!(set(&wrapped).to_string(), format!("{B}:1-3,{A}:1-5:7"));
    }

    #[test]
    fn upper_case_and_unhyphenated_uuids_render_lower_case_hyphenated() {
        let upper = A.to_uppercase();
        let bare = A.replace('-', "");
        assert_eq!(set(&format!("{upper}:1-2")).to_string(), format!("{A}:1-2"));
        assert_eq!(set(&format!("{bare}:1-2")).to_string(), format!("{A}:1-2"));
    }

    #[test]
    fn overlapping_and_adjacent_intervals_merge() {
        assert_eq!(set(&format!("{A}:1-3:4-6")).to_string(), format!("{A}:1-6"));
        assert_eq!(set(&format!("{A}:1-5:3-8")).to_string(), format!("{A}:1-8"));
        assert_eq!(set(&format!("{A}:5:1-3:4")).to_string(), format!("{A}:1-5"));
        assert_eq!(
            set(&format!("{A}:1-3:5-6:9")).to_string(),
            format!("{A}:1-3:5-6:9")
        );
        assert_eq!(set(&format!("{A}:1-9:3-4")).to_string(), format!("{A}:1-9"));
        assert_eq!(
            set(&format!("{A}:1-3:7-9:2-8")).to_string(),
            format!("{A}:1-9")
        );
    }

    #[test]
    fn entries_for_one_uuid_merge_into_one() {
        assert_eq!(
            set(&format!("{A}:1-3,{A}:4-6,{B}:2,{A}:9")).to_string(),
            format!("{B}:2,{A}:1-6:9")
        );
    }

    #[test]
    fn a_set_is_a_subset_of_one_that_holds_every_transaction_of_it() {
        let wide = set(&format!("{A}:1-10:20-30,{B}:1-3"));
        assert!(set("").is_subset_of(&wide));
        assert!(set(&format!("{A}:1-10")).is_subset_of(&wide));
        assert!(set(&format!("{A}:4:25-30,{B}:2-3")).is_subset_of(&wide));
        assert!(wide.is_subset_of(&wide));
        // An interval spanning the gap between two of theirs is not inside either.
        assert!(!set(&format!("{A}:9-21")).is_subset_of(&wide));
        assert!(!set(&format!("{A}:31")).is_subset_of(&wide));
        assert!(!set(&format!("{B}:4")).is_subset_of(&wide));
        assert!(!wide.is_subset_of(&set(&format!("{A}:1-10"))));
        // Another tag is another group.
        assert!(!set(&format!("{A}:t:1")).is_subset_of(&wide));
    }

    #[test]
    fn the_transaction_count_orders_sets_that_contain_one_another() {
        assert_eq!(set("").transactions(), 0);
        assert_eq!(set(&format!("{A}:1-10:20-30,{B}:1-3")).transactions(), 24);
        assert!(set(&format!("{A}:1-5")).transactions() < set(&format!("{A}:1-6")).transactions());
    }

    #[test]
    fn insert_extends_merges_and_ignores_duplicates() {
        let mut gtids = GtidSet::default();
        let id = uuid(A);
        for gno in [5, 3, 4, 1, 1, 9, 7] {
            gtids.insert(id, None, gno).expect("insert");
        }
        assert_eq!(gtids.to_string(), format!("{A}:1:3-5:7:9"));
        gtids.insert(id, None, 2).expect("insert");
        gtids.insert(id, None, 6).expect("insert");
        gtids.insert(id, None, 8).expect("insert");
        assert_eq!(gtids.to_string(), format!("{A}:1-9"));
        gtids.insert(id, None, 10).expect("insert");
        assert_eq!(gtids.to_string(), format!("{A}:1-10"));
    }

    #[test]
    fn insert_into_a_parsed_set_continues_the_last_interval() {
        let mut gtids = set(&format!("{A}:1-5"));
        gtids.insert(uuid(A), None, 6).expect("insert");
        gtids.insert(uuid(B), None, 1).expect("insert");
        assert_eq!(gtids.to_string(), format!("{B}:1,{A}:1-6"));
    }

    #[test]
    fn insert_rejects_a_zero_gno_a_gno_above_the_server_maximum_and_a_bad_tag() {
        let mut gtids = GtidSet::default();
        let id = uuid(A);
        assert!(gtids.insert(id, None, 0).is_err());
        assert!(gtids.insert(id, None, MAX_GNO + 1).is_err());
        assert!(gtids.insert(id, None, u64::MAX).is_err());
        assert!(gtids.insert(id, Some("Bad"), 1).is_err());
        assert!(gtids.insert(id, Some(""), 1).is_err());
        assert!(gtids.is_empty());
        gtids.insert(id, None, MAX_GNO).expect("the largest gno");
        assert_eq!(gtids.to_string(), format!("{A}:{MAX_GNO}"));
        assert_eq!(gtids.to_sids()[0].intervals()[0].end(), MAX_GNO + 1);
    }

    #[test]
    fn a_tagged_set_round_trips_and_keeps_tags_apart() {
        let text = format!("{A}:1-5:7:tag_a:1-3:9:zeta:4");
        let parsed = set(&text);
        assert_eq!(parsed.to_string(), text);
        assert_eq!(parsed.to_sids().len(), 3);
        assert_eq!(set(&parsed.to_string()), parsed);
    }

    #[test]
    fn untagged_groups_render_before_tags_and_tags_sort() {
        let mut gtids = set(&format!("{A}:zeta:1:tag_a:2-3"));
        gtids.insert(uuid(A), None, 8).expect("insert");
        assert_eq!(gtids.to_string(), format!("{A}:8:tag_a:2-3:zeta:1"));
        assert_eq!(set(&gtids.to_string()), gtids);
    }

    #[test]
    fn a_tag_applies_until_the_next_tag() {
        let parsed = set(&format!("{A}:t:1-2:5:u:3"));
        assert_eq!(parsed.to_string(), format!("{A}:t:1-2:5:u:3"));
        let mut gtids = GtidSet::default();
        gtids.insert(uuid(A), Some("t"), 3).expect("insert");
        gtids.insert(uuid(A), Some("t"), 4).expect("insert");
        gtids.insert(uuid(A), None, 4).expect("insert");
        assert_eq!(gtids.to_string(), format!("{A}:4:t:3-4"));
    }

    #[test]
    fn to_sids_uses_exclusive_ends_like_sid_from_str() {
        let text = format!("{A}:1-5:7");
        let sids = set(&text).to_sids();
        assert_eq!(sids.len(), 1);
        let from_str: Sid<'static> = text.parse().expect("sid");
        assert_eq!(sids[0], from_str);
        assert_eq!(sids[0].uuid(), uuid(A));
        let intervals: Vec<(u64, u64)> = sids[0]
            .intervals()
            .iter()
            .map(|i| (i.start(), i.end()))
            .collect();
        assert_eq!(intervals, [(1, 6), (7, 8)]);
        assert_eq!(sids[0].to_string(), text);
    }

    #[test]
    fn a_single_transaction_is_a_one_wide_half_open_interval() {
        let sids = set(&format!("{A}:3")).to_sids();
        let interval = &sids[0].intervals()[0];
        assert_eq!((interval.start(), interval.end()), (3, 4));
    }

    #[test]
    fn to_sids_carries_the_tag_and_matches_sid_from_str() {
        let text = format!("{A}:tag_a:1-3");
        let sids = set(&text).to_sids();
        assert_eq!(sids.len(), 1);
        assert_eq!(sids[0].tag().map(Tag::as_str), Some("tag_a"));
        let from_str: Sid<'static> = text.parse().expect("sid");
        assert_eq!(sids[0], from_str);
        assert_eq!(sids[0].intervals()[0].end(), 4);
    }

    #[test]
    fn to_sids_emits_one_block_per_uuid_and_tag() {
        let sids = set(&format!("{A}:1-2:t:5,{B}:1")).to_sids();
        assert_eq!(sids.len(), 3);
        assert_eq!(sids[0].uuid(), uuid(B));
        assert!(sids[0].tag().is_none());
        assert_eq!(sids[1].uuid(), uuid(A));
        assert!(sids[1].tag().is_none());
        assert_eq!(sids[2].tag().map(Tag::as_str), Some("t"));
    }

    #[test]
    fn format_gtid_renders_untagged_and_tagged_transactions() {
        assert_eq!(format_gtid(&uuid(A), None, 12), format!("{A}:12"));
        assert_eq!(format_gtid(&uuid(A), Some(""), 12), format!("{A}:12"));
        assert_eq!(format_gtid(&uuid(A), Some("t"), 12), format!("{A}:t:12"));
    }

    #[test]
    fn malformed_sets_are_refused_with_the_offending_text() {
        for (text, needle) in [
            ("nonsense", "is not a UUID"),
            ("3e11fa47-71ca-11e1-9e33-c80aa942956:1", "is not a UUID"),
            ("3e11fa4771ca-11e1-9e33-c80aa9429562:1", "is not a UUID"),
            ("zz11fa47-71ca-11e1-9e33-c80aa9429562:1", "is not a UUID"),
            (A, "no transaction numbers"),
            (&format!("{A}:"), "empty segment"),
            (&format!("{A}:1::2"), "empty segment"),
            (&format!("{A}:0"), "outside"),
            (&format!("{A}:1-0"), "outside"),
            (&format!("{A}:5-3"), "ends before it starts"),
            (&format!("{A}:1-2-3"), "not a transaction number"),
            (&format!("{A}:x1"), "tag 'x1' has no transactions"),
            (&format!("{A}:1,"), "is not a UUID"),
            (&format!("{A}:Tag:1"), "not a transaction number"),
            (&format!("{A}:t:u:1"), "tag 't' has no transactions"),
            (&format!("{A}:1:t"), "tag 't' has no transactions"),
            (&format!("{A}:9223372036854775808"), "outside"),
            (
                &format!("{A}:99999999999999999999999"),
                "not a transaction number",
            ),
            (&format!("{A}:{}", "a".repeat(33)), "too long"),
        ] {
            let err = text.parse::<GtidSet>().expect_err(text);
            assert!(
                err.contains(needle),
                "'{text}' gave '{err}', wanted '{needle}'"
            );
            assert!(err.starts_with("invalid GTID set '"), "{err}");
        }
    }

    #[test]
    fn the_largest_server_gno_parses() {
        let text = format!("{A}:1-{MAX_GNO}");
        assert_eq!(set(&text).to_string(), text);
        assert_eq!(set(&text).to_sids()[0].intervals()[0].end(), MAX_GNO + 1);
    }
}
