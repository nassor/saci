//! The generated entry key: `{prefix}{seq:020}{suffix}`.
//!
//! Twenty zero-padded digits, which is the width of [`u64::MAX`], so the
//! table's lexicographic key order is the order the sink wrote its entries in
//! and a range scan needs no sort. The sink reads the highest key back at open
//! to resume the sequence, which is what [`parse_seq`] is for.

/// Digits in the sequence segment. `u64::MAX` is 20 digits wide.
const SEQ_WIDTH: usize = 20;

/// Compose the key entry number `seq` is stored under.
pub(crate) fn entry_key(prefix: &str, seq: u64, suffix: &str) -> String {
    format!("{prefix}{seq:0width$}{suffix}", width = SEQ_WIDTH)
}

/// Read the sequence number back out of a key this connector generated.
///
/// `None` for a key that carries another prefix or suffix, or whose middle is
/// not exactly [`SEQ_WIDTH`] digits: the sink resumes past the highest key it
/// recognises and ignores the rest, so a foreign entry sharing the table never
/// silently claims the next sequence number.
pub(crate) fn parse_seq(key: &str, prefix: &str, suffix: &str) -> Option<u64> {
    let middle = key.strip_prefix(prefix)?.strip_suffix(suffix)?;
    if middle.len() != SEQ_WIDTH || !middle.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    middle.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_generated_key_parses_back_to_its_sequence_number() {
        for seq in [0, 1, 42, u64::MAX] {
            let key = entry_key("orders/", seq, ".csv");
            assert_eq!(parse_seq(&key, "orders/", ".csv"), Some(seq));
        }
    }

    #[test]
    fn the_sequence_segment_is_twenty_digits_wide() {
        assert_eq!(entry_key("", 7, ""), "00000000000000000007");
        assert_eq!(entry_key("a/", u64::MAX, ".b"), "a/18446744073709551615.b");
    }

    #[test]
    fn lexicographic_order_is_insertion_order() {
        let mut keys: Vec<String> = [3u64, 1, 20, 100, 2]
            .iter()
            .map(|&s| entry_key("k", s, ""))
            .collect();
        keys.sort();
        let seqs: Vec<u64> = keys
            .iter()
            .map(|k| parse_seq(k, "k", "").expect("generated key"))
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 20, 100]);
    }

    #[test]
    fn a_key_that_is_not_ours_does_not_parse() {
        // Another prefix, another suffix, too few digits, a non-digit middle,
        // and a middle that is wide enough but not numeric.
        assert_eq!(
            parse_seq("archive/00000000000000000000.csv", "orders/", ".csv"),
            None
        );
        assert_eq!(
            parse_seq("orders/00000000000000000000.json", "orders/", ".csv"),
            None
        );
        assert_eq!(
            parse_seq("orders/0000000000000000000.csv", "orders/", ".csv"),
            None
        );
        assert_eq!(
            parse_seq("orders/0000000000000000000x.csv", "orders/", ".csv"),
            None
        );
        assert_eq!(
            parse_seq("orders/-0000000000000000001.csv", "orders/", ".csv"),
            None
        );
    }
}
