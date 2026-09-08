//! The PostgreSQL `numeric` wire forms, binary and text, in both directions,
//! plus the one `money` rescale both halves of the connector share.
//!
//! Neither `postgres-types` nor `postgres-protocol` exposes this, so the codec
//! lives here. The layout is
//!
//! ```text
//! i16 ndigits          number of base-10000 digit groups that follow
//! i16 weight           base-10000 exponent of the first group
//! u16 sign             0x0000 positive, 0x4000 negative,
//!                      0xC000 NaN, 0xD000 +Infinity, 0xF000 -Infinity
//! u16 dscale           display scale: decimal digits after the point
//! i16 digits[ndigits]  each 0..=9999, most significant first
//! ```
//!
//! and the value is `Σ digits[i] · 10000^(weight − i)`.
//!
//! Arrow's `Decimal128` is a fixed-scale `i128`, so both directions carry the
//! declared scale. A value whose wire `dscale` exceeds the declared scale is
//! rejected rather than truncated: silently dropping fractional digits is the
//! one failure mode a numeric codec must not have.
//!
//! `money` lives here too: it is an `i64` of hundredths rather than a
//! `numeric`, but the rule that carries it to and from a declared
//! `Decimal128` scale is the same never-truncate rule, so
//! [`money_factor`] is the single place that decides it.

use bytes::{BufMut, BytesMut};
use saci_core::error::SaciError;

const SIGN_POS: u16 = 0x0000;
const SIGN_NEG: u16 = 0x4000;
const SIGN_NAN: u16 = 0xC000;
const SIGN_PINF: u16 = 0xD000;
const SIGN_NINF: u16 = 0xF000;

/// Decode one `numeric` binary value into an `i128` scaled by `target_scale`.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] for a truncated or malformed buffer, for the
/// NaN and ±Infinity sign codes, when the wire value carries more fractional
/// digits than `target_scale` admits, and when the rescaled value does not fit
/// an `i128`. Every message names `column`.
pub(crate) fn numeric_to_i128(
    raw: &[u8],
    target_scale: i8,
    column: &str,
) -> Result<i128, SaciError> {
    if raw.len() < 8 {
        return Err(SaciError::generic(format!(
            "column '{column}': numeric value is {} bytes, expected at least the 8-byte header",
            raw.len()
        )));
    }
    let ndigits = i16::from_be_bytes([raw[0], raw[1]]);
    let weight = i32::from(i16::from_be_bytes([raw[2], raw[3]]));
    let sign = u16::from_be_bytes([raw[4], raw[5]]);
    let dscale = u16::from_be_bytes([raw[6], raw[7]]);

    match sign {
        SIGN_POS | SIGN_NEG => {}
        SIGN_NAN => {
            return Err(SaciError::generic(format!(
                "column '{column}': numeric NaN has no decimal128 representation; select the \
                 column with a ::text cast and declare type = \"utf8\" to carry it"
            )));
        }
        SIGN_PINF | SIGN_NINF => {
            return Err(SaciError::generic(format!(
                "column '{column}': numeric {}Infinity has no decimal128 representation; select \
                 the column with a ::text cast and declare type = \"utf8\" to carry it",
                if sign == SIGN_PINF { "+" } else { "-" }
            )));
        }
        other => {
            return Err(SaciError::generic(format!(
                "column '{column}': numeric sign field 0x{other:04X} is not a known sign code"
            )));
        }
    }

    if ndigits < 0 {
        return Err(SaciError::generic(format!(
            "column '{column}': numeric ndigits is negative ({ndigits})"
        )));
    }
    let n = usize::from(ndigits.unsigned_abs());
    if raw.len() != 8 + 2 * n {
        return Err(SaciError::generic(format!(
            "column '{column}': numeric value is {} bytes, expected {} for {n} digit group(s)",
            raw.len(),
            8 + 2 * n
        )));
    }

    if target_scale < 0 {
        return Err(SaciError::generic(format!(
            "column '{column}': declared decimal128 scale {target_scale} is negative"
        )));
    }
    if i32::from(dscale) > i32::from(target_scale) {
        return Err(SaciError::generic(format!(
            "column '{column}': numeric value carries {dscale} fractional digit(s) but the \
             declared decimal128 scale is {target_scale}; raise 'scale' to keep the value exact"
        )));
    }

    let overflow = || {
        SaciError::generic(format!(
            "column '{column}': numeric value does not fit a decimal128 at scale {target_scale}"
        ))
    };

    let digit = |i: usize| -> Result<i128, SaciError> {
        let raw_digit = i16::from_be_bytes([raw[8 + 2 * i], raw[9 + 2 * i]]);
        if !(0..10_000).contains(&raw_digit) {
            return Err(SaciError::generic(format!(
                "column '{column}': numeric digit group {i} is {raw_digit}, outside 0..=9999"
            )));
        }
        Ok(i128::from(raw_digit))
    };

    let inexact = || {
        SaciError::generic(format!(
            "column '{column}': numeric value needs more than {target_scale} fractional digit(s); \
             raise 'scale' to keep the value exact"
        ))
    };

    // The digit groups form one base-10000 integer whose value is that integer
    // times 10^exponent. A negative exponent is applied *before* the groups are
    // folded together, because the unshifted integer can exceed an i128 even
    // when the scaled value fits: 38 nines at scale 10 pads to 40 digits.
    let exponent = 4 * (weight - n as i32 + 1) + i32::from(target_scale);
    let mut value: i128 = 0;

    if exponent >= 0 {
        // value >= mantissa here, so folding first cannot overflow spuriously.
        for i in 0..n {
            let d = digit(i)?;
            value = value
                .checked_mul(10_000)
                .and_then(|v| v.checked_add(d))
                .ok_or_else(overflow)?;
        }
        for _ in 0..exponent {
            if value == 0 {
                break;
            }
            value = value.checked_mul(10).ok_or_else(overflow)?;
        }
    } else {
        let shift = (-exponent) as usize;
        let dropped_groups = shift / 4;
        let kept = n.saturating_sub(dropped_groups);
        // Every group the shift consumes whole must be zero, or the value has
        // more fractional digits than the declared scale admits.
        for i in kept..n {
            if digit(i)? != 0 {
                return Err(inexact());
            }
        }
        if kept > 0 {
            let divisor = 10i128.pow((shift % 4) as u32);
            for i in 0..kept - 1 {
                let d = digit(i)?;
                value = value
                    .checked_mul(10_000)
                    .and_then(|v| v.checked_add(d))
                    .ok_or_else(overflow)?;
            }
            let last = digit(kept - 1)?;
            if last % divisor != 0 {
                return Err(inexact());
            }
            // 10000 is divisible by 10^(shift % 4), so the division distributes
            // over the final fold and never loses a digit.
            value = value
                .checked_mul(10_000 / divisor)
                .and_then(|v| v.checked_add(last / divisor))
                .ok_or_else(overflow)?;
        }
    }

    Ok(if sign == SIGN_NEG { -value } else { value })
}

/// Decode `numeric`'s canonical **text** form into an `i128` scaled by
/// `target_scale`.
///
/// The text half of [`numeric_to_i128`], for the logical path's text tuples
/// and for a `money` column, whose text is the same grammar behind a currency
/// symbol and thousands separators. Enforces the identical rule: a value
/// carrying more fractional digits than `target_scale` admits is rejected
/// rather than rounded, because silently dropping digits is the one failure
/// mode a numeric codec must not have.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] for `NaN`/`±Infinity`, for a malformed
/// number, for excess fractional digits, and when the rescaled value does not
/// fit an `i128`. Every message names `column`.
pub(crate) fn numeric_text_to_i128(
    text: &str,
    target_scale: i8,
    column: &str,
) -> Result<i128, SaciError> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("nan") {
        return Err(SaciError::generic(format!(
            "column '{column}': numeric NaN has no decimal128 representation; declare type = \
             \"utf8\" to carry it"
        )));
    }
    let unsigned = trimmed
        .strip_prefix('-')
        .or_else(|| trimmed.strip_prefix('+'))
        .unwrap_or(trimmed);
    if unsigned.eq_ignore_ascii_case("infinity") || unsigned.eq_ignore_ascii_case("inf") {
        return Err(SaciError::generic(format!(
            "column '{column}': numeric {trimmed} has no decimal128 representation; declare type \
             = \"utf8\" to carry it"
        )));
    }

    let negative = trimmed.starts_with('-');
    // PostgreSQL's `numeric` output carries no exponent and no separators, but
    // `money` renders thousands separators under every locale, so they are
    // skipped rather than rejected.
    let (integer, fraction) = match unsigned.split_once('.') {
        Some((integer, fraction)) => (integer, fraction),
        None => (unsigned, ""),
    };

    let mut digits = 0usize;
    let mut value: i128 = 0;
    for byte in integer.bytes().chain(fraction.bytes()) {
        if byte == b',' || byte == b'_' || byte == b' ' {
            continue;
        }
        let digit = match byte {
            b'0'..=b'9' => i128::from(byte - b'0'),
            _ => {
                return Err(SaciError::generic(format!(
                    "column '{column}': '{trimmed}' is not a numeric value"
                )));
            }
        };
        digits += 1;
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(digit))
            .ok_or_else(|| {
                SaciError::generic(format!(
                    "column '{column}': numeric {trimmed} does not fit a 128-bit decimal"
                ))
            })?;
    }
    if digits == 0 {
        return Err(SaciError::generic(format!(
            "column '{column}': '{trimmed}' is not a numeric value"
        )));
    }

    // `fraction` may carry separators too, so count what was actually read.
    let fraction_digits = fraction.bytes().filter(|b| b.is_ascii_digit()).count();
    let target = usize::try_from(target_scale).unwrap_or(0);
    if fraction_digits > target {
        return Err(SaciError::generic(format!(
            "column '{column}': numeric {trimmed} carries {fraction_digits} fractional digit(s) \
             but the declared scale is {target_scale}; widen 'scale' rather than lose digits"
        )));
    }
    for _ in fraction_digits..target {
        value = value.checked_mul(10).ok_or_else(|| {
            SaciError::generic(format!(
                "column '{column}': numeric {trimmed} does not fit a 128-bit decimal at scale \
                 {target_scale}"
            ))
        })?;
    }

    Ok(if negative { -value } else { value })
}

/// The factor between `money`'s two fractional digits and a declared
/// `Decimal128` scale.
///
/// `lc_monetary = 'C'` is pinned on every session, so a `money` value is
/// always hundredths of the currency unit: a wider declared scale multiplies
/// by this factor on the way into Arrow and divides by it on the way back out,
/// and a narrower one could not hold the two digits at all. One function
/// rather than three, because the decoder, the encoder and the sink's
/// connect-time check all have to agree on it.
///
/// # Errors
///
/// Returns the reason `scale` cannot carry `money`, with no column prefix:
/// each caller adds its own.
pub(crate) fn money_factor(scale: i8) -> Result<i128, String> {
    let extra = u32::try_from(i16::from(scale) - 2).map_err(|_| {
        format!(
            "money keeps 2 fractional digits, so scale {scale} would lose them; declare \
             scale = 2 or wider"
        )
    })?;
    10i128
        .checked_pow(extra)
        .ok_or_else(|| format!("scale {scale} is too wide to rescale money into"))
}

/// Encode an `i128` scaled by `scale` into the `numeric` binary wire form.
///
/// `scale` is the declared Arrow `Decimal128` scale and is written as the wire
/// `dscale`, so the value round-trips through [`numeric_to_i128`] unchanged.
pub(crate) fn i128_to_numeric(value: i128, scale: i8, out: &mut BytesMut) {
    let scale = scale.max(0);
    let scale_digits = u32::from(scale.unsigned_abs());
    let negative = value < 0;

    // Decimal digits, least significant first, left-padded so that the
    // fractional part fills whole base-10000 groups. i128 spans 39 decimal
    // digits and the pad adds at most 3.
    let mut lsd = [0u8; 48];
    let pad = ((4 - scale_digits % 4) % 4) as usize;
    let mut len = pad;
    let mut magnitude = value.unsigned_abs();
    while magnitude > 0 {
        lsd[len] = (magnitude % 10) as u8;
        magnitude /= 10;
        len += 1;
    }

    let groups = len.div_ceil(4);
    let fraction_groups = (scale_digits as usize + pad) / 4;

    let mut digits = [0i16; 16];
    for group in 0..groups {
        let mut packed = 0i16;
        let mut place = 1i16;
        for offset in 0..4 {
            let index = 4 * group + offset;
            if index < len {
                packed += i16::from(lsd[index]) * place;
            }
            place = place.saturating_mul(10);
        }
        digits[groups - 1 - group] = packed;
    }

    // Leading and trailing all-zero groups are not transmitted; dropping a
    // leading group lowers the exponent of the one that becomes first.
    let mut first = 0usize;
    let mut last = groups;
    let mut weight = groups as i32 - 1 - fraction_groups as i32;
    while first < last && digits[first] == 0 {
        first += 1;
        weight -= 1;
    }
    while last > first && digits[last - 1] == 0 {
        last -= 1;
    }

    let ndigits = last - first;
    let (weight, sign) = if ndigits == 0 {
        (0, SIGN_POS)
    } else {
        (weight, if negative { SIGN_NEG } else { SIGN_POS })
    };

    out.reserve(8 + 2 * ndigits);
    out.put_i16(ndigits as i16);
    out.put_i16(weight as i16);
    out.put_u16(sign);
    out.put_u16(scale_digits as u16);
    for digit in &digits[first..last] {
        out.put_i16(*digit);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(value: i128, scale: i8) -> i128 {
        let mut buf = BytesMut::new();
        i128_to_numeric(value, scale, &mut buf);
        numeric_to_i128(&buf, scale, "col").expect("decode")
    }

    fn header(buf: &[u8]) -> (i16, i16, u16, u16) {
        (
            i16::from_be_bytes([buf[0], buf[1]]),
            i16::from_be_bytes([buf[2], buf[3]]),
            u16::from_be_bytes([buf[4], buf[5]]),
            u16::from_be_bytes([buf[6], buf[7]]),
        )
    }

    #[test]
    fn round_trips_representative_values() {
        for (value, scale) in [
            (0i128, 0i8),
            (0, 4),
            (1, 0),
            (-1, 0),
            (1, 4),
            (-1, 4),
            (123, 2),
            (-123, 2),
            (123_456_789, 4),
            (-123_456_789, 4),
            (10_000, 0),
            (1_000_000, 6),
            (i64::MAX as i128, 0),
            (i64::MIN as i128, 3),
        ] {
            assert_eq!(
                round_trip(value, scale),
                value,
                "value {value} scale {scale}"
            );
        }
    }

    #[test]
    fn round_trips_a_thirty_eight_digit_value() {
        let value: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;
        assert_eq!(value.to_string().len(), 38);
        assert_eq!(round_trip(value, 0), value);
        assert_eq!(round_trip(-value, 0), -value);
        assert_eq!(round_trip(value, 10), value);
    }

    #[test]
    fn encodes_12345_6789_as_three_digit_groups() {
        let mut buf = BytesMut::new();
        i128_to_numeric(123_456_789, 4, &mut buf);
        assert_eq!(header(&buf), (3, 1, SIGN_POS, 4));
        assert_eq!(
            &buf[8..],
            &[0, 1, 9, 41, 26, 133][..],
            "digit groups should be 1, 2345, 6789"
        );
        assert_eq!(i16::from_be_bytes([buf[10], buf[11]]), 2345);
        assert_eq!(i16::from_be_bytes([buf[12], buf[13]]), 6789);
    }

    #[test]
    fn zero_is_the_empty_digit_list() {
        let mut buf = BytesMut::new();
        i128_to_numeric(0, 4, &mut buf);
        assert_eq!(header(&buf), (0, 0, SIGN_POS, 4));
        assert_eq!(buf.len(), 8);
    }

    #[test]
    fn trailing_zero_groups_are_not_transmitted() {
        // 10000 with scale 0 is one group of 1 at weight 1, not [1, 0].
        let mut buf = BytesMut::new();
        i128_to_numeric(10_000, 0, &mut buf);
        assert_eq!(header(&buf), (1, 1, SIGN_POS, 0));
        assert_eq!(buf.len(), 10);
    }

    #[test]
    fn leading_zero_groups_are_not_transmitted() {
        // 1 at scale 4 is 0.0001: one group of 1 at weight -1.
        let mut buf = BytesMut::new();
        i128_to_numeric(1, 4, &mut buf);
        assert_eq!(header(&buf), (1, -1, SIGN_POS, 4));
        assert_eq!(i16::from_be_bytes([buf[8], buf[9]]), 1);
    }

    #[test]
    fn decodes_a_wire_value_with_fewer_fractional_digits_than_declared() {
        // 1.2 sent as digits [1, 2000], weight 0, dscale 1, read at scale 4.
        let mut raw = BytesMut::new();
        raw.put_i16(2);
        raw.put_i16(0);
        raw.put_u16(SIGN_POS);
        raw.put_u16(1);
        raw.put_i16(1);
        raw.put_i16(2000);
        assert_eq!(numeric_to_i128(&raw, 4, "col").unwrap(), 12_000);
        assert_eq!(numeric_to_i128(&raw, 1, "col").unwrap(), 12);
    }

    #[test]
    fn wire_dscale_above_declared_scale_is_rejected() {
        let mut raw = BytesMut::new();
        i128_to_numeric(1_234_567, 6, &mut raw);
        let err = numeric_to_i128(&raw, 2, "amount").unwrap_err();
        assert_eq!(err.category(), "generic");
        assert!(err.message().contains("amount"), "{}", err.message());
        assert!(err.message().contains("scale"), "{}", err.message());
    }

    #[test]
    fn nan_and_infinities_are_rejected_with_a_text_cast_hint() {
        for sign in [SIGN_NAN, SIGN_PINF, SIGN_NINF] {
            let mut raw = BytesMut::new();
            raw.put_i16(0);
            raw.put_i16(0);
            raw.put_u16(sign);
            raw.put_u16(0);
            let err = numeric_to_i128(&raw, 2, "amount").unwrap_err();
            assert!(err.message().contains("::text"), "{}", err.message());
            assert!(err.message().contains("amount"), "{}", err.message());
        }
    }

    #[test]
    fn unknown_sign_code_is_rejected() {
        let mut raw = BytesMut::new();
        raw.put_i16(0);
        raw.put_i16(0);
        raw.put_u16(0x1234);
        raw.put_u16(0);
        let err = numeric_to_i128(&raw, 0, "amount").unwrap_err();
        assert!(err.message().contains("0x1234"), "{}", err.message());
    }

    #[test]
    fn short_and_mismatched_buffers_are_rejected() {
        let err = numeric_to_i128(&[0, 1, 0], 0, "amount").unwrap_err();
        assert!(err.message().contains("8-byte header"), "{}", err.message());

        let mut raw = BytesMut::new();
        raw.put_i16(2);
        raw.put_i16(0);
        raw.put_u16(SIGN_POS);
        raw.put_u16(0);
        raw.put_i16(1);
        let err = numeric_to_i128(&raw, 0, "amount").unwrap_err();
        assert!(err.message().contains("expected 12"), "{}", err.message());
    }

    #[test]
    fn out_of_range_digit_group_is_rejected() {
        let mut raw = BytesMut::new();
        raw.put_i16(1);
        raw.put_i16(0);
        raw.put_u16(SIGN_POS);
        raw.put_u16(0);
        raw.put_i16(10_000);
        let err = numeric_to_i128(&raw, 0, "amount").unwrap_err();
        assert!(err.message().contains("0..=9999"), "{}", err.message());
    }

    #[test]
    fn a_value_beyond_decimal128_range_is_rejected() {
        // 12 digit groups of 9999 is far past 10^38.
        let mut raw = BytesMut::new();
        raw.put_i16(12);
        raw.put_i16(11);
        raw.put_u16(SIGN_POS);
        raw.put_u16(0);
        for _ in 0..12 {
            raw.put_i16(9999);
        }
        let err = numeric_to_i128(&raw, 0, "amount").unwrap_err();
        assert!(err.message().contains("decimal128"), "{}", err.message());
    }

    #[test]
    fn numeric_text_pads_to_the_declared_scale_and_keeps_the_sign() {
        for (text, scale, expected) in [
            ("0", 0, 0i128),
            ("12", 4, 120_000),
            ("-1234.5", 2, -123_450),
            ("+7.25", 2, 725),
            // `money`'s own rendering: the parser skips thousands separators.
            ("1,234.56", 2, 123_456),
        ] {
            let value = numeric_text_to_i128(text, scale, "amount")
                .unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(value, expected, "{text} at scale {scale}");
        }
    }

    #[test]
    fn numeric_text_that_is_not_a_number_names_the_column_and_the_value() {
        for text in ["", "  ", "12.3.4", "1e5", "abc", "-", "."] {
            let reason = numeric_text_to_i128(text, 2, "amount")
                .expect_err("not a numeric value")
                .message()
                .to_string();
            assert!(reason.contains("'amount'"), "{text:?}: {reason}");
            assert!(
                reason.contains("is not a numeric value"),
                "{text:?}: {reason}"
            );
            // The offending spelling itself, quoted, so a user can see what
            // reached the parser.
            assert!(
                reason.contains(&format!("'{}'", text.trim())),
                "{text:?}: {reason}"
            );
        }
    }

    #[test]
    fn numeric_text_beyond_a_128_bit_decimal_is_rejected() {
        // 40 nines is past i128's 39-digit span.
        let wide = "9".repeat(40);
        let err = numeric_text_to_i128(&wide, 0, "amount").unwrap_err();
        assert!(
            err.message().contains("128-bit decimal"),
            "{}",
            err.message()
        );

        // And so is a value that only overflows once padded to the scale.
        let err = numeric_text_to_i128(&"9".repeat(38), 4, "amount").unwrap_err();
        assert!(
            err.message().contains("128-bit decimal"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn money_factor_carries_the_two_wire_digits_to_the_declared_scale() {
        assert_eq!(money_factor(2), Ok(1));
        assert_eq!(money_factor(4), Ok(100));

        // A narrower scale cannot hold the two digits money always carries.
        for scale in [i8::MIN, 0, 1] {
            let reason = money_factor(scale).expect_err("money keeps 2 digits");
            assert!(reason.contains("2 fractional digits"), "{reason}");
        }
    }
}
