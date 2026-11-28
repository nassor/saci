//! Cron expression parsing and timezone-aware calendar scheduling.
//!
//! Supports standard 5-field (`min hour dom month dow`) and 6-field
//! (`sec min hour dom month dow`) cron expressions, standard macros
//! (`@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`,
//! `@hourly`), and timezone-aware evaluation using UTC, fixed offsets
//! (`+HH:MM`/`-HH:MM`), built-in IANA DST rules, and `/usr/share/zoneinfo`
//! TZif fallback on Unix systems.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, Timelike, Utc, Weekday};

/// Maximum search horizon when computing the next cron fire time (~5 years).
const MAX_SEARCH_SECONDS: i64 = 5 * 366 * 86_400;

/// Bitmask-backed set of valid values for a single cron field.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FieldSet {
    /// Allowed values in `[min, max]`.
    bits: u64,
    /// Whether the field was written as a bare `*` (relevant for DOM/DOW Vixie rule).
    wildcard: bool,
}

impl FieldSet {
    fn contains(&self, val: u8) -> bool {
        val < 64 && (self.bits & (1u64 << val)) != 0
    }
}

/// Timezone used to evaluate local calendar fields for a [`CronSchedule`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronTimezone {
    /// Coordinated Universal Time.
    Utc,
    /// Fixed offset east of UTC in seconds.
    FixedOffset(i32),
    /// Named IANA timezone with built-in DST rule or system TZif transitions.
    Named(NamedZone),
}

/// Supported named IANA timezones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamedZone {
    /// Built-in DST rule.
    Builtin(&'static str, BuiltinDstRule),
    /// Parsed system TZif transitions `(utc_transition_secs, offset_secs)` sorted ascending.
    #[cfg(unix)]
    Tzif(String, Vec<(i64, i32)>),
}

/// Daylight saving time rule for a built-in IANA timezone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinDstRule {
    /// No DST; fixed standard offset in seconds.
    Fixed(i32),
    /// US/Canada rule: 2nd Sunday in March 02:00 local -> 1st Sunday in November 02:00 local (+3600s).
    Us(i32),
    /// EU/UK rule: last Sunday in March 01:00 UTC -> last Sunday in October 01:00 UTC (+3600s).
    Eu(i32),
    /// Australia (NSW/VIC): 1st Sunday in October 02:00 local -> 1st Sunday in April 03:00 local (+3600s).
    AuSouth(i32),
    /// New Zealand: last Sunday in September 02:00 local -> 1st Sunday in April 03:00 local (+3600s).
    Nz(i32),
}

impl CronTimezone {
    /// Parse a timezone name or fixed offset string.
    ///
    /// Accepts `"UTC"`, `"GMT"`, `"Z"`, `"Etc/UTC"`, `"Etc/GMT"`, fixed offsets
    /// (`+HH:MM`, `-HH:MM`, `+HHMM`, `-HHMM`, `+HH`, `-HH`), `Etc/GMT±N`,
    /// common IANA timezone names (with DST), and on Unix any zone present in
    /// `/usr/share/zoneinfo`.
    ///
    /// # Errors
    ///
    /// Returns an error naming the invalid timezone when unrecognized.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let trimmed = spec.trim();
        if trimmed.is_empty() {
            return Err("timezone cannot be empty".to_string());
        }
        if matches!(
            trimmed.to_ascii_uppercase().as_str(),
            "UTC" | "GMT" | "Z" | "ETC/UTC" | "ETC/GMT" | "ETC/UCT"
        ) {
            return Ok(Self::Utc);
        }
        if let Some(offset_secs) = parse_fixed_offset(trimmed) {
            return Ok(Self::FixedOffset(offset_secs));
        }
        if let Some((canonical, rule)) = lookup_builtin_iana(trimmed) {
            return Ok(Self::Named(NamedZone::Builtin(canonical, rule)));
        }
        #[cfg(unix)]
        if let Some(transitions) = load_system_tzif(trimmed) {
            return Ok(Self::Named(NamedZone::Tzif(
                trimmed.to_string(),
                transitions,
            )));
        }
        Err(format!("unknown or unsupported timezone '{trimmed}'"))
    }

    /// Resolve the UTC offset in seconds at the given UTC unix timestamp.
    pub fn offset_at(&self, utc_secs: i64) -> i32 {
        match self {
            Self::Utc => 0,
            Self::FixedOffset(secs) => *secs,
            Self::Named(NamedZone::Builtin(_, rule)) => rule.offset_at(utc_secs),
            #[cfg(unix)]
            Self::Named(NamedZone::Tzif(_, transitions)) => tzif_offset_at(transitions, utc_secs),
        }
    }

    /// Convert a UTC unix timestamp to local `(year, month, day, hour, minute, second, weekday_sun0)`.
    fn to_local_parts(&self, utc_secs: i64) -> Option<(i32, u8, u8, u8, u8, u8, u8)> {
        let offset = self.offset_at(utc_secs);
        let fixed = FixedOffset::east_opt(offset)?;
        let dt_utc = DateTime::<Utc>::from_timestamp(utc_secs, 0)?;
        let local = dt_utc.with_timezone(&fixed);
        let dow = match local.weekday() {
            Weekday::Sun => 0,
            Weekday::Mon => 1,
            Weekday::Tue => 2,
            Weekday::Wed => 3,
            Weekday::Thu => 4,
            Weekday::Fri => 5,
            Weekday::Sat => 6,
        };
        Some((
            local.year(),
            local.month() as u8,
            local.day() as u8,
            local.hour() as u8,
            local.minute() as u8,
            local.second() as u8,
            dow,
        ))
    }
}

impl BuiltinDstRule {
    fn offset_at(self, utc_secs: i64) -> i32 {
        match self {
            Self::Fixed(std_off) => std_off,
            Self::Us(std_off) => {
                let approx_local =
                    DateTime::<Utc>::from_timestamp(utc_secs + i64::from(std_off), 0)
                        .map(|dt| dt.year())
                        .unwrap_or(2026);
                let start_utc = nth_weekday_of_month(approx_local, 3, Weekday::Sun, 2)
                    .and_then(|d| d.and_hms_opt(2, 0, 0))
                    .map(|ndt| ndt.and_utc().timestamp() - i64::from(std_off))
                    .unwrap_or(i64::MAX);
                let end_utc = nth_weekday_of_month(approx_local, 11, Weekday::Sun, 1)
                    .and_then(|d| d.and_hms_opt(2, 0, 0))
                    .map(|ndt| ndt.and_utc().timestamp() - i64::from(std_off + 3600))
                    .unwrap_or(i64::MIN);
                if utc_secs >= start_utc && utc_secs < end_utc {
                    std_off + 3600
                } else {
                    std_off
                }
            }
            Self::Eu(std_off) => {
                let year = DateTime::<Utc>::from_timestamp(utc_secs, 0)
                    .map(|dt| dt.year())
                    .unwrap_or(2026);
                let start_utc = last_weekday_of_month(year, 3, Weekday::Sun)
                    .and_then(|d| d.and_hms_opt(1, 0, 0))
                    .map(|ndt| ndt.and_utc().timestamp())
                    .unwrap_or(i64::MAX);
                let end_utc = last_weekday_of_month(year, 10, Weekday::Sun)
                    .and_then(|d| d.and_hms_opt(1, 0, 0))
                    .map(|ndt| ndt.and_utc().timestamp())
                    .unwrap_or(i64::MIN);
                if utc_secs >= start_utc && utc_secs < end_utc {
                    std_off + 3600
                } else {
                    std_off
                }
            }
            Self::AuSouth(std_off) => {
                let year = DateTime::<Utc>::from_timestamp(utc_secs + i64::from(std_off), 0)
                    .map(|dt| dt.year())
                    .unwrap_or(2026);
                let end_utc = nth_weekday_of_month(year, 4, Weekday::Sun, 1)
                    .and_then(|d| d.and_hms_opt(3, 0, 0))
                    .map(|ndt| ndt.and_utc().timestamp() - i64::from(std_off + 3600))
                    .unwrap_or(i64::MIN);
                let start_utc = nth_weekday_of_month(year, 10, Weekday::Sun, 1)
                    .and_then(|d| d.and_hms_opt(2, 0, 0))
                    .map(|ndt| ndt.and_utc().timestamp() - i64::from(std_off))
                    .unwrap_or(i64::MAX);
                if utc_secs < end_utc || utc_secs >= start_utc {
                    std_off + 3600
                } else {
                    std_off
                }
            }
            Self::Nz(std_off) => {
                let year = DateTime::<Utc>::from_timestamp(utc_secs + i64::from(std_off), 0)
                    .map(|dt| dt.year())
                    .unwrap_or(2026);
                let end_utc = nth_weekday_of_month(year, 4, Weekday::Sun, 1)
                    .and_then(|d| d.and_hms_opt(3, 0, 0))
                    .map(|ndt| ndt.and_utc().timestamp() - i64::from(std_off + 3600))
                    .unwrap_or(i64::MIN);
                let start_utc = last_weekday_of_month(year, 9, Weekday::Sun)
                    .and_then(|d| d.and_hms_opt(2, 0, 0))
                    .map(|ndt| ndt.and_utc().timestamp() - i64::from(std_off))
                    .unwrap_or(i64::MAX);
                if utc_secs < end_utc || utc_secs >= start_utc {
                    std_off + 3600
                } else {
                    std_off
                }
            }
        }
    }
}

fn nth_weekday_of_month(year: i32, month: u32, target: Weekday, nth: u32) -> Option<NaiveDate> {
    let first = NaiveDate::from_ymd_opt(year, month, 1)?;
    let diff = (target.num_days_from_sunday() + 7 - first.weekday().num_days_from_sunday()) % 7;
    let day = 1 + diff + (nth - 1) * 7;
    NaiveDate::from_ymd_opt(year, month, day)
}

fn last_weekday_of_month(year: i32, month: u32, target: Weekday) -> Option<NaiveDate> {
    let (ny, nm) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let last = NaiveDate::from_ymd_opt(ny, nm, 1)?.pred_opt()?;
    let diff = (last.weekday().num_days_from_sunday() + 7 - target.num_days_from_sunday()) % 7;
    NaiveDate::from_ymd_opt(year, month, last.day() - diff)
}

fn parse_fixed_offset(spec: &str) -> Option<i32> {
    if let Some(rest) = spec
        .strip_prefix("Etc/GMT")
        .or_else(|| spec.strip_prefix("etc/gmt"))
        && let Some(sign_ch) = rest.chars().next()
        && let Ok(hours) = rest[1..].parse::<i32>()
        && (0..=14).contains(&hours)
    {
        // POSIX Etc/GMT+N is west of UTC (negative east offset).
        return match sign_ch {
            '+' => Some(-hours * 3600),
            '-' => Some(hours * 3600),
            _ => None,
        };
    }

    let (sign, rest) = if let Some(r) = spec.strip_prefix('+') {
        (1i32, r)
    } else {
        let r = spec.strip_prefix('-')?;
        (-1i32, r)
    };

    let (hours, mins) = if let Some((h, m)) = rest.split_once(':') {
        if h.len() > 2 || m.len() != 2 {
            return None;
        }
        (h.parse::<i32>().ok()?, m.parse::<i32>().ok()?)
    } else if rest.len() == 4 && rest.bytes().all(|b| b.is_ascii_digit()) {
        (
            rest[..2].parse::<i32>().ok()?,
            rest[2..].parse::<i32>().ok()?,
        )
    } else if (1..=2).contains(&rest.len()) && rest.bytes().all(|b| b.is_ascii_digit()) {
        (rest.parse::<i32>().ok()?, 0)
    } else {
        return None;
    };

    if !(0..=23).contains(&hours) || !(0..=59).contains(&mins) {
        return None;
    }
    let total = sign * (hours * 3600 + mins * 60);
    if total.abs() > 86_399 {
        return None;
    }
    Some(total)
}

fn lookup_builtin_iana(name: &str) -> Option<(&'static str, BuiltinDstRule)> {
    const H: i32 = 3600;
    let entry = match name {
        "America/New_York" | "US/Eastern" => ("America/New_York", BuiltinDstRule::Us(-5 * H)),
        "America/Toronto" => ("America/Toronto", BuiltinDstRule::Us(-5 * H)),
        "America/Chicago" | "US/Central" => ("America/Chicago", BuiltinDstRule::Us(-6 * H)),
        "America/Denver" | "US/Mountain" => ("America/Denver", BuiltinDstRule::Us(-7 * H)),
        "America/Phoenix" | "US/Arizona" => ("America/Phoenix", BuiltinDstRule::Fixed(-7 * H)),
        "America/Los_Angeles" | "US/Pacific" => ("America/Los_Angeles", BuiltinDstRule::Us(-8 * H)),
        "America/Vancouver" => ("America/Vancouver", BuiltinDstRule::Us(-8 * H)),
        "America/Anchorage" | "US/Alaska" => ("America/Anchorage", BuiltinDstRule::Us(-9 * H)),
        "Pacific/Honolulu" | "US/Hawaii" => ("Pacific/Honolulu", BuiltinDstRule::Fixed(-10 * H)),
        "America/Sao_Paulo" => ("America/Sao_Paulo", BuiltinDstRule::Fixed(-3 * H)),
        "America/Argentina/Buenos_Aires" => (
            "America/Argentina/Buenos_Aires",
            BuiltinDstRule::Fixed(-3 * H),
        ),
        "America/Mexico_City" => ("America/Mexico_City", BuiltinDstRule::Fixed(-6 * H)),
        "Europe/London" | "GB" => ("Europe/London", BuiltinDstRule::Eu(0)),
        "Europe/Dublin" => ("Europe/Dublin", BuiltinDstRule::Eu(0)),
        "Europe/Lisbon" => ("Europe/Lisbon", BuiltinDstRule::Eu(0)),
        "Europe/Paris" => ("Europe/Paris", BuiltinDstRule::Eu(H)),
        "Europe/Berlin" => ("Europe/Berlin", BuiltinDstRule::Eu(H)),
        "Europe/Madrid" => ("Europe/Madrid", BuiltinDstRule::Eu(H)),
        "Europe/Rome" => ("Europe/Rome", BuiltinDstRule::Eu(H)),
        "Europe/Amsterdam" => ("Europe/Amsterdam", BuiltinDstRule::Eu(H)),
        "Europe/Brussels" => ("Europe/Brussels", BuiltinDstRule::Eu(H)),
        "Europe/Zurich" => ("Europe/Zurich", BuiltinDstRule::Eu(H)),
        "Europe/Vienna" => ("Europe/Vienna", BuiltinDstRule::Eu(H)),
        "Europe/Stockholm" => ("Europe/Stockholm", BuiltinDstRule::Eu(H)),
        "Europe/Warsaw" => ("Europe/Warsaw", BuiltinDstRule::Eu(H)),
        "Europe/Athens" => ("Europe/Athens", BuiltinDstRule::Eu(2 * H)),
        "Europe/Helsinki" => ("Europe/Helsinki", BuiltinDstRule::Eu(2 * H)),
        "Europe/Istanbul" => ("Europe/Istanbul", BuiltinDstRule::Fixed(3 * H)),
        "Europe/Moscow" => ("Europe/Moscow", BuiltinDstRule::Fixed(3 * H)),
        "Africa/Cairo" => ("Africa/Cairo", BuiltinDstRule::Fixed(2 * H)),
        "Africa/Johannesburg" => ("Africa/Johannesburg", BuiltinDstRule::Fixed(2 * H)),
        "Africa/Lagos" => ("Africa/Lagos", BuiltinDstRule::Fixed(H)),
        "Asia/Dubai" => ("Asia/Dubai", BuiltinDstRule::Fixed(4 * H)),
        "Asia/Kolkata" | "Asia/Calcutta" => ("Asia/Kolkata", BuiltinDstRule::Fixed(5 * H + 1800)),
        "Asia/Bangkok" => ("Asia/Bangkok", BuiltinDstRule::Fixed(7 * H)),
        "Asia/Jakarta" => ("Asia/Jakarta", BuiltinDstRule::Fixed(7 * H)),
        "Asia/Shanghai" | "Asia/Hong_Kong" | "Asia/Taipei" => {
            ("Asia/Shanghai", BuiltinDstRule::Fixed(8 * H))
        }
        "Asia/Singapore" | "Asia/Kuala_Lumpur" | "Asia/Manila" => {
            ("Asia/Singapore", BuiltinDstRule::Fixed(8 * H))
        }
        "Asia/Tokyo" => ("Asia/Tokyo", BuiltinDstRule::Fixed(9 * H)),
        "Asia/Seoul" => ("Asia/Seoul", BuiltinDstRule::Fixed(9 * H)),
        "Australia/Perth" => ("Australia/Perth", BuiltinDstRule::Fixed(8 * H)),
        "Australia/Brisbane" => ("Australia/Brisbane", BuiltinDstRule::Fixed(10 * H)),
        "Australia/Sydney" => ("Australia/Sydney", BuiltinDstRule::AuSouth(10 * H)),
        "Australia/Melbourne" => ("Australia/Melbourne", BuiltinDstRule::AuSouth(10 * H)),
        "Pacific/Auckland" => ("Pacific/Auckland", BuiltinDstRule::Nz(12 * H)),
        _ => return None,
    };
    Some(entry)
}

#[cfg(unix)]
fn load_system_tzif(name: &str) -> Option<Vec<(i64, i32)>> {
    if name.contains("..") || name.starts_with('/') {
        return None;
    }
    let path = std::path::Path::new("/usr/share/zoneinfo").join(name);
    let bytes = std::fs::read(path).ok()?;
    parse_tzif(&bytes)
}

#[cfg(unix)]
fn parse_tzif(bytes: &[u8]) -> Option<Vec<(i64, i32)>> {
    if bytes.len() < 44 || &bytes[0..4] != b"TZif" {
        return None;
    }
    let version = bytes[4];
    let read_u32 = |off: usize| -> Option<usize> {
        let slice: [u8; 4] = bytes.get(off..off + 4)?.try_into().ok()?;
        Some(u32::from_be_bytes(slice) as usize)
    };
    let v1_ttisutcnt = read_u32(20)?;
    let v1_ttisstdcnt = read_u32(24)?;
    let v1_leapcnt = read_u32(28)?;
    let v1_timecnt = read_u32(32)?;
    let v1_typecnt = read_u32(36)?;
    let v1_charcnt = read_u32(40)?;

    let v1_data_len = v1_timecnt * 5
        + v1_typecnt * 6
        + v1_charcnt
        + v1_leapcnt * 8
        + v1_ttisstdcnt
        + v1_ttisutcnt;

    let (hdr_off, is_64) = if version >= b'2' {
        let v2_off = 44 + v1_data_len;
        if bytes.get(v2_off..v2_off + 4) == Some(b"TZif") {
            (v2_off, true)
        } else {
            (0, false)
        }
    } else {
        (0, false)
    };

    let read_hdr_u32 = |rel: usize| -> Option<usize> {
        let slice: [u8; 4] = bytes
            .get(hdr_off + rel..hdr_off + rel + 4)?
            .try_into()
            .ok()?;
        Some(u32::from_be_bytes(slice) as usize)
    };
    let timecnt = read_hdr_u32(32)?;
    let typecnt = read_hdr_u32(36)?;
    let time_size = if is_64 { 8 } else { 4 };
    let times_off = hdr_off + 44;
    let indices_off = times_off + timecnt * time_size;
    let types_off = indices_off + timecnt;

    if typecnt == 0 || types_off + typecnt * 6 > bytes.len() {
        return None;
    }

    let mut type_offsets = Vec::with_capacity(typecnt);
    for t in 0..typecnt {
        let off_bytes: [u8; 4] = bytes[types_off + t * 6..types_off + t * 6 + 4]
            .try_into()
            .ok()?;
        type_offsets.push(i32::from_be_bytes(off_bytes));
    }

    let mut out = Vec::with_capacity(timecnt + 1);
    // Initial offset before the first transition: first non-DST type or type 0.
    out.push((i64::MIN, type_offsets[0]));
    for i in 0..timecnt {
        let t_off = times_off + i * time_size;
        let ts = if is_64 {
            let b: [u8; 8] = bytes[t_off..t_off + 8].try_into().ok()?;
            i64::from_be_bytes(b)
        } else {
            let b: [u8; 4] = bytes[t_off..t_off + 4].try_into().ok()?;
            i64::from(i32::from_be_bytes(b))
        };
        let type_idx = *bytes.get(indices_off + i)? as usize;
        let offset = *type_offsets.get(type_idx)?;
        out.push((ts, offset));
    }
    Some(out)
}

#[cfg(unix)]
fn tzif_offset_at(transitions: &[(i64, i32)], utc_secs: i64) -> i32 {
    let idx = transitions.partition_point(|&(t, _)| t <= utc_secs);
    if idx == 0 {
        transitions.first().map_or(0, |&(_, off)| off)
    } else {
        transitions[idx - 1].1
    }
}

/// Parsed cron schedule paired with its timezone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronSchedule {
    seconds: FieldSet,
    minutes: FieldSet,
    hours: FieldSet,
    dom: FieldSet,
    months: FieldSet,
    dow: FieldSet,
    timezone: CronTimezone,
}

impl CronSchedule {
    /// Parse a cron expression (`expr`) and optional timezone (`timezone`, defaults to UTC).
    ///
    /// Accepts 5-field (`min hour dom month dow`) and 6-field
    /// (`sec min hour dom month dow`) expressions as well as `@yearly`,
    /// `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, and `@hourly`.
    ///
    /// # Errors
    ///
    /// Returns a descriptive error string when the expression or timezone is invalid.
    pub fn parse(expr: &str, timezone: Option<&str>) -> Result<Self, String> {
        let tz = match timezone {
            Some(name) => CronTimezone::parse(name)?,
            None => CronTimezone::Utc,
        };
        let expanded = expand_macro(expr.trim());
        let fields: Vec<&str> = expanded.split_whitespace().collect();
        let (sec_str, min_str, hour_str, dom_str, month_str, dow_str) = match fields.as_slice() {
            [min, hour, dom, month, dow] => ("0", *min, *hour, *dom, *month, *dow),
            [sec, min, hour, dom, month, dow] => (*sec, *min, *hour, *dom, *month, *dow),
            _ => {
                return Err(format!(
                    "cron expression '{expr}' must have 5 or 6 whitespace-separated fields (got {})",
                    fields.len()
                ));
            }
        };

        let seconds = parse_field(sec_str, 0, 59, None).map_err(|e| format!("seconds: {e}"))?;
        let minutes = parse_field(min_str, 0, 59, None).map_err(|e| format!("minutes: {e}"))?;
        let hours = parse_field(hour_str, 0, 23, None).map_err(|e| format!("hours: {e}"))?;
        let dom = parse_field(dom_str, 1, 31, None).map_err(|e| format!("day-of-month: {e}"))?;
        let months = parse_field(month_str, 1, 12, Some(parse_month_name))
            .map_err(|e| format!("month: {e}"))?;
        let mut dow = parse_field(dow_str, 0, 7, Some(parse_dow_name))
            .map_err(|e| format!("day-of-week: {e}"))?;
        // Normalize Sunday: both 0 and 7 mean Sunday (0).
        if dow.contains(7) {
            dow.bits |= 1u64 << 0;
            dow.bits &= !(1u64 << 7);
        }

        // Verify that the DOM/month combination can exist (e.g. Feb 30 is impossible).
        if !dom.wildcard && !has_possible_month_day(&months, &dom) {
            return Err(format!(
                "cron expression '{expr}' matches no calendar date (impossible day-of-month for selected months)"
            ));
        }

        Ok(Self {
            seconds,
            minutes,
            hours,
            dom,
            months,
            dow,
            timezone: tz,
        })
    }

    /// Return the parsed timezone.
    pub fn timezone(&self) -> &CronTimezone {
        &self.timezone
    }

    /// Whether the calendar date matches the month, DOM, and DOW constraints.
    fn matches_date(&self, month: u8, day: u8, dow: u8) -> bool {
        if !self.months.contains(month) {
            return false;
        }
        let dom_match = self.dom.contains(day);
        let dow_match = self.dow.contains(dow);
        if !self.dom.wildcard && !self.dow.wildcard {
            dom_match || dow_match
        } else {
            dom_match && dow_match
        }
    }

    /// Compute the next fire timestamp in Unix milliseconds strictly after `after_unix_ms`.
    pub fn next_after_ms(&self, after_unix_ms: u64) -> Option<u64> {
        let mut utc_secs = i64::try_from(after_unix_ms / 1000).ok()? + 1;
        let limit = utc_secs.saturating_add(MAX_SEARCH_SECONDS);

        while utc_secs <= limit {
            let (_year, month, day, hour, minute, second, dow) =
                self.timezone.to_local_parts(utc_secs)?;

            if !self.matches_date(month, day, dow) {
                // Advance safely to 21:00:00 (or past the current hour if >= 21) without
                // overshooting a 23-hour DST spring-forward day.
                let jump = if hour < 21 {
                    (i64::from(21 - hour) * 3600 - i64::from(minute) * 60 - i64::from(second))
                        .max(1)
                } else {
                    (3600 - (i64::from(minute) * 60 + i64::from(second))).max(1)
                };
                utc_secs += jump;
                continue;
            }

            if !self.hours.contains(hour) {
                let jump = (i64::from(59 - minute) * 60 + i64::from(60 - second)).max(1);
                utc_secs += jump;
                continue;
            }

            if !self.minutes.contains(minute) {
                let jump = i64::from(60 - second).max(1);
                utc_secs += jump;
                continue;
            }

            if !self.seconds.contains(second) {
                utc_secs += 1;
                continue;
            }

            return u64::try_from(utc_secs).ok().map(|s| s * 1000);
        }

        None
    }

    /// Compute the delay from `now` until the next fire time.
    pub fn delay_from(&self, now: SystemTime) -> Option<Duration> {
        let now_ms = now
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let next_ms = self.next_after_ms(now_ms)?;
        Some(Duration::from_millis(next_ms.saturating_sub(now_ms)))
    }
}

fn expand_macro(expr: &str) -> &str {
    match expr.to_ascii_lowercase().as_str() {
        "@yearly" | "@annually" => "0 0 0 1 1 *",
        "@monthly" => "0 0 0 1 * *",
        "@weekly" => "0 0 0 * * 0",
        "@daily" | "@midnight" => "0 0 0 * * *",
        "@hourly" => "0 0 * * * *",
        _ => expr,
    }
}

fn has_possible_month_day(months: &FieldSet, dom: &FieldSet) -> bool {
    const MAX_DAYS: [u8; 13] = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for m in 1..=12u8 {
        if !months.contains(m) {
            continue;
        }
        for d in 1..=MAX_DAYS[m as usize] {
            if dom.contains(d) {
                return true;
            }
        }
    }
    false
}

fn parse_field(
    raw: &str,
    min: u8,
    max: u8,
    name_fn: Option<fn(&str) -> Option<u8>>,
) -> Result<FieldSet, String> {
    if raw.is_empty() {
        return Err("empty field".to_string());
    }
    let wildcard = raw == "*";
    let mut bits = 0u64;

    for part in raw.split(',') {
        if part.is_empty() {
            return Err(format!("empty list item in '{raw}'"));
        }
        let (range_part, step) = match part.split_once('/') {
            Some((lhs, rhs)) => {
                let s = rhs
                    .parse::<u8>()
                    .map_err(|_| format!("invalid step '{rhs}'"))?;
                if s == 0 {
                    return Err("step must be at least 1".to_string());
                }
                (lhs, s)
            }
            None => (part, 1),
        };

        let (start, end) = if range_part == "*" {
            (min, max)
        } else if let Some((a, b)) = range_part.split_once('-') {
            let start = parse_atom(a, min, max, name_fn)?;
            let end = parse_atom(b, min, max, name_fn)?;
            if start > end {
                return Err(format!("range start {start} exceeds end {end}"));
            }
            (start, end)
        } else {
            let val = parse_atom(range_part, min, max, name_fn)?;
            if step == 1 { (val, val) } else { (val, max) }
        };

        let mut v = start;
        while v <= end {
            bits |= 1u64 << v;
            match v.checked_add(step) {
                Some(next) => v = next,
                None => break,
            }
        }
    }

    if bits == 0 {
        return Err(format!("field '{raw}' matched no values"));
    }

    Ok(FieldSet { bits, wildcard })
}

fn parse_atom(
    raw: &str,
    min: u8,
    max: u8,
    name_fn: Option<fn(&str) -> Option<u8>>,
) -> Result<u8, String> {
    let val = if let Some(named) = name_fn.and_then(|f| f(raw)) {
        named
    } else {
        raw.parse::<u8>()
            .map_err(|_| format!("invalid value '{raw}'"))?
    };
    if val < min || val > max {
        return Err(format!("value {val} out of range {min}..={max}"));
    }
    Ok(val)
}

fn parse_month_name(s: &str) -> Option<u8> {
    match s.to_ascii_uppercase().as_str() {
        "JAN" => Some(1),
        "FEB" => Some(2),
        "MAR" => Some(3),
        "APR" => Some(4),
        "MAY" => Some(5),
        "JUN" => Some(6),
        "JUL" => Some(7),
        "AUG" => Some(8),
        "SEP" => Some(9),
        "OCT" => Some(10),
        "NOV" => Some(11),
        "DEC" => Some(12),
        _ => None,
    }
}

fn parse_dow_name(s: &str) -> Option<u8> {
    match s.to_ascii_uppercase().as_str() {
        "SUN" => Some(0),
        "MON" => Some(1),
        "TUE" => Some(2),
        "WED" => Some(3),
        "THU" => Some(4),
        "FRI" => Some(5),
        "SAT" => Some(6),
        _ => None,
    }
}

/// Parse an arbitrary ISO-8601/RFC3339 or unix-ms timestamp string to Unix milliseconds.
#[allow(dead_code)]
pub(crate) fn parse_timestamp_utc_ms(raw: &str, end_of_day_for_date: bool) -> Result<i64, String> {
    let trimmed = raw.trim();
    if trimmed.len() == 8
        && trimmed.bytes().all(|b| b.is_ascii_digit())
        && let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y%m%d")
        && (1970..=2199).contains(&date.year())
    {
        let ndt = if end_of_day_for_date {
            date.and_hms_milli_opt(23, 59, 59, 999)
        } else {
            date.and_hms_milli_opt(0, 0, 0, 0)
        }
        .ok_or_else(|| format!("invalid date '{trimmed}'"))?;
        return Ok(ndt.and_utc().timestamp_millis());
    }
    if let Ok(ms) = trimmed.parse::<i64>() {
        return Ok(ms);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(dt.timestamp_millis());
    }
    if let Ok(ndt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S"))
    {
        return Ok(ndt.and_utc().timestamp_millis());
    }
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        let ndt = if end_of_day_for_date {
            date.and_hms_milli_opt(23, 59, 59, 999)
        } else {
            date.and_hms_milli_opt(0, 0, 0, 0)
        }
        .ok_or_else(|| format!("invalid date '{trimmed}'"))?;
        return Ok(ndt.and_utc().timestamp_millis());
    }
    Err(format!(
        "invalid timestamp '{trimmed}': expected RFC3339, YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, or unix_ms integer"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc_ms(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> u64 {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(hh, mm, ss)
            .unwrap()
            .and_utc()
            .timestamp_millis() as u64
    }

    #[test]
    fn parses_five_and_six_field_expressions_and_macros() {
        let every_15 = CronSchedule::parse("*/15 * * * *", None).expect("5-field");
        let after = utc_ms(2026, 5, 1, 10, 14, 0);
        assert_eq!(
            every_15.next_after_ms(after),
            Some(utc_ms(2026, 5, 1, 10, 15, 0))
        );

        let six_field = CronSchedule::parse("30 0 12 * * MON-FRI", None).expect("6-field");
        // 2026-05-02 is Saturday -> next weekday is Monday 2026-05-04 12:00:30 UTC.
        let sat = utc_ms(2026, 5, 2, 10, 0, 0);
        assert_eq!(
            six_field.next_after_ms(sat),
            Some(utc_ms(2026, 5, 4, 12, 0, 30))
        );

        let hourly = CronSchedule::parse("@hourly", None).expect("@hourly");
        assert_eq!(
            hourly.next_after_ms(utc_ms(2026, 5, 1, 10, 59, 59)),
            Some(utc_ms(2026, 5, 1, 11, 0, 0))
        );
    }

    #[test]
    fn timezone_and_dst_transitions_are_respected() {
        // America/New_York on 2026-03-08 (US spring forward: 02:00 EST -> 03:00 EDT).
        // 09:00 local in EST (2026-03-07) is 14:00 UTC.
        // 09:00 local in EDT (2026-03-08) is 13:00 UTC.
        let sched = CronSchedule::parse("0 9 * * *", Some("America/New_York")).expect("ny tz");
        assert_eq!(
            sched.next_after_ms(utc_ms(2026, 3, 7, 13, 0, 0)),
            Some(utc_ms(2026, 3, 7, 14, 0, 0))
        );
        assert_eq!(
            sched.next_after_ms(utc_ms(2026, 3, 7, 14, 0, 0)),
            Some(utc_ms(2026, 3, 8, 13, 0, 0))
        );

        let fixed = CronSchedule::parse("30 14 * * *", Some("+05:30")).expect("fixed offset");
        // 14:30 +05:30 is 09:00 UTC.
        assert_eq!(
            fixed.next_after_ms(utc_ms(2026, 5, 1, 8, 59, 0)),
            Some(utc_ms(2026, 5, 1, 9, 0, 0))
        );
    }

    #[test]
    fn vixie_dom_dow_union_semantics() {
        // Both DOM (15) and DOW (FRI) constrained -> matches on 15th OR any Friday.
        let sched = CronSchedule::parse("0 0 15 * FRI", None).expect("dom+dow");
        // 2026-05-01 is Friday.
        assert_eq!(
            sched.next_after_ms(utc_ms(2026, 4, 30, 0, 0, 0)),
            Some(utc_ms(2026, 5, 1, 0, 0, 0))
        );
        // 2026-05-15 is also Friday; 2026-05-16 -> next Friday is 2026-05-22.
        assert_eq!(
            sched.next_after_ms(utc_ms(2026, 5, 15, 0, 0, 0)),
            Some(utc_ms(2026, 5, 22, 0, 0, 0))
        );
    }

    #[test]
    fn rejects_invalid_expressions_and_timezones() {
        assert!(CronSchedule::parse("* * *", None).is_err());
        assert!(CronSchedule::parse("60 * * * *", None).is_err());
        assert!(CronSchedule::parse("0 0 30 2 *", None).is_err());
        assert!(CronSchedule::parse("* * * * *", Some("Mars/Olympus_Mons")).is_err());
    }

    #[test]
    fn dst_spring_forward_skips_nonexistent_hour_and_compact_date_parses_cleanly() {
        // 02:30 AM does not exist in America/New_York on 2026-03-08 (clocks jump 02:00 -> 03:00).
        // After 2026-03-07 02:30 EST (07:30 UTC), next fire must skip 2026-03-08 and land on 2026-03-09 02:30 EDT (06:30 UTC).
        let sched = CronSchedule::parse("30 2 * * *", Some("America/New_York")).expect("ny tz");
        assert_eq!(
            sched.next_after_ms(utc_ms(2026, 3, 7, 7, 30, 0)),
            Some(utc_ms(2026, 3, 9, 6, 30, 0))
        );

        // Fall-back on 2026-11-01 (01:00..02:00 EDT repeats as 01:00..02:00 EST); 02:30 local fires 2026-11-01 07:30 UTC.
        assert_eq!(
            sched.next_after_ms(utc_ms(2026, 10, 31, 6, 30, 0)),
            Some(utc_ms(2026, 11, 1, 7, 30, 0))
        );

        // Compact 8-digit YYYYMMDD parses as calendar date rather than 1970 unix_ms.
        assert_eq!(
            parse_timestamp_utc_ms("20260105", false).unwrap(),
            utc_ms(2026, 1, 5, 0, 0, 0) as i64
        );
        assert_eq!(
            parse_timestamp_utc_ms("20260105", true).unwrap(),
            utc_ms(2026, 1, 5, 23, 59, 59) as i64 + 999
        );
        assert_eq!(
            parse_timestamp_utc_ms("1700000000000", false).unwrap(),
            1_700_000_000_000
        );
    }
}
