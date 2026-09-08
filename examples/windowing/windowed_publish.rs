//! Publish synthetic sales onto the windowing examples' two NATS subjects.
//!
//! One NDJSON object per message, alternating between `windowing.sales.a`
//! and `windowing.sales.b` so both sources of whichever mode's config is
//! running (`examples/windowing/{tumbling/tumbling,sliding/sliding,
//! session/session}.kdl`) stay busy. Every message carries the three `Sale`
//! fields the processors read: `timestamp_ms` (simulated event time,
//! advancing by `--ts-step-ms` per message), `symbol` (drawn from a small
//! set) and `amount` (uniform in a range). Because the simulated clock runs
//! `rate * ts-step-ms` milliseconds per wall second, the tumbling config's
//! 30-second windows and the sliding config's 60-second ones close
//! continuously while the publisher runs.
//!
//! ```text
//! cargo run -p saci-service --features connector-nats --example windowed_publish -- --rate 20 --ts-step-ms 2000
//! ```
//!
//! A session window closes on silence in one key's own event time. A steady
//! clock leaves such a hole only where the random symbol draw happens to skip
//! a symbol for long enough, so nothing bounds a session's span.
//! `--gap-every N` adds `--gap-ms M` to the simulated clock after every `N`
//! messages, so `--gap-every 20 --gap-ms 30000` publishes bursts of 20
//! messages 2000 ms apart separated by 30-second jumps: every symbol's
//! session ends at every burst boundary. The timestamp stays a pure function
//! of the message id, so a re-run still reproduces the same sequence.
//!
//! Flags: `--count` (default 0, runs until Ctrl-C), `--rate` messages per
//! second (default 20), `--ts-step-ms` simulated milliseconds per message
//! (default 2000), `--gap-every` messages per burst (default 0, no gaps),
//! `--gap-ms` simulated milliseconds each gap adds (default 0), `--url`
//! (default `nats://localhost:4222`), `--subject-a`
//! (default `windowing.sales.a`), `--subject-b` (default `windowing.sales.b`),
//! `--seed` (default a fixed constant, so a re-run reproduces the same
//! sequence).
//!
//! The clock starts at [`BASE_TS_MS`] on every run, so window ids repeat and
//! the sinks' upserts make a re-run idempotent. Restarting the publisher
//! against a still-running service therefore rewinds event time by the whole
//! previous run: a windowed node's watermark does not rewind with it, so it
//! drops every arrival as beyond its lateness budget and its sink receives
//! nothing until the new run's clock climbs back. The service says so on the
//! `saci::windowing` target and counts it in
//! `saci_window_late_arrivals_total`. Restart the service with the publisher
//! to avoid it.

use std::time::{Duration, Instant};

// `random_range` lives on `RngExt` in rand 0.10, not on `Rng`.
use rand::{RngExt as _, SeedableRng as _};
use tokio_util::sync::CancellationToken;

/// Simulated epoch the demo's clock starts at, milliseconds.
const BASE_TS_MS: i64 = 1_700_000_000_000;

/// The symbols sales are drawn from; the windowing key.
const SYMBOLS: [&str; 3] = ["AAPL", "GOOG", "MSFT"];

struct Args {
    count: u64,
    rate: u64,
    ts_step_ms: i64,
    gap_every: u64,
    gap_ms: i64,
    url: String,
    subject_a: String,
    subject_b: String,
    seed: u64,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            count: 0,
            rate: 20,
            ts_step_ms: 2_000,
            gap_every: 0,
            gap_ms: 0,
            url: "nats://localhost:4222".to_string(),
            subject_a: "windowing.sales.a".to_string(),
            subject_b: "windowing.sales.b".to_string(),
            seed: 0x5eed_1234_abcd_0003,
        }
    }
}

/// Parse `--key value` pairs, hand-rolled like the branching publisher.
fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--count" => args.count = value()?.parse().map_err(|e| format!("--count: {e}"))?,
            "--rate" => args.rate = value()?.parse().map_err(|e| format!("--rate: {e}"))?,
            "--ts-step-ms" => {
                args.ts_step_ms = value()?.parse().map_err(|e| format!("--ts-step-ms: {e}"))?;
            }
            "--gap-every" => {
                args.gap_every = value()?.parse().map_err(|e| format!("--gap-every: {e}"))?;
            }
            "--gap-ms" => {
                args.gap_ms = value()?.parse().map_err(|e| format!("--gap-ms: {e}"))?;
            }
            "--url" => args.url = value()?,
            "--subject-a" => args.subject_a = value()?,
            "--subject-b" => args.subject_b = value()?,
            "--seed" => args.seed = value()?.parse().map_err(|e| format!("--seed: {e}"))?,
            "--help" | "-h" => {
                println!(
                    "usage: windowed_publish [--count N (0 = continuous)] [--rate N] \
                     [--ts-step-ms N] [--gap-every N] [--gap-ms N] [--url URL] \
                     [--subject-a SUBJECT] [--subject-b SUBJECT] [--seed N]"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    if args.rate == 0 {
        return Err("--rate must be at least 1".to_string());
    }
    if args.ts_step_ms <= 0 {
        return Err("--ts-step-ms must be at least 1".to_string());
    }
    if args.gap_ms < 0 {
        return Err("--gap-ms must be zero or positive".to_string());
    }
    if args.gap_every > 0 && args.gap_ms == 0 {
        return Err("--gap-every needs --gap-ms".to_string());
    }
    Ok(args)
}

/// One sale as the NDJSON line the source decodes, field order matching the
/// config's `schema_fields`.
fn line(ts: i64, symbol: &str, amount: f64) -> String {
    format!(r#"{{"timestamp_ms":{ts},"symbol":"{symbol}","amount":{amount:.2}}}"#)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("error: {message}");
            std::process::exit(2);
        }
    };

    let cancel = CancellationToken::new();
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            cancel.cancel();
        });
    }

    let client = async_nats::connect(&args.url).await?;
    match args.count {
        0 => println!(
            "publishing to {} and {} on {} at {}/s until Ctrl-C ({} ms of simulated time per message)",
            args.subject_a, args.subject_b, args.url, args.rate, args.ts_step_ms
        ),
        count => println!(
            "publishing {count} sales to {} and {} on {} at {}/s",
            args.subject_a, args.subject_b, args.url, args.rate
        ),
    }

    let mut rng = rand::rngs::StdRng::seed_from_u64(args.seed);
    let spacing = Duration::from_secs_f64(1.0 / args.rate as f64);
    let started = Instant::now();

    let mut id: u64 = 1;
    let mut sent: u64 = 0;
    // The last timestamp published, so the closing summary reports the span
    // of simulated time actually covered: with gaps that is no longer
    // `sent * ts_step_ms`.
    let mut last_ts = BASE_TS_MS;
    loop {
        if args.count != 0 && id > args.count {
            break;
        }
        if cancel.is_cancelled() {
            break;
        }

        // One gap per completed burst of `gap_every` messages, so the
        // timestamp stays a pure function of `id` and a re-run reproduces
        // the same sequence. `gap_every` 0 is "no gaps", which is exactly the
        // division `checked_div` refuses.
        let gaps = (id - 1).checked_div(args.gap_every).unwrap_or(0);
        let ts = BASE_TS_MS
            + i64::try_from(id).unwrap_or(i64::MAX) * args.ts_step_ms
            + i64::try_from(gaps).unwrap_or(0) * args.gap_ms;
        let symbol = SYMBOLS[rng.random_range(0..SYMBOLS.len())];
        let amount = rng.random_range(1.0..100.0);
        // Alternate subjects so both sources stay equally busy.
        let subject = if id.is_multiple_of(2) {
            &args.subject_a
        } else {
            &args.subject_b
        };

        client
            .publish(subject.clone(), line(ts, symbol, amount).into())
            .await?;
        sent += 1;
        last_ts = ts;

        let deadline = started + spacing * u32::try_from(id).unwrap_or(u32::MAX);
        if let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
            tokio::select! {
                _ = tokio::time::sleep(remaining) => {}
                _ = cancel.cancelled() => break,
            }
        }

        id += 1;
    }

    client.flush().await?;

    let elapsed = started.elapsed();
    println!(
        "published {} sales in {:.2}s ({:.0}/s, {} simulated seconds)",
        sent,
        elapsed.as_secs_f64(),
        sent as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        (last_ts - BASE_TS_MS) / 1000,
    );
    Ok(())
}
