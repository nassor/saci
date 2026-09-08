//! Publish synthetic sales onto the multi-workflow example's two NATS
//! subjects.
//!
//! One NDJSON object per message, alternating between `multi.orders` (the
//! route workflow's ingest) and `multi.offsets` (the settle workflow's second
//! source). Every message carries the three `Sale` fields the processors
//! read: `timestamp_ms` (simulated event time, advancing by `--ts-step-ms`
//! per message), `symbol` (drawn from a small set) and `amount` (uniform in
//! 50.0..150.0). The router's 100.0 threshold sits inside that range, so both
//! branches fire: `rush` batches land straight in `rush_sales`, `standard`
//! batches bridge into the settle workflow. Because the simulated clock runs
//! `rate * ts-step-ms` milliseconds per wall second, the config's 30-second
//! tumbling windows close continuously while the publisher runs.
//!
//! ```text
//! cargo run -p saci-service --features connector-nats --example multi_workflow_publish -- --rate 20 --ts-step-ms 2000
//! ```
//!
//! Flags: `--count` (default 0, runs until Ctrl-C), `--rate` messages per
//! second (default 20), `--ts-step-ms` simulated milliseconds per message
//! (default 2000), `--url` (default `nats://localhost:4222`), `--subject-a`
//! (default `multi.orders`), `--subject-b` (default `multi.offsets`), `--seed`
//! (default a fixed constant, so a re-run reproduces the same sequence).
//!
//! The clock starts at [`BASE_TS_MS`] on every run, so window ids repeat and
//! the sinks' upserts make a re-run idempotent. Restarting the publisher
//! against a still-running service therefore rewinds event time by the whole
//! previous run: `window_sales`'s watermark does not rewind with it, so it
//! drops every arrival as beyond its lateness budget and `window_totals`
//! receives nothing until the new run's clock climbs back. The service says
//! so on the `saci::windowing` target and counts it in
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
            url: "nats://localhost:4222".to_string(),
            subject_a: "multi.orders".to_string(),
            subject_b: "multi.offsets".to_string(),
            seed: 0x5eed_1234_abcd_0004,
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
            "--url" => args.url = value()?,
            "--subject-a" => args.subject_a = value()?,
            "--subject-b" => args.subject_b = value()?,
            "--seed" => args.seed = value()?.parse().map_err(|e| format!("--seed: {e}"))?,
            "--help" | "-h" => {
                println!(
                    "usage: multi_workflow_publish [--count N (0 = continuous)] [--rate N] \
                     [--ts-step-ms N] [--url URL] [--subject-a SUBJECT] [--subject-b SUBJECT] \
                     [--seed N]"
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
    loop {
        if args.count != 0 && id > args.count {
            break;
        }
        if cancel.is_cancelled() {
            break;
        }

        let ts = BASE_TS_MS + i64::try_from(id).unwrap_or(i64::MAX) * args.ts_step_ms;
        let symbol = SYMBOLS[rng.random_range(0..SYMBOLS.len())];
        // The 100.0 threshold sits inside this range, so both router branches
        // fire as the stream runs.
        let amount = rng.random_range(50.0..150.0);
        // Alternate subjects so both workflows stay equally busy.
        let subject = if id.is_multiple_of(2) {
            &args.subject_a
        } else {
            &args.subject_b
        };

        client
            .publish(subject.clone(), line(ts, symbol, amount).into())
            .await?;
        sent += 1;

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
        sent * args.ts_step_ms as u64 / 1000,
    );
    Ok(())
}
