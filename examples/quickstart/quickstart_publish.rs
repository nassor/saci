//! Publish synthetic card authorisations onto the Quick Start's NATS subject.
//!
//! One NDJSON object per message on `authorizations.raw`, which is what
//! `examples/quickstart/quickstart.kdl`'s `NatsSource` decodes. Every message
//! carries all twelve `Order` fields with the eight derived ones zeroed, matching
//! the `Order` schema the stages declare: the two stages mutate the Arrow buffer
//! in place and cannot add a column, so a field that arrives missing can never
//! appear later.
//!
//! ```text
//! cargo run -p saci-service --features connector-nats --example quickstart_publish -- --count 5000 --rate 500
//! ```
//!
//! Flags: `--count` (default 5000, `0` runs until Ctrl-C), `--rate` messages
//! per second (default 500), `--url` (default `nats://localhost:4222`),
//! `--subject` (default `authorizations.raw`), `--seed` (default a fixed
//! constant, so a re-run reproduces the same rows and the same `settlements`
//! contents).

use std::time::{Duration, Instant};

// `random_range` lives on `RngExt` in rand 0.10, not on `Rng`.
use rand::{RngExt as _, SeedableRng as _};
use tokio_util::sync::CancellationToken;

/// Regions the generator draws from, matching the polyglot fixture's vocabulary.
const REGIONS: [&str; 4] = ["emea", "apac", "amer", "latam"];

/// Currencies the generator draws from.
const CURRENCIES: [&str; 3] = ["USD", "EUR", "GBP"];

/// Amounts are drawn from a three-part mixture rather than one uniform range,
/// so every branch of both processors fires at a plausible frequency instead of
/// only in principle:
///
/// | share | range | what it exercises |
/// |---|---|---|
/// | 2% | 0.01 to 0.49 | below `quickstart.kdl`'s `min_amount="0.50"`: the Go stage marks it invalid and the C# stage rejects it (`review_tier = 2`) |
/// | 78% | 1.00 to 250.00 | ordinary spend: settles (`review_tier = 0`) |
/// | 20% | 250.00 to 5000.00 | partly above `quickstart.kdl`'s `hold_above="1000"`: held for review (`review_tier = 1`) |
///
/// A single uniform draw over the whole range would put four rows in five above
/// the hold threshold and put a rejection in roughly one run in ten thousand,
/// which is neither realistic card spend nor a useful demonstration.
const PROBE_SHARE: f64 = 0.02;
const LARGE_SHARE: f64 = 0.20;
const PROBE_RANGE: (f64, f64) = (0.01, 0.49);
const EVERYDAY_RANGE: (f64, f64) = (1.00, 250.00);
const LARGE_RANGE: (f64, f64) = (250.00, 5_000.00);

/// Draw one authorisation amount from the mixture above.
fn draw_amount(rng: &mut impl rand::RngExt) -> f64 {
    let bucket = rng.random_range(0.0..1.0);
    let (low, high) = if bucket < PROBE_SHARE {
        PROBE_RANGE
    } else if bucket < PROBE_SHARE + LARGE_SHARE {
        LARGE_RANGE
    } else {
        EVERYDAY_RANGE
    };
    rng.random_range(low..high)
}

struct Args {
    count: u64,
    rate: u64,
    url: String,
    subject: String,
    seed: u64,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            count: 5_000,
            rate: 500,
            url: "nats://localhost:4222".to_string(),
            subject: "authorizations.raw".to_string(),
            seed: 0x5eed_1234_abcd_0001,
        }
    }
}

/// Parse `--key value` pairs.
///
/// Hand-rolled rather than clap: the binary's own CLI needs clap, but an example
/// with five flags does not, and `cargo check --examples` should not pull a
/// derive macro for it.
fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--count" => args.count = value()?.parse().map_err(|e| format!("--count: {e}"))?,
            "--rate" => args.rate = value()?.parse().map_err(|e| format!("--rate: {e}"))?,
            "--url" => args.url = value()?,
            "--subject" => args.subject = value()?,
            "--seed" => args.seed = value()?.parse().map_err(|e| format!("--seed: {e}"))?,
            "--help" | "-h" => {
                println!(
                    "usage: quickstart_publish [--count N (0 = continuous)] [--rate N] \
                     [--url URL] [--subject SUBJECT] [--seed N]"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    if args.rate == 0 {
        return Err("--rate must be at least 1".to_string());
    }
    Ok(args)
}

/// One authorisation as the NDJSON line the source decodes.
///
/// Written by hand rather than through `serde_json::to_string`: the field order
/// has to match the `Order` schema for a reader to be able to diff a message
/// against it, and a `Serialize` impl would sort by struct order anyway while
/// costing a dependency on the schema crate from this example.
fn line(id: u64, region: &str, currency: &str, amount: f64) -> String {
    format!(r#"{{"id":{id},"region":"{region}","currency":"{currency}","amount":{amount:.2},"#)
        + r#""valid":false,"usd_amount":0.0,"usd_amount_display":"","risk_score":0.0,"#
        + r#""flagged":false,"fee":0.0,"review_tier":0,"settlement":""}"#
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

    // Installed before connecting, so a Ctrl-C during a stuck connection
    // attempt interrupts it instead of waiting on the OS default handler.
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
            "publishing to {} on {} at {}/s until Ctrl-C",
            args.subject, args.url, args.rate
        ),
        count => println!(
            "publishing {count} authorisations to {} on {} at {}/s",
            args.subject, args.url, args.rate
        ),
    }

    let mut rng = rand::rngs::StdRng::seed_from_u64(args.seed);
    // One sleep per message at the requested spacing. A batched send with one
    // sleep per batch would produce a sawtooth the dashboard's rate graph would
    // show as bursts rather than the steady rate that was asked for.
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

        let region = REGIONS[rng.random_range(0..REGIONS.len())];
        let currency = CURRENCIES[rng.random_range(0..CURRENCIES.len())];
        let amount = draw_amount(&mut rng);

        client
            .publish(
                args.subject.clone(),
                line(id, region, currency, amount).into(),
            )
            .await?;
        sent += 1;

        // Deadline-based rather than cumulative sleeping, so a slow publish does
        // not push every later message out by the same amount.
        let deadline = started + spacing * u32::try_from(id).unwrap_or(u32::MAX);
        if let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
            tokio::select! {
                _ = tokio::time::sleep(remaining) => {}
                _ = cancel.cancelled() => break,
            }
        }

        id += 1;
    }

    // A core NATS publish is fire and forget, so without this the process can
    // exit with messages still in the client's write buffer.
    client.flush().await?;

    let elapsed = started.elapsed();
    println!(
        "published {} authorisations in {:.2}s ({:.0}/s)",
        sent,
        elapsed.as_secs_f64(),
        sent as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
    );
    Ok(())
}
