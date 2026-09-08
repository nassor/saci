//! Publish synthetic orders onto the branching example's NATS subject.
//!
//! One NDJSON object per message on `branching.orders`, which is what
//! `examples/branching/branching.kdl`'s `NatsSource` decodes. Every message
//! carries the two `Order` fields the routers read, `id` and `priority`.
//! Priority is drawn 50/50 between `"high"` and `"low"`, so both branches of
//! both processor splits fire as the stream runs: each message is one batch,
//! and each batch goes to exactly one branch.
//!
//! ```text
//! cargo run -p saci-service --features connector-nats --example branching_publish -- --rate 50
//! ```
//!
//! Flags: `--count` (default 0, runs until Ctrl-C), `--rate` messages per
//! second (default 50), `--url` (default `nats://localhost:4222`),
//! `--subject` (default `branching.orders`), `--seed` (default a fixed
//! constant, so a re-run reproduces the same priority sequence).

use std::time::{Duration, Instant};

// `random_range` lives on `RngExt` in rand 0.10, not on `Rng`.
use rand::{RngExt as _, SeedableRng as _};
use tokio_util::sync::CancellationToken;

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
            count: 0,
            rate: 50,
            url: "nats://localhost:4222".to_string(),
            subject: "branching.orders".to_string(),
            seed: 0x5eed_1234_abcd_0002,
        }
    }
}

/// Parse `--key value` pairs.
///
/// Hand-rolled rather than clap: the binary's own CLI needs clap, but an
/// example with five flags does not, and `cargo check --examples` should not
/// pull a derive macro for it.
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
                    "usage: branching_publish [--count N (0 = continuous)] [--rate N] \
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

/// One order as the NDJSON line the source decodes.
///
/// Written by hand rather than through `serde_json::to_string`: the field
/// order has to match the `Order` schema for a reader to be able to diff a
/// message against it, and a `Serialize` impl would sort by struct order
/// anyway while costing a dependency on the schema crate from this example.
fn line(id: u64, priority: &str) -> String {
    format!(r#"{{"id":{id},"priority":"{priority}"}}"#)
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
            "publishing {count} orders to {} on {} at {}/s",
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

        // A 50/50 draw keeps both branches of both processor splits busy.
        let priority = if rng.random_range(0..2) == 0 {
            "high"
        } else {
            "low"
        };

        client
            .publish(args.subject.clone(), line(id, priority).into())
            .await?;
        sent += 1;

        // Deadline-based rather than cumulative sleeping, so a slow publish
        // does not push every later message out by the same amount.
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
        "published {} orders in {:.2}s ({:.0}/s)",
        sent,
        elapsed.as_secs_f64(),
        sent as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
    );
    Ok(())
}
