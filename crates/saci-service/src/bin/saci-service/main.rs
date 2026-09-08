//! saci-service: SACI distributed batch processing service binary.
//!
//! Reference implementation of the SACI service layer, gated on the `service`
//! feature flag.
//!
//! ## Usage
//!
//! ```text
//! saci-service serve --config service.kdl
//! saci-service validate --config service.kdl
//! saci-service status --addr http://localhost:8080
//! saci-service cluster init --config service.kdl
//! saci-service cluster status --addr http://localhost:8080
//! ```

use clap::Parser;

mod cli;
mod commands;

// Pipelines allocate and free multi-megabyte Arrow arrays once per batch.
// System allocators return those large blocks to the OS on free, so the next
// batch soft-faults every page again; mimalloc retains them instead.
//
// Opt out with `--no-default-features` plus the feature set you want; the
// library itself never installs an allocator.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    // Load a repo-root `.env` so `${VAR}` placeholders in configs and cloud
    // credentials resolve without a manual export. Optional: an absent or
    // unreadable file is ignored, and `dotenvy::dotenv` never overrides a
    // variable already present in the environment.
    let _ = dotenvy::dotenv();
    let parsed = cli::Cli::parse();
    // `status`/`cluster status` build a reqwest client; reqwest 0.13's
    // `rustls-no-provider` needs the crypto provider installed first.
    saci_service::service::install_ring_provider();
    let result = match &parsed.cmd {
        cli::Command::Serve(args) => commands::serve::run(&parsed.global, args).await,
        cli::Command::Validate(args) => commands::validate::run(&parsed.global, args).await,
        cli::Command::Status(args) => commands::status::run(&parsed.global, args).await,
        cli::Command::Cluster { cmd } => commands::cluster::run(&parsed.global, cmd).await,
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
