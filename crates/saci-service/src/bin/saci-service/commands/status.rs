//! `saci-service status`: query the status of a running service instance.
//!
//! Sends a GET request to `{addr}/status` and prints either a summary line
//! (default) or the full JSON document (`--full`).

use saci_service::SaciError;

use crate::cli::{GlobalOpts, StatusArgs};

/// Entry point for the `status` subcommand.
pub async fn run(global: &GlobalOpts, args: &StatusArgs) -> Result<(), SaciError> {
    let addr = global.addr.as_ref().ok_or_else(|| {
        SaciError::configuration("--addr is required for status (e.g., http://localhost:8080)")
    })?;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/status"))
        .send()
        .await
        .map_err(|e| SaciError::generic(format!("failed to reach {addr}: {e}")))?;

    if !resp.status().is_success() {
        return Err(SaciError::generic(format!(
            "status endpoint returned HTTP {}",
            resp.status()
        )));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| SaciError::generic(format!("failed to parse /status JSON: {e}")))?;

    if args.full {
        println!(
            "{}",
            serde_json::to_string_pretty(&body).unwrap_or_else(|_| body.to_string())
        );
    } else {
        let node_id = body.get("node_id").and_then(|v| v.as_u64()).unwrap_or(0);
        let mode = body
            .get("mode")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let uptime = body
            .get("uptime_seconds")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let node_name = body
            .get("node_name")
            .and_then(|v| v.as_str())
            .map(|s| format!(" name={s}"))
            .unwrap_or_default();
        println!("node {node_id}{node_name}  mode={mode}  uptime={uptime}s");
    }

    Ok(())
}
