//! `saci-service status`: query the status of a running service instance.
//!
//! Sends a GET request to `{url}/status` and prints either a summary line
//! (default) or the full JSON document (`--full`). Without `--url` the node
//! is the one `--config` points at, reached on `http://{http.bind}`.

use crate::SaciError;
use crate::service::config::ServiceConfig;

use crate::service::cli::{GlobalOpts, StatusArgs};

/// The base URL to query: `--url` when given, else `http://{http.bind}` from
/// the config at `--config`.
pub(crate) fn resolve_url(global: &GlobalOpts, url: Option<&str>) -> Result<String, SaciError> {
    if let Some(url) = url {
        return Ok(url.trim_end_matches('/').to_owned());
    }
    let config = ServiceConfig::load(&global.config).map_err(|_| {
        SaciError::configuration("pass --url, or --config pointing at the node's config")
    })?;
    Ok(format!("http://{}", config.http.bind))
}

/// Entry point for the `status` subcommand.
pub async fn run(global: &GlobalOpts, args: &StatusArgs) -> Result<(), SaciError> {
    let addr = resolve_url(global, args.url.as_deref())?;

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
