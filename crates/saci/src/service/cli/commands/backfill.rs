//! `saci-service backfill`, `cursor`, and `reload` CLI subcommands.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::SaciError;
use crate::inspector_wire::{
    BackfillReport, BackfillRequest, CursorResetReport, CursorResetRequest, ReloadReport,
    WorkflowCursorsReport,
};
use crate::service::ServiceBuilder;
use crate::service::cli::{BackfillArgs, CursorCmd, GlobalOpts, ReloadArgs};
use crate::service::config::{ServiceConfig, StoreConfig};
use crate::service::lifecycle::execute_backfill;
use crate::service::redb_state::RedbStateClient;
use crate::service::standalone::WorkflowStats;

use super::status::resolve_url;

fn parse_key_value_params(raw: &[String]) -> Result<BTreeMap<String, String>, SaciError> {
    let mut map = BTreeMap::new();
    for item in raw {
        let Some((k, v)) = item.split_once('=') else {
            return Err(SaciError::configuration(format!(
                "invalid --param '{item}': expected KEY=VALUE"
            )));
        };
        if k.trim().is_empty() {
            return Err(SaciError::configuration(format!(
                "invalid --param '{item}': key must not be empty"
            )));
        }
        map.insert(k.trim().to_string(), v.to_string());
    }
    Ok(map)
}

async fn post_json<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
    url: &str,
    body: &Req,
) -> Result<Resp, SaciError> {
    let client = reqwest::Client::new();
    let resp = client
        .post(url)
        .json(body)
        .send()
        .await
        .map_err(|e| SaciError::generic(format!("request to {url} failed: {e}")))?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(SaciError::generic(format!(
            "HTTP {status} from {url}: {text}"
        )));
    }
    resp.json::<Resp>()
        .await
        .map_err(|e| SaciError::generic(format!("parsing JSON from {url}: {e}")))
}

/// Entry point for `saci-service backfill`.
pub async fn run_backfill(
    global: &GlobalOpts,
    args: &BackfillArgs,
    customize: &dyn Fn(ServiceBuilder) -> ServiceBuilder,
) -> Result<(), SaciError> {
    let request = BackfillRequest {
        start_date: args.start.clone(),
        end_date: args.end.clone(),
        time_field: args.time_field.clone(),
        params: parse_key_value_params(&args.params)?,
        reset_state: args.reset_state,
    };

    let report: BackfillReport = if args.local {
        let raw = std::fs::read_to_string(&global.config).map_err(|e| {
            SaciError::configuration(format!(
                "reading config file {}: {e}",
                global.config.display()
            ))
        })?;
        let mut overrides: std::collections::HashMap<String, String> = request
            .params
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if let Some(start) = &request.start_date {
            overrides.insert("start_date".to_string(), start.clone());
        }
        if let Some(end) = &request.end_date {
            overrides.insert("end_date".to_string(), end.clone());
        }
        let config = ServiceConfig::from_kdl_with_overrides(&raw, &overrides)?;
        crate::service::validate_build_capabilities(&config)?;
        let wf = config
            .workflows
            .iter()
            .find(|w| w.id == args.workflow)
            .ok_or_else(|| {
                SaciError::configuration(format!(
                    "workflow '{}' not found in {}",
                    args.workflow,
                    global.config.display()
                ))
            })?
            .clone();
        let state = match &config.store {
            Some(StoreConfig::Redb { path, .. }) => Some(Arc::new(RedbStateClient::open(path)?)),
            None => None,
        };
        let factory = RefCell::new(customize(ServiceBuilder::new()).into_factory(&config));
        let mut total = WorkflowStats::default();
        let cancel = CancellationToken::new();
        execute_backfill(&wf, &request, &factory, &state, &None, &mut total, &cancel)
            .await
            .map_err(SaciError::generic)?
    } else {
        let addr = resolve_url(global, args.url.as_deref())?;
        let url = format!("{addr}/api/workflows/{}/backfill", args.workflow);
        post_json(&url, &request).await?
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&report).unwrap_or_else(|_| format!("{report:?}"))
    );
    Ok(())
}

/// Entry point for `saci-service cursor`.
pub async fn run_cursor(global: &GlobalOpts, cmd: &CursorCmd) -> Result<(), SaciError> {
    let client = reqwest::Client::new();
    match cmd {
        CursorCmd::List { workflow, url } => {
            let addr = resolve_url(global, url.as_deref())?;
            let endpoint = format!("{addr}/api/workflows/{workflow}/cursors");
            let resp = client
                .get(&endpoint)
                .send()
                .await
                .map_err(|e| SaciError::generic(format!("failed to reach {endpoint}: {e}")))?;
            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(SaciError::generic(format!(
                    "HTTP {status} from {endpoint}: {text}"
                )));
            }
            let report: WorkflowCursorsReport = resp
                .json()
                .await
                .map_err(|e| SaciError::generic(format!("parsing response: {e}")))?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).unwrap_or_else(|_| format!("{report:?}"))
            );
        }
        CursorCmd::Reset {
            workflow,
            source,
            to,
            to_unix_ms,
            items_processed,
            keep_priors,
            url,
        } => {
            let addr = resolve_url(global, url.as_deref())?;
            let endpoint = format!("{addr}/api/workflows/{workflow}/cursors/reset");
            let request = CursorResetRequest {
                source: source.clone(),
                to_unix_ms: *to_unix_ms,
                timestamp: to.clone(),
                items_processed: *items_processed,
                reset_priors: Some(!keep_priors),
            };
            let report: CursorResetReport = post_json(&endpoint, &request).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).unwrap_or_else(|_| format!("{report:?}"))
            );
        }
    }
    Ok(())
}

/// Entry point for `saci-service reload`.
pub async fn run_reload(global: &GlobalOpts, args: &ReloadArgs) -> Result<(), SaciError> {
    let addr = resolve_url(global, args.url.as_deref())?;
    let endpoint = format!("{addr}/api/reload");
    let body = match &args.file {
        Some(path) => std::fs::read_to_string(path).map_err(|e| {
            SaciError::configuration(format!("reading reload config {}: {e}", path.display()))
        })?,
        None => String::new(),
    };

    let client = reqwest::Client::new();
    let resp = client
        .post(&endpoint)
        .body(body)
        .send()
        .await
        .map_err(|e| SaciError::generic(format!("failed to reach {endpoint}: {e}")))?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(SaciError::generic(format!(
            "HTTP {status} from {endpoint}: {text}"
        )));
    }
    let report: ReloadReport = resp
        .json()
        .await
        .map_err(|e| SaciError::generic(format!("parsing reload response: {e}")))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).unwrap_or_else(|_| format!("{report:?}"))
    );
    Ok(())
}
