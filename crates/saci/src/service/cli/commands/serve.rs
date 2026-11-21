//! `saci-service serve`: start the SACI service.
//!
//! Loads the config, refuses one this binary cannot run, applies the command
//! line overrides, then hands the config and the customised
//! [`ServiceBuilder`] to [`serve`], which owns the
//! logging, stores, HTTP plane and runners and waits for SIGINT or SIGTERM
//! before draining within its 30-second shutdown budget.

use crate::SaciError;
use crate::service::config::{LogFormat, ServiceConfig};
use crate::service::{ServeExit, ServeOptions, ServiceBuilder, serve};

use crate::service::cli::{GlobalOpts, LogFormatArg, ServeArgs};

/// Entry point for the `serve` subcommand.
///
/// `customize` receives the builder once the built-in factories are
/// registered, so a custom server adds or replaces factories there. Overrunning
/// the shutdown budget exits 1 without printing the clean-stop line.
pub async fn run(
    global: &GlobalOpts,
    args: &ServeArgs,
    customize: &dyn Fn(ServiceBuilder) -> ServiceBuilder,
) -> Result<(), SaciError> {
    let config_path = &global.config;
    let mut config = ServiceConfig::load(config_path)?;
    // Before logging, the store, the meter provider or the port: a config
    // naming a host this binary was not built with cannot run whatever else
    // succeeds, and refusing here keeps the message first rather than buried
    // under a startup banner.
    crate::service::validate_build_capabilities(&config)?;

    if let Some(node_id) = args.node_id {
        config.node.id = node_id;
    }
    if let Some(level) = &args.log_level {
        config.observability.log_level = level.clone();
    }
    if let Some(format) = &args.log_format {
        config.observability.log_format = match format {
            LogFormatArg::Pretty => LogFormat::Pretty,
            LogFormatArg::Json => LogFormat::Json,
        };
    }
    if let Some(endpoint) = &args.otlp_endpoint {
        config.observability.otlp_endpoint = Some(endpoint.clone());
    }
    if let Some(bind) = &args.bind {
        config.http.bind = bind.clone();
    }

    let options = ServeOptions {
        config_path: Some(config_path.clone()),
        announce: true,
        shutdown: None,
    };
    match serve(config, customize(ServiceBuilder::new()), options).await? {
        ServeExit::Clean => Ok(()),
        ServeExit::ShutdownBudgetExceeded => std::process::exit(1),
    }
}
