//! `saci-service validate`: validate a config file without starting the service.
//!
//! Runs three validation gates:
//!
//! **Gate 0 (capabilities)**: the config schema is the same in every build, so
//! a file may name a host this binary does not carry, a `wasm` node, a
//! `plugin` node, or `mode "cluster"`.
//! [`validate_build_capabilities`](saci_service::service::validation::validate_build_capabilities)
//! refuses one before anything is built, naming the `--features` flag that
//! would run it. It runs ahead of `--connectors-only` too: that mode skips
//! processor nodes by design, so without this gate a config whose processor
//! this binary cannot host would report OK and exit 0.
//!
//! **Gate 1 (structural)**: parses the KDL and verifies every field. Syntax
//! errors, missing required fields, and keys the service cannot honour
//! (`workflow.systems`, a `watch` property on `wasm`) fail here.
//!
//! **Gate 2 (build + graph)**: builds the service with the built-in factory
//! registry, compiling any WASM module and checking its WIT world through
//! wasmtime instantiation, then validates every declared `link` end to end:
//! matching components and field-for-field identical Arrow schemas
//! ([`validate_workflow_graph`](saci_service::service::validation::validate_workflow_graph)).
//!
//! The schema-fingerprint gate
//! ([`validate_schema_fingerprint`](saci_service::service::validation::validate_schema_fingerprint))
//! is not run here. It compares the workflow against persisted state in
//! `node.data_dir`, which exists only once the cluster runner has opened it, so
//! `saci-service serve` applies it at cluster startup.
//!
//! ## `--connectors-only`
//!
//! Runs Gate 1, then builds every source, sink and transformer
//! ([`ServiceBuilder::build_connectors_only`](saci_service::service::builder::ServiceBuilder::build_connectors_only))
//! instead of Gate 2: every connector config still gets checked against its
//! `deny_unknown_fields` struct, but a processor node's module or library is
//! never touched, so a config naming a build artifact that does not exist on
//! this machine still validates.
//!
//! ## Unknown type handling
//!
//! A declared `type` the built-in registry does not hold is one of two very
//! different things, and
//! [`builtin_feature`](saci_service::service::factories::builtin_feature)
//! separates them.
//!
//! A name in
//! [`BUILTIN_CONNECTOR_FEATURES`](saci_service::service::factories::BUILTIN_CONNECTOR_FEATURES)
//! is a connector this crate ships whose feature is compiled out. It is
//! unusable by this binary whatever happens at serve time, so it is an error
//! in every mode, named with the feature to rebuild with, and `--strict`
//! changes nothing about it.
//!
//! Any other name may be a user-defined factory registered at serve time by
//! an embedder, so it stays a warning and `--strict` is what promotes it.
//!
//! ## Exit codes
//!
//! | Condition | Exit code |
//! |-----------|-----------|
//! | Config is structurally valid and all declared types resolve | 0 |
//! | Config is structurally valid but some types are unknown (default mode) | 0 (warnings printed to stderr) |
//! | Unknown types present and `--strict` is set | 1 |
//! | A declared type is a built-in connector this binary lacks the feature for | 1 |
//! | A `transformer` node's `format` that no transformer is registered for, whether this crate ships it and the feature is off or the name is unknown | 1 |
//! | A declared `wasm`/`plugin` node, or `mode "cluster"`, needs a host this binary lacks the feature for | 1 |
//! | Config fails structural validation | 1 |
//! | Workflow graph mismatch (link component/schema disagreement) | 1 |

use saci_service::SaciError;
use saci_service::service::ServiceBuilder;
use saci_service::service::config::{ServiceConfig, ServiceMode};
use saci_service::service::factories::{builtin_feature, register_builtin_factories};
use saci_service::service::validation::validate_build_capabilities;

use crate::cli::{GlobalOpts, ValidateArgs};

/// Entry point for the `validate` subcommand.
pub async fn run(global: &GlobalOpts, args: &ValidateArgs) -> Result<(), SaciError> {
    let config = ServiceConfig::load(&global.config)?;
    validate_build_capabilities(&config)?;
    let builder = register_builtin_factories(ServiceBuilder::new());

    if args.connectors_only {
        return run_connectors_only(builder, &config, args);
    }

    // Building with the built-in registry also compiles any WASM module,
    // verifies its WIT world, and validates the workflow graph. Unknown type
    // names surface as configuration errors naming the missing factory.
    let build_result = builder.build_all(&config);

    // Unknown-type errors become warnings; every other error is fatal
    // regardless of --strict.
    let (unknown_warnings, built) = match build_result {
        Ok(built) => (vec![], Some(built)),
        Err(e) if is_unknown_factory_error(&e) => (vec![e.message().to_string()], None),
        Err(e) => {
            return Err(SaciError::configuration(format!(
                "factory build failed: {}",
                e.message()
            )));
        }
    };

    if built.is_some() {
        println!("OK: workflow graph validated (components and schemas agree end to end)");
    }

    println!("OK: config is structurally valid");
    println!("  node.id:  {}", config.node.id);
    if let Some(name) = &config.node.name {
        println!("  node.name: {name}");
    }
    println!(
        "  mode:     {}",
        match config.mode {
            ServiceMode::Standalone { .. } => "standalone",
            ServiceMode::Cluster { .. } => "cluster",
        }
    );
    for workflow in &config.workflows {
        println!("  workflow: {}", workflow.id);
        if !workflow.wasm.is_empty() {
            println!(
                "  processors: {}",
                workflow
                    .wasm
                    .iter()
                    .map(|spec| spec
                        .module
                        .as_deref()
                        .unwrap_or("(runtime supplied programmatically)"))
                    .collect::<Vec<_>>()
                    .join(" -> ")
            );
        }
        println!("  sources:  {}", workflow.sources.len());
        println!("  sinks:    {}", workflow.sinks.len());
    }
    println!("  http.bind: {}", config.http.bind);
    println!("  log_level: {}", config.observability.log_level);
    report_unknown_warnings(&unknown_warnings, args)
}

/// `--connectors-only`: Gate 1, then every source, sink and transformer,
/// skipping the processor and workflow-graph gate entirely.
fn run_connectors_only(
    builder: ServiceBuilder,
    config: &ServiceConfig,
    args: &ValidateArgs,
) -> Result<(), SaciError> {
    let (unknown_warnings, built) = match builder.build_connectors_only(config) {
        Ok(()) => (vec![], true),
        Err(e) if is_unknown_factory_error(&e) => (vec![e.message().to_string()], false),
        Err(e) => {
            return Err(SaciError::configuration(format!(
                "factory build failed: {}",
                e.message()
            )));
        }
    };

    println!("OK: config is structurally valid");
    if built {
        println!("OK: every source, sink and transformer built");
    }
    for workflow in &config.workflows {
        println!(
            "  workflow: {} (sources: {}, sinks: {})",
            workflow.id,
            workflow.sources.len(),
            workflow.sinks.len()
        );
    }
    report_unknown_warnings(&unknown_warnings, args)
}

/// Shared tail of both validation paths.
///
/// Splits the collected misses into the two kinds the module doc describes,
/// prints each, and fails when either kind is fatal.
fn report_unknown_warnings(
    unknown_warnings: &[String],
    args: &ValidateArgs,
) -> Result<(), SaciError> {
    let (unavailable, unknown): (Vec<&String>, Vec<&String>) = unknown_warnings
        .iter()
        .partition(|message| declared_type(message).and_then(builtin_feature).is_some());

    if unavailable.is_empty() && unknown.is_empty() {
        println!("OK: all declared types resolved in built-in registry");
        return Ok(());
    }

    for message in &unavailable {
        eprintln!("ERROR: {message}");
    }
    for message in &unknown {
        eprintln!("WARNING: {message}");
    }
    if !unknown.is_empty() {
        eprintln!(
            "NOTE: {} unknown type(s) above are not in the built-in registry. \
             They may be user-defined types registered at serve time. \
             Use --strict to treat these as errors.",
            unknown.len()
        );
    }

    if !unavailable.is_empty() {
        return Err(SaciError::configuration(format!(
            "{} declared type(s) name a built-in connector this binary was built \
             without. Rebuild or reinstall with the feature named above.",
            unavailable.len()
        )));
    }

    if args.strict {
        return Err(SaciError::configuration(format!(
            "{} unknown factory type(s) found (--strict mode). \
             Register the factory or fix the type name in the config.",
            unknown.len()
        )));
    }
    Ok(())
}

/// The `type` string a missing-factory message names.
///
/// `missing_factory_error` writes `for type '<name>'` into every message
/// [`is_unknown_factory_error`] accepts, so reading the name back out is exact
/// rather than a guess. The producing end of that contract is pinned by
/// `factories::tests::the_feature_hint_keeps_the_classified_prefix`, which
/// covers the arm a build with every connector cannot reach through
/// `build_all`; the reading end by
/// `declared_type_reads_a_real_builder_message` below.
fn declared_type(message: &str) -> Option<&str> {
    let (_, after) = message.split_once("for type '")?;
    let (name, _) = after.split_once('\'')?;
    Some(name)
}

/// Returns `true` if the error is specifically a missing factory registration
/// (as opposed to a factory build failure or schema error).
fn is_unknown_factory_error(e: &SaciError) -> bool {
    // ServiceBuilder::build_all formats missing-factory errors as
    // "no source/sink factory registered for type '...'".
    e.category() == "configuration"
        && (e.message().contains("no source factory registered")
            || e.message().contains("no sink factory registered"))
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use saci_connector::{ConfigMap, ConfigValue};
    use saci_service::service::config::{
        HttpConfig, LinkSpec, NodeConfig, ObservabilityConfig, RetryConfig, ServiceConfig,
        ServiceMode, SinkSpec, SourceSpec, StandaloneConfig, WorkflowSpec,
    };
    use std::path::PathBuf;

    fn make_workflow(sources: Vec<SourceSpec>, sinks: Vec<SinkSpec>) -> WorkflowSpec {
        let links = sources
            .iter()
            .flat_map(|s| {
                sinks.iter().map(move |k| LinkSpec {
                    from: s.id.clone(),
                    to: k.id.clone(),
                    branch: None,
                })
            })
            .collect();
        WorkflowSpec {
            id: "test".to_string(),
            name: None,
            transformers: Vec::new(),
            sources,
            wasm: Vec::new(),
            plugin: Vec::new(),
            sinks,
            links,
            dlq: None,
        }
    }

    fn make_config(sources: Vec<SourceSpec>, sinks: Vec<SinkSpec>) -> ServiceConfig {
        ServiceConfig {
            heal: Default::default(),
            flow_control: Default::default(),
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/saci-test"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            workflows: vec![make_workflow(sources, sinks)],
            http: HttpConfig::default(),
            store: None,
            observability: ObservabilityConfig::default(),
            variables: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_builtin_only_config_validates_cleanly() {
        let config = make_config(vec![], vec![]);
        let builder = register_builtin_factories(ServiceBuilder::new());
        let result = builder.build_all(&config);
        assert!(
            result.is_ok(),
            "empty config should build cleanly: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_unknown_sink_type_is_unknown_factory_error() {
        let config = make_config(
            vec![],
            vec![SinkSpec {
                heal: None,
                id: "sink1".to_string(),
                name: None,
                type_name: "ClickHouseSink".to_string(), // not built-in
                transformer: None,
                component: "orders".to_string(),
                retry: RetryConfig::default(),
                config: ConfigValue::Object(ConfigMap::new()),
            }],
        );
        let builder = register_builtin_factories(ServiceBuilder::new());
        let err = builder.build_all(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            is_unknown_factory_error(&err),
            "ClickHouseSink should be classified as unknown factory: {err}"
        );
    }

    #[test]
    fn test_unknown_source_type_is_unknown_factory_error() {
        let config = make_config(
            vec![SourceSpec {
                heal: None,
                flow_control: None,
                id: "src1".to_string(),
                name: None,
                type_name: "MongoSource".to_string(), // not built-in
                transformer: None,
                component: "orders".to_string(),
                retry: RetryConfig::default(),
                config: ConfigValue::Object(ConfigMap::new()),
            }],
            vec![],
        );
        let builder = register_builtin_factories(ServiceBuilder::new());
        let err = builder.build_all(&config).unwrap_err();
        assert!(
            is_unknown_factory_error(&err),
            "MongoSource should be classified as unknown factory: {err}"
        );
    }

    #[test]
    fn test_non_factory_errors_not_classified_as_unknown() {
        let schema_err = SaciError::configuration("schema mismatch");
        assert!(
            !is_unknown_factory_error(&schema_err),
            "generic config error should not be classified as unknown factory"
        );
    }

    fn args(strict: bool) -> ValidateArgs {
        ValidateArgs {
            strict,
            connectors_only: false,
        }
    }

    /// The `for type '...'` shape [`declared_type`] reads is written by the
    /// builder, not by this file, so the contract is checked against a message
    /// the builder really produced.
    #[test]
    fn declared_type_reads_a_real_builder_message() {
        let config = make_config(
            vec![SourceSpec {
                heal: None,
                flow_control: None,
                id: "src1".to_string(),
                name: None,
                type_name: "MongoSource".to_string(),
                transformer: None,
                component: "orders".to_string(),
                retry: RetryConfig::default(),
                config: ConfigValue::Object(ConfigMap::new()),
            }],
            vec![],
        );
        let builder = register_builtin_factories(ServiceBuilder::new());
        let err = builder.build_all(&config).unwrap_err();
        assert_eq!(declared_type(&err.message()), Some("MongoSource"));
    }

    /// A connector this crate ships is unusable by a binary built without its
    /// feature, so it fails whether or not `--strict` was passed.
    #[test]
    fn a_feature_gated_builtin_fails_in_both_modes() {
        let message = "no source factory registered for type 'PostgresSource' \
                       (required by source 'pg_orders')"
            .to_string();
        for strict in [false, true] {
            let err = report_unknown_warnings(std::slice::from_ref(&message), &args(strict))
                .expect_err("a built-in whose feature is off is never a warning");
            assert!(
                err.message().contains("built-in connector"),
                "the failure should say why: {err}"
            );
        }
    }

    /// A type no crate here provides may still be registered at serve time, so
    /// it keeps the warning and the zero exit.
    #[test]
    fn a_user_defined_type_stays_a_warning() {
        let message = "no source factory registered for type 'MongoSource' \
                       (required by source 'mongo_orders')"
            .to_string();
        assert!(report_unknown_warnings(std::slice::from_ref(&message), &args(false)).is_ok());
        assert!(report_unknown_warnings(std::slice::from_ref(&message), &args(true)).is_err());
    }
}
