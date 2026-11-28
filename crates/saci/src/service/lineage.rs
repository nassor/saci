//! OpenLineage v2 event emission, catalog export, and transitive column-level lineage.
//!
//! Builds dataset and column-level lineage graphs across declarative `operator`
//! nodes, runtime (`wasm`/`plugin`/`native`) processors, sources, and sinks,
//! serves them on the HTTP control plane (`/api/lineage`, `/api/workflows/{id}/lineage`),
//! and dispatches OpenLineage v2 `RunEvent`s asynchronously to external catalogs
//! (Marquez, DataHub, Amundsen, OpenLineage collectors).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::config::ConfigValue;
use crate::connector::{parse_optional_fields, schema_of};
use crate::error::{SaciError, SaciResult};
use crate::operators::Expr;

use super::builder::{BuiltNode, BuiltNodeKind, BuiltWorkflow};
use super::config::{OperatorSpec, ServiceConfig, WorkflowSpec};

const PRODUCER_URI: &str = "https://saci.dev/lineage/v1";
const OPENLINEAGE_SCHEMA_URL: &str =
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";
const MAX_RECENT_EVENTS: usize = 128;

fn default_namespace() -> String {
    "saci".to_string()
}

fn default_timeout_ms() -> u64 {
    5_000
}

fn is_default_namespace(v: &str) -> bool {
    v == "saci"
}

fn is_default_timeout(v: &u64) -> bool {
    *v == 5_000
}

fn is_false(v: &bool) -> bool {
    !*v
}

fn is_default<T: Default + PartialEq>(v: &T) -> bool {
    *v == T::default()
}

/// When OpenLineage `RunEvent`s are emitted during workflow execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LineageEmitMode {
    /// Emit `START`, `COMPLETE`, `FAIL`, and `ABORT` around workflow runs.
    #[default]
    Lifecycle,
    /// Emit `START`, per-pass `RUNNING` updates with row statistics, and
    /// completion/failure events.
    Pass,
}

/// Top-level `lineage { ... }` configuration block on [`ServiceConfig`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineageConfig {
    /// Enable lineage collection and emission.
    #[serde(default, skip_serializing_if = "is_false")]
    pub enabled: bool,
    /// Optional OpenLineage collector HTTP endpoint (for example `http://localhost:5000/api/v1/lineage`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// OpenLineage namespace for jobs and datasets (default `"saci"`).
    #[serde(
        default = "default_namespace",
        skip_serializing_if = "is_default_namespace"
    )]
    pub namespace: String,
    /// Optional prefix prepended to workflow job names (`<prefix>.<workflow_id>`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_prefix: Option<String>,
    /// Optional Bearer API token sent to the OpenLineage HTTP endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    /// Event emission cadence (`"lifecycle"` or `"pass"`).
    #[serde(default, skip_serializing_if = "is_default")]
    pub emit_on: LineageEmitMode,
    /// HTTP request timeout in milliseconds (default `5000`).
    #[serde(
        default = "default_timeout_ms",
        skip_serializing_if = "is_default_timeout"
    )]
    pub timeout_ms: u64,
}

impl Default for LineageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            namespace: default_namespace(),
            job_prefix: None,
            api_key: None,
            emit_on: LineageEmitMode::Lifecycle,
            timeout_ms: default_timeout_ms(),
        }
    }
}

impl LineageConfig {
    /// Validate configuration invariants.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `namespace` is empty, `endpoint`
    /// is empty, or `timeout_ms == 0`.
    pub fn validate(&self) -> SaciResult<()> {
        if self.namespace.trim().is_empty() {
            return Err(SaciError::configuration(
                "lineage: 'namespace' must not be empty",
            ));
        }
        if let Some(ep) = &self.endpoint
            && ep.trim().is_empty()
        {
            return Err(SaciError::configuration(
                "lineage: 'endpoint' must not be empty when set",
            ));
        }
        if self.timeout_ms == 0 {
            return Err(SaciError::configuration(
                "lineage: 'timeout_ms' must be greater than 0",
            ));
        }
        Ok(())
    }

    /// Job name formatted for `workflow_id`.
    #[must_use]
    pub fn job_name(&self, workflow_id: &str) -> String {
        match &self.job_prefix {
            Some(prefix) if !prefix.trim().is_empty() => format!("{prefix}.{workflow_id}"),
            _ => workflow_id.to_string(),
        }
    }
}

/// OpenLineage v2 RunEvent type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LineageEventType {
    /// Run started.
    Start,
    /// Periodic in-progress update.
    Running,
    /// Run finished cleanly.
    Complete,
    /// Run terminated due to error.
    Fail,
    /// Run cancelled/aborted.
    Abort,
}

/// One column in an OpenLineage `SchemaDatasetFacet`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaFieldFacet {
    /// Column name.
    pub name: String,
    /// Column Arrow/SQL data type representation.
    #[serde(rename = "type")]
    pub data_type: String,
    /// Whether NULL values are permitted.
    pub nullable: bool,
}

/// OpenLineage `schema` dataset facet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaDatasetFacet {
    /// Producer URI.
    #[serde(rename = "_producer")]
    pub producer: String,
    /// OpenLineage schema facet URI.
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    /// Ordered dataset fields.
    pub fields: Vec<SchemaFieldFacet>,
}

/// OpenLineage `dataSource` dataset facet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataSourceDatasetFacet {
    /// Producer URI.
    #[serde(rename = "_producer")]
    pub producer: String,
    /// OpenLineage dataSource facet URI.
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    /// Logical datasource name (connector type).
    pub name: String,
    /// Redacted datasource URI (`<connector>://<resource>`).
    pub uri: String,
}

/// One upstream column contribution in [`ColumnLineageField`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnLineageInputField {
    /// Upstream dataset namespace.
    pub namespace: String,
    /// Upstream dataset name.
    pub name: String,
    /// Upstream column name.
    pub field: String,
}

/// Column provenance entry inside [`ColumnLineageDatasetFacet`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnLineageField {
    /// Contributing upstream columns.
    pub input_fields: Vec<ColumnLineageInputField>,
    /// Optional transformation kind (`"IDENTITY"`, `"TRANSFORMATION"`, `"AGGREGATION"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transformation_type: Option<String>,
    /// Human-readable transformation summary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transformation_description: Option<String>,
}

/// OpenLineage `columnLineage` dataset facet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnLineageDatasetFacet {
    /// Producer URI.
    #[serde(rename = "_producer")]
    pub producer: String,
    /// OpenLineage columnLineage facet URI.
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    /// Column lineage keyed by output field name.
    pub fields: BTreeMap<String, ColumnLineageField>,
}

/// OpenLineage `inputStatistics` / `outputStatistics` dataset facet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetStatisticsFacet {
    /// Producer URI.
    #[serde(rename = "_producer")]
    pub producer: String,
    /// Facet schema URL.
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    /// Row count processed on this dataset.
    pub row_count: u64,
    /// Optional byte count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

/// Container of standard OpenLineage dataset facets.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineageDatasetFacets {
    /// Dataset schema fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaDatasetFacet>,
    /// Data source connector URI facet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_source: Option<DataSourceDatasetFacet>,
    /// Column-level provenance facet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_lineage: Option<ColumnLineageDatasetFacet>,
    /// Optional run-time input row statistics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_statistics: Option<DatasetStatisticsFacet>,
    /// Optional run-time output row statistics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_statistics: Option<DatasetStatisticsFacet>,
}

/// Input or output dataset in an OpenLineage event or lineage graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineageDataset {
    /// Dataset namespace.
    pub namespace: String,
    /// Dataset identifier.
    pub name: String,
    /// Dataset facets (`schema`, `dataSource`, `columnLineage`, statistics).
    #[serde(default)]
    pub facets: LineageDatasetFacets,
}

/// OpenLineage run identifier + optional run facets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineageRun {
    /// Unique run identifier (UUID string).
    pub run_id: String,
    /// Additional run facets (e.g. errorMessage).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub facets: BTreeMap<String, Value>,
}

/// OpenLineage job identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineageJob {
    /// Job namespace.
    pub namespace: String,
    /// Job name.
    pub name: String,
    /// Additional job facets.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub facets: BTreeMap<String, Value>,
}

/// OpenLineage v2 `RunEvent` JSON payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageEvent {
    /// Event phase (`START`, `RUNNING`, `COMPLETE`, `FAIL`, `ABORT`).
    pub event_type: LineageEventType,
    /// RFC 3339 UTC timestamp.
    pub event_time: String,
    /// Producer URI.
    pub producer: String,
    /// OpenLineage spec schema URI.
    #[serde(rename = "schemaURL")]
    pub schema_url: String,
    /// Run handle.
    pub run: LineageRun,
    /// Job handle.
    pub job: LineageJob,
    /// Input datasets consumed by this job.
    pub inputs: Vec<LineageDataset>,
    /// Output datasets produced by this job.
    pub outputs: Vec<LineageDataset>,
}

/// One directed link inside [`WorkflowLineageGraph`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineageEdge {
    /// Source node id.
    pub from: String,
    /// Destination node id.
    pub to: String,
    /// Optional branch filter on this link.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

/// Full dataset & column-level lineage graph for one workflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowLineageGraph {
    /// Workflow identifier.
    pub workflow_id: String,
    /// Optional human-readable workflow name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_name: Option<String>,
    /// OpenLineage namespace.
    pub namespace: String,
    /// OpenLineage job name.
    pub job_name: String,
    /// Input source datasets with schemas and URIs.
    pub inputs: Vec<LineageDataset>,
    /// Output sink datasets with schemas, URIs, and transitive column lineage.
    pub outputs: Vec<LineageDataset>,
    /// Topological links within the workflow.
    pub edges: Vec<LineageEdge>,
}

/// HTTP snapshot returned by `GET /api/lineage`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineageSnapshot {
    /// Whether active OpenLineage emission is enabled.
    pub enabled: bool,
    /// Configured OpenLineage namespace.
    pub namespace: String,
    /// Configured emission mode (`lifecycle` or `pass`).
    pub emit_on: LineageEmitMode,
    /// Lineage graphs for all declared workflows.
    pub workflows: Vec<WorkflowLineageGraph>,
    /// Recent emitted OpenLineage `RunEvent`s buffered in memory.
    pub recent_events: Vec<OpenLineageEvent>,
}

/// Column origin tracking: output_col -> (upstream inputs, transform_type, description).
type ColumnProvenance = (Vec<ColumnLineageInputField>, &'static str, String);
type ComponentColumns = BTreeMap<String, ColumnProvenance>;

/// Compute dataset and transitive column-level lineage for every workflow in `config`.
pub fn build_service_lineage(
    config: &ServiceConfig,
    built: Option<&[BuiltWorkflow]>,
) -> Vec<WorkflowLineageGraph> {
    config
        .workflows
        .iter()
        .map(|wf| {
            let nodes = built
                .and_then(|list| list.iter().find(|b| b.workflow_id == wf.id))
                .map(|b| b.nodes.as_slice());
            build_workflow_lineage(config, wf, nodes)
        })
        .collect()
}

/// Compute dataset and transitive column-level lineage for a single [`WorkflowSpec`].
pub fn build_workflow_lineage(
    config: &ServiceConfig,
    workflow: &WorkflowSpec,
    built_nodes: Option<&[BuiltNode]>,
) -> WorkflowLineageGraph {
    let ns = config.lineage.namespace.as_str();
    let mut comp_schemas: HashMap<String, Vec<SchemaFieldFacet>> = HashMap::new();
    let mut provenance: HashMap<String, ComponentColumns> = HashMap::new();

    // Seed explicit workflow-wide schema declarations.
    for s in &workflow.schemas {
        let mut table = crate::config::ConfigMap::new();
        table.insert("field".to_string(), s.field.clone());
        let val = ConfigValue::Object(table);
        if let Ok(Some(fields)) = parse_optional_fields(&val, "lineage")
            && let Ok(schema) = schema_of(&fields)
        {
            comp_schemas.insert(s.id.clone(), schema_facets_from_arrow(&schema));
        }
    }

    // Collect runtime template schemas when built nodes are provided.
    let mut built_map: HashMap<&str, &BuiltNode> = HashMap::new();
    if let Some(nodes) = built_nodes {
        for bn in nodes {
            built_map.insert(bn.id.as_str(), bn);
            match &bn.kind {
                BuiltNodeKind::Source(src) => {
                    if let Some(comp) = bn.component {
                        comp_schemas
                            .insert(comp.to_string(), schema_facets_from_arrow(&src.schema()));
                    }
                }
                BuiltNodeKind::Processor { runtime, .. } => {
                    let tmpl = runtime.template_dataset();
                    for (comp, entry) in tmpl.schemas().iter() {
                        let s = tmpl
                            .output_schema(comp)
                            .unwrap_or_else(|| entry.schema.clone());
                        comp_schemas.insert((*comp).to_string(), schema_facets_from_arrow(&s));
                    }
                }
                BuiltNodeKind::Sink(sink) => {
                    if let Some(comp) = bn.component {
                        comp_schemas
                            .entry(comp.to_string())
                            .or_insert_with(|| schema_facets_from_arrow(&sink.schema()));
                    }
                }
            }
        }
    }

    // Build inputs & seed source column provenance.
    let mut inputs = Vec::with_capacity(workflow.sources.len());
    for src in &workflow.sources {
        let ds_name = dataset_name(&workflow.id, &src.id, &src.connector, &src.config);
        let fields = comp_schemas
            .get(&src.component)
            .cloned()
            .unwrap_or_else(|| facets_from_config(&src.config));
        comp_schemas
            .entry(src.component.clone())
            .or_insert_with(|| fields.clone());

        let comp_prov = provenance.entry(src.component.clone()).or_default();
        for f in &fields {
            comp_prov.insert(
                f.name.clone(),
                (
                    vec![ColumnLineageInputField {
                        namespace: ns.to_string(),
                        name: ds_name.clone(),
                        field: f.name.clone(),
                    }],
                    "IDENTITY",
                    format!("source '{}' ({})", src.id, src.connector),
                ),
            );
        }

        inputs.push(LineageDataset {
            namespace: ns.to_string(),
            name: ds_name.clone(),
            facets: LineageDatasetFacets {
                schema: (!fields.is_empty()).then(|| SchemaDatasetFacet {
                    producer: PRODUCER_URI.to_string(),
                    schema_url: "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json"
                        .to_string(),
                    fields,
                }),
                data_source: Some(DataSourceDatasetFacet {
                    producer: PRODUCER_URI.to_string(),
                    schema_url:
                        "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json"
                            .to_string(),
                    name: src.connector.clone(),
                    uri: connector_uri(&src.connector, &src.config),
                }),
                ..Default::default()
            },
        });
    }

    // Propagate column provenance through processors in topological order (if built) or declaration order.
    let op_by_id: HashMap<&str, &OperatorSpec> = workflow
        .operator
        .iter()
        .map(|o| (o.id.as_str(), o))
        .collect();
    if let Some(nodes) = built_nodes {
        for bn in nodes {
            if let Some(op) = op_by_id.get(bn.id.as_str()) {
                propagate_operator_lineage(op, ns, &mut comp_schemas, &mut provenance);
            } else if let BuiltNodeKind::Processor { runtime, .. } = &bn.kind {
                propagate_opaque_processor(
                    &bn.id,
                    runtime.as_ref(),
                    &mut comp_schemas,
                    &mut provenance,
                );
            }
        }
    } else {
        for op in &workflow.operator {
            propagate_operator_lineage(op, ns, &mut comp_schemas, &mut provenance);
        }
    }

    // Build output datasets with transitive ColumnLineageDatasetFacet.
    let mut outputs = Vec::with_capacity(workflow.sinks.len());
    for sink in &workflow.sinks {
        let ds_name = dataset_name(&workflow.id, &sink.id, &sink.connector, &sink.config);
        let mut fields = facets_from_config(&sink.config);
        if fields.is_empty() {
            fields = comp_schemas
                .get(&sink.component)
                .cloned()
                .unwrap_or_default();
        }
        let col_lineage = build_sink_column_lineage(&sink.component, &fields, &provenance);
        outputs.push(LineageDataset {
            namespace: ns.to_string(),
            name: ds_name,
            facets: LineageDatasetFacets {
                schema: (!fields.is_empty()).then(|| SchemaDatasetFacet {
                    producer: PRODUCER_URI.to_string(),
                    schema_url: "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json"
                        .to_string(),
                    fields,
                }),
                data_source: Some(DataSourceDatasetFacet {
                    producer: PRODUCER_URI.to_string(),
                    schema_url:
                        "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json"
                            .to_string(),
                    name: sink.connector.clone(),
                    uri: connector_uri(&sink.connector, &sink.config),
                }),
                column_lineage: col_lineage,
                ..Default::default()
            },
        });
    }

    let edges = workflow
        .links
        .iter()
        .map(|l| LineageEdge {
            from: l.from.clone(),
            to: l.to.clone(),
            branch: l.branch.clone(),
        })
        .collect();

    WorkflowLineageGraph {
        workflow_id: workflow.id.clone(),
        workflow_name: workflow.name.clone(),
        namespace: ns.to_string(),
        job_name: config.lineage.job_name(&workflow.id),
        inputs,
        outputs,
        edges,
    }
}

fn schema_facets_from_arrow(schema: &arrow_schema::Schema) -> Vec<SchemaFieldFacet> {
    schema
        .fields()
        .iter()
        .map(|f| SchemaFieldFacet {
            name: f.name().clone(),
            data_type: format!("{:?}", f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect()
}

fn facets_from_config(config: &ConfigValue) -> Vec<SchemaFieldFacet> {
    parse_optional_fields(config, "lineage")
        .ok()
        .flatten()
        .and_then(|fields| schema_of(&fields).ok())
        .map(|schema| schema_facets_from_arrow(&schema))
        .unwrap_or_default()
}

/// Safe allowlisted resource identifiers extracted from connector configs.
/// Credentials, passwords, tokens, and raw DSNs are never inspected.
fn safe_resource_label(config: &ConfigValue) -> Option<String> {
    for key in [
        "table", "topic", "subject", "path", "bucket", "database", "name",
    ] {
        if let Some(ConfigValue::String(v)) = config.get(key)
            && !v.trim().is_empty()
        {
            if key == "bucket"
                && let Some(ConfigValue::String(prefix)) = config.get("prefix")
            {
                return Some(format!("{v}/{}", prefix.trim_start_matches('/')));
            }
            return Some(v.clone());
        }
    }
    None
}

fn dataset_name(
    workflow_id: &str,
    node_id: &str,
    _connector: &str,
    config: &ConfigValue,
) -> String {
    match safe_resource_label(config) {
        Some(res) => res,
        None => format!("{workflow_id}.{node_id}"),
    }
}

fn connector_uri(connector: &str, config: &ConfigValue) -> String {
    match safe_resource_label(config) {
        Some(res) => format!("{connector}://{res}"),
        None => format!("{connector}://default"),
    }
}

fn resolve_input_cols(
    comp_prov: Option<&ComponentColumns>,
    col_names: &[String],
) -> Vec<ColumnLineageInputField> {
    let mut out = Vec::new();
    let Some(map) = comp_prov else {
        return out;
    };
    for col in col_names {
        if let Some((origins, _, _)) = map.get(col.as_str()) {
            for origin in origins {
                if !out.contains(origin) {
                    out.push(origin.clone());
                }
            }
        }
    }
    out
}

fn propagate_operator_lineage(
    op: &OperatorSpec,
    _ns: &str,
    comp_schemas: &mut HashMap<String, Vec<SchemaFieldFacet>>,
    provenance: &mut HashMap<String, ComponentColumns>,
) {
    let cfg = op.config.as_object();
    match op.kind.as_str() {
        "filter" | "deduplicate" | "sort" | "top_n" | "route" | "branch" | "quality" | "assert" => {
            let Some(src) = op.component.as_deref() else {
                return;
            };
            let dst = op.into.as_deref().unwrap_or(src);
            if dst != src {
                if let Some(prev) = provenance.get(src).cloned() {
                    provenance.insert(dst.to_string(), prev);
                }
                if let Some(fields) = comp_schemas.get(src).cloned() {
                    comp_schemas.entry(dst.to_string()).or_insert(fields);
                }
            }
            if matches!(op.kind.as_str(), "quality" | "assert")
                && let Some(ConfigValue::String(q_into)) =
                    cfg.and_then(|m| m.get("quarantine_into"))
                && let Some(src_prov) = provenance.get(src).cloned()
            {
                let mut all_origins = Vec::new();
                for (origins, _, _) in src_prov.values() {
                    for o in origins {
                        if !all_origins.contains(o) {
                            all_origins.push(o.clone());
                        }
                    }
                }
                let mut q_prov = src_prov;
                for diag in ["__dq_rule", "__dq_column", "__dq_reason"] {
                    q_prov.insert(
                        diag.to_string(),
                        (
                            all_origins.clone(),
                            "TRANSFORMATION",
                            format!("quality diagnostic from '{}'", op.id),
                        ),
                    );
                }
                provenance.insert(q_into.clone(), q_prov);
            }
        }
        "project" => {
            let Some(src) = op.component.as_deref() else {
                return;
            };
            let dst = op.into.as_deref().unwrap_or(src);
            let src_prov = provenance.get(src).cloned().unwrap_or_default();
            let mut next_prov = ComponentColumns::new();
            let raw_clauses = extract_string_list(cfg.and_then(|m| m.get("select")));
            for raw in raw_clauses {
                for clause in split_top_level_commas(&raw) {
                    let (expr_str, alias) = split_select_alias(clause);
                    let out_name = alias.unwrap_or_else(|| expr_str.trim().to_string());
                    if let Ok(parsed) = Expr::parse(expr_str) {
                        let refs = parsed.referenced_columns();
                        let origins = resolve_input_cols(Some(&src_prov), &refs);
                        let kind = if expr_str.trim() == out_name && refs.len() == 1 {
                            "IDENTITY"
                        } else {
                            "TRANSFORMATION"
                        };
                        next_prov.insert(
                            out_name,
                            (
                                origins,
                                kind,
                                format!("project({}): {}", op.id, clause.trim()),
                            ),
                        );
                    }
                }
            }
            if !next_prov.is_empty() {
                provenance.insert(dst.to_string(), next_prov);
            }
        }
        "group_by" | "window" => {
            let Some(src) = op.component.as_deref() else {
                return;
            };
            let dst = op.into.as_deref().unwrap_or(src);
            let src_prov = provenance.get(src).cloned().unwrap_or_default();
            let mut next_prov = ComponentColumns::new();
            for key in extract_string_list(cfg.and_then(|m| m.get("keys"))) {
                if let Some(entry) = src_prov.get(&key).cloned() {
                    next_prov.insert(key, entry);
                }
            }
            if let Some(win) = &op.window {
                for key in &win.key_fields {
                    if let Some(entry) = src_prov.get(key).cloned() {
                        next_prov.insert(key.clone(), entry);
                    }
                }
            }
            if let Some(aggs) = cfg.and_then(|m| m.get("aggregates").or_else(|| m.get("aggregate")))
            {
                for (out_name, input_col, desc) in extract_aggregate_refs(aggs) {
                    let origins = match input_col {
                        Some(c) => resolve_input_cols(Some(&src_prov), &[c]),
                        None => src_prov.values().flat_map(|(o, _, _)| o.clone()).collect(),
                    };
                    next_prov.insert(
                        out_name,
                        (
                            origins,
                            "AGGREGATION",
                            format!("{}({}): {desc}", op.kind, op.id),
                        ),
                    );
                }
            }
            if !next_prov.is_empty() {
                provenance.insert(dst.to_string(), next_prov);
            }
        }
        "join" | "interval_join" | "lookup_join" => {
            let (Some(left), Some(right), Some(into)) =
                (op.left.as_deref(), op.right.as_deref(), op.into.as_deref())
            else {
                return;
            };
            let prefix = cfg
                .and_then(|m| m.get("right_prefix"))
                .and_then(ConfigValue::as_str)
                .unwrap_or("right_");
            let l_prov = provenance.get(left).cloned().unwrap_or_default();
            let r_prov = provenance.get(right).cloned().unwrap_or_default();
            let mut merged = l_prov;
            for (k, v) in r_prov {
                let target_name = if merged.contains_key(&k) {
                    format!("{prefix}{k}")
                } else {
                    k
                };
                merged.insert(target_name, v);
            }
            provenance.insert(into.to_string(), merged);
        }
        "unnest" => {
            let Some(src) = op.component.as_deref() else {
                return;
            };
            let dst = op.into.as_deref().unwrap_or(src);
            let mut next_prov = provenance.get(src).cloned().unwrap_or_default();
            if let Some(field) = cfg
                .and_then(|m| m.get("field"))
                .and_then(ConfigValue::as_str)
            {
                let alias = cfg
                    .and_then(|m| m.get("alias").or_else(|| m.get("as")))
                    .and_then(ConfigValue::as_str)
                    .unwrap_or(field);
                let origins = resolve_input_cols(Some(&next_prov), &[field.to_string()]);
                next_prov.insert(
                    alias.to_string(),
                    (
                        origins,
                        "TRANSFORMATION",
                        format!("unnest({}): {field}", op.id),
                    ),
                );
            }
            provenance.insert(dst.to_string(), next_prov);
        }
        "sql" => {
            let Some(dst) = op.into.as_deref().or(op.component.as_deref()) else {
                return;
            };
            let Some(query) = cfg
                .and_then(|m| m.get("query"))
                .and_then(ConfigValue::as_str)
            else {
                return;
            };
            if let Ok(sys) = crate::operators::SqlSystem::parse(dst, query) {
                let mut combined_src = ComponentColumns::new();
                for &src_comp in sys.source_components() {
                    if let Some(sp) = provenance.get(src_comp) {
                        for (k, v) in sp {
                            combined_src.insert(k.clone(), v.clone());
                        }
                    }
                }
                let next_prov = sql_select_provenance(query, &combined_src, &op.id);
                if !next_prov.is_empty() {
                    provenance.insert(dst.to_string(), next_prov);
                }
            }
        }
        _ => {}
    }
}

fn propagate_opaque_processor(
    node_id: &str,
    runtime: &dyn crate::runtime::PipelineRuntime,
    comp_schemas: &mut HashMap<String, Vec<SchemaFieldFacet>>,
    provenance: &mut HashMap<String, ComponentColumns>,
) {
    let tmpl = runtime.template_dataset();
    let all_known_origins: Vec<ColumnLineageInputField> = provenance
        .values()
        .flat_map(|cols| cols.values().flat_map(|(o, _, _)| o.clone()))
        .collect();

    for (comp, entry) in tmpl.schemas().iter() {
        let out_schema = tmpl
            .output_schema(comp)
            .unwrap_or_else(|| entry.schema.clone());
        let facets = schema_facets_from_arrow(&out_schema);
        comp_schemas.insert((*comp).to_string(), facets.clone());

        let existing = provenance.get(*comp).cloned().unwrap_or_default();
        let mut updated = ComponentColumns::new();
        for f in &facets {
            if let Some(prev) = existing.get(&f.name) {
                updated.insert(f.name.clone(), prev.clone());
            } else {
                updated.insert(
                    f.name.clone(),
                    (
                        all_known_origins.clone(),
                        "TRANSFORMATION",
                        format!("processor '{node_id}'"),
                    ),
                );
            }
        }
        provenance.insert((*comp).to_string(), updated);
    }
}

fn build_sink_column_lineage(
    component: &str,
    fields: &[SchemaFieldFacet],
    provenance: &HashMap<String, ComponentColumns>,
) -> Option<ColumnLineageDatasetFacet> {
    let comp_prov = provenance.get(component)?;
    if comp_prov.is_empty() {
        return None;
    }
    let mut map = BTreeMap::new();
    if fields.is_empty() {
        for (col, (origins, ttype, desc)) in comp_prov {
            map.insert(
                col.clone(),
                ColumnLineageField {
                    input_fields: origins.clone(),
                    transformation_type: Some((*ttype).to_string()),
                    transformation_description: Some(desc.clone()),
                },
            );
        }
    } else {
        for f in fields {
            if let Some((origins, ttype, desc)) = comp_prov.get(&f.name) {
                map.insert(
                    f.name.clone(),
                    ColumnLineageField {
                        input_fields: origins.clone(),
                        transformation_type: Some((*ttype).to_string()),
                        transformation_description: Some(desc.clone()),
                    },
                );
            }
        }
    }
    (!map.is_empty()).then(|| ColumnLineageDatasetFacet {
        producer: PRODUCER_URI.to_string(),
        schema_url: "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json"
            .to_string(),
        fields: map,
    })
}

fn split_select_alias(clause: &str) -> (&str, Option<String>) {
    let trimmed = clause.trim();
    let upper = trimmed.to_ascii_uppercase();
    let mut depth = 0i32;
    let mut in_single = false;
    let bytes = upper.as_bytes();
    let mut last_as: Option<usize> = None;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => in_single = !in_single,
            b'(' if !in_single => depth += 1,
            b')' if !in_single => depth -= 1,
            b'A' if !in_single
                && depth == 0
                && i > 0
                && bytes.get(i + 1) == Some(&b'S')
                && bytes[i - 1].is_ascii_whitespace()
                && bytes.get(i + 2).is_some_and(u8::is_ascii_whitespace) =>
            {
                last_as = Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    if let Some(pos) = last_as {
        let expr_part = trimmed[..pos].trim();
        let alias_part = trimmed[pos + 2..]
            .trim()
            .trim_matches('"')
            .trim_matches('`');
        if !alias_part.is_empty() {
            return (expr_part, Some(alias_part.to_string()));
        }
    }
    (trimmed, None)
}

fn extract_string_list(val: Option<&ConfigValue>) -> Vec<String> {
    match val {
        Some(ConfigValue::String(s)) if !s.trim().is_empty() => vec![s.clone()],
        Some(ConfigValue::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    }
}

fn push_aggregate_spec(raw: &str, out: &mut Vec<(String, Option<String>, String)>) {
    for part in split_top_level_commas(raw) {
        let (expr_s, alias_opt) = split_select_alias(part);
        let col = extract_first_paren_arg(expr_s);
        let alias = alias_opt.unwrap_or_else(|| expr_s.trim().to_string());
        out.push((alias, col, part.trim().to_string()));
    }
}

fn extract_aggregate_refs(val: &ConfigValue) -> Vec<(String, Option<String>, String)> {
    let mut out = Vec::new();
    match val {
        ConfigValue::Object(map) => {
            if let Some(ConfigValue::Array(arr)) = map.get("aggregate") {
                for item in arr {
                    if let Some(obj) = item.as_object() {
                        let op = obj.get("op").and_then(ConfigValue::as_str).unwrap_or("agg");
                        let field = obj
                            .get("field")
                            .and_then(ConfigValue::as_str)
                            .map(str::to_string);
                        let alias = obj
                            .get("alias")
                            .or_else(|| obj.get("name"))
                            .and_then(ConfigValue::as_str)
                            .map(str::to_string)
                            .unwrap_or_else(|| match &field {
                                Some(f) => format!("{op}_{f}"),
                                None => op.to_string(),
                            });
                        out.push((alias, field, op.to_string()));
                    }
                }
            } else {
                for (alias, expr_val) in map {
                    if let Some(expr_s) = expr_val.as_str() {
                        let col = extract_first_paren_arg(expr_s);
                        out.push((alias.clone(), col, expr_s.to_string()));
                    }
                }
            }
        }
        ConfigValue::String(s) => push_aggregate_spec(s, &mut out),
        ConfigValue::Array(arr) => {
            for item in arr {
                if let Some(s) = item.as_str() {
                    push_aggregate_spec(s, &mut out);
                }
            }
        }
        _ => {}
    }
    out
}

fn extract_first_paren_arg(expr: &str) -> Option<String> {
    let open = expr.find('(')?;
    let close = expr.rfind(')')?;
    let inner = expr[open + 1..close].trim();
    if inner == "*" || inner.is_empty() {
        None
    } else {
        Some(
            inner
                .split(',')
                .next()
                .unwrap_or(inner)
                .trim()
                .trim_matches('"')
                .to_string(),
        )
    }
}

fn sql_select_provenance(
    query: &str,
    combined_src: &ComponentColumns,
    op_id: &str,
) -> ComponentColumns {
    let upper = query.to_ascii_uppercase();
    let Some(sel_pos) = upper.find("SELECT") else {
        return combined_src.clone();
    };
    let after_sel = &query[sel_pos + 6..];
    let after_upper = &upper[sel_pos + 6..];
    let Some(from_pos) = after_upper.find(" FROM ") else {
        return combined_src.clone();
    };
    let select_list = &after_sel[..from_pos];
    let mut out = ComponentColumns::new();
    for item in split_top_level_commas(select_list) {
        let trimmed = item.trim();
        if trimmed == "*" {
            for (k, v) in combined_src {
                out.insert(k.clone(), v.clone());
            }
            continue;
        }
        let (expr_s, alias) = split_select_alias(trimmed);
        let out_col = alias.unwrap_or_else(|| {
            expr_s
                .rsplit('.')
                .next()
                .unwrap_or(expr_s)
                .trim()
                .trim_matches('"')
                .to_string()
        });
        if let Ok(parsed) = Expr::parse(expr_s) {
            let refs: Vec<String> = parsed
                .referenced_columns()
                .into_iter()
                .map(|c| c.rsplit('.').next().unwrap_or(&c).to_string())
                .collect();
            let origins = resolve_input_cols(Some(combined_src), &refs);
            let kind = if refs.len() == 1 && expr_s.trim().ends_with(&refs[0]) {
                "IDENTITY"
            } else {
                "TRANSFORMATION"
            };
            out.insert(out_col, (origins, kind, format!("sql({op_id}): {trimmed}")));
        }
    }
    out
}

fn split_top_level_commas(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0i32;
    let mut in_single = false;
    for (idx, b) in input.bytes().enumerate() {
        match b {
            b'\'' => in_single = !in_single,
            b'(' if !in_single => depth += 1,
            b')' if !in_single => depth -= 1,
            b',' if !in_single && depth == 0 => {
                parts.push(&input[start..idx]);
                start = idx + 1;
            }
            _ => {}
        }
    }
    if start < input.len() {
        parts.push(&input[start..]);
    }
    parts
}

struct LineageInner {
    config: LineageConfig,
    graphs: HashMap<String, WorkflowLineageGraph>,
    order: Vec<String>,
    recent_events: VecDeque<OpenLineageEvent>,
}

enum WorkerMsg {
    Event(Box<OpenLineageEvent>),
    Flush(tokio::sync::oneshot::Sender<()>),
}

/// Thread-safe asynchronous OpenLineage dispatcher shared across the service
/// runner and HTTP control plane.
pub struct LineageDispatcher {
    inner: Arc<RwLock<LineageInner>>,
    tx: Mutex<Option<mpsc::Sender<WorkerMsg>>>,
}

impl LineageDispatcher {
    /// Construct a disabled dispatcher (used in unit tests or default builders).
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(RwLock::new(LineageInner {
                config: LineageConfig::default(),
                graphs: HashMap::new(),
                order: Vec::new(),
                recent_events: VecDeque::new(),
            })),
            tx: Mutex::new(None),
        }
    }

    /// Construct a dispatcher configured by `config` and spawn the async HTTP
    /// delivery task if `config.enabled && config.endpoint.is_some()` and a
    /// Tokio runtime is active.
    #[must_use]
    pub fn new(config: &ServiceConfig, built: Option<&[BuiltWorkflow]>) -> Self {
        let dispatcher = Self::disabled();
        dispatcher.update_config(config, built);
        dispatcher
    }

    /// Replace lineage configuration and graphs on startup or hot reload.
    pub fn update_config(&self, config: &ServiceConfig, built: Option<&[BuiltWorkflow]>) {
        let graphs = build_service_lineage(config, built);
        if let Ok(mut guard) = self.inner.write() {
            guard.config = config.lineage.clone();
            guard.graphs.clear();
            guard.order.clear();
            for g in graphs {
                guard.order.push(g.workflow_id.clone());
                guard.graphs.insert(g.workflow_id.clone(), g);
            }
        }
        self.ensure_worker(&config.lineage);
    }

    /// Refresh computed lineage graphs after workflows are built.
    pub fn update_graphs(&self, config: &ServiceConfig, built: Option<&[BuiltWorkflow]>) {
        let graphs = build_service_lineage(config, built);
        if let Ok(mut guard) = self.inner.write() {
            guard.graphs.clear();
            guard.order.clear();
            for g in graphs {
                guard.order.push(g.workflow_id.clone());
                guard.graphs.insert(g.workflow_id.clone(), g);
            }
        }
    }

    fn ensure_worker(&self, cfg: &LineageConfig) {
        let Ok(mut slot) = self.tx.lock() else {
            return;
        };
        if !cfg.enabled || cfg.endpoint.is_none() {
            *slot = None;
            return;
        }
        let endpoint = cfg.endpoint.clone().expect("checked");
        let api_key = cfg.api_key.clone();
        let timeout = Duration::from_millis(cfg.timeout_ms.max(1));
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            crate::service::install_ring_provider();
            let client = reqwest::Client::builder().timeout(timeout).build().ok();
            let (tx, mut rx) = mpsc::channel::<WorkerMsg>(256);
            *slot = Some(tx);
            handle.spawn(async move {
                while let Some(msg) = rx.recv().await {
                    match msg {
                        WorkerMsg::Event(ev) => {
                            let Some(c) = &client else {
                                continue;
                            };
                            let mut req = c.post(&endpoint).json(ev.as_ref());
                            if let Some(key) = &api_key {
                                req = req.bearer_auth(key);
                            }
                            match req.send().await {
                                Ok(resp) if !resp.status().is_success() => {
                                    #[cfg(feature = "tracing")]
                                    tracing::warn!(
                                        status = %resp.status(),
                                        endpoint = %endpoint,
                                        "openlineage event POST returned non-success status"
                                    );
                                }
                                Err(_e) => {
                                    #[cfg(feature = "tracing")]
                                    tracing::warn!(
                                        error = %_e,
                                        endpoint = %endpoint,
                                        "openlineage event POST failed"
                                    );
                                }
                                Ok(_) => {}
                            }
                        }
                        WorkerMsg::Flush(ack) => {
                            let _ = ack.send(());
                        }
                    }
                }
            });
        }
    }

    /// Wait for any queued HTTP OpenLineage events to finish sending (up to 5s).
    pub async fn flush(&self) {
        let tx = self.tx.lock().ok().and_then(|g| g.clone());
        let Some(tx) = tx else {
            return;
        };
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        if tx.send(WorkerMsg::Flush(ack_tx)).await.is_ok() {
            let _ = tokio::time::timeout(Duration::from_secs(5), ack_rx).await;
        }
    }

    /// Current lineage snapshot served by `GET /api/lineage`.
    #[must_use]
    pub fn snapshot(&self) -> LineageSnapshot {
        let guard = self
            .inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let workflows = guard
            .order
            .iter()
            .filter_map(|id| guard.graphs.get(id).cloned())
            .collect();
        LineageSnapshot {
            enabled: guard.config.enabled,
            namespace: guard.config.namespace.clone(),
            emit_on: guard.config.emit_on,
            workflows,
            recent_events: guard.recent_events.iter().cloned().collect(),
        }
    }

    /// Lineage graph for a single workflow (`GET /api/workflows/{id}/lineage`).
    #[must_use]
    pub fn workflow_lineage(&self, workflow_id: &str) -> Option<WorkflowLineageGraph> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .graphs
            .get(workflow_id)
            .cloned()
    }

    /// Emit `START` when lineage emission is enabled.
    pub fn emit_start(&self, workflow_id: &str, run_id: &str) {
        self.emit(
            workflow_id,
            run_id,
            LineageEventType::Start,
            None,
            None,
            None,
        );
    }

    /// Emit a per-pass `RUNNING` event when `emit_on == Pass`.
    pub fn emit_pass(&self, workflow_id: &str, run_id: &str, rows_in: u64, rows_out: u64) {
        let emit_on_pass = self
            .inner
            .read()
            .map(|g| g.config.enabled && g.config.emit_on == LineageEmitMode::Pass)
            .unwrap_or(false);
        if emit_on_pass {
            self.emit(
                workflow_id,
                run_id,
                LineageEventType::Running,
                Some(rows_in),
                Some(rows_out),
                None,
            );
        }
    }

    /// Emit `COMPLETE` when a workflow finishes.
    pub fn emit_complete(&self, workflow_id: &str, run_id: &str, rows_in: u64, rows_out: u64) {
        self.emit(
            workflow_id,
            run_id,
            LineageEventType::Complete,
            Some(rows_in),
            Some(rows_out),
            None,
        );
    }

    /// Emit `FAIL` with an error description.
    pub fn emit_fail(&self, workflow_id: &str, run_id: &str, error: &str) {
        self.emit(
            workflow_id,
            run_id,
            LineageEventType::Fail,
            None,
            None,
            Some(error),
        );
    }

    /// Emit `ABORT` when cancelled.
    pub fn emit_abort(&self, workflow_id: &str, run_id: &str) {
        self.emit(
            workflow_id,
            run_id,
            LineageEventType::Abort,
            None,
            None,
            None,
        );
    }

    fn emit(
        &self,
        workflow_id: &str,
        run_id: &str,
        event_type: LineageEventType,
        rows_in: Option<u64>,
        rows_out: Option<u64>,
        error_msg: Option<&str>,
    ) {
        let event = {
            let mut guard = self
                .inner
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !guard.config.enabled {
                return;
            }
            let (ns, job_name, mut inputs, mut outputs) =
                if let Some(g) = guard.graphs.get(workflow_id) {
                    (
                        g.namespace.clone(),
                        g.job_name.clone(),
                        g.inputs.clone(),
                        g.outputs.clone(),
                    )
                } else {
                    (
                        guard.config.namespace.clone(),
                        guard.config.job_name(workflow_id),
                        Vec::new(),
                        Vec::new(),
                    )
                };

            if let Some(in_rows) = rows_in {
                for ds in &mut inputs {
                    ds.facets.input_statistics = Some(DatasetStatisticsFacet {
                        producer: PRODUCER_URI.to_string(),
                        schema_url:
                            "https://openlineage.io/spec/facets/1-0-0/InputStatisticsInputDatasetFacet.json"
                                .to_string(),
                        row_count: in_rows,
                        size: None,
                    });
                }
            }
            if let Some(out_rows) = rows_out {
                for ds in &mut outputs {
                    ds.facets.output_statistics = Some(DatasetStatisticsFacet {
                        producer: PRODUCER_URI.to_string(),
                        schema_url:
                            "https://openlineage.io/spec/facets/1-1-1/OutputStatisticsOutputDatasetFacet.json"
                                .to_string(),
                        row_count: out_rows,
                        size: None,
                    });
                }
            }

            let mut run_facets = BTreeMap::new();
            if let Some(msg) = error_msg {
                run_facets.insert(
                    "errorMessage".to_string(),
                    json!({
                        "_producer": PRODUCER_URI,
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json",
                        "message": msg,
                        "programmingLanguage": "RUST"
                    }),
                );
            }

            let ev = OpenLineageEvent {
                event_type,
                event_time: chrono::Utc::now().to_rfc3339(),
                producer: PRODUCER_URI.to_string(),
                schema_url: OPENLINEAGE_SCHEMA_URL.to_string(),
                run: LineageRun {
                    run_id: run_id.to_string(),
                    facets: run_facets,
                },
                job: LineageJob {
                    namespace: ns,
                    name: job_name,
                    facets: BTreeMap::new(),
                },
                inputs,
                outputs,
            };
            if guard.recent_events.len() >= MAX_RECENT_EVENTS {
                guard.recent_events.pop_front();
            }
            guard.recent_events.push_back(ev.clone());
            ev
        };

        if let Ok(slot) = self.tx.lock()
            && let Some(tx) = slot.as_ref()
        {
            let _ = tx.try_send(WorkerMsg::Event(Box::new(event)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::config::test_support::parse;

    #[test]
    fn lineage_traces_column_provenance_across_operators() {
        let kdl = r#"
mode "standalone"
node id=1 data_dir="/tmp/saci-lineage"
lineage enabled=#true namespace="analytics" job_prefix="prod"

workflow "orders_etl" {
    source "raw" connector="file" component="RawOrder" {
        config path="/data/raw.csv" {
            field "id" type="int64" nullable=#false
            field "price" type="float64" nullable=#false
            field "qty" type="int64" nullable=#false
        }
    }
    operator "calc" kind="project" component="RawOrder" into="ScoredOrder" {
        config select="id, price * qty AS total"
    }
    operator "dq" kind="quality" component="ScoredOrder" into="CleanOrder" {
        config {
            rules {
                rule "pos_total" type="range" column="total" min=0.0
            }
        }
    }
    sink "out" connector="file" component="CleanOrder" {
        config path="/data/clean.parquet" {
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }
    }
    link from="raw" to="calc"
    link from="calc" to="dq"
    link from="dq" to="out"
}
"#;
        let cfg = parse(kdl).expect("config parses");
        let graphs = build_service_lineage(&cfg, None);
        assert_eq!(graphs.len(), 1);
        let g = &graphs[0];
        assert_eq!(g.namespace, "analytics");
        assert_eq!(g.job_name, "prod.orders_etl");
        assert_eq!(g.inputs.len(), 1);
        assert_eq!(g.outputs.len(), 1);

        let col_lineage = g.outputs[0]
            .facets
            .column_lineage
            .as_ref()
            .expect("column lineage computed");
        assert_eq!(col_lineage.fields["id"].input_fields.len(), 1);
        assert_eq!(col_lineage.fields["id"].input_fields[0].field, "id");
        let total_inputs: Vec<&str> = col_lineage.fields["total"]
            .input_fields
            .iter()
            .map(|f| f.field.as_str())
            .collect();
        assert_eq!(total_inputs, vec!["price", "qty"]);
        assert_eq!(
            col_lineage.fields["total"].transformation_type.as_deref(),
            Some("TRANSFORMATION")
        );
    }

    #[tokio::test]
    async fn dispatcher_posts_openlineage_events_over_http() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        let (done_tx, mut done_rx) = mpsc::channel::<String>(4);

        tokio::spawn(async move {
            for _ in 0..2 {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 16_384];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let raw = String::from_utf8_lossy(&buf[..n]).to_string();
                let body = raw.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
                let _ = done_tx.send(body).await;
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
            }
        });

        let kdl = format!(
            r#"
mode "standalone"
node id=1 data_dir="/tmp/saci-lineage-http"
lineage enabled=#true endpoint="http://{addr}/api/v1/lineage" namespace="ns" emit_on="pass"
workflow "w" {{
    source "in" connector="noop" component="X"
    sink "out" connector="noop" component="X"
    link from="in" to="out"
}}
"#
        );
        let cfg = parse(&kdl).expect("parse");
        let dispatcher = LineageDispatcher::new(&cfg, None);
        dispatcher.emit_start("w", "run-1");
        dispatcher.emit_complete("w", "run-1", 42, 40);
        dispatcher.flush().await;

        let ev1 = tokio::time::timeout(Duration::from_secs(3), done_rx.recv())
            .await
            .expect("start received")
            .unwrap();
        let ev2 = tokio::time::timeout(Duration::from_secs(3), done_rx.recv())
            .await
            .expect("complete received")
            .unwrap();
        assert!(ev1.contains("\"START\""), "{ev1}");
        assert!(
            ev2.contains("\"COMPLETE\"") && ev2.contains("\"rowCount\":40"),
            "{ev2}"
        );
        assert_eq!(dispatcher.snapshot().recent_events.len(), 2);
    }

    #[test]
    fn lineage_traces_join_group_by_and_sql_wildcard_columns() {
        let kdl = r#"
mode "standalone"
node id=1 data_dir="/tmp/saci-lineage-complex"
lineage enabled=#true namespace="analytics"

workflow "join_sum" {
    source "s_orders" connector="file" component="Orders" {
        config path="/data/orders.csv" {
            field "order_id" type="int64" nullable=#false
            field "cust_id" type="int64" nullable=#false
            field "amount" type="float64" nullable=#false
        }
    }
    source "s_cust" connector="file" component="Customers" {
        config path="/data/customers.csv" {
            field "id" type="int64" nullable=#false
            field "tier" type="utf8" nullable=#false
        }
    }
    operator "join_oc" kind="join" left="Orders" right="Customers" into="Joined" {
        config on="cust_id=id" join_type="inner"
    }
    operator "agg_tier" kind="group_by" component="Joined" into="TierTotals" {
        config keys="tier" {
            aggregates "sum(amount) AS total_amount"
        }
    }
    operator "pass_sql" kind="sql" into="OutRows" {
        config query="SELECT * FROM TierTotals"
    }
    sink "out" connector="file" component="OutRows" {
        config path="/data/tier_totals.parquet" {
            field "tier" type="utf8" nullable=#false
            field "total_amount" type="float64" nullable=#false
        }
    }
    link from="s_orders" to="join_oc"
    link from="s_cust" to="join_oc"
    link from="join_oc" to="agg_tier"
    link from="agg_tier" to="pass_sql"
    link from="pass_sql" to="out"
}
"#;
        let cfg = parse(kdl).expect("parse");
        let g = build_workflow_lineage(&cfg, &cfg.workflows[0], None);
        let col = g.outputs[0]
            .facets
            .column_lineage
            .as_ref()
            .expect("column lineage present");
        assert_eq!(
            col.fields["tier"].input_fields[0].name,
            "/data/customers.csv"
        );
        assert_eq!(col.fields["tier"].input_fields[0].field, "tier");
        assert_eq!(
            col.fields["total_amount"].input_fields[0].name,
            "/data/orders.csv"
        );
        assert_eq!(col.fields["total_amount"].input_fields[0].field, "amount");
        assert_eq!(
            col.fields["total_amount"].transformation_type.as_deref(),
            Some("AGGREGATION")
        );
    }

    #[test]
    fn emit_fail_records_error_message_run_facet() {
        let cfg = parse(
            r#"
mode "standalone"
node id=1 data_dir="/tmp/saci-lineage-fail"
lineage enabled=#true namespace="prod"
workflow "wf" {
    source "in" connector="noop" component="X"
    sink "out" connector="noop" component="X"
    link from="in" to="out"
}
"#,
        )
        .unwrap();
        let dispatcher = LineageDispatcher::new(&cfg, None);
        dispatcher.emit_fail("wf", "run-fail", "connection reset");
        let events = dispatcher.snapshot().recent_events;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, LineageEventType::Fail);
        assert_eq!(
            events[0].run.facets["errorMessage"]["message"],
            "connection reset"
        );
    }
}
