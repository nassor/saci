//! Builds declarative `operator` nodes into native [`Pipeline`] runtimes.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema;

use crate::config::ConfigValue;
use crate::connector::{parse_fields, schema_of};
use crate::dataset::intern_component_name;
use crate::error::{SaciError, SaciResult};
use crate::operators::{
    AggregateOp, AggregateSpec, BranchRule, DataQualityState, DataQualitySystem, DeduplicateState,
    DeduplicateSystem, FilterSystem, GroupBySystem, IntervalJoinState, IntervalJoinSystem, JoinKey,
    JoinKind, JoinSystem, KeepPolicy, LookupJoinState, LookupJoinSystem, ProjectSystem,
    QualityFailureAction, QualityRule, RouteMode, RouteSystem, SortKey, SortSystem, SqlSystem,
    TopNSystem, UnnestSystem,
};
#[cfg(feature = "windows")]
use crate::operators::{WindowAggregateState, WindowAggregateSystem, WindowColumns};
use crate::pipeline::Pipeline;

use super::builder::{BuiltNode, BuiltNodeKind};
use super::config::{OperatorSpec, WorkflowSpec};

pub(super) fn validate_operator_config(spec: &OperatorSpec, workflow_id: &str) -> SaciResult<()> {
    build_operator_pipeline(spec, workflow_id, None).map(|_| ())
}

pub(super) fn build_operator_node(
    spec: &OperatorSpec,
    workflow: &WorkflowSpec,
    built_nodes: &[BuiltNode],
) -> SaciResult<BuiltNode> {
    if !cfg!(feature = "windows") && spec.window.is_some() {
        return Err(super::factories::missing_windows_engine_error(
            "operator",
            &spec.id,
            &workflow.id,
        ));
    }
    spec.validate(&workflow.id)?;

    let schemas = collect_known_schemas(workflow, built_nodes)?;
    let pipeline = build_operator_pipeline(spec, &workflow.id, Some(&schemas))?;

    Ok(BuiltNode {
        id: spec.id.clone(),
        name: spec.name.clone(),
        connector: format!("operator:{}", spec.kind),
        component: None,
        kind: BuiltNodeKind::Processor {
            runtime: Box::new(pipeline),
            kind: "native",
        },
        downstream: Vec::new(),
        artifact: None,
        heal_recovered: None,
        window: spec.window.clone(),
        evolution: workflow
            .schema_evolution
            .map_or_else(crate::schema::EvolutionPolicy::strict, |c| c.policy()),
        edge_evolutions: Vec::new(),
        on_corrupt: super::config::CorruptRecordPolicy::default(),
    })
}

fn build_operator_pipeline(
    spec: &OperatorSpec,
    workflow_id: &str,
    schemas: Option<&HashMap<String, Arc<Schema>>>,
) -> SaciResult<Pipeline> {
    let ctx = format!("workflow '{workflow_id}' operator '{}'", spec.id);
    let cfg = config_map(&spec.config, &ctx)?;
    let mut pipeline = Pipeline::new(spec.id.clone());

    match spec.kind.as_str() {
        "filter" => {
            check_allowed_keys(cfg, &["where", "predicate"], &ctx)?;
            let src = req_endpoint(spec.component.as_deref(), "component", &ctx)?;
            let dst = spec.into.as_deref().unwrap_or(src);
            let pred = req_str(cfg, "where", &ctx).or_else(|_| req_str(cfg, "predicate", &ctx))?;
            let mut sys = FilterSystem::parse(src, &pred)?;
            if dst != src {
                sys = sys.into_component(dst);
            }
            if let Some(in_schema) = schemas.and_then(|m| m.get(src)) {
                register_if_absent(&mut pipeline, src, in_schema.clone())?;
                if dst != src {
                    register_if_absent(&mut pipeline, dst, in_schema.clone())?;
                }
            }
            pipeline = pipeline.system(sys);
        }
        "project" => {
            check_allowed_keys(cfg, &["select"], &ctx)?;
            let src = req_endpoint(spec.component.as_deref(), "component", &ctx)?;
            let dst = spec.into.as_deref().unwrap_or(src);
            let clauses = req_expr_list(cfg, "select", &ctx)?;
            let mut sys = ProjectSystem::parse(src, &clauses)?;
            if dst != src {
                sys = sys.into_component(dst);
            }
            if let Some(in_schema) = schemas.and_then(|m| m.get(src)) {
                let out_schema = sys.output_schema(in_schema)?;
                register_if_absent(&mut pipeline, src, in_schema.clone())?;
                if dst != src {
                    register_if_absent(&mut pipeline, dst, out_schema)?;
                } else {
                    pipeline.data_mut().set_output_schema(dst, out_schema);
                }
            }
            pipeline = pipeline.system(sys);
        }
        "window" => {
            pipeline = build_window_operator_pipeline(pipeline, spec, cfg, &ctx, schemas)?;
        }
        "group_by" if spec.window.is_some() => {
            pipeline = build_window_operator_pipeline(pipeline, spec, cfg, &ctx, schemas)?;
        }
        "group_by" => {
            check_allowed_keys(cfg, &["keys", "aggregates", "aggregate"], &ctx)?;
            let src = req_endpoint(spec.component.as_deref(), "component", &ctx)?;
            let dst = spec.into.as_deref().unwrap_or(src);
            let keys = opt_ident_list(cfg, "keys");
            let aggs = parse_aggregates(cfg, &ctx)?;
            let sys = GroupBySystem::new(src, dst, keys, aggs)?;
            if let Some(in_schema) = schemas.and_then(|m| m.get(src)) {
                let out_schema = sys.output_schema(in_schema)?;
                register_if_absent(&mut pipeline, src, in_schema.clone())?;
                if dst != src {
                    register_if_absent(&mut pipeline, dst, out_schema)?;
                } else {
                    pipeline.data_mut().set_output_schema(dst, out_schema);
                }
            }
            pipeline = pipeline.system(sys);
        }
        "route" | "branch" => {
            check_allowed_keys(cfg, &["branches", "mode", "exclusive", "default"], &ctx)?;
            let src = req_endpoint(spec.component.as_deref(), "component", &ctx)?;
            let dst = spec.into.as_deref().unwrap_or(src);
            let rules = parse_branch_rules(cfg, &ctx)?;
            let mode = match opt_str(cfg, "mode")
                .map(|m| m.to_ascii_lowercase())
                .as_deref()
            {
                None | Some("row") => RouteMode::Row,
                Some("first_row") => RouteMode::FirstRow,
                Some("any") | Some("batch_any") => RouteMode::BatchAny,
                Some("all") | Some("batch_all") => RouteMode::BatchAll,
                Some(other) => {
                    return Err(SaciError::configuration(format!(
                        "{ctx}: invalid route mode '{other}' (expected row, first_row, any, or all)"
                    )));
                }
            };
            let exclusive = opt_bool(cfg, "exclusive", &ctx)?.unwrap_or(true);
            let mut sys = RouteSystem::new(src, rules)?
                .mode(mode)
                .exclusive(exclusive);
            if dst != src {
                sys = sys.into_component(dst);
            }
            if let Some(def) = opt_str(cfg, "default") {
                if def.is_empty() {
                    return Err(SaciError::configuration(format!(
                        "{ctx}: default branch must not be empty"
                    )));
                }
                sys = sys.default_branch(def);
            }
            if let Some(in_schema) = schemas.and_then(|m| m.get(src)) {
                sys.validate_schema(in_schema)?;
                register_if_absent(&mut pipeline, src, in_schema.clone())?;
                if dst != src {
                    register_if_absent(&mut pipeline, dst, in_schema.clone())?;
                }
            }
            pipeline = pipeline.system(sys);
        }
        "sort" => {
            check_allowed_keys(cfg, &["order_by", "limit", "offset"], &ctx)?;
            let src = req_endpoint(spec.component.as_deref(), "component", &ctx)?;
            let dst = spec.into.as_deref().unwrap_or(src);
            let keys = parse_sort_keys(cfg, &ctx)?;
            let mut sys = SortSystem::new(src, dst, keys)?;
            if let Some(limit) = opt_usize(cfg, "limit", &ctx)? {
                sys = sys.limit(limit);
            }
            if let Some(offset) = opt_usize(cfg, "offset", &ctx)? {
                sys = sys.offset(offset);
            }
            if let Some(in_schema) = schemas.and_then(|m| m.get(src)) {
                register_if_absent(&mut pipeline, src, in_schema.clone())?;
                if dst != src {
                    register_if_absent(&mut pipeline, dst, in_schema.clone())?;
                }
            }
            pipeline = pipeline.system(sys);
        }
        "top_n" => {
            check_allowed_keys(cfg, &["order_by", "limit", "offset"], &ctx)?;
            let src = req_endpoint(spec.component.as_deref(), "component", &ctx)?;
            let dst = spec.into.as_deref().unwrap_or(src);
            let keys = parse_sort_keys(cfg, &ctx)?;
            let limit = req_usize(cfg, "limit", &ctx)?;
            let mut sys = TopNSystem::new(src, dst, keys, limit)?;
            if let Some(offset) = opt_usize(cfg, "offset", &ctx)? {
                sys = sys.offset(offset);
            }
            if let Some(in_schema) = schemas.and_then(|m| m.get(src)) {
                register_if_absent(&mut pipeline, src, in_schema.clone())?;
                if dst != src {
                    register_if_absent(&mut pipeline, dst, in_schema.clone())?;
                }
            }
            pipeline = pipeline.system(sys);
        }
        "unnest" => {
            check_allowed_keys(cfg, &["field", "alias", "as", "preserve_empty"], &ctx)?;
            let src = req_endpoint(spec.component.as_deref(), "component", &ctx)?;
            let dst = spec.into.as_deref().unwrap_or(src);
            let field = req_str(cfg, "field", &ctx)?;
            let alias = opt_str(cfg, "alias")
                .or_else(|| opt_str(cfg, "as"))
                .unwrap_or_else(|| field.clone());
            let mut sys = UnnestSystem::new(src, dst, &field, &alias)?;
            if let Some(preserve) = opt_bool(cfg, "preserve_empty", &ctx)? {
                sys = sys.preserve_empty(preserve);
            }
            if let Some(in_schema) = schemas.and_then(|m| m.get(src)) {
                let out_schema = sys.output_schema(in_schema)?;
                register_if_absent(&mut pipeline, src, in_schema.clone())?;
                if dst != src {
                    register_if_absent(&mut pipeline, dst, out_schema)?;
                } else {
                    pipeline.data_mut().set_output_schema(dst, out_schema);
                }
            }
            pipeline = pipeline.system(sys);
        }
        "join" => {
            check_allowed_keys(cfg, &["on", "join_type", "right_prefix"], &ctx)?;
            let left = req_endpoint(spec.left.as_deref(), "left", &ctx)?;
            let right = req_endpoint(spec.right.as_deref(), "right", &ctx)?;
            let into = req_endpoint(spec.into.as_deref(), "into", &ctx)?;
            let keys = parse_join_keys(cfg, &ctx)?;
            let kind = parse_join_kind(opt_str(cfg, "join_type").as_deref(), &ctx)?;
            let mut sys = JoinSystem::new(left, right, into, kind, keys)?;
            if let Some(prefix) = opt_str(cfg, "right_prefix") {
                sys = sys.right_prefix(prefix);
            }
            if let Some(map) = schemas
                && let (Some(ls), Some(rs)) = (map.get(left), map.get(right))
            {
                let out_schema = sys.output_schema(ls, rs)?;
                register_if_absent(&mut pipeline, left, ls.clone())?;
                register_if_absent(&mut pipeline, right, rs.clone())?;
                register_if_absent(&mut pipeline, into, out_schema)?;
            }
            pipeline = pipeline.system(sys);
        }
        "interval_join" => {
            check_allowed_keys(
                cfg,
                &[
                    "on",
                    "time_field",
                    "left_time_field",
                    "right_time_field",
                    "lower_bound_ms",
                    "upper_bound_ms",
                    "allowed_lateness_ms",
                    "watermark_delay_ms",
                    "max_buffered_rows",
                    "right_prefix",
                ],
                &ctx,
            )?;
            let left = req_endpoint(spec.left.as_deref(), "left", &ctx)?;
            let right = req_endpoint(spec.right.as_deref(), "right", &ctx)?;
            let into = req_endpoint(spec.into.as_deref(), "into", &ctx)?;
            let keys = parse_join_keys(cfg, &ctx)?;
            let shared_ts = opt_str(cfg, "time_field");
            let left_ts = opt_str(cfg, "left_time_field")
                .or_else(|| shared_ts.clone())
                .ok_or_else(|| {
                    SaciError::configuration(format!(
                        "{ctx}: missing 'left_time_field' (or 'time_field')"
                    ))
                })?;
            let right_ts = opt_str(cfg, "right_time_field")
                .or(shared_ts)
                .ok_or_else(|| {
                    SaciError::configuration(format!(
                        "{ctx}: missing 'right_time_field' (or 'time_field')"
                    ))
                })?;
            let lower = req_i64(cfg, "lower_bound_ms", &ctx)?;
            let upper = req_i64(cfg, "upper_bound_ms", &ctx)?;
            let mut sys = IntervalJoinSystem::new(
                left, right, into, &left_ts, &right_ts, keys, lower, upper,
            )?;
            if let Some(lateness) = opt_i64(cfg, "allowed_lateness_ms", &ctx)?.or(opt_i64(
                cfg,
                "watermark_delay_ms",
                &ctx,
            )?) {
                sys = sys.allowed_lateness_ms(lateness);
            }
            if let Some(max_rows) = opt_usize(cfg, "max_buffered_rows", &ctx)? {
                sys = sys.max_buffered_rows(max_rows);
            }
            if let Some(prefix) = opt_str(cfg, "right_prefix") {
                sys = sys.right_prefix(prefix);
            }
            if let Some(map) = schemas
                && let (Some(ls), Some(rs)) = (map.get(left), map.get(right))
            {
                let out_schema = sys.output_schema(ls, rs)?;
                register_if_absent(&mut pipeline, left, ls.clone())?;
                register_if_absent(&mut pipeline, right, rs.clone())?;
                register_if_absent(&mut pipeline, into, out_schema)?;
            }
            pipeline = pipeline.state::<IntervalJoinState>().system(sys);
        }
        "lookup_join" => {
            check_allowed_keys(cfg, &["on", "join_type", "right_prefix"], &ctx)?;
            let left = req_endpoint(spec.left.as_deref(), "left", &ctx)?;
            let right = req_endpoint(spec.right.as_deref(), "right", &ctx)?;
            let into = req_endpoint(spec.into.as_deref(), "into", &ctx)?;
            let keys = parse_join_keys(cfg, &ctx)?;
            let kind = parse_join_kind(opt_str(cfg, "join_type").as_deref(), &ctx)?;
            let mut sys = LookupJoinSystem::new(left, right, into, kind, keys)?;
            if let Some(prefix) = opt_str(cfg, "right_prefix") {
                sys = sys.right_prefix(prefix);
            }
            if let Some(map) = schemas
                && let (Some(ls), Some(rs)) = (map.get(left), map.get(right))
            {
                let out_schema = sys.output_schema(ls, rs)?;
                register_if_absent(&mut pipeline, left, ls.clone())?;
                register_if_absent(&mut pipeline, right, rs.clone())?;
                register_if_absent(&mut pipeline, into, out_schema)?;
            }
            pipeline = pipeline.state::<LookupJoinState>().system(sys);
        }
        "deduplicate" => {
            check_allowed_keys(
                cfg,
                &[
                    "keys",
                    "keep",
                    "time_field",
                    "ttl_ms",
                    "max_keys",
                    "stateful",
                ],
                &ctx,
            )?;
            let src = req_endpoint(spec.component.as_deref(), "component", &ctx)?;
            let dst = spec.into.as_deref().unwrap_or(src);
            let keys = opt_ident_list(cfg, "keys");
            let mut sys = DeduplicateSystem::new(src, dst, keys)?;
            if let Some(keep_raw) = opt_str(cfg, "keep") {
                let keep = match keep_raw.to_ascii_lowercase().as_str() {
                    "first" => KeepPolicy::First,
                    "last" => KeepPolicy::Last,
                    _ => {
                        return Err(SaciError::configuration(format!(
                            "{ctx}: invalid keep '{keep_raw}' (expected first or last)"
                        )));
                    }
                };
                sys = sys.keep(keep);
            }
            if let Some(tf) = opt_str(cfg, "time_field") {
                sys = sys.time_field(tf);
            }
            if let Some(ttl) = opt_i64(cfg, "ttl_ms", &ctx)? {
                sys = sys.ttl_ms(ttl);
            }
            if let Some(max_k) = opt_usize(cfg, "max_keys", &ctx)? {
                sys = sys.max_keys(max_k);
            }
            let stateful = opt_bool(cfg, "stateful", &ctx)?.unwrap_or(true);
            sys = sys.stateful(stateful);
            if let Some(in_schema) = schemas.and_then(|m| m.get(src)) {
                register_if_absent(&mut pipeline, src, in_schema.clone())?;
                if dst != src {
                    register_if_absent(&mut pipeline, dst, in_schema.clone())?;
                }
            }
            if stateful {
                pipeline = pipeline.state::<DeduplicateState>();
            }
            pipeline = pipeline.system(sys);
        }
        "sql" => {
            check_allowed_keys(cfg, &["query"], &ctx)?;
            let dst = spec
                .into
                .as_deref()
                .or(spec.component.as_deref())
                .ok_or_else(|| {
                    SaciError::configuration(format!("{ctx}: sql operator requires 'into'"))
                })?;
            let query = req_str(cfg, "query", &ctx)?;
            let sys = SqlSystem::parse(dst, &query)?;
            if let Some(map) = schemas {
                let mut source_pairs: Vec<(&str, &Schema)> = Vec::new();
                for &src in sys.source_components() {
                    if let Some(s) = map.get(src) {
                        source_pairs.push((src, s.as_ref()));
                        register_if_absent(&mut pipeline, src, s.clone())?;
                    }
                }
                if source_pairs.len() == sys.source_components().len() {
                    let out_schema = sys.infer_output_schema(&source_pairs)?;
                    if !sys.source_components().contains(&dst) {
                        register_if_absent(&mut pipeline, dst, out_schema)?;
                    } else {
                        pipeline.data_mut().set_output_schema(dst, out_schema);
                    }
                }
            }
            pipeline = pipeline.system(sys);
        }
        "quality" | "assert" => {
            pipeline = build_quality_operator_pipeline(pipeline, spec, cfg, &ctx, schemas)?;
        }
        other => {
            return Err(SaciError::configuration(format!(
                "{ctx}: unsupported operator kind '{other}'"
            )));
        }
    }

    Ok(pipeline)
}

fn build_window_operator_pipeline(
    #[cfg_attr(not(feature = "windows"), allow(unused_mut))] mut pipeline: Pipeline,
    spec: &OperatorSpec,
    cfg: &crate::config::ConfigMap,
    ctx: &str,
    #[cfg_attr(not(feature = "windows"), allow(unused_variables))] schemas: Option<
        &HashMap<String, Arc<Schema>>,
    >,
) -> SaciResult<Pipeline> {
    check_allowed_keys(
        cfg,
        &[
            "keys",
            "aggregates",
            "aggregate",
            "window_id_field",
            "window_start_field",
            "window_end_field",
            "emit_bounds",
        ],
        ctx,
    )?;
    let win = spec
        .window
        .as_ref()
        .ok_or_else(|| SaciError::configuration(format!("{ctx}: missing 'window' block")))?;
    let src = req_endpoint(spec.component.as_deref(), "component", ctx)?;
    let dst = spec.into.as_deref().unwrap_or(src);
    let mut keys = win.key_fields.clone();
    for extra in opt_ident_list(cfg, "keys") {
        if !keys.contains(&extra) {
            keys.push(extra);
        }
    }
    let aggs = parse_aggregates(cfg, ctx)?;
    let emit_bounds = opt_bool(cfg, "emit_bounds", ctx)?.unwrap_or(true);
    let window_id_field = opt_str(cfg, "window_id_field");
    let window_start_field = if emit_bounds {
        Some(opt_str(cfg, "window_start_field").unwrap_or_else(|| "window_start_ms".to_string()))
    } else {
        opt_str(cfg, "window_start_field")
    };
    let window_end_field = if emit_bounds {
        Some(opt_str(cfg, "window_end_field").unwrap_or_else(|| "window_end_ms".to_string()))
    } else {
        opt_str(cfg, "window_end_field")
    };

    #[cfg(feature = "windows")]
    {
        let sys =
            WindowAggregateSystem::new(src, dst, &win.time_field, win.spec.clone(), keys, aggs)?
                .allowed_lateness_ms(win.allowed_lateness_ms)
                .window_columns(WindowColumns {
                    window_id_field,
                    window_start_field,
                    window_end_field,
                });
        if let Some(in_schema) = schemas.and_then(|m| m.get(src)) {
            let out_schema = sys.output_schema(in_schema)?;
            register_if_absent(&mut pipeline, src, in_schema.clone())?;
            if dst != src {
                register_if_absent(&mut pipeline, dst, out_schema)?;
            } else {
                pipeline.data_mut().set_output_schema(dst, out_schema);
            }
        }
        pipeline = pipeline.state::<WindowAggregateState>().system(sys);
    }
    #[cfg(not(feature = "windows"))]
    let _ = (
        win,
        src,
        dst,
        keys,
        aggs,
        window_id_field,
        window_start_field,
        window_end_field,
    );

    Ok(pipeline)
}

fn parse_branch_rules(cfg: &crate::config::ConfigMap, ctx: &str) -> SaciResult<Vec<BranchRule>> {
    let val = cfg
        .get("branches")
        .ok_or_else(|| SaciError::configuration(format!("{ctx}: missing 'branches' in config")))?;
    let mut rules = Vec::new();
    match val {
        ConfigValue::Object(map) => {
            if map.len() == 1
                && let Some(nested) = map.get("branch")
            {
                let items: &[ConfigValue] = match nested {
                    ConfigValue::Array(arr) => arr.as_slice(),
                    single => std::slice::from_ref(single),
                };
                for item in items {
                    rules.push(parse_branch_rule_object(item, ctx)?);
                }
            } else {
                for (name, expr_val) in map {
                    let expr = match expr_val {
                        ConfigValue::String(s) if !s.is_empty() => s.as_str(),
                        other => {
                            return Err(SaciError::configuration(format!(
                                "{ctx}: branch '{name}' predicate must be a non-empty string, got {other}"
                            )));
                        }
                    };
                    rules.push(BranchRule::parse(name, expr)?);
                }
            }
        }
        ConfigValue::Array(arr) => {
            for item in arr {
                rules.push(parse_branch_rule_object(item, ctx)?);
            }
        }
        other => {
            return Err(SaciError::configuration(format!(
                "{ctx}: invalid 'branches' value {other}"
            )));
        }
    }
    if rules.is_empty() {
        return Err(SaciError::configuration(format!(
            "{ctx}: 'branches' must declare at least one rule"
        )));
    }
    Ok(rules)
}

fn parse_branch_rule_object(val: &ConfigValue, ctx: &str) -> SaciResult<BranchRule> {
    let obj = val.as_object().ok_or_else(|| {
        SaciError::configuration(format!("{ctx}: branch rule entry must be an object"))
    })?;
    let name = opt_str(obj, "id")
        .or_else(|| opt_str(obj, "name"))
        .or_else(|| opt_str(obj, "branch"))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            SaciError::configuration(format!("{ctx}: branch rule missing 'name' or 'branch'"))
        })?;
    let expr = opt_str(obj, "where")
        .or_else(|| opt_str(obj, "predicate"))
        .or_else(|| opt_str(obj, "when"))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            SaciError::configuration(format!("{ctx}: branch '{name}' missing 'where' predicate"))
        })?;
    BranchRule::parse(name, &expr)
}

fn build_quality_operator_pipeline(
    mut pipeline: Pipeline,
    spec: &OperatorSpec,
    cfg: &crate::config::ConfigMap,
    ctx: &str,
    schemas: Option<&HashMap<String, Arc<Schema>>>,
) -> SaciResult<Pipeline> {
    check_allowed_keys(
        cfg,
        &["rules", "on_violation", "quarantine_into", "diagnostics"],
        ctx,
    )?;
    let src = req_endpoint(spec.component.as_deref(), "component", ctx)?;
    let dst = spec.into.as_deref().unwrap_or(src);
    let rules = parse_quality_rules(cfg, ctx)?;
    let stateful = rules
        .iter()
        .any(|r| matches!(r, QualityRule::Unique { stateful: true, .. }));
    let mut sys = DataQualitySystem::new(src, rules)?;
    if dst != src {
        sys = sys.into_component(dst);
    }
    if let Some(q_into) = opt_str(cfg, "quarantine_into") {
        sys = sys.quarantine_into(&q_into);
    }
    if let Some(action_raw) = opt_str(cfg, "on_violation") {
        sys = sys.on_violation(QualityFailureAction::from_name(&action_raw)?);
    }
    if let Some(diag) = opt_bool(cfg, "diagnostics", ctx)? {
        sys = sys.include_diagnostics(diag);
    }
    if let Some(in_schema) = schemas.and_then(|m| m.get(src)) {
        register_if_absent(&mut pipeline, src, in_schema.clone())?;
        if dst != src {
            register_if_absent(&mut pipeline, dst, in_schema.clone())?;
        }
        if let Some(q_comp) = sys.quarantine_component() {
            let q_schema = sys.quarantine_schema(in_schema);
            register_if_absent(&mut pipeline, q_comp, q_schema)?;
        }
    }
    if stateful {
        pipeline = pipeline.state::<DataQualityState>();
    }
    Ok(pipeline.system(sys))
}

fn parse_quality_rules(cfg: &crate::config::ConfigMap, ctx: &str) -> SaciResult<Vec<QualityRule>> {
    let val = cfg
        .get("rules")
        .ok_or_else(|| SaciError::configuration(format!("{ctx}: missing 'rules' in config")))?;
    let mut raw_rules = Vec::new();
    match val {
        ConfigValue::Object(map) => {
            if let Some(nested) = map.get("rule") {
                match nested {
                    ConfigValue::Array(arr) => raw_rules.extend(arr.iter().cloned()),
                    single => raw_rules.push(single.clone()),
                }
            } else {
                raw_rules.push(ConfigValue::Object(map.clone()));
            }
        }
        ConfigValue::Array(arr) => raw_rules.extend(arr.iter().cloned()),
        other => {
            return Err(SaciError::configuration(format!(
                "{ctx}: invalid 'rules' value {other}"
            )));
        }
    }
    if raw_rules.is_empty() {
        return Err(SaciError::configuration(format!(
            "{ctx}: 'rules' must declare at least one rule"
        )));
    }
    raw_rules
        .iter()
        .enumerate()
        .map(|(idx, item)| parse_single_quality_rule(item, idx, ctx))
        .collect()
}

fn parse_single_quality_rule(val: &ConfigValue, idx: usize, ctx: &str) -> SaciResult<QualityRule> {
    let obj = val.as_object().ok_or_else(|| {
        SaciError::configuration(format!("{ctx}: quality rule entry must be an object"))
    })?;
    let kind = opt_str(obj, "type")
        .or_else(|| opt_str(obj, "kind"))
        .ok_or_else(|| {
            SaciError::configuration(format!("{ctx}: quality rule #{idx} missing 'type'"))
        })?;
    let default_name = || format!("{kind}_{idx}");
    let name = opt_str(obj, "id")
        .or_else(|| opt_str(obj, "name"))
        .unwrap_or_else(default_name);

    match kind.trim().to_ascii_lowercase().as_str() {
        "not_null" => Ok(QualityRule::NotNull {
            name,
            column: req_str(obj, "column", ctx)?,
        }),
        "not_empty" => Ok(QualityRule::NotEmpty {
            name,
            column: req_str(obj, "column", ctx)?,
        }),
        "regex" => Ok(QualityRule::Regex {
            name,
            column: req_str(obj, "column", ctx)?,
            pattern: req_str(obj, "pattern", ctx)?,
            case_insensitive: opt_bool(obj, "case_insensitive", ctx)?.unwrap_or(false),
        }),
        "range" => {
            let column = req_str(obj, "column", ctx)?;
            let min = opt_f64(obj, "min", ctx)?;
            let max = opt_f64(obj, "max", ctx)?;
            let min_exclusive = opt_bool(obj, "min_exclusive", ctx)?.unwrap_or(false);
            let max_exclusive = opt_bool(obj, "max_exclusive", ctx)?.unwrap_or(false);
            Ok(QualityRule::Range {
                name,
                column,
                min,
                max,
                min_exclusive,
                max_exclusive,
            })
        }
        "in_set" => {
            let column = req_str(obj, "column", ctx)?;
            let values = match obj.get("values") {
                Some(ConfigValue::Array(arr)) => arr
                    .iter()
                    .map(config_scalar_to_string)
                    .collect::<SaciResult<Vec<_>>>()?,
                Some(single) => vec![config_scalar_to_string(single)?],
                None => {
                    return Err(SaciError::configuration(format!(
                        "{ctx}: in_set rule '{name}' missing 'values'"
                    )));
                }
            };
            Ok(QualityRule::InSet {
                name,
                column,
                values,
            })
        }
        "unique" => {
            let mut columns = opt_ident_list(obj, "columns");
            if columns.is_empty()
                && let Some(col) = opt_str(obj, "column")
            {
                columns.push(col);
            }
            if columns.is_empty() {
                return Err(SaciError::configuration(format!(
                    "{ctx}: unique rule '{name}' requires 'column' or 'columns'"
                )));
            }
            let stateful = opt_bool(obj, "stateful", ctx)?.unwrap_or(false);
            Ok(QualityRule::Unique {
                name,
                columns,
                stateful,
            })
        }
        "check" => {
            let expr = opt_str(obj, "expr")
                .or_else(|| opt_str(obj, "where"))
                .or_else(|| opt_str(obj, "predicate"))
                .ok_or_else(|| {
                    SaciError::configuration(format!(
                        "{ctx}: check rule '{name}' missing 'expr' / 'where'"
                    ))
                })?;
            Ok(QualityRule::Check { name, expr })
        }
        other => Err(SaciError::configuration(format!(
            "{ctx}: unknown quality rule type '{other}'"
        ))),
    }
}

fn opt_f64(cfg: &crate::config::ConfigMap, key: &str, ctx: &str) -> SaciResult<Option<f64>> {
    match cfg.get(key) {
        None => Ok(None),
        Some(ConfigValue::Number(n)) => n
            .as_f64()
            .map(Some)
            .ok_or_else(|| SaciError::configuration(format!("{ctx}: '{key}' must be finite"))),
        Some(other) => Err(SaciError::configuration(format!(
            "{ctx}: '{key}' must be numeric, got {other}"
        ))),
    }
}

fn config_scalar_to_string(val: &ConfigValue) -> SaciResult<String> {
    match val {
        ConfigValue::String(s) => Ok(s.clone()),
        ConfigValue::Number(n) => Ok(n.to_string()),
        ConfigValue::Bool(b) => Ok(b.to_string()),
        other => Err(SaciError::configuration(format!(
            "expected scalar value in quality rule, got {other}"
        ))),
    }
}

fn check_allowed_keys(
    cfg: &crate::config::ConfigMap,
    allowed: &[&str],
    ctx: &str,
) -> SaciResult<()> {
    for key in cfg.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(SaciError::configuration(format!(
                "{ctx}: unknown config key '{key}' (expected one of: {})",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

fn register_if_absent(
    pipeline: &mut Pipeline,
    component: &str,
    schema: Arc<Schema>,
) -> SaciResult<()> {
    if !pipeline.data().schemas().contains(component) {
        pipeline
            .data_mut()
            .register_schema(intern_component_name(component), schema)?;
    }
    Ok(())
}

fn collect_known_schemas(
    workflow: &WorkflowSpec,
    built_nodes: &[BuiltNode],
) -> SaciResult<HashMap<String, Arc<Schema>>> {
    let mut map = HashMap::new();
    for s in &workflow.schemas {
        let mut table = crate::config::ConfigMap::new();
        table.insert("field".to_string(), s.field.clone());
        let val = ConfigValue::Object(table);
        let fields = parse_fields(
            &val,
            &format!("workflow '{}' schema '{}'", workflow.id, s.id),
        )?;
        map.insert(s.id.clone(), schema_of(&fields)?);
    }
    for node in built_nodes {
        match &node.kind {
            BuiltNodeKind::Source(src) => {
                if let Some(comp) = node.component {
                    map.insert(comp.to_string(), src.schema());
                }
            }
            BuiltNodeKind::Processor { runtime, .. } => {
                let tmpl = runtime.template_dataset();
                for (comp, entry) in tmpl.schemas().iter() {
                    let schema = tmpl
                        .output_schema(comp)
                        .unwrap_or_else(|| entry.schema.clone());
                    map.insert((*comp).to_string(), schema);
                }
            }
            BuiltNodeKind::Sink(_) => {}
        }
    }
    Ok(map)
}

fn config_map<'a>(val: &'a ConfigValue, ctx: &str) -> SaciResult<&'a crate::config::ConfigMap> {
    match val {
        ConfigValue::Object(map) => Ok(map),
        _ => Err(SaciError::configuration(format!(
            "{ctx}: expected config object"
        ))),
    }
}

fn req_endpoint<'a>(val: Option<&'a str>, field: &str, ctx: &str) -> SaciResult<&'a str> {
    val.filter(|s| !s.is_empty())
        .ok_or_else(|| SaciError::configuration(format!("{ctx}: missing '{field}'")))
}

fn opt_str(cfg: &crate::config::ConfigMap, key: &str) -> Option<String> {
    cfg.get(key).and_then(|v| match v {
        ConfigValue::String(s) => Some(s.clone()),
        _ => None,
    })
}

fn req_str(cfg: &crate::config::ConfigMap, key: &str, ctx: &str) -> SaciResult<String> {
    match cfg.get(key) {
        Some(ConfigValue::String(s)) if !s.is_empty() => Ok(s.clone()),
        _ => Err(SaciError::configuration(format!(
            "{ctx}: missing non-empty string '{key}' in config"
        ))),
    }
}

fn opt_bool(cfg: &crate::config::ConfigMap, key: &str, ctx: &str) -> SaciResult<Option<bool>> {
    match cfg.get(key) {
        None => Ok(None),
        Some(ConfigValue::Bool(b)) => Ok(Some(*b)),
        Some(other) => Err(SaciError::configuration(format!(
            "{ctx}: '{key}' must be boolean, got {other}"
        ))),
    }
}

fn opt_i64(cfg: &crate::config::ConfigMap, key: &str, ctx: &str) -> SaciResult<Option<i64>> {
    match cfg.get(key) {
        None => Ok(None),
        Some(ConfigValue::Number(n)) => n
            .as_i64()
            .ok_or_else(|| SaciError::configuration(format!("{ctx}: '{key}' must be integer")))
            .map(Some),
        Some(other) => Err(SaciError::configuration(format!(
            "{ctx}: '{key}' must be integer, got {other}"
        ))),
    }
}

fn req_i64(cfg: &crate::config::ConfigMap, key: &str, ctx: &str) -> SaciResult<i64> {
    opt_i64(cfg, key, ctx)?.ok_or_else(|| {
        SaciError::configuration(format!("{ctx}: missing integer '{key}' in config"))
    })
}

fn opt_usize(cfg: &crate::config::ConfigMap, key: &str, ctx: &str) -> SaciResult<Option<usize>> {
    match opt_i64(cfg, key, ctx)? {
        None => Ok(None),
        Some(v) if v >= 0 => Ok(Some(v as usize)),
        Some(v) => Err(SaciError::configuration(format!(
            "{ctx}: '{key}' must be non-negative, got {v}"
        ))),
    }
}

fn req_usize(cfg: &crate::config::ConfigMap, key: &str, ctx: &str) -> SaciResult<usize> {
    opt_usize(cfg, key, ctx)?.ok_or_else(|| {
        SaciError::configuration(format!("{ctx}: missing integer '{key}' in config"))
    })
}

fn opt_ident_list(cfg: &crate::config::ConfigMap, key: &str) -> Vec<String> {
    match cfg.get(key) {
        Some(ConfigValue::String(s)) => s
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(ToString::to_string)
            .collect(),
        Some(ConfigValue::Array(arr)) => arr
            .iter()
            .filter_map(|v| match v {
                ConfigValue::String(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn req_expr_list(cfg: &crate::config::ConfigMap, key: &str, ctx: &str) -> SaciResult<Vec<String>> {
    match cfg.get(key) {
        Some(ConfigValue::String(s)) => Ok(vec![s.clone()]),
        Some(ConfigValue::Array(arr)) if !arr.is_empty() => arr
            .iter()
            .map(|v| match v {
                ConfigValue::String(s) => Ok(s.clone()),
                other => Err(SaciError::configuration(format!(
                    "{ctx}: '{key}' element must be string, got {other}"
                ))),
            })
            .collect(),
        _ => Err(SaciError::configuration(format!(
            "{ctx}: missing '{key}' expression(s) in config"
        ))),
    }
}

fn parse_aggregates(cfg: &crate::config::ConfigMap, ctx: &str) -> SaciResult<Vec<AggregateSpec>> {
    let val = cfg
        .get("aggregates")
        .or_else(|| cfg.get("aggregate"))
        .ok_or_else(|| {
            SaciError::configuration(format!("{ctx}: missing 'aggregates' in config"))
        })?;
    let items: &[ConfigValue] = match val {
        ConfigValue::Array(arr) => arr.as_slice(),
        single => std::slice::from_ref(single),
    };
    let mut specs = Vec::with_capacity(items.len());
    for item in items {
        match item {
            ConfigValue::String(s) => specs.push(parse_aggregate_string(s, ctx)?),
            ConfigValue::Object(obj) => {
                let op_raw = req_str(obj, "op", ctx)?;
                let op = match op_raw.to_ascii_lowercase().as_str() {
                    "sum" => AggregateOp::Sum,
                    "min" => AggregateOp::Min,
                    "max" => AggregateOp::Max,
                    "count" => AggregateOp::Count,
                    "count_all" => AggregateOp::CountAll,
                    "mean" | "avg" => AggregateOp::Mean,
                    "count_distinct" => AggregateOp::CountDistinct,
                    "variance" | "var" => AggregateOp::Variance,
                    "stddev" | "std" => AggregateOp::StdDev,
                    "first" => AggregateOp::First,
                    "last" => AggregateOp::Last,
                    "list_agg" | "array_agg" => AggregateOp::ListAgg,
                    "approx_percentile" => {
                        let q = obj
                            .get("quantile")
                            .and_then(ConfigValue::as_f64)
                            .ok_or_else(|| {
                                SaciError::configuration(format!(
                                    "{ctx}: approx_percentile requires numeric 'quantile'"
                                ))
                            })?;
                        AggregateOp::ApproxPercentile(q)
                    }
                    "string_agg" => AggregateOp::StringAgg {
                        separator: opt_str(obj, "separator").unwrap_or_else(|| ",".to_string()),
                    },
                    _ => {
                        return Err(SaciError::configuration(format!(
                            "{ctx}: unknown aggregate op '{op_raw}'"
                        )));
                    }
                };
                let mut spec = if op == AggregateOp::CountAll {
                    AggregateSpec::count_all()
                } else {
                    AggregateSpec::new(req_str(obj, "field", ctx)?, op)
                };
                if let Some(alias) = opt_str(obj, "alias") {
                    spec = spec.alias(alias);
                }
                specs.push(spec);
            }
            other => {
                return Err(SaciError::configuration(format!(
                    "{ctx}: invalid aggregate entry {other}"
                )));
            }
        }
    }
    Ok(specs)
}

fn parse_aggregate_string(raw: &str, ctx: &str) -> SaciResult<AggregateSpec> {
    let trimmed = raw.trim();
    let (expr_part, alias) = if let Some(idx) = trimmed.to_ascii_uppercase().rfind(" AS ") {
        (
            trimmed[..idx].trim(),
            Some(trimmed[idx + 4..].trim().to_string()),
        )
    } else {
        (trimmed, None)
    };
    let open = expr_part
        .find('(')
        .filter(|_| expr_part.ends_with(')'))
        .ok_or_else(|| {
            SaciError::configuration(format!(
                "{ctx}: invalid aggregate string '{raw}' (expected 'op(field) [AS alias]')"
            ))
        })?;
    let fn_name = expr_part[..open].trim().to_ascii_lowercase();
    let args_str = expr_part[open + 1..expr_part.len() - 1].trim();
    let mut spec = match fn_name.as_str() {
        "count" if args_str == "*" => AggregateSpec::count_all(),
        "count_distinct" => AggregateSpec::count_distinct(args_str),
        "count" => AggregateSpec::count(args_str),
        "sum" => AggregateSpec::sum(args_str),
        "min" => AggregateSpec::min(args_str),
        "max" => AggregateSpec::max(args_str),
        "mean" | "avg" => AggregateSpec::mean(args_str),
        "variance" | "var" => AggregateSpec::variance(args_str),
        "stddev" | "std" => AggregateSpec::stddev(args_str),
        "first" => AggregateSpec::first(args_str),
        "last" => AggregateSpec::last(args_str),
        "list_agg" | "array_agg" => AggregateSpec::list_agg(args_str),
        "approx_percentile" => {
            let (field, q_str) = args_str.split_once(',').ok_or_else(|| {
                SaciError::configuration(format!(
                    "{ctx}: approx_percentile expects 'approx_percentile(field, quantile)'"
                ))
            })?;
            let q: f64 = q_str.trim().parse().map_err(|_| {
                SaciError::configuration(format!("{ctx}: invalid quantile '{q_str}'"))
            })?;
            AggregateSpec::approx_percentile(field.trim(), q)
        }
        "string_agg" => {
            if let Some((field, sep)) = args_str.split_once(',') {
                let sep_clean = sep.trim().trim_matches('\'').trim_matches('"');
                AggregateSpec::string_agg(field.trim(), sep_clean)
            } else {
                AggregateSpec::string_agg(args_str, ",")
            }
        }
        _ => {
            return Err(SaciError::configuration(format!(
                "{ctx}: unknown aggregate function '{fn_name}'"
            )));
        }
    };
    if let Some(a) = alias {
        spec = spec.alias(a);
    }
    Ok(spec)
}

fn parse_sort_keys(cfg: &crate::config::ConfigMap, ctx: &str) -> SaciResult<Vec<SortKey>> {
    let val = cfg
        .get("order_by")
        .ok_or_else(|| SaciError::configuration(format!("{ctx}: missing 'order_by' in config")))?;
    let mut keys = Vec::new();
    match val {
        ConfigValue::String(s) => {
            for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                keys.push(parse_single_sort_key(part, ctx)?);
            }
        }
        ConfigValue::Array(arr) => {
            for item in arr {
                match item {
                    ConfigValue::String(s) => keys.push(parse_single_sort_key(s, ctx)?),
                    ConfigValue::Object(obj) => {
                        let field = req_str(obj, "field", ctx)?;
                        let desc = opt_bool(obj, "descending", ctx)?.unwrap_or(false);
                        let mut k = if desc {
                            SortKey::desc(field)
                        } else {
                            SortKey::asc(field)
                        };
                        if let Some(nf) = opt_bool(obj, "nulls_first", ctx)? {
                            k = k.nulls_first(nf);
                        }
                        keys.push(k);
                    }
                    other => {
                        return Err(SaciError::configuration(format!(
                            "{ctx}: invalid order_by item {other}"
                        )));
                    }
                }
            }
        }
        other => {
            return Err(SaciError::configuration(format!(
                "{ctx}: invalid order_by {other}"
            )));
        }
    }
    Ok(keys)
}

fn parse_single_sort_key(raw: &str, ctx: &str) -> SaciResult<SortKey> {
    let parts: Vec<&str> = raw.split_whitespace().collect();
    let Some(&field) = parts.first() else {
        return Err(SaciError::configuration(format!("{ctx}: empty sort key")));
    };
    let mut key = SortKey::asc(field);
    let mut i = 1;
    while i < parts.len() {
        match parts[i].to_ascii_uppercase().as_str() {
            "ASC" => {
                key.descending = false;
                i += 1;
            }
            "DESC" => {
                key.descending = true;
                i += 1;
            }
            "NULLS" => {
                let dir = parts.get(i + 1).ok_or_else(|| {
                    SaciError::configuration(format!("{ctx}: expected FIRST/LAST after NULLS"))
                })?;
                if dir.eq_ignore_ascii_case("FIRST") {
                    key.nulls_first = true;
                } else if dir.eq_ignore_ascii_case("LAST") {
                    key.nulls_first = false;
                } else {
                    return Err(SaciError::configuration(format!(
                        "{ctx}: unexpected token '{dir}' after NULLS"
                    )));
                }
                i += 2;
            }
            other => {
                return Err(SaciError::configuration(format!(
                    "{ctx}: unexpected sort modifier '{other}'"
                )));
            }
        }
    }
    Ok(key)
}

fn parse_join_keys(cfg: &crate::config::ConfigMap, ctx: &str) -> SaciResult<Vec<JoinKey>> {
    let val = cfg.get("on").ok_or_else(|| {
        SaciError::configuration(format!("{ctx}: missing join key 'on' in config"))
    })?;
    let mut keys = Vec::new();
    match val {
        ConfigValue::String(s) => {
            for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                keys.push(parse_single_join_key(part));
            }
        }
        ConfigValue::Object(obj) => {
            keys.push(JoinKey::new(
                req_str(obj, "left", ctx)?,
                req_str(obj, "right", ctx)?,
            ));
        }
        ConfigValue::Array(arr) => {
            for item in arr {
                match item {
                    ConfigValue::String(s) => keys.push(parse_single_join_key(s)),
                    ConfigValue::Object(obj) => keys.push(JoinKey::new(
                        req_str(obj, "left", ctx)?,
                        req_str(obj, "right", ctx)?,
                    )),
                    other => {
                        return Err(SaciError::configuration(format!(
                            "{ctx}: invalid join key entry {other}"
                        )));
                    }
                }
            }
        }
        other => {
            return Err(SaciError::configuration(format!(
                "{ctx}: invalid join 'on' value {other}"
            )));
        }
    }
    Ok(keys)
}

fn parse_single_join_key(raw: &str) -> JoinKey {
    if let Some((l, r)) = raw.split_once('=') {
        JoinKey::new(l.trim(), r.trim())
    } else {
        JoinKey::same(raw.trim())
    }
}

fn parse_join_kind(raw: Option<&str>, ctx: &str) -> SaciResult<JoinKind> {
    match raw.unwrap_or("inner").to_ascii_lowercase().as_str() {
        "inner" => Ok(JoinKind::Inner),
        "left" => Ok(JoinKind::Left),
        "right" => Ok(JoinKind::Right),
        "full" => Ok(JoinKind::Full),
        "semi" => Ok(JoinKind::Semi),
        "anti" => Ok(JoinKind::Anti),
        other => Err(SaciError::configuration(format!(
            "{ctx}: unsupported join_type '{other}'"
        ))),
    }
}
