//! Declarative row-level data quality validation and quarantine routing ([`DataQualitySystem`]).
//!
//! Evaluates declarative quality rules (`not_null`, `not_empty`, `regex`,
//! `range`, `in_set`, `unique`, and arbitrary SQL `check` expressions) on each
//! batch and splits rows into valid records and offending quarantine records.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use regex::RegexBuilder;
use serde::{Deserialize, Serialize};

use crate::component::Component;
use crate::dataset::{Dataset, intern_component_name};
use crate::error::{SaciError, SaciResult};
use crate::sdk::{RouteDecision, State};
use crate::system::{System, SystemMeta};

use super::expr::Expr;
use super::hash::compute_row_keys;
use super::route::BranchPayloads;

/// Action taken when one or more rows violate a [`QualityRule`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QualityFailureAction {
    /// Emit valid rows on branch `"valid"` (and `target_component`) and
    /// offending rows on branch `"quarantine"` (and/or `quarantine_into`).
    #[default]
    Quarantine,
    /// Forward valid rows and silently drop offending rows.
    Drop,
    /// Fail the operator execution on the first offending row.
    Fail,
}

impl QualityFailureAction {
    /// Parse a failure action string (`"quarantine"`, `"drop"`, `"fail"`).
    pub fn from_name(raw: &str) -> SaciResult<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "quarantine" | "split" => Ok(Self::Quarantine),
            "drop" => Ok(Self::Drop),
            "fail" | "error" => Ok(Self::Fail),
            other => Err(SaciError::configuration(format!(
                "quality: unknown on_violation action '{other}'; expected quarantine, drop, or fail"
            ))),
        }
    }
}

/// Declarative per-row data quality assertion rule.
#[derive(Debug, Clone, PartialEq)]
pub enum QualityRule {
    /// Value in `column` must not be `NULL`.
    NotNull {
        /// Rule display name.
        name: String,
        /// Target column name.
        column: String,
    },
    /// Value in string column `column` must not be `NULL` or whitespace-only.
    NotEmpty {
        /// Rule display name.
        name: String,
        /// Target column name.
        column: String,
    },
    /// String value in `column` must match `pattern`. `NULL` fails the rule.
    Regex {
        /// Rule display name.
        name: String,
        /// Target column name.
        column: String,
        /// Regular expression pattern.
        pattern: String,
        /// Case-insensitive match when `true`.
        case_insensitive: bool,
    },
    /// Numeric value in `column` must satisfy `[min, max]` bounds. `NULL` fails the rule.
    Range {
        /// Rule display name.
        name: String,
        /// Target column name.
        column: String,
        /// Lower bound (if any).
        min: Option<f64>,
        /// Upper bound (if any).
        max: Option<f64>,
        /// Strict `>` bound instead of `>=`.
        min_exclusive: bool,
        /// Strict `<` bound instead of `<=`.
        max_exclusive: bool,
    },
    /// Value in `column` (converted to text) must belong to `values`. `NULL` fails the rule.
    InSet {
        /// Rule display name.
        name: String,
        /// Target column name.
        column: String,
        /// Permitted string representations.
        values: Vec<String>,
    },
    /// Tuple across `columns` must be unique within the batch (and across batches when `stateful`).
    Unique {
        /// Rule display name.
        name: String,
        /// Key columns forming the uniqueness constraint.
        columns: Vec<String>,
        /// Maintain seen keys in [`DataQualityState`] across passes.
        stateful: bool,
    },
    /// Arbitrary SQL boolean expression parsed by [`Expr::parse`]. Must evaluate to `true`.
    Check {
        /// Rule display name.
        name: String,
        /// SQL boolean predicate expression.
        expr: String,
    },
}

impl QualityRule {
    /// Name reported in diagnostics (`__dq_rule`) and errors.
    #[must_use]
    pub fn rule_name(&self) -> &str {
        match self {
            Self::NotNull { name, .. }
            | Self::NotEmpty { name, .. }
            | Self::Regex { name, .. }
            | Self::Range { name, .. }
            | Self::InSet { name, .. }
            | Self::Unique { name, .. }
            | Self::Check { name, .. } => name,
        }
    }

    fn column_label(&self) -> String {
        match self {
            Self::NotNull { column, .. }
            | Self::NotEmpty { column, .. }
            | Self::Regex { column, .. }
            | Self::Range { column, .. }
            | Self::InSet { column, .. } => column.clone(),
            Self::Unique { columns, .. } => columns.join(","),
            Self::Check { expr, .. } => expr.clone(),
        }
    }
}

/// Checkpointable cross-batch uniqueness state for stateful [`QualityRule::Unique`] rules.
///
/// Parallel vectors `(rules, keys)` record each seen `(rule_name, row_key)`
/// pair so the state serializes natively across WASM, native plugin, and
/// service checkpoints.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataQualityState {
    /// Rule name for each tracked unique key entry.
    pub rules: Vec<String>,
    /// Canonical row key for each tracked unique key entry.
    pub keys: Vec<String>,
    /// Transient O(1) lookup index rebuilt lazily after deserialization.
    #[serde(skip)]
    index: HashSet<(String, String)>,
}

impl PartialEq for DataQualityState {
    fn eq(&self, other: &Self) -> bool {
        self.rules == other.rules && self.keys == other.keys
    }
}

impl Eq for DataQualityState {}

impl DataQualityState {
    /// Clears all tracked uniqueness keys across every rule.
    pub fn clear(&mut self) {
        self.rules.clear();
        self.keys.clear();
        self.index.clear();
    }

    /// Number of unique keys tracked across all stateful rules.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns `true` if no keys are currently tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    fn ensure_index(&mut self) {
        if self.index.len() != self.keys.len() {
            self.index.clear();
            for (r, k) in self.rules.iter().zip(self.keys.iter()) {
                self.index.insert((r.clone(), k.clone()));
            }
        }
    }

    fn insert(&mut self, rule_name: &str, key: String) -> bool {
        self.ensure_index();
        if self.index.insert((rule_name.to_string(), key.clone())) {
            self.rules.push(rule_name.to_string());
            self.keys.push(key);
            true
        } else {
            false
        }
    }
}

impl Component for DataQualityState {
    fn name() -> &'static str {
        "DataQualityState"
    }

    fn schema() -> Arc<Schema> {
        use serde_arrow::schema::{SchemaLike as _, TracingOptions};
        let sample = DataQualityState {
            rules: vec!["r".to_string()],
            keys: vec!["k".to_string()],
            ..Self::default()
        };
        let fields =
            Vec::<arrow_schema::FieldRef>::from_samples(&[sample], TracingOptions::default())
                .expect("DataQualityState schema traces from sample");
        Arc::new(Schema::new(fields))
    }
}

/// Outcome of evaluating a [`DataQualitySystem`] over a single [`RecordBatch`].
#[derive(Debug, Clone)]
pub struct QualitySplit {
    /// Rows that passed all quality rules.
    pub valid: RecordBatch,
    /// Rows that failed at least one quality rule (with diagnostics attached if enabled).
    pub quarantine: RecordBatch,
}

#[derive(Debug, Clone)]
enum CompiledRule {
    Plain,
    Regex(regex::Regex),
    InSet(HashSet<String>),
    Check(Expr),
}

/// Arrow [`System`] that checks declarative [`QualityRule`]s per row and splits
/// valid records from offending records.
#[derive(Debug)]
pub struct DataQualitySystem {
    source_component: &'static str,
    target_component: &'static str,
    quarantine_component: Option<&'static str>,
    rules: Vec<QualityRule>,
    compiled: Vec<CompiledRule>,
    on_violation: QualityFailureAction,
    include_diagnostics: bool,
    fallback_state: Mutex<DataQualityState>,
}

impl Clone for DataQualitySystem {
    fn clone(&self) -> Self {
        let guard = self
            .fallback_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self {
            source_component: self.source_component,
            target_component: self.target_component,
            quarantine_component: self.quarantine_component,
            rules: self.rules.clone(),
            compiled: self.compiled.clone(),
            on_violation: self.on_violation,
            include_diagnostics: self.include_diagnostics,
            fallback_state: Mutex::new(guard.clone()),
        }
    }
}

impl DataQualitySystem {
    /// Construct a data quality validator for `component`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `rules` is empty or any regex rule
    /// or SQL check fails to compile.
    pub fn new(component: &str, rules: Vec<QualityRule>) -> SaciResult<Self> {
        if rules.is_empty() {
            return Err(SaciError::configuration(
                "DataQualitySystem requires at least one QualityRule",
            ));
        }
        let mut compiled = Vec::with_capacity(rules.len());
        for rule in &rules {
            compiled.push(compile_rule(rule)?);
        }
        let comp = intern_component_name(component);
        Ok(Self {
            source_component: comp,
            target_component: comp,
            quarantine_component: None,
            rules,
            compiled,
            on_violation: QualityFailureAction::Quarantine,
            include_diagnostics: false,
            fallback_state: Mutex::new(DataQualityState::default()),
        })
    }

    /// Write valid rows into `target` instead of mutating `source_component` in place.
    #[must_use]
    pub fn into_component(mut self, target: &str) -> Self {
        self.target_component = intern_component_name(target);
        self
    }

    /// Also write quarantined rows into `component` in [`Dataset`] (useful for
    /// unlabelled multi-sink fan-out).
    #[must_use]
    pub fn quarantine_into(mut self, component: &str) -> Self {
        self.quarantine_component = Some(intern_component_name(component));
        self
    }

    /// Configure action on offending rows (`Quarantine`, `Drop`, or `Fail`).
    #[must_use]
    pub fn on_violation(mut self, action: QualityFailureAction) -> Self {
        self.on_violation = action;
        self
    }

    /// Attach diagnostic columns (`__dq_rule`, `__dq_column`, `__dq_reason`) to
    /// quarantined rows when `enabled` is `true`.
    #[must_use]
    pub fn include_diagnostics(mut self, enabled: bool) -> Self {
        self.include_diagnostics = enabled;
        self
    }

    /// Whether quarantined rows carry `__dq_rule`, `__dq_column`, and `__dq_reason`.
    #[must_use]
    pub fn diagnostics_enabled(&self) -> bool {
        self.include_diagnostics
    }

    /// Configured violation action.
    #[must_use]
    pub fn action(&self) -> QualityFailureAction {
        self.on_violation
    }

    /// Quarantine output component, if configured.
    #[must_use]
    pub fn quarantine_component(&self) -> Option<&'static str> {
        self.quarantine_component
    }

    /// Compute the quarantine [`SchemaRef`] given `input_schema`.
    #[must_use]
    pub fn quarantine_schema(&self, input_schema: &Schema) -> SchemaRef {
        if !self.include_diagnostics {
            return Arc::new(input_schema.clone());
        }
        let mut fields = input_schema.fields().to_vec();
        fields.push(Arc::new(Field::new("__dq_rule", DataType::Utf8, false)));
        fields.push(Arc::new(Field::new("__dq_column", DataType::Utf8, false)));
        fields.push(Arc::new(Field::new("__dq_reason", DataType::Utf8, false)));
        Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ))
    }

    /// Evaluate every rule over `batch`, returning valid and quarantined slices.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError`] if a referenced column is absent or a rule fails
    /// to evaluate, or if `on_violation` is [`QualityFailureAction::Fail`] and
    /// at least one row violates a rule.
    pub fn evaluate_batch(
        &self,
        batch: &RecordBatch,
        state: Option<&mut DataQualityState>,
    ) -> SaciResult<QualitySplit> {
        let n = batch.num_rows();
        let q_schema = self.quarantine_schema(batch.schema_ref());
        if n == 0 {
            return Ok(QualitySplit {
                valid: batch.clone(),
                quarantine: RecordBatch::new_empty(q_schema),
            });
        }

        let mut row_failed = vec![false; n];
        let mut first_rule: Vec<Option<String>> = vec![None; n];
        let mut first_col: Vec<Option<String>> = vec![None; n];
        let mut first_reason: Vec<Option<String>> = vec![None; n];

        let mut state_opt = state;
        for (rule, compiled) in self.rules.iter().zip(self.compiled.iter()) {
            let state_ref = state_opt.as_deref_mut();
            let (pass_mask, reason_msg) = evaluate_rule(rule, compiled, batch, state_ref)?;
            for i in 0..n {
                if !pass_mask[i] && !row_failed[i] {
                    row_failed[i] = true;
                    first_rule[i] = Some(rule.rule_name().to_string());
                    first_col[i] = Some(rule.column_label());
                    first_reason[i] = Some(reason_msg.clone());
                }
            }
        }

        let failed_count = row_failed.iter().filter(|f| **f).count();
        if failed_count > 0 && self.on_violation == QualityFailureAction::Fail {
            let first_idx = row_failed.iter().position(|f| *f).unwrap_or(0);
            let r_name = first_rule[first_idx].as_deref().unwrap_or("unknown");
            let r_reason = first_reason[first_idx].as_deref().unwrap_or("violation");
            return Err(SaciError::generic(format!(
                "data quality rule '{r_name}' failed for {failed_count} row(s): {r_reason}"
            )));
        }

        let valid_bool: BooleanArray = row_failed.iter().map(|f| Some(!*f)).collect();
        let invalid_bool: BooleanArray = row_failed.iter().map(|f| Some(*f)).collect();

        let valid = filter_record_batch(batch, &valid_bool)
            .map_err(|e| SaciError::generic(format!("quality filter valid: {e}")))?;

        let raw_quarantine = if failed_count == 0 {
            RecordBatch::new_empty(batch.schema())
        } else {
            filter_record_batch(batch, &invalid_bool)
                .map_err(|e| SaciError::generic(format!("quality filter quarantine: {e}")))?
        };

        let quarantine = if !self.include_diagnostics {
            raw_quarantine
        } else {
            let mut rules_col = Vec::with_capacity(failed_count);
            let mut cols_col = Vec::with_capacity(failed_count);
            let mut reasons_col = Vec::with_capacity(failed_count);
            for i in 0..n {
                if row_failed[i] {
                    rules_col.push(first_rule[i].clone().unwrap_or_default());
                    cols_col.push(first_col[i].clone().unwrap_or_default());
                    reasons_col.push(first_reason[i].clone().unwrap_or_default());
                }
            }
            let mut cols = raw_quarantine.columns().to_vec();
            cols.push(Arc::new(StringArray::from(rules_col)));
            cols.push(Arc::new(StringArray::from(cols_col)));
            cols.push(Arc::new(StringArray::from(reasons_col)));
            RecordBatch::try_new(q_schema, cols).map_err(|e| {
                SaciError::generic(format!("quality attach diagnostics columns: {e}"))
            })?
        };

        Ok(QualitySplit { valid, quarantine })
    }

    fn run_impl(&self, data: &mut Dataset) -> SaciResult<()> {
        let batch = match data.batch_named(self.source_component) {
            Ok(b) => b.clone(),
            Err(_) => {
                if self.on_violation == QualityFailureAction::Quarantine {
                    let _ = data.take_resource::<RouteDecision>();
                    let _ = data.take_resource::<BranchPayloads>();
                }
                return Ok(());
            }
        };

        let split = if let Some(state_res) = data.get_resource_mut::<State<DataQualityState>>() {
            if state_res.rows.is_empty() {
                state_res.rows.push(DataQualityState::default());
            }
            self.evaluate_batch(&batch, Some(&mut state_res.rows[0]))?
        } else {
            let mut guard = self
                .fallback_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.evaluate_batch(&batch, Some(&mut guard))?
        };

        data.put_batch_named(self.target_component, split.valid.clone())?;

        if let Some(q_comp) = self.quarantine_component {
            data.put_batch_named(q_comp, split.quarantine.clone())?;
        }

        if self.on_violation == QualityFailureAction::Quarantine {
            let mut selected = Vec::new();
            let mut branches = std::collections::HashMap::new();
            if split.valid.num_rows() > 0 {
                selected.push("valid".to_string());
                branches.insert("valid".to_string(), split.valid);
            }
            if split.quarantine.num_rows() > 0 {
                selected.push("quarantine".to_string());
                branches.insert("quarantine".to_string(), split.quarantine);
            }
            data.insert_resource(RouteDecision::to(selected));
            data.insert_resource(BranchPayloads {
                component: self.target_component,
                batches: branches,
            });
        }

        Ok(())
    }
}

fn compile_rule(rule: &QualityRule) -> SaciResult<CompiledRule> {
    if rule.rule_name().trim().is_empty() {
        return Err(SaciError::configuration(
            "quality rule name must not be empty",
        ));
    }
    match rule {
        QualityRule::NotNull { column, .. } | QualityRule::NotEmpty { column, .. } => {
            if column.trim().is_empty() {
                return Err(SaciError::configuration(format!(
                    "quality rule '{}': column name must not be empty",
                    rule.rule_name()
                )));
            }
            Ok(CompiledRule::Plain)
        }
        QualityRule::Regex {
            column,
            pattern,
            case_insensitive,
            ..
        } => {
            if column.trim().is_empty() {
                return Err(SaciError::configuration(format!(
                    "quality rule '{}': column name must not be empty",
                    rule.rule_name()
                )));
            }
            let re = RegexBuilder::new(pattern)
                .case_insensitive(*case_insensitive)
                .build()
                .map_err(|e| {
                    SaciError::configuration(format!(
                        "quality rule '{}': invalid regex '{pattern}': {e}",
                        rule.rule_name()
                    ))
                })?;
            Ok(CompiledRule::Regex(re))
        }
        QualityRule::Range {
            column, min, max, ..
        } => {
            if column.trim().is_empty() {
                return Err(SaciError::configuration(format!(
                    "quality rule '{}': column name must not be empty",
                    rule.rule_name()
                )));
            }
            if min.is_none() && max.is_none() {
                return Err(SaciError::configuration(format!(
                    "quality rule '{}': range requires at least one of 'min' or 'max'",
                    rule.rule_name()
                )));
            }
            Ok(CompiledRule::Plain)
        }
        QualityRule::InSet { column, values, .. } => {
            if column.trim().is_empty() || values.is_empty() {
                return Err(SaciError::configuration(format!(
                    "quality rule '{}': in_set requires non-empty column and values",
                    rule.rule_name()
                )));
            }
            Ok(CompiledRule::InSet(values.iter().cloned().collect()))
        }
        QualityRule::Unique { columns, .. } => {
            if columns.is_empty() || columns.iter().any(|c| c.trim().is_empty()) {
                return Err(SaciError::configuration(format!(
                    "quality rule '{}': unique requires at least one column",
                    rule.rule_name()
                )));
            }
            Ok(CompiledRule::Plain)
        }
        QualityRule::Check { expr, .. } => {
            let parsed = Expr::parse(expr).map_err(|e| {
                SaciError::configuration(format!(
                    "quality rule '{}': invalid SQL check expression '{expr}': {}",
                    rule.rule_name(),
                    e.message()
                ))
            })?;
            Ok(CompiledRule::Check(parsed))
        }
    }
}

fn evaluate_rule(
    rule: &QualityRule,
    compiled: &CompiledRule,
    batch: &RecordBatch,
    state: Option<&mut DataQualityState>,
) -> SaciResult<(Vec<bool>, String)> {
    let n = batch.num_rows();
    match rule {
        QualityRule::NotNull { column, .. } => {
            let col = batch.column_by_name(column).ok_or_else(|| {
                SaciError::configuration(format!("quality: column '{column}' not found in batch"))
            })?;
            let mask = (0..n).map(|i| !col.is_null(i)).collect();
            Ok((mask, format!("column '{column}' is null")))
        }
        QualityRule::NotEmpty { column, .. } => {
            let col = batch.column_by_name(column).ok_or_else(|| {
                SaciError::configuration(format!("quality: column '{column}' not found in batch"))
            })?;
            let utf8 = cast_to_utf8(col)?;
            let s_arr = utf8
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| SaciError::generic("quality not_empty: failed Utf8 downcast"))?;
            let mask = (0..n)
                .map(|i| !s_arr.is_null(i) && !s_arr.value(i).trim().is_empty())
                .collect();
            Ok((mask, format!("column '{column}' is null or empty")))
        }
        QualityRule::Regex {
            column, pattern, ..
        } => {
            let col = batch.column_by_name(column).ok_or_else(|| {
                SaciError::configuration(format!("quality: column '{column}' not found in batch"))
            })?;
            let CompiledRule::Regex(re) = compiled else {
                unreachable!("Regex rule always compiles to CompiledRule::Regex")
            };
            let utf8 = cast_to_utf8(col)?;
            let s_arr = utf8
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| SaciError::generic("quality regex: failed Utf8 downcast"))?;
            let mask = (0..n)
                .map(|i| !s_arr.is_null(i) && re.is_match(s_arr.value(i)))
                .collect();
            Ok((mask, format!("column '{column}' failed regex '{pattern}'")))
        }
        QualityRule::Range {
            column,
            min,
            max,
            min_exclusive,
            max_exclusive,
            ..
        } => {
            let col = batch.column_by_name(column).ok_or_else(|| {
                SaciError::configuration(format!("quality: column '{column}' not found in batch"))
            })?;
            let floats = cast_with_options(
                col,
                &DataType::Float64,
                &CastOptions {
                    safe: true,
                    ..Default::default()
                },
            )
            .map_err(|e| {
                SaciError::generic(format!("quality range: cast '{column}' to Float64: {e}"))
            })?;
            let f_arr = floats
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| SaciError::generic("quality range: failed Float64 downcast"))?;
            let mask = (0..n)
                .map(|i| {
                    if f_arr.is_null(i) {
                        return false;
                    }
                    let v = f_arr.value(i);
                    let min_ok = match min {
                        None => true,
                        Some(m) => {
                            if *min_exclusive {
                                v > *m
                            } else {
                                v >= *m
                            }
                        }
                    };
                    let max_ok = match max {
                        None => true,
                        Some(m) => {
                            if *max_exclusive {
                                v < *m
                            } else {
                                v <= *m
                            }
                        }
                    };
                    min_ok && max_ok
                })
                .collect();
            Ok((mask, format!("column '{column}' outside permitted range")))
        }
        QualityRule::InSet { column, values, .. } => {
            let col = batch.column_by_name(column).ok_or_else(|| {
                SaciError::configuration(format!("quality: column '{column}' not found in batch"))
            })?;
            let CompiledRule::InSet(allowed) = compiled else {
                unreachable!("InSet rule always compiles to CompiledRule::InSet")
            };
            let utf8 = cast_to_utf8(col)?;
            let s_arr = utf8
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| SaciError::generic("quality in_set: failed Utf8 downcast"))?;
            let mask = (0..n)
                .map(|i| !s_arr.is_null(i) && allowed.contains(s_arr.value(i)))
                .collect();
            Ok((
                mask,
                format!(
                    "column '{column}' not in allowed set [{}]",
                    values.join(", ")
                ),
            ))
        }
        QualityRule::Unique {
            name,
            columns,
            stateful,
        } => {
            let key_cols: Vec<&ArrayRef> = columns
                .iter()
                .map(|c| {
                    batch.column_by_name(c).ok_or_else(|| {
                        SaciError::configuration(format!("quality unique: column '{c}' not found"))
                    })
                })
                .collect::<SaciResult<Vec<_>>>()?;
            let row_keys = compute_row_keys(&key_cols)?;
            let mut local_seen = HashSet::new();
            let mut mask = Vec::with_capacity(n);
            match (*stateful, state) {
                (true, Some(st)) => {
                    for (_hash, key) in row_keys {
                        mask.push(st.insert(name, key));
                    }
                }
                _ => {
                    for (_hash, key) in row_keys {
                        mask.push(local_seen.insert(key));
                    }
                }
            }
            Ok((mask, format!("duplicate key on ({})", columns.join(", "))))
        }
        QualityRule::Check { name, expr } => {
            let CompiledRule::Check(parsed) = compiled else {
                unreachable!("Check rule always compiles to CompiledRule::Check")
            };
            let pred = parsed.evaluate_predicate(batch)?;
            let mask = (0..n).map(|i| !pred.is_null(i) && pred.value(i)).collect();
            Ok((
                mask,
                format!("check '{name}' ({expr}) evaluated to false/null"),
            ))
        }
    }
}

fn cast_to_utf8(col: &ArrayRef) -> SaciResult<ArrayRef> {
    if matches!(col.data_type(), DataType::Utf8) {
        return Ok(Arc::clone(col));
    }
    cast_with_options(
        col,
        &DataType::Utf8,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )
    .map_err(|e| SaciError::generic(format!("quality: cast column to Utf8: {e}")))
}

#[async_trait]
impl System for DataQualitySystem {
    fn meta(&self) -> SystemMeta {
        let mut meta = SystemMeta::new("quality")
            .reads_all_named(self.source_component)
            .writes_all_named(self.target_component)
            .writes_resource::<State<DataQualityState>>();
        if let Some(q_comp) = self.quarantine_component {
            meta = meta.writes_all_named(q_comp);
        }
        if self.on_violation == QualityFailureAction::Quarantine {
            meta = meta.writes_resource::<RouteDecision>();
            meta = meta.writes_resource::<BranchPayloads>();
        }
        meta
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        self.run_impl(data)
    }

    fn run_sync(&self, data: &mut Dataset) -> Option<Result<(), SaciError>> {
        Some(self.run_impl(data))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Float64Array, Int64Array};

    use super::*;
    use crate::pipeline::Pipeline;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("email", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
            Field::new("status", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(2),
                    None,
                    Some(5),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("a@b.com"),
                    Some("bad-email"),
                    Some("c@d.com"),
                    Some("e@f.com"),
                    Some("   "),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(10.0),
                    Some(20.0),
                    Some(-5.0),
                    Some(30.0),
                    Some(40.0),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("NEW"),
                    Some("NEW"),
                    Some("PAID"),
                    Some("NEW"),
                    Some("BAD"),
                ])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn quality_system_splits_valid_and_quarantined_rows_with_diagnostics() {
        let sys = DataQualitySystem::new(
            "Order",
            vec![
                QualityRule::NotNull {
                    name: "id_present".to_string(),
                    column: "id".to_string(),
                },
                QualityRule::NotEmpty {
                    name: "email_non_empty".to_string(),
                    column: "email".to_string(),
                },
                QualityRule::Regex {
                    name: "email_format".to_string(),
                    column: "email".to_string(),
                    pattern: r"^[^@\s]+@[^@\s]+\.[^@\s]+$".to_string(),
                    case_insensitive: false,
                },
                QualityRule::Range {
                    name: "amount_positive".to_string(),
                    column: "amount".to_string(),
                    min: Some(0.0),
                    max: None,
                    min_exclusive: false,
                    max_exclusive: false,
                },
                QualityRule::InSet {
                    name: "valid_status".to_string(),
                    column: "status".to_string(),
                    values: vec!["NEW".to_string(), "PAID".to_string()],
                },
                QualityRule::Unique {
                    name: "unique_id".to_string(),
                    columns: vec!["id".to_string()],
                    stateful: true,
                },
            ],
        )
        .expect("rules valid")
        .include_diagnostics(true);

        let batch = sample_batch();
        let mut pipeline = Pipeline::new("dq_test");
        pipeline
            .data_mut()
            .register_schema("Order", batch.schema())
            .unwrap();
        pipeline = pipeline.state::<DataQualityState>().system(sys);
        pipeline
            .data_mut()
            .append_record_batch("Order", batch)
            .unwrap();

        pipeline.run().await.expect("run succeeds");

        let route = pipeline.data().get_resource::<RouteDecision>().unwrap();
        assert_eq!(
            route.branches(),
            &["valid".to_string(), "quarantine".to_string()]
        );

        let payloads = pipeline.data().get_resource::<BranchPayloads>().unwrap();
        let valid = &payloads.batches["valid"];
        let quarantine = &payloads.batches["quarantine"];
        assert_eq!(valid.num_rows(), 1);
        assert_eq!(
            valid
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1]
        );
        assert_eq!(quarantine.num_rows(), 4);
        let rules_arr = quarantine
            .column_by_name("__dq_rule")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(rules_arr.value(0), "email_format");
        assert_eq!(rules_arr.value(1), "amount_positive");
        assert_eq!(rules_arr.value(2), "id_present");
        assert_eq!(rules_arr.value(3), "email_non_empty");

        // Second batch reusing the same stateful DataQualityState rejects duplicate id=1.
        let batch2 = sample_batch().slice(0, 1);
        pipeline
            .data_mut()
            .put_batch_named("Order", batch2)
            .unwrap();
        pipeline.run().await.expect("second pass succeeds");
        let payloads2 = pipeline.data().get_resource::<BranchPayloads>().unwrap();
        assert!(!payloads2.batches.contains_key("valid"));
        let q2 = &payloads2.batches["quarantine"];
        assert_eq!(q2.num_rows(), 1);
        let r2 = q2
            .column_by_name("__dq_rule")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(r2.value(0), "unique_id");
    }

    #[tokio::test]
    async fn quality_system_fail_action_errors_and_check_supports_regex() {
        let batch = sample_batch();
        let sys = DataQualitySystem::new(
            "Order",
            vec![QualityRule::Check {
                name: "email_regex".to_string(),
                expr: "email ~ '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'".to_string(),
            }],
        )
        .unwrap()
        .on_violation(QualityFailureAction::Fail);

        let err = sys
            .evaluate_batch(&batch, None)
            .expect_err("on_violation=Fail rejects bad rows");
        assert!(err.message().contains("email_regex"), "got: {err}");
    }
}
