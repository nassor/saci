//! Declarative conditional branching / row-splitting operator ([`RouteSystem`]).
//!
//! Evaluates SQL-style boolean [`Expr`] predicates on an input component batch
//! and emits both a [`RouteDecision`](crate::sdk::RouteDecision) (the active
//! branch names for downstream `link branch="..."` edges) and, in
//! [`RouteMode::Row`], [`BranchPayloads`] holding per-branch filtered
//! [`RecordBatch`] slices so each downstream branch receives only its matching
//! rows.

use std::collections::HashMap;

use arrow_arith::boolean::{and_kleene, not, or_kleene};
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::Schema;
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;

use crate::dataset::{Dataset, intern_component_name};
use crate::error::{SaciError, SaciResult};
use crate::sdk::RouteDecision;
use crate::system::{System, SystemMeta};

use super::expr::Expr;
use super::time::concat_non_empty;

/// A named predicate rule that routes matching rows/batches to `branch`.
#[derive(Debug, Clone)]
pub struct BranchRule {
    /// Target branch label matching `link ... branch="<name>"`.
    pub branch: String,
    /// Boolean predicate expression evaluated over the input batch.
    pub predicate: Expr,
}

impl BranchRule {
    /// Construct a branch rule from a branch name and parsed [`Expr`].
    pub fn new(branch: impl Into<String>, predicate: Expr) -> Self {
        Self {
            branch: branch.into(),
            predicate,
        }
    }

    /// Construct a branch rule by parsing a SQL-style boolean predicate string.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `branch` is empty or `expr_str`
    /// fails to parse.
    pub fn parse(branch: impl Into<String>, expr_str: &str) -> SaciResult<Self> {
        let branch = branch.into();
        if branch.is_empty() {
            return Err(SaciError::configuration(
                "BranchRule branch name must not be empty",
            ));
        }
        let predicate = Expr::parse(expr_str)?;
        Ok(Self { branch, predicate })
    }
}

/// How [`RouteSystem`] evaluates predicates and partitions outputs across branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RouteMode {
    /// Evaluate every predicate per row and split the batch so each branch
    /// receives only its matching rows via [`BranchPayloads`].
    #[default]
    Row,
    /// Evaluate predicates against the batch's first row and route the whole
    /// batch unchanged to the selected branch(es).
    FirstRow,
    /// Route the whole batch unchanged to every branch where at least one row
    /// satisfies the predicate.
    BatchAny,
    /// Route the whole batch unchanged to every branch where every row in a
    /// non-empty batch satisfies the predicate.
    BatchAll,
}

/// Per-branch filtered batches produced by [`RouteSystem`] in [`RouteMode::Row`].
///
/// Inserted into [`Dataset`] as a resource and extracted by [`PipelineRuntime`](crate::PipelineRuntime)
/// so [`deliver_outbound`](crate::service::runner) can stage per-branch rows onto
/// each labelled edge.
#[derive(Debug, Clone, Default)]
pub struct BranchPayloads {
    /// Component name carrying the split batches.
    pub component: &'static str,
    /// Non-empty filtered `RecordBatch` keyed by branch name.
    pub batches: HashMap<String, RecordBatch>,
}

/// Evaluates declarative branch predicates to route batches or split rows across
/// labelled workflow edges (`link ... branch="..."`).
#[derive(Debug, Clone)]
pub struct RouteSystem {
    source_component: &'static str,
    target_component: &'static str,
    rules: Vec<BranchRule>,
    mode: RouteMode,
    exclusive: bool,
    default_branch: Option<String>,
}

impl RouteSystem {
    /// Route batches of `component` according to ordered `rules`.
    ///
    /// Defaults to [`RouteMode::Row`] with `exclusive = true` (first matching
    /// rule claims each row) and no `default_branch`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `rules` is empty or any rule has
    /// an empty branch name.
    pub fn new(component: &str, rules: Vec<BranchRule>) -> SaciResult<Self> {
        if rules.is_empty() {
            return Err(SaciError::configuration(
                "RouteSystem requires at least one BranchRule",
            ));
        }
        for rule in &rules {
            if rule.branch.is_empty() {
                return Err(SaciError::configuration(
                    "RouteSystem rule branch name must not be empty",
                ));
            }
        }
        let comp = intern_component_name(component);
        Ok(Self {
            source_component: comp,
            target_component: comp,
            rules,
            mode: RouteMode::default(),
            exclusive: true,
            default_branch: None,
        })
    }

    /// Write routed component data to `target` instead of reading and routing
    /// `source_component` in place.
    pub fn into_component(mut self, target: &str) -> Self {
        self.target_component = intern_component_name(target);
        self
    }

    /// Set the routing granularity ([`RouteMode`]).
    pub fn mode(mut self, mode: RouteMode) -> Self {
        self.mode = mode;
        self
    }

    /// Control whether only the first matching branch receives a row/batch
    /// (`true`, default) or every matching branch does (`false`).
    pub fn exclusive(mut self, exclusive: bool) -> Self {
        self.exclusive = exclusive;
        self
    }

    /// Route rows (or batches in batch modes) that match no declared rule to
    /// `branch`.
    pub fn default_branch(mut self, branch: impl Into<String>) -> Self {
        let branch = branch.into();
        if !branch.is_empty() {
            self.default_branch = Some(branch);
        }
        self
    }

    /// Declared branch names (all rule branches followed by optional default).
    pub fn branches(&self) -> Vec<&str> {
        let mut names: Vec<&str> = Vec::new();
        for rule in &self.rules {
            if !names.contains(&rule.branch.as_str()) {
                names.push(rule.branch.as_str());
            }
        }
        if let Some(def) = &self.default_branch
            && !names.contains(&def.as_str())
        {
            names.push(def.as_str());
        }
        names
    }

    /// Validate all branch predicates against `schema`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError`] if any predicate references an unknown column or fails
    /// type checking against `schema`.
    pub fn validate_schema(&self, schema: &Schema) -> SaciResult<()> {
        let empty = RecordBatch::new_empty(std::sync::Arc::new(schema.clone()));
        for rule in &self.rules {
            rule.predicate.evaluate_predicate(&empty)?;
        }
        Ok(())
    }

    /// Evaluate all rules on `batch` and return `(selected_branches, payloads)`.
    ///
    /// `payloads` is `Some` when [`RouteMode::Row`] is active and `None` in batch
    /// granularity modes.
    pub fn route_batch(
        &self,
        batch: &RecordBatch,
    ) -> SaciResult<(Vec<String>, Option<BranchPayloads>)> {
        if batch.num_rows() == 0 {
            let payloads = (self.mode == RouteMode::Row).then(|| BranchPayloads {
                component: self.target_component,
                batches: HashMap::new(),
            });
            return Ok((Vec::new(), payloads));
        }

        match self.mode {
            RouteMode::Row => self.route_by_row(batch),
            RouteMode::FirstRow | RouteMode::BatchAny | RouteMode::BatchAll => {
                let eval_batch = if self.mode == RouteMode::FirstRow {
                    batch.slice(0, 1)
                } else {
                    batch.clone()
                };
                let mut selected = Vec::new();
                for rule in &self.rules {
                    let mask = clean_bool_mask(&rule.predicate.evaluate_predicate(&eval_batch)?);
                    let matches = match self.mode {
                        RouteMode::FirstRow | RouteMode::BatchAny => mask.true_count() > 0,
                        RouteMode::BatchAll => mask.true_count() == eval_batch.num_rows(),
                        RouteMode::Row => unreachable!(),
                    };
                    if matches {
                        if !selected.contains(&rule.branch) {
                            selected.push(rule.branch.clone());
                        }
                        if self.exclusive {
                            break;
                        }
                    }
                }
                if selected.is_empty()
                    && let Some(def) = &self.default_branch
                {
                    selected.push(def.clone());
                }
                Ok((selected, None))
            }
        }
    }

    fn route_by_row(
        &self,
        batch: &RecordBatch,
    ) -> SaciResult<(Vec<String>, Option<BranchPayloads>)> {
        let mut selected = Vec::new();
        let mut batches: HashMap<String, RecordBatch> = HashMap::new();
        let mut unclaimed: Option<BooleanArray> = None;
        let mut any_matched: Option<BooleanArray> = None;

        for rule in &self.rules {
            let mask = clean_bool_mask(&rule.predicate.evaluate_predicate(batch)?);

            any_matched = Some(match any_matched {
                None => mask.clone(),
                Some(prev) => or_kleene(&prev, &mask)
                    .map_err(|e| SaciError::generic(format!("RouteSystem or_kleene: {e}")))?,
            });

            let effective = if self.exclusive {
                match &unclaimed {
                    None => mask.clone(),
                    Some(rem) => and_kleene(&mask, rem)
                        .map_err(|e| SaciError::generic(format!("RouteSystem and_kleene: {e}")))?,
                }
            } else {
                mask.clone()
            };

            if self.exclusive {
                let not_mask =
                    not(&mask).map_err(|e| SaciError::generic(format!("RouteSystem not: {e}")))?;
                unclaimed = Some(match unclaimed {
                    None => not_mask,
                    Some(rem) => and_kleene(&rem, &not_mask).map_err(|e| {
                        SaciError::generic(format!("RouteSystem remaining mask: {e}"))
                    })?,
                });
            }

            if effective.true_count() > 0 {
                let slice = filter_record_batch(batch, &effective)
                    .map_err(|e| SaciError::generic(format!("RouteSystem filter: {e}")))?;
                if !selected.contains(&rule.branch) {
                    selected.push(rule.branch.clone());
                }
                if let Some(existing) = batches.get(&rule.branch) {
                    let merged = concat_non_empty(&batch.schema(), &[existing.clone(), slice])?;
                    batches.insert(rule.branch.clone(), merged);
                } else {
                    batches.insert(rule.branch.clone(), slice);
                }
            }
        }

        if let Some(def) = &self.default_branch
            && let Some(matched) = &any_matched
        {
            let unmatched = not(matched)
                .map_err(|e| SaciError::generic(format!("RouteSystem default mask: {e}")))?;
            if unmatched.true_count() > 0 {
                let slice = filter_record_batch(batch, &unmatched)
                    .map_err(|e| SaciError::generic(format!("RouteSystem default filter: {e}")))?;
                if !selected.contains(def) {
                    selected.push(def.clone());
                }
                if let Some(existing) = batches.get(def) {
                    let merged = concat_non_empty(&batch.schema(), &[existing.clone(), slice])?;
                    batches.insert(def.clone(), merged);
                } else {
                    batches.insert(def.clone(), slice);
                }
            }
        }

        Ok((
            selected,
            Some(BranchPayloads {
                component: self.target_component,
                batches,
            }),
        ))
    }

    fn run_impl(&self, data: &mut Dataset) -> SaciResult<()> {
        let batch = data.batch_named(self.source_component)?.clone();
        if self.target_component != self.source_component {
            data.put_batch_named(self.target_component, batch.clone())?;
        }
        let (branches, payloads) = self.route_batch(&batch)?;
        if let Some(payloads) = payloads {
            data.insert_resource(payloads);
        } else {
            let _ = data.take_resource::<BranchPayloads>();
        }
        data.insert_resource(RouteDecision::to(branches));
        Ok(())
    }
}

/// Convert SQL three-valued boolean mask into a null-free `BooleanArray` where
/// `NULL` evaluates to `false`.
fn clean_bool_mask(arr: &BooleanArray) -> BooleanArray {
    if arr.null_count() == 0 {
        return arr.clone();
    }
    BooleanArray::from_iter((0..arr.len()).map(|i| Some(arr.is_valid(i) && arr.value(i))))
}

#[async_trait]
impl System for RouteSystem {
    fn meta(&self) -> SystemMeta {
        let mut meta = SystemMeta::new("route")
            .reads_all_named(self.source_component)
            .writes_resource::<RouteDecision>();
        if self.target_component != self.source_component {
            meta = meta.writes_all_named(self.target_component);
        }
        if self.mode == RouteMode::Row {
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
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type};
    use arrow_array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field};

    use super::*;
    use crate::pipeline::Pipeline;
    use crate::runtime::PipelineRuntime;

    fn order_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("priority", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }

    fn sample_orders() -> RecordBatch {
        RecordBatch::try_new(
            order_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(StringArray::from(vec!["high", "high", "low", "unknown"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![15_000.0, 200.0, 50.0, 10.0])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn row_mode_exclusive_splits_and_uses_default() {
        let sys = RouteSystem::new(
            "Order",
            vec![
                BranchRule::parse("vip", "amount >= 10000.0 AND priority = 'high'").unwrap(),
                BranchRule::parse("high", "priority = 'high'").unwrap(),
            ],
        )
        .unwrap()
        .default_branch("standard");

        let mut p = Pipeline::new("router");
        p.data_mut()
            .register_schema("Order", order_schema())
            .unwrap();
        p = p.system(sys);
        let rt: Box<dyn PipelineRuntime> = Box::new(p);

        let mut ds = rt.template_dataset();
        ds.append_record_batch("Order", sample_orders()).unwrap();
        let out = rt.run_batch(&mut ds, None).await.unwrap();

        let routes = out.routes.expect("routes present");
        assert_eq!(routes, vec!["vip", "high", "standard"]);
        let payloads = out.branch_batches.expect("row payloads");
        assert_eq!(payloads.batches["vip"].num_rows(), 1);
        assert_eq!(
            payloads.batches["vip"]
                .column(0)
                .as_primitive::<Int64Type>()
                .value(0),
            1
        );
        // Row 1 was claimed by "vip", so "high" receives only row 2 (id=2).
        assert_eq!(payloads.batches["high"].num_rows(), 1);
        assert_eq!(
            payloads.batches["high"]
                .column(0)
                .as_primitive::<Int64Type>()
                .value(0),
            2
        );
        // Rows 3 and 4 fell through to "standard".
        assert_eq!(payloads.batches["standard"].num_rows(), 2);
        assert_eq!(
            payloads.batches["standard"]
                .column(0)
                .as_primitive::<Int64Type>()
                .values(),
            &[3, 4]
        );
    }

    #[tokio::test]
    async fn row_mode_non_exclusive_multicasts_matching_rows() {
        let sys = RouteSystem::new(
            "Order",
            vec![
                BranchRule::parse("vip", "amount >= 10000.0 AND priority = 'high'").unwrap(),
                BranchRule::parse("high", "priority = 'high'").unwrap(),
            ],
        )
        .unwrap()
        .exclusive(false);

        let (routes, payloads) = sys.route_batch(&sample_orders()).unwrap();
        assert_eq!(routes, vec!["vip", "high"]);
        let payloads = payloads.unwrap();
        assert_eq!(payloads.batches["vip"].num_rows(), 1);
        assert_eq!(payloads.batches["high"].num_rows(), 2);
        assert!(
            (payloads.batches["high"]
                .column(2)
                .as_primitive::<Float64Type>()
                .value(0)
                - 15_000.0)
                .abs()
                < 1e-9
        );
    }

    #[test]
    fn batch_modes_evaluate_first_row_any_and_all() {
        let rules = vec![
            BranchRule::parse("high", "priority = 'high'").unwrap(),
            BranchRule::parse("low", "priority = 'low'").unwrap(),
        ];

        let first = RouteSystem::new("Order", rules.clone())
            .unwrap()
            .mode(RouteMode::FirstRow);
        let (r_first, p_first) = first.route_batch(&sample_orders()).unwrap();
        assert_eq!(r_first, vec!["high"]);
        assert!(p_first.is_none());

        let any = RouteSystem::new("Order", rules.clone())
            .unwrap()
            .mode(RouteMode::BatchAny)
            .exclusive(false);
        let (r_any, _) = any.route_batch(&sample_orders()).unwrap();
        assert_eq!(r_any, vec!["high", "low"]);

        let all = RouteSystem::new("Order", rules)
            .unwrap()
            .mode(RouteMode::BatchAll)
            .default_branch("mixed");
        let (r_all, _) = all.route_batch(&sample_orders()).unwrap();
        assert_eq!(r_all, vec!["mixed"]);
    }
}
