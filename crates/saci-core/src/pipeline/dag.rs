use std::sync::Arc;

use crate::error::{SaciError, SaciResult};
use crate::retry::SystemConfig;
use crate::schema::SchemaRegistry;
use crate::system::{FieldAccess, ParallelSystem, System, SystemMeta};

use super::Pipeline;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OwnedFieldAccess {
    pub(super) component: &'static str,
    pub(super) field: String,
}

impl OwnedFieldAccess {
    pub(super) fn from_static(fa: &FieldAccess) -> Self {
        Self {
            component: fa.component,
            field: fa.field.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ExpandedMeta {
    /// The system's declared name, used as the `system.execute` span field.
    #[cfg_attr(not(feature = "tracing"), allow(dead_code))]
    pub(super) name: &'static str,
    pub(super) reads: Vec<OwnedFieldAccess>,
    pub(super) writes: Vec<OwnedFieldAccess>,
    pub(super) reads_resources: Vec<std::any::TypeId>,
    pub(super) writes_resources: Vec<std::any::TypeId>,
}

impl ExpandedMeta {
    pub(super) fn from_meta(
        meta: &SystemMeta,
        schemas: &SchemaRegistry,
    ) -> Result<Self, SaciError> {
        let mut reads: Vec<OwnedFieldAccess> = meta
            .reads
            .iter()
            .map(OwnedFieldAccess::from_static)
            .collect();
        let mut writes: Vec<OwnedFieldAccess> = meta
            .writes
            .iter()
            .map(OwnedFieldAccess::from_static)
            .collect();

        for fa in &meta.reads {
            validate_field(fa, schemas)?;
        }
        for fa in &meta.writes {
            validate_field(fa, schemas)?;
        }

        for component_name in &meta.reads_components {
            let schema = schemas.get(component_name).ok_or_else(|| {
                SaciError::configuration(format!(
                    "Pipeline: system '{}' declares reads_components for '{}', \
                     but that component is not registered",
                    meta.name, component_name
                ))
            })?;
            for field in schema.fields() {
                reads.push(OwnedFieldAccess {
                    component: component_name,
                    field: field.name().clone(),
                });
            }
        }

        for component_name in &meta.writes_components {
            let schema = schemas.get(component_name).ok_or_else(|| {
                SaciError::configuration(format!(
                    "Pipeline: system '{}' declares writes_components for '{}', \
                     but that component is not registered",
                    meta.name, component_name
                ))
            })?;
            for field in schema.fields() {
                writes.push(OwnedFieldAccess {
                    component: component_name,
                    field: field.name().clone(),
                });
            }
        }

        Ok(Self {
            name: meta.name,
            reads,
            writes,
            reads_resources: meta.reads_resources.clone(),
            writes_resources: meta.writes_resources.clone(),
        })
    }
}

pub(super) fn validate_field(fa: &FieldAccess, schemas: &SchemaRegistry) -> Result<(), SaciError> {
    let schema = schemas.get(fa.component).ok_or_else(|| {
        SaciError::configuration(format!(
            "Pipeline: declared access for component '{}' field '{}', \
             but component '{}' is not registered",
            fa.component, fa.field, fa.component
        ))
    })?;
    if schema.index_of(fa.field).is_err() {
        return Err(SaciError::configuration(format!(
            "Pipeline: declared access for field '{}.{}', \
             but that field does not exist in the component's schema. \
             Known fields: {}",
            fa.component,
            fa.field,
            schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    Ok(())
}

/// A system registered in the pipeline, either sequential or parallel.
pub(crate) enum SystemEntry {
    Sequential(Box<dyn System>),
    Parallel(Arc<dyn ParallelSystem>),
}

impl SystemEntry {
    pub(super) fn meta(&self) -> SystemMeta {
        match self {
            SystemEntry::Sequential(s) => s.meta(),
            SystemEntry::Parallel(s) => s.meta(),
        }
    }

    pub(super) fn config(&self) -> SystemConfig {
        match self {
            SystemEntry::Sequential(s) => s.config(),
            SystemEntry::Parallel(s) => s.config(),
        }
    }

    #[cfg(feature = "runtime")]
    pub(super) fn is_parallel(&self) -> bool {
        matches!(self, SystemEntry::Parallel(_))
    }
}

pub(super) fn build_stages_inner(
    systems: &[SystemEntry],
    metas: &[ExpandedMeta],
) -> (Vec<Vec<usize>>, Vec<SystemConfig>) {
    use std::collections::HashMap;

    if systems.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let configs: Vec<SystemConfig> = systems.iter().map(|s| s.config()).collect();
    let n = systems.len();

    type FieldKey<'a> = (&'a str, &'a str);

    let mut writers_by_field: HashMap<FieldKey<'_>, Vec<usize>> = HashMap::new();
    let mut readers_by_field: HashMap<FieldKey<'_>, Vec<usize>> = HashMap::new();

    for (i, meta) in metas.iter().enumerate() {
        for fa in &meta.writes {
            writers_by_field
                .entry((fa.component, fa.field.as_str()))
                .or_default()
                .push(i);
        }
        for fa in &meta.reads {
            readers_by_field
                .entry((fa.component, fa.field.as_str()))
                .or_default()
                .push(i);
        }
    }

    let mut resource_writers: HashMap<std::any::TypeId, Vec<usize>> = HashMap::new();
    let mut resource_readers: HashMap<std::any::TypeId, Vec<usize>> = HashMap::new();

    for (i, meta) in metas.iter().enumerate() {
        for &tid in &meta.writes_resources {
            resource_writers.entry(tid).or_default().push(i);
        }
        for &tid in &meta.reads_resources {
            resource_readers.entry(tid).or_default().push(i);
        }
    }

    let mut levels = vec![0usize; n];
    let mut edges: Vec<(usize, usize)> = Vec::new();
    let mut in_degree = vec![0u32; n];
    let mut seen_edges: std::collections::HashSet<(usize, usize)> =
        std::collections::HashSet::new();

    let add_edge = |edges: &mut Vec<_>,
                    in_degree: &mut Vec<_>,
                    seen: &mut std::collections::HashSet<_>,
                    i: usize,
                    j: usize| {
        if i < j && seen.insert((i, j)) {
            edges.push((i, j));
            in_degree[j] += 1;
        }
    };

    for (j, meta_j) in metas.iter().enumerate() {
        for fa in meta_j.reads.iter().chain(meta_j.writes.iter()) {
            let key: FieldKey<'_> = (fa.component, fa.field.as_str());
            if let Some(writers) = writers_by_field.get(&key) {
                for &i in writers {
                    add_edge(&mut edges, &mut in_degree, &mut seen_edges, i, j);
                }
            }
        }

        for fa in &meta_j.writes {
            let key: FieldKey<'_> = (fa.component, fa.field.as_str());
            if let Some(readers) = readers_by_field.get(&key) {
                for &i in readers {
                    add_edge(&mut edges, &mut in_degree, &mut seen_edges, i, j);
                }
            }
        }

        for &tid in meta_j
            .reads_resources
            .iter()
            .chain(meta_j.writes_resources.iter())
        {
            if let Some(writers) = resource_writers.get(&tid) {
                for &i in writers {
                    add_edge(&mut edges, &mut in_degree, &mut seen_edges, i, j);
                }
            }
        }

        for &tid in &meta_j.writes_resources {
            if let Some(readers) = resource_readers.get(&tid) {
                for &i in readers {
                    add_edge(&mut edges, &mut in_degree, &mut seen_edges, i, j);
                }
            }
        }
    }

    edges.sort_unstable();

    let mut queue: Vec<usize> = Vec::with_capacity(n);
    for (i, &deg) in in_degree.iter().enumerate() {
        if deg == 0 {
            queue.push(i);
        }
    }

    let mut head = 0;
    while head < queue.len() {
        let i = queue[head];
        head += 1;

        let start = edges.partition_point(|&(src, _)| src < i);
        let end = edges.partition_point(|&(src, _)| src <= i);

        for &(_, j) in &edges[start..end] {
            levels[j] = levels[j].max(levels[i] + 1);
            in_degree[j] -= 1;
            if in_degree[j] == 0 {
                queue.push(j);
            }
        }
    }

    let max_level = levels.iter().copied().max().unwrap_or(0);
    let mut stages: Vec<Vec<usize>> = vec![Vec::new(); max_level + 1];
    for (i, &level) in levels.iter().enumerate() {
        stages[level].push(i);
    }

    (stages, configs)
}

impl Pipeline {
    /// Validate all declared field access against registered schemas.
    pub fn validate(&self) -> SaciResult<()> {
        self.ensure_plan(self.data.schemas())?;
        Ok(())
    }

    /// Return the stage groupings computed from the current system set.
    pub fn stages(&self) -> Option<Vec<Vec<usize>>> {
        self.stages.get().and_then(|r| r.as_ref().ok()).cloned()
    }

    /// Return the number of stages in the execution plan, or `None` if not yet built.
    pub fn stage_count(&self) -> Option<usize> {
        self.stages
            .get()
            .and_then(|r| r.as_ref().ok())
            .map(|s| s.len())
    }

    pub(super) fn ensure_plan(&self, schemas: &SchemaRegistry) -> SaciResult<()> {
        let metas_result = self.expanded_metas.get_or_init(|| {
            self.systems
                .iter()
                .map(|entry| ExpandedMeta::from_meta(&entry.meta(), schemas))
                .collect::<Result<Vec<_>, _>>()
        });

        if let Err(e) = metas_result {
            return Err(e.clone());
        }

        let stages_result = self.stages.get_or_init(|| {
            let metas = metas_result.as_ref().unwrap();
            let (stages, configs) = build_stages_inner(&self.systems, metas);
            let _ = self.configs.get_or_init(|| configs);
            Ok(stages)
        });

        if let Err(e) = stages_result {
            return Err(e.clone());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::pipeline::Pipeline;
    use crate::system::{System, SystemMeta, system_fn};

    use crate::dataset::Dataset;

    #[derive(Serialize, Deserialize)]
    struct DagOrder {
        id: u64,
        total: f64,
    }

    impl Component for DagOrder {
        fn name() -> &'static str {
            "DagOrder"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("total", DataType::Float64, false),
            ]))
        }
    }

    #[test]
    fn test_pipeline_validate_unknown_field_returns_error() {
        let mut p = Pipeline::new("test");
        p.data.register_component::<DagOrder>().unwrap();

        struct BadSystem;
        #[async_trait::async_trait]
        impl System for BadSystem {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("bad").write("DagOrder", "nonexistent_field")
            }
            async fn run(&self, _: &mut Dataset) -> crate::SaciResult<()> {
                Ok(())
            }
        }

        p.add_system(BadSystem);
        let result = p.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_pipeline_stage_count_after_validate() {
        let p = Pipeline::builder("staged")
            .with::<DagOrder>()
            .with_system(system_fn(
                SystemMeta::new("s1").read("DagOrder", "id"),
                |_: &mut Dataset| Ok(()),
            ))
            .with_system(system_fn(
                SystemMeta::new("s2").write("DagOrder", "total"),
                |_: &mut Dataset| Ok(()),
            ))
            .build();

        p.validate().unwrap();
        assert!(p.stage_count().unwrap() >= 1);
    }

    #[test]
    fn test_field_access_validation_write_missing_component() {
        let mut p = Pipeline::new("conflict-test");

        struct WriteUnregistered;
        #[async_trait::async_trait]
        impl System for WriteUnregistered {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("bad").write("UnregisteredComp", "field")
            }
            async fn run(&self, _: &mut Dataset) -> crate::SaciResult<()> {
                Ok(())
            }
        }

        p.add_system(WriteUnregistered);
        assert!(p.validate().is_err());
    }
}
