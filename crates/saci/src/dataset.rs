//! [`Dataset`]: the Arrow-backed columnar data container.
//!
//! `Dataset` stores data in Apache Arrow [`RecordBatch`]es, one per registered
//! component type. Components normally share the same row count, so a row
//! index is valid across every component at once; a component may hold fewer
//! rows than the dataset's row count when it is a *results* component, such as
//! the reduced output of a windowing processor.
//!
//! ## Design
//!
//! - **Column-first access**: there is no per-row `get::<C>(row)`. Callers read
//!   a whole column, or a projection, and operate over it.
//! - **Batch-only ingestion**: data enters via [`append`](Dataset::append),
//!   which adds an aligned slice across all components in one shot.
//! - **Lazy deletes**: [`mark_dead`](Dataset::mark_dead) flips a validity bit.
//!   [`compact`](Dataset::compact) filters all batches at once when the dead
//!   fraction is large enough.
//! - **IPC round-trip**: [`write_ipc`](Dataset::write_ipc) and
//!   [`read_ipc`](Dataset::read_ipc) serialise and reconstruct the whole
//!   dataset as an Arrow IPC stream with no intermediate copying. The `ipc`
//!   submodule owns that segment framing.

use std::{
    collections::{HashMap, HashSet},
    sync::{Mutex, OnceLock, RwLock},
};

use arrow_array::RecordBatch;
use arrow_buffer::builder::BooleanBufferBuilder;

use crate::{resource::ResourceMap, schema::SchemaRegistry};

pub(crate) const COMPONENT_NAME_KEY: &str = "__saci_component";
pub(crate) const SCHEMA_VERSION_KEY: &str = "__saci_schema_version";

const ALIVE_BATCH_NAME: &str = "__alive";

/// Chunks a component may accumulate before `append` compacts them into one
/// `RecordBatch`.
///
/// A count, not a row budget, so how many rows it lets pile up depends
/// entirely on how large the appends are, and that size is not the producer's
/// choice alone: the service's flow control sizes a cooperative source's
/// fetch to its own admission target, which can sit well below a connector's
/// configured batch size, so such a dataset reaches sixteen chunks after
/// fewer rows and pays `concat_batches` sooner. Left at sixteen deliberately:
/// making it row-aware is a change to make against a profile, not against
/// this reasoning.
const MERGE_THRESHOLD: usize = 16;

static NAME_INTERNER: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();

pub(crate) fn intern_component_name(name: &str) -> &'static str {
    let set = NAME_INTERNER.get_or_init(|| Mutex::new(HashSet::new()));
    let mut guard = set.lock().unwrap();
    if let Some(existing) = guard.get(name) {
        return existing;
    }
    let leaked: &'static str = Box::leak(name.to_string().into_boxed_str());
    guard.insert(leaked);
    leaked
}

/// Columnar data container backed by Apache Arrow [`RecordBatch`]es.
///
/// All component data lives in columnar form; resources remain as boxed Rust
/// values. Row indices (`Row`) are stable until [`compact`](Self::compact) is
/// called.
///
/// ## Invariant
///
/// Every registered component's accumulated chunks have at most `row_count`
/// rows in total — a component may hold fewer when it is a results component,
/// but never more. The alive bitmap has exactly `row_count` bits.
pub struct Dataset {
    pub(crate) components: HashMap<&'static str, Vec<RecordBatch>>,
    merged_cache: RwLock<HashMap<&'static str, Box<RecordBatch>>>,
    /// Zero-row `RecordBatch` per component, built once at registration.
    ///
    /// `clear()` needs an empty sentinel chunk per component so `batch_named`
    /// keeps working on an emptied dataset. Cloning a prebuilt batch costs a
    /// few `Arc` bumps instead of building an empty Arrow array per column.
    empty_templates: HashMap<&'static str, RecordBatch>,
    schemas: SchemaRegistry,
    row_count: usize,
    alive: BooleanBufferBuilder,
    live_count: usize,
    dead_count: usize,
    resources: ResourceMap,
}

// SAFETY: `BooleanBufferBuilder` contains a raw pointer internally, but it is
// only accessed through `&mut self` methods, and `Dataset` is otherwise composed
// of `Send + Sync` types. The `merged_cache` lock keeps interior-mutability
// accesses from `&self` data-race-free.
unsafe impl Send for Dataset {}
unsafe impl Sync for Dataset {}

impl Dataset {
    /// Create an empty dataset with no components and no resources.
    ///
    /// # Example
    ///
    /// ```rust
    /// #
    /// # {
    /// use saci::dataset::Dataset;
    /// let dataset = Dataset::new();
    /// assert_eq!(dataset.rows(), 0);
    /// # }
    /// ```
    pub fn new() -> Self {
        Self {
            components: HashMap::new(),
            merged_cache: RwLock::new(HashMap::new()),
            empty_templates: HashMap::new(),
            schemas: SchemaRegistry::new(),
            row_count: 0,
            alive: BooleanBufferBuilder::new(0),
            live_count: 0,
            dead_count: 0,
            resources: ResourceMap::new(),
        }
    }
}

impl Default for Dataset {
    fn default() -> Self {
        Self::new()
    }
}

mod append;
mod chunks;
mod forward;
mod ipc;
mod lifecycle;
mod reads;
mod register;
mod resources;
mod write;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_component_name_pointer_stable() {
        let p1 = intern_component_name("__test_intern_dataset_foo__");
        let p2 = intern_component_name("__test_intern_dataset_foo__");
        assert!(std::ptr::eq(p1, p2));
    }

    #[test]
    fn test_intern_component_name_distinct_names() {
        let p1 = intern_component_name("__test_intern_dataset_alpha__");
        let p2 = intern_component_name("__test_intern_dataset_beta__");
        assert!(!std::ptr::eq(p1, p2));
    }
}
