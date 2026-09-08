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
use arrow_ipc::{
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};

use crate::{resource::ResourceMap, schema::SchemaRegistry};

pub(crate) const COMPONENT_NAME_KEY: &str = "__saci_component";
pub(crate) const SCHEMA_VERSION_KEY: &str = "__saci_schema_version";

const ALIVE_BATCH_NAME: &str = "__alive";

/// Chunks a component may accumulate before `append` compacts them into one
/// `RecordBatch`.
///
/// A count, not a row budget, so how many rows it lets pile up depends
/// entirely on how large the appends are, and that size is not the producer's
/// choice alone: `saci-service`'s flow control sizes a cooperative source's
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
    /// `clear()` needs an empty sentinel chunk per component so `batch_for`
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
    /// use saci_core::dataset::Dataset;
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

/// Fluent builder for [`Dataset`].
///
/// # Example
///
/// ```rust
/// # {
/// # use std::sync::Arc;
/// # use arrow_schema::{DataType, Field, Schema};
/// # use saci_core::component::Component;
/// # use saci_core::dataset::{Dataset, DatasetBuilder};
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct Order { id: u64 }
/// # impl Component for Order {
/// #     fn name() -> &'static str { "Order" }
/// #     fn schema() -> Arc<Schema> {
/// #         Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]))
/// #     }
/// # }
/// struct Config { max: u32 }
/// let dataset = Dataset::builder()
///     .with::<Order>()
///     .with_resource(Config { max: 100 })
///     .build();
/// # }
/// ```
pub struct DatasetBuilder(pub(crate) Dataset);

/// Capacity headroom for `batch_to_ipc_bytes` beyond the column buffers: the
/// schema and record-batch flatbuffer messages, their continuation markers and
/// length prefixes, the end-of-stream marker, and each buffer's padding to the
/// IPC write alignment (64 bytes by default). Both flatbuffer messages carry
/// one field node and two or more buffer descriptors per column, so the
/// per-field term scales with the column count.
const IPC_ENVELOPE_FIXED: usize = 1024;
const IPC_ENVELOPE_PER_FIELD: usize = 256;

/// Encode one `RecordBatch` as a standalone Arrow IPC stream.
///
/// The single `StreamWriter` call in the crate. `IpcWriteOptions::default()`
/// fixes MetadataVersion V5, 8 byte alignment and no compression, which is
/// what keeps the output readable by a hand written decoder in a language with
/// no Arrow library.
pub(crate) fn batch_to_ipc_bytes(batch: &RecordBatch) -> Result<Vec<u8>, crate::error::SaciError> {
    // Size the buffer up front. `StreamWriter` appends every column's buffer
    // into `buf` via `extend_from_slice`, and growing from capacity 0 costs
    // about twenty amortised doublings whose re-copies sum, by geometric
    // series, to roughly the final size again.
    //
    // The estimate is biased in two directions:
    //
    // * `get_array_memory_size` sums each buffer's *capacity*, not its sliced
    //   length, so it over-counts an over-allocated or sliced buffer.
    // * It cannot see the validity bitmap `arrow-ipc` synthesises for a column
    //   whose `nulls()` is `None`: an all-ones `ceil(rows/8)` buffer. That
    //   term is added back explicitly below.
    //
    // Net effect is a small over-estimate, which costs address space and
    // nothing else. `Vec` still grows if the hint turns out short.
    let mut buf = Vec::with_capacity(
        batch.get_array_memory_size()
            + batch.num_columns() * (batch.num_rows().div_ceil(8) + IPC_ENVELOPE_PER_FIELD)
            + IPC_ENVELOPE_FIXED,
    );
    let options = IpcWriteOptions::default();
    {
        let mut sw = StreamWriter::try_new_with_options(&mut buf, batch.schema_ref(), options)
            .map_err(|e| crate::error::SaciError::generic(format!("IPC StreamWriter init: {e}")))?;
        sw.write(batch).map_err(|e| {
            crate::error::SaciError::generic(format!("IPC StreamWriter write: {e}"))
        })?;
        sw.finish().map_err(|e| {
            crate::error::SaciError::generic(format!("IPC StreamWriter finish: {e}"))
        })?;
    }
    Ok(buf)
}

/// Decode the first `RecordBatch` of an IPC stream straight out of `reader`.
///
/// Takes a reader rather than a `&[u8]`: `arrow-ipc`'s `MessageReader` already
/// materialises the message body into an Arrow-owned `MutableBuffer`, so a
/// caller-filled slice would copy the whole body twice.
///
/// Does not consume the stream's end-of-stream marker; it stops as soon as the
/// first record-batch message is decoded. Callers that frame segments must
/// drain the remainder themselves.
///
/// Not panic-safe on its own: some malformed buffer spans make arrow-rs
/// assert rather than return `Err` (see `Buffer::slice_with_length`'s
/// `# Panics`). Its one caller, [`Dataset::read_ipc`](super::Dataset::read_ipc),
/// is the unwind boundary; a new caller decoding untrusted bytes must add one
/// too.
pub(crate) fn ipc_stream_to_batch<R: std::io::Read>(
    reader: R,
) -> Result<RecordBatch, crate::error::SaciError> {
    let mut sr = StreamReader::try_new(reader, None)
        .map_err(|e| crate::error::SaciError::generic(format!("IPC StreamReader init: {e}")))?;
    sr.next()
        .ok_or_else(|| crate::error::SaciError::generic("IPC stream contained no batches"))?
        .map_err(|e| crate::error::SaciError::generic(format!("IPC StreamReader read: {e}")))
}

mod append;
mod builder;
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
