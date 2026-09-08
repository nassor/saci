//! Glue for SDKs that drive a [`Pipeline`] across a foreign function boundary.
//!
//! Two SDKs sit on top of this module. `saci-processor` wires a pipeline to the
//! `saci:pipeline@0.3.0` WIT world for a WebAssembly component, and `saci-plugin`
//! wires the same pipeline to the C ABI of a native shared library. Both face
//! the same four problems: hold the pipeline behind a sync entry point, move
//! state across a batch boundary that resets memory, classify a [`SaciError`]
//! for the caller's error type, and report schemas the way the host expects.
//!
//! Nothing here is specific to either boundary, so both SDKs re-export it
//! rather than keeping a copy.

use std::marker::PhantomData;
use std::sync::{Mutex, MutexGuard, OnceLock};

use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;

use crate::{Component, Dataset, Pipeline, SaciError, SaciResult};

/// Holds the lazily built [`Pipeline`] owned by an SDK's exported entry points.
///
/// An SDK creates exactly one static instance per exported component. The
/// pipeline is built on the first call to any entry point, then reused.
///
/// `OnceLock` rather than `LazyLock`: the initializer is the caller's `build`
/// function, handed to [`PipelineSlot::pipeline`] per call, so there is nothing
/// for a `LazyLock` to store at declaration time.
pub struct PipelineSlot {
    pipeline: OnceLock<Mutex<Pipeline>>,
}

impl Default for PipelineSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineSlot {
    /// Construct an empty slot.
    pub const fn new() -> Self {
        Self {
            pipeline: OnceLock::new(),
        }
    }

    /// Lock and return the pipeline, initialising via `build` on first use.
    ///
    /// # Panics
    ///
    /// Panics if the inner `Mutex` is poisoned, which needs a panic while the
    /// lock is held. A WebAssembly processor traps before reaching this path; a
    /// native plugin catches the unwind at its boundary and reports a permanent
    /// error, so the poisoned lock surfaces on the next call instead.
    pub fn pipeline<F>(&self, build: F) -> MutexGuard<'_, Pipeline>
    where
        F: FnOnce() -> Pipeline,
    {
        self.pipeline
            .get_or_init(|| Mutex::new(build()))
            .lock()
            .expect("saci-core sdk: pipeline mutex poisoned")
    }
}

/// How an SDK moves processor state across a batch boundary.
///
/// An SDK picks an impl from its macro's invocation form: [`NoState`] for a
/// stateless pipeline, [`Stateful<C>`] for one that carries rows. Keeping the
/// branch in the type system rather than in `macro_rules!` keeps the large
/// expansion body to one copy.
pub trait ProcessorStateSpec {
    /// Value the SDK reports as the descriptor's `stateful` flag.
    const STATEFUL: bool;

    /// Make the previous batch's state available to the pipeline's systems.
    ///
    /// Called before the pipeline runs, with `prior` exactly as the host passed
    /// it. [`Stateful`] decodes it into a [`ProcessorState<C>`] resource on `data`;
    /// [`NoState`] does nothing.
    fn restore(data: &mut Dataset, prior: Option<&[u8]>) -> SaciResult<()>;

    /// Serialise the post-run state back out of `data`.
    ///
    /// The returned blob becomes the checkpoint the host persists verbatim and
    /// returns as the next `prior`.
    fn capture(data: &Dataset) -> SaciResult<Option<Vec<u8>>>;
}

/// [`ProcessorStateSpec`] for a stateless pipeline: both hooks are no-ops.
pub struct NoState;

impl ProcessorStateSpec for NoState {
    const STATEFUL: bool = false;

    fn restore(_data: &mut Dataset, _prior: Option<&[u8]>) -> SaciResult<()> {
        Ok(())
    }

    fn capture(_data: &Dataset) -> SaciResult<Option<Vec<u8>>> {
        Ok(None)
    }
}

/// [`ProcessorStateSpec`] backed by one Arrow component `C`.
///
/// The state lives in the batch dataset as a [`ProcessorState<C>`] resource, not as
/// a registered component: [`Dataset`]'s IPC format requires every component to
/// hold exactly the dataset's row count, while state rows are independent of
/// batch rows. Resources are invisible to [`Dataset::write_ipc`], so state never
/// leaks into the output.
///
/// The blob is a standalone single-component Arrow IPC stream, so the host
/// persists it with the existing checkpoint machinery without knowing the
/// schema.
pub struct Stateful<C>(PhantomData<C>);

impl<C> ProcessorStateSpec for Stateful<C>
where
    C: Component + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    const STATEFUL: bool = true;

    fn restore(data: &mut Dataset, prior: Option<&[u8]>) -> SaciResult<()> {
        let rows = match prior {
            // No prior, or an empty payload: either way this batch is the
            // pipeline's first.
            None | Some([]) => Vec::new(),
            Some(bytes) => {
                let name = C::name();
                let prior_data = Dataset::read_ipc(&mut &bytes[..])?;
                match prior_data.batch_for(name) {
                    None => Vec::new(),
                    Some(batch) => {
                        let on_disk = prior_data.schemas().get_version(name).unwrap_or(1);
                        let migrated = C::migrate(on_disk, batch.clone())?;
                        C::from_record_batch(&migrated)?
                    }
                }
            }
        };
        data.insert_resource(ProcessorState::<C>::new(rows));
        Ok(())
    }

    fn capture(data: &Dataset) -> SaciResult<Option<Vec<u8>>> {
        let state = data.get_resource::<ProcessorState<C>>().ok_or_else(|| {
            SaciError::generic(format!(
                "saci sdk: ProcessorState<{}> resource is missing after the run; \
                     a system must have replaced the dataset wholesale",
                C::name()
            ))
        })?;

        // Serialise through a scratch dataset holding only `C`, so the stream
        // satisfies Dataset's "every component has row_count rows" invariant
        // whatever the batch's own row count is.
        let mut scratch = Dataset::new();
        scratch.register_component::<C>()?;
        if !state.rows.is_empty() {
            scratch.append::<C>(&state.rows)?;
        }
        let mut buf: Vec<u8> = Vec::new();
        scratch.write_ipc(&mut buf)?;
        Ok(Some(buf))
    }
}

/// Map a [`SaciError`] from [`Pipeline::run_on_with_stats`] into a
/// `(is_retryable, message)` pair an SDK turns into its own error type.
///
/// `retryable` only for [`SaciError::RetryExhausted`] and
/// [`SaciError::SystemExecution`]; everything else, [`SaciError::Generic`]
/// included, is permanent. A schema mismatch is never reported from a batch.
pub fn classify_run_error(err: &SaciError) -> (bool, String) {
    let is_retryable = matches!(
        err,
        SaciError::RetryExhausted { .. } | SaciError::SystemExecution(_)
    );
    (is_retryable, err.to_string())
}

/// Serialize a single Arrow [`Schema`] as IPC schema-message bytes.
///
/// Writes an empty `StreamWriter`, whose stream start carries the schema
/// descriptor and nothing else. This is the shape a host parses out of a
/// component descriptor.
pub fn schema_to_ipc_bytes(schema: &Schema) -> SaciResult<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| SaciError::generic(format!("schema_to_ipc_bytes new: {e}")))?;
        writer
            .finish()
            .map_err(|e| SaciError::generic(format!("schema_to_ipc_bytes finish: {e}")))?;
    }
    Ok(buf)
}

/// Format a [`crate::SchemaRegistry::fingerprint`] value as the stable 8-char
/// lowercase hex string a descriptor's `schema_fingerprint` field carries.
pub fn fingerprint_hex(fp: u32) -> String {
    format!("{fp:08x}")
}

/// Cross-batch state, held as a [`Dataset`] resource.
///
/// An SDK's `state = C` form inserts one of these into the batch dataset before
/// the systems run, holding the rows the previous batch left behind (empty on a
/// cold start). Whatever the systems leave in it is serialised into the
/// checkpoint afterwards.
///
/// It is a resource rather than a registered component because [`Dataset`]'s
/// Arrow IPC format requires every component to hold exactly the dataset's row
/// count, while state rows are independent of batch rows. Resources are not
/// serialised by [`Dataset::write_ipc`], so state never leaks into the output.
///
/// A single-row state component is the common case, and
/// [`get_or_insert_default`](Self::get_or_insert_default) covers it:
///
/// ```ignore
/// fn count_batches(data: &mut Dataset) -> SaciResult<()> {
///     ProcessorState::<Counter>::get_or_insert_default(data)?.count += 1;
///     Ok(())
/// }
/// ```
pub struct ProcessorState<C> {
    /// The state rows. Systems mutate this in place.
    pub rows: Vec<C>,
}

/// The branch names this batch's output is delivered to.
///
/// A pipeline's systems insert one of these into the batch dataset to route
/// its output; the SDK export macros read it after the systems run. Absent =
/// legacy behaviour: the host multicasts the output to every downstream link.
/// `Some(vec![])` sends the output nowhere.
#[derive(Debug, Clone)]
pub struct RouteDecision(pub Vec<String>);

impl<C> ProcessorState<C> {
    /// Wrap the rows restored from the previous batch.
    pub fn new(rows: Vec<C>) -> Self {
        Self { rows }
    }

    /// `true` on a cold start, before any system has written state.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl<C: Default + 'static> ProcessorState<C> {
    /// Borrow the single state row, creating `C::default()` on a cold start.
    ///
    /// The shape almost every stateful processor wants: one row of running
    /// totals that starts at zero. It replaces the
    /// `match state.rows.first_mut() { Some(x) => .., None => rows.push(..) }`
    /// idiom each such system would otherwise hand-roll, and is what the
    /// `#[fold]` expansion calls.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::ResourceNotFound`] when no `ProcessorState<C>`
    /// resource is on the dataset. That means the SDK export macro was not
    /// given `state = C`, so nothing restored the previous batch's rows and
    /// nothing will capture these; inserting the resource here would silently
    /// drop every update at the batch boundary.
    pub fn get_or_insert_default(data: &mut Dataset) -> SaciResult<&mut C> {
        let state = data
            .get_resource_mut::<ProcessorState<C>>()
            .ok_or_else(|| {
                SaciError::resource_not_found(format!(
                    "ProcessorState<{}>: the SDK export macro was not given `state = <Type>`",
                    std::any::type_name::<C>()
                ))
            })?;
        if state.rows.is_empty() {
            state.rows.push(C::default());
        }
        Ok(&mut state.rows[0])
    }
}

impl<C> Default for ProcessorState<C> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_hex_is_lowercase_and_zero_padded_to_eight() {
        assert_eq!(fingerprint_hex(0), "00000000");
        assert_eq!(fingerprint_hex(0xd52f95a6), "d52f95a6");
        assert_eq!(fingerprint_hex(0xff), "000000ff");
        assert_eq!(fingerprint_hex(u32::MAX), "ffffffff");
    }

    #[test]
    fn classify_run_error_marks_only_execution_and_retry_exhausted_retryable() {
        let (retryable, message) = classify_run_error(&SaciError::system_execution("boom"));
        assert!(retryable);
        assert!(message.contains("boom"));

        assert!(
            classify_run_error(&SaciError::retry_exhausted(SaciError::generic("why"), 3)).0,
            "an exhausted retry budget is worth another tick"
        );

        assert!(!classify_run_error(&SaciError::configuration("bad")).0);
        assert!(!classify_run_error(&SaciError::generic("other")).0);
    }

    #[test]
    fn no_state_captures_nothing() {
        let mut data = Dataset::new();
        assert!(NoState::restore(&mut data, Some(&[1, 2, 3])).is_ok());
        assert_eq!(NoState::capture(&data).unwrap(), None);
        const { assert!(!NoState::STATEFUL) };
    }

    #[test]
    fn a_pipeline_slot_builds_once() {
        static SLOT: PipelineSlot = PipelineSlot::new();
        {
            let pipeline = SLOT.pipeline(|| Pipeline::new("first"));
            assert_eq!(pipeline.name(), "first");
        }
        // The second call must not rebuild, so the name from the first build
        // survives even though this closure would produce a different one.
        let pipeline = SLOT.pipeline(|| Pipeline::new("second"));
        assert_eq!(pipeline.name(), "first");
    }

    /// A single-row state component, as `#[fold]` expects.
    #[derive(Default, Debug, PartialEq)]
    struct Ledger {
        settled_count: i64,
    }

    #[test]
    fn get_or_insert_default_creates_the_row_on_a_cold_start() {
        let mut data = Dataset::new();
        data.insert_resource(ProcessorState::<Ledger>::default());

        ProcessorState::<Ledger>::get_or_insert_default(&mut data)
            .expect("state resource present")
            .settled_count += 7;

        assert_eq!(
            data.get_resource::<ProcessorState<Ledger>>().unwrap().rows,
            vec![Ledger { settled_count: 7 }]
        );
    }

    #[test]
    fn get_or_insert_default_reuses_the_restored_row() {
        let mut data = Dataset::new();
        data.insert_resource(ProcessorState::new(vec![Ledger { settled_count: 41 }]));

        ProcessorState::<Ledger>::get_or_insert_default(&mut data)
            .expect("state resource present")
            .settled_count += 1;

        let rows = &data.get_resource::<ProcessorState<Ledger>>().unwrap().rows;
        assert_eq!(rows.len(), 1, "a second row would be a lost update");
        assert_eq!(rows[0].settled_count, 42);
    }

    #[test]
    fn get_or_insert_default_without_the_resource_is_an_error() {
        let mut data = Dataset::new();
        let err = ProcessorState::<Ledger>::get_or_insert_default(&mut data)
            .expect_err("a stateless processor must not silently accumulate state");
        assert_eq!(err.category(), "resource_not_found");
        assert!(err.message().contains("Ledger"), "got: {err}");
    }
}
