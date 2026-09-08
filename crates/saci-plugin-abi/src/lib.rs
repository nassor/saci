//! The C ABI a SACI native plugin exports.
//!
//! A native plugin is a shared library that a host loads with `dlopen` or
//! `LoadLibrary`. It mirrors the `saci:pipeline@0.3.0` WIT world that a
//! WebAssembly processor implements: two exported entry points, four vtable calls,
//! three host callbacks, Arrow IPC bytes as the data plane, and one opaque
//! checkpoint blob as the only state the host carries across a batch
//! boundary. The host holds one loaded instance for a node's whole life, so
//! the library's own memory survives a batch as well; keeping state there is
//! still wrong, because consecutive batches of one partition may land on
//! different processes and only the checkpoint travels with the claim.
//!
//! `include/saci_plugin.h` declares the same types for a plugin written in
//! another language, and carries the per-language build recipe.
//!
//! # Entry points
//!
//! A plugin exports exactly these two symbols and nothing else:
//!
//! ```c
//! uint32_t  saci_abi_version(void);
//! SaciStatus saci_plugin_v1(const SaciHostV1 *host, SaciPluginV1 *out);
//! ```
//!
//! The host calls `saci_abi_version` first and refuses the library unless
//! [`abi_is_compatible`] accepts what it returns. Only then does it call
//! `saci_plugin_v1`, which fills a host-provided [`SaciPluginV1`].
//!
//! # Ownership
//!
//! - A [`SaciSlice`] is borrowed. It is valid for the duration of the call that
//!   received it, and never longer. The one exception is the slice
//!   [`SaciHostV1::get_config`] writes, which stays valid for the life of the
//!   plugin instance because host config is immutable once built.
//! - A [`SaciBuffer`] the plugin writes is owned by the plugin. The host copies
//!   out of it and hands it back to [`SaciPluginV1::free_buffer`]. The host never
//!   frees plugin memory, and never gives a plugin a buffer it allocated.
//! - The vtable itself is host-allocated: `saci_plugin_v1` fills a
//!   caller-provided struct, so nothing allocates it and nothing frees it.
//!   [`SaciPluginV1::destroy`] frees only [`SaciPluginV1::instance`].
//! - The host keeps its [`SaciHostV1`] alive for the whole life of the instance,
//!   because the plugin stores the pointer it was handed.
//!
//! # Threading
//!
//! The host never calls into one instance concurrently. Successive calls may
//! arrive on different OS threads, so no state may be thread-affine across
//! calls.
//!
//! # Unwinding
//!
//! [`SaciPluginV1::describe`] and [`SaciPluginV1::run_batch`] are
//! `extern "C-unwind"`: a Rust panic that reaches either slot's boundary
//! unwinds using the platform's native unwind mechanism instead of aborting
//! immediately, so a Rust host wrapping the call in `catch_unwind` can
//! observe it as an `Err` and keep running. [`SaciPluginV1::free_buffer`] and
//! [`SaciPluginV1::destroy`] are plain `extern "C"`; nothing about them is
//! data-dependent, so the host does not wrap those calls.
//!
//! This is a narrower guarantee than "the host survives any plugin bug," and
//! every implementation should still guard itself rather than lean on it:
//!
//! - It only helps a *Rust* plugin. A Go panic or a .NET/Kotlin exception is
//!   not a Rust unwind; one reaching a Rust `catch_unwind` frame is defined to
//!   abort the process regardless of the extern ABI, because Rust cannot
//!   safely resume an exception whose cleanup semantics it does not
//!   understand. Go must `recover()`, C# and Kotlin must `try`/`catch`, before
//!   ever returning to Rust.
//! - It does nothing for a plugin compiled with `panic = "abort"`: there is no
//!   unwind to catch, by that plugin's own choice.
//! - It does nothing for memory corruption, a wedged thread, or anything else
//!   that is not a clean unwind.
//!
//! `export_plugin!` already wraps every call into user code in `catch_unwind`
//! and reports [`SaciStatus::PERMANENT`], so a plugin built with it never
//! actually exercises the host-side catch. The extern-ABI change exists for
//! the plugin that does not: it turns an otherwise-unconditional process
//! abort into a recoverable `Err`, for that one case only. `saci-service`'s
//! host wraps both calls; see `NativePluginRuntime` there.
//!
//! # No preemption
//!
//! A native plugin runs in-process with full host privileges and has no
//! equivalent of the wasmtime epoch deadline that bounds a WebAssembly processor.
//! A plugin that wedges wedges its caller, and one that corrupts memory takes
//! the host with it. Plugin paths are operator-trusted.

#![cfg_attr(not(test), no_std)]
#![deny(missing_docs)]

use core::ffi::c_void;

/// The ABI version this crate defines, as `major << 16 | minor`.
///
/// A host accepts a plugin whose major equals this major and whose minor is no
/// greater than this minor. See [`abi_is_compatible`].
///
/// `0x0001_0001`: minor 1 retyped [`SaciPluginV1::describe`] and
/// [`SaciPluginV1::run_batch`] from `extern "C"` to `extern "C-unwind"` (see
/// the `# Unwinding` section above). Layout is unchanged, and a plugin built
/// against minor 0 still loads and behaves exactly as before: its own
/// exported functions remain compiled nounwind either way, so only a plugin
/// rebuilt against minor 1 gains anything from the host's new catch.
///
/// `0x0001_0002`: minor 2 appended `routes`/`has_routes` to [`SaciRunResult`].
/// Layout grows by 32 bytes; a minor-1 plugin leaves the new trailing fields
/// zeroed, which the host reads as "no routing decision", so it loads and
/// behaves exactly as before.
pub const SACI_ABI_VERSION: u32 = 0x0001_0002;

/// Extract the major half of an ABI version.
#[must_use]
pub const fn abi_major(version: u32) -> u16 {
    (version >> 16) as u16
}

/// Extract the minor half of an ABI version.
#[must_use]
pub const fn abi_minor(version: u32) -> u16 {
    (version & 0xffff) as u16
}

/// Whether a host built against [`SACI_ABI_VERSION`] can drive `plugin`.
///
/// The major must match exactly: a major bump is a breaking layout change. The
/// plugin's minor must be no greater than the host's, because a newer minor may
/// rely on vtable slots this host does not know how to fill.
#[must_use]
pub const fn abi_is_compatible(plugin: u32) -> bool {
    abi_major(plugin) == abi_major(SACI_ABI_VERSION)
        && abi_minor(plugin) <= abi_minor(SACI_ABI_VERSION)
}

/// Log level for [`SaciHostV1::log`], matching the WIT `host-io::log-level`
/// variant order.
pub mod log_level {
    /// Finest granularity.
    pub const TRACE: u32 = 0;
    /// Debugging detail.
    pub const DEBUG: u32 = 1;
    /// Normal operational message.
    pub const INFO: u32 = 2;
    /// Something recoverable deserves attention.
    pub const WARN: u32 = 3;
    /// A failure.
    pub const ERROR: u32 = 4;
}

/// A borrowed byte range.
///
/// Valid only for the duration of the call that received it, unless the call's
/// documentation says otherwise.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SaciSlice {
    /// Start of the range. May be null when [`SaciSlice::len`] is zero.
    pub ptr: *const u8,
    /// Length in bytes.
    pub len: usize,
}

impl SaciSlice {
    /// An empty slice with a null pointer.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            ptr: core::ptr::null(),
            len: 0,
        }
    }

    /// Borrow `bytes` as a slice valid as long as `bytes` is.
    #[must_use]
    pub const fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
        }
    }

    /// Whether this slice carries no bytes.
    ///
    /// A null pointer and a zero length are both empty, which is why a caller
    /// checks this before dereferencing.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0 || self.ptr.is_null()
    }

    /// View the range as a Rust slice.
    ///
    /// Returns an empty slice when [`SaciSlice::is_empty`] holds, so a null
    /// pointer never reaches [`core::slice::from_raw_parts`].
    ///
    /// # Safety
    ///
    /// When non-empty, `ptr` must point at `len` initialised bytes that stay
    /// valid and unaliased for `'a`.
    #[must_use]
    pub const unsafe fn as_bytes<'a>(self) -> &'a [u8] {
        if self.is_empty() {
            &[]
        } else {
            unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

/// A byte buffer owned by whichever side allocated it.
///
/// Every buffer a plugin writes goes back to [`SaciPluginV1::free_buffer`]
/// unchanged, `cap` included: the allocator that produced it is the only one
/// that can release it.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SaciBuffer {
    /// Start of the allocation, or null when the buffer is absent.
    pub ptr: *mut u8,
    /// Initialised length in bytes.
    pub len: usize,
    /// Allocated capacity in bytes.
    pub cap: usize,
}

impl SaciBuffer {
    /// An absent buffer.
    #[must_use]
    pub const fn null() -> Self {
        Self {
            ptr: core::ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }

    /// Whether the buffer carries nothing worth reading or freeing.
    #[must_use]
    pub const fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    /// View the buffer's initialised bytes.
    ///
    /// Returns an empty slice when the pointer is null or the length is zero.
    ///
    /// # Safety
    ///
    /// When non-null, `ptr` must point at `len` initialised bytes that stay
    /// valid and unaliased for `'a`.
    #[must_use]
    pub const unsafe fn as_bytes<'a>(&self) -> &'a [u8] {
        if self.ptr.is_null() || self.len == 0 {
            &[]
        } else {
            unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

/// The outcome of a vtable call.
///
/// `#[repr(transparent)]` over `i32`, so a function returning one has the same
/// C ABI as a function returning `int32_t`.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SaciStatus(pub i32);

impl SaciStatus {
    /// The call succeeded. Output parameters are populated.
    pub const OK: Self = Self(0);
    /// Transient failure. The host releases the partition claim and retries on
    /// the next tick, matching the WIT `run-error::retryable` arm.
    pub const RETRYABLE: Self = Self(1);
    /// Permanent failure: bad input shape, plugin logic bug, unknown error. The
    /// host acks the claim and surfaces it, matching `run-error::permanent`.
    pub const PERMANENT: Self = Self(2);
    /// Reserved for load time. `run_batch` must never return it; a mismatch
    /// surfacing mid-batch is a plugin bug and the host folds it into the
    /// permanent path.
    pub const SCHEMA_MISMATCH: Self = Self(3);

    /// Whether the call succeeded.
    #[must_use]
    pub const fn is_ok(self) -> bool {
        self.0 == Self::OK.0
    }

    /// The raw integer, for a caller formatting an unrecognised status.
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        self.0
    }
}

/// What a plugin reports about one batch.
///
/// Mirrors the WIT `run-metrics` record field for field.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct SaciRunMetrics {
    /// Wall-clock nanoseconds the plugin spent on the batch.
    pub wall_ns: u64,
    /// Live rows the plugin decoded from `input`.
    pub rows_in: u64,
    /// Live rows the plugin encoded into `output`.
    pub rows_out: u64,
    /// Systems the plugin ran.
    pub systems_run: u32,
    /// Retry attempts the plugin made internally.
    pub retries: u32,
}

/// What a successful [`SaciPluginV1::run_batch`] produces.
#[repr(C)]
pub struct SaciRunResult {
    /// Arrow IPC bytes for the mutated dataset. Plugin-owned.
    pub output: SaciBuffer,
    /// The plugin's new opaque state blob. Plugin-owned. Read only when
    /// [`SaciRunResult::has_checkpoint`] is non-zero.
    pub checkpoint: SaciBuffer,
    /// Non-zero when [`SaciRunResult::checkpoint`] carries a blob the host must
    /// persist. Zero means the plugin is stateless for this batch, which is
    /// distinct from a present but empty blob.
    pub has_checkpoint: i32,
    /// Per-batch metrics.
    pub metrics: SaciRunMetrics,
    /// UTF-8 JSON array of branch names this batch's output is delivered to.
    /// Plugin-owned; read only when [`SaciRunResult::has_routes`] is non-zero.
    /// A null buffer means no routing decision (legacy multicast).
    pub routes: SaciBuffer,
    /// Non-zero when [`SaciRunResult::routes`] carries a JSON list.
    pub has_routes: i32,
}

impl SaciRunResult {
    /// A result with no output, no checkpoint, and zeroed metrics.
    ///
    /// The host creates one of these and passes `&mut` it into `run_batch`, so
    /// a plugin that returns a failure without writing the out-parameter still
    /// leaves the host reading well-defined values.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            output: SaciBuffer::null(),
            checkpoint: SaciBuffer::null(),
            has_checkpoint: 0,
            metrics: SaciRunMetrics {
                wall_ns: 0,
                rows_in: 0,
                rows_out: 0,
                systems_run: 0,
                retries: 0,
            },
            routes: SaciBuffer::null(),
            has_routes: 0,
        }
    }
}

/// Host capabilities a plugin may call while a vtable call is in progress.
///
/// Every slot is an [`Option`], which has the same C ABI as a bare function
/// pointer thanks to the null-pointer optimisation. A plugin therefore checks
/// for `None` instead of calling through a null pointer, which is what a
/// zero-filled or partially-filled struct from a mismatched host would give it.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SaciHostV1 {
    /// Opaque host context, passed back as the first argument of every callback.
    pub ctx: *mut c_void,
    /// Emit a structured log line. `level` is one of the [`log_level`]
    /// constants; an unrecognised value is treated as
    /// [`log_level::INFO`].
    pub log: Option<
        unsafe extern "C" fn(ctx: *mut c_void, level: u32, target: SaciSlice, message: SaciSlice),
    >,
    /// Record a named metric value.
    pub metric: Option<unsafe extern "C" fn(ctx: *mut c_void, name: SaciSlice, value: f64)>,
    /// Look up a static config value.
    ///
    /// Returns 1 and writes `out` when the key is present, 0 and leaves `out`
    /// untouched when it is absent. The slice written to `out` stays valid for
    /// the life of the plugin instance, because host config is immutable once
    /// built.
    pub get_config:
        Option<unsafe extern "C" fn(ctx: *mut c_void, key: SaciSlice, out: *mut SaciSlice) -> i32>,
}

impl SaciHostV1 {
    /// A host vtable with no context and no callbacks.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            ctx: core::ptr::null_mut(),
            log: None,
            metric: None,
            get_config: None,
        }
    }
}

/// The plugin's vtable, filled by `saci_plugin_v1`.
///
/// Every call takes [`SaciPluginV1::instance`] as its first argument. Slots are
/// [`Option`] for the same reason as [`SaciHostV1`]: the host zero-fills this
/// struct before the call, so a plugin that forgets a slot produces a clean
/// load-time error instead of a jump through null.
#[repr(C)]
pub struct SaciPluginV1 {
    /// Opaque plugin state, released by [`SaciPluginV1::destroy`].
    pub instance: *mut c_void,
    /// Report identity and component schemas, once at load.
    ///
    /// Writes UTF-8 JSON into `manifest_json`, owned by the plugin. On failure,
    /// writes a UTF-8 message into `err` and returns a non-OK status.
    pub describe: Option<
        unsafe extern "C-unwind" fn(
            instance: *mut c_void,
            manifest_json: *mut SaciBuffer,
            err: *mut SaciBuffer,
        ) -> SaciStatus,
    >,
    /// Run one batch.
    ///
    /// `input` is Arrow IPC bytes for the batch. `prior` is the blob this
    /// plugin returned for the previous batch, valid only when `has_prior` is
    /// non-zero. On success, fills `out`. On failure, writes a UTF-8 message
    /// into `err` and returns [`SaciStatus::RETRYABLE`] or
    /// [`SaciStatus::PERMANENT`].
    pub run_batch: Option<
        unsafe extern "C-unwind" fn(
            instance: *mut c_void,
            input: SaciSlice,
            prior: SaciSlice,
            has_prior: i32,
            out: *mut SaciRunResult,
            err: *mut SaciBuffer,
        ) -> SaciStatus,
    >,
    /// Release a buffer this plugin allocated. A null buffer is a no-op.
    pub free_buffer: Option<unsafe extern "C" fn(instance: *mut c_void, buffer: SaciBuffer)>,
    /// Release [`SaciPluginV1::instance`]. Called once, last.
    pub destroy: Option<unsafe extern "C" fn(instance: *mut c_void)>,
}

impl SaciPluginV1 {
    /// A vtable with no instance and no slots filled.
    ///
    /// The host creates one of these, hands it to `saci_plugin_v1`, and then
    /// checks every slot. This is why the ABI needs no `MaybeUninit` dance: all
    /// zeroes is a valid, inert value.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            instance: core::ptr::null_mut(),
            describe: None,
            run_batch: None,
            free_buffer: None,
            destroy: None,
        }
    }

    /// The name of the first unfilled slot, if any.
    ///
    /// A host reports this instead of calling through a null pointer. `instance`
    /// is deliberately not checked: a stateless plugin may legitimately leave it
    /// null and ignore it in every call.
    #[must_use]
    pub const fn missing_slot(&self) -> Option<&'static str> {
        if self.describe.is_none() {
            Some("describe")
        } else if self.run_batch.is_none() {
            Some("run_batch")
        } else if self.free_buffer.is_none() {
            Some("free_buffer")
        } else if self.destroy.is_none() {
            Some("destroy")
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, size_of};

    // These lock the Rust layout against include/saci_plugin.h. A field added on
    // one side and not the other changes a size or an offset and fails here.
    #[test]
    fn layout_matches_the_c_header() {
        assert_eq!(size_of::<SaciSlice>(), 16);
        assert_eq!(align_of::<SaciSlice>(), 8);

        assert_eq!(size_of::<SaciBuffer>(), 24);
        assert_eq!(align_of::<SaciBuffer>(), 8);

        assert_eq!(size_of::<SaciStatus>(), 4);
        assert_eq!(align_of::<SaciStatus>(), 4);

        assert_eq!(size_of::<SaciRunMetrics>(), 32);
        assert_eq!(align_of::<SaciRunMetrics>(), 8);

        assert_eq!(size_of::<SaciRunResult>(), 120);
        assert_eq!(align_of::<SaciRunResult>(), 8);

        assert_eq!(size_of::<SaciHostV1>(), 32);
        assert_eq!(align_of::<SaciHostV1>(), 8);

        assert_eq!(size_of::<SaciPluginV1>(), 40);
        assert_eq!(align_of::<SaciPluginV1>(), 8);
    }

    // An Option-wrapped extern fn must stay pointer-sized, or the header's bare
    // function pointers would not line up with these structs.
    #[test]
    fn optional_callbacks_are_pointer_sized() {
        assert_eq!(
            size_of::<Option<unsafe extern "C" fn(*mut c_void)>>(),
            size_of::<*mut c_void>()
        );
    }

    #[test]
    fn version_compatibility_matches_the_documented_rule() {
        assert_eq!(abi_major(SACI_ABI_VERSION), 1);
        assert_eq!(abi_minor(SACI_ABI_VERSION), 2);

        assert!(abi_is_compatible(SACI_ABI_VERSION));
        // A newer minor may need vtable slots this host cannot fill. Accepting
        // an OLDER minor is the interesting half of the rule, and it needs a
        // parameterised host version to test, which `check_abi_version` in the
        // host's loader provides.
        assert!(abi_is_compatible(0x0001_0000));
        assert!(!abi_is_compatible(0x0001_0003));
        assert!(!abi_is_compatible(0x0001_ffff));
        // A different major is a layout change either way.
        assert!(!abi_is_compatible(0x0002_0000));
        assert!(!abi_is_compatible(0x0000_ffff));
    }

    #[test]
    fn zeroed_vtable_names_its_first_missing_slot() {
        let mut vtable = SaciPluginV1::empty();
        assert_eq!(vtable.missing_slot(), Some("describe"));

        unsafe extern "C-unwind" fn describe(
            _: *mut c_void,
            _: *mut SaciBuffer,
            _: *mut SaciBuffer,
        ) -> SaciStatus {
            SaciStatus::OK
        }
        vtable.describe = Some(describe);
        assert_eq!(vtable.missing_slot(), Some("run_batch"));
    }

    #[test]
    fn empty_slices_and_buffers_never_dereference_null() {
        let slice = SaciSlice::empty();
        assert!(slice.is_empty());
        assert!(unsafe { slice.as_bytes() }.is_empty());

        let buffer = SaciBuffer::null();
        assert!(buffer.is_null());
        assert!(unsafe { buffer.as_bytes() }.is_empty());

        // A non-null pointer with a zero length is still empty.
        let bytes: [u8; 4] = [1, 2, 3, 4];
        let zero_len = SaciSlice {
            ptr: bytes.as_ptr(),
            len: 0,
        };
        assert!(zero_len.is_empty());
        assert!(unsafe { zero_len.as_bytes() }.is_empty());
    }

    #[test]
    fn slices_round_trip_their_bytes() {
        let bytes: [u8; 5] = [9, 8, 7, 6, 5];
        let slice = SaciSlice::from_bytes(&bytes);
        assert_eq!(slice.len, 5);
        assert_eq!(unsafe { slice.as_bytes() }, &bytes[..]);
    }

    #[test]
    fn an_empty_run_result_reads_as_absent() {
        let result = SaciRunResult::empty();
        assert!(result.output.is_null());
        assert!(result.checkpoint.is_null());
        assert_eq!(result.has_checkpoint, 0);
        assert_eq!(result.metrics.rows_in, 0);
    }

    #[test]
    fn status_constants_are_distinct_and_ok_is_zero() {
        assert!(SaciStatus::OK.is_ok());
        assert!(!SaciStatus::RETRYABLE.is_ok());
        assert!(!SaciStatus::PERMANENT.is_ok());
        assert!(!SaciStatus::SCHEMA_MISMATCH.is_ok());
        assert_eq!(SaciStatus::OK.as_i32(), 0);
        assert_eq!(SaciStatus::SCHEMA_MISMATCH.as_i32(), 3);
    }

    // Proves the claim in the `# Unwinding` module doc on this actual target,
    // not just per the `extern "C-unwind"` RFC: a panic that reaches the
    // boundary of a function shaped like `SaciPluginV1::describe` unwinds
    // instead of aborting, and a caller's `catch_unwind` observes it as
    // `Err`. The struct-literal assignment below is what pins the field's
    // signature — this fails to compile if `describe` ever reverts to a
    // plain (nounwind) `extern "C"` function pointer.
    #[test]
    fn describe_slot_lets_a_panic_be_caught_at_the_call_site() {
        unsafe extern "C-unwind" fn panics(
            _instance: *mut c_void,
            _manifest_json: *mut SaciBuffer,
            _err: *mut SaciBuffer,
        ) -> SaciStatus {
            panic!("simulated plugin bug crossing the FFI boundary");
        }

        let vtable = SaciPluginV1 {
            describe: Some(panics),
            ..SaciPluginV1::empty()
        };
        let f = vtable.describe.expect("just set");

        let mut manifest = SaciBuffer::null();
        let mut err = SaciBuffer::null();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            f(core::ptr::null_mut(), &mut manifest, &mut err)
        }));

        assert!(
            result.is_err(),
            "a panic crossing an extern \"C-unwind\" boundary must be observable via \
             catch_unwind on this target, not silently lost or turned into a normal return"
        );
    }
}
