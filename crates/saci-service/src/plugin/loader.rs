//! Opening a shared library and resolving its vtable.
//!
//! Everything unsound about a plugin is caught here: an unloadable file, a
//! missing entry point, an ABI version this host cannot drive, a failed
//! initialisation, an unfilled vtable slot. Past [`LoadedPlugin::open`] every
//! slot is a plain function pointer, so no call site checks for `None`.

use std::ffi::c_void;
use std::path::Path;

use libloading::{Library, Symbol};
use saci_core::{SaciError, SaciResult};
use saci_plugin_abi::{
    SACI_ABI_VERSION, SaciBuffer, SaciHostV1, SaciPluginV1, SaciRunResult, SaciSlice, SaciStatus,
    abi_major, abi_minor,
};

/// `uint32_t saci_abi_version(void)`.
type AbiVersionFn = unsafe extern "C" fn() -> u32;

/// `SaciStatus saci_plugin_v1(const SaciHostV1 *, SaciPluginV1 *)`.
type PluginInitFn = unsafe extern "C" fn(*const SaciHostV1, *mut SaciPluginV1) -> SaciStatus;

/// [`SaciPluginV1::describe`] with the slot's `Option` discharged.
pub(crate) type DescribeFn =
    unsafe extern "C-unwind" fn(*mut c_void, *mut SaciBuffer, *mut SaciBuffer) -> SaciStatus;

/// [`SaciPluginV1::run_batch`] with the slot's `Option` discharged.
pub(crate) type RunBatchFn = unsafe extern "C-unwind" fn(
    *mut c_void,
    SaciSlice,
    SaciSlice,
    i32,
    *mut SaciRunResult,
    *mut SaciBuffer,
) -> SaciStatus;

/// [`SaciPluginV1::free_buffer`] with the slot's `Option` discharged.
type FreeBufferFn = unsafe extern "C" fn(*mut c_void, SaciBuffer);

/// [`SaciPluginV1::destroy`] with the slot's `Option` discharged.
type DestroyFn = unsafe extern "C" fn(*mut c_void);

/// The vtable after [`LoadedPlugin::open`] has proven every slot present.
struct Calls {
    instance: *mut c_void,
    describe: DescribeFn,
    run_batch: RunBatchFn,
    free_buffer: FreeBufferFn,
    destroy: DestroyFn,
}

/// A plugin instance and everything that has to outlive it.
pub(crate) struct LoadedPlugin {
    /// Field order is load-bearing. `Drop for LoadedPlugin` calls `destroy`
    /// first, then Rust drops the fields in declaration order, so the instance
    /// is released while the library is still mapped and while the host vtable
    /// it was handed is still alive. Unmapping the library first would turn the
    /// `destroy` call into a jump into freed pages.
    calls: Calls,
    /// The plugin stored this pointer at init and calls back through it, so it
    /// outlives every call the plugin can make, `destroy` included.
    _host: Box<SaciHostV1>,
    /// Unmapped last.
    _library: Library,
}

impl LoadedPlugin {
    /// Load `path`, check its ABI version, and initialise one instance.
    ///
    /// `host` is the vtable the plugin calls back through. It moves in here
    /// because the plugin keeps the pointer for its whole life.
    pub(crate) fn open(path: &Path, host: Box<SaciHostV1>) -> SaciResult<Self> {
        // SAFETY: loading arbitrary code is the point of this module. The
        // library's initialisers run with host privileges, which is why the
        // plugin path is operator-trusted.
        let library = unsafe { Library::new(path) }.map_err(|e| {
            SaciError::configuration(format!(
                "cannot load plugin library `{}`: {e}",
                path.display()
            ))
        })?;

        let version = {
            let symbol: Symbol<'_, AbiVersionFn> = unsafe { library.get(b"saci_abi_version\0") }
                .map_err(|e| {
                    SaciError::configuration(format!(
                        "plugin library `{}` does not export `saci_abi_version`: {e}",
                        path.display()
                    ))
                })?;
            unsafe { symbol() }
        };

        check_abi_version(version, SACI_ABI_VERSION).map_err(|detail| {
            SaciError::configuration(format!("plugin library `{}`: {detail}", path.display()))
        })?;

        let mut vtable = SaciPluginV1::empty();
        let status = {
            let symbol: Symbol<'_, PluginInitFn> = unsafe { library.get(b"saci_plugin_v1\0") }
                .map_err(|e| {
                    SaciError::configuration(format!(
                        "plugin library `{}` does not export `saci_plugin_v1`: {e}",
                        path.display()
                    ))
                })?;
            unsafe { symbol(std::ptr::from_ref(host.as_ref()), &mut vtable) }
        };

        if !status.is_ok() {
            return Err(SaciError::configuration(format!(
                "plugin library `{}` returned status {} from `saci_plugin_v1`",
                path.display(),
                status.as_i32()
            )));
        }

        if let Some(slot) = vtable.missing_slot() {
            // Nothing to call: a plugin that left `destroy` or any other slot
            // empty cannot be told to release whatever it just built.
            return Err(SaciError::configuration(format!(
                "plugin library `{}` left vtable slot `{slot}` unfilled",
                path.display()
            )));
        }

        let SaciPluginV1 {
            instance,
            describe: Some(describe),
            run_batch: Some(run_batch),
            free_buffer: Some(free_buffer),
            destroy: Some(destroy),
        } = vtable
        else {
            // `missing_slot` returned `None` just above, so every slot is
            // `Some`. This arm exists so the destructuring needs no panic.
            return Err(SaciError::configuration(format!(
                "plugin library `{}` filled its vtable inconsistently",
                path.display()
            )));
        };

        Ok(Self {
            calls: Calls {
                instance,
                describe,
                run_batch,
                free_buffer,
                destroy,
            },
            _host: host,
            _library: library,
        })
    }

    /// The opaque instance every vtable call takes as its first argument.
    pub(crate) fn instance(&self) -> *mut c_void {
        self.calls.instance
    }

    /// The plugin's `describe` entry point.
    pub(crate) fn describe_fn(&self) -> DescribeFn {
        self.calls.describe
    }

    /// The plugin's `run_batch` entry point.
    pub(crate) fn run_batch_fn(&self) -> RunBatchFn {
        self.calls.run_batch
    }

    /// Copy a plugin-owned buffer out and hand the original straight back.
    ///
    /// Every buffer a plugin writes is allocated by the plugin, so the host
    /// copies what it needs and returns the buffer to the plugin's own
    /// `free_buffer`. This is the only way a call site should read one: it
    /// cannot forget the free. A null buffer yields an empty vector and makes
    /// no call, which is what the ABI defines as a no-op.
    pub(crate) fn take_buffer(&self, buffer: SaciBuffer) -> Vec<u8> {
        if buffer.is_null() {
            return Vec::new();
        }

        // SAFETY: the plugin wrote this buffer and promises `len` initialised
        // bytes at `ptr`. The copy finishes before the buffer goes back.
        let copied = unsafe { buffer.as_bytes() }.to_vec();
        unsafe { (self.calls.free_buffer)(self.calls.instance, buffer) };
        copied
    }
}

impl Drop for LoadedPlugin {
    fn drop(&mut self) {
        // Exactly once, and before any field drops, so the library is still
        // mapped and the host vtable still alive.
        unsafe { (self.calls.destroy)(self.calls.instance) };
    }
}

// SAFETY: the ABI states the host never calls into one instance concurrently,
// and a `LoadedPlugin` has exactly one owner, so successive calls are ordered
// even when they land on different threads. The raw pointers here are the
// plugin instance and the library handle, neither of which is thread affine.
// `PipelineRuntime: Send` requires this.
unsafe impl Send for LoadedPlugin {}

/// Whether a host at ABI version `host` can drive a plugin reporting `plugin`.
///
/// The major must match exactly, and the plugin's minor must be no greater than
/// the host's. Parameterising on the host version keeps the rule testable at
/// minor versions this host has not reached yet. [`LoadedPlugin::open`] always
/// passes [`SACI_ABI_VERSION`], where this is
/// [`saci_plugin_abi::abi_is_compatible`]; a test below pins the two together.
fn check_abi_version(plugin: u32, host: u32) -> Result<(), String> {
    if abi_major(plugin) == abi_major(host) && abi_minor(plugin) <= abi_minor(host) {
        return Ok(());
    }

    Err(format!(
        "plugin ABI version {}.{} is incompatible with host ABI version {}.{}",
        abi_major(plugin),
        abi_minor(plugin),
        abi_major(host),
        abi_minor(host)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use saci_plugin_abi::abi_is_compatible;

    const fn version(major: u16, minor: u16) -> u32 {
        ((major as u32) << 16) | (minor as u32)
    }

    #[test]
    fn accepts_the_hosts_own_version() {
        assert!(check_abi_version(SACI_ABI_VERSION, SACI_ABI_VERSION).is_ok());
    }

    #[test]
    fn accepts_a_lower_minor() {
        assert!(check_abi_version(version(1, 2), version(1, 7)).is_ok());
        assert!(check_abi_version(version(1, 0), version(1, 7)).is_ok());
    }

    #[test]
    fn rejects_a_higher_minor() {
        let err = check_abi_version(version(1, 8), version(1, 7)).unwrap_err();
        assert!(err.contains("1.8"), "{err}");
        assert!(err.contains("1.7"), "{err}");
    }

    #[test]
    fn rejects_a_different_major() {
        let err = check_abi_version(version(2, 0), version(1, 0)).unwrap_err();
        assert!(err.contains("2.0"), "{err}");
        assert!(err.contains("1.0"), "{err}");

        assert!(check_abi_version(version(0, 0), version(1, 0)).is_err());
    }

    #[test]
    fn agrees_with_the_abi_crates_own_gate() {
        for candidate in [
            SACI_ABI_VERSION,
            version(0, 0),
            version(0, 9),
            version(1, 1),
            version(2, 0),
        ] {
            assert_eq!(
                check_abi_version(candidate, SACI_ABI_VERSION).is_ok(),
                abi_is_compatible(candidate),
                "disagreement at {candidate:#010x}"
            );
        }
    }
}
