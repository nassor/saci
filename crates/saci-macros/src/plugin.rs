//! `#[plugin]`: the native plugin C ABI exports for a `fn() -> Pipeline`.
//!
//! Emits the two `#[no_mangle]` symbols the host looks up and the four vtable
//! thunks they install. Every thunk forwards to a function in
//! `saci::__private::plugin`, so the boundary logic exists once in `saci` and
//! the expansion stays a handful of one-line bodies.

use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

use crate::processor::check_build_fn;

/// Expand `#[plugin]` over the pipeline constructor `func`.
pub(crate) fn expand(func: ItemFn) -> syn::Result<TokenStream> {
    check_build_fn(&func, "#[plugin]")?;
    let build_fn = &func.sig.ident;

    Ok(quote! {
        #func

        /// The ABI version this plugin was built against.
        #[unsafe(no_mangle)]
        pub extern "C" fn saci_abi_version() -> u32 {
            ::saci::__private::plugin::SACI_ABI_VERSION
        }

        unsafe extern "C-unwind" fn __saci_plugin_describe(
            instance: *mut ::core::ffi::c_void,
            manifest_json: *mut ::saci::__private::plugin::SaciBuffer,
            err: *mut ::saci::__private::plugin::SaciBuffer,
        ) -> ::saci::__private::plugin::SaciStatus {
            unsafe {
                ::saci::__private::plugin::describe_impl(
                    instance,
                    #build_fn,
                    ::core::env!("CARGO_PKG_VERSION"),
                    manifest_json,
                    err,
                )
            }
        }

        unsafe extern "C-unwind" fn __saci_plugin_run_batch(
            instance: *mut ::core::ffi::c_void,
            input: ::saci::__private::plugin::SaciSlice,
            prior: ::saci::__private::plugin::SaciSlice,
            has_prior: i32,
            out: *mut ::saci::__private::plugin::SaciRunResult,
            err: *mut ::saci::__private::plugin::SaciBuffer,
        ) -> ::saci::__private::plugin::SaciStatus {
            unsafe {
                ::saci::__private::plugin::run_batch_impl(
                    instance, #build_fn, input, prior, has_prior, out, err,
                )
            }
        }

        unsafe extern "C" fn __saci_plugin_free_buffer(
            _instance: *mut ::core::ffi::c_void,
            buffer: ::saci::__private::plugin::SaciBuffer,
        ) {
            unsafe { ::saci::__private::plugin::free_buffer(buffer) }
        }

        unsafe extern "C" fn __saci_plugin_destroy(instance: *mut ::core::ffi::c_void) {
            unsafe { ::saci::__private::plugin::PluginInstance::from_raw(instance) }
        }

        /// Hand the host this plugin's vtable.
        ///
        /// # Safety
        ///
        /// `host` must be null or point at a valid vtable that outlives the
        /// instance. `out` must point at writable storage for one
        /// `SaciPluginV1`.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn saci_plugin_v1(
            host: *const ::saci::__private::plugin::SaciHostV1,
            out: *mut ::saci::__private::plugin::SaciPluginV1,
        ) -> ::saci::__private::plugin::SaciStatus {
            if out.is_null() {
                return ::saci::__private::plugin::SaciStatus::PERMANENT;
            }
            // A null host is tolerated: the plugin then has no config, no
            // logging and no metrics, which is exactly what an empty vtable
            // gives it.
            let host = if host.is_null() {
                ::saci::__private::plugin::SaciHostV1::empty()
            } else {
                unsafe { *host }
            };
            let instance = ::saci::__private::plugin::PluginInstance::new(host).into_raw();
            unsafe {
                *out = ::saci::__private::plugin::SaciPluginV1 {
                    instance,
                    describe: ::core::option::Option::Some(__saci_plugin_describe),
                    run_batch: ::core::option::Option::Some(__saci_plugin_run_batch),
                    free_buffer: ::core::option::Option::Some(__saci_plugin_free_buffer),
                    destroy: ::core::option::Option::Some(__saci_plugin_destroy),
                };
            }
            ::saci::__private::plugin::SaciStatus::OK
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item_fn(source: &str) -> ItemFn {
        syn::parse_str::<ItemFn>(source).expect("test fixture must parse")
    }

    #[test]
    fn the_expansion_exports_both_abi_symbols_with_no_target_gate() {
        let out = expand(item_fn("fn build() -> Pipeline { Pipeline::new(\"p\") }"))
            .unwrap()
            .to_string();
        assert!(out.contains("fn saci_abi_version"), "{out}");
        assert!(out.contains("fn saci_plugin_v1"), "{out}");
        assert!(out.contains("describe_impl"), "{out}");
        assert!(!out.contains("target_arch"), "{out}");
        assert!(!out.contains("Stateful"), "{out}");
    }

    #[test]
    fn a_function_with_arguments_is_rejected() {
        let err = expand(item_fn("fn build(x: u8) -> Pipeline { todo!() }")).unwrap_err();
        assert!(err.to_string().contains("#[plugin]"), "{err}");
    }
}
