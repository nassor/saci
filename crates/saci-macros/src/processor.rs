//! `#[processor]`: the WIT world and the guest exports.
//!
//! This is the macro that makes a processor crate cfg-free. Everything a
//! WebAssembly component needs, the `wit_bindgen::generate!` expansion, the
//! `Guest` impl covering `describe` and `run-batch`, the host-io backend and
//! the `export!` handshake, is emitted here under
//! `#[cfg(target_arch = "wasm32")]`, so the author's source carries no target
//! gates at all. `fn build()` itself is left untouched and compiles for every
//! target, which keeps the pipeline testable on the host.
//!
//! The boundary logic lives in `saci::__private` as ordinary functions; the
//! expansion only maps between them and the bindings, which have to be
//! generated in the processor crate because WIT bindings are caller-side.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, ItemFn};

use crate::PIPELINE_WIT;

/// Reject a pipeline constructor that is not a plain `fn() -> Pipeline`.
pub(crate) fn check_build_fn(func: &ItemFn, macro_name: &str) -> syn::Result<()> {
    if !func.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig,
            format!("{macro_name} applies to a `fn() -> Pipeline`, which takes no arguments"),
        ));
    }
    if func.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            func.sig.asyncness,
            format!("{macro_name} applies to a synchronous `fn() -> Pipeline`"),
        ));
    }
    if !func.sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig.generics,
            format!("{macro_name} does not support generic parameters"),
        ));
    }
    Ok(())
}

/// Expand `#[processor]` over the pipeline constructor `func`.
pub(crate) fn expand(func: ItemFn) -> syn::Result<TokenStream> {
    check_build_fn(&func, "#[processor]")?;
    let build_fn = &func.sig.ident;
    let bindings = bindings_module();
    let guest = guest_impl(build_fn);

    Ok(quote! {
        #func
        #bindings
        #guest
    })
}

/// The `wit_bindgen::generate!` expansion, wrapped in a private module.
///
/// The world text is embedded via `inline:` rather than `path:`. A `path:` is
/// resolved against the *calling* crate's `CARGO_MANIFEST_DIR`, so every
/// processor crate would have to hand-write its own `../../..` walk to
/// `crates/saci/wit`. Inlining the text this crate already embedded at
/// its own compile time removes that per-crate path entirely.
///
/// `default_bindings_module` is required, not decorative: `export!` expands to
/// a `macro_rules!` whose body names the module it came from, and without this
/// option it emits `self::`, which resolves against the *invocation* site
/// (wit-bindgen issue #973).
fn bindings_module() -> TokenStream {
    quote! {
        #[cfg(target_arch = "wasm32")]
        #[allow(warnings)]
        mod bindings {
            ::wit_bindgen::generate!({
                inline: #PIPELINE_WIT,
                world: "saci-pipeline",
                default_bindings_module: "crate::bindings",
                generate_all
            });
        }
    }
}

/// The host-io backend, the `Guest` impl covering the two WIT exports, and
/// the `export!` handshake.
fn guest_impl(build_fn: &Ident) -> TokenStream {
    quote! {
        #[cfg(target_arch = "wasm32")]
        const _: () = {
            static __SACI_PIPELINE: ::saci::__private::PipelineSlot =
                ::saci::__private::PipelineSlot::new();

            /// `sdk::host` calls, answered through the `saci:pipeline/host-io`
            /// imports.
            struct __SaciHost;

            impl ::saci::__private::HostIo for __SaciHost {
                fn config(&self, key: &str) -> ::core::option::Option<::std::string::String> {
                    crate::bindings::saci::pipeline::host_io::get_config(key)
                }

                fn log(&self, level: ::saci::__private::Level, target: &str, message: &str) {
                    use crate::bindings::saci::pipeline::host_io::LogLevel;
                    let level = match level {
                        ::saci::__private::Level::Trace => LogLevel::Trace,
                        ::saci::__private::Level::Debug => LogLevel::Debug,
                        ::saci::__private::Level::Info => LogLevel::Info,
                        ::saci::__private::Level::Warn => LogLevel::Warn,
                        ::saci::__private::Level::Error => LogLevel::Error,
                    };
                    crate::bindings::saci::pipeline::host_io::log(level, target, message);
                }

                fn metric(&self, name: &str, value: f64) {
                    crate::bindings::saci::pipeline::host_io::metric(name, value);
                }
            }

            static __SACI_HOST: __SaciHost = __SaciHost;

            struct __SaciComponent;

            impl crate::bindings::exports::saci::pipeline::pipeline::Guest for __SaciComponent {
                fn describe()
                -> crate::bindings::exports::saci::pipeline::pipeline::PipelineDescriptor {
                    // `PipelineDescriptor` is re-exported inside the exports
                    // module because `interface pipeline` uses it directly.
                    // `ComponentDescriptor` is only reached transitively, so it
                    // comes from the package-level `types` module.
                    use crate::bindings::exports::saci::pipeline::pipeline::PipelineDescriptor;
                    use crate::bindings::saci::pipeline::types::ComponentDescriptor;

                    ::saci::__private::install_host(&__SACI_HOST);
                    let descriptor = ::saci::__private::describe(
                        __SACI_PIPELINE.pipeline(#build_fn),
                        env!("CARGO_PKG_VERSION"),
                    );
                    PipelineDescriptor {
                        name: descriptor.name,
                        version: descriptor.version,
                        components: descriptor
                            .components
                            .into_iter()
                            .map(|c| ComponentDescriptor {
                                name: c.name,
                                arrow_schema_ipc: c.arrow_schema_ipc,
                            })
                            .collect(),
                        stateful: descriptor.stateful,
                        schema_fingerprint: descriptor.schema_fingerprint,
                    }
                }

                async fn run_batch(
                    input: ::std::vec::Vec<u8>,
                    prior: ::core::option::Option<::std::vec::Vec<u8>>,
                ) -> ::core::result::Result<
                    crate::bindings::exports::saci::pipeline::pipeline::RunResult,
                    crate::bindings::exports::saci::pipeline::pipeline::RunError,
                > {
                    // `RunResult` / `RunError` are re-exported in the exports
                    // module; `RunMetrics` is only reached transitively, so it
                    // comes from the package-level `types` module.
                    use crate::bindings::exports::saci::pipeline::pipeline::{RunError, RunResult};
                    use crate::bindings::saci::pipeline::types::RunMetrics;

                    ::saci::__private::install_host(&__SACI_HOST);
                    match ::saci::__private::run_batch(
                        __SACI_PIPELINE.pipeline(#build_fn),
                        &input,
                        prior.as_deref(),
                    )
                    .await
                    {
                        ::core::result::Result::Ok(batch) => {
                            let m = batch.metrics;
                            ::core::result::Result::Ok(RunResult {
                                output: batch.output,
                                checkpoint: batch.checkpoint,
                                metrics: RunMetrics {
                                    wall_ns: m.wall_ns,
                                    rows_in: m.rows_in,
                                    rows_out: m.rows_out,
                                    systems_run: m.systems_run,
                                    retries: m.retries,
                                },
                                routes: batch.routes,
                            })
                        }
                        ::core::result::Result::Err(e) if e.retryable => {
                            ::core::result::Result::Err(RunError::Retryable(e.message))
                        }
                        ::core::result::Result::Err(e) => {
                            ::core::result::Result::Err(RunError::Permanent(e.message))
                        }
                    }
                }
            }

            crate::bindings::export!(__SaciComponent with_types_in crate::bindings);
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item_fn(source: &str) -> ItemFn {
        syn::parse_str::<ItemFn>(source).expect("test fixture must parse")
    }

    #[test]
    fn the_embedded_wit_is_the_pipeline_world_saci_owns() {
        assert!(PIPELINE_WIT.starts_with("package saci:pipeline@0.4.0;"));
        assert!(PIPELINE_WIT.contains("world saci-pipeline {"));
        assert!(PIPELINE_WIT.contains("run-batch: async func("));
    }

    #[test]
    fn the_expansion_delegates_to_the_shared_glue() {
        let out = expand(item_fn("pub fn build() -> Pipeline { todo!() }"))
            .unwrap()
            .to_string();

        assert!(out.contains("install_host"), "{out}");
        assert!(out.contains(":: saci :: __private :: run_batch"), "{out}");
        assert!(out.contains("inline :"), "{out}");
        assert!(out.contains("default_bindings_module"), "{out}");
        assert!(
            out.contains("export ! (__SaciComponent with_types_in crate :: bindings)"),
            "{out}"
        );
        assert!(!out.contains("saci_config_get"), "{out}");
        assert!(!out.contains("Stateful"), "{out}");
        assert!(!out.contains("NoState"), "{out}");
    }

    #[test]
    fn every_wasm_only_item_carries_its_own_target_gate() {
        let out = expand(item_fn("pub fn build() -> Pipeline { todo!() }"))
            .unwrap()
            .to_string();

        // The bindings module and the guest const block, nothing else.
        assert_eq!(
            out.matches("cfg (target_arch = \"wasm32\")").count(),
            2,
            "{out}"
        );
        assert_eq!(
            out.matches("cfg (not (target_arch = \"wasm32\"))").count(),
            0,
            "{out}"
        );

        // `build` itself must stay unconditional: the host build of a processor
        // crate has to be able to construct the pipeline.
        let build_at = out.find("fn build ()").expect("build is re-emitted");
        let first_gate = out.find("# [cfg (target_arch").expect("a gate exists");
        assert!(build_at < first_gate, "{out}");
    }

    #[test]
    fn a_processor_taking_arguments_is_rejected() {
        let err = expand(item_fn("fn build(x: u8) -> Pipeline { todo!() }")).unwrap_err();
        assert!(err.to_string().contains("takes no arguments"), "{err}");
    }
}
