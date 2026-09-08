//! `#[processor]`: the WIT world, the guest exports, and the host-io shims.
//!
//! This is the macro that makes a processor crate cfg-free. Everything a
//! WebAssembly component needs — the `wit_bindgen::generate!` expansion, the
//! `Guest` impl covering `describe` and `run-batch`, the `export!` handshake,
//! and the four host-io shims — is emitted here under
//! `#[cfg(target_arch = "wasm32")]`, so the author's source carries no target
//! gates at all. `fn build()` itself is left untouched and compiles for every
//! target, which keeps the pipeline testable on the host.
//!
//! # Why the shims cannot be library functions
//!
//! `wit_bindgen::generate!` emits its bindings into the crate where it
//! textually expands. `crate::bindings` therefore only exists inside the
//! processor crate, and any function reading a host-io import has to be emitted
//! there too. `saci_config_get`, `saci_config_parse`, `log` and `metric` are that
//! set.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, ItemFn, LitStr, Type};

use crate::PIPELINE_WIT;
use crate::util::eat_comma;

/// `#[processor(name = "...", state = Ledger)]`.
pub(crate) struct ProcessorArgs {
    /// The pipeline identity the host gates config and checkpoint
    /// compatibility on. Reported as `pipeline-descriptor.name`.
    name: LitStr,
    /// The one component whose rows survive across `run-batch` calls. `None`
    /// makes the processor stateless.
    state: Option<Type>,
}

impl Parse for ProcessorArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut name: Option<LitStr> = None;
        let mut state: Option<Type> = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<syn::Token![=]>()?;
            match key.to_string().as_str() {
                "name" => {
                    if name.is_some() {
                        return Err(syn::Error::new(
                            key.span(),
                            "`name` is given more than once",
                        ));
                    }
                    name = Some(input.parse()?);
                }
                "state" => crate::util::parse_type_arg(input, &key, &mut state)?,
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        "unknown #[processor] argument; the two are `name = \"...\"` and \
                         `state = <Type>`",
                    ));
                }
            }
            if !eat_comma(input)? {
                break;
            }
        }

        let name = name.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "#[processor] requires `name = \"...\"`",
            )
        })?;
        Ok(Self { name, state })
    }
}

/// Expand `#[processor(...)]` over the pipeline constructor `func`.
pub(crate) fn expand(args: ProcessorArgs, func: ItemFn) -> syn::Result<TokenStream> {
    if !func.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "#[processor] applies to a `fn() -> Pipeline`, which takes no arguments",
        ));
    }
    if func.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            func.sig.asyncness,
            "#[processor] applies to a synchronous `fn() -> Pipeline`",
        ));
    }
    if !func.sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig.generics,
            "#[processor] does not support generic parameters",
        ));
    }

    let build_fn = &func.sig.ident;
    let ProcessorArgs { name, state } = &args;

    let spec = match state {
        Some(state) => quote! { ::saci_processor::__rt::Stateful<#state> },
        None => quote! { ::saci_processor::__rt::NoState },
    };

    let bindings = bindings_module();
    let host_io = host_io_shims();
    let guest = guest_impl(build_fn, name, &spec);

    Ok(quote! {
        #func
        #bindings
        #host_io
        #guest
    })
}

/// The `wit_bindgen::generate!` expansion, wrapped in a private module.
///
/// The world text is embedded via `inline:` rather than `path:`. A `path:` is
/// resolved against the *calling* crate's `CARGO_MANIFEST_DIR`, so every
/// processor crate would have to hand-write its own `../../..` walk to
/// `crates/saci-processor/wit`. Inlining the text this crate already embedded at
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

/// The four host-io shims, each with a host-target stand-in.
fn host_io_shims() -> TokenStream {
    quote! {
        /// Read a `config` value the host injected via the `wasm` node of the
        /// service config.
        ///
        /// Backed by the `saci:pipeline/host-io` `get-config` import, so the
        /// value is available in every export including `describe`. Returns
        /// `None` on a host build, where there is no import to call.
        #[cfg(target_arch = "wasm32")]
        pub fn saci_config_get(key: &str) -> ::core::option::Option<::std::string::String> {
            crate::bindings::saci::pipeline::host_io::get_config(key)
        }

        /// Host-target stand-in: there is no host-io import to call.
        #[cfg(not(target_arch = "wasm32"))]
        pub fn saci_config_get(_key: &str) -> ::core::option::Option<::std::string::String> {
            ::core::option::Option::None
        }

        /// Read and parse a host-injected config value.
        ///
        /// `None` means the key was absent; `Some(Err(_))` means it was present
        /// but did not parse as `T`.
        pub fn saci_config_parse<T: ::core::str::FromStr>(
            key: &str,
        ) -> ::core::option::Option<::core::result::Result<T, T::Err>> {
            saci_config_get(key).map(|value| value.parse::<T>())
        }

        /// Emit an info-level log line through `saci:pipeline/host-io`, bridged
        /// to `tracing` on the host.
        #[cfg(target_arch = "wasm32")]
        pub fn log(target: &str, message: &str) {
            use crate::bindings::saci::pipeline::host_io::{LogLevel, log as __saci_host_log};
            __saci_host_log(LogLevel::Info, target, message);
        }

        /// Host-target stand-in: there is no host-io import to call.
        #[cfg(not(target_arch = "wasm32"))]
        pub fn log(_target: &str, _message: &str) {}

        /// Observe a named metric through `saci:pipeline/host-io`. The host
        /// records it as the `saci_processor_metric` histogram, labelled
        /// `metric="<name>"`.
        #[cfg(target_arch = "wasm32")]
        pub fn metric(name: &str, value: f64) {
            crate::bindings::saci::pipeline::host_io::metric(name, value);
        }

        /// Host-target stand-in: there is no host-io import to call.
        #[cfg(not(target_arch = "wasm32"))]
        pub fn metric(_name: &str, _value: f64) {}
    }
}

/// The `Guest` impl covering the two WIT exports, plus the `export!`
/// handshake.
///
/// A straight port of `saci_processor::export_pipeline!`: same descriptor
/// assembly, same state hooks, same error classification, same `RunMetrics`
/// arithmetic, same `RouteDecision` read-back.
fn guest_impl(build_fn: &Ident, name: &LitStr, spec: &TokenStream) -> TokenStream {
    quote! {
        #[cfg(target_arch = "wasm32")]
        const _: () = {
            use ::saci_processor::__rt::ProcessorStateSpec as _;

            static __SACI_PIPELINE: ::saci_processor::__rt::PipelineSlot =
                ::saci_processor::__rt::PipelineSlot::new();

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

                    let __saci_pipeline = __SACI_PIPELINE.pipeline(#build_fn);
                    let __saci_registry = __saci_pipeline.data.schemas();

                    // Sorted by component name, so the WIT
                    // `components: list<component-descriptor>` order is
                    // deterministic across runs and across host and processor.
                    let mut __saci_entries: Vec<(
                        &'static str,
                        ::std::sync::Arc<::saci_processor::__rt::Schema>,
                    )> = __saci_registry
                        .iter()
                        .map(|(name, entry)| (*name, entry.schema.clone()))
                        .collect();
                    __saci_entries.sort_by_key(|(name, _)| *name);

                    let components: Vec<ComponentDescriptor> = __saci_entries
                        .into_iter()
                        .map(|(name, schema)| {
                            // On a schema-to-IPC failure, emit an empty
                            // descriptor rather than trapping: the host's
                            // load-time validation rejects zero-length schema
                            // bytes with a clean error.
                            let arrow_schema_ipc =
                                ::saci_processor::__rt::schema_to_ipc_bytes(&schema)
                                    .unwrap_or_default();
                            ComponentDescriptor {
                                name: name.to_string(),
                                arrow_schema_ipc,
                            }
                        })
                        .collect();

                    let fingerprint =
                        ::saci_processor::__rt::fingerprint_hex(__saci_registry.fingerprint());

                    PipelineDescriptor {
                        name: #name.to_string(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                        components,
                        stateful: <#spec>::STATEFUL,
                        schema_fingerprint: fingerprint,
                    }
                }

                fn run_batch(
                    input: Vec<u8>,
                    prior: Option<Vec<u8>>,
                ) -> Result<
                    crate::bindings::exports::saci::pipeline::pipeline::RunResult,
                    crate::bindings::exports::saci::pipeline::pipeline::RunError,
                > {
                    // `RunResult` / `RunError` are re-exported in the exports
                    // module; `RunMetrics` is only reached transitively, so it
                    // comes from the package-level `types` module.
                    use crate::bindings::exports::saci::pipeline::pipeline::{RunError, RunResult};
                    use crate::bindings::saci::pipeline::types::RunMetrics;

                    let start = ::std::time::Instant::now();

                    let mut reader: &[u8] = &input[..];
                    let mut dataset = ::saci_processor::__rt::Dataset::read_ipc(&mut reader)
                        .map_err(|e| RunError::Permanent(format!("ipc decode: {e}")))?;

                    // Measured before the state blob is merged, so `rows_in`
                    // reports the data plane only.
                    let rows_in = dataset.rows() as u64;

                    <#spec>::restore(&mut dataset, prior.as_deref())
                        .map_err(|e| RunError::Permanent(format!("state restore: {e}")))?;

                    let pipeline = __SACI_PIPELINE.pipeline(#build_fn);
                    let run_result = ::saci_processor::__rt::pollster::block_on(
                        pipeline.run_on_with_stats(&mut dataset),
                    );

                    let stats = match run_result {
                        Ok(stats) => stats,
                        Err(err) => {
                            let (is_retryable, msg) =
                                ::saci_processor::__rt::classify_run_error(&err);
                            return Err(if is_retryable {
                                RunError::Retryable(msg)
                            } else {
                                RunError::Permanent(msg)
                            });
                        }
                    };

                    let rows_out = dataset.rows() as u64;

                    let checkpoint = <#spec>::capture(&dataset)
                        .map_err(|e| RunError::Permanent(format!("state capture: {e}")))?;
                    let routes: Option<Vec<String>> = dataset
                        .get_resource::<::saci_processor::__rt::RouteDecision>()
                        .map(|decision| decision.0.clone());

                    let mut output: Vec<u8> = Vec::new();
                    dataset
                        .write_ipc(&mut output)
                        .map_err(|e| RunError::Permanent(format!("ipc encode: {e}")))?;

                    let metrics = RunMetrics {
                        wall_ns: start.elapsed().as_nanos() as u64,
                        rows_in,
                        rows_out,
                        systems_run: stats.systems_run as u32,
                        retries: stats.retries_this_batch,
                    };

                    Ok(RunResult {
                        output,
                        checkpoint,
                        metrics,
                        routes,
                    })
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
    fn the_embedded_wit_is_the_pipeline_world_saci_processor_owns() {
        assert!(PIPELINE_WIT.starts_with("package saci:pipeline@0.3.0;"));
        assert!(PIPELINE_WIT.contains("world saci-pipeline {"));
        assert!(PIPELINE_WIT.contains("run-batch: func("));
    }

    #[test]
    fn a_stateful_processor_selects_the_stateful_spec() {
        let args = syn::parse_str::<ProcessorArgs>("name = \"demo\", state = Ledger").unwrap();
        let out = expand(args, item_fn("pub fn build() -> Pipeline { todo!() }"))
            .unwrap()
            .to_string();

        assert!(out.contains("Stateful < Ledger >"), "{out}");
        assert!(out.contains("inline :"), "{out}");
        assert!(out.contains("default_bindings_module"), "{out}");
        assert!(
            out.contains("export ! (__SaciComponent with_types_in crate :: bindings)"),
            "{out}"
        );
        assert!(out.contains("\"demo\" . to_string ()"), "{out}");
    }

    #[test]
    fn a_stateless_processor_selects_no_state() {
        let args = syn::parse_str::<ProcessorArgs>("name = \"demo\"").unwrap();
        let out = expand(args, item_fn("fn build() -> Pipeline { todo!() }"))
            .unwrap()
            .to_string();

        assert!(out.contains("NoState"), "{out}");
        assert!(!out.contains("Stateful"), "{out}");
    }

    #[test]
    fn every_wasm_only_item_carries_its_own_target_gate() {
        let args = syn::parse_str::<ProcessorArgs>("name = \"demo\", state = Ledger").unwrap();
        let out = expand(args, item_fn("pub fn build() -> Pipeline { todo!() }"))
            .unwrap()
            .to_string();

        // The bindings module, the guest const block, and both halves of the
        // two gated shims: five `target_arch = "wasm32"` gates plus two
        // negated ones.
        let gated = out.matches("cfg (target_arch = \"wasm32\")").count();
        let host_only = out.matches("cfg (not (target_arch = \"wasm32\"))").count();
        assert_eq!(gated, 5, "{out}");
        assert_eq!(host_only, 3, "{out}");

        // `build` itself must stay unconditional: the host build of a processor
        // crate has to be able to construct the pipeline.
        let build_at = out.find("fn build ()").expect("build is re-emitted");
        let first_gate = out.find("# [cfg (target_arch").expect("a gate exists");
        assert!(build_at < first_gate, "{out}");
    }

    #[test]
    fn a_processor_taking_arguments_is_rejected() {
        let args = syn::parse_str::<ProcessorArgs>("name = \"demo\"").unwrap();
        let err = expand(args, item_fn("fn build(x: u8) -> Pipeline { todo!() }")).unwrap_err();
        assert!(err.to_string().contains("takes no arguments"), "{err}");
    }

    #[test]
    fn processor_args_require_a_name() {
        let err = crate::util::parse_err::<ProcessorArgs>("state = Ledger");
        assert!(err.contains("requires `name"), "{err}");
    }
}
