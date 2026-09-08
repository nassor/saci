//! Shared parsing and emission helpers for the four macros.

use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote};
use syn::{ItemFn, Visibility};

/// Parse one `key = <type>` pair, where the value is a type path such as
/// `Order` or `my_mod::Ledger`.
///
/// `syn::Type` parsing stops at the enclosing `,`, so a comma-separated
/// attribute list needs no lookahead beyond the separator itself.
pub(crate) fn parse_type_arg(
    input: syn::parse::ParseStream<'_>,
    key: &Ident,
    slot: &mut Option<syn::Type>,
) -> syn::Result<()> {
    if slot.is_some() {
        return Err(syn::Error::new(
            key.span(),
            format!("`{key}` is given more than once"),
        ));
    }
    *slot = Some(input.parse()?);
    Ok(())
}

/// Consume the `,` between two attribute arguments.
///
/// Returns `false` when the list is finished, so the caller's loop can stop
/// without peeking twice.
pub(crate) fn eat_comma(input: syn::parse::ParseStream<'_>) -> syn::Result<bool> {
    if input.peek(syn::Token![,]) {
        input.parse::<syn::Token![,]>()?;
        Ok(!input.is_empty())
    } else {
        Ok(false)
    }
}

/// Reject the function shapes the generated `System` body cannot call.
///
/// The macros call the annotated function by name from a synchronous
/// `System::run_sync`, so a receiver, a generic parameter, `async`, or a
/// variadic signature has no expansion. Rejecting them here turns what would
/// be an error inside generated code into one pointing at the author's
/// signature.
pub(crate) fn check_callable(func: &ItemFn, macro_name: &str) -> syn::Result<()> {
    let sig = &func.sig;
    if sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            sig.asyncness,
            format!(
                "#[{macro_name}] functions are called from a synchronous system body; \
                 remove `async`"
            ),
        ));
    }
    if !sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &sig.generics,
            format!("#[{macro_name}] does not support generic parameters"),
        ));
    }
    if sig.variadic.is_some() {
        return Err(syn::Error::new_spanned(
            &sig.variadic,
            format!("#[{macro_name}] does not support variadic functions"),
        ));
    }
    if let Some(receiver) = sig.inputs.iter().find_map(|arg| match arg {
        syn::FnArg::Receiver(receiver) => Some(receiver),
        syn::FnArg::Typed(_) => None,
    }) {
        return Err(syn::Error::new_spanned(
            receiver,
            format!("#[{macro_name}] applies to free functions, not methods"),
        ));
    }
    Ok(())
}

/// A `System` implementation the `#[transform]` and `#[fold]` macros emit
/// around an author-written function.
pub(crate) struct SystemImpl<'a> {
    /// The annotated function, whose name becomes the `SystemMeta` name and
    /// the `<name>_system` constructor's prefix.
    pub(crate) func: &'a ItemFn,
    /// Which macro produced this, for the generated doc comment.
    pub(crate) macro_name: &'static str,
    /// The `SystemMeta` expression.
    pub(crate) meta: TokenStream,
    /// The body of the synchronous execution path. It receives the dataset as
    /// `__saci_data` and returns `SaciResult<()>`.
    pub(crate) body: TokenStream,
}

impl SystemImpl<'_> {
    /// Emit the hidden zero-sized system type, its `System` impl, and the
    /// `<name>_system()` constructor.
    pub(crate) fn emit(self) -> TokenStream {
        let Self {
            func,
            macro_name,
            meta,
            body,
        } = self;

        let fn_name = &func.sig.ident;
        let vis: &Visibility = &func.vis;
        let ctor = format_ident!("{}_system", fn_name);
        let ty = Ident::new(&format!("__SaciSystem_{fn_name}"), Span::call_site());

        let ctor_doc = format!(
            "The `System` that `#[{macro_name}]` generated for `{fn_name}`.\n\
             \n\
             Pass it to `PipelineBuilder::with_system`. The system is zero-sized: all \
             behaviour comes from `{fn_name}`, which stays callable and unit-testable on \
             its own."
        );

        quote! {
            #[doc(hidden)]
            #[allow(non_camel_case_types)]
            struct #ty;

            impl #ty {
                fn __saci_exec(
                    &self,
                    __saci_data: &mut ::saci_processor::__rt::Dataset,
                ) -> ::saci_processor::__rt::SaciResult<()> {
                    #body
                }
            }

            #[::saci_processor::__rt::async_trait]
            impl ::saci_processor::__rt::System for #ty {
                fn meta(&self) -> ::saci_processor::__rt::SystemMeta {
                    #meta
                }

                async fn run(
                    &self,
                    __saci_data: &mut ::saci_processor::__rt::Dataset,
                ) -> ::saci_processor::__rt::SaciResult<()> {
                    self.__saci_exec(__saci_data)
                }

                // The body never awaits, so the scheduler takes this path and
                // never builds the boxed future `#[async_trait]` imposes.
                fn run_sync(
                    &self,
                    __saci_data: &mut ::saci_processor::__rt::Dataset,
                ) -> ::core::option::Option<::saci_processor::__rt::SaciResult<()>> {
                    ::core::option::Option::Some(self.__saci_exec(__saci_data))
                }
            }

            #[doc = #ctor_doc]
            #vis fn #ctor() -> impl ::saci_processor::__rt::System + 'static {
                #ty
            }
        }
    }
}

/// Decode the rows of component `component` out of the batch dataset.
///
/// Emitted as a block so the immutable borrow of `__saci_data` ends before the
/// generated body needs it mutably.
pub(crate) fn decode_rows(component: &syn::Type, system_name: &str) -> TokenStream {
    quote! {
        {
            let __saci_batch = __saci_data
                .columns::<#component>()
                .ok_or_else(|| ::saci_processor::__rt::SaciError::generic(format!(
                    "{}: component '{}' is not registered on the batch dataset",
                    #system_name,
                    <#component as ::saci_processor::__rt::Component>::name(),
                )))?;
            <#component as ::saci_processor::__rt::Component>::from_record_batch(__saci_batch)?
        }
    }
}

/// Map a `saci_processor::Error` out of an author-written function into a
/// `saci_core::SaciError`.
///
/// `SystemExecution`, not `Generic`: a failing transform or fold is by
/// definition a failure inside a system's own processing logic, which is the
/// variant `classify_run_error` reports to the host as `retryable`.
pub(crate) fn map_user_error() -> TokenStream {
    quote! {
        |__saci_err| ::saci_processor::__rt::SaciError::system_execution(__saci_err.into_message())
    }
}

/// The message from a rejected attribute-argument parse.
///
/// `Result::unwrap_err` is unavailable here: the `Ok` half holds `syn::Type`
/// values, and `syn`'s `Debug` impls sit behind its `extra-traits` feature.
#[cfg(test)]
pub(crate) fn parse_err<T: syn::parse::Parse>(source: &str) -> String {
    match syn::parse_str::<T>(source) {
        Ok(_) => panic!("expected `{source}` to be rejected"),
        Err(err) => err.to_string(),
    }
}
