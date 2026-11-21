//! Shared emission helpers for the system-authoring macros.

use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote};
use syn::{ItemFn, Visibility};

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

/// Reject any attribute argument: the macros read everything they need from
/// the function signature.
pub(crate) fn reject_args(attr: TokenStream, macro_name: &str) -> syn::Result<()> {
    if attr.is_empty() {
        return Ok(());
    }
    Err(syn::Error::new_spanned(
        attr,
        format!("#[{macro_name}] takes no arguments; the types come from the function signature"),
    ))
}

/// The type `T` behind the `index`th parameter when it is written `&mut T`
/// (`mutable`) or `&T`.
pub(crate) fn param_ref_type(func: &ItemFn, index: usize, mutable: bool) -> Option<&syn::Type> {
    let syn::FnArg::Typed(pat) = func.sig.inputs.iter().nth(index)? else {
        return None;
    };
    let syn::Type::Reference(reference) = pat.ty.as_ref() else {
        return None;
    };
    (reference.mutability.is_some() == mutable).then_some(reference.elem.as_ref())
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
             Pass it to `Pipeline::system`. The system is zero-sized: all behaviour \
             comes from `{fn_name}`, which stays callable and unit-testable on its own."
        );

        quote! {
            #[doc(hidden)]
            #[allow(non_camel_case_types)]
            struct #ty;

            impl #ty {
                fn __saci_exec(
                    &self,
                    __saci_data: &mut ::saci::__private::Dataset,
                ) -> ::saci::__private::SaciResult<()> {
                    #body
                }
            }

            #[::saci::__private::async_trait]
            impl ::saci::__private::System for #ty {
                fn meta(&self) -> ::saci::__private::SystemMeta {
                    #meta
                }

                async fn run(
                    &self,
                    __saci_data: &mut ::saci::__private::Dataset,
                ) -> ::saci::__private::SaciResult<()> {
                    self.__saci_exec(__saci_data)
                }

                // The body never awaits, so the scheduler takes this path and
                // never builds the boxed future `#[async_trait]` imposes.
                fn run_sync(
                    &self,
                    __saci_data: &mut ::saci::__private::Dataset,
                ) -> ::core::option::Option<::saci::__private::SaciResult<()>> {
                    ::core::option::Option::Some(self.__saci_exec(__saci_data))
                }
            }

            #[doc = #ctor_doc]
            #vis fn #ctor() -> impl ::saci::__private::System + 'static {
                #ty
            }
        }
    }
}

/// Decode the rows of component `component` out of the batch dataset.
///
/// Emitted as a block so the immutable borrow of `__saci_data` ends before the
/// generated body needs it mutably.
pub(crate) fn decode_rows(component: &syn::Type) -> TokenStream {
    quote! {
        {
            let __saci_batch = __saci_data.batch::<#component>()?;
            <#component as ::saci::__private::Component>::from_record_batch(__saci_batch)?
        }
    }
}

/// Map an `sdk::Error` out of an author-written function into a
/// `SaciError`.
///
/// `SystemExecution`, not `Generic`: a failing transform or fold is by
/// definition a failure inside a system's own processing logic, which is
/// retryable.
pub(crate) fn map_user_error() -> TokenStream {
    quote! {
        |__saci_err: ::saci::__private::Error| {
            ::saci::__private::SaciError::system_execution(__saci_err.into_message())
        }
    }
}
