//! `#[transform]` and `#[fold]`: the two system-authoring attributes.
//!
//! Both leave the annotated function exactly as written and add a hidden
//! zero-sized `System` around it, so the business logic stays a plain function
//! that a unit test can call directly. Neither takes arguments: the component
//! and state types are read from the function's parameters.

use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

use crate::util::{SystemImpl, check_callable, decode_rows, map_user_error, param_ref_type};

const TRANSFORM_SHAPE: &str = "#[transform] takes `fn(row: &mut Row) -> sdk::Result<()>` or \
     `fn(row: &mut Row, config: &sdk::Config) -> sdk::Result<()>`";

const FOLD_SHAPE: &str = "#[fold] takes `fn(rows: &[Row], state: &mut State) -> sdk::Result<()>`";

/// Expand `#[transform]` over `func`.
pub(crate) fn expand_transform(func: ItemFn) -> syn::Result<TokenStream> {
    check_callable(&func, "transform")?;
    let arity = func.sig.inputs.len();
    if arity != 1 && arity != 2 {
        return Err(syn::Error::new_spanned(&func.sig, TRANSFORM_SHAPE));
    }
    let Some(component) = param_ref_type(&func, 0, true) else {
        return Err(syn::Error::new_spanned(&func.sig.inputs, TRANSFORM_SHAPE));
    };

    let fn_name = &func.sig.ident;
    let system_name = fn_name.to_string();
    let decode = decode_rows(component);
    let map_err = map_user_error();

    let (setup, call) = if arity == 1 {
        (
            quote! {},
            quote! { #fn_name(__saci_row).map_err(#map_err)?; },
        )
    } else {
        (
            quote! { let __saci_config = ::saci::__private::Config::new(); },
            quote! { #fn_name(__saci_row, &__saci_config).map_err(#map_err)?; },
        )
    };

    // Whole-component read *and* write over-declaration is always safe: the DAG
    // expands both into every field of the component, which can only add edges
    // and therefore only cost parallelism.
    let meta = quote! {
        ::saci::__private::SystemMeta::new(#system_name)
            .reads_all::<#component>()
            .writes_all::<#component>()
    };

    let body = quote! {
        let mut __saci_rows = #decode;
        #setup
        for __saci_row in __saci_rows.iter_mut() {
            #call
        }
        __saci_data.replace_batch::<#component>(
            <#component as ::saci::__private::Component>::to_record_batch(&__saci_rows)?,
        )
    };

    let generated = SystemImpl {
        func: &func,
        macro_name: "transform",
        meta,
        body,
    }
    .emit();

    Ok(quote! {
        #func
        #generated
    })
}

/// Expand `#[fold]` over `func`.
pub(crate) fn expand_fold(func: ItemFn) -> syn::Result<TokenStream> {
    check_callable(&func, "fold")?;
    if func.sig.inputs.len() != 2 {
        return Err(syn::Error::new_spanned(&func.sig, FOLD_SHAPE));
    }
    let reads = match param_ref_type(&func, 0, false) {
        Some(syn::Type::Slice(slice)) => slice.elem.as_ref(),
        _ => return Err(syn::Error::new_spanned(&func.sig.inputs, FOLD_SHAPE)),
    };
    let Some(state) = param_ref_type(&func, 1, true) else {
        return Err(syn::Error::new_spanned(&func.sig.inputs, FOLD_SHAPE));
    };

    let fn_name = &func.sig.ident;
    let system_name = fn_name.to_string();
    let decode = decode_rows(reads);
    let map_err = map_user_error();

    let meta = quote! {
        ::saci::__private::SystemMeta::new(#system_name)
            .reads_all::<#reads>()
            .writes_resource::<::saci::__private::State<#state>>()
    };

    // The rows are decoded into an owned `Vec` inside a block so the immutable
    // borrow of the dataset ends before `get_or_insert_default` takes it
    // mutably.
    let body = quote! {
        let __saci_rows = #decode;
        let __saci_state =
            ::saci::__private::State::<#state>::get_or_insert_default(__saci_data)?;
        #fn_name(&__saci_rows, __saci_state).map_err(#map_err)
    };

    let generated = SystemImpl {
        func: &func,
        macro_name: "fold",
        meta,
        body,
    }
    .emit();

    Ok(quote! {
        #func
        #generated
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item_fn(source: &str) -> ItemFn {
        syn::parse_str::<ItemFn>(source).expect("test fixture must parse")
    }

    #[test]
    fn a_transform_expansion_keeps_the_original_function_and_adds_a_constructor() {
        let func = item_fn("pub fn settle(row: &mut Order) -> Result<()> { Ok(()) }");
        let out = expand_transform(func).unwrap().to_string();

        assert!(out.contains("pub fn settle (row : & mut Order)"), "{out}");
        assert!(out.contains("pub fn settle_system ()"), "{out}");
        assert!(out.contains("struct __SaciSystem_settle"), "{out}");
        assert!(out.contains("reads_all :: < Order >"), "{out}");
        assert!(out.contains("writes_all :: < Order >"), "{out}");
        // The one-parameter form builds no config.
        assert!(!out.contains("Config :: new"), "{out}");
    }

    #[test]
    fn a_two_parameter_transform_builds_the_config_once_per_batch() {
        let func = item_fn("fn settle(row: &mut Order, cfg: &Config) -> Result<()> { Ok(()) }");
        let out = expand_transform(func).unwrap().to_string();

        let config_setup = out
            .find("__private :: Config :: new ()")
            .expect("config binding");
        let row_loop = out.find("for __saci_row").expect("row loop");
        assert!(
            config_setup < row_loop,
            "the config must be built before the loop, not per row"
        );
    }

    #[test]
    fn a_transform_whose_first_parameter_is_not_a_mutable_reference_is_rejected() {
        for source in [
            "fn f(row: Order) -> Result<()> { Ok(()) }",
            "fn f(row: &Order) -> Result<()> { Ok(()) }",
            "fn f() -> Result<()> { Ok(()) }",
            "fn f(a: &mut Order, b: &Config, c: u8) -> Result<()> { Ok(()) }",
        ] {
            let err = expand_transform(item_fn(source)).unwrap_err();
            assert!(
                err.to_string().contains("#[transform] takes"),
                "`{source}`: {err}"
            );
        }
    }

    #[test]
    fn an_async_or_generic_transform_is_rejected_at_the_signature() {
        let err = expand_transform(item_fn(
            "async fn f(row: &mut Order) -> Result<()> { Ok(()) }",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("remove `async`"), "{err}");

        let err =
            expand_transform(item_fn("fn f<T>(row: &mut T) -> Result<()> { Ok(()) }")).unwrap_err();
        assert!(err.to_string().contains("generic parameters"), "{err}");
    }

    #[test]
    fn a_fold_expansion_reaches_for_the_state() {
        let func =
            item_fn("fn ledger(rows: &[Order], state: &mut Ledger) -> Result<()> { Ok(()) }");
        let out = expand_fold(func).unwrap().to_string();

        assert!(
            out.contains("State :: < Ledger > :: get_or_insert_default"),
            "{out}"
        );
        assert!(out.contains("reads_all :: < Order >"), "{out}");
        assert!(out.contains("writes_resource"), "{out}");
        assert!(out.contains("fn ledger_system ()"), "{out}");
        // A fold hands the whole slice over once; it must not loop rows.
        assert!(!out.contains("for __saci_row"), "{out}");
    }

    #[test]
    fn a_fold_with_the_wrong_shape_is_rejected() {
        for source in [
            "fn ledger(rows: &[Order]) -> Result<()> { Ok(()) }",
            "fn ledger(rows: Vec<Order>, state: &mut Ledger) -> Result<()> { Ok(()) }",
            "fn ledger(rows: &[Order], state: &Ledger) -> Result<()> { Ok(()) }",
        ] {
            let err = expand_fold(item_fn(source)).unwrap_err();
            assert!(
                err.to_string().contains("#[fold] takes"),
                "`{source}`: {err}"
            );
        }
    }
}
