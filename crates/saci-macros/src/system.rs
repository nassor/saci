//! `#[transform]` and `#[fold]`: the two system-authoring attributes.
//!
//! Both leave the annotated function exactly as written and add a hidden
//! zero-sized `System` around it, so the business logic stays a plain function
//! that a unit test can call directly.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, ItemFn, Type};

use crate::util::{
    SystemImpl, check_callable, decode_rows, eat_comma, map_user_error, parse_type_arg,
};

/// `#[transform(component = Order)]`.
pub(crate) struct TransformArgs {
    component: Type,
}

impl Parse for TransformArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut component = None;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<syn::Token![=]>()?;
            match key.to_string().as_str() {
                "component" => parse_type_arg(input, &key, &mut component)?,
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        "unknown #[transform] argument; the only one is `component = <Type>`",
                    ));
                }
            }
            if !eat_comma(input)? {
                break;
            }
        }
        let component = component.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "#[transform] requires `component = <Type>`",
            )
        })?;
        Ok(Self { component })
    }
}

/// How many parameters the annotated transform takes.
///
/// Detected syntactically from the parameter count: one is `&mut Row`, two is
/// `(&mut Row, &Config)`. No type inference is involved, so the author may
/// write the config parameter as `&Config`, `&saci_processor::Config`, or any
/// alias of it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransformArity {
    /// `fn(row: &mut Row) -> Result<()>`
    Row,
    /// `fn(row: &mut Row, config: &Config) -> Result<()>`
    RowAndConfig,
}

/// Classify a `#[transform]` function by parameter count.
pub(crate) fn transform_arity(func: &ItemFn) -> syn::Result<TransformArity> {
    match func.sig.inputs.len() {
        1 => Ok(TransformArity::Row),
        2 => Ok(TransformArity::RowAndConfig),
        other => Err(syn::Error::new_spanned(
            &func.sig,
            format!(
                "#[transform] takes `fn(row: &mut Row) -> Result<()>` or \
                 `fn(row: &mut Row, config: &Config) -> Result<()>`, found {other} parameter(s)"
            ),
        )),
    }
}

/// Expand `#[transform(component = C)]` over `func`.
pub(crate) fn expand_transform(args: TransformArgs, func: ItemFn) -> syn::Result<TokenStream> {
    check_callable(&func, "transform")?;
    let arity = transform_arity(&func)?;

    let component = &args.component;
    let fn_name = &func.sig.ident;
    let system_name = fn_name.to_string();
    let decode = decode_rows(component, &system_name);
    let map_err = map_user_error();

    // `crate::saci_config_get` is emitted by `#[processor]` into the processor
    // crate's root. It cannot live in saci-processor: the WIT bindings it reads
    // through are caller-side, so only the crate that expanded
    // `wit_bindgen::generate!` can name them.
    let (setup, call) = match arity {
        TransformArity::Row => (
            quote! {},
            quote! { #fn_name(__saci_row).map_err(#map_err)?; },
        ),
        TransformArity::RowAndConfig => (
            quote! {
                let __saci_config = ::saci_processor::Config::new(crate::saci_config_get);
            },
            quote! { #fn_name(__saci_row, &__saci_config).map_err(#map_err)?; },
        ),
    };

    let meta = quote! {
        ::saci_processor::__rt::SystemMeta::new(#system_name)
            .read_component(<#component as ::saci_processor::__rt::Component>::name())
            .write_component(<#component as ::saci_processor::__rt::Component>::name())
    };

    // Whole-component read *and* write over-declaration is always safe: the DAG
    // expands both into every field of the component, which can only add edges
    // and therefore only cost parallelism.
    let body = quote! {
        let mut __saci_rows = #decode;
        #setup
        for __saci_row in __saci_rows.iter_mut() {
            #call
        }
        __saci_data.replace_batch::<#component>(
            <#component as ::saci_processor::__rt::Component>::to_record_batch(&__saci_rows)?,
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

/// `#[fold(reads = Order, state = Ledger)]`.
pub(crate) struct FoldArgs {
    reads: Type,
    state: Type,
}

impl Parse for FoldArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut reads = None;
        let mut state = None;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<syn::Token![=]>()?;
            match key.to_string().as_str() {
                "reads" => parse_type_arg(input, &key, &mut reads)?,
                "state" => parse_type_arg(input, &key, &mut state)?,
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        "unknown #[fold] argument; the two are `reads = <Type>` and \
                         `state = <Type>`",
                    ));
                }
            }
            if !eat_comma(input)? {
                break;
            }
        }
        let span = proc_macro2::Span::call_site();
        Ok(Self {
            reads: reads
                .ok_or_else(|| syn::Error::new(span, "#[fold] requires `reads = <Type>`"))?,
            state: state
                .ok_or_else(|| syn::Error::new(span, "#[fold] requires `state = <Type>`"))?,
        })
    }
}

/// Expand `#[fold(reads = C, state = S)]` over `func`.
pub(crate) fn expand_fold(args: FoldArgs, func: ItemFn) -> syn::Result<TokenStream> {
    check_callable(&func, "fold")?;
    if func.sig.inputs.len() != 2 {
        return Err(syn::Error::new_spanned(
            &func.sig,
            format!(
                "#[fold] takes `fn(rows: &[Row], state: &mut State) -> Result<()>`, \
                 found {} parameter(s)",
                func.sig.inputs.len()
            ),
        ));
    }

    let FoldArgs { reads, state } = &args;
    let fn_name = &func.sig.ident;
    let system_name = fn_name.to_string();
    let decode = decode_rows(reads, &system_name);
    let map_err = map_user_error();

    let meta = quote! {
        ::saci_processor::__rt::SystemMeta::new(#system_name)
            .read_component(<#reads as ::saci_processor::__rt::Component>::name())
            .write_resource::<::saci_processor::__rt::ProcessorState<#state>>()
    };

    // The rows are decoded into an owned `Vec` inside a block so the immutable
    // borrow of the dataset ends before `get_or_insert_default` takes it
    // mutably.
    let body = quote! {
        let __saci_rows = #decode;
        let __saci_state =
            ::saci_processor::__rt::ProcessorState::<#state>::get_or_insert_default(__saci_data)?;
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
    fn a_one_parameter_transform_is_the_row_only_form() {
        let func = item_fn("fn settle(row: &mut Order) -> Result<()> { Ok(()) }");
        assert_eq!(transform_arity(&func).unwrap(), TransformArity::Row);
    }

    #[test]
    fn a_two_parameter_transform_takes_the_config() {
        let func = item_fn("fn settle(row: &mut Order, config: &Config) -> Result<()> { Ok(()) }");
        assert_eq!(
            transform_arity(&func).unwrap(),
            TransformArity::RowAndConfig
        );
    }

    #[test]
    fn arity_detection_is_syntactic_not_type_directed() {
        // A fully qualified config type, an alias, and a plain `&Config` must
        // all classify the same: only the parameter count is read.
        for source in [
            "fn f(row: &mut Order, config: &saci_processor::Config) -> Result<()> { Ok(()) }",
            "fn f(row: &mut Order, config: &Cfg) -> Result<()> { Ok(()) }",
        ] {
            assert_eq!(
                transform_arity(&item_fn(source)).unwrap(),
                TransformArity::RowAndConfig
            );
        }
    }

    #[test]
    fn zero_and_three_parameter_transforms_are_rejected() {
        for source in [
            "fn f() -> Result<()> { Ok(()) }",
            "fn f(a: &mut Order, b: &Config, c: u8) -> Result<()> { Ok(()) }",
        ] {
            let err = transform_arity(&item_fn(source)).unwrap_err();
            assert!(
                err.to_string().contains("#[transform] takes"),
                "unexpected message: {err}"
            );
        }
    }

    #[test]
    fn an_async_or_generic_transform_is_rejected_at_the_signature() {
        let err = check_callable(
            &item_fn("async fn f(row: &mut Order) -> Result<()> { Ok(()) }"),
            "transform",
        )
        .unwrap_err();
        assert!(err.to_string().contains("remove `async`"), "{err}");

        let err = check_callable(
            &item_fn("fn f<T>(row: &mut T) -> Result<()> { Ok(()) }"),
            "transform",
        )
        .unwrap_err();
        assert!(err.to_string().contains("generic parameters"), "{err}");
    }

    #[test]
    fn transform_args_require_the_component() {
        let err = crate::util::parse_err::<TransformArgs>("");
        assert!(err.contains("component = <Type>"), "{err}");

        let err = crate::util::parse_err::<TransformArgs>("state = Ledger");
        assert!(err.contains("unknown #[transform]"), "{err}");
    }

    #[test]
    fn fold_args_require_both_reads_and_state() {
        let args = syn::parse_str::<FoldArgs>("reads = Order, state = Ledger").unwrap();
        let (reads, state) = (&args.reads, &args.state);
        assert_eq!(quote! { #reads }.to_string(), "Order");
        assert_eq!(quote! { #state }.to_string(), "Ledger");

        let err = crate::util::parse_err::<FoldArgs>("reads = Order");
        assert!(err.contains("state = <Type>"), "{err}");
    }

    #[test]
    fn a_transform_expansion_keeps_the_original_function_and_adds_a_constructor() {
        let args = syn::parse_str::<TransformArgs>("component = Order").unwrap();
        let func = item_fn("pub fn settle(row: &mut Order) -> Result<()> { Ok(()) }");
        let out = expand_transform(args, func).unwrap().to_string();

        assert!(out.contains("pub fn settle (row : & mut Order)"), "{out}");
        assert!(out.contains("pub fn settle_system ()"), "{out}");
        assert!(out.contains("struct __SaciSystem_settle"), "{out}");
        assert!(out.contains("read_component"), "{out}");
        assert!(out.contains("write_component"), "{out}");
        // The one-parameter form must not reach for the config getter.
        assert!(!out.contains("saci_config_get"), "{out}");
    }

    #[test]
    fn a_two_parameter_transform_expansion_builds_the_config_once_per_batch() {
        let args = syn::parse_str::<TransformArgs>("component = Order").unwrap();
        let func = item_fn("fn settle(row: &mut Order, cfg: &Config) -> Result<()> { Ok(()) }");
        let out = expand_transform(args, func).unwrap().to_string();

        assert!(out.contains("crate :: saci_config_get"), "{out}");
        let config_setup = out.find("__saci_config").expect("config binding");
        let row_loop = out.find("for __saci_row").expect("row loop");
        assert!(
            config_setup < row_loop,
            "the config must be built before the loop, not per row"
        );
    }

    #[test]
    fn a_fold_expansion_reaches_for_the_processor_state() {
        let args = syn::parse_str::<FoldArgs>("reads = Order, state = Ledger").unwrap();
        let func =
            item_fn("fn ledger(rows: &[Order], state: &mut Ledger) -> Result<()> { Ok(()) }");
        let out = expand_fold(args, func).unwrap().to_string();

        assert!(
            out.contains("ProcessorState :: < Ledger > :: get_or_insert_default"),
            "{out}"
        );
        assert!(out.contains("write_resource"), "{out}");
        assert!(out.contains("fn ledger_system ()"), "{out}");
        // A fold hands the whole slice over once; it must not loop rows.
        assert!(!out.contains("for __saci_row"), "{out}");
    }

    #[test]
    fn a_fold_with_the_wrong_parameter_count_is_rejected() {
        let args = syn::parse_str::<FoldArgs>("reads = Order, state = Ledger").unwrap();
        let func = item_fn("fn ledger(rows: &[Order]) -> Result<()> { Ok(()) }");
        let err = expand_fold(args, func).unwrap_err();
        assert!(err.to_string().contains("#[fold] takes"), "{err}");
    }
}
