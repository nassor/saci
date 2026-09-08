//! `#[derive(Component)]`: the row struct's `Component` impl.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr, Meta};

/// Expand `#[derive(Component)]` for `input`.
pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "#[derive(Component)] does not support generic parameters: `Component::schema()` \
             has no type parameters to resolve them against",
        ));
    }

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            Fields::Unnamed(_) | Fields::Unit => {
                return Err(syn::Error::new_spanned(
                    &data.fields,
                    "#[derive(Component)] requires named fields: Arrow columns are keyed by name",
                ));
            }
        },
        Data::Enum(_) | Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "#[derive(Component)] applies to structs only",
            ));
        }
    };

    for field in fields {
        reject_field_attrs(field)?;
    }

    let ident = &input.ident;
    let component_name = component_name(&input)?;
    let expect_msg = format!(
        "#[derive(Component)] on `{ident}`: serde_arrow could not derive an Arrow schema \
         from the type"
    );

    Ok(quote! {
        impl ::saci_processor::__rt::Component for #ident {
            fn name() -> &'static str {
                #component_name
            }

            fn schema() -> ::std::sync::Arc<::saci_processor::arrow_schema::Schema> {
                use ::saci_processor::__rt::serde_arrow::schema::{SchemaLike as _, TracingOptions};

                // The three overrides are load-bearing, not stylistic.
                // serde_arrow's defaults trace `String` to `LargeUtf8`,
                // `Vec<_>` to `LargeList` and `&[u8]` to `LargeBinary`, and
                // every non-Rust SACI codec assumes the 32-bit offset forms.
                let __saci_opts = TracingOptions::default()
                    .strings_as_large_utf8(false)
                    .sequence_as_large_list(false)
                    .bytes_as_large_binary(false);

                let __saci_fields =
                    ::std::vec::Vec::<::saci_processor::arrow_schema::FieldRef>::from_type::<Self>(
                        __saci_opts,
                    )
                    .expect(#expect_msg);

                ::std::sync::Arc::new(::saci_processor::arrow_schema::Schema::new(__saci_fields))
            }
        }
    })
}

/// The component name: the struct identifier, or the `#[saci(name = "...")]`
/// override.
fn component_name(input: &DeriveInput) -> syn::Result<LitStr> {
    let mut name: Option<LitStr> = None;

    for attr in &input.attrs {
        if !attr.path().is_ident("saci") {
            continue;
        }
        for meta in attr.parse_args_with(
            syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
        )? {
            match &meta {
                Meta::NameValue(nv) if nv.path.is_ident("name") => {
                    if name.is_some() {
                        return Err(syn::Error::new_spanned(
                            &meta,
                            "`#[saci(name = \"...\")]` is given more than once",
                        ));
                    }
                    name = Some(match &nv.value {
                        syn::Expr::Lit(syn::ExprLit {
                            lit: syn::Lit::Str(text),
                            ..
                        }) => text.clone(),
                        other => {
                            return Err(syn::Error::new_spanned(
                                other,
                                "`#[saci(name = ...)]` takes a string literal",
                            ));
                        }
                    });
                }
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "unknown `#[saci(...)]` argument on a struct; the only one is \
                         `name = \"...\"`, which renames the component",
                    ));
                }
            }
        }
    }

    Ok(name.unwrap_or_else(|| LitStr::new(&input.ident.to_string(), input.ident.span())))
}

/// Reject `#[saci(...)]` on a field.
///
/// A field's wire name is not ours to choose: `Component::schema()` traces the
/// type's `Deserialize` impl, and `to_record_batch` / `from_record_batch` match
/// the struct's serde field names against that schema. Renaming only the schema
/// side desynchronises the two and breaks every encode and decode, so the
/// mechanism is `#[serde(rename = "...")]`, which serde_arrow traces and both
/// halves therefore agree on.
fn reject_field_attrs(field: &syn::Field) -> syn::Result<()> {
    for attr in &field.attrs {
        if attr.path().is_ident("saci") {
            return Err(syn::Error::new_spanned(
                attr,
                "`#[saci(...)]` is not accepted on a field. To rename a column, use \
                 `#[serde(rename = \"...\")]`: `Component::schema()` is traced from the \
                 type's `Deserialize` impl, so a serde rename moves the schema field name \
                 and the encoder/decoder together, while a schema-only rename would break \
                 both.",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expand_str(source: &str) -> syn::Result<String> {
        let input = syn::parse_str::<DeriveInput>(source).expect("test fixture must parse");
        expand(input).map(|tokens| tokens.to_string())
    }

    #[test]
    fn the_component_name_is_the_struct_identifier() {
        let out = expand_str("struct Order { id: i64 }").unwrap();
        assert!(
            out.contains("fn name () -> & 'static str { \"Order\" }"),
            "{out}"
        );
    }

    #[test]
    fn a_struct_level_name_attribute_renames_the_component() {
        let out = expand_str("#[saci(name = \"RunningTotals\")] struct Totals { n: i64 }").unwrap();
        assert!(out.contains("\"RunningTotals\""), "{out}");
        assert!(!out.contains("\"Totals\""), "{out}");
    }

    #[test]
    fn the_traced_schema_pins_the_thirty_two_bit_offset_forms() {
        let out = expand_str("struct Order { label: String }").unwrap();
        assert!(out.contains("strings_as_large_utf8 (false)"), "{out}");
        assert!(out.contains("sequence_as_large_list (false)"), "{out}");
        assert!(out.contains("bytes_as_large_binary (false)"), "{out}");
    }

    #[test]
    fn a_field_level_saci_attribute_points_at_serde_rename() {
        let err = expand_str("struct Order { #[saci(rename = \"ident\")] id: i64 }").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not accepted on a field"), "{msg}");
        assert!(msg.contains("#[serde(rename = \"...\")]"), "{msg}");
    }

    #[test]
    fn an_unknown_struct_level_argument_is_rejected() {
        let err =
            expand_str("#[saci(component = \"Order\")] struct Order { id: i64 }").unwrap_err();
        assert!(err.to_string().contains("unknown `#[saci(...)]`"), "{err}");
    }

    #[test]
    fn a_non_literal_name_is_rejected() {
        let err = expand_str("#[saci(name = ORDER)] struct Order { id: i64 }").unwrap_err();
        assert!(err.to_string().contains("string literal"), "{err}");
    }

    #[test]
    fn generics_enums_unions_and_unnamed_fields_are_rejected() {
        for (source, needle) in [
            ("struct Order<T> { id: T }", "generic parameters"),
            ("struct Order(i64);", "named fields"),
            ("struct Order;", "named fields"),
            ("enum Order { A }", "structs only"),
            ("union Order { id: i64 }", "structs only"),
        ] {
            let err = expand_str(source).unwrap_err();
            assert!(
                err.to_string().contains(needle),
                "`{source}` should mention `{needle}`, got: {err}"
            );
        }
    }
}
