//! `#[derive(Component)]`: the row struct's `Component` impl and one
//! `FieldRef` constant per column.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
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
    let args = struct_args(&input)?;
    let component_name = args.name;
    let expect_msg = format!(
        "#[derive(Component)] on `{ident}`: serde_arrow could not derive an Arrow schema \
         from the type"
    );

    let field_consts = if args.fields {
        field_consts(&input, fields)
    } else {
        TokenStream::new()
    };

    Ok(quote! {
        impl ::saci::__private::Component for #ident {
            fn name() -> &'static str {
                #component_name
            }

            fn schema() -> ::std::sync::Arc<::saci::__private::Schema> {
                use ::saci::__private::serde_arrow::schema::{SchemaLike as _, TracingOptions};

                // Traced once per process: the pipeline asks for the schema
                // on every append, and tracing walks the whole type.
                static __SACI_SCHEMA: ::saci::__private::LazyLock<
                    ::std::sync::Arc<::saci::__private::Schema>,
                > = ::saci::__private::LazyLock::new(|| {
                    // The three overrides are load-bearing, not stylistic.
                    // serde_arrow's defaults trace `String` to `LargeUtf8`,
                    // `Vec<_>` to `LargeList` and `&[u8]` to `LargeBinary`,
                    // and every non-Rust SACI codec assumes the 32-bit
                    // offset forms.
                    let __saci_opts = TracingOptions::default()
                        .strings_as_large_utf8(false)
                        .sequence_as_large_list(false)
                        .bytes_as_large_binary(false);

                    let __saci_fields = ::std::vec::Vec::<
                        ::saci::__private::arrow_schema::FieldRef,
                    >::from_type::<#ident>(__saci_opts)
                    .expect(#expect_msg);

                    ::std::sync::Arc::new(::saci::__private::Schema::new(__saci_fields))
                });

                ::std::sync::Arc::clone(&__SACI_SCHEMA)
            }
        }

        #field_consts
    })
}

/// The struct-level `#[saci(...)]` arguments.
struct StructArgs {
    /// The component name: the struct identifier, or `name = "..."`.
    name: LitStr,
    /// `false` under `fields = false`: no `FieldRef` constants.
    fields: bool,
}

/// Parse the struct-level `#[saci(name = "...", fields = false)]`.
fn struct_args(input: &DeriveInput) -> syn::Result<StructArgs> {
    let mut name: Option<LitStr> = None;
    let mut fields: Option<bool> = None;

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
                Meta::NameValue(nv) if nv.path.is_ident("fields") => {
                    if fields.is_some() {
                        return Err(syn::Error::new_spanned(
                            &meta,
                            "`#[saci(fields = ...)]` is given more than once",
                        ));
                    }
                    fields = Some(match &nv.value {
                        syn::Expr::Lit(syn::ExprLit {
                            lit: syn::Lit::Bool(flag),
                            ..
                        }) => flag.value,
                        other => {
                            return Err(syn::Error::new_spanned(
                                other,
                                "`#[saci(fields = ...)]` takes `true` or `false`",
                            ));
                        }
                    });
                }
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "unknown `#[saci(...)]` argument on a struct; the two are \
                         `name = \"...\"`, which renames the component, and `fields = false`, \
                         which declares no field constants",
                    ));
                }
            }
        }
    }

    Ok(StructArgs {
        name: name.unwrap_or_else(|| LitStr::new(&input.ident.to_string(), input.ident.span())),
        fields: fields.unwrap_or(true),
    })
}

/// `impl Struct { pub const FIELD: FieldRef<Struct> = FieldRef::new("column"); .. }`,
/// one constant per field that maps to exactly one column.
fn field_consts(
    input: &DeriveInput,
    fields: &syn::punctuated::Punctuated<syn::Field, syn::Token![,]>,
) -> TokenStream {
    let ident = &input.ident;
    let vis = &input.vis;
    let Some(rule) = container_rename_rule(&input.attrs) else {
        // A split `rename_all(serialize = .., deserialize = ..)` gives a
        // column two names; no constant can name both.
        return TokenStream::new();
    };

    let consts = fields.iter().filter_map(|field| {
        let field_ident = field.ident.as_ref()?;
        let raw = field_ident.to_string();
        let bare = raw.strip_prefix("r#").unwrap_or(&raw);
        let column = match field_serde(&field.attrs) {
            FieldSerde::NoColumn => return None,
            FieldSerde::Renamed(column) => column,
            FieldSerde::Plain => rule.apply(bare),
        };
        let const_ident = format_ident!("{}", bare.to_uppercase(), span = field_ident.span());
        let doc = format!("The `{column}` column of [`{ident}`].");
        Some(quote! {
            #[doc = #doc]
            #[allow(dead_code)]
            #vis const #const_ident: ::saci::__private::FieldRef<#ident> =
                ::saci::__private::FieldRef::new(#column);
        })
    });

    quote! {
        impl #ident {
            #( #consts )*
        }
    }
}

/// What a field's `#[serde(...)]` attributes say about its column.
enum FieldSerde {
    /// No rename: the column follows the container rule.
    Plain,
    /// `rename = "..."`: the column is this literal.
    Renamed(String),
    /// Skipped, flattened or split-renamed: no single column to name.
    NoColumn,
}

/// The `serde(...)` metas of `attrs`. An attribute whose arguments do not
/// parse as metas contributes nothing; serde itself reports it.
fn serde_metas(attrs: &[syn::Attribute]) -> impl Iterator<Item = Meta> + '_ {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("serde"))
        .filter_map(|attr| {
            attr.parse_args_with(
                syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
            )
            .ok()
        })
        .flatten()
}

fn field_serde(attrs: &[syn::Attribute]) -> FieldSerde {
    let mut renamed = None;
    for meta in serde_metas(attrs) {
        match &meta {
            Meta::Path(path)
                if ["skip", "skip_serializing", "skip_deserializing", "flatten"]
                    .iter()
                    .any(|word| path.is_ident(word)) =>
            {
                return FieldSerde::NoColumn;
            }
            Meta::List(list) if list.path.is_ident("rename") => return FieldSerde::NoColumn,
            Meta::NameValue(nv) if nv.path.is_ident("rename") => {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(text),
                    ..
                }) = &nv.value
                {
                    renamed = Some(text.value());
                }
            }
            _ => {}
        }
    }
    renamed.map_or(FieldSerde::Plain, FieldSerde::Renamed)
}

/// serde's `rename_all` rules, applied to a snake_case field identifier.
#[derive(Clone, Copy)]
enum RenameRule {
    None,
    Lower,
    Upper,
    Pascal,
    Camel,
    Snake,
    ScreamingSnake,
    Kebab,
    ScreamingKebab,
}

impl RenameRule {
    fn from_str(rule: &str) -> Option<Self> {
        Some(match rule {
            "lowercase" => Self::Lower,
            "UPPERCASE" => Self::Upper,
            "PascalCase" => Self::Pascal,
            "camelCase" => Self::Camel,
            "snake_case" => Self::Snake,
            "SCREAMING_SNAKE_CASE" => Self::ScreamingSnake,
            "kebab-case" => Self::Kebab,
            "SCREAMING-KEBAB-CASE" => Self::ScreamingKebab,
            _ => return None,
        })
    }

    /// The column serde writes for `field` under this rule; the same
    /// algorithm as serde's `RenameRule::apply_to_field`.
    fn apply(self, field: &str) -> String {
        match self {
            Self::None | Self::Lower | Self::Snake => field.to_owned(),
            Self::Upper | Self::ScreamingSnake => field.to_ascii_uppercase(),
            Self::Pascal => {
                let mut pascal = String::with_capacity(field.len());
                let mut capitalize = true;
                for ch in field.chars() {
                    if ch == '_' {
                        capitalize = true;
                    } else if capitalize {
                        pascal.push(ch.to_ascii_uppercase());
                        capitalize = false;
                    } else {
                        pascal.push(ch);
                    }
                }
                pascal
            }
            Self::Camel => {
                let pascal = Self::Pascal.apply(field);
                let mut chars = pascal.chars();
                match chars.next() {
                    Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
                    None => pascal,
                }
            }
            Self::Kebab => field.replace('_', "-"),
            Self::ScreamingKebab => field.to_ascii_uppercase().replace('_', "-"),
        }
    }
}

/// The container's `rename_all` rule. `None` for the split form, whose two
/// directions may disagree; an unknown rule name is left to serde to report.
fn container_rename_rule(attrs: &[syn::Attribute]) -> Option<RenameRule> {
    let mut rule = RenameRule::None;
    for meta in serde_metas(attrs) {
        match &meta {
            Meta::List(list) if list.path.is_ident("rename_all") => return None,
            Meta::NameValue(nv) if nv.path.is_ident("rename_all") => {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(text),
                    ..
                }) = &nv.value
                {
                    rule = RenameRule::from_str(&text.value()).unwrap_or(RenameRule::None);
                }
            }
            _ => {}
        }
    }
    Some(rule)
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
    fn the_schema_is_traced_once() {
        let out = expand_str("struct Order { id: i64 }").unwrap();
        assert!(out.contains("static __SACI_SCHEMA"), "{out}");
        assert!(out.contains("LazyLock :: new"), "{out}");
    }

    #[test]
    fn a_plain_field_gets_an_upper_case_const_naming_its_column() {
        let out = expand_str("pub struct Order { pub usd_amount: f64 }").unwrap();
        assert!(
            out.contains(
                "pub const USD_AMOUNT : :: saci :: __private :: FieldRef < Order > = \
                 :: saci :: __private :: FieldRef :: new (\"usd_amount\")"
            ),
            "{out}"
        );
    }

    #[test]
    fn a_renamed_field_names_the_serde_column() {
        let out = expand_str("struct Order { #[serde(rename = \"amt\")] amount: f64 }").unwrap();
        assert!(out.contains("const AMOUNT"), "{out}");
        assert!(out.contains("FieldRef :: new (\"amt\")"), "{out}");
    }

    #[test]
    fn a_camel_case_container_renames_every_column() {
        let out = expand_str(
            "#[serde(rename_all = \"camelCase\")] struct Order { usd_amount: f64, r#type: i64 }",
        )
        .unwrap();
        assert!(out.contains("const USD_AMOUNT"), "{out}");
        assert!(out.contains("FieldRef :: new (\"usdAmount\")"), "{out}");
        assert!(out.contains("const TYPE"), "{out}");
        assert!(out.contains("FieldRef :: new (\"type\")"), "{out}");
    }

    #[test]
    fn the_rename_rules_match_serde() {
        for (rule, expected) in [
            ("lowercase", "usd_amount"),
            ("UPPERCASE", "USD_AMOUNT"),
            ("PascalCase", "UsdAmount"),
            ("camelCase", "usdAmount"),
            ("snake_case", "usd_amount"),
            ("SCREAMING_SNAKE_CASE", "USD_AMOUNT"),
            ("kebab-case", "usd-amount"),
            ("SCREAMING-KEBAB-CASE", "USD-AMOUNT"),
        ] {
            assert_eq!(
                RenameRule::from_str(rule).unwrap().apply("usd_amount"),
                expected,
                "{rule}"
            );
        }
    }

    #[test]
    fn a_skipped_or_flattened_field_gets_no_const() {
        let out = expand_str(
            "struct Order { id: i64, #[serde(skip)] cache: u8, #[serde(flatten)] extra: X, \
             #[serde(rename(serialize = \"a\", deserialize = \"b\"))] split: i64 }",
        )
        .unwrap();
        assert!(out.contains("const ID"), "{out}");
        assert!(!out.contains("const CACHE"), "{out}");
        assert!(!out.contains("const EXTRA"), "{out}");
        assert!(!out.contains("const SPLIT"), "{out}");
    }

    #[test]
    fn fields_false_declares_no_consts() {
        let out = expand_str("#[saci(fields = false)] struct Order { id: i64 }").unwrap();
        assert!(!out.contains("const ID"), "{out}");
        assert!(!out.contains("FieldRef ::"), "{out}");
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
