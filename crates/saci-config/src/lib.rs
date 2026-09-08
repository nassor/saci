//! The SACI configuration language.
//!
//! Configuration is written in [KDL](https://kdl.dev). This crate turns that
//! text into [`ConfigValue`], a self-describing value tree that carries a
//! `serde::Deserializer`, so the host config schema and every connector,
//! transformer and plugin config struct deserializes from it with the serde
//! attributes it already has.
//!
//! ```
//! use saci_config::from_kdl_str;
//!
//! let value = from_kdl_str(r#"
//!     source "csv_orders" type="FileSource" {
//!         config path="orders.csv" format="csv"
//!     }
//! "#).unwrap();
//!
//! let source = &value["source"];
//! assert_eq!(source["id"], "csv_orders");
//! assert_eq!(source["config"]["format"], "csv");
//! ```
//!
//! # Mapping
//!
//! The mapping is schema blind: it knows no key names, so the same seven rules
//! apply to a host section and to an opaque connector config bag.
//!
//! 1. A document, and every children block, is a table. Each node in it
//!    contributes one entry keyed by the node's name.
//! 2. Properties (`key=value`) are scalar entries of the node's own table.
//!    Naming one property twice on a node is an error, not KDL's rightmost
//!    wins override: a config that says one thing twice is a mistake.
//! 3. A node with arguments only holds a scalar when it has one argument and
//!    an array of scalars when it has two or more.
//! 4. A node with properties or children is a table built from both. One
//!    leading argument, if present, is stored under the key `id`.
//! 5. Sibling nodes sharing a name collapse into an array of their values, in
//!    document order. Any other key collision is an error naming the key.
//! 6. A node with nothing on it is an empty table.
//! 7. Numbers map to `i64`, `u64` or `f64`. Type annotations, `#null`, `#inf`
//!    and `#nan` are errors: the mapping is schema blind, so silently dropping
//!    syntax would hide a typo.
//!
//! Rule 5 needs two occurrences to build an array, so a list-valued field
//! written once is a single value. Such fields carry [`one_or_many`], which
//! accepts both shapes.
//!
//! # Variable substitution
//!
//! Both entry points run over the raw text before the parser, so they are
//! format independent. `${VAR}` is replaced with the value of `VAR` and
//! `${VAR:-default}` falls back to `default` when `VAR` is unset.
//!
//! [`substitute_env_vars`] resolves names against the process environment
//! only. [`substitute_vars`] takes an overlay of declared names that wins
//! over a same-named env var, with the environment as the fallback, so a
//! config file can declare its own variables once and reference them with
//! the same `${name}` / `${name:-default}` syntax.
//!
//! [`from_kdl_str_with_vars`] pairs the second one with the parser. A value
//! is inserted verbatim, so one holding a backslash or a double quote lands
//! as syntax inside a KDL quoted string and the line stops parsing; that
//! error names the variables whose values were inserted and which of them
//! carried such a character, never the value itself.

use kdl::{KdlDocument, KdlEntry, KdlNode, KdlValue};
use saci_core::error::{SaciError, SaciResult};
use std::collections::HashMap;

/// A parsed configuration value.
///
/// `serde_json::Value` is the tree rather than a bespoke enum because it
/// already carries a `serde::Deserializer` that internally tagged enums,
/// `flatten` and `deny_unknown_fields` are all exercised against, plus the
/// accessor surface (`get`, `as_str`, `as_i64`, `as_bool`, `as_array`,
/// `as_object`) a factory reads an opaque config bag with.
pub type ConfigValue = serde_json::Value;

/// The table form of a [`ConfigValue`].
pub type ConfigMap = serde_json::Map<String, serde_json::Value>;

/// Parse KDL `text` into a [`ConfigValue`], always a table at the top level.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when the document is not valid KDL, or
/// when it violates one of the mapping rules. Every message opens with
/// `parsing KDL: line:column:`, then a parser diagnostic or the offending node
/// and key.
pub fn from_kdl_str(text: &str) -> SaciResult<ConfigValue> {
    let document = KdlDocument::parse(text).map_err(|error| {
        SaciError::configuration(format!("parsing KDL: {}", render_parse_error(&error)))
    })?;
    let mut table = ConfigMap::new();
    extend_with_document(&mut table, &document, "document", text)?;
    Ok(ConfigValue::Object(table))
}

/// Substitute `raw`'s placeholders against `declared`, then parse it.
///
/// [`substitute_vars`] then [`from_kdl_str`], with one addition: when the
/// substituted text does not parse, the error also names the variables whose
/// values were inserted, and says which of them holds a backslash or a double
/// quote. Both are syntax inside a KDL quoted string, so a value carrying one
/// breaks a line that reads correctly in the file. A value is never printed:
/// a variable can hold a DSN or a credential.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] for anything [`substitute_vars`] or
/// [`from_kdl_str`] rejects.
pub fn from_kdl_str_with_vars(
    raw: &str,
    declared: &HashMap<String, String>,
) -> SaciResult<ConfigValue> {
    let substituted = substitute_vars(raw, declared)?;
    from_kdl_str(&substituted).map_err(|error| with_substitution_context(error, raw, declared))
}

/// Deserialize a field that may be written once or several times into a `Vec`.
///
/// Sibling nodes collapse into an array only from the second occurrence
/// (rule 5), so a one-element list is a single value in the tree. Every
/// list-valued config field carries this.
///
/// Goes through [`ConfigValue`] first rather than an untagged enum so a
/// `deny_unknown_fields` violation inside one entry still names the offending
/// key: an untagged enum would collapse that into a generic "data did not
/// match any variant" error instead.
///
/// # Errors
///
/// Returns the deserializer's own error when an entry does not match `T`.
pub fn one_or_many<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    use serde::Deserialize as _;
    use serde::de::Error as _;

    match ConfigValue::deserialize(deserializer)? {
        ConfigValue::Array(items) => items
            .into_iter()
            .map(|item| T::deserialize(item).map_err(D::Error::custom))
            .collect(),
        single => T::deserialize(single)
            .map(|value| vec![value])
            .map_err(D::Error::custom),
    }
}

/// Substitute `${VAR}` and `${VAR:-default}` placeholders in `raw` by asking
/// `resolve` for each variable name.
///
/// `Ok(Some(value))` pushes `value` into the output verbatim — never
/// re-scanned. `Ok(None)` means the name is unset: `${VAR:-default}`
/// substitutes `default` and a bare `${VAR}` is an error.
fn substitute_with(
    raw: &str,
    resolve: &mut dyn FnMut(&str) -> SaciResult<Option<String>>,
) -> SaciResult<String> {
    let mut result = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next();

            let mut placeholder = String::new();
            let mut closed = false;
            for inner in chars.by_ref() {
                if inner == '}' {
                    closed = true;
                    break;
                }
                placeholder.push(inner);
            }

            if !closed {
                return Err(SaciError::configuration(
                    "unclosed '${' in config — missing '}'",
                ));
            }

            let (var_name, fallback) = if let Some(pos) = placeholder.find(":-") {
                (&placeholder[..pos], Some(&placeholder[pos + 2..]))
            } else {
                (placeholder.as_str(), None)
            };

            match resolve(var_name)? {
                Some(val) => result.push_str(&val),
                None => match fallback {
                    Some(default) => result.push_str(default),
                    None => {
                        return Err(SaciError::configuration(format!(
                            "env var '${{{var_name}}}' is not set and has no default"
                        )));
                    }
                },
            }
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}

/// Substitute `${VAR}` and `${VAR:-default}` placeholders in `raw`.
///
/// - `${VAR}`: replaced with the value of env var `VAR`. Returns
///   [`SaciError::Configuration`] if `VAR` is not set.
/// - `${VAR:-default}`: replaced with the value of `VAR`, or `default` when
///   `VAR` is not set.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] for an unclosed `${`, or for a
/// placeholder whose var is unset and which declares no default.
pub fn substitute_env_vars(raw: &str) -> SaciResult<String> {
    substitute_with(raw, &mut |name| Ok(std::env::var(name).ok()))
}

/// Substitute `${name}` and `${name:-default}` placeholders in `raw`, with
/// `declared` names taking precedence over process environment variables.
///
/// Per placeholder the lookup order is `declared` first (case-sensitive exact
/// match), then [`std::env::var`]. A declared value may itself contain
/// placeholders, resolved recursively under the same rule, so declarations
/// can reference each other in any order; a value that reaches itself again
/// through the chain is a [`SaciError::Configuration`] cycle error.
/// Environment-provided values are inserted literally and never re-expanded.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] for an unclosed `${`, a placeholder
/// that is neither declared nor set in the environment and declares no
/// default, or a cyclic declared-value reference.
pub fn substitute_vars(raw: &str, declared: &HashMap<String, String>) -> SaciResult<String> {
    substitute_with(raw, &mut |name| {
        resolve_declared_or_env(name, declared, &mut Vec::new())
    })
}

/// Resolve one placeholder name the way [`substitute_vars`] does: `declared`
/// first, then the process environment.
///
/// A declared value is expanded under the same rule, with `stack` holding the
/// names currently being expanded so a chain that reaches one of them again
/// is a cycle error instead of an infinite descent.
fn resolve_declared_or_env(
    name: &str,
    declared: &HashMap<String, String>,
    stack: &mut Vec<String>,
) -> SaciResult<Option<String>> {
    if let Some(value) = declared.get(name) {
        if stack.iter().any(|on_stack| on_stack == name) {
            return Err(SaciError::configuration(format!(
                "cyclic variable reference: ${{{name}}}"
            )));
        }
        stack.push(name.to_string());
        let expanded = substitute_with(value, &mut |inner| {
            resolve_declared_or_env(inner, declared, stack)
        })?;
        stack.pop();
        return Ok(Some(expanded));
    }
    Ok(std::env::var(name).ok())
}

/// One placeholder whose resolved value reached the parser.
struct Insertion {
    name: String,
    /// The value held `\`, which opens an escape sequence in a quoted string.
    backslash: bool,
    /// The value held `"`, which closes a quoted string.
    quote: bool,
}

/// Tell the reader of a failed parse that the text was substituted first, and
/// which variables carried syntax into it.
///
/// The names come from scanning `raw` a second time, which happens only here:
/// the substitution that succeeded keeps no bookkeeping, so a file that parses
/// pays nothing for a note no one reads.
fn with_substitution_context(
    error: SaciError,
    raw: &str,
    declared: &HashMap<String, String>,
) -> SaciError {
    let mut inserted: Vec<Insertion> = Vec::new();
    let rescan = substitute_with(raw, &mut |name| {
        let resolved = resolve_declared_or_env(name, declared, &mut Vec::new())?;
        if let Some(value) = &resolved {
            let backslash = value.contains('\\');
            let quote = value.contains('"');
            match inserted.iter_mut().find(|entry| entry.name == name) {
                Some(entry) => {
                    entry.backslash |= backslash;
                    entry.quote |= quote;
                }
                None => inserted.push(Insertion {
                    name: name.to_string(),
                    backslash,
                    quote,
                }),
            }
        }
        Ok(resolved)
    });

    if rescan.is_err() || inserted.is_empty() {
        return error;
    }

    let names = inserted
        .iter()
        .map(|entry| format!("${{{}}}", entry.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut message = format!(
        "{}\n  note: substitution ran before the parser and inserted {names}",
        error.message()
    );
    for entry in &inserted {
        let held = match (entry.backslash, entry.quote) {
            (true, true) => "a backslash and a double quote",
            (true, false) => "a backslash",
            (false, true) => "a double quote",
            (false, false) => continue,
        };
        message.push_str(&format!(
            "\n  note: ${{{}}} holds {held}, which KDL reads as syntax inside a quoted string",
            entry.name
        ));
    }
    SaciError::configuration(message)
}

/// Add every node of `document` to `table`, collapsing same-named siblings.
///
/// `owner` names the enclosing scope in an error message and `input` is the
/// whole document text, which every error resolves its `line:column` against.
fn extend_with_document(
    table: &mut ConfigMap,
    document: &KdlDocument,
    owner: &str,
    input: &str,
) -> SaciResult<()> {
    // Grouped in document order in a Vec rather than a map: a config node has
    // a handful of children, and the linear scan keeps declaration order
    // without pulling in an insertion-ordered map.
    let mut groups: Vec<(&str, Vec<ConfigValue>, usize)> = Vec::new();

    for node in document.nodes() {
        let name = node.name().value();
        let value = node_to_value(node, input)?;
        match groups.iter_mut().find(|(key, _, _)| *key == name) {
            Some((_, values, _)) => values.push(value),
            None => groups.push((name, vec![value], node.span().offset())),
        }
    }

    for (name, values, offset) in groups {
        if table.contains_key(name) {
            return Err(duplicate_key(owner, name, input, offset));
        }
        let value = match <[ConfigValue; 1]>::try_from(values) {
            Ok([single]) => single,
            Err(many) => ConfigValue::Array(many),
        };
        table.insert(name.to_string(), value);
    }

    Ok(())
}

/// Map one node to its value under rules 2 through 6.
fn node_to_value(node: &KdlNode, input: &str) -> SaciResult<ConfigValue> {
    let name = node.name().value();
    let owner = format!("node \"{name}\"");
    let node_at = node.span().offset();

    if let Some(ty) = node.ty() {
        return Err(config_error(
            input,
            node_at,
            format!(
                "{owner}: type annotation '({})' is not a configuration value",
                ty.value()
            ),
        ));
    }

    let mut arguments: Vec<&KdlEntry> = Vec::new();
    let mut properties: Vec<(&str, &KdlEntry)> = Vec::new();
    // Iterating entries rather than calling `KdlNode::get`: the parser retains
    // every duplicate property and `get` returns only the last, which would
    // hide the repetition rule 2 rejects.
    for entry in node.entries() {
        if let Some(ty) = entry.ty() {
            return Err(config_error(
                input,
                entry.span().offset(),
                format!(
                    "{owner}: type annotation '({})' is not a configuration value",
                    ty.value()
                ),
            ));
        }
        match entry.name() {
            None => arguments.push(entry),
            Some(key) => properties.push((key.value(), entry)),
        }
    }

    let children = node.children();
    if properties.is_empty() && children.is_none() {
        return match arguments.len() {
            0 => Ok(ConfigValue::Object(ConfigMap::new())),
            1 => scalar_to_value(arguments[0], &owner, input),
            _ => arguments
                .into_iter()
                .map(|entry| scalar_to_value(entry, &owner, input))
                .collect::<SaciResult<Vec<_>>>()
                .map(ConfigValue::Array),
        };
    }

    let mut table = ConfigMap::new();

    if arguments.len() > 1 {
        return Err(config_error(
            input,
            node_at,
            format!(
                "{owner}: a node with properties or children takes at most one leading argument, \
                 found {}",
                arguments.len()
            ),
        ));
    }
    if let Some(argument) = arguments.first() {
        table.insert("id".to_string(), scalar_to_value(argument, &owner, input)?);
    }

    for (key, entry) in properties {
        if table.contains_key(key) {
            return Err(duplicate_key(&owner, key, input, entry.span().offset()));
        }
        table.insert(key.to_string(), scalar_to_value(entry, &owner, input)?);
    }

    if let Some(document) = children {
        extend_with_document(&mut table, document, &owner, input)?;
    }

    Ok(ConfigValue::Object(table))
}

/// Map one KDL scalar under rule 7.
fn scalar_to_value(entry: &KdlEntry, owner: &str, input: &str) -> SaciResult<ConfigValue> {
    let at = entry.span().offset();
    match entry.value() {
        KdlValue::String(text) => Ok(ConfigValue::String(text.clone())),
        KdlValue::Bool(flag) => Ok(ConfigValue::Bool(*flag)),
        KdlValue::Integer(number) => integer_to_value(*number, owner, input, at),
        // `Number::from_f64` rejects exactly the non-finite floats KDL spells
        // `#inf`, `#-inf` and `#nan`.
        KdlValue::Float(number) => serde_json::Number::from_f64(*number)
            .map(ConfigValue::Number)
            .ok_or_else(|| {
                config_error(
                    input,
                    at,
                    format!("{owner}: float '{number}' is not representable in configuration"),
                )
            }),
        KdlValue::Null => Err(config_error(
            input,
            at,
            format!("{owner}: #null is not a configuration value; omit the key instead"),
        )),
    }
}

fn integer_to_value(number: i128, owner: &str, input: &str, at: usize) -> SaciResult<ConfigValue> {
    if let Ok(signed) = i64::try_from(number) {
        return Ok(ConfigValue::Number(signed.into()));
    }
    if let Ok(unsigned) = u64::try_from(number) {
        return Ok(ConfigValue::Number(unsigned.into()));
    }
    Err(config_error(
        input,
        at,
        format!("{owner}: integer '{number}' does not fit in 64 bits"),
    ))
}

fn duplicate_key(owner: &str, key: &str, input: &str, at: usize) -> SaciError {
    config_error(input, at, format!("{owner}: duplicate key \"{key}\""))
}

/// A mapping-rule violation, positioned the same way a parser diagnostic is.
fn config_error(input: &str, at: usize, message: String) -> SaciError {
    let (line, column) = line_column(input, at);
    SaciError::configuration(format!("parsing KDL: {line}:{column}: {message}"))
}

/// Render every parser diagnostic as `line:column: message (help)`.
///
/// `KdlError` alone prints "Failed to parse KDL document", and its detail
/// lives in the diagnostics, whose spans are byte offsets into the input.
fn render_parse_error(error: &kdl::KdlError) -> String {
    let input = error.input.as_str();
    let mut rendered = String::new();

    for diagnostic in &error.diagnostics {
        if !rendered.is_empty() {
            rendered.push_str("; ");
        }
        let (line, column) = line_column(input, diagnostic.span.offset());
        let message = diagnostic.message.as_deref().unwrap_or("invalid syntax");
        rendered.push_str(&format!("{line}:{column}: {message}"));
        if let Some(help) = &diagnostic.help {
            rendered.push_str(&format!(" ({help})"));
        }
    }

    if rendered.is_empty() {
        rendered.push_str("invalid KDL document");
    }
    rendered
}

fn line_column(input: &str, offset: usize) -> (usize, usize) {
    let mut line = 1;
    let mut column = 1;
    for (index, ch) in input.char_indices() {
        if index >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    (line, column)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> ConfigValue {
        from_kdl_str(text).expect("valid config")
    }

    fn error(text: &str) -> String {
        let err = from_kdl_str(text).expect_err("invalid config");
        assert_eq!(err.category(), "configuration");
        err.message().to_string()
    }

    #[test]
    fn a_single_argument_node_is_a_scalar() {
        let value = parse("mode \"standalone\"\nid 7\nenabled #true\nratio 0.5\n");
        assert_eq!(value["mode"], "standalone");
        assert_eq!(value["id"], 7);
        assert_eq!(value["enabled"], true);
        assert_eq!(value["ratio"], 0.5);
    }

    #[test]
    fn several_arguments_on_one_node_are_an_array() {
        let value = parse("servers \"nats://a:4222\" \"nats://b:4222\"\nbackoff_ms 100 500\n");
        assert_eq!(
            value["servers"],
            serde_json::json!(["nats://a:4222", "nats://b:4222"])
        );
        assert_eq!(value["backoff_ms"], serde_json::json!([100, 500]));
    }

    #[test]
    fn properties_build_the_nodes_own_table() {
        let value = parse("node id=1 name=\"saci\" data_dir=\"/tmp/saci\"\n");
        assert_eq!(
            value["node"],
            serde_json::json!({ "id": 1, "name": "saci", "data_dir": "/tmp/saci" })
        );
    }

    #[test]
    fn a_leading_argument_on_a_table_node_becomes_the_id_key() {
        let value = parse("source \"csv_orders\" type=\"FileSource\"\n");
        assert_eq!(
            value["source"],
            serde_json::json!({ "id": "csv_orders", "type": "FileSource" })
        );
    }

    #[test]
    fn children_nest_as_tables_under_their_node_name() {
        let value = parse(
            r#"
source "csv_orders" type="FileSource" {
    config path="orders.csv" format="csv" {
        format_options has_headers=#true
    }
}
"#,
        );
        assert_eq!(
            value["source"],
            serde_json::json!({
                "id": "csv_orders",
                "type": "FileSource",
                "config": {
                    "path": "orders.csv",
                    "format": "csv",
                    "format_options": { "has_headers": true },
                },
            })
        );
    }

    #[test]
    fn same_named_siblings_collapse_into_an_array_in_document_order() {
        let value = parse(
            r#"
schema_fields "id" type="Int64" nullable=#false
schema_fields "amount" type="Float64"
"#,
        );
        assert_eq!(
            value["schema_fields"],
            serde_json::json!([
                { "id": "id", "type": "Int64", "nullable": false },
                { "id": "amount", "type": "Float64" },
            ])
        );
    }

    #[test]
    fn one_sibling_stays_a_single_value_that_one_or_many_accepts() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Spec {
            #[serde(deserialize_with = "one_or_many")]
            peer: Vec<Peer>,
        }
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Peer {
            id: u64,
        }

        let one: Spec = serde::Deserialize::deserialize(parse("peer id=1\n")).expect("one peer");
        assert_eq!(one.peer, vec![Peer { id: 1 }]);

        let two: Spec =
            serde::Deserialize::deserialize(parse("peer id=1\npeer id=2\n")).expect("two peers");
        assert_eq!(two.peer, vec![Peer { id: 1 }, Peer { id: 2 }]);
    }

    #[test]
    fn a_bare_node_is_an_empty_table() {
        let value = parse("pipeline\nhttp {}\n");
        assert_eq!(value["pipeline"], serde_json::json!({}));
        assert_eq!(value["http"], serde_json::json!({}));
    }

    #[test]
    fn an_integer_above_i64_maps_to_u64() {
        let value = parse("big 18446744073709551615\n");
        assert_eq!(value["big"], serde_json::json!(18446744073709551615u64));
    }

    #[test]
    fn a_repeated_property_is_an_error_naming_the_key() {
        assert_eq!(
            error("config path=\"a.csv\" path=\"b.csv\"\n"),
            "parsing KDL: 1:21: node \"config\": duplicate key \"path\""
        );
    }

    #[test]
    fn a_leading_argument_beside_an_id_property_is_a_duplicate_key() {
        assert_eq!(
            error("source \"a\" id=\"b\"\n"),
            "parsing KDL: 1:12: node \"source\": duplicate key \"id\""
        );
    }

    #[test]
    fn two_leading_arguments_on_a_table_node_are_an_error() {
        assert_eq!(
            error("source \"a\" \"b\" type=\"FileSource\"\n"),
            "parsing KDL: 1:1: node \"source\": a node with properties or children takes at most \
             one leading argument, found 2"
        );
    }

    #[test]
    fn a_property_colliding_with_a_child_node_is_an_error() {
        assert_eq!(
            error("config format=\"csv\" {\n    format \"ndjson\"\n}\n"),
            "parsing KDL: 2:5: node \"config\": duplicate key \"format\""
        );
    }

    #[test]
    fn null_is_rejected_with_the_advice_to_omit_the_key() {
        assert_eq!(
            error("sha3_256 #null\n"),
            "parsing KDL: 1:10: node \"sha3_256\": #null is not a configuration value; omit the \
             key instead"
        );
    }

    #[test]
    fn a_non_finite_float_is_rejected() {
        assert_eq!(
            error("trace_sample_ratio #inf\n"),
            "parsing KDL: 1:20: node \"trace_sample_ratio\": float 'inf' is not representable in \
             configuration"
        );
    }

    #[test]
    fn an_integer_beyond_u64_is_rejected() {
        assert_eq!(
            error("big 18446744073709551616\n"),
            "parsing KDL: 1:5: node \"big\": integer '18446744073709551616' does not fit in 64 bits"
        );
    }

    #[test]
    fn a_type_annotation_is_rejected() {
        assert_eq!(
            error("node id=(u8)1\n"),
            "parsing KDL: 1:6: node \"node\": type annotation '(u8)' is not a configuration value"
        );
        assert_eq!(
            error("(person)node id=1\n"),
            "parsing KDL: 1:1: node \"node\": type annotation '(person)' is not a configuration value"
        );
    }

    #[test]
    fn a_malformed_document_reports_line_and_column() {
        let message = error("mode \"standalone\nnode id=1\n");
        assert!(message.starts_with("parsing KDL: 1:6:"), "got: {message}");
    }

    #[test]
    fn a_slashdash_elides_the_node_it_precedes() {
        let value = parse("/-source \"gone\" type=\"FileSource\"\nsink \"kept\"\n");
        assert!(value.get("source").is_none());
        assert_eq!(value["sink"], "kept");
    }

    #[test]
    fn env_vars_substitute_before_the_parser_sees_the_text() {
        unsafe { std::env::set_var("SACI_TEST_KDL_DIR", "/var/lib/saci") };
        let raw = "node data_dir=\"${SACI_TEST_KDL_DIR}\" name=\"${SACI_TEST_KDL_NAME:-saci}\"\n";
        let value = parse(&substitute_env_vars(raw).expect("substituted"));
        assert_eq!(value["node"]["data_dir"], "/var/lib/saci");
        assert_eq!(value["node"]["name"], "saci");
        unsafe { std::env::remove_var("SACI_TEST_KDL_DIR") };
    }

    #[test]
    fn an_unset_env_var_without_a_default_is_an_error() {
        let err = substitute_env_vars("node data_dir=\"${SACI_TEST_KDL_UNSET}\"\n")
            .expect_err("unset var");
        assert_eq!(
            err.message(),
            "env var '${SACI_TEST_KDL_UNSET}' is not set and has no default"
        );
    }

    #[test]
    fn declared_variables_substitute_in_place_of_placeholders() {
        let declared = HashMap::from([("mod_dir".to_string(), "wasm".to_string())]);
        let out = substitute_vars(r#"node p="/tmp/${mod_dir}/x""#, &declared).expect("substituted");
        assert_eq!(out, r#"node p="/tmp/wasm/x""#);
    }

    #[test]
    fn a_declared_variable_shadows_a_same_named_env_var() {
        unsafe { std::env::set_var("SACI_TEST_SHADOW", "from-env") };
        let declared = HashMap::from([("SACI_TEST_SHADOW".to_string(), "from-file".to_string())]);
        let out =
            substitute_vars("node p=\"${SACI_TEST_SHADOW}\"", &declared).expect("substituted");
        assert_eq!(out, r#"node p="from-file""#);
        unsafe { std::env::remove_var("SACI_TEST_SHADOW") };
    }

    #[test]
    fn an_undeclared_variable_falls_back_to_the_env() {
        unsafe { std::env::set_var("SACI_TEST_UNDECLARED_FALLBACK", "/env/path") };
        let out = substitute_vars(
            "node p=\"${SACI_TEST_UNDECLARED_FALLBACK}\"",
            &HashMap::new(),
        )
        .expect("substituted");
        assert_eq!(out, r#"node p="/env/path""#);
        unsafe { std::env::remove_var("SACI_TEST_UNDECLARED_FALLBACK") };
    }

    #[test]
    fn a_default_is_used_when_a_variable_is_nowhere_and_loses_to_a_declared_value() {
        let out = substitute_vars(
            "node p=\"${SACI_TEST_NOWHERE:-/fallback}\"",
            &HashMap::new(),
        )
        .expect("substituted");
        assert_eq!(out, r#"node p="/fallback""#);

        let declared = HashMap::from([("SACI_TEST_NOWHERE".to_string(), "/declared".to_string())]);
        let out = substitute_vars("node p=\"${SACI_TEST_NOWHERE:-/fallback}\"", &declared)
            .expect("substituted");
        assert_eq!(out, r#"node p="/declared""#);
    }

    #[test]
    fn declared_values_expand_recursively_but_env_values_stay_literal() {
        let declared = HashMap::from([
            ("a".to_string(), "${b}/x".to_string()),
            ("b".to_string(), "2".to_string()),
        ]);
        let out = substitute_vars("node p=\"${a}\"", &declared).expect("substituted");
        assert_eq!(out, r#"node p="2/x""#);

        unsafe { std::env::set_var("SACI_TEST_INNER", "expanded") };
        unsafe { std::env::set_var("SACI_TEST_LITERAL", "${SACI_TEST_INNER}") };
        let out = substitute_vars("node p=\"${SACI_TEST_LITERAL}\"", &HashMap::new())
            .expect("substituted");
        assert_eq!(out, r#"node p="${SACI_TEST_INNER}""#);
        unsafe { std::env::remove_var("SACI_TEST_INNER") };
        unsafe { std::env::remove_var("SACI_TEST_LITERAL") };
    }

    #[test]
    fn a_cycle_in_declared_values_is_an_error() {
        let declared = HashMap::from([("a".to_string(), "${a}".to_string())]);
        let err = substitute_vars("node p=\"${a}\"", &declared).expect_err("self cycle");
        assert!(
            err.message().contains("cyclic variable reference"),
            "{}",
            err.message()
        );

        let declared = HashMap::from([
            ("a".to_string(), "${b}".to_string()),
            ("b".to_string(), "${a}".to_string()),
        ]);
        let err = substitute_vars("node p=\"${a}\"", &declared).expect_err("mutual cycle");
        assert!(
            err.message().contains("cyclic variable reference"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn a_substituted_backslash_names_its_variable_and_never_its_value() {
        let declared = HashMap::from([("out_dir".to_string(), r"C:\Users\me\out".to_string())]);
        let err = from_kdl_str_with_vars(
            "sink \"orders\" {\n    config path=\"${out_dir}/orders.csv\"\n}\n",
            &declared,
        )
        .expect_err("a backslash breaks the quoted string");

        let message = err.message();
        assert!(message.contains("${out_dir}"), "got: {message}");
        assert!(message.contains("backslash"), "got: {message}");
        assert!(!message.contains(r"C:\"), "leaked the value: {message}");
        assert!(!message.contains("Users"), "leaked the value: {message}");
    }
}
