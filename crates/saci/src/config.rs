//! The SACI configuration language.
//!
//! Configuration is written in [KDL](https://kdl.dev). This crate turns that
//! text into [`ConfigValue`], a self-describing value tree that carries a
//! `serde::Deserializer`, so the host config schema and every connector,
//! codec and plugin config struct deserializes from it with the serde
//! attributes it already has.
//!
//! ```
//! use saci::config::from_kdl_str;
//!
//! let value = from_kdl_str(r#"
//!     source "csv_orders" connector="file" {
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
//! # Writing
//!
//! [`to_kdl_string`] and [`to_kdl`] run the mapping backwards: a table entry
//! with a scalar value is a property, a string `id` is the leading argument,
//! nested tables are child nodes and arrays are repeated nodes or one node
//! with several arguments. Reading the output back with [`from_kdl_str`]
//! returns the tree that was written, except that `null` entries and empty
//! arrays vanish and a one-element array comes back as its element (which is
//! why list fields carry [`one_or_many`]). An array of arrays, or one that
//! mixes tables with scalars, has no spelling and is an error naming the key
//! path.
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

use crate::error::{SaciError, SaciResult};
use kdl::{
    FormatConfig, KdlDocument, KdlDocumentFormat, KdlEntry, KdlEntryFormat, KdlNode, KdlValue,
};
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

/// Write `value` as KDL text: the inverse of [`from_kdl_str`].
///
/// `value` must be a table. The document is built from `kdl` nodes, so
/// quoting and escaping are that crate's, then laid out the way the files
/// under `examples/configs` are: two-space indent, strings always quoted, a
/// blank line between top-level nodes. Entries come out in the table's own
/// order, which is declaration order (the `config` feature turns on
/// `serde_json/preserve_order`), so the same value is always the same bytes.
///
/// The mapping is the seven rules of the module docs read backwards:
///
/// - A scalar entry of a table is a property on the node that owns the
///   table. A scalar at the top of the document is a node with one argument:
///   `mode "standalone"`.
/// - A string `id` is the owning node's leading argument (`source "orders"
///   connector="file"`), unless it is the only entry that survives: then it
///   is the property `id="orders"`, because a node holding one argument
///   alone reads back as a scalar, not as a table. A numeric or boolean `id`
///   is always the property, as `node id=1` is written in every example.
/// - A nested table is a child node; an empty one is a bare node.
/// - An array of tables is repeated sibling nodes. An array of scalars is
///   one child node with the elements as arguments, never a property, which
///   holds one value only.
/// - `null` and an empty array are omitted.
///
/// Strings are written exactly as they are. A `${NAME}` or `${NAME:-fallback}`
/// inside one stays a placeholder, which [`substitute_vars`] expands on the
/// next load, so a value that really holds `${` cannot be spelled.
///
/// # Round trip
///
/// `from_kdl_str(&to_kdl_string(&value)?)` is `value` for every table the
/// reader can return. For a hand-built tree it is `value` with these
/// differences, each a consequence of the reader's rules:
///
/// - `null` entries and empty arrays are gone: KDL has no spelling for
///   either. A table that held only those is `{}`.
/// - An array of one element comes back as that element, because sibling
///   nodes collapse into an array only from the second one (rule 5). Fields
///   that hold a list read it with [`one_or_many`], which accepts both.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`], naming the key path
/// (`workflow[1].source.tags`), when:
///
/// - `value` is not a table;
/// - an array holds an array, a `null`, or tables mixed with scalars: none
///   has a spelling the reader maps back to the same array;
/// - a name or string holds a character KDL cannot carry inside a quoted
///   string (a control character, a direction mark or a line separator other
///   than `\n` and `\r`).
///
/// ```
/// use saci::config::{from_kdl_str, to_kdl_string};
///
/// let value = from_kdl_str(r#"
///     source "csv_orders" connector="file" {
///         config path="orders.csv"
///     }
/// "#).unwrap();
///
/// let text = to_kdl_string(&value).unwrap();
/// assert_eq!(
///     text,
///     "source \"csv_orders\" connector=\"file\" {\n  config path=\"orders.csv\"\n}\n"
/// );
/// assert_eq!(from_kdl_str(&text).unwrap(), value);
/// ```
pub fn to_kdl_string(value: &ConfigValue) -> SaciResult<String> {
    let ConfigValue::Object(table) = value else {
        let found = match value {
            ConfigValue::Null => "null",
            ConfigValue::Bool(_) => "a boolean",
            ConfigValue::Number(_) => "a number",
            ConfigValue::String(_) => "a string",
            ConfigValue::Array(_) => "an array",
            ConfigValue::Object(_) => "a table",
        };
        return Err(SaciError::configuration(format!(
            "writing KDL: the top level must be a table, found {found}"
        )));
    };

    let mut document = KdlDocument::new();
    for (key, value) in table {
        let at = KeyPath::key(None, key);
        push_entry_nodes(document.nodes_mut(), key, value, &at)?;
    }
    document.autoformat_config(&FormatConfig::builder().indent("  ").build());
    for node in document.nodes_mut().iter_mut().skip(1) {
        if let Some(format) = node.format_mut() {
            format.leading.insert(0, '\n');
        }
    }
    Ok(document.to_string())
}

/// Serialize `value` and write it as KDL text with [`to_kdl_string`].
///
/// `value` goes through `serde_json::to_value` first, so a struct writes its
/// fields in declaration order and honours `skip_serializing_if`, and the
/// round-trip rules and errors are those of [`to_kdl_string`]. A type with a
/// `HashMap` field should serialize it sorted, or the text changes from run
/// to run.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `value` cannot be serialized,
/// or for anything [`to_kdl_string`] rejects.
pub fn to_kdl<T: serde::Serialize + ?Sized>(value: &T) -> SaciResult<String> {
    let value = serde_json::to_value(value)
        .map_err(|error| SaciError::configuration(format!("serializing config: {error}")))?;
    to_kdl_string(&value)
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

/// Where a value sits in the tree, for an error message: `workflow[1].source`.
///
/// A linked list on the stack rather than a `String` built per entry: the path
/// is only rendered when a write fails.
struct KeyPath<'a> {
    parent: Option<&'a KeyPath<'a>>,
    step: Step<'a>,
}

enum Step<'a> {
    Key(&'a str),
    Index(usize),
}

impl<'a> KeyPath<'a> {
    fn key(parent: Option<&'a KeyPath<'a>>, key: &'a str) -> Self {
        Self {
            parent,
            step: Step::Key(key),
        }
    }

    fn index(parent: Option<&'a KeyPath<'a>>, index: usize) -> Self {
        Self {
            parent,
            step: Step::Index(index),
        }
    }
}

impl std::fmt::Display for KeyPath<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(parent) = self.parent {
            parent.fmt(f)?;
            if matches!(self.step, Step::Key(_)) {
                f.write_str(".")?;
            }
        }
        match self.step {
            Step::Key(key) => f.write_str(key),
            Step::Index(index) => write!(f, "[{index}]"),
        }
    }
}

fn write_error(path: &KeyPath<'_>, message: &str) -> SaciError {
    SaciError::configuration(format!("writing KDL: {path}: {message}"))
}

/// Append the nodes `key = value` contributes to its owner: none for a `null`
/// or an empty array, one node for a scalar or a table, one node per element
/// for an array of tables.
fn push_entry_nodes(
    out: &mut Vec<KdlNode>,
    key: &str,
    value: &ConfigValue,
    path: &KeyPath<'_>,
) -> SaciResult<()> {
    match value {
        ConfigValue::Null => {}
        ConfigValue::Array(items) => push_array_nodes(out, key, items, path)?,
        ConfigValue::Object(table) => out.push(table_node(key, table, path)?),
        scalar => {
            let mut node = named_node(key, path)?;
            node.entries_mut().push(scalar_entry(None, scalar, path)?);
            out.push(node);
        }
    }
    Ok(())
}

/// An array is one node with several arguments when every element is a scalar,
/// and repeated sibling nodes when every element is a table. Anything else has
/// no spelling the reader would map back to the same array.
fn push_array_nodes(
    out: &mut Vec<KdlNode>,
    key: &str,
    items: &[ConfigValue],
    path: &KeyPath<'_>,
) -> SaciResult<()> {
    let mut tables = 0usize;
    let mut scalars = 0usize;
    for item in items {
        match item {
            ConfigValue::Array(_) => {
                return Err(write_error(
                    path,
                    "an array of arrays has no KDL spelling; \
                     sibling nodes collapse into one flat array",
                ));
            }
            ConfigValue::Null => {
                return Err(write_error(
                    path,
                    "a null inside an array has no KDL spelling; \
                     drop the element or the key",
                ));
            }
            ConfigValue::Object(_) => tables += 1,
            _ => scalars += 1,
        }
    }
    if tables > 0 && scalars > 0 {
        return Err(write_error(
            path,
            "an array mixing tables and scalars has no KDL spelling; \
             an array holds either one argument list or repeated nodes",
        ));
    }

    if tables > 0 {
        for (index, item) in items.iter().enumerate() {
            if let ConfigValue::Object(table) = item {
                let at = KeyPath::index(Some(path), index);
                out.push(table_node(key, table, &at)?);
            }
        }
    } else if scalars > 0 {
        let mut node = named_node(key, path)?;
        for item in items {
            node.entries_mut().push(scalar_entry(None, item, path)?);
        }
        out.push(node);
    }
    Ok(())
}

/// One table as a node named `name`: rule 4 read backwards.
///
/// Scalar entries become properties, every other entry becomes a child node,
/// and a string `id` becomes the leading argument once the node has anything
/// else on it. A lone `id` stays a property: `source "a"` alone would be read
/// back as the scalar `"a"` (rule 3), not as a table holding `id`.
fn table_node(name: &str, table: &ConfigMap, path: &KeyPath<'_>) -> SaciResult<KdlNode> {
    let mut node = named_node(name, path)?;
    let mut properties: Vec<(&str, &ConfigValue)> = Vec::new();
    let mut children = KdlDocument::new();

    for (key, value) in table {
        let at = KeyPath::key(Some(path), key);
        match value {
            ConfigValue::Null => {}
            ConfigValue::Array(_) | ConfigValue::Object(_) => {
                push_entry_nodes(children.nodes_mut(), key, value, &at)?;
            }
            scalar => properties.push((key, scalar)),
        }
    }

    // Only a string id leads: `node id=1` and `peer id=1` are how every
    // example spells a numeric one, and the reader takes either form.
    let leading_id = match table.get("id") {
        Some(id @ ConfigValue::String(_)) if properties.len() > 1 || !children.is_empty() => {
            let at = KeyPath::key(Some(path), "id");
            node.entries_mut().push(scalar_entry(None, id, &at)?);
            true
        }
        _ => false,
    };
    for (key, value) in properties {
        if leading_id && key == "id" {
            continue;
        }
        let at = KeyPath::key(Some(path), key);
        node.entries_mut()
            .push(scalar_entry(Some(key), value, &at)?);
    }

    if !children.is_empty() {
        // A document with a format is laid out by `autoformat`; one without is
        // printed with a fixed four-space indent that ignores the nesting.
        children.set_format(KdlDocumentFormat::default());
        node.set_children(children);
    }
    Ok(node)
}

fn named_node(name: &str, path: &KeyPath<'_>) -> SaciResult<KdlNode> {
    reject_unspellable(name, path)?;
    Ok(KdlNode::new(name))
}

/// One KDL argument, or a property when `name` is given.
///
/// Quoting and escaping are the `kdl` crate's. Its one choice made here: a
/// string it would print as a bare identifier (`mode standalone`) is wrapped
/// in quotes, which is how every example writes one and cannot be mistaken
/// for a keyword or a number.
fn scalar_entry(
    name: Option<&str>,
    value: &ConfigValue,
    path: &KeyPath<'_>,
) -> SaciResult<KdlEntry> {
    let value = match value {
        ConfigValue::String(text) => {
            reject_unspellable(text, path)?;
            KdlValue::String(text.clone())
        }
        ConfigValue::Bool(flag) => KdlValue::Bool(*flag),
        ConfigValue::Number(number) => {
            if let Some(signed) = number.as_i64() {
                KdlValue::Integer(signed.into())
            } else if let Some(unsigned) = number.as_u64() {
                KdlValue::Integer(unsigned.into())
            } else {
                let float = number.as_f64().ok_or_else(|| {
                    write_error(path, &format!("number {number} is not representable"))
                })?;
                KdlValue::Float(float)
            }
        }
        ConfigValue::Null | ConfigValue::Array(_) | ConfigValue::Object(_) => {
            return Err(write_error(path, "expected a scalar"));
        }
    };

    let mut entry = match name {
        Some(key) => {
            reject_unspellable(key, path)?;
            KdlEntry::new_prop(key, value)
        }
        None => KdlEntry::new(value),
    };
    if entry.value().is_string() {
        let mut repr = entry.value().to_string();
        if !repr.starts_with('"') {
            repr = format!("\"{repr}\"");
        }
        // `autoformat_keep` makes `autoformat` retain the representation and
        // the leading space instead of resetting both.
        entry.set_format(KdlEntryFormat {
            value_repr: repr,
            leading: " ".into(),
            autoformat_keep: true,
            ..KdlEntryFormat::default()
        });
    }
    Ok(entry)
}

/// Refuse a name or string the `kdl` crate would print in a form its own
/// parser rejects.
///
/// The crate escapes quotes, backslashes and the common whitespace controls
/// but writes every other character as itself, and KDL forbids literal
/// control characters, direction marks and the line separators beyond `\n`
/// and `\r`. A file that cannot be read back is worse than an error here.
fn reject_unspellable(text: &str, path: &KeyPath<'_>) -> SaciResult<()> {
    let unspellable = |c: &char| {
        matches!(
            *c,
            '\u{0}'..='\u{7}'
                | '\u{B}'
                | '\u{E}'..='\u{1F}'
                | '\u{85}'
                | '\u{200E}'..='\u{200F}'
                | '\u{2028}'..='\u{202E}'
                | '\u{2066}'..='\u{2069}'
                | '\u{FEFF}'
        )
    };
    match text.chars().find(unspellable) {
        Some(c) => Err(write_error(
            path,
            &format!(
                "U+{:04X} cannot be written in a KDL string or name",
                u32::from(c)
            ),
        )),
        None => Ok(()),
    }
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
        let value = parse("source \"csv_orders\" connector=\"file\"\n");
        assert_eq!(
            value["source"],
            serde_json::json!({ "id": "csv_orders", "connector": "file" })
        );
    }

    #[test]
    fn children_nest_as_tables_under_their_node_name() {
        let value = parse(
            r#"
source "csv_orders" connector="file" {
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
                "connector": "file",
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
field "id" type="int64" nullable=#false
field "amount" type="float64"
"#,
        );
        assert_eq!(
            value["field"],
            serde_json::json!([
                { "id": "id", "type": "int64", "nullable": false },
                { "id": "amount", "type": "float64" },
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
            error("source \"a\" \"b\" connector=\"file\"\n"),
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
        let value = parse("/-source \"gone\" connector=\"file\"\nsink \"kept\"\n");
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

    // ── Writer ───────────────────────────────────────────────────────────────

    fn write(value: &ConfigValue) -> String {
        to_kdl_string(value).expect("writable")
    }

    fn write_failure(value: &ConfigValue) -> String {
        let err = to_kdl_string(value).expect_err("unwritable");
        assert_eq!(err.category(), "configuration");
        err.message().to_string()
    }

    /// The tree read back from what was written. Equal to the input whenever
    /// the round-trip caveats (nulls, empty arrays, one-element arrays) do not
    /// apply to it.
    fn round_trip(value: &ConfigValue) -> ConfigValue {
        let text = write(value);
        from_kdl_str(&text).unwrap_or_else(|error| panic!("{error}\n{text}"))
    }

    #[test]
    fn a_top_level_scalar_is_a_node_with_one_argument() {
        let value = serde_json::json!({
            "mode": "standalone", "retries": 3, "on": true, "ratio": 0.5,
        });
        assert_eq!(
            write(&value),
            "mode \"standalone\"\n\nretries 3\n\non #true\n\nratio 0.5\n"
        );
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn an_empty_document_writes_no_text() {
        let value = serde_json::json!({});
        assert_eq!(write(&value), "");
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn a_table_holding_only_an_id_writes_it_as_a_property() {
        let value = serde_json::json!({ "peer": { "id": 1 }, "source": { "id": "orders" } });
        assert_eq!(write(&value), "peer id=1\n\nsource id=\"orders\"\n");
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn a_string_id_beside_other_entries_is_the_leading_argument() {
        let value = serde_json::json!({
            "source": { "id": "csv_orders", "connector": "file", "component": "orders" },
        });
        assert_eq!(
            write(&value),
            "source \"csv_orders\" connector=\"file\" component=\"orders\"\n"
        );
        assert_eq!(round_trip(&value), value);

        let with_child = serde_json::json!({ "source": { "id": "a", "config": { "path": "x" } } });
        assert_eq!(
            write(&with_child),
            "source \"a\" {\n  config path=\"x\"\n}\n"
        );
        assert_eq!(round_trip(&with_child), with_child);
    }

    #[test]
    fn a_numeric_id_stays_a_property_as_in_the_examples() {
        let value = serde_json::json!({
            "node": { "id": 1, "name": "saci", "data_dir": "/tmp/saci" },
        });
        assert_eq!(
            write(&value),
            "node id=1 name=\"saci\" data_dir=\"/tmp/saci\"\n"
        );
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn an_id_whose_siblings_are_all_omitted_is_still_a_property() {
        // `source "a"` would read back as the scalar "a", so the omitted
        // entries count as absent when deciding where the id goes.
        let value = serde_json::json!({ "source": { "id": "a", "codec": null, "tags": [] } });
        assert_eq!(write(&value), "source id=\"a\"\n");
        assert_eq!(
            round_trip(&value),
            serde_json::json!({ "source": { "id": "a" } })
        );
    }

    #[test]
    fn an_id_that_is_not_a_scalar_is_an_ordinary_entry() {
        let value = serde_json::json!({ "x": { "id": [1, 2], "a": 1 } });
        assert_eq!(write(&value), "x a=1 {\n  id 1 2\n}\n");
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn an_array_of_scalars_is_one_child_node_with_arguments() {
        let value = serde_json::json!({
            "cluster": { "servers": ["nats://a", "nats://b"], "backoff_ms": [100, 500] },
            "tags": ["x", "y"],
        });
        assert_eq!(
            write(&value),
            "cluster {\n  servers \"nats://a\" \"nats://b\"\n  backoff_ms 100 500\n}\n\ntags \"x\" \"y\"\n"
        );
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn an_array_of_tables_is_repeated_sibling_nodes() {
        let value = serde_json::json!({
            "workflow": {
                "id": "w",
                "source": [{ "id": "a", "connector": "file" }, { "id": "b" }],
            },
        });
        assert_eq!(
            write(&value),
            "workflow \"w\" {\n  source \"a\" connector=\"file\"\n  source id=\"b\"\n}\n"
        );
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn nesting_indents_two_spaces_a_level() {
        let value = serde_json::json!({
            "workflow": {
                "id": "w",
                "source": {
                    "id": "in",
                    "config": {
                        "path": "a",
                        "field": [{ "id": "x", "type": "int64" }, { "id": "y", "type": "utf8" }],
                    },
                },
            },
        });
        assert_eq!(
            write(&value),
            "workflow \"w\" {\n  source \"in\" {\n    config path=\"a\" {\n      \
             field \"x\" type=\"int64\"\n      field \"y\" type=\"utf8\"\n    }\n  }\n}\n"
        );
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn quotes_backslashes_and_whitespace_are_escaped_by_the_kdl_crate() {
        let value = serde_json::json!({
            "sink": {
                "id": "s",
                "path": "C:\\dir\\\"quoted\"",
                "note": "line1\nline2\ttab\u{8}\u{c}",
                "empty": "",
                "keyword": "true",
                "number": "42",
            },
        });
        let text = write(&value);
        assert!(
            text.contains(r##"path="C:\\dir\\\"quoted\"""##),
            "got: {text}"
        );
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn a_plain_string_is_still_quoted() {
        let value =
            serde_json::json!({ "mode": "standalone", "sink": { "id": "s", "kind": "file" } });
        assert_eq!(
            write(&value),
            "mode \"standalone\"\n\nsink \"s\" kind=\"file\"\n"
        );
    }

    #[test]
    fn a_placeholder_is_written_verbatim_and_survives_a_read_without_substitution() {
        let value = serde_json::json!({
            "node": { "name": "saci-${NODE:-1}", "data_dir": "${SACI_DATA_DIR:-/tmp/saci}" },
        });
        let text = write(&value);
        assert!(
            text.contains(r#"data_dir="${SACI_DATA_DIR:-/tmp/saci}""#),
            "got: {text}"
        );
        assert_eq!(round_trip(&value), value);

        // The next load expands it, which is why a literal `${` cannot be spelled.
        let declared = HashMap::from([("NODE".to_string(), "7".to_string())]);
        let expanded = from_kdl_str_with_vars(&text, &declared).expect("substituted");
        assert_eq!(expanded["node"]["name"], "saci-7");
    }

    #[test]
    fn names_that_are_not_plain_identifiers_are_quoted_by_the_kdl_crate() {
        let value = serde_json::json!({
            "content-type": "x",
            "has space": { "k y": 1 },
            "true": 2,
            "": 3,
        });
        assert_eq!(
            write(&value),
            "content-type \"x\"\n\n\"has space\" \"k y\"=1\n\n\"true\" 2\n\n\"\" 3\n"
        );
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn entries_are_written_in_declaration_order() {
        let value = serde_json::json!({ "zeta": 1, "alpha": { "y": 1, "b": 2 }, "mid": 3 });
        assert_eq!(write(&value), "zeta 1\n\nalpha y=1 b=2\n\nmid 3\n");
    }

    #[test]
    fn numbers_round_trip_with_their_kind() {
        let value = serde_json::json!({
            "negative": -7,
            "big": 18446744073709551615u64,
            "min": i64::MIN,
            "whole_float": 2.0,
            "huge": 1e21,
            "tiny": 1.5e-7,
            "max": f64::MAX,
            "fraction": 0.1,
        });
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn null_and_empty_array_entries_are_omitted() {
        let value = serde_json::json!({
            "a": null,
            "b": [],
            "c": { "d": null, "e": [], "f": 1 },
            "g": { "h": null },
        });
        assert_eq!(write(&value), "c f=1\n\ng\n");
        assert_eq!(
            round_trip(&value),
            serde_json::json!({ "c": { "f": 1 }, "g": {} })
        );
    }

    #[test]
    fn an_empty_table_is_a_bare_node() {
        let value = serde_json::json!({ "http": {}, "pipeline": {} });
        assert_eq!(write(&value), "http\n\npipeline\n");
        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn a_one_element_array_reads_back_as_its_element_which_one_or_many_accepts() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Spec {
            #[serde(deserialize_with = "one_or_many")]
            peer: Vec<Peer>,
        }
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Peer {
            id: u64,
        }

        let value = serde_json::json!({ "peer": [{ "id": 1 }], "tags": ["x"] });
        let back = round_trip(&value);
        assert_eq!(
            back,
            serde_json::json!({ "peer": { "id": 1 }, "tags": "x" })
        );

        let spec: Spec = serde::Deserialize::deserialize(back).expect("one_or_many");
        assert_eq!(spec.peer, vec![Peer { id: 1 }]);
    }

    #[test]
    fn a_document_that_is_not_a_table_is_an_error() {
        assert_eq!(
            write_failure(&serde_json::json!([1, 2])),
            "writing KDL: the top level must be a table, found an array"
        );
        assert_eq!(
            write_failure(&serde_json::json!("mode")),
            "writing KDL: the top level must be a table, found a string"
        );
    }

    #[test]
    fn an_array_of_arrays_is_an_error_naming_the_key_path() {
        let message = write_failure(&serde_json::json!({ "a": { "b": [[1], [2]] } }));
        assert!(
            message.starts_with("writing KDL: a.b: an array of arrays"),
            "{message}"
        );
    }

    #[test]
    fn tables_mixed_with_scalars_are_an_error_naming_the_key_path() {
        let message = write_failure(&serde_json::json!({
            "workflow": [{ "id": "w", "source": [{ "id": "a" }, "b"] }],
        }));
        assert!(
            message
                .starts_with("writing KDL: workflow[0].source: an array mixing tables and scalars"),
            "{message}"
        );
    }

    #[test]
    fn a_null_inside_an_array_is_an_error_naming_the_key_path() {
        let message = write_failure(&serde_json::json!({ "a": [1, null] }));
        assert!(
            message.starts_with("writing KDL: a: a null inside an array"),
            "{message}"
        );
    }

    #[test]
    fn a_character_kdl_cannot_carry_is_an_error_naming_the_key_path() {
        let message = write_failure(&serde_json::json!({ "a": { "b": "x\u{1}y" } }));
        assert_eq!(
            message,
            "writing KDL: a.b: U+0001 cannot be written in a KDL string or name"
        );

        let message = write_failure(&serde_json::json!({ "a": { "b\u{85}": 1 } }));
        assert!(
            message.starts_with("writing KDL: a.b\u{85}: U+0085"),
            "{message}"
        );
    }

    #[test]
    fn the_writer_refuses_exactly_the_characters_the_parser_refuses() {
        // Characters the kdl crate prints unescaped: the writer must reject
        // each one the parser rejects and accept each one it accepts.
        let candidates = [
            '\u{0}',
            '\u{1}',
            '\u{7}',
            '\u{B}',
            '\u{E}',
            '\u{1F}',
            '\u{7F}',
            '\u{85}',
            '\u{200D}',
            '\u{200E}',
            '\u{200F}',
            '\u{2028}',
            '\u{2029}',
            '\u{202A}',
            '\u{202E}',
            '\u{2030}',
            '\u{2066}',
            '\u{2069}',
            '\u{FEFF}',
            'é',
            '\u{1F600}',
        ];
        for c in candidates {
            let parses = from_kdl_str(&format!("k \"{c}\"\n")).is_ok();
            let writes = to_kdl_string(&serde_json::json!({ "k": c.to_string() })).is_ok();
            assert_eq!(writes, parses, "U+{:04X}", u32::from(c));
        }
    }

    #[test]
    fn to_kdl_serializes_a_struct_in_field_order_and_drops_nones() {
        #[derive(serde::Serialize)]
        struct Peer {
            id: u64,
            addr: String,
        }
        #[derive(serde::Serialize)]
        struct Cluster {
            bootstrap: bool,
            note: Option<String>,
            peer: Vec<Peer>,
        }

        let text = to_kdl(&Cluster {
            bootstrap: true,
            note: None,
            peer: vec![
                Peer {
                    id: 1,
                    addr: "a:1".into(),
                },
                Peer {
                    id: 2,
                    addr: "b:2".into(),
                },
            ],
        })
        .expect("serializable");
        assert_eq!(
            text,
            "bootstrap #true\n\npeer id=1 addr=\"a:1\"\n\npeer id=2 addr=\"b:2\"\n"
        );

        let err = to_kdl(&5).expect_err("a number is not a document");
        assert_eq!(
            err.message(),
            "writing KDL: the top level must be a table, found a number"
        );
    }
}
