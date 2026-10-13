//! Connector-installed triggers for `cdc_trigger` with `install_triggers #true`.
//!
//! Runs once per session before the first read. It creates the changelog table
//! and three triggers on the watched table, then the [`CursorReader`] reads the
//! changelog with the cursor `__seq`.
//!
//! The changelog holds `__seq` (`BIGINT UNSIGNED AUTO_INCREMENT`), `__op`
//! (`CHAR(1)`), `__commit_ts` (`TIMESTAMP(6)`) and every watched column with its
//! `COLUMN_TYPE` and, for a text column, its character set, all but the first
//! three nullable. The character set matters: `COLUMN_TYPE` carries none, so a
//! copy without it would take the schema default, and in a `latin1` schema an
//! emoji in a `utf8mb4` watched column would fail the application's own write.
//!
//! The triggers are `saci_<n>_ai`, `saci_<n>_au` and `saci_<n>_ad`. An update
//! that changes a primary key is recorded as a delete then an insert, so
//! `apply_changes` never leaves the old key behind. The connector never drops
//! anything.
//!
//! A trigger runs under the `sql_mode` captured when it was created, so the
//! installing session sets an empty `sql_mode` while it creates them: copying a
//! value into a column of its own type cannot fail, but a zero date copied under
//! a strict mode would fail the application's `UPDATE`.
//!
//! `__seq` is allocated when the trigger inserts, not when the writer commits, so
//! the reader holds back rows behind a gap in it (see [`super::sequence`]).
//!
//! The statements and trigger bodies build from `connection::quote_ident` and
//! `quote_columns` only, and the catalog comes from `connection::columns_of` and
//! `primary_key_of`. Errors name the target through `Connector::target`.
//!
//! [`CursorReader`]: super::cursor::CursorReader

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use saci_core::error::SaciError;

use crate::connection::{
    ER_BINLOG_CREATE_ROUTINE_NEED_SUPER, ER_TRG_ALREADY_EXISTS, TableRef, columns_of, mysql_detail,
    primary_key_of, quote_columns, quote_ident, server_code,
};
use crate::types::CatalogColumn;

/// `ER_TABLEACCESS_DENIED_ERROR`: the user lacks a privilege on one table, which
/// is what a missing `TRIGGER` privilege reports.
const ER_TABLEACCESS_DENIED_ERROR: u16 = 1142;

/// The longest the source name's part of a trigger name may be: 64 characters
/// less `saci_` and `_ai`.
const MAX_STEM: usize = 56;

/// The columns the changelog adds ahead of the watched table's own.
pub(crate) const META_COLUMNS: [&str; 3] = ["__seq", "__op", "__commit_ts"];

/// Which write a trigger watches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Insert,
    Update,
    Delete,
}

impl Kind {
    /// The order the triggers are created in.
    const ALL: [Kind; 3] = [Kind::Insert, Kind::Delete, Kind::Update];

    fn suffix(self) -> &'static str {
        match self {
            Kind::Insert => "ai",
            Kind::Update => "au",
            Kind::Delete => "ad",
        }
    }

    fn event(self) -> &'static str {
        match self {
            Kind::Insert => "INSERT",
            Kind::Update => "UPDATE",
            Kind::Delete => "DELETE",
        }
    }
}

/// What the installer works on: the watched table, its changelog, and the name
/// the triggers carry.
#[derive(Debug, Clone)]
pub(crate) struct TriggerSpec {
    pub(crate) watched: TableRef,
    pub(crate) changelog: TableRef,
    stem: String,
}

/// What `install` found or made, for the reader to resolve its columns from.
#[derive(Debug)]
pub(crate) struct Installed {
    /// The watched table's columns, which the declared data fields resolve
    /// against.
    pub(crate) watched: Vec<CatalogColumn>,
    /// The changelog's columns, which the metadata fields resolve against.
    pub(crate) changelog: Vec<CatalogColumn>,
}

impl TriggerSpec {
    /// A spec for the source called `source_name`. The name is validated ASCII
    /// by the config, so truncating bytes is truncating characters.
    pub(crate) fn new(source_name: &str, watched: TableRef, changelog: TableRef) -> Self {
        let mut stem = source_name.replace('-', "_");
        stem.truncate(MAX_STEM);
        Self {
            watched,
            changelog,
            stem,
        }
    }

    /// `saci_<n>_ai`, `saci_<n>_au` or `saci_<n>_ad`.
    pub(crate) fn trigger_name(&self, kind: Kind) -> String {
        format!("saci_{}_{}", self.stem, kind.suffix())
    }

    /// `CREATE TABLE IF NOT EXISTS` for the changelog of `columns`.
    pub(crate) fn changelog_ddl(&self, columns: &[CatalogColumn]) -> String {
        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             `__seq` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, \
             `__op` CHAR(1) NOT NULL, \
             `__commit_ts` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)",
            self.changelog.quoted()
        );
        for column in columns {
            sql.push_str(", ");
            sql.push_str(&quote_ident(&column.name));
            sql.push(' ');
            sql.push_str(&column.column_type);
            if let Some(charset) = column
                .charset
                .as_deref()
                .filter(|name| name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
            {
                sql.push_str(" CHARACTER SET ");
                sql.push_str(charset);
            }
            sql.push_str(" NULL");
        }
        sql.push(')');
        sql
    }

    /// One `INSERT` into the changelog of the `side` (`NEW` or `OLD`) image,
    /// recorded as `op`.
    fn insert(&self, columns: &[CatalogColumn], op: char, side: &str) -> String {
        let names = quote_columns(columns.iter().map(|c| c.name.as_str()));
        let values = columns
            .iter()
            .map(|c| format!("{side}.{}", quote_ident(&c.name)))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "INSERT INTO {} (`__op`, {names}) VALUES ('{op}', {values})",
            self.changelog.quoted()
        )
    }

    /// The statement `ACTION_STATEMENT` must equal, whitespace aside.
    pub(crate) fn body(
        &self,
        kind: Kind,
        columns: &[CatalogColumn],
        primary_key: &[String],
    ) -> String {
        match kind {
            Kind::Insert => self.insert(columns, 'I', "NEW"),
            Kind::Delete => self.insert(columns, 'D', "OLD"),
            Kind::Update if primary_key.is_empty() => self.insert(columns, 'U', "NEW"),
            Kind::Update => {
                let unchanged = primary_key
                    .iter()
                    .map(|k| {
                        let k = quote_ident(k);
                        format!("OLD.{k} <=> NEW.{k}")
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ");
                format!(
                    "BEGIN IF NOT ({unchanged}) THEN {}; {}; ELSE {}; END IF; END",
                    self.insert(columns, 'D', "OLD"),
                    self.insert(columns, 'I', "NEW"),
                    self.insert(columns, 'U', "NEW"),
                )
            }
        }
    }

    /// `CREATE TRIGGER ... FOR EACH ROW <body>`, with no terminator.
    pub(crate) fn create_trigger(
        &self,
        kind: Kind,
        columns: &[CatalogColumn],
        primary_key: &[String],
    ) -> String {
        format!(
            "CREATE TRIGGER {}.{} AFTER {} ON {} FOR EACH ROW {}",
            quote_ident(&self.watched.schema),
            quote_ident(&self.trigger_name(kind)),
            kind.event(),
            self.watched.quoted(),
            self.body(kind, columns, primary_key)
        )
    }

    /// The remedy a failed `CREATE TRIGGER` earns, when its code has one.
    fn remedy(&self, code: Option<u16>) -> Option<String> {
        match code? {
            ER_BINLOG_CREATE_ROUTINE_NEED_SUPER => Some(
                "with binary logging on, creating triggers needs SUPER or \
                 log_bin_trust_function_creators = 1"
                    .to_string(),
            ),
            ER_TABLEACCESS_DENIED_ERROR => Some(format!(
                "the connecting user needs the TRIGGER privilege on '{}' and INSERT on '{}', \
                 because a trigger writes as its definer",
                self.watched, self.changelog
            )),
            _ => None,
        }
    }
}

/// Collapse every run of whitespace to one space, so a body the server stored
/// with different line breaks compares equal to the one generated.
pub(crate) fn normalise(statement: &str) -> String {
    statement.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Why `changelog` no longer matches `watched`, or `None` when it does. Column
/// order is ignored; a metadata column must keep its type.
pub(crate) fn changelog_drift(
    changelog: &[CatalogColumn],
    watched: &[CatalogColumn],
) -> Option<String> {
    let mut problems = Vec::new();

    let meta = |name: &str| changelog.iter().find(|c| c.name.eq_ignore_ascii_case(name));
    match meta("__seq") {
        Some(c)
            if c.data_type == "bigint"
                && c.column_type.to_ascii_lowercase().contains("unsigned") => {}
        Some(c) => problems.push(format!(
            "column '__seq' is {} in the changelog, not bigint unsigned",
            c.column_type
        )),
        None => problems.push("column '__seq' is missing from the changelog".to_string()),
    }
    match meta("__op") {
        Some(c) if c.data_type == "char" => {}
        Some(c) => problems.push(format!(
            "column '__op' is {} in the changelog, not char(1)",
            c.column_type
        )),
        None => problems.push("column '__op' is missing from the changelog".to_string()),
    }
    match meta("__commit_ts") {
        Some(c) if c.data_type == "timestamp" => {}
        Some(c) => problems.push(format!(
            "column '__commit_ts' is {} in the changelog, not timestamp(6)",
            c.column_type
        )),
        None => problems.push("column '__commit_ts' is missing from the changelog".to_string()),
    }

    for column in watched {
        match changelog.iter().find(|c| c.name == column.name) {
            None => problems.push(format!(
                "column '{}' is missing from the changelog",
                column.name
            )),
            Some(copy) => problems.extend(column_mismatch(copy, column)),
        }
    }
    for copy in changelog {
        let known = META_COLUMNS
            .iter()
            .any(|name| copy.name.eq_ignore_ascii_case(name))
            || watched.iter().any(|c| c.name == copy.name);
        if !known {
            problems.push(format!(
                "column '{}' exists only in the changelog",
                copy.name
            ));
        }
    }

    (!problems.is_empty()).then(|| problems.join("; "))
}

/// Why the changelog's `copy` of `column` cannot hold what `column` does, or
/// `None` when it can.
fn column_mismatch(copy: &CatalogColumn, column: &CatalogColumn) -> Option<String> {
    let same_charset = match (&copy.charset, &column.charset) {
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
        (None, None) => true,
        _ => false,
    };
    (!copy.column_type.eq_ignore_ascii_case(&column.column_type) || !same_charset).then(|| {
        format!(
            "column '{}' is {} in the changelog and {} in the table",
            column.name, copy.column_type, column.column_type
        )
    })
}

/// Why a column both tables have no longer matches, or `None` when none does.
/// Unlike [`changelog_drift`], a column added to or dropped from the table is
/// not a problem here: the triggers keep writing the columns they name, so a
/// running source goes on without the new one. A changed type is, because the
/// trigger copies under an empty `sql_mode`, which truncates a value too long
/// for the changelog's older, narrower column and says so only in a warning.
pub(crate) fn type_drift(changelog: &[CatalogColumn], watched: &[CatalogColumn]) -> Option<String> {
    let problems: Vec<String> = watched
        .iter()
        .filter_map(|column| {
            let copy = changelog.iter().find(|c| c.name == column.name)?;
            column_mismatch(copy, column)
        })
        .collect();
    (!problems.is_empty()).then(|| problems.join("; "))
}

/// Refuse to deliver rows the triggers captured after the watched table's
/// column types changed under a running source. [`install`] checks the whole
/// changelog once per session; this is the check a source that never
/// reconnects needs, run before a batch of captured rows goes out.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when a column's type or character set
/// differs between the table and the changelog, and [`SaciError::Generic`]
/// when a catalog read fails.
pub(crate) async fn verify_types(
    what: &str,
    conn: &mut Conn,
    spec: &TriggerSpec,
    target: &str,
) -> Result<(), SaciError> {
    let watched = columns_of(conn, what, &spec.watched, target).await?;
    let changelog = columns_of(conn, what, &spec.changelog, target).await?;
    match type_drift(&changelog, &watched) {
        Some(drift) => Err(SaciError::configuration(format!(
            "{what}: the changelog table '{}' no longer holds what '{}' does ({drift}), so rows \
             the triggers captured since may be truncated; drop the changelog and its triggers to \
             rebuild, or set install_triggers #false",
            spec.changelog, spec.watched
        ))),
        None => Ok(()),
    }
}

/// A trigger as `information_schema.TRIGGERS` lists it.
struct Existing {
    name: String,
    table: String,
    timing: String,
    event: String,
    statement: String,
}

/// Create the changelog and the three triggers, or confirm that an earlier
/// installation is intact.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when the watched table is missing, a
/// watched column collides with a changelog column, the changelog has drifted
/// from the table, or a same-named trigger has a different body, and
/// [`SaciError::Generic`] when a statement fails (a missing privilege names the
/// grant).
pub(crate) async fn install(
    what: &str,
    conn: &mut Conn,
    spec: &TriggerSpec,
    target: &str,
) -> Result<Installed, SaciError> {
    let watched = columns_of(conn, what, &spec.watched, target).await?;
    let primary_key = primary_key_of(conn, what, &spec.watched).await?;
    if let Some(clash) = watched.iter().find(|column| {
        META_COLUMNS
            .iter()
            .any(|meta| column.name.eq_ignore_ascii_case(meta))
    }) {
        return Err(SaciError::configuration(format!(
            "{what}: table '{}' has a column named '{}', which the changelog reserves; rename \
             it or read the table with mode kind \"polling\"",
            spec.watched, clash.name
        )));
    }

    let saved: Option<Option<String>> = conn
        .query_first("SELECT @@SESSION.sql_mode")
        .await
        .map_err(|e| step_error(what, "read sql_mode", &e))?;
    conn.query_drop("SET SESSION sql_mode = ''")
        .await
        .map_err(|e| step_error(what, "set sql_mode", &e))?;

    let result = ddl(what, conn, spec, target, &watched, &primary_key).await;

    // Restored whether or not the DDL worked; a dead connection cannot be
    // restored and the caller drops it.
    if let Some(Some(mode)) = saved {
        let _ = conn
            .exec_drop("SET SESSION sql_mode = ?", (mode.as_str(),))
            .await;
    }

    let changelog = result?;
    Ok(Installed { watched, changelog })
}

fn step_error(what: &str, step: &str, e: &mysql_async::Error) -> SaciError {
    SaciError::generic(format!(
        "{what}: cannot {step} to install triggers: {}",
        mysql_detail(e)
    ))
}

/// The statements that run under the empty `sql_mode`. Returns the changelog's
/// columns.
async fn ddl(
    what: &str,
    conn: &mut Conn,
    spec: &TriggerSpec,
    target: &str,
    watched: &[CatalogColumn],
    primary_key: &[String],
) -> Result<Vec<CatalogColumn>, SaciError> {
    // `CREATE TABLE IF NOT EXISTS` on an existing table still needs the CREATE
    // privilege, so the catalog decides whether to run it.
    let changelog = match columns_of(conn, what, &spec.changelog, target).await {
        Ok(columns) => columns,
        Err(SaciError::Configuration(_)) => {
            conn.query_drop(spec.changelog_ddl(watched))
                .await
                .map_err(|e| {
                    let grant = if server_code(&e) == Some(ER_TABLEACCESS_DENIED_ERROR) {
                        format!(
                            "; grant CREATE on '{}' or create '{}' yourself",
                            spec.changelog.schema, spec.changelog
                        )
                    } else {
                        String::new()
                    };
                    SaciError::generic(format!(
                        "{what}: cannot create changelog table '{}': {}{grant}",
                        spec.changelog,
                        mysql_detail(&e)
                    ))
                })?;
            columns_of(conn, what, &spec.changelog, target).await?
        }
        Err(e) => return Err(e),
    };
    if let Some(drift) = changelog_drift(&changelog, watched) {
        return Err(SaciError::configuration(format!(
            "{what}: changelog table '{}' no longer matches '{}' ({drift}); drop it and its \
             triggers to rebuild, or set install_triggers #false",
            spec.changelog, spec.watched
        )));
    }

    let existing = existing_triggers(what, conn, spec).await?;
    let mut created = 0usize;
    let mut reused = 0usize;
    for kind in Kind::ALL {
        let name = spec.trigger_name(kind);
        let body = spec.body(kind, watched, primary_key);
        match existing.iter().find(|t| t.name == name) {
            Some(found) => {
                check_existing(what, spec, kind, found, &body)?;
                reused += 1;
            }
            None => {
                let sql = spec.create_trigger(kind, watched, primary_key);
                match conn.query_drop(sql).await {
                    Ok(()) => created += 1,
                    Err(e) if server_code(&e) == Some(ER_TRG_ALREADY_EXISTS) => {
                        // A concurrent installer won the race: judge its work
                        // by the same rule.
                        let again = existing_triggers(what, conn, spec).await?;
                        let found = again.iter().find(|t| t.name == name).ok_or_else(|| {
                            SaciError::generic(format!(
                                "{what}: trigger '{name}' was reported as existing but is not \
                                 listed: {}",
                                mysql_detail(&e)
                            ))
                        })?;
                        check_existing(what, spec, kind, found, &body)?;
                        reused += 1;
                    }
                    Err(e) => {
                        let remedy = spec
                            .remedy(server_code(&e))
                            .map(|r| format!("; {r}"))
                            .unwrap_or_default();
                        return Err(SaciError::generic(format!(
                            "{what}: cannot create trigger '{name}' on '{}': {}{remedy}",
                            spec.watched,
                            mysql_detail(&e)
                        )));
                    }
                }
            }
        }
    }

    #[cfg(feature = "tracing")]
    tracing::info!(
        table = %spec.watched,
        changelog = %spec.changelog,
        created,
        reused,
        "triggers installed"
    );
    #[cfg(not(feature = "tracing"))]
    let _ = (created, reused);

    Ok(changelog)
}

async fn existing_triggers(
    what: &str,
    conn: &mut Conn,
    spec: &TriggerSpec,
) -> Result<Vec<Existing>, SaciError> {
    let rows: Vec<(String, String, String, String, String)> = conn
        .exec(
            "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION, \
             ACTION_STATEMENT FROM information_schema.TRIGGERS \
             WHERE TRIGGER_SCHEMA = ? AND TRIGGER_NAME IN (?, ?, ?)",
            (
                spec.watched.schema.as_str(),
                spec.trigger_name(Kind::Insert),
                spec.trigger_name(Kind::Update),
                spec.trigger_name(Kind::Delete),
            ),
        )
        .await
        .map_err(|e| {
            SaciError::generic(format!(
                "{what}: cannot list the triggers of '{}': {}",
                spec.watched,
                mysql_detail(&e)
            ))
        })?;
    Ok(rows
        .into_iter()
        .map(|(name, table, timing, event, statement)| Existing {
            name,
            table,
            timing,
            event,
            statement,
        })
        .collect())
}

/// Accept a same-named trigger only when it is the one this source would make.
fn check_existing(
    what: &str,
    spec: &TriggerSpec,
    kind: Kind,
    found: &Existing,
    body: &str,
) -> Result<(), SaciError> {
    let same = found.table == spec.watched.name
        && found.timing.eq_ignore_ascii_case("AFTER")
        && found.event.eq_ignore_ascii_case(kind.event())
        && normalise(&found.statement) == normalise(body);
    if same {
        Ok(())
    } else {
        Err(SaciError::configuration(format!(
            "{what}: trigger '{}' exists with a different body; drop it or set install_triggers \
             #false",
            found.name
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, column_type: &str, charset: Option<&str>) -> CatalogColumn {
        CatalogColumn {
            name: name.to_string(),
            ordinal: 0,
            data_type: column_type
                .split(['(', ' '])
                .next()
                .unwrap_or_default()
                .to_string(),
            column_type: column_type.to_string(),
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
            charset: charset.map(str::to_string),
        }
    }

    fn spec(name: &str) -> TriggerSpec {
        TriggerSpec::new(
            name,
            TableRef {
                schema: "shop".to_string(),
                name: "orders".to_string(),
            },
            TableRef {
                schema: "shop".to_string(),
                name: "saci_cdc_x".to_string(),
            },
        )
    }

    fn columns() -> Vec<CatalogColumn> {
        vec![
            column("id", "bigint(20)", None),
            column("label", "varchar(8)", Some("utf8mb4")),
        ]
    }

    #[test]
    fn trigger_names_swap_dashes_and_fit_64_chars() {
        let s = spec("my-orders");
        assert_eq!(s.trigger_name(Kind::Insert), "saci_my_orders_ai");
        assert_eq!(s.trigger_name(Kind::Update), "saci_my_orders_au");
        assert_eq!(s.trigger_name(Kind::Delete), "saci_my_orders_ad");
        let long = spec(&"n".repeat(191));
        for kind in Kind::ALL {
            assert_eq!(long.trigger_name(kind).len(), 64);
        }
    }

    #[test]
    fn the_insert_and_delete_bodies_copy_new_and_old_values() {
        let s = spec("x");
        assert_eq!(
            s.body(Kind::Insert, &columns(), &[]),
            "INSERT INTO `shop`.`saci_cdc_x` (`__op`, `id`, `label`) \
             VALUES ('I', NEW.`id`, NEW.`label`)"
        );
        assert_eq!(
            s.body(Kind::Delete, &columns(), &[]),
            "INSERT INTO `shop`.`saci_cdc_x` (`__op`, `id`, `label`) \
             VALUES ('D', OLD.`id`, OLD.`label`)"
        );
    }

    #[test]
    fn the_update_body_splits_a_key_change_into_delete_then_insert() {
        let body = spec("x").body(
            Kind::Update,
            &columns(),
            &["id".to_string(), "label".to_string()],
        );
        assert_eq!(
            body,
            "BEGIN IF NOT (OLD.`id` <=> NEW.`id` AND OLD.`label` <=> NEW.`label`) THEN \
             INSERT INTO `shop`.`saci_cdc_x` (`__op`, `id`, `label`) VALUES ('D', OLD.`id`, OLD.`label`); \
             INSERT INTO `shop`.`saci_cdc_x` (`__op`, `id`, `label`) VALUES ('I', NEW.`id`, NEW.`label`); \
             ELSE INSERT INTO `shop`.`saci_cdc_x` (`__op`, `id`, `label`) VALUES ('U', NEW.`id`, NEW.`label`); \
             END IF; END"
        );
    }

    #[test]
    fn an_update_body_without_a_primary_key_is_one_update_row() {
        let body = spec("x").body(Kind::Update, &columns(), &[]);
        assert!(body.starts_with("INSERT INTO"), "{body}");
        assert!(body.contains("VALUES ('U', NEW.`id`"), "{body}");
        assert!(!body.contains("IF"), "{body}");
    }

    #[test]
    fn names_with_a_backtick_are_quoted_in_every_statement() {
        let mut s = spec("x");
        s.watched.name = "o`; DROP TABLE x; --".to_string();
        let evil = vec![column("a`b", "int(11)", None)];
        let sql = s.create_trigger(Kind::Insert, &evil, &[]);
        assert!(sql.contains("ON `shop`.`o``; DROP TABLE x; --`"), "{sql}");
        assert!(sql.contains("NEW.`a``b`"), "{sql}");
        assert!(s.changelog_ddl(&evil).contains(", `a``b` int(11) NULL"));
    }

    #[test]
    fn the_changelog_ddl_copies_column_type_and_character_set() {
        let ddl = spec("x").changelog_ddl(&columns());
        assert!(
            ddl.contains(", `id` bigint(20) NULL, `label` varchar(8) CHARACTER SET utf8mb4 NULL)"),
            "{ddl}"
        );
    }

    #[test]
    fn the_changelog_ddl_has_the_metadata_columns_first() {
        assert_eq!(
            spec("x").changelog_ddl(&[]),
            "CREATE TABLE IF NOT EXISTS `shop`.`saci_cdc_x` (\
             `__seq` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, \
             `__op` CHAR(1) NOT NULL, \
             `__commit_ts` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6))"
        );
    }

    #[test]
    fn create_trigger_has_no_statement_terminator() {
        let s = spec("x");
        for kind in Kind::ALL {
            let sql = s.create_trigger(kind, &columns(), &["id".to_string()]);
            assert!(!sql.trim_end().ends_with(';'), "{sql}");
            assert!(sql.starts_with(&format!(
                "CREATE TRIGGER `shop`.`saci_x_{}` AFTER {} ON `shop`.`orders` FOR EACH ROW ",
                kind.suffix(),
                kind.event()
            )));
        }
    }

    #[test]
    fn bodies_compare_whitespace_normalised() {
        let generated = spec("x").body(Kind::Update, &columns(), &["id".to_string()]);
        let stored = generated
            .replace("; ", ";\r\n\t")
            .replace("THEN ", "THEN\n  ");
        assert_eq!(normalise(&stored), normalise(&generated));
        assert_ne!(normalise(&generated), normalise("INSERT INTO other"));
    }

    fn changelog_of(columns: &[CatalogColumn]) -> Vec<CatalogColumn> {
        let mut all = vec![
            column("__seq", "bigint(20) unsigned", None),
            column("__op", "char(1)", Some("utf8mb4")),
            column("__commit_ts", "timestamp(6)", None),
        ];
        all.extend(columns.iter().cloned());
        all
    }

    #[test]
    fn an_intact_changelog_has_no_drift_whatever_its_column_order() {
        let mut copy = changelog_of(&columns());
        assert_eq!(changelog_drift(&copy, &columns()), None);
        copy.reverse();
        assert_eq!(changelog_drift(&copy, &columns()), None);
    }

    #[test]
    fn a_drifted_changelog_names_the_column() {
        let mut copy = changelog_of(&columns());
        copy[4].column_type = "varchar(16)".to_string();
        let drift = changelog_drift(&copy, &columns()).expect("drift");
        assert!(
            drift.contains("column 'label' is varchar(16) in the changelog and varchar(8)"),
            "{drift}"
        );

        let mut missing = changelog_of(&columns());
        missing.pop();
        assert!(
            changelog_drift(&missing, &columns())
                .expect("drift")
                .contains("column 'label' is missing")
        );

        let mut extra = changelog_of(&columns());
        extra.push(column("stray", "int(11)", None));
        assert!(
            changelog_drift(&extra, &columns())
                .expect("drift")
                .contains("column 'stray' exists only in the changelog")
        );

        let mut charset = changelog_of(&columns());
        charset[4].charset = Some("latin1".to_string());
        assert!(changelog_drift(&charset, &columns()).is_some());
    }

    #[test]
    fn a_metadata_column_with_the_wrong_type_is_drift() {
        let mut copy = changelog_of(&columns());
        copy[0].column_type = "bigint(20)".to_string();
        assert!(
            changelog_drift(&copy, &columns())
                .expect("drift")
                .contains("__seq")
        );
    }

    #[test]
    fn only_a_changed_column_type_is_type_drift() {
        assert_eq!(type_drift(&changelog_of(&columns()), &columns()), None);

        let mut narrower = changelog_of(&columns());
        narrower[4].column_type = "varchar(4)".to_string();
        let drift = type_drift(&narrower, &columns()).expect("drift");
        assert!(
            drift.contains("column 'label' is varchar(4) in the changelog and varchar(8)"),
            "{drift}"
        );

        let mut charset = changelog_of(&columns());
        charset[4].charset = Some("latin1".to_string());
        assert!(type_drift(&charset, &columns()).is_some());

        // A column the table gained or lost is not a truncation hazard.
        let mut gained = columns();
        gained.push(column("extra", "int(11)", None));
        assert_eq!(type_drift(&changelog_of(&columns()), &gained), None);
        let mut lost = changelog_of(&columns());
        lost.pop();
        assert_eq!(type_drift(&lost, &columns()), None);
    }

    #[test]
    fn the_privilege_remedies_name_the_grant() {
        let s = spec("x");
        assert!(
            s.remedy(Some(ER_BINLOG_CREATE_ROUTINE_NEED_SUPER))
                .expect("1419")
                .contains("log_bin_trust_function_creators = 1")
        );
        let grant = s.remedy(Some(ER_TABLEACCESS_DENIED_ERROR)).expect("1142");
        assert!(
            grant.contains("TRIGGER privilege on 'shop.orders'"),
            "{grant}"
        );
        assert!(grant.contains("INSERT on 'shop.saci_cdc_x'"), "{grant}");
        assert_eq!(s.remedy(Some(1064)), None);
        assert_eq!(s.remedy(None), None);
    }

    #[test]
    fn a_same_named_trigger_with_another_body_or_table_is_refused() {
        let s = spec("x");
        let body = s.body(Kind::Insert, &columns(), &[]);
        let found = |table: &str, statement: &str| Existing {
            name: "saci_x_ai".to_string(),
            table: table.to_string(),
            timing: "AFTER".to_string(),
            event: "INSERT".to_string(),
            statement: statement.to_string(),
        };
        assert!(check_existing("W", &s, Kind::Insert, &found("orders", &body), &body).is_ok());
        let err = check_existing("W", &s, Kind::Insert, &found("orders", "SELECT 1"), &body)
            .expect_err("different body");
        assert!(err.message().contains("exists with a different body"));
        assert!(check_existing("W", &s, Kind::Insert, &found("other", &body), &body).is_err());
    }
}
