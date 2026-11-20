//! ClickHouse client construction and SQL query formatting helpers.

use crate::error::SaciError;

use crate::connector::clickhouse::config::{CompressionConfig, ConnectionConfig};
use crate::connector::schema::FieldType;

/// Builds a [`clickhouse::Client`] from [`ConnectionConfig`].
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] if the URL or database is malformed.
pub fn build_client(config: &ConnectionConfig) -> Result<clickhouse::Client, SaciError> {
    config.validate("clickhouse connection")?;

    let mut client = clickhouse::Client::default().with_url(&config.url);

    if let Some(user) = &config.user {
        client = client.with_user(user);
    }
    if let Some(password) = &config.password {
        client = client.with_password(password);
    }
    client = client.with_database(&config.database);

    match config.compression {
        CompressionConfig::Lz4 => {
            client = client.with_compression(clickhouse::Compression::Lz4);
        }
        CompressionConfig::None => {
            client = client.with_compression(clickhouse::Compression::None);
        }
    }

    // ClickHouse compresses an `ArrowStream` result with LZ4 by default
    // (`output_format_arrow_compression_method = lz4_frame`), which this crate's
    // `arrow-ipc` dependency cannot decode: `arrow-ipc/lz4` is enabled only
    // under the `conformance` feature, so a normal build refuses a compressed
    // batch by design. Ask for the uncompressed stream instead. A user setting
    // of the same name still wins, because it is applied after.
    client = client.with_setting("output_format_arrow_compression_method", "none");

    for (k, v) in &config.settings {
        client = client.with_setting(k, v);
    }

    if config.query_timeout_ms > 0 {
        let secs = config.query_timeout_ms.div_ceil(1000);
        client = client.with_setting("max_execution_time", secs.to_string());
    }

    Ok(client)
}

/// Quotes a single ClickHouse identifier with backticks.
pub fn quote_identifier(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

/// Quotes a potentially qualified ClickHouse table name (e.g. `db.table` -> `` `db`.`table` ``).
pub fn quote_qualified(name: &str) -> String {
    name.split('.')
        .map(quote_identifier)
        .collect::<Vec<_>>()
        .join(".")
}

/// Escapes a string literal for ClickHouse SQL.
pub fn escape_string_literal(val: &str) -> String {
    format!("'{}'", val.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// Formats a cursor scalar value for a SQL comparison clause.
pub fn format_cursor_value(val: &str, ty: FieldType) -> String {
    match ty {
        FieldType::Int8
        | FieldType::Int16
        | FieldType::Int32
        | FieldType::Int64
        | FieldType::UInt8
        | FieldType::UInt16
        | FieldType::UInt32
        | FieldType::UInt64
        | FieldType::Float32
        | FieldType::Float64
        | FieldType::Decimal128 => val.to_string(),
        FieldType::Boolean => {
            if val.eq_ignore_ascii_case("true") || val == "1" {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        _ => escape_string_literal(val),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_escapes_backticks() {
        assert_eq!(quote_identifier("my_col"), "`my_col`");
        assert_eq!(quote_identifier("col`name"), "`col``name`");
    }

    #[test]
    fn quote_qualified_handles_dotted_names() {
        assert_eq!(quote_qualified("events"), "`events`");
        assert_eq!(quote_qualified("mydb.events"), "`mydb`.`events`");
        assert_eq!(quote_qualified("db`1.tbl`2"), "`db``1`.`tbl``2`");
    }

    #[test]
    fn escape_string_literal_escapes_special_chars() {
        assert_eq!(escape_string_literal("plain"), "'plain'");
        assert_eq!(escape_string_literal("it's"), "'it\\'s'");
        assert_eq!(escape_string_literal("path\\to"), "'path\\\\to'");
    }

    #[test]
    fn format_cursor_value_differentiates_types() {
        assert_eq!(format_cursor_value("123", FieldType::Int64), "123");
        assert_eq!(format_cursor_value("45.67", FieldType::Float64), "45.67");
        assert_eq!(format_cursor_value("true", FieldType::Boolean), "1");
        assert_eq!(format_cursor_value("false", FieldType::Boolean), "0");
        assert_eq!(
            format_cursor_value("abc'def", FieldType::Utf8),
            "'abc\\'def'"
        );
    }

    #[test]
    fn build_client_with_full_options() {
        use std::collections::HashMap;
        let mut settings = HashMap::new();
        settings.insert("max_execution_time".to_string(), "60".to_string());

        let cfg = ConnectionConfig {
            url: "https://clickhouse.internal:8443".to_string(),
            user: Some("admin".to_string()),
            password: Some("s3cret".to_string()),
            database: "analytics".to_string(),
            query_timeout_ms: 60_000,
            compression: CompressionConfig::None,
            settings,
        };

        assert!(build_client(&cfg).is_ok());
    }

    #[test]
    fn build_client_fails_on_invalid_config() {
        use std::collections::HashMap;
        let cfg = ConnectionConfig {
            url: "not_a_valid_url".to_string(),
            user: None,
            password: None,
            database: "default".to_string(),
            query_timeout_ms: 30000,
            compression: CompressionConfig::Lz4,
            settings: HashMap::new(),
        };

        assert!(build_client(&cfg).is_err());
    }

    #[test]
    fn format_cursor_value_covers_all_types() {
        // Signed integers
        assert_eq!(format_cursor_value("-10", FieldType::Int8), "-10");
        assert_eq!(format_cursor_value("-300", FieldType::Int16), "-300");
        assert_eq!(format_cursor_value("-50000", FieldType::Int32), "-50000");
        assert_eq!(
            format_cursor_value("-123456789", FieldType::Int64),
            "-123456789"
        );

        // Unsigned integers
        assert_eq!(format_cursor_value("10", FieldType::UInt8), "10");
        assert_eq!(format_cursor_value("300", FieldType::UInt16), "300");
        assert_eq!(format_cursor_value("50000", FieldType::UInt32), "50000");
        assert_eq!(
            format_cursor_value("123456789", FieldType::UInt64),
            "123456789"
        );

        // Floats and Decimals
        assert_eq!(format_cursor_value("1.23", FieldType::Float32), "1.23");
        assert_eq!(format_cursor_value("45.67", FieldType::Float64), "45.67");
        assert_eq!(
            format_cursor_value("99.999", FieldType::Decimal128),
            "99.999"
        );

        // Boolean variants
        assert_eq!(format_cursor_value("true", FieldType::Boolean), "1");
        assert_eq!(format_cursor_value("TRUE", FieldType::Boolean), "1");
        assert_eq!(format_cursor_value("1", FieldType::Boolean), "1");
        assert_eq!(format_cursor_value("false", FieldType::Boolean), "0");
        assert_eq!(format_cursor_value("0", FieldType::Boolean), "0");
        assert_eq!(format_cursor_value("unknown", FieldType::Boolean), "0");

        // Strings and Dates/Timestamps are escaped
        assert_eq!(
            format_cursor_value("2026-10-10", FieldType::Date32),
            "'2026-10-10'"
        );
        assert_eq!(
            format_cursor_value("2026-10-10", FieldType::Date64),
            "'2026-10-10'"
        );
        assert_eq!(
            format_cursor_value("2026-10-10 12:00:00", FieldType::TimestampMicros),
            "'2026-10-10 12:00:00'"
        );
        assert_eq!(
            format_cursor_value("2026-10-10 12:00:00", FieldType::TimestampMillis),
            "'2026-10-10 12:00:00'"
        );
        assert_eq!(
            format_cursor_value("O'Connor", FieldType::Utf8),
            "'O\\'Connor'"
        );
    }
}
