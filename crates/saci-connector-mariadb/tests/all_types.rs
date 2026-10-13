//! Every type family, end to end, on both servers.
//!
//! Each case seeds a table with SQL literals, reads it with `polling` and with
//! `cdc_binlog` (a change made after the stream position), requires the two
//! readers to produce identical Arrow values, writes the batches through a sink
//! into a twin table, and compares what the server stores in both
//! (`CAST(col AS CHAR)`, `HEX(col)` for binary and geometry) with the expected
//! text.

mod common;

use arrow_array::{Array as _, ArrayRef, RecordBatch};
use common::{Server, drain, sink, source};
use mysql_async::prelude::Queryable;
use saci_core::io::sink::Sink as _;

/// One column: name, SQL type, declared field attributes, and how to render
/// it for comparison.
struct Col {
    name: &'static str,
    sql: &'static str,
    kdl: &'static str,
    /// `HEX(c)` for a binary column, `CAST(c AS CHAR)` otherwise.
    hex: bool,
}

const fn col(name: &'static str, sql: &'static str, kdl: &'static str) -> Col {
    Col {
        name,
        sql,
        kdl,
        hex: false,
    }
}

const fn hexcol(name: &'static str, sql: &'static str, kdl: &'static str) -> Col {
    Col {
        name,
        sql,
        kdl,
        hex: true,
    }
}

fn fields(cols: &[Col]) -> String {
    let mut out = String::from("schema_fields \"id\" type=\"int64\" nullable=#false\n");
    for c in cols {
        out.push_str(&format!("schema_fields \"{}\" {}\n", c.name, c.kdl));
    }
    out
}

async fn exec(server: &Server, sql: &str) {
    let mut conn = server.conn().await;
    conn.query_drop(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn slices(batches: &[RecordBatch], name: &str) -> Vec<ArrayRef> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch.column_by_name(name).expect("column");
            (0..batch.num_rows())
                .map(|row| column.slice(row, 1))
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn rendered(server: &Server, table: &str, cols: &[Col]) -> Vec<Vec<Option<String>>> {
    let exprs: Vec<String> = cols
        .iter()
        .map(|c| {
            if c.hex {
                format!("HEX(`{}`)", c.name)
            } else {
                format!("CAST(`{}` AS CHAR)", c.name)
            }
        })
        .collect();
    let sql = format!("SELECT {} FROM {table} ORDER BY id", exprs.join(", "));
    let mut conn = server.conn().await;
    let rows: Vec<mysql_async::Row> = conn.query(sql).await.expect("render");
    rows.iter()
        .map(|row| {
            (0..cols.len())
                .map(|i| row.get::<Option<String>, _>(i).expect("column"))
                .collect()
        })
        .collect()
}

/// Run one family. `rows` are `VALUES` tuples starting with the id; `expected`
/// is what each row's columns read back as, in column order, or empty when the
/// server's own rendering is the reference (geometry).
async fn family(
    server: &Server,
    tag: &str,
    cols: &[Col],
    rows: &[&str],
    expected: &[&[Option<&str>]],
) {
    let src = format!("{tag}_src");
    let twin = format!("{tag}_twin");
    let defs: Vec<String> = cols
        .iter()
        .map(|c| format!("`{}` {}", c.name, c.sql))
        .collect();
    for table in [&src, &twin] {
        exec(server, &format!("DROP TABLE IF EXISTS {table}")).await;
        exec(
            server,
            &format!(
                "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, {})",
                defs.join(", ")
            ),
        )
        .await;
    }
    let declared = fields(cols);

    // The stream resolves `now` on its first call; changes made after it are
    // the ones it reads.
    let mut binlog = source(
        &server.dsn(),
        &format!(
            "name \"{tag}_cdc\"\nbatch_rows 100\n\nmode kind=\"cdc_binlog\" table=\"{src}\"\n{declared}"
        ),
    );
    assert!(drain(&mut binlog).await.is_empty());
    exec(
        server,
        &format!("INSERT INTO {src} VALUES {}", rows.join(", ")),
    )
    .await;
    let from_binlog = drain(&mut binlog).await;

    let mut polling = source(
        &server.dsn(),
        &format!(
            "name \"{tag}_poll\"\nbatch_rows 100\n\n\
             mode kind=\"polling\" table=\"{src}\" cursor_column=\"id\" initial=\"beginning\"\n{declared}"
        ),
    );
    let from_polling = drain(&mut polling).await;

    let total = |b: &[RecordBatch]| b.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(total(&from_binlog), rows.len(), "{tag}: binlog rows");
    assert_eq!(total(&from_polling), rows.len(), "{tag}: polling rows");
    for name in std::iter::once("id").chain(cols.iter().map(|c| c.name)) {
        assert_eq!(
            slices(&from_binlog, name),
            slices(&from_polling, name),
            "{tag}: column {name} differs between the binlog and polling"
        );
    }

    let mut writer = sink(
        &server.dsn(),
        &format!("name \"{tag}_sink\"\ntable \"{twin}\"\n{declared}"),
    );
    for batch in &from_polling {
        writer.write_batch(batch).await.expect("write");
    }
    writer.finish().await.expect("finish");

    let from_source = rendered(server, &src, cols).await;
    let from_twin = rendered(server, &twin, cols).await;
    if expected.is_empty() {
        assert!(
            from_source.iter().flatten().all(Option::is_some),
            "{tag}: {from_source:?}"
        );
    } else {
        let wanted: Vec<Vec<Option<String>>> = expected
            .iter()
            .map(|row| row.iter().map(|v| v.map(str::to_string)).collect())
            .collect();
        assert_eq!(from_source, wanted, "{tag}: source");
    }
    assert_eq!(from_twin, from_source, "{tag}: twin");
}

async fn integers(server: &Server) {
    let cols = [
        col("a", "TINYINT", "type=\"int8\""),
        col("b", "TINYINT UNSIGNED", "type=\"uint8\""),
        col("c", "SMALLINT", "type=\"int16\""),
        col("d", "SMALLINT UNSIGNED", "type=\"uint16\""),
        col("e", "MEDIUMINT", "type=\"int32\""),
        col("f", "MEDIUMINT UNSIGNED", "type=\"uint32\""),
        col("g", "INT", "type=\"int32\""),
        col("h", "INT UNSIGNED", "type=\"uint32\""),
        col("i", "BIGINT", "type=\"int64\""),
        col("j", "BIGINT UNSIGNED", "type=\"uint64\""),
        col("k", "TINYINT", "type=\"int64\""),
    ];
    family(
        server,
        "ints",
        &cols,
        &[
            "(1, -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, \
             -9223372036854775808, 18446744073709551615, -1)",
            "(2, 127, 0, 32767, 0, 8388607, 0, 2147483647, 0, 9223372036854775807, 0, 127)",
        ],
        &[
            &[
                Some("-128"),
                Some("255"),
                Some("-32768"),
                Some("65535"),
                Some("-8388608"),
                Some("16777215"),
                Some("-2147483648"),
                Some("4294967295"),
                Some("-9223372036854775808"),
                Some("18446744073709551615"),
                Some("-1"),
            ],
            &[
                Some("127"),
                Some("0"),
                Some("32767"),
                Some("0"),
                Some("8388607"),
                Some("0"),
                Some("2147483647"),
                Some("0"),
                Some("9223372036854775807"),
                Some("0"),
                Some("127"),
            ],
        ],
    )
    .await;
}

async fn decimals_and_floats(server: &Server) {
    let cols = [
        col(
            "wide",
            "DECIMAL(38,10)",
            "type=\"decimal128\" precision=38 scale=10",
        ),
        col(
            "money",
            "DECIMAL(10,2)",
            "type=\"decimal128\" precision=10 scale=2",
        ),
        col("over", "DECIMAL(40,2)", "type=\"utf8\""),
        col("f", "FLOAT", "type=\"float32\""),
        col("d", "DOUBLE", "type=\"float64\""),
    ];
    family(
        server,
        "nums",
        &cols,
        &[
            "(1, 1234567890123456789012345678.1234567890, -99999999.99, \
             12345678901234567890123456789012345678.12, 1.5, -2.25e100)",
            "(2, -0.0000000001, 0.01, 0.00, 0, 0)",
        ],
        &[
            &[
                Some("1234567890123456789012345678.1234567890"),
                Some("-99999999.99"),
                Some("12345678901234567890123456789012345678.12"),
                Some("1.5"),
                Some("-2.25e100"),
            ],
            &[
                Some("-0.0000000001"),
                Some("0.01"),
                Some("0.00"),
                Some("0"),
                Some("0"),
            ],
        ],
    )
    .await;
}

async fn text_and_enums(server: &Server) {
    let cols = [
        col("c", "CHAR(10) CHARACTER SET utf8mb4", "type=\"utf8\""),
        col("v", "VARCHAR(300) CHARACTER SET utf8mb4", "type=\"utf8\""),
        col("t", "TEXT CHARACTER SET utf8mb4", "type=\"utf8\""),
        col("l", "LONGTEXT CHARACTER SET utf8mb4", "type=\"utf8\""),
        col("e", "ENUM('x','y','it''s')", "type=\"utf8\""),
        col("s", "SET('x','y','z')", "type=\"utf8\""),
    ];
    family(
        server,
        "texts",
        &cols,
        &[
            "(1, 'abc', 'h\u{e9}llo \u{1f600} w\u{f6}rld', 'multi\nline', 'big', 'it''s', 'x,z')",
            "(2, '', '', '', '', 'x', '')",
        ],
        &[
            &[
                Some("abc"),
                Some("h\u{e9}llo \u{1f600} w\u{f6}rld"),
                Some("multi\nline"),
                Some("big"),
                Some("it's"),
                Some("x,z"),
            ],
            &[Some(""), Some(""), Some(""), Some(""), Some("x"), Some("")],
        ],
    )
    .await;
}

async fn binary_and_bits(server: &Server) {
    let cols = [
        hexcol("b4", "BINARY(4)", "type=\"binary\""),
        hexcol("vb", "VARBINARY(20)", "type=\"binary\""),
        hexcol("bl", "BLOB", "type=\"binary\""),
        hexcol("bit8", "BIT(8)", "type=\"uint64\""),
        hexcol("bit64", "BIT(64)", "type=\"uint64\""),
    ];
    family(
        server,
        "bins",
        &cols,
        &[
            "(1, 0x01020304, 0x00ff00, 0xdeadbeef, b'10100101', \
             b'1111111111111111111111111111111111111111111111111111111111111111')",
            "(2, 0x0a, '', '', b'0', b'1')",
        ],
        &[
            &[
                Some("01020304"),
                Some("00FF00"),
                Some("DEADBEEF"),
                Some("A5"),
                Some("FFFFFFFFFFFFFFFF"),
            ],
            // BINARY(4) pads with zero bytes.
            &[Some("0A000000"), Some(""), Some(""), Some("0"), Some("1")],
        ],
    )
    .await;
}

async fn json(server: &Server) {
    let cols = [
        col("j", "JSON", "type=\"json\""),
        col("as_text", "JSON", "type=\"utf8\""),
    ];
    // Already canonical (sorted keys, compact), so every mode renders alike.
    let doc = r#"{"a":1,"b":[1,2,{"c":null}],"d":"x"}"#;
    let shown = match server.flavor {
        common::Flavor::Mysql => r#"{"a": 1, "b": [1, 2, {"c": null}], "d": "x"}"#,
        common::Flavor::Mariadb => doc,
    };
    family(
        server,
        "jsons",
        &cols,
        &[&format!("(1, '{doc}', '{doc}')"), "(2, NULL, '[]')"],
        &[&[Some(shown), Some(shown)], &[None, Some("[]")]],
    )
    .await;
}

async fn temporal(server: &Server) {
    let cols = [
        col("d", "DATE", "type=\"date32\""),
        col("t", "TIME(6)", "type=\"time64_micros\""),
        col("dt", "DATETIME(6)", "type=\"timestamp_micros\""),
        col("ts", "TIMESTAMP(6) NULL", "type=\"timestamp_micros_utc\""),
        col("y", "YEAR", "type=\"int16\""),
        col("d_txt", "DATE", "type=\"utf8\""),
        col("t_txt", "TIME(3)", "type=\"utf8\""),
        col("dt_txt", "DATETIME(3)", "type=\"utf8\""),
    ];
    family(
        server,
        "times",
        &cols,
        &[
            "(1, '2024-02-29', '12:34:56.123456', '2023-12-31 23:59:59.999999', \
             '2024-02-29 12:34:56.654321', 2024, '1999-01-02', '-838:59:58.500', \
             '2001-02-03 04:05:06.007')",
            "(2, '1000-01-01', '00:00:00', '1000-01-01 00:00:00', '1970-01-01 00:00:01', \
             1901, '9999-12-31', '838:59:59.000', '9999-12-31 23:59:59.999')",
            "(3, '2020-06-15', NULL, NULL, NULL, 2155, '2000-01-01', '00:00:00.000', \
             '2000-01-01 00:00:00.000')",
        ],
        &[
            &[
                Some("2024-02-29"),
                Some("12:34:56.123456"),
                Some("2023-12-31 23:59:59.999999"),
                Some("2024-02-29 12:34:56.654321"),
                Some("2024"),
                Some("1999-01-02"),
                Some("-838:59:58.500"),
                Some("2001-02-03 04:05:06.007"),
            ],
            &[
                Some("1000-01-01"),
                Some("00:00:00.000000"),
                Some("1000-01-01 00:00:00.000000"),
                Some("1970-01-01 00:00:01.000000"),
                Some("1901"),
                Some("9999-12-31"),
                Some("838:59:59.000"),
                Some("9999-12-31 23:59:59.999"),
            ],
            &[
                Some("2020-06-15"),
                None,
                None,
                None,
                Some("2155"),
                Some("2000-01-01"),
                Some("00:00:00.000"),
                Some("2000-01-01 00:00:00.000"),
            ],
        ],
    )
    .await;
}

async fn geometry(server: &Server) {
    let cols = [
        hexcol("p", "POINT", "type=\"binary\""),
        hexcol("g", "GEOMETRY", "type=\"binary\""),
    ];
    family(
        server,
        "geoms",
        &cols,
        &[
            "(1, ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('LINESTRING(0 0, 1 1, 2 4)'))",
            "(2, ST_GeomFromText('POINT(-3.5 4.25)'), \
             ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'))",
        ],
        // The stored bytes are the server's own layout, so the source's
        // rendering is the reference and the twin must equal it.
        &[],
    )
    .await;
}

common::on_every_server!(
    integers,
    decimals_and_floats,
    text_and_enums,
    binary_and_bits,
    json,
    temporal,
    geometry,
);
