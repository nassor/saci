//! TLS against a real MariaDB and a real MySQL server that refuse plaintext.
//!
//! The server presents a self-signed certificate for `localhost`. Soft-skips a
//! flavour whose container cannot start; see `common::try_start_tls`.

mod common;

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use common::{Flavor, Server};
use mysql_async::prelude::Queryable;
use saci_connector_mariadb::{MariadbSink, MariadbSinkConfig, MariadbSource, MariadbSourceConfig};
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use serde::Deserialize as _;

/// A self-signed certificate for `localhost`: the PEMs the server loads, and the
/// file the client trusts.
struct Certificate {
    cert_pem: String,
    key_pem: String,
    ca_file: tempfile::NamedTempFile,
}

fn certificate() -> Certificate {
    certificate_for("localhost")
}

/// A certificate that names a host other than the one the tests dial.
fn other_name_certificate() -> Certificate {
    certificate_for("db.elsewhere.example")
}

fn certificate_for(host: &str) -> Certificate {
    let key =
        rcgen::generate_simple_self_signed([host.to_string()]).expect("self-signed certificate");
    let cert_pem = key.cert.pem();
    let key_pem = key.signing_key.serialize_pem();
    let ca_file = tempfile::NamedTempFile::new().expect("temp file");
    std::fs::write(ca_file.path(), &cert_pem).expect("write the CA file");
    Certificate {
        cert_pem,
        key_pem,
        ca_file,
    }
}

async fn tls_server(flavor: Flavor, cert: &Certificate) -> Option<Server> {
    common::try_start_tls(flavor, &cert.cert_pem, &cert.key_pem).await
}

fn connection(host: &str, server: &Server, mode: &str, ca: &Certificate) -> String {
    let dsn = server.dsn_at(host, "root", "saci");
    format!(
        "connection dsn={} ssl_mode=\"{mode}\" ssl_ca={}\n",
        common::quoted(&dsn),
        common::quoted(&ca.ca_file.path().to_string_lossy())
    )
}

/// A `connection` that trusts nothing beyond the built-in roots.
fn connection_without_ca(host: &str, server: &Server, mode: &str) -> String {
    let dsn = server.dsn_at(host, "root", "saci");
    format!(
        "connection dsn={} ssl_mode=\"{mode}\"\n",
        common::quoted(&dsn)
    )
}

fn source(body: &str, connection: &str) -> MariadbSource {
    let text = format!("{body}\n\n{connection}");
    let cfg =
        MariadbSourceConfig::deserialize(saci_connector::from_kdl_str(&text).expect("parse kdl"))
            .expect("parse config");
    MariadbSource::new(cfg).expect("build source")
}

fn sink(body: &str, connection: &str) -> MariadbSink {
    let text = format!("{body}\n\n{connection}");
    let cfg =
        MariadbSinkConfig::deserialize(saci_connector::from_kdl_str(&text).expect("parse kdl"))
            .expect("parse config");
    MariadbSink::new(cfg).expect("build sink")
}

const SOURCE: &str = "name \"t\"\nbatch_rows 10\n\n\
    mode kind=\"polling\" table=\"t\" cursor_column=\"id\"\n\
    schema_fields \"id\" type=\"int64\" nullable=#false\n\
    schema_fields \"label\" type=\"utf8\"\n";

const SINK: &str = "name \"t\"\ntable \"t\"\n\n\
    schema_fields \"id\" type=\"int64\" nullable=#false\n\
    schema_fields \"label\" type=\"utf8\"\n";

macro_rules! on_every_tls_server {
    ($($case:ident => $make:ident),* $(,)?) => {
        mod mariadb {
            $(
                #[tokio::test]
                async fn $case() {
                    let cert = crate::$make();
                    let Some(server) = crate::tls_server(crate::Flavor::Mariadb, &cert).await
                    else {
                        return;
                    };
                    super::$case(&server, &cert).await;
                }
            )*
        }
        mod mysql {
            $(
                #[tokio::test]
                async fn $case() {
                    let cert = crate::$make();
                    let Some(server) = crate::tls_server(crate::Flavor::Mysql, &cert).await
                    else {
                        return;
                    };
                    super::$case(&server, &cert).await;
                }
            )*
        }
    };
}

on_every_tls_server!(
    verify_identity_with_ssl_ca_connects_over_tls => certificate,
    ssl_mode_disabled_is_refused_by_a_server_requiring_tls => certificate,
    verify_identity_against_the_wrong_host_name_fails => certificate,
    verify_ca_against_an_ip_address_host_fails => certificate,
    required_accepts_a_self_signed_certificate_by_ip_address => certificate,
    verify_identity_without_ssl_ca_refuses_a_self_signed_certificate => certificate,
    a_certificate_for_another_name_is_refused_by_both_verifying_modes => other_name_certificate,
    verify_ca_connects_when_the_certificate_names_the_host => certificate,
);

async fn fixture(server: &Server) {
    let mut conn = server.conn().await;
    conn.query_drop("CREATE TABLE t (id BIGINT PRIMARY KEY, label VARCHAR(20))")
        .await
        .expect("create");
    conn.query_drop("INSERT INTO t VALUES (1, 'one')")
        .await
        .expect("seed");
}

async fn verify_identity_with_ssl_ca_connects_over_tls(server: &Server, cert: &Certificate) {
    fixture(server).await;
    let connection = connection("localhost", server, "verify_identity", cert);

    // The server refuses plaintext, so a read that works was encrypted.
    let mut reader = source(SOURCE, &connection);
    let batches = common::drain(&mut reader).await;
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1);

    // Record the cipher the sink's own session negotiated, server side.
    let status_table = match server.flavor {
        Flavor::Mariadb => "information_schema.SESSION_STATUS",
        Flavor::Mysql => "performance_schema.session_status",
    };
    let mut setup = server.conn().await;
    setup
        .query_drop("ALTER TABLE t ADD COLUMN cipher VARCHAR(64)")
        .await
        .expect("cipher column");
    setup
        .query_drop(format!(
            "CREATE TRIGGER t_cipher BEFORE INSERT ON t FOR EACH ROW SET NEW.cipher = \
             (SELECT VARIABLE_VALUE FROM {status_table} WHERE VARIABLE_NAME = 'Ssl_cipher')"
        ))
        .await
        .expect("trigger");

    let mut writer = sink(SINK, &connection);
    let batch = RecordBatch::try_new(
        writer.schema(),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["two"])),
        ],
    )
    .expect("batch");
    writer.write_batch(&batch).await.expect("write over TLS");
    writer.finish().await.expect("finish");

    let mut conn = server.conn().await;
    let label: Option<String> = conn
        .query_first("SELECT label FROM t WHERE id = 2")
        .await
        .expect("read back");
    assert_eq!(label.as_deref(), Some("two"));

    let cipher: Option<Option<String>> = conn
        .query_first("SELECT cipher FROM t WHERE id = 2")
        .await
        .expect("cipher");
    assert!(
        cipher.flatten().is_some_and(|c| !c.is_empty()),
        "the sink's own session negotiated a cipher"
    );

    // The fixture connection is encrypted too, and says so.
    let status: Option<(String, String)> = conn
        .query_first("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        .await
        .expect("status");
    assert!(
        status.is_some_and(|(_, cipher)| !cipher.is_empty()),
        "the server reports a negotiated cipher"
    );
}

async fn ssl_mode_disabled_is_refused_by_a_server_requiring_tls(
    server: &Server,
    cert: &Certificate,
) {
    fixture(server).await;
    let connection = connection("localhost", server, "disabled", cert);
    let mut reader = source(SOURCE, &connection);
    let err = reader.next_batch().await.expect_err("plaintext is refused");
    let message = err.message();
    assert!(message.contains("MariadbSource"), "{message}");
    assert!(
        !message.contains("saci@"),
        "the DSN never reaches an error: {message}"
    );
}

async fn verify_identity_against_the_wrong_host_name_fails(server: &Server, cert: &Certificate) {
    fixture(server).await;
    // The certificate names `localhost`, not 127.0.0.1.
    let connection = connection("127.0.0.1", server, "verify_identity", cert);
    let mut reader = source(SOURCE, &connection);
    let err = reader
        .next_batch()
        .await
        .expect_err("the name does not match");
    assert!(!err.message().is_empty());
}

async fn verify_ca_against_an_ip_address_host_fails(server: &Server, cert: &Certificate) {
    fixture(server).await;
    // `verify_ca` skips the host name check for a DNS name only: the
    // certificate names `localhost`, and an IP address host is still refused.
    let connection = connection("127.0.0.1", server, "verify_ca", cert);
    let mut reader = source(SOURCE, &connection);
    let err = reader
        .next_batch()
        .await
        .expect_err("an IP host is refused");
    assert!(err.message().contains("127.0.0.1"), "{}", err.message());
}

fn one_row(writer: &MariadbSink, id: i64) -> RecordBatch {
    RecordBatch::try_new(
        writer.schema(),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec!["row"])),
        ],
    )
    .expect("batch")
}

async fn required_accepts_a_self_signed_certificate_by_ip_address(
    server: &Server,
    _cert: &Certificate,
) {
    fixture(server).await;
    // No `ssl_ca` and an IP host: nothing about the certificate can verify, and
    // `required` only insists the session is encrypted.
    let connection = connection_without_ca("127.0.0.1", server, "required");
    let mut writer = sink(SINK, &connection);
    writer
        .write_batch(&one_row(&writer, 2))
        .await
        .expect("an encrypted session to an untrusted certificate");
    let mut conn = server.conn().await;
    let rows: Option<i64> = conn
        .query_first("SELECT COUNT(*) FROM t WHERE id = 2")
        .await
        .expect("read back");
    assert_eq!(rows, Some(1));
}

async fn verify_identity_without_ssl_ca_refuses_a_self_signed_certificate(
    server: &Server,
    _cert: &Certificate,
) {
    fixture(server).await;
    // The built-in roots do not trust the server's own certificate, whatever
    // name it carries.
    let connection = connection_without_ca("localhost", server, "verify_identity");
    let mut writer = sink(SINK, &connection);
    let err = writer
        .write_batch(&one_row(&writer, 2))
        .await
        .expect_err("an untrusted certificate");
    assert!(err.message().contains("MariadbSink"), "{}", err.message());
    let mut conn = server.conn().await;
    let rows: Option<i64> = conn
        .query_first("SELECT COUNT(*) FROM t WHERE id = 2")
        .await
        .expect("read back");
    assert_eq!(
        rows,
        Some(0),
        "nothing was written over the refused session"
    );
}

async fn a_certificate_for_another_name_is_refused_by_both_verifying_modes(
    server: &Server,
    cert: &Certificate,
) {
    fixture(server).await;
    // The certificate names `db.elsewhere.example`; the tests dial `localhost`.
    // `verify_ca` is meant to accept this, but `mysql_async` skips the host name
    // check by matching a rustls error text that rustls 0.23 no longer emits, so
    // today both modes refuse it. The docs say so; if a driver update makes this
    // case connect under `verify_ca`, flip its half of this test and the docs.
    for mode in ["verify_identity", "verify_ca"] {
        let connection = connection("localhost", server, mode, cert);
        let mut writer = sink(SINK, &connection);
        let err = writer
            .write_batch(&one_row(&writer, 2))
            .await
            .expect_err("the name does not match");
        assert!(
            err.message().contains("not valid for name"),
            "{mode}: {}",
            err.message()
        );
    }
}

async fn verify_ca_connects_when_the_certificate_names_the_host(
    server: &Server,
    cert: &Certificate,
) {
    fixture(server).await;
    let connection = connection("localhost", server, "verify_ca", cert);
    let mut writer = sink(SINK, &connection);
    writer
        .write_batch(&one_row(&writer, 2))
        .await
        .expect("a trusted chain and a matching name");
    let mut conn = server.conn().await;
    let rows: Option<i64> = conn
        .query_first("SELECT COUNT(*) FROM t WHERE id = 2")
        .await
        .expect("read back");
    assert_eq!(rows, Some(1));
}
