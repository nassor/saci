//! End-to-end demonstration of SACI layered encryption:
//!
//! 1. **Transport mTLS + field-level AEAD (`PeerSink` -> `PeerSource`)**:
//!    mutual TLS over `rustls` (`ring` provider) combined with selective
//!    field-level AEAD (`EncryptingSink` / `DecryptingSource`) that encrypts
//!    sensitive columns before serialization and restores their exact Arrow
//!    types on receipt.
//! 2. **Whole-payload codec encryption at rest (`EncryptedCodec<ParquetCodec>`)**:
//!    chunked AEAD encryption around any SACI byte codec (`SACS` v1 framing).
//!
//! Run with:
//! ```bash
//! cargo run -p saci --example encrypted_pipelines \
//!     --features "crypto,connector-peer,connector-file,codec-parquet"
//! ```

use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use saci::codec::parquet::ParquetCodec;
use saci::connector::NodeIdentity;
use saci::connector::file::{FileSink, FileSinkConfig, FileSource, FileSourceConfig};
use saci::connector::peer::{PeerSink, PeerSinkConfig, PeerSource, PeerSourceConfig};
use saci::crypto::{
    Algorithm, Cipher, DecryptingSource, EncryptedCodec, EncryptingSink, EncryptionMode,
    KeyMaterial, StaticKeyProvider, wire_schema,
};
use saci::io::{Sink, Source};
use saci::tls::{ClientTlsConfig, ServerTlsConfig};

struct MtlsCerts {
    ca_file: std::path::PathBuf,
    srv_cert_file: std::path::PathBuf,
    srv_key_file: std::path::PathBuf,
    cli_cert_file: std::path::PathBuf,
    cli_key_file: std::path::PathBuf,
}

fn write_ephemeral_mtls_certs(
    dir: &std::path::Path,
) -> Result<MtlsCerts, Box<dyn std::error::Error>> {
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String("SACI Example CA".to_string()),
    );
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate()?;
    let ca_cert = ca_params.self_signed(&ca_key)?;
    let issuer = Issuer::new(ca_params, ca_key);

    let mut srv_params =
        CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
    srv_params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String("localhost".to_string()),
    );
    srv_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let srv_key = KeyPair::generate()?;
    let srv_cert = srv_params.signed_by(&srv_key, &issuer)?;

    let mut cli_params = CertificateParams::new(vec!["saci-peer-client".to_string()])?;
    cli_params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String("saci-peer-client".to_string()),
    );
    cli_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let cli_key = KeyPair::generate()?;
    let cli_cert = cli_params.signed_by(&cli_key, &issuer)?;

    let ca_file = dir.join("ca.pem");
    let srv_cert_file = dir.join("server.pem");
    let srv_key_file = dir.join("server.key");
    let cli_cert_file = dir.join("client.pem");
    let cli_key_file = dir.join("client.key");

    std::fs::write(&ca_file, ca_cert.pem())?;
    std::fs::write(&srv_cert_file, srv_cert.pem())?;
    std::fs::write(&srv_key_file, srv_key.serialize_pem())?;
    std::fs::write(&cli_cert_file, cli_cert.pem())?;
    std::fs::write(&cli_key_file, cli_key.serialize_pem())?;

    Ok(MtlsCerts {
        ca_file,
        srv_cert_file,
        srv_key_file,
        cli_cert_file,
        cli_key_file,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let MtlsCerts {
        ca_file,
        srv_cert_file,
        srv_key_file,
        cli_cert_file,
        cli_key_file,
    } = write_ephemeral_mtls_certs(dir.path())?;

    let provider = Arc::new(StaticKeyProvider::single(
        "k1",
        KeyMaterial::from_bytes([0x42; 32]),
    )?);
    let cipher = Arc::new(Cipher::new(provider, Algorithm::Aes256Gcm));

    let plain_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ssn", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
    ]));
    let input = RecordBatch::try_new(
        Arc::clone(&plain_schema),
        vec![
            Arc::new(Int64Array::from(vec![101, 102])),
            Arc::new(StringArray::from(vec!["000-11-2222", "000-33-4444"])),
            Arc::new(Float64Array::from(vec![125_000.50, 142_500.75])),
        ],
    )?;

    // 1. Peer mTLS transport + field-level AEAD on ("ssn", "salary").
    let mode = EncryptionMode::fields(["ssn", "salary"]);
    let wire = wire_schema(&plain_schema, &mode)?;

    let source = PeerSource::new(
        PeerSourceConfig {
            tls: Some(ServerTlsConfig {
                cert_file: srv_cert_file,
                key_file: srv_key_file,
                client_ca_file: Some(ca_file.clone()),
            }),
            ..PeerSourceConfig::new("127.0.0.1:0")
        },
        Arc::clone(&wire),
        NodeIdentity {
            service: "downstream".to_string(),
            workflow: "ingest".to_string(),
            node: "peer_in".to_string(),
        },
    )?;
    let addr = source.local_addr().to_string();
    let mut decrypting_source = DecryptingSource::new(
        source,
        Arc::clone(&plain_schema),
        Arc::clone(&cipher),
        mode.clone(),
    )?;

    let receiver = tokio::spawn(async move { decrypting_source.next_batch().await });

    let peer_sink = PeerSink::new(
        PeerSinkConfig {
            tls: Some(ClientTlsConfig {
                ca_file: Some(ca_file),
                cert_file: Some(cli_cert_file),
                key_file: Some(cli_key_file),
                server_name: Some("localhost".to_string()),
                use_platform_roots: false,
            }),
            ..PeerSinkConfig::new([addr])
        },
        wire,
        NodeIdentity {
            service: "upstream".to_string(),
            workflow: "export".to_string(),
            node: "peer_out".to_string(),
        },
    )?;
    let mut encrypting_sink = EncryptingSink::new(
        peer_sink,
        Arc::clone(&plain_schema),
        Arc::clone(&cipher),
        mode,
    )?;
    encrypting_sink.write_batch(&input).await?;
    encrypting_sink.finish().await?;

    let received = receiver
        .await??
        .expect("decrypted batch received from peer");
    let recv_ssn = received
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let recv_salary = received
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(recv_ssn.value(0), "000-11-2222");
    assert_eq!(recv_salary.value(1), 142_500.75);
    println!(
        "Peer mTLS + field-level AEAD delivered {} rows cleanly.",
        received.num_rows()
    );

    // 2. Whole-payload EncryptedCodec<ParquetCodec> at rest.
    let encrypted_path = dir.path().join("records.parquet.enc");
    let encrypted_codec = Arc::new(EncryptedCodec::new(
        Arc::new(ParquetCodec::new()),
        Arc::clone(&cipher),
    ));

    let mut file_sink = FileSink::new(
        FileSinkConfig {
            truncate: true,
            ..FileSinkConfig::new(&encrypted_path)
        },
        Arc::clone(&plain_schema),
        Arc::clone(&encrypted_codec) as _,
    )?;
    file_sink.write_batch(&received).await?;
    file_sink.finish().await?;

    let raw_bytes = std::fs::read(&encrypted_path)?;
    assert!(raw_bytes.starts_with(b"SACS\x01"));
    assert!(!raw_bytes.starts_with(b"PAR1"));

    let mut file_source = FileSource::new(
        FileSourceConfig::new(&encrypted_path),
        None,
        encrypted_codec,
    )?;
    let at_rest = file_source
        .next_batch()
        .await?
        .expect("decrypted parquet batch");
    assert_eq!(at_rest.num_rows(), 2);
    assert_eq!(at_rest.schema().fields(), plain_schema.fields());
    println!(
        "EncryptedCodec<ParquetCodec> verified {} encrypted bytes and restored {} rows.",
        raw_bytes.len(),
        at_rest.num_rows()
    );

    Ok(())
}
