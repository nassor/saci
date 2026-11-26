//! Integration round-trip tests for SACI layered encryption:
//!
//! 1. Transport TLS and mutual TLS (`saci::tls` / `rustls` `ring` provider) across
//!    `peer`, `tcp`, `http-listen`, and `otlp-metrics`.
//! 2. Whole-payload AEAD encryption via `EncryptedCodec` across all five codecs
//!    (`arrow-ipc`, `avro`, `csv`, `ndjson`, `parquet`).
//! 3. Selective field-level and whole-batch payload AEAD (`EncryptingSink` /
//!    `DecryptingSource` / `encrypt_batch` / `decrypt_batch`), AAD tamper checks,
//!    and multi-key rotation (`k1` -> `k2`).
//! 4. End-to-end declarative `saci-service` KDL workflows combining top-level
//!    `encryption`, `codec.encryption`, `sink.encrypt`, `source.decrypt`, and
//!    `peer` mTLS.

#![cfg(all(
    feature = "crypto",
    feature = "tls",
    feature = "service",
    feature = "connector-peer",
    feature = "connector-tcp",
    feature = "connector-http",
    feature = "connector-otlp",
    feature = "connector-file",
    feature = "codec-arrow-ipc",
    feature = "codec-avro",
    feature = "codec-csv",
    feature = "codec-ndjson",
    feature = "codec-parquet",
))]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::{
    Array, BooleanArray, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use saci::codec::arrow_ipc::ArrowIpcCodec;
use saci::codec::avro::AvroCodec;
use saci::codec::csv::CsvCodec;
use saci::codec::ndjson::NdjsonCodec;
use saci::codec::parquet::ParquetCodec;
use saci::codec::{Codec, ReadInput};
use saci::connector::NodeIdentity;
use saci::connector::http::{
    HttpListenConfig, HttpListenSource, HttpSink, HttpSinkConfig, ServerTls, TlsFiles,
};
use saci::connector::otlp::{
    Encoding, OtlpMetricsSink, OtlpMetricsSinkConfig, OtlpMetricsSource, OtlpMetricsSourceConfig,
    PushMode, SourceMode, schema as otlp_schema,
};
use saci::connector::peer::{PeerSink, PeerSinkConfig, PeerSource, PeerSourceConfig};
use saci::connector::tcp::{TcpSink, TcpSinkConfig, TcpSource, TcpSourceConfig};
use saci::crypto::{
    Algorithm, Cipher, EncryptedCodec, EncryptionMode, FieldEncoding, KeyMaterial,
    StaticKeyProvider, decrypt_batch, encrypt_batch, wire_schema,
};
use saci::io::{Sink as _, Source as _};
use saci::service::{RunOptions, ServiceBuilder, ServiceConfig, validate_build_capabilities};
use saci::tls::{ClientTlsConfig, ServerTlsConfig};
use tokio_util::sync::CancellationToken;

struct MtlsFixture {
    _dir: tempfile::TempDir,
    ca_file: PathBuf,
    untrusted_ca_file: PathBuf,
    srv_cert_file: PathBuf,
    srv_key_file: PathBuf,
    cli_cert_file: PathBuf,
    cli_key_file: PathBuf,
    rogue_srv_cert_file: PathBuf,
    rogue_srv_key_file: PathBuf,
    rogue_cli_cert_file: PathBuf,
    rogue_cli_key_file: PathBuf,
}

impl MtlsFixture {
    fn generate() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");

        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
        ca_params.distinguished_name.push(
            rcgen::DnType::CommonName,
            rcgen::DnValue::Utf8String("SACI Test CA".to_string()),
        );
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_key = KeyPair::generate().expect("ca key");
        let ca_cert = ca_params.self_signed(&ca_key).expect("ca cert");
        let issuer = Issuer::new(ca_params, ca_key);

        let mut other_ca_params =
            CertificateParams::new(Vec::<String>::new()).expect("other ca params");
        other_ca_params.distinguished_name.push(
            rcgen::DnType::CommonName,
            rcgen::DnValue::Utf8String("SACI Untrusted CA".to_string()),
        );
        other_ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        other_ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let other_ca_key = KeyPair::generate().expect("other ca key");
        let other_ca_cert = other_ca_params
            .self_signed(&other_ca_key)
            .expect("other ca cert");
        let rogue_issuer = Issuer::new(other_ca_params, other_ca_key);

        let make_srv = |issuer: &Issuer<'_, KeyPair>| {
            let mut params =
                CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
                    .expect("srv params");
            params.distinguished_name.push(
                rcgen::DnType::CommonName,
                rcgen::DnValue::Utf8String("localhost".to_string()),
            );
            params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
            let key = KeyPair::generate().expect("srv key");
            let cert = params.signed_by(&key, issuer).expect("srv cert");
            (cert, key)
        };
        let make_cli = |issuer: &Issuer<'_, KeyPair>| {
            let mut params =
                CertificateParams::new(vec!["saci-client".to_string()]).expect("cli params");
            params.distinguished_name.push(
                rcgen::DnType::CommonName,
                rcgen::DnValue::Utf8String("saci-client".to_string()),
            );
            params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
            let key = KeyPair::generate().expect("cli key");
            let cert = params.signed_by(&key, issuer).expect("cli cert");
            (cert, key)
        };

        let (srv_cert, srv_key) = make_srv(&issuer);
        let (cli_cert, cli_key) = make_cli(&issuer);
        let (rogue_srv_cert, rogue_srv_key) = make_srv(&rogue_issuer);
        let (rogue_cli_cert, rogue_cli_key) = make_cli(&rogue_issuer);

        let ca_file = dir.path().join("ca.pem");
        let untrusted_ca_file = dir.path().join("untrusted_ca.pem");
        let srv_cert_file = dir.path().join("srv.pem");
        let srv_key_file = dir.path().join("srv.key");
        let cli_cert_file = dir.path().join("cli.pem");
        let cli_key_file = dir.path().join("cli.key");
        let rogue_srv_cert_file = dir.path().join("rogue_srv.pem");
        let rogue_srv_key_file = dir.path().join("rogue_srv.key");
        let rogue_cli_cert_file = dir.path().join("rogue_cli.pem");
        let rogue_cli_key_file = dir.path().join("rogue_cli.key");

        std::fs::write(&ca_file, ca_cert.pem()).expect("write ca");
        std::fs::write(&untrusted_ca_file, other_ca_cert.pem()).expect("write untrusted ca");
        std::fs::write(&srv_cert_file, srv_cert.pem()).expect("write srv cert");
        std::fs::write(&srv_key_file, srv_key.serialize_pem()).expect("write srv key");
        std::fs::write(&cli_cert_file, cli_cert.pem()).expect("write cli cert");
        std::fs::write(&cli_key_file, cli_key.serialize_pem()).expect("write cli key");
        std::fs::write(&rogue_srv_cert_file, rogue_srv_cert.pem()).expect("write rogue srv cert");
        std::fs::write(&rogue_srv_key_file, rogue_srv_key.serialize_pem())
            .expect("write rogue srv key");
        std::fs::write(&rogue_cli_cert_file, rogue_cli_cert.pem()).expect("write rogue cli cert");
        std::fs::write(&rogue_cli_key_file, rogue_cli_key.serialize_pem())
            .expect("write rogue cli key");

        Self {
            _dir: dir,
            ca_file,
            untrusted_ca_file,
            srv_cert_file,
            srv_key_file,
            cli_cert_file,
            cli_key_file,
            rogue_srv_cert_file,
            rogue_srv_key_file,
            rogue_cli_cert_file,
            rogue_cli_key_file,
        }
    }

    fn server_tls(&self) -> ServerTlsConfig {
        ServerTlsConfig {
            cert_file: self.srv_cert_file.clone(),
            key_file: self.srv_key_file.clone(),
            client_ca_file: Some(self.ca_file.clone()),
        }
    }

    fn client_tls(&self) -> ClientTlsConfig {
        ClientTlsConfig {
            ca_file: Some(self.ca_file.clone()),
            cert_file: Some(self.cli_cert_file.clone()),
            key_file: Some(self.cli_key_file.clone()),
            server_name: Some("localhost".to_string()),
            use_platform_roots: false,
        }
    }
}

fn test_cipher(id: &str, byte: u8, alg: Algorithm) -> Arc<Cipher> {
    let provider =
        Arc::new(StaticKeyProvider::single(id, KeyMaterial::from_bytes([byte; 32])).expect("key"));
    Arc::new(Cipher::new(provider, alg))
}

fn simple_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("msg", DataType::Utf8, false),
    ]))
}

fn simple_batch() -> RecordBatch {
    RecordBatch::try_new(
        simple_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
        ],
    )
    .expect("simple batch")
}

#[tokio::test]
async fn peer_mtls_delivers_and_rejects_missing_or_untrusted_certs() {
    let tls = MtlsFixture::generate();
    let schema = simple_schema();
    let input = simple_batch();

    let mut source = PeerSource::new(
        PeerSourceConfig {
            tls: Some(tls.server_tls()),
            ..PeerSourceConfig::new("127.0.0.1:0")
        },
        Arc::clone(&schema),
        NodeIdentity {
            service: "svc-b".to_string(),
            workflow: "w2".to_string(),
            node: "in".to_string(),
        },
    )
    .expect("peer source binds");
    let addr = source.local_addr().to_string();

    let reader = tokio::spawn(async move { source.next_batch().await });

    // Missing client certificate must fail mTLS handshake.
    let mut no_cert_sink = PeerSink::new(
        PeerSinkConfig {
            tls: Some(ClientTlsConfig {
                cert_file: None,
                key_file: None,
                ..tls.client_tls()
            }),
            ..PeerSinkConfig::new([addr.clone()])
        },
        Arc::clone(&schema),
        NodeIdentity {
            service: "svc-a".to_string(),
            workflow: "w1".to_string(),
            node: "out".to_string(),
        },
    )
    .expect("no-cert sink config");
    assert!(
        no_cert_sink.write_batch(&input).await.is_err(),
        "server must reject peer client lacking client cert"
    );

    // Untrusted server CA must fail client verification.
    let mut untrusted_sink = PeerSink::new(
        PeerSinkConfig {
            tls: Some(ClientTlsConfig {
                ca_file: Some(tls.untrusted_ca_file.clone()),
                ..tls.client_tls()
            }),
            ..PeerSinkConfig::new([addr.clone()])
        },
        Arc::clone(&schema),
        NodeIdentity {
            service: "svc-a".to_string(),
            workflow: "w1".to_string(),
            node: "out".to_string(),
        },
    )
    .expect("untrusted CA sink config");
    assert!(
        untrusted_sink.write_batch(&input).await.is_err(),
        "client trusting wrong CA must reject server cert"
    );

    // Valid mTLS client succeeds.
    let mut ok_sink = PeerSink::new(
        PeerSinkConfig {
            tls: Some(tls.client_tls()),
            ..PeerSinkConfig::new([addr])
        },
        Arc::clone(&schema),
        NodeIdentity {
            service: "svc-a".to_string(),
            workflow: "w1".to_string(),
            node: "out".to_string(),
        },
    )
    .expect("ok sink");
    ok_sink.write_batch(&input).await.expect("write");
    ok_sink.finish().await.expect("finish");

    let out = reader
        .await
        .expect("join")
        .expect("next_batch ok")
        .expect("batch");
    assert_eq!(out.num_rows(), 3);
    let msgs = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(msgs.value(2), "gamma");
}

#[tokio::test]
async fn tcp_mtls_with_encrypted_arrow_ipc_roundtrip() {
    let tls = MtlsFixture::generate();
    let schema = simple_schema();
    let input = simple_batch();
    let cipher = test_cipher("k1", 0x11, Algorithm::ChaCha20Poly1305);
    let enc_codec: Arc<dyn Codec> =
        Arc::new(EncryptedCodec::new(Arc::new(ArrowIpcCodec::new()), cipher));

    let mut source = TcpSource::new(
        TcpSourceConfig {
            tls: Some(tls.server_tls()),
            ..TcpSourceConfig::new("127.0.0.1:0")
        },
        Arc::clone(&schema),
        Arc::clone(&enc_codec),
    )
    .expect("tcp source binds");
    let addr = source.local_addr().to_string();

    let reader = tokio::spawn(async move { source.next_batch().await });

    let mut sink = TcpSink::new(
        TcpSinkConfig {
            tls: Some(tls.client_tls()),
            ..TcpSinkConfig::new(addr)
        },
        schema,
        enc_codec,
    )
    .expect("tcp sink");
    sink.write_batch(&input).await.expect("tcp write");
    sink.finish().await.expect("tcp finish");

    let out = reader
        .await
        .expect("join")
        .expect("next_batch ok")
        .expect("batch");
    assert_eq!(out.num_rows(), 3);
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 2, 3]
    );
}

#[tokio::test]
async fn http_listen_mtls_accepts_client_cert_and_refuses_anonymous() {
    let tls = MtlsFixture::generate();
    let schema = simple_schema();
    let input = simple_batch();
    let cipher = test_cipher("k1", 0x22, Algorithm::Aes256Gcm);
    let codec: Arc<dyn Codec> = Arc::new(EncryptedCodec::new(Arc::new(CsvCodec::new()), cipher));

    let mut source = HttpListenSource::new(
        HttpListenConfig {
            tls: Some(ServerTls {
                cert_file: tls.srv_cert_file.clone(),
                key_file: tls.srv_key_file.clone(),
                client_ca_file: Some(tls.ca_file.clone()),
            }),
            ..HttpListenConfig::new("127.0.0.1:0")
        },
        Arc::clone(&schema),
        Arc::clone(&codec),
    )
    .expect("http listen source");
    let addr = source.local_addr();
    let reader = tokio::spawn(async move { source.next_batch().await });

    let mut anon_cfg = HttpSinkConfig::new(format!("https://{addr}/"));
    anon_cfg.client.tls = TlsFiles {
        ca_file: Some(tls.ca_file.clone()),
        cert_file: None,
        key_file: None,
    };
    anon_cfg.client.timeout = Duration::from_secs(5);
    let mut anon_sink = HttpSink::new(anon_cfg, Arc::clone(&schema), Arc::clone(&codec)).unwrap();
    assert!(
        anon_sink.write_batch(&input).await.is_err(),
        "client without cert must be refused under client_ca_file"
    );

    let mut mtls_cfg = HttpSinkConfig::new(format!("https://{addr}/"));
    mtls_cfg.client.tls = TlsFiles {
        ca_file: Some(tls.ca_file.clone()),
        cert_file: Some(tls.cli_cert_file.clone()),
        key_file: Some(tls.cli_key_file.clone()),
    };
    mtls_cfg.client.timeout = Duration::from_secs(5);
    let mut mtls_sink = HttpSink::new(mtls_cfg, schema, codec).unwrap();
    mtls_sink.write_batch(&input).await.expect("mtls post");
    mtls_sink.finish().await.expect("finish");

    let out = reader
        .await
        .expect("join")
        .expect("next_batch ok")
        .expect("batch");
    assert_eq!(out.num_rows(), 3);
}

#[tokio::test]
async fn otlp_push_and_sink_mtls_roundtrip() {
    let tls = MtlsFixture::generate();

    let mut source = OtlpMetricsSource::new(
        OtlpMetricsSourceConfig {
            mode: Some(SourceMode::Push(PushMode {
                bind: "127.0.0.1:0".to_string(),
                path: "/v1/metrics".to_string(),
                auth: None,
                buffer: 16,
                max_body_bytes: 1024 * 1024,
                tls: Some(tls.server_tls()),
            })),
            ..OtlpMetricsSourceConfig::default()
        },
        None,
    )
    .expect("otlp push binds");
    let port = source.local_addr().expect("push local_addr").port();
    let reader = tokio::spawn(async move { source.next_batch().await });

    let batch = RecordBatch::try_new(
        otlp_schema::metrics_schema(),
        vec![
            Arc::new(StringArray::from(vec!["requests_total"])),
            Arc::new(Float64Array::from(vec![42.0])),
            Arc::new(UInt64Array::from(vec![1_700_000_000_000_000_000])),
            Arc::new(StringArray::from(vec!["gauge"])),
            Arc::new(StringArray::from(vec![Some("1")])),
            Arc::new(StringArray::from(vec![Some("req count")])),
            Arc::new(StringArray::from(vec![Some("{}")])),
            Arc::new(StringArray::from(vec![Some(r#"{"service.name":"saci"}"#)])),
            Arc::new(UInt64Array::from(vec![None])),
            Arc::new(UInt64Array::from(vec![None])),
        ],
    )
    .expect("otlp batch");

    let mut sink = OtlpMetricsSink::new(
        OtlpMetricsSinkConfig {
            url: format!("https://127.0.0.1:{port}/v1/metrics"),
            encoding: Encoding::Protobuf,
            headers: HashMap::new(),
            timeout_ms: 5_000,
            service_name: Some("saci".to_string()),
            metric_name: None,
            batch_rows: 100,
            tls: Some(tls.client_tls()),
        },
        None,
    )
    .expect("otlp sink");
    sink.write_batch(&batch).await.expect("otlp write");
    sink.finish().await.expect("otlp finish");

    let recv = reader
        .await
        .expect("join")
        .expect("next_batch ok")
        .expect("batch");
    assert_eq!(recv.num_rows(), 1);
    assert_eq!(
        recv.column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "requests_total"
    );
}

#[test]
fn encrypted_codec_covers_all_five_formats_and_detects_tamper() {
    let dir = tempfile::tempdir().expect("tempdir");
    let schema = simple_schema();
    let input = simple_batch();
    let cipher = test_cipher("k1", 0x33, Algorithm::Aes256Gcm);
    let wrong_cipher = test_cipher("k1", 0x99, Algorithm::Aes256Gcm);

    let codecs: Vec<Arc<dyn Codec>> = vec![
        Arc::new(ArrowIpcCodec::new()),
        Arc::new(AvroCodec::new()),
        Arc::new(CsvCodec::new()),
        Arc::new(NdjsonCodec::new()),
        Arc::new(ParquetCodec::new()),
    ];

    for inner in codecs {
        let format_name = inner.format();
        let enc = EncryptedCodec::new(Arc::clone(&inner), Arc::clone(&cipher));
        let wrong = EncryptedCodec::new(inner, Arc::clone(&wrong_cipher));

        // 1. Stream round-trip over ReadInput::Memory and ReadInput::File.
        let file_path = dir.path().join(format!("{format_name}.enc"));
        let file = std::fs::File::create(&file_path).expect("create");
        let mut writer = enc
            .open_writer(Box::new(file), Arc::clone(&schema))
            .expect("open_writer");
        writer.write_batch(&input).expect("write_batch");
        writer.finish().expect("finish");

        let mut bytes = std::fs::read(&file_path).expect("read");
        assert!(
            bytes.starts_with(b"SACS\x01"),
            "{format_name}: expected SACS v1 stream magic"
        );

        for read_in in [
            ReadInput::from(bytes.clone()),
            ReadInput::from(std::fs::File::open(&file_path).expect("reopen")),
        ] {
            let mut reader = enc
                .open_reader(read_in, Some(Arc::clone(&schema)))
                .expect("open_reader");
            let mut total_rows = 0usize;
            while let Some(b) = reader.next_batch().expect("next_batch") {
                total_rows += b.num_rows();
            }
            assert_eq!(total_rows, 3, "{format_name} row count mismatch");
        }

        // Wrong key must fail decryption.
        assert!(
            wrong
                .open_reader(ReadInput::from(bytes.clone()), Some(Arc::clone(&schema)))
                .and_then(|mut r| r.next_batch())
                .is_err(),
            "{format_name}: wrong key must fail"
        );

        // Tampered ciphertext byte must fail AEAD authentication.
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(
            enc.open_reader(ReadInput::from(bytes), Some(Arc::clone(&schema)))
                .and_then(|mut r| r.next_batch())
                .is_err(),
            "{format_name}: tampered ciphertext must fail"
        );

        // 2. Message round-trip (`SACI` v1 envelope framing).
        let encoded = enc.encode_messages(&input).expect("encode_messages");
        assert!(!encoded.is_empty());
        assert!(encoded[0].starts_with(b"SACI\x01"));

        let mut decoder = enc
            .open_message_decoder(Arc::clone(&schema))
            .expect("open_message_decoder");
        for frame in &encoded {
            decoder.push(frame).expect("push");
        }
        let msg_batch = decoder
            .flush()
            .expect("flush")
            .expect("decoded message batch");
        assert_eq!(msg_batch.num_rows(), 3, "{format_name} message row count");
    }
}

#[test]
fn field_level_all_types_payload_aad_tamper_and_key_rotation() {
    let mut list_builder = ListBuilder::new(Int32Builder::new());
    list_builder.values().append_value(10);
    list_builder.values().append_value(20);
    list_builder.append(true);
    list_builder.append_null();

    let rich_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("flag", DataType::Boolean, true),
        Field::new("score", DataType::Float64, true),
        Field::new("note", DataType::Utf8, true),
        Field::new("blob", DataType::Binary, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("amount", DataType::Decimal128(10, 2), true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
    ]));
    let rich_batch = RecordBatch::try_new(
        Arc::clone(&rich_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(Float64Array::from(vec![Some(3.5), Some(9.25)])),
            Arc::new(StringArray::from(vec![Some("top-secret"), None])),
            Arc::new(arrow_array::BinaryArray::from_opt_vec(vec![
                Some(b"raw-bytes"),
                Some(b""),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_700_000_000_000_000),
                None,
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(12345), Some(-9900)])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(list_builder.finish()),
        ],
    )
    .expect("rich batch");

    let k1 = KeyMaterial::from_bytes([0xAA; 32]);
    let k2 = KeyMaterial::from_bytes([0xBB; 32]);
    let cipher_k1 = Arc::new(Cipher::new(
        Arc::new(StaticKeyProvider::single("k1", k1.clone()).unwrap()),
        Algorithm::Aes256Gcm,
    ));

    // Exercise both Binary and Base64Utf8 across diverse Arrow types + Payload mode.
    for enc in [FieldEncoding::Binary, FieldEncoding::Base64Utf8] {
        let mode = EncryptionMode::fields_with_encoding(
            ["flag", "score", "note", "blob", "ts", "amount", "tags"],
            enc,
        );
        let wire = wire_schema(&rich_schema, &mode).unwrap();
        let encrypted = encrypt_batch(&rich_batch, &wire, &cipher_k1, &mode).unwrap();
        let decrypted = decrypt_batch(&encrypted, &rich_schema, &cipher_k1, &mode).unwrap();
        assert_eq!(decrypted, rich_batch);

        let payload_mode = EncryptionMode::payload_with_encoding(enc);
        let p_wire = wire_schema(&rich_schema, &payload_mode).unwrap();
        let p_enc = encrypt_batch(&rich_batch, &p_wire, &cipher_k1, &payload_mode).unwrap();
        let p_dec = decrypt_batch(&p_enc, &rich_schema, &cipher_k1, &payload_mode).unwrap();
        assert_eq!(p_dec, rich_batch);
    }

    // AAD tamper check: swapping encrypted columns between fields ("a" <-> "b") must fail.
    let swap_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let swap_batch = RecordBatch::try_new(
        Arc::clone(&swap_schema),
        vec![
            Arc::new(StringArray::from(vec!["left"])),
            Arc::new(StringArray::from(vec!["right"])),
        ],
    )
    .unwrap();
    let swap_mode = EncryptionMode::fields(["a", "b"]);
    let swap_wire = wire_schema(&swap_schema, &swap_mode).unwrap();
    let enc_swap = encrypt_batch(&swap_batch, &swap_wire, &cipher_k1, &swap_mode).unwrap();
    let tampered = RecordBatch::try_new(
        Arc::clone(&swap_wire),
        vec![enc_swap.column(1).clone(), enc_swap.column(0).clone()],
    )
    .unwrap();
    assert!(
        decrypt_batch(&tampered, &swap_schema, &cipher_k1, &swap_mode).is_err(),
        "swapping columns must fail field-name AAD verification"
    );

    // Key rotation: k1 seals batch 1; k2 seals batch 2; keyring {k1, k2} decrypts both.
    let mut ring = HashMap::new();
    ring.insert("k1".to_string(), k1);
    ring.insert("k2".to_string(), k2);
    let cipher_k2 = Arc::new(Cipher::new(
        Arc::new(StaticKeyProvider::new("k2", ring).unwrap()),
        Algorithm::Aes256Gcm,
    ));
    let mode = EncryptionMode::fields(["note"]);
    let wire = wire_schema(&rich_schema, &mode).unwrap();
    let batch1 = encrypt_batch(&rich_batch, &wire, &cipher_k1, &mode).unwrap();
    let batch2 = encrypt_batch(&rich_batch, &wire, &cipher_k2, &mode).unwrap();
    assert_eq!(
        decrypt_batch(&batch1, &rich_schema, &cipher_k2, &mode).unwrap(),
        rich_batch
    );
    assert_eq!(
        decrypt_batch(&batch2, &rich_schema, &cipher_k2, &mode).unwrap(),
        rich_batch
    );
}

#[tokio::test]
async fn declarative_kdl_workflows_encrypt_fields_and_codec_and_decrypt_end_to_end() {
    let dir = tempfile::tempdir().expect("tempdir");
    let plain_csv = dir.path().join("input.csv");
    std::fs::write(
        &plain_csv,
        "id,secret,amount\n1,alice-ssn,99.5\n2,bob-ssn,42.0\n",
    )
    .unwrap();

    let encrypted_parquet = dir.path().join("wire.parquet.enc");
    let output_csv = dir.path().join("output.csv");

    let kdl_path = |p: &std::path::Path| p.display().to_string().replace('\\', "/");

    let encryption_block = r#"
encryption algorithm="chacha20-poly1305" active_key="k2" {
    key "k1" hex="000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
    key "k2" hex="f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff00112233445566778899aabbccddeeff"
}
"#;

    let producer_kdl = format!(
        r#"
mode "standalone"
node id=1 data_dir="{data_dir}"
run "once"
{encryption_block}
workflow "producer" {{
    codec "csv_plain" format="csv"
    codec "parquet_enc" format="parquet" {{
        encryption enabled=#true active_key="k1"
    }}
    schema "Order" {{
        field "id" type="int64" nullable=#false
        field "secret" type="utf8" nullable=#false
        field "amount" type="float64" nullable=#false
    }}
    source "src_csv" connector="file" codec="csv_plain" component="Order" {{
        config path="{plain_csv}"
    }}
    sink "sink_enc" connector="file" codec="parquet_enc" component="Order" {{
        encrypt fields="secret,amount" encoding="base64" active_key="k2"
        config path="{encrypted_parquet}" truncate=#true
    }}
    link from="src_csv" to="sink_enc"
}}
"#,
        data_dir = kdl_path(dir.path()),
        plain_csv = kdl_path(&plain_csv),
        encrypted_parquet = kdl_path(&encrypted_parquet),
    );

    let consumer_kdl = format!(
        r#"
mode "standalone"
node id=2 data_dir="{data_dir}"
run "once"
{encryption_block}
workflow "consumer" {{
    codec "parquet_enc" format="parquet" {{
        encryption enabled=#true
    }}
    codec "csv_out" format="csv"
    schema "Order" {{
        field "id" type="int64" nullable=#false
        field "secret" type="utf8" nullable=#false
        field "amount" type="float64" nullable=#false
    }}
    source "src_enc" connector="file" codec="parquet_enc" component="Order" {{
        decrypt fields="secret,amount" encoding="base64"
        config path="{encrypted_parquet}"
    }}
    sink "sink_plain" connector="file" codec="csv_out" component="Order" {{
        config path="{output_csv}" truncate=#true
    }}
    link from="src_enc" to="sink_plain"
}}
"#,
        data_dir = kdl_path(dir.path()),
        encrypted_parquet = kdl_path(&encrypted_parquet),
        output_csv = kdl_path(&output_csv),
    );

    for kdl in [&producer_kdl, &consumer_kdl] {
        let cfg = ServiceConfig::from_kdl(kdl).expect("valid KDL");
        validate_build_capabilities(&cfg).expect("capabilities");
        for wf in ServiceBuilder::new().build(&cfg).expect("build") {
            wf.run(RunOptions::new(CancellationToken::new()))
                .await
                .expect("workflow run");
        }
    }

    let raw_enc = std::fs::read(&encrypted_parquet).expect("read encrypted parquet");
    assert!(raw_enc.starts_with(b"SACS\x01"));

    let restored = std::fs::read_to_string(&output_csv).expect("read restored csv");
    assert!(restored.contains("1,alice-ssn,99.5"), "got:\n{restored}");
    assert!(restored.contains("2,bob-ssn,42"), "got:\n{restored}");
}

#[tokio::test]
async fn peer_and_tcp_negative_tls_cases_reject_bad_identities_and_plaintext() {
    let tls = MtlsFixture::generate();
    let schema = simple_schema();
    let input = simple_batch();

    let mut peer_srv = PeerSource::new(
        PeerSourceConfig {
            tls: Some(tls.server_tls()),
            ..PeerSourceConfig::new("127.0.0.1:0")
        },
        Arc::clone(&schema),
        NodeIdentity {
            service: "svc-b".to_string(),
            workflow: "w2".to_string(),
            node: "in".to_string(),
        },
    )
    .expect("peer binds");
    let peer_addr = peer_srv.local_addr().to_string();
    let peer_reader = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(600), peer_srv.next_batch()).await
    });

    // 1. Rogue client certificate is refused by PeerSource.
    let mut rogue_cli_sink = PeerSink::new(
        PeerSinkConfig {
            tls: Some(ClientTlsConfig {
                cert_file: Some(tls.rogue_cli_cert_file.clone()),
                key_file: Some(tls.rogue_cli_key_file.clone()),
                ..tls.client_tls()
            }),
            ..PeerSinkConfig::new([peer_addr.clone()])
        },
        Arc::clone(&schema),
        NodeIdentity {
            service: "svc-a".to_string(),
            workflow: "w1".to_string(),
            node: "out".to_string(),
        },
    )
    .expect("rogue client sink");
    assert!(rogue_cli_sink.write_batch(&input).await.is_err());

    // 2. Wrong server_name is refused by PeerSink.
    let mut wrong_san_sink = PeerSink::new(
        PeerSinkConfig {
            tls: Some(ClientTlsConfig {
                server_name: Some("wrong.example".to_string()),
                ..tls.client_tls()
            }),
            ..PeerSinkConfig::new([peer_addr.clone()])
        },
        Arc::clone(&schema),
        NodeIdentity {
            service: "svc-a".to_string(),
            workflow: "w1".to_string(),
            node: "out".to_string(),
        },
    )
    .expect("wrong SAN sink");
    assert!(wrong_san_sink.write_batch(&input).await.is_err());

    // 3. Plaintext PeerSink cannot talk to TLS PeerSource.
    let mut plain_sink = PeerSink::new(
        PeerSinkConfig::new([peer_addr]),
        Arc::clone(&schema),
        NodeIdentity {
            service: "svc-a".to_string(),
            workflow: "w1".to_string(),
            node: "out".to_string(),
        },
    )
    .expect("plain peer sink");
    assert!(plain_sink.write_batch(&input).await.is_err());
    assert!(
        peer_reader.await.expect("join").is_err(),
        "no rejected session may deliver a batch"
    );

    // 3b. Server cert signed by rogue CA is refused by PeerSink.
    let mut rogue_srv = PeerSource::new(
        PeerSourceConfig {
            tls: Some(ServerTlsConfig {
                cert_file: tls.rogue_srv_cert_file.clone(),
                key_file: tls.rogue_srv_key_file.clone(),
                client_ca_file: None,
            }),
            ..PeerSourceConfig::new("127.0.0.1:0")
        },
        Arc::clone(&schema),
        NodeIdentity {
            service: "svc-r".to_string(),
            workflow: "w".to_string(),
            node: "in".to_string(),
        },
    )
    .expect("rogue peer binds");
    let rogue_addr = rogue_srv.local_addr().to_string();
    let rogue_reader = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(500), rogue_srv.next_batch()).await
    });
    let mut rogue_chk_sink = PeerSink::new(
        PeerSinkConfig {
            tls: Some(tls.client_tls()),
            ..PeerSinkConfig::new([rogue_addr])
        },
        Arc::clone(&schema),
        NodeIdentity {
            service: "svc-a".to_string(),
            workflow: "w1".to_string(),
            node: "out".to_string(),
        },
    )
    .expect("sink");
    assert!(rogue_chk_sink.write_batch(&input).await.is_err());
    assert!(rogue_reader.await.expect("join").is_err());

    // 4. Plaintext PeerSource cannot accept a TLS PeerSink.
    let mut plain_peer_srv = PeerSource::new(
        PeerSourceConfig::new("127.0.0.1:0"),
        Arc::clone(&schema),
        NodeIdentity {
            service: "svc-p".to_string(),
            workflow: "w".to_string(),
            node: "in".to_string(),
        },
    )
    .expect("plain peer binds");
    let plain_addr = plain_peer_srv.local_addr().to_string();
    let plain_reader = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(500), plain_peer_srv.next_batch()).await
    });
    let mut tls_sink = PeerSink::new(
        PeerSinkConfig {
            tls: Some(tls.client_tls()),
            ..PeerSinkConfig::new([plain_addr])
        },
        Arc::clone(&schema),
        NodeIdentity {
            service: "svc-t".to_string(),
            workflow: "w".to_string(),
            node: "out".to_string(),
        },
    )
    .expect("tls peer sink");
    assert!(tls_sink.write_batch(&input).await.is_err());
    assert!(plain_reader.await.expect("join").is_err());

    // 5. TcpSink with TLS trusting wrong CA fails handshake; TLS TcpSource rejects anonymous TCP client.
    let codec: Arc<dyn Codec> = Arc::new(ArrowIpcCodec::new());
    let mut tcp_srv = TcpSource::new(
        TcpSourceConfig {
            tls: Some(tls.server_tls()),
            ..TcpSourceConfig::new("127.0.0.1:0")
        },
        Arc::clone(&schema),
        Arc::clone(&codec),
    )
    .expect("tcp srv");
    let tcp_addr = tcp_srv.local_addr().to_string();
    let tcp_reader = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(600), tcp_srv.next_batch()).await
    });

    let mut untrusted_tcp_sink = TcpSink::new(
        TcpSinkConfig {
            tls: Some(ClientTlsConfig {
                ca_file: Some(tls.untrusted_ca_file.clone()),
                ..tls.client_tls()
            }),
            ..TcpSinkConfig::new(tcp_addr.clone())
        },
        Arc::clone(&schema),
        Arc::clone(&codec),
    )
    .expect("untrusted tcp sink");
    assert!(untrusted_tcp_sink.write_batch(&input).await.is_err());

    let mut anon_tcp_sink = TcpSink::new(
        TcpSinkConfig {
            tls: Some(ClientTlsConfig {
                cert_file: None,
                key_file: None,
                ..tls.client_tls()
            }),
            ..TcpSinkConfig::new(tcp_addr)
        },
        Arc::clone(&schema),
        Arc::clone(&codec),
    )
    .expect("anon tcp sink");
    let _ = anon_tcp_sink.write_batch(&input).await;
    let _ = anon_tcp_sink.finish().await;
    assert!(
        tcp_reader.await.expect("join").is_err(),
        "TcpSource must reject anonymous client when client_ca_file is configured"
    );

    // 6. TLS TcpSink against plaintext TcpListener refuses handshake.
    let plain_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let plain_tcp_addr = plain_listener.local_addr().expect("addr").to_string();
    let accept_task = tokio::spawn(async move {
        if let Ok((mut s, _)) = plain_listener.accept().await {
            use tokio::io::AsyncWriteExt as _;
            let _ = s.write_all(b"HTTP/1.1 200 OK\r\n\r\n").await;
        }
    });
    let mut tls_to_plain_tcp = TcpSink::new(
        TcpSinkConfig {
            tls: Some(tls.client_tls()),
            ..TcpSinkConfig::new(plain_tcp_addr)
        },
        schema,
        codec,
    )
    .expect("tls tcp sink");
    assert!(tls_to_plain_tcp.write_batch(&input).await.is_err());
    accept_task.abort();
}

#[tokio::test]
async fn http_listen_kdl_enforces_client_ca_and_rejects_ca_file() {
    let tls = MtlsFixture::generate();
    let dir = tempfile::tempdir().expect("tempdir");
    let kdl_path = |p: &std::path::Path| p.display().to_string().replace('\\', "/");

    let bad_kdl = format!(
        r#"
mode "standalone"
node id=1 data_dir="{data_dir}"
run "stream"
workflow "bad_http_listen" {{
    codec "ipc" format="arrow-ipc"
    source "in" connector="http-listen" codec="ipc" component="Order" {{
        config bind="127.0.0.1:0" {{
            tls {{
                cert_file "{srv_cert}"
                key_file "{srv_key}"
                ca_file "{ca}"
            }}
            field "id" type="int64" nullable=#false
        }}
    }}
    wasm "p" name="native-pipeline"
    sink "out" connector="file" codec="ipc" component="Order" {{
        config path="{out}" truncate=#true {{
            field "id" type="int64" nullable=#false
        }}
    }}
    link from="in" to="p"
    link from="p" to="out"
}}
"#,
        data_dir = kdl_path(dir.path()),
        srv_cert = kdl_path(&tls.srv_cert_file),
        srv_key = kdl_path(&tls.srv_key_file),
        ca = kdl_path(&tls.ca_file),
        out = kdl_path(&dir.path().join("bad.ipc")),
    );
    let bad_cfg = ServiceConfig::from_kdl(&bad_kdl).expect("parses");
    let err = ServiceBuilder::new()
        .runtime("p", saci::Pipeline::new("p"))
        .build(&bad_cfg)
        .expect_err("ca_file on http-listen must be rejected");
    assert!(err.to_string().contains("ca_file"), "got: {err}");

    let probe_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = probe_listener.local_addr().expect("addr").port();
    drop(probe_listener);

    let out_path = dir.path().join("out.ipc");
    let good_kdl = format!(
        r#"
mode "standalone"
node id=1 data_dir="{data_dir}"
run "stream"
encryption active_key="k1" {{
    key "k1" hex="000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
}}
workflow "good_http_listen" {{
    codec "ipc" format="arrow-ipc" {{
        encryption enabled=#true
    }}
    source "in" connector="http-listen" codec="ipc" component="Order" {{
        config bind="127.0.0.1:{port}" path="/ingest" {{
            tls {{
                cert_file "{srv_cert}"
                key_file "{srv_key}"
                client_ca_file "{ca}"
            }}
            field "id" type="int64" nullable=#false
            field "msg" type="utf8" nullable=#false
        }}
    }}
    sink "out" connector="file" codec="ipc" component="Order" {{
        config path="{out}" truncate=#true {{
            field "id" type="int64" nullable=#false
            field "msg" type="utf8" nullable=#false
        }}
    }}
    link from="in" to="out"
}}
http enabled=#false
"#,
        data_dir = kdl_path(dir.path()),
        srv_cert = kdl_path(&tls.srv_cert_file),
        srv_key = kdl_path(&tls.srv_key_file),
        ca = kdl_path(&tls.ca_file),
        out = kdl_path(&out_path),
    );
    let good_cfg = ServiceConfig::from_kdl(&good_kdl).expect("valid KDL");
    let mut workflows = ServiceBuilder::new().build(&good_cfg).expect("builds");
    let wf = workflows.remove(0);

    let cancel = CancellationToken::new();
    let wf_cancel = cancel.clone();
    let live = Arc::new(tokio::sync::RwLock::new(
        saci::service::WorkflowStats::default(),
    ));

    // Exact key bytes 0x00..=0x1f match the hex key in good_kdl.
    let key_bytes: [u8; 32] = std::array::from_fn(|i| i as u8);
    let key_provider =
        Arc::new(StaticKeyProvider::single("k1", KeyMaterial::from_bytes(key_bytes)).unwrap());
    let cipher = Arc::new(Cipher::new(key_provider, Algorithm::Aes256Gcm));
    let enc_codec: Arc<dyn Codec> =
        Arc::new(EncryptedCodec::new(Arc::new(ArrowIpcCodec::new()), cipher));

    let driver = async {
        let mut sink_cfg = HttpSinkConfig::new(format!("https://127.0.0.1:{port}/ingest"));
        sink_cfg.client.tls = TlsFiles {
            ca_file: Some(tls.ca_file.clone()),
            cert_file: Some(tls.cli_cert_file.clone()),
            key_file: Some(tls.cli_key_file.clone()),
        };
        let mut sink = HttpSink::new(sink_cfg, simple_schema(), Arc::clone(&enc_codec)).unwrap();
        sink.write_batch(&simple_batch()).await.expect("mtls post");
        sink.finish().await.expect("finish");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            if live.read().await.sink_batches_written >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cancel.cancel();
    };
    let (wf_res, ()) = tokio::join!(
        wf.run(RunOptions::new(wf_cancel).live_stats(Arc::clone(&live))),
        driver
    );
    let stats = wf_res.expect("stream exits cleanly");
    assert_eq!(stats.rows_processed, 3);
    assert_eq!(stats.sink_batches_written, 1);

    let file = std::fs::File::open(&out_path).expect("open out");
    let mut reader = enc_codec
        .open_reader(ReadInput::from(file), Some(simple_schema()))
        .expect("decrypt out");
    let read_batch = reader.next_batch().expect("batch").expect("some");
    assert_eq!(read_batch.num_rows(), 3);
}

#[tokio::test]
async fn otlp_push_rejects_unauthenticated_client_when_client_ca_set() {
    let tls = MtlsFixture::generate();

    let mut source = OtlpMetricsSource::new(
        OtlpMetricsSourceConfig {
            mode: Some(SourceMode::Push(PushMode {
                bind: "127.0.0.1:0".to_string(),
                path: "/v1/metrics".to_string(),
                auth: None,
                buffer: 16,
                max_body_bytes: 1024 * 1024,
                tls: Some(tls.server_tls()),
            })),
            ..OtlpMetricsSourceConfig::default()
        },
        None,
    )
    .expect("otlp push binds");
    let port = source.local_addr().expect("push local_addr").port();
    let reader = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(600), source.next_batch()).await
    });

    let batch = RecordBatch::try_new(
        otlp_schema::metrics_schema(),
        vec![
            Arc::new(StringArray::from(vec!["requests_total"])),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(UInt64Array::from(vec![1_700_000_000_000_000_000])),
            Arc::new(StringArray::from(vec!["gauge"])),
            Arc::new(StringArray::from(vec![Some("1")])),
            Arc::new(StringArray::from(vec![Some("req count")])),
            Arc::new(StringArray::from(vec![Some("{}")])),
            Arc::new(StringArray::from(vec![Some("{}")])),
            Arc::new(UInt64Array::from(vec![None])),
            Arc::new(UInt64Array::from(vec![None])),
        ],
    )
    .expect("otlp batch");

    let mut anon_sink = OtlpMetricsSink::new(
        OtlpMetricsSinkConfig {
            url: format!("https://127.0.0.1:{port}/v1/metrics"),
            encoding: Encoding::Protobuf,
            headers: HashMap::new(),
            timeout_ms: 2_000,
            service_name: Some("saci".to_string()),
            metric_name: None,
            batch_rows: 100,
            tls: Some(ClientTlsConfig {
                cert_file: None,
                key_file: None,
                ..tls.client_tls()
            }),
        },
        None,
    )
    .expect("otlp sink");
    assert!(
        anon_sink.write_batch(&batch).await.is_err(),
        "otlp push with client_ca_file must refuse client without cert"
    );
    assert!(reader.await.expect("join").is_err());
}

#[test]
fn driver_s3_and_postgres_tls_encryption_config_hardening() {
    #[cfg(feature = "connector-s3")]
    {
        use saci::connector::s3::ConnectionConfig as S3Conn;
        use saci::connector::s3::config::S3EncryptionConfig;

        let dir = tempfile::tempdir().expect("tempdir");
        let raw_key_path = dir.path().join("s3_raw.key");
        std::fs::write(&raw_key_path, [0x42u8; 32]).unwrap();
        let b64_key_path = dir.path().join("s3_b64.key");
        let b64 = "QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI=";
        std::fs::write(&b64_key_path, format!("{b64}\n")).unwrap();
        let bad_len_path = dir.path().join("s3_bad.key");
        std::fs::write(&bad_len_path, [0x11u8; 31]).unwrap();

        let base = S3Conn {
            bucket: "b".to_string(),
            endpoint: Some("https://s3.example.com".to_string()),
            access_key_id: Some("ak".to_string()),
            secret_access_key: Some("sk".to_string()),
            ..Default::default()
        };

        let mut raw_cfg = base.clone();
        raw_cfg.encryption = Some(Box::new(S3EncryptionConfig::SseC {
            customer_key_base64: None,
            customer_key_file: Some(raw_key_path.to_string_lossy().into_owned()),
        }));
        assert!(raw_cfg.build_store("S3Sink").is_ok());

        let mut b64_cfg = base.clone();
        b64_cfg.encryption = Some(Box::new(S3EncryptionConfig::SseC {
            customer_key_base64: None,
            customer_key_file: Some(b64_key_path.to_string_lossy().into_owned()),
        }));
        assert!(b64_cfg.build_store("S3Sink").is_ok());

        let mut bad_len_cfg = base.clone();
        bad_len_cfg.encryption = Some(Box::new(S3EncryptionConfig::SseC {
            customer_key_base64: None,
            customer_key_file: Some(bad_len_path.to_string_lossy().into_owned()),
        }));
        assert!(bad_len_cfg.build_store("S3Sink").is_err());

        let mut both_cfg = base.clone();
        both_cfg.encryption = Some(Box::new(S3EncryptionConfig::SseC {
            customer_key_base64: Some(b64.to_string()),
            customer_key_file: Some(b64_key_path.to_string_lossy().into_owned()),
        }));
        assert!(both_cfg.build_store("S3Sink").is_err());

        let mut bad_b64_cfg = base.clone();
        bad_b64_cfg.encryption = Some(Box::new(S3EncryptionConfig::SseC {
            customer_key_base64: Some("not!valid!b64!".to_string()),
            customer_key_file: None,
        }));
        assert!(bad_b64_cfg.build_store("S3Sink").is_err());

        let mut empty_kms_cfg = base;
        empty_kms_cfg.encryption = Some(Box::new(S3EncryptionConfig::SseKms {
            kms_key_id: Some("   ".to_string()),
        }));
        assert!(empty_kms_cfg.build_store("S3Sink").is_err());
    }

    #[cfg(feature = "connector-postgresql")]
    {
        use saci::connector::postgresql::{PostgresSource, PostgresSourceConfig};
        use serde::Deserialize as _;

        let tls = MtlsFixture::generate();
        let kdl_str = |p: &std::path::Path| p.display().to_string().replace('\\', "/");
        let parse_pg = |conn_block: &str| {
            let text = format!(
                r#"
name "t"
batch_rows 10
mode kind="polling" table="t" cursor_column="id"
field "id" type="int64" nullable=#false
{conn_block}
"#
            );
            let val = saci::connector::from_kdl_str(&text).expect("kdl parses");
            PostgresSourceConfig::deserialize(val).expect("pg cfg deserializes")
        };

        // Missing sslkey when sslcert is set -> refused.
        let half_cfg = parse_pg(&format!(
            r#"connection dsn="postgres://postgres:saci@127.0.0.1:5432/postgres" sslmode="require" sslrootcert="{ca}" sslcert="{cert}""#,
            ca = kdl_str(&tls.ca_file),
            cert = kdl_str(&tls.cli_cert_file),
        ));
        assert!(PostgresSource::new(half_cfg).is_err());

        // sslcert/sslkey with sslmode = disable -> refused.
        let disabled_cfg = parse_pg(&format!(
            r#"connection dsn="postgres://postgres:saci@127.0.0.1:5432/postgres" sslmode="disable" sslrootcert="{ca}" sslcert="{cert}" sslkey="{key}""#,
            ca = kdl_str(&tls.ca_file),
            cert = kdl_str(&tls.cli_cert_file),
            key = kdl_str(&tls.cli_key_file),
        ));
        assert!(PostgresSource::new(disabled_cfg).is_err());

        // Full mTLS PostgresSourceConfig builds synchronously without touching the socket.
        let ok_cfg = parse_pg(&format!(
            r#"connection dsn="postgres://postgres:saci@127.0.0.1:5432/postgres" sslmode="require" sslrootcert="{ca}" sslcert="{cert}" sslkey="{key}""#,
            ca = kdl_str(&tls.ca_file),
            cert = kdl_str(&tls.cli_cert_file),
            key = kdl_str(&tls.cli_key_file),
        ));
        assert!(PostgresSource::new(ok_cfg).is_ok());
    }
}

#[tokio::test]
async fn file_sink_and_crypto_wrappers_enforce_schema_and_detect_tamper() {
    use saci::connector::file::{FileSink, FileSinkConfig, FileSource, FileSourceConfig};
    use saci::crypto::{DecryptingSource, EncryptingSink};
    use saci::io::{Sink, Source};

    let dir = tempfile::tempdir().expect("tempdir");
    let cipher = test_cipher("k1", 0x55, Algorithm::Aes256Gcm);
    let schema = simple_schema();
    let batch = simple_batch();

    // 1. Multi-batch FileSink + EncryptedCodec + FileSource round-trip and tamper detection.
    for (ext, codec) in [
        ("ndjson.enc", Arc::new(NdjsonCodec::new()) as Arc<dyn Codec>),
        ("csv.enc", Arc::new(CsvCodec::new()) as Arc<dyn Codec>),
    ] {
        let path = dir.path().join(ext);
        let enc: Arc<dyn Codec> = Arc::new(EncryptedCodec::new(codec, Arc::clone(&cipher)));
        let mut sink = FileSink::new(
            FileSinkConfig {
                truncate: true,
                ..FileSinkConfig::new(&path)
            },
            Arc::clone(&schema),
            Arc::clone(&enc),
        )
        .expect("file sink");
        sink.write_batch(&batch).await.expect("write 1");
        sink.write_batch(&batch).await.expect("write 2");
        sink.finish().await.expect("finish");

        let mut source = FileSource::new(
            FileSourceConfig::new(&path),
            Some(Arc::clone(&schema)),
            Arc::clone(&enc),
        )
        .expect("file source");
        let mut rows = 0usize;
        while let Some(b) = source.next_batch().await.expect("next") {
            rows += b.num_rows();
        }
        assert_eq!(rows, 6);

        // Corrupt a byte inside the encrypted file and confirm read fails.
        let mut raw = std::fs::read(&path).unwrap();
        let mid = raw.len() / 2;
        raw[mid] ^= 0xFF;
        std::fs::write(&path, &raw).unwrap();
        let bad_res = FileSource::new(
            FileSourceConfig::new(&path),
            Some(Arc::clone(&schema)),
            Arc::clone(&enc),
        );
        let failed = match bad_res {
            Err(_) => true,
            Ok(mut src) => src.next_batch().await.is_err(),
        };
        assert!(failed, "{ext}: tampered encrypted file must fail readback");
    }

    // 2. EncryptingSink / DecryptingSource reject wrong inner/batch schemas.
    let mode = EncryptionMode::fields(["msg"]);
    let wrong_inner = FileSink::new(
        FileSinkConfig::new(dir.path().join("wrong_inner.ipc")),
        Arc::clone(&schema), // plaintext schema instead of derived wire schema
        Arc::new(ArrowIpcCodec::new()),
    )
    .unwrap();
    assert!(
        EncryptingSink::new(
            wrong_inner,
            Arc::clone(&schema),
            Arc::clone(&cipher),
            mode.clone(),
        )
        .is_err()
    );

    let wire = wire_schema(&schema, &mode).unwrap();
    let enc_file = dir.path().join("wrapped.ipc");
    let right_inner = FileSink::new(
        FileSinkConfig {
            truncate: true,
            ..FileSinkConfig::new(&enc_file)
        },
        Arc::clone(&wire),
        Arc::new(ArrowIpcCodec::new()),
    )
    .unwrap();
    let mut enc_sink = EncryptingSink::new(
        right_inner,
        Arc::clone(&schema),
        Arc::clone(&cipher),
        mode.clone(),
    )
    .unwrap();
    let bad_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    assert!(enc_sink.write_batch(&bad_batch).await.is_err());
    enc_sink.write_batch(&batch).await.unwrap();
    enc_sink.finish().await.unwrap();

    let wrong_src_inner = FileSource::new(
        FileSourceConfig::new(&enc_file),
        Some(Arc::clone(&schema)),
        Arc::new(ArrowIpcCodec::new()),
    )
    .unwrap();
    assert!(
        DecryptingSource::new(wrong_src_inner, schema, cipher, mode).is_err(),
        "DecryptingSource must reject inner source whose schema != wire schema"
    );
}

#[test]
fn service_config_kdl_roundtrip_and_topology_redaction() {
    use saci::inspector::{Inspector, InspectorConfig};
    use saci::service::build_topology;

    let tls = MtlsFixture::generate();
    let dir = tempfile::tempdir().expect("tempdir");
    let kdl_path = |p: &std::path::Path| p.display().to_string().replace('\\', "/");

    let secret_hex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    let in_path = dir.path().join("in.csv");
    std::fs::write(&in_path, "id,secret\n1,s\n").unwrap();
    let out_path = dir.path().join("out.ndjson");

    let kdl = format!(
        r#"
mode "standalone"

node id=1 data_dir="{data_dir}"

run "once"

encryption algorithm="chacha20-poly1305" active_key="k2" {{
    key "k1" hex="{secret_hex}"
    key "k2" hex="fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
}}

workflow "roundtrip" {{
    codec "plain" format="csv"
    codec "enc" format="ndjson" {{
        encryption enabled=#true algorithm="aes-256-gcm" active_key="k1"
    }}
    schema "Row" {{
        field "id" type="int64" nullable=#false
        field "secret" type="utf8" nullable=#false
    }}
    source "in" connector="file" codec="plain" component="Row" {{
        decrypt mode="fields" fields="secret" encoding="base64"
        config path="{in_path}"
    }}
    wasm "proc" name="native-pipeline"
    sink "out" connector="peer" component="Row" {{
        encrypt mode="payload" encoding="binary" active_key="k2"
        config connect="127.0.0.1:9999" {{
            tls {{
                ca_file "{ca}"
                cert_file "{cli_cert}"
                key_file "{cli_key}"
                server_name "localhost"
                use_platform_roots #false
            }}
        }}
    }}
    link from="in" to="proc"
    link from="proc" to="out"
}}

http enabled=#false
"#,
        data_dir = kdl_path(dir.path()),
        in_path = kdl_path(&in_path),
        ca = kdl_path(&tls.ca_file),
        cli_cert = kdl_path(&tls.cli_cert_file),
        cli_key = kdl_path(&tls.cli_key_file),
    );
    let _ = &out_path;

    let parsed = ServiceConfig::from_kdl(&kdl).expect("parse");
    let written = parsed.to_kdl().expect("to_kdl");
    let reparsed = ServiceConfig::from_kdl(&written).expect("reparse");
    assert_eq!(parsed.encryption, reparsed.encryption);
    assert_eq!(written, reparsed.to_kdl().expect("to_kdl twice"));

    let inspector = Inspector::new(&InspectorConfig::default());
    let built = ServiceBuilder::new()
        .inspector(inspector.clone())
        .runtime("proc", saci::Pipeline::new("proc"))
        .build(&parsed)
        .expect("build");
    let slices: Vec<&[_]> = built.iter().map(|b| b.nodes.as_slice()).collect();
    let topo = build_topology(&parsed, ServiceBuilder::new().registry(), &slices, 1);
    let json = serde_json::to_string(&topo).expect("serialize topology");
    assert!(
        !json.contains(secret_hex),
        "hex key leaked into topology JSON: {json}"
    );
    assert!(
        !json.contains(&kdl_path(&tls.cli_key_file)),
        "TLS private key path leaked into topology JSON: {json}"
    );
    assert!(
        !json.contains("chacha20-poly1305"),
        "encryption metadata unexpectedly exposed in node details: {json}"
    );
    let _ = inspector;
}

#[tokio::test]
async fn self_healing_tcp_sink_under_encrypt_wrapper_recovers() {
    use saci::connector::{ConfigValue, Connector, ConnectorContext, Role};
    use saci::error::SaciError;
    use saci::io::Source;

    struct PulseSource {
        next: i64,
        schema: Arc<Schema>,
    }
    #[async_trait::async_trait]
    impl Source for PulseSource {
        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }
        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.next += 1;
            let batch = RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![
                    Arc::new(Int64Array::from(vec![self.next])),
                    Arc::new(StringArray::from(vec![format!("secret-{}", self.next)])),
                ],
            )
            .expect("pulse batch");
            Ok(Some(batch))
        }
    }
    struct PulseConnector(Arc<Schema>);
    impl Connector for PulseConnector {
        fn name(&self) -> &'static str {
            "pulse"
        }
        fn roles(&self) -> &'static [Role] {
            &[Role::Source]
        }
        fn source(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Source>, SaciError> {
            Ok(Box::new(PulseSource {
                next: 0,
                schema: Arc::clone(&self.0),
            }))
        }
    }

    let tls = MtlsFixture::generate();
    let dir = tempfile::tempdir().expect("tempdir");
    let kdl_path = |p: &std::path::Path| p.display().to_string().replace('\\', "/");

    let listener1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener1.local_addr().unwrap().port();
    let addr = format!("127.0.0.1:{port}");

    let srv_tls_cfg =
        saci::tls::load_server_config("test", &tls.server_tls(), &[]).expect("server tls");
    let acceptor = tokio_rustls::TlsAcceptor::from(srv_tls_cfg);

    let key_bytes: [u8; 32] = std::array::from_fn(|i| (i as u8) ^ 0xA5);
    let provider =
        Arc::new(StaticKeyProvider::single("k1", KeyMaterial::from_bytes(key_bytes)).unwrap());

    let kdl = format!(
        r#"
mode "standalone"
node id=1 data_dir="{data_dir}"
run "stream"
heal {{
    after_failures 1
    base_delay_ms 15
    max_delay_ms 30
}}
workflow "heal_enc" {{
    codec "ipc" format="arrow-ipc"
    schema "Row" {{
        field "id" type="int64" nullable=#false
        field "secret" type="utf8" nullable=#false
    }}
    source "in" connector="pulse" component="Row"
    sink "out" connector="tcp" codec="ipc" component="Row" {{
        retry max_attempts=1
        encrypt fields="secret"
        config connect="{addr}" {{
            tls {{
                ca_file "{ca}"
                cert_file "{cli_cert}"
                key_file "{cli_key}"
                server_name "localhost"
                use_platform_roots #false
            }}
        }}
    }}
    link from="in" to="out"
}}
http enabled=#false
observability log_level="error"
"#,
        data_dir = kdl_path(dir.path()),
        ca = kdl_path(&tls.ca_file),
        cli_cert = kdl_path(&tls.cli_cert_file),
        cli_key = kdl_path(&tls.cli_key_file),
    );
    let plain_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("secret", DataType::Utf8, false),
    ]));
    let cfg = ServiceConfig::from_kdl(&kdl).expect("valid kdl");
    let wf = ServiceBuilder::new()
        .connector(PulseConnector(Arc::clone(&plain_schema)))
        .key_provider(Arc::clone(&provider) as Arc<dyn saci::crypto::KeyProvider>)
        .build(&cfg)
        .expect("builds healing+encrypt service")
        .remove(0);

    fn count_frames(
        listener: tokio::net::TcpListener,
        acc: tokio_rustls::TlsAcceptor,
        plain_schema: Arc<Schema>,
        provider: Arc<StaticKeyProvider>,
        frames: Arc<std::sync::atomic::AtomicUsize>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt as _;
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut tls_stream) = acc.accept(tcp).await else {
                return;
            };
            let mut header = [0u8; 4];
            while tls_stream.read_exact(&mut header).await.is_ok() {
                let len = u32::from_be_bytes(header) as usize;
                let mut payload = vec![0u8; len];
                if tls_stream.read_exact(&mut payload).await.is_err() {
                    break;
                }
                // Verify encrypted column is wire-typed Binary, not plaintext Utf8.
                let wire_codec = ArrowIpcCodec::new();
                let wire = wire_schema(&plain_schema, &EncryptionMode::fields(["secret"])).unwrap();
                let mut dec = wire_codec.open_message_decoder(wire).unwrap();
                dec.push(&payload).unwrap();
                let batch = dec.flush().unwrap().unwrap();
                let restored = decrypt_batch(
                    &batch,
                    &plain_schema,
                    &Cipher::aes_256_gcm(provider.clone()),
                    &EncryptionMode::fields(["secret"]),
                )
                .unwrap();
                assert_eq!(restored.num_rows(), 1);
                frames.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        })
    }

    let before = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let first = count_frames(
        listener1,
        acceptor.clone(),
        Arc::clone(&plain_schema),
        Arc::clone(&provider),
        Arc::clone(&before),
    );

    let cancel = CancellationToken::new();
    let wf_cancel = cancel.clone();

    let driver = async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while before.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out before heal"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        first.abort();
        let _ = first.await;

        let reopened = loop {
            match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => break l,
                Err(_) => tokio::time::sleep(Duration::from_millis(15)).await,
            }
        };
        let after = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let second = count_frames(
            reopened,
            acceptor,
            Arc::clone(&plain_schema),
            Arc::clone(&provider),
            Arc::clone(&after),
        );

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while after.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out after heal"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        second.abort();
        let _ = second.await;
        cancel.cancel();
    };

    let (wf_res, ()) = tokio::join!(wf.run(RunOptions::new(wf_cancel)), driver);
    let stats = wf_res.expect("healed run succeeds");
    assert!(
        stats.iteration_errors > 0,
        "expected failure count across listener swap"
    );
}
