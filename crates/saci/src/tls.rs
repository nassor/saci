//! Shared `rustls` (`ring` provider) TLS and mutual-TLS configuration helpers.
//!
//! Every connector that speaks TLS over raw TCP, Peer Arrow IPC, HTTP, or OTLP
//! shares [`ClientTlsConfig`], [`ServerTlsConfig`], and the loaders here. All
//! configurations are built explicitly with [`rustls::crypto::ring::default_provider`]
//! so a process-global crypto provider installed by another dependency cannot
//! panic or swap backends silently.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::RootCertStore;
use rustls::crypto::ring::default_provider;
use rustls::server::WebPkiClientVerifier;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use serde::{Deserialize, Serialize};

use crate::error::SaciError;

fn default_true() -> bool {
    true
}

/// Client-side TLS and optional mutual-TLS settings.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTlsConfig {
    /// PEM bundle of trusted CA certificates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
    /// Client certificate chain PEM for mutual TLS. Paired with `key_file`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_file: Option<PathBuf>,
    /// Private key PEM for `cert_file`. Paired with `cert_file`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_file: Option<PathBuf>,
    /// Optional SNI and certificate hostname/IP override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_name: Option<String>,
    /// Trust OS platform roots in addition to `ca_file` (default `true`).
    ///
    /// Set to `#false` with `ca_file` to isolate trust strictly to a private CA.
    #[serde(default = "default_true")]
    pub use_platform_roots: bool,
}

impl Default for ClientTlsConfig {
    fn default() -> Self {
        Self {
            ca_file: None,
            cert_file: None,
            key_file: None,
            server_name: None,
            use_platform_roots: true,
        }
    }
}

/// Server-side TLS and optional mutual-TLS settings.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServerTlsConfig {
    /// Server certificate chain PEM.
    pub cert_file: PathBuf,
    /// Server private key PEM.
    pub key_file: PathBuf,
    /// Optional PEM bundle of client CA certificates to enforce mutual TLS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_ca_file: Option<PathBuf>,
}

/// Read one named PEM file, naming `key` and `path` on failure.
pub(crate) fn read_pem(what: &str, key: &str, path: &Path) -> Result<Vec<u8>, SaciError> {
    std::fs::read(path).map_err(|e| {
        SaciError::configuration(format!(
            "{what}: cannot read tls.{key} '{}': {e}",
            path.display()
        ))
    })
}

/// Parse every X.509 certificate in a PEM bundle, refusing an empty bundle.
pub(crate) fn parse_certs(
    what: &str,
    key: &str,
    path: &Path,
    pem: &[u8],
) -> Result<Vec<CertificateDer<'static>>, SaciError> {
    let mut chain = Vec::new();
    for cert in CertificateDer::pem_slice_iter(pem) {
        chain.push(cert.map_err(|e| {
            SaciError::configuration(format!(
                "{what}: tls.{key} '{}' is not a valid PEM bundle: {e}",
                path.display()
            ))
        })?);
    }
    if chain.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: tls.{key} '{}' contains no certificates",
            path.display()
        )));
    }
    Ok(chain)
}

/// Parse one private key from a PEM file.
pub(crate) fn parse_private_key(
    what: &str,
    key: &str,
    path: &Path,
    pem: &[u8],
) -> Result<PrivateKeyDer<'static>, SaciError> {
    PrivateKeyDer::from_pem_slice(pem).map_err(|e| {
        SaciError::configuration(format!(
            "{what}: tls.{key} '{}' holds no private key: {e}",
            path.display()
        ))
    })
}

/// Load client identity `(chain, key)` when both `cert_file` and `key_file` are set.
pub(crate) fn load_client_identity(
    what: &str,
    cert_file: Option<&Path>,
    key_file: Option<&Path>,
) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>, SaciError> {
    match (cert_file, key_file) {
        (Some(cert_path), Some(key_path)) => {
            let cert_pem = read_pem(what, "cert_file", cert_path)?;
            let chain = parse_certs(what, "cert_file", cert_path, &cert_pem)?;
            let key_pem = read_pem(what, "key_file", key_path)?;
            let key = parse_private_key(what, "key_file", key_path, &key_pem)?;
            Ok(Some((chain, key)))
        }
        (None, None) => Ok(None),
        _ => Err(SaciError::configuration(format!(
            "{what} config.tls needs both 'cert_file' and 'key_file', or neither"
        ))),
    }
}

/// Build a `rustls::ClientConfig` using the `ring` provider and `alpn` protocols.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when files are missing or invalid, when
/// `use_platform_roots` is `false` without `ca_file`, when only one of
/// `cert_file` and `key_file` is supplied, or when rustls rejects the pair.
pub fn load_client_config(
    what: &str,
    cfg: &ClientTlsConfig,
    alpn: &[&[u8]],
) -> Result<Arc<rustls::ClientConfig>, SaciError> {
    let provider = Arc::new(default_provider());
    let roots = match &cfg.ca_file {
        Some(path) => {
            let pem = read_pem(what, "ca_file", path)?;
            parse_certs(what, "ca_file", path, &pem)?
        }
        None => Vec::new(),
    };

    let identity = load_client_identity(what, cfg.cert_file.as_deref(), cfg.key_file.as_deref())?;

    let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            SaciError::configuration(format!("{what}: cannot build a rustls configuration: {e}"))
        })?;

    let with_verifier = if cfg.use_platform_roots {
        let verifier = if roots.is_empty() {
            rustls_platform_verifier::Verifier::new(provider)
        } else {
            extra_roots_verifier(what, &roots, provider)?
        }
        .map_err(|e| {
            SaciError::configuration(format!("{what}: cannot build a certificate verifier: {e}"))
        })?;
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
    } else {
        if roots.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: tls.use_platform_roots is false, so tls.ca_file must be set"
            )));
        }
        let mut store = RootCertStore::empty();
        for cert in roots {
            store.add(cert).map_err(|e| {
                SaciError::configuration(format!(
                    "{what}: tls.ca_file holds an unusable certificate: {e}"
                ))
            })?;
        }
        builder.with_root_certificates(store)
    };

    let mut client = match identity {
        Some((chain, key)) => with_verifier
            .with_client_auth_cert(chain, key)
            .map_err(|e| {
                SaciError::configuration(format!(
                    "{what}: config.tls cert_file and key_file are not a usable pair: {e}"
                ))
            })?,
        None => with_verifier.with_no_client_auth(),
    };
    client.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(Arc::new(client))
}

#[cfg(not(target_os = "android"))]
fn extra_roots_verifier(
    _what: &str,
    roots: &[CertificateDer<'static>],
    provider: Arc<rustls::crypto::CryptoProvider>,
) -> Result<Result<rustls_platform_verifier::Verifier, rustls::Error>, SaciError> {
    Ok(rustls_platform_verifier::Verifier::new_with_extra_roots(
        roots.to_vec(),
        provider,
    ))
}

#[cfg(target_os = "android")]
fn extra_roots_verifier(
    what: &str,
    _roots: &[CertificateDer<'static>],
    _provider: Arc<rustls::crypto::CryptoProvider>,
) -> Result<Result<rustls_platform_verifier::Verifier, rustls::Error>, SaciError> {
    Err(SaciError::configuration(format!(
        "{what}: tls.ca_file is not supported on this platform"
    )))
}

/// Build a `rustls::ServerConfig` using the `ring` provider and `alpn` protocols.
///
/// When `cfg.client_ca_file` is present, mutual TLS is enforced against that CA
/// bundle.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when any PEM file cannot be read or
/// parsed, or when the certificate/key pair is invalid.
pub fn load_server_config(
    what: &str,
    cfg: &ServerTlsConfig,
    alpn: &[&[u8]],
) -> Result<Arc<rustls::ServerConfig>, SaciError> {
    let provider = Arc::new(default_provider());
    let cert_pem = read_pem(what, "cert_file", &cfg.cert_file)?;
    let chain = parse_certs(what, "cert_file", &cfg.cert_file, &cert_pem)?;
    let key_pem = read_pem(what, "key_file", &cfg.key_file)?;
    let key = parse_private_key(what, "key_file", &cfg.key_file, &key_pem)?;

    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            SaciError::configuration(format!("{what}: cannot build a rustls configuration: {e}"))
        })?;

    let with_auth = match &cfg.client_ca_file {
        Some(ca_path) => {
            let ca_pem = read_pem(what, "client_ca_file", ca_path)?;
            let ca_certs = parse_certs(what, "client_ca_file", ca_path, &ca_pem)?;
            let mut store = RootCertStore::empty();
            for cert in ca_certs {
                store.add(cert).map_err(|e| {
                    SaciError::configuration(format!(
                        "{what}: tls.client_ca_file '{}' holds an unusable certificate: {e}",
                        ca_path.display()
                    ))
                })?;
            }
            let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(store), provider)
                .build()
                .map_err(|e| {
                    SaciError::configuration(format!(
                        "{what}: cannot build a client certificate verifier from '{}': {e}",
                        ca_path.display()
                    ))
                })?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };

    let mut server = with_auth.with_single_cert(chain, key).map_err(|e| {
        SaciError::configuration(format!(
            "{what}: config.tls cert_file and key_file are not a usable pair: {e}"
        ))
    })?;
    server.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(Arc::new(server))
}

/// Resolve the [`ServerName`] for a TLS dial from `override_name` or a `host:port` string.
///
/// Handles DNS names, IPv4 addresses, and bracketed IPv6 literals (`[::1]:9700`).
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when the extracted host is empty or not
/// a valid DNS name or IP address.
pub fn resolve_server_name(
    what: &str,
    addr: &str,
    override_name: Option<&str>,
) -> Result<ServerName<'static>, SaciError> {
    let host = if let Some(name) = override_name {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: tls.server_name must not be empty"
            )));
        }
        trimmed
    } else {
        extract_host(addr)
    };
    ServerName::try_from(host.to_string()).map_err(|e| {
        SaciError::configuration(format!(
            "{what}: '{host}' is not a valid TLS server name or IP: {e}"
        ))
    })
}

fn extract_host(addr: &str) -> &str {
    let trimmed = addr.trim();
    if let Some(rest) = trimmed.strip_prefix('[')
        && let Some((host, _)) = rest.split_once(']')
    {
        return host;
    }
    if let Some((host, port)) = trimmed.rsplit_once(':')
        && port.chars().all(|c| c.is_ascii_digit())
        && !host.contains(':')
    {
        return host;
    }
    trimmed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cert_pair(cn: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().expect("temp dir");
        let issued =
            rcgen::generate_simple_self_signed(vec![cn.to_string()]).expect("self-signed cert");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, issued.cert.pem()).expect("write cert");
        std::fs::write(&key, issued.signing_key.serialize_pem()).expect("write key");
        (dir, cert, key)
    }

    #[test]
    fn client_and_server_configs_load_with_mtls() {
        let (_sdir, scert, skey) = cert_pair("localhost");
        let (_cdir, ccert, ckey) = cert_pair("client");

        let server = load_server_config(
            "PeerSource",
            &ServerTlsConfig {
                cert_file: scert.clone(),
                key_file: skey,
                client_ca_file: Some(ccert.clone()),
            },
            &[b"saci-peer"],
        )
        .expect("server config builds");
        assert_eq!(server.alpn_protocols, vec![b"saci-peer".to_vec()]);

        let client = load_client_config(
            "PeerSink",
            &ClientTlsConfig {
                ca_file: Some(scert),
                cert_file: Some(ccert),
                key_file: Some(ckey),
                server_name: Some("localhost".to_string()),
                use_platform_roots: false,
            },
            &[b"saci-peer"],
        )
        .expect("client config builds");
        assert_eq!(client.alpn_protocols, vec![b"saci-peer".to_vec()]);
    }

    #[test]
    fn isolated_roots_without_ca_file_is_refused() {
        let err = load_client_config(
            "PeerSink",
            &ClientTlsConfig {
                use_platform_roots: false,
                ..ClientTlsConfig::default()
            },
            &[],
        )
        .expect_err("must refuse missing ca_file when platform roots are disabled");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("ca_file"), "got: {err}");
    }

    #[test]
    fn half_identity_is_refused() {
        let (_dir, cert, _key) = cert_pair("localhost");
        let err = load_client_config(
            "PeerSink",
            &ClientTlsConfig {
                cert_file: Some(cert),
                ..ClientTlsConfig::default()
            },
            &[],
        )
        .expect_err("half identity must fail");
        assert_eq!(
            err.message(),
            "PeerSink config.tls needs both 'cert_file' and 'key_file', or neither"
        );
    }

    #[test]
    fn resolves_dns_ipv4_and_ipv6_server_names() {
        let dns = resolve_server_name("PeerSink", "node.internal:9700", None).expect("dns");
        assert!(matches!(dns, ServerName::DnsName(_)));

        let v4 = resolve_server_name("PeerSink", "127.0.0.1:9700", None).expect("ipv4");
        assert!(matches!(v4, ServerName::IpAddress(_)));

        let v6 = resolve_server_name("PeerSink", "[::1]:9700", None).expect("ipv6");
        assert!(matches!(v6, ServerName::IpAddress(_)));

        let ovr =
            resolve_server_name("PeerSink", "127.0.0.1:9700", Some("svc.local")).expect("override");
        assert!(matches!(ovr, ServerName::DnsName(_)));
    }
}
