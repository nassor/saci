//! The `tls` block: a private CA to trust, and a client certificate to present.
//!
//! Every file the config names is read and parsed once, while the connector is
//! built, so a missing or malformed PEM is a configuration error before the
//! first request rather than a transport failure on the first batch. The
//! parsing is `rustls_pki_types`'s `PemObject`, the same crate and pattern
//! `saci-connector-postgresql`'s `build_client_config` uses.
//!
//! rustls configurations here are built with `builder_with_provider` and the
//! `ring` provider rather than `ClientConfig::builder`, so a process where
//! another dependency installed a different default provider cannot make this
//! connector panic or silently link the wrong backend.
//!
//! The verifier is `rustls_platform_verifier`, which is what reqwest's own
//! rustls path uses: the OS trust store, augmented by `tls.ca_file` when one is
//! named. That augmentation is unavailable on Android, where the platform
//! verifier has no extra-roots entry point, so a `ca_file` there is refused
//! instead of quietly ignored.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use saci_core::error::SaciError;

/// The files a `tls` block names, all optional.
///
/// `ca_file` is a PEM bundle added to the platform trust store. `cert_file` and
/// `key_file` are the client identity, and either both are set or neither is.
#[derive(Debug, Clone, Default)]
pub struct TlsFiles {
    /// A PEM bundle of extra roots to trust on top of the platform store.
    pub ca_file: Option<PathBuf>,
    /// The client certificate chain to present, PEM.
    pub cert_file: Option<PathBuf>,
    /// The private key for `cert_file`, PEM.
    pub key_file: Option<PathBuf>,
}

/// What a [`TlsFiles`] parsed into, held for the connector's lifetime.
pub(crate) struct LoadedTls {
    /// Extra roots from `ca_file`, empty when none was named.
    pub(crate) roots: Vec<CertificateDer<'static>>,
    /// The client identity from `cert_file` + `key_file`, `None` when neither
    /// was named.
    pub(crate) identity: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    /// `cert_file`'s bytes, kept because reqwest takes an identity as one PEM
    /// blob rather than as parsed DER.
    pub(crate) cert_pem: Option<Vec<u8>>,
    /// `key_file`'s bytes, for the same reason.
    pub(crate) key_pem: Option<Vec<u8>>,
    /// `ca_file`'s bytes, likewise: `reqwest::Certificate::from_pem_bundle`
    /// parses them itself.
    pub(crate) ca_pem: Option<Vec<u8>>,
}

/// Read and parse every file a client-side `tls` block names.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when a named file cannot be read, holds
/// no certificate or no private key, or when exactly one of `cert_file` and
/// `key_file` is set.
pub(crate) fn load_client(what: &str, files: &TlsFiles) -> Result<LoadedTls, SaciError> {
    let (ca_pem, roots) = match &files.ca_file {
        Some(path) => {
            let pem = read(what, "ca_file", path)?;
            let roots = certificates(what, "ca_file", path, &pem)?;
            (Some(pem), roots)
        }
        None => (None, Vec::new()),
    };

    let (cert_pem, key_pem, identity) = match (&files.cert_file, &files.key_file) {
        (Some(cert_path), Some(key_path)) => {
            let cert_pem = read(what, "cert_file", cert_path)?;
            let chain = certificates(what, "cert_file", cert_path, &cert_pem)?;
            let key_pem = read(what, "key_file", key_path)?;
            let key = private_key(what, key_path, &key_pem)?;
            (Some(cert_pem), Some(key_pem), Some((chain, key)))
        }
        (None, None) => (None, None, None),
        _ => {
            return Err(SaciError::configuration(format!(
                "{what} config.tls needs both 'cert_file' and 'key_file', or neither"
            )));
        }
    };

    Ok(LoadedTls {
        roots,
        identity,
        cert_pem,
        key_pem,
        ca_pem,
    })
}

/// Build the rustls client configuration the HTTP/3 client and the TLS tests
/// hand to QUIC, with `alpn` as the offered protocol list.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when the platform verifier cannot be
/// built, when extra roots are named on a platform that cannot accept them, or
/// when the client identity is not a usable certificate and key pair.
pub(crate) fn client_config(
    what: &str,
    loaded: &LoadedTls,
    alpn: &[&[u8]],
) -> Result<rustls::ClientConfig, SaciError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = if loaded.roots.is_empty() {
        rustls_platform_verifier::Verifier::new(Arc::clone(&provider))
    } else {
        extra_roots_verifier(what, &loaded.roots, Arc::clone(&provider))?
    }
    .map_err(|e| {
        SaciError::configuration(format!("{what}: cannot build a certificate verifier: {e}"))
    })?;

    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            SaciError::configuration(format!("{what}: cannot build a rustls configuration: {e}"))
        })?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier));

    let mut config = match &loaded.identity {
        Some((chain, key)) => builder
            .with_client_auth_cert(chain.clone(), key.clone_key())
            .map_err(|e| {
                SaciError::configuration(format!(
                    "{what}: config.tls cert_file and key_file are not a usable pair: {e}"
                ))
            })?,
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(config)
}

/// `new_with_extra_roots` is where a private CA is added to the platform store.
/// Android's platform verifier has no such entry point, so a `ca_file` there is
/// refused rather than silently dropped, the way reqwest refuses it.
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

/// Build the server configuration the push listener terminates TLS with.
///
/// ALPN is the caller's: one listener offers `h2` and `http/1.1`, the QUIC
/// endpoint offers `h3`, and both come from the same certificate.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when either file cannot be read or
/// parsed, or when the pair is not a usable certificate and key.
pub(crate) fn load_server(
    what: &str,
    cert_file: &Path,
    key_file: &Path,
) -> Result<rustls::ServerConfig, SaciError> {
    let cert_pem = read(what, "cert_file", cert_file)?;
    let chain = certificates(what, "cert_file", cert_file, &cert_pem)?;
    let key_pem = read(what, "key_file", key_file)?;
    let key = private_key(what, key_file, &key_pem)?;

    rustls::ServerConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            SaciError::configuration(format!("{what}: cannot build a rustls configuration: {e}"))
        })?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e| {
            SaciError::configuration(format!(
                "{what}: config.tls cert_file and key_file are not a usable pair: {e}"
            ))
        })
}

/// Read one named PEM file, naming the key and the path on failure.
fn read(what: &str, key: &str, path: &Path) -> Result<Vec<u8>, SaciError> {
    std::fs::read(path).map_err(|e| {
        SaciError::configuration(format!(
            "{what}: cannot read tls.{key} '{}': {e}",
            path.display()
        ))
    })
}

/// Every certificate in a PEM bundle, refusing a file that holds none.
fn certificates(
    what: &str,
    key: &str,
    path: &Path,
    pem: &[u8],
) -> Result<Vec<CertificateDer<'static>>, SaciError> {
    let mut chain = Vec::new();
    for certificate in CertificateDer::pem_slice_iter(pem) {
        chain.push(certificate.map_err(|e| {
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

/// The private key in a PEM file, whichever of the three PKCS encodings it is.
fn private_key(what: &str, path: &Path, pem: &[u8]) -> Result<PrivateKeyDer<'static>, SaciError> {
    PrivateKeyDer::from_pem_slice(pem).map_err(|e| {
        SaciError::configuration(format!(
            "{what}: tls.key_file '{}' holds no private key: {e}",
            path.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A self-signed certificate and its key, written to a fresh temp
    /// directory: the same fixture the round-trip tests use, in miniature.
    fn pair() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().expect("temp dir");
        let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("self-signed certificate");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, issued.cert.pem()).expect("write cert");
        std::fs::write(&key, issued.signing_key.serialize_pem()).expect("write key");
        (dir, cert, key)
    }

    #[test]
    fn a_ca_file_and_an_identity_both_load() {
        let (_dir, cert, key) = pair();
        let loaded = load_client(
            "HttpSink",
            &TlsFiles {
                ca_file: Some(cert.clone()),
                cert_file: Some(cert),
                key_file: Some(key),
            },
        )
        .expect("the pem files parse");
        assert_eq!(loaded.roots.len(), 1);
        assert!(loaded.identity.is_some());
    }

    #[test]
    fn a_client_config_carries_the_requested_alpn() {
        let loaded = load_client("HttpSink", &TlsFiles::default()).expect("no files is legal");
        let config = client_config("HttpSink", &loaded, &[b"h3"]).expect("config builds");
        assert_eq!(config.alpn_protocols, vec![b"h3".to_vec()]);
    }

    #[test]
    fn a_missing_file_names_the_key_and_the_path() {
        let Err(err) = load_client(
            "HttpSource",
            &TlsFiles {
                ca_file: Some(PathBuf::from("no/such/ca.pem")),
                ..TlsFiles::default()
            },
        ) else {
            panic!("a file that does not exist cannot be trusted");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("tls.ca_file"), "got: {err}");
        assert!(err.message().contains("ca.pem"), "got: {err}");
    }

    #[test]
    fn a_cert_file_holding_no_certificate_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("empty.pem");
        std::fs::write(&path, b"not a pem file").expect("write");

        let Err(err) = load_client(
            "HttpSink",
            &TlsFiles {
                ca_file: Some(path),
                ..TlsFiles::default()
            },
        ) else {
            panic!("an empty bundle trusts nothing");
        };
        assert!(
            err.message().contains("contains no certificates"),
            "got: {err}"
        );
    }

    /// The pair is the identity, so half of it is a config error rather than a
    /// silent fall back to no client certificate at all.
    #[test]
    fn half_an_identity_is_refused() {
        let (_dir, cert, _key) = pair();
        let Err(err) = load_client(
            "HttpSink",
            &TlsFiles {
                cert_file: Some(cert),
                ..TlsFiles::default()
            },
        ) else {
            panic!("a certificate with no key cannot be presented");
        };
        assert_eq!(
            err.message(),
            "HttpSink config.tls needs both 'cert_file' and 'key_file', or neither"
        );
    }

    /// The commonest way to fill the block in wrong: both keys pointing at the
    /// certificate.
    #[test]
    fn a_key_file_holding_a_certificate_is_refused() {
        let (_dir, cert, _key) = pair();
        let Err(err) = load_client(
            "HttpSink",
            &TlsFiles {
                cert_file: Some(cert.clone()),
                key_file: Some(cert),
                ..TlsFiles::default()
            },
        ) else {
            panic!("a certificate is not a private key");
        };
        assert!(err.message().contains("holds no private key"), "got: {err}");
    }

    #[test]
    fn a_server_config_loads_from_the_same_pair() {
        let (_dir, cert, key) = pair();
        let config = load_server("HttpPushSource", &cert, &key).expect("server config");
        assert!(config.alpn_protocols.is_empty(), "alpn is the caller's");
    }
}
