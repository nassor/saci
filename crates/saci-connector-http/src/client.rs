//! The one request path both client halves are built on.
//!
//! [`HttpClient`] is either a reqwest client, which speaks HTTP/1.1 and HTTP/2,
//! or the direct [`h3client`](crate::h3client) QUIC client, which speaks
//! HTTP/3. [`HttpVersion`] picks; everything above this module sends through
//! [`HttpClient::send`] and reads the reply out of an [`Exchange`] without
//! caring which transport carried it.
//!
//! reqwest 0.13's `rustls-no-provider` links rustls without a crypto provider
//! and panics when a `Client` is built before one is installed, so
//! [`install_ring`] installs `ring` first. That is the backend the rest of the
//! workspace links (saci-connector-postgresql's TLS path, saci-service's own
//! client), and the install is process global and idempotent, so a host that
//! already installed one keeps it.
//!
//! Response bodies arrive chunk by chunk through [`Body::next_chunk`], never as
//! one whole-body `Bytes`: a document body is spooled to a temp file as it
//! arrives, and a live body has no end to buffer to.

use std::sync::LazyLock;
use std::time::Duration;

use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode};

use saci_core::error::SaciError;

use crate::h3client;
use crate::tls::{self, TlsFiles};

/// Which HTTP version a client half speaks.
///
/// [`Auto`](HttpVersion::Auto) negotiates HTTP/2 or HTTP/1.1 over ALPN on
/// `https` and sends HTTP/1.1 on `http`. It never reaches HTTP/3: there is no
/// `Alt-Svc` discovery here and no h2c upgrade dance, so HTTP/2 on cleartext
/// and HTTP/3 anywhere are chosen by name.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HttpVersion {
    /// ALPN picks `h2` or `http/1.1` on https; HTTP/1.1 on http.
    #[default]
    Auto,
    /// HTTP/1.1 only, whatever the peer offers.
    Http11,
    /// HTTP/2 only: ALPN `h2` on https, prior knowledge (h2c) on http.
    Http2,
    /// HTTP/3 over QUIC. Needs an `https` url.
    Http3,
}

/// Which 3xx answers a client half follows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Redirects {
    /// reqwest's own policy: up to [`REDIRECT_HOPS`] hops, and a `301`, `302`
    /// or `303` is followed as a bodyless `GET`. What a read wants, because a
    /// read is a `GET` already.
    #[default]
    Any,
    /// `307` and `308` only, the two that keep the method and the body. Every
    /// other 3xx is handed back as the answer it is, so a write whose target
    /// moved is reported instead of arriving somewhere as a bodyless `GET`
    /// that the peer answers `200`.
    MethodPreserving,
}

/// How many redirects a client follows, reqwest's own default.
const REDIRECT_HOPS: usize = 10;

/// Everything a client half needs to build its transport.
pub(crate) struct ClientOptions<'a> {
    /// The connector type name an error is prefixed with.
    pub(crate) what: &'static str,
    /// Which HTTP version to speak.
    pub(crate) version: HttpVersion,
    /// The `tls` block, empty when the config named none.
    pub(crate) tls: &'a TlsFiles,
    /// The request budget. On a streaming read this is the idle gap between
    /// body chunks instead of a whole-request deadline, because a live body
    /// has no end.
    pub(crate) timeout: Duration,
    /// Whether the body this client reads is a live stream.
    pub(crate) streaming: bool,
    /// Which 3xx answers this client follows.
    pub(crate) redirects: Redirects,
}

/// The transport one client half sends every request through.
pub(crate) enum HttpClient {
    /// reqwest, for HTTP/1.1 and HTTP/2.
    Hyper(reqwest::Client),
    /// The direct QUIC client, for HTTP/3. Boxed: a `quinn` endpoint plus a
    /// rustls configuration dwarfs a reqwest handle, and every non-HTTP/3
    /// client would carry that footprint inline.
    Quic(Box<h3client::Client>),
}

/// One response: the head, and a body not yet read.
pub(crate) struct Exchange {
    /// The status the peer answered with.
    pub(crate) status: StatusCode,
    /// The url the response came from, which is the last hop when a redirect
    /// was followed. RFC 8288's link context, so a relative `Link` target
    /// resolves against it.
    pub(crate) url: String,
    /// The response headers.
    pub(crate) headers: HeaderMap,
    /// The body, read chunk by chunk.
    pub(crate) body: Body,
}

/// A response body, read one chunk at a time whichever transport carried it.
pub(crate) enum Body {
    /// A reqwest response with its head already consumed.
    Reqwest(reqwest::Response),
    /// An HTTP/3 response stream. Boxed for the same reason its client is.
    Quic(Box<h3client::RecvBody>),
}

impl Body {
    /// The next body chunk, or `None` at end of body.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] carrying the transport's own text when
    /// the body stops early or the read times out.
    pub(crate) async fn next_chunk(&mut self) -> Result<Option<Bytes>, SaciError> {
        match self {
            Self::Reqwest(response) => response
                .chunk()
                .await
                .map_err(|e| SaciError::generic(e.to_string())),
            Self::Quic(body) => body.next_chunk().await,
        }
    }
}

/// The workspace's `ring` crypto provider, installed on first force.
///
/// Process global and idempotent: a provider another crate installed first, or
/// a racing install from another thread, is left in place.
static RING_PROVIDER: LazyLock<()> = LazyLock::new(|| {
    let _ = rustls::crypto::ring::default_provider().install_default();
});

/// Install the `ring` provider, once per process.
///
/// Every client and listener build in this crate goes through here before it
/// touches rustls, so reqwest's "no process-level CryptoProvider" panic cannot
/// be reached.
pub(crate) fn install_ring() {
    LazyLock::force(&RING_PROVIDER);
}

impl HttpClient {
    /// Build the transport `options` describes.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when reqwest cannot build a client
    /// (for example when the platform trust store cannot be read), when a
    /// `tls` file is missing or malformed, or when HTTP/3's rustls
    /// configuration cannot be built.
    pub(crate) fn build(options: ClientOptions<'_>) -> Result<Self, SaciError> {
        install_ring();
        let what = options.what;
        let loaded = tls::load_client(what, options.tls)?;

        if options.version == HttpVersion::Http3 {
            return Ok(Self::Quic(Box::new(h3client::Client::new(
                what,
                tls::client_config(what, &loaded, &[b"h3"])?,
                options.timeout,
                options.streaming,
            ))));
        }

        let mut builder = reqwest::Client::builder();
        // A live body never ends, so a whole-request deadline would abort a
        // healthy stream. The same number becomes the connect budget and the
        // idle gap between chunks instead.
        builder = if options.streaming {
            builder
                .connect_timeout(options.timeout)
                .read_timeout(options.timeout)
        } else {
            builder.timeout(options.timeout)
        };
        builder = match options.version {
            HttpVersion::Http11 => builder.http1_only(),
            // Prior knowledge is h2c on `http://` and h2-only ALPN on
            // `https://`, which is what "speak HTTP/2" means with no upgrade
            // dance to fall back through.
            HttpVersion::Http2 => builder.http2_prior_knowledge(),
            HttpVersion::Auto | HttpVersion::Http3 => builder,
        };
        if options.redirects == Redirects::MethodPreserving {
            builder =
                builder.redirect(reqwest::redirect::Policy::custom(
                    |attempt| match sink_redirect(attempt.status(), attempt.previous().len()) {
                        Redirect::Follow => attempt.follow(),
                        Redirect::Stop => attempt.stop(),
                        Redirect::TooMany => {
                            attempt.error(format!("more than {REDIRECT_HOPS} redirects"))
                        }
                    },
                ));
        }
        if let Some(pem) = &loaded.ca_pem {
            // `tls_certs_merge` rather than the deprecated
            // `add_root_certificate`: same effect, the platform store plus
            // these, and it takes the whole bundle at once.
            builder = builder.tls_certs_merge(
                reqwest::Certificate::from_pem_bundle(pem).map_err(|e| tls_error(what, &e))?,
            );
        }
        if let (Some(cert), Some(key)) = (&loaded.cert_pem, &loaded.key_pem) {
            let mut pem = cert.clone();
            pem.extend_from_slice(key);
            builder = builder
                .identity(reqwest::Identity::from_pem(&pem).map_err(|e| tls_error(what, &e))?);
        }

        builder.build().map(Self::Hyper).map_err(|e| {
            SaciError::configuration(format!("{what}: cannot build an HTTP client: {e}"))
        })
    }

    /// Send one request and return its response with the body unread.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] carrying the transport's own text when
    /// the request cannot be made or the response head does not arrive. The
    /// caller prefixes it, so the wording names the connector and the url.
    pub(crate) async fn send(
        &self,
        method: &Method,
        url: &str,
        headers: &HeaderMap,
        body: Option<Bytes>,
    ) -> Result<Exchange, SaciError> {
        match self {
            Self::Hyper(client) => {
                let mut request = client.request(method.clone(), url).headers(headers.clone());
                if let Some(body) = body {
                    request = request.body(body);
                }
                let response = request
                    .send()
                    .await
                    .map_err(|e| SaciError::generic(e.to_string()))?;
                Ok(Exchange {
                    status: response.status(),
                    // The last hop, which reqwest tracks across the redirects
                    // it followed.
                    url: response.url().to_string(),
                    headers: response.headers().clone(),
                    body: Body::Reqwest(response),
                })
            }
            Self::Quic(client) => client.send(method, url, headers, body).await,
        }
    }
}

/// What a write half does with one 3xx answer.
#[derive(Debug, PartialEq, Eq)]
enum Redirect {
    /// Ask for the new target with the same method and body.
    Follow,
    /// Hand this answer back as the result of the request.
    Stop,
    /// Refuse: the chain is longer than [`REDIRECT_HOPS`].
    TooMany,
}

/// Where one 3xx answer leaves a request that carries a body.
///
/// Only `307` and `308` keep the method and the body. The older three are
/// defined to be followed as a `GET`, which would turn one batch of a write
/// into a bodyless request the new target answers `200`, so those stop here
/// and are reported as the status they are. `hops` is how many redirects this
/// chain already took, which a custom reqwest policy has to bound itself.
fn sink_redirect(status: StatusCode, hops: usize) -> Redirect {
    if hops > REDIRECT_HOPS {
        Redirect::TooMany
    } else if matches!(
        status,
        StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT
    ) {
        Redirect::Follow
    } else {
        Redirect::Stop
    }
}

/// The wording a `tls` block reqwest itself refused carries.
fn tls_error(what: &str, error: &reqwest::Error) -> SaciError {
    SaciError::configuration(format!("{what}: config.tls is not usable: {error}"))
}

/// Turn configured `(name, value)` pairs into the map every request carries.
///
/// Done once in the constructor rather than per request, so a header a server
/// would reject as malformed is a config error before the first batch.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] naming the offending header when the
/// name is not a valid HTTP field name or the value holds bytes a field value
/// cannot carry.
pub(crate) fn header_map(
    what: &str,
    headers: Vec<(String, String)>,
) -> Result<HeaderMap, SaciError> {
    let mut map = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        let parsed_name = HeaderName::try_from(name.as_str()).map_err(|e| {
            SaciError::configuration(format!("{what}: '{name}' is not a valid header name: {e}"))
        })?;
        let parsed_value = HeaderValue::try_from(value.as_str()).map_err(|e| {
            SaciError::configuration(format!("{what}: header '{name}' has an invalid value: {e}"))
        })?;
        map.append(parsed_name, parsed_value);
    }
    Ok(map)
}

/// HTTP/3 runs over TLS, so a cleartext url is a configuration error rather
/// than a connection that cannot be attempted.
pub(crate) fn require_https(what: &str, version: HttpVersion, url: &str) -> Result<(), SaciError> {
    if version == HttpVersion::Http3 && !url.starts_with("https://") {
        return Err(SaciError::configuration(format!(
            "{what}: http_version \"3\" needs an https url: HTTP/3 runs over TLS"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(version: HttpVersion) -> ClientOptions<'static> {
        /// A shared empty block, so the borrow outlives the call.
        static NO_TLS: LazyLock<TlsFiles> = LazyLock::new(TlsFiles::default);
        ClientOptions {
            what: "HttpSource",
            version,
            tls: &NO_TLS,
            timeout: Duration::from_secs(1),
            streaming: false,
            redirects: Redirects::default(),
        }
    }

    /// A `301`, `302` or `303` is defined to be followed as a `GET`, so a
    /// write half that followed one would report the new target's `200` for a
    /// batch that was never delivered.
    #[test]
    fn only_307_and_308_are_followed_with_the_body() {
        for status in [
            StatusCode::TEMPORARY_REDIRECT,
            StatusCode::PERMANENT_REDIRECT,
        ] {
            assert_eq!(sink_redirect(status, 0), Redirect::Follow, "{status}");
        }
        for status in [
            StatusCode::MOVED_PERMANENTLY,
            StatusCode::FOUND,
            StatusCode::SEE_OTHER,
            StatusCode::MULTIPLE_CHOICES,
        ] {
            assert_eq!(sink_redirect(status, 0), Redirect::Stop, "{status}");
        }
    }

    /// A custom reqwest policy carries no hop limit of its own, so this is
    /// what stops a `307` chain that points at itself.
    #[test]
    fn a_chain_past_ten_hops_is_refused() {
        let status = StatusCode::TEMPORARY_REDIRECT;
        assert_eq!(sink_redirect(status, REDIRECT_HOPS), Redirect::Follow);
        assert_eq!(sink_redirect(status, REDIRECT_HOPS + 1), Redirect::TooMany);
    }

    #[test]
    fn a_method_preserving_client_builds() {
        let mut opts = options(HttpVersion::Auto);
        opts.redirects = Redirects::MethodPreserving;
        HttpClient::build(opts).expect("the custom policy is accepted");
    }

    #[test]
    fn every_version_builds_a_client() {
        for version in [
            HttpVersion::Auto,
            HttpVersion::Http11,
            HttpVersion::Http2,
            HttpVersion::Http3,
        ] {
            let client = HttpClient::build(options(version)).expect("client builds");
            let quic = matches!(client, HttpClient::Quic(_));
            assert_eq!(
                quic,
                version == HttpVersion::Http3,
                "only HTTP/3 leaves reqwest, got {version:?}"
            );
        }
    }

    /// A streaming client trades the whole-request deadline for a read
    /// timeout; the build has to accept both shapes.
    #[test]
    fn a_streaming_client_builds() {
        let mut opts = options(HttpVersion::Auto);
        opts.streaming = true;
        HttpClient::build(opts).expect("client builds");
    }

    #[test]
    fn http3_over_cleartext_is_a_configuration_error() {
        let Err(err) = require_https("HttpSink", HttpVersion::Http3, "http://example.invalid/")
        else {
            panic!("HTTP/3 has no cleartext form");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "HttpSink: http_version \"3\" needs an https url: HTTP/3 runs over TLS"
        );
        require_https("HttpSink", HttpVersion::Http3, "https://example.invalid/")
            .expect("https is what it needs");
        require_https("HttpSink", HttpVersion::Auto, "http://example.invalid/")
            .expect("every other version is cleartext capable");
    }

    #[test]
    fn header_pairs_become_a_map_keeping_repeated_names() {
        let map = header_map(
            "HttpSink",
            vec![
                ("x-token".to_string(), "abc".to_string()),
                ("accept".to_string(), "text/csv".to_string()),
                ("accept".to_string(), "text/plain".to_string()),
            ],
        )
        .expect("valid headers");
        assert_eq!(map.get("x-token").expect("x-token"), "abc");
        assert_eq!(map.get_all("accept").iter().count(), 2);
    }

    #[test]
    fn a_bad_header_name_is_a_configuration_error_naming_it() {
        let Err(err) = header_map(
            "HttpSource",
            vec![("bad header".to_string(), "v".to_string())],
        ) else {
            panic!("a space is not legal in a header name");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'bad header'"), "got: {err}");
    }

    #[test]
    fn a_bad_header_value_is_a_configuration_error_naming_the_header() {
        let Err(err) = header_map(
            "HttpSink",
            vec![("x-token".to_string(), "a\nb".to_string())],
        ) else {
            panic!("a newline is not legal in a header value");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("header 'x-token'"), "got: {err}");
    }
}
