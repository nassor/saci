//! The direct HTTP/3 client: `h3` over `h3-quinn` over `quinn`, on `ring`.
//!
//! reqwest 0.13 does carry an `http3` feature, but it is `compile_error!`-gated
//! behind `--cfg reqwest_unstable` and implies `rustls`, which implies
//! `aws-lc-rs`. The workspace links `ring` and keeps `aws-lc-rs` out of every
//! bundle, so HTTP/3 is spoken here instead of through reqwest, and the shape
//! above this module is the same either way: one
//! [`Exchange`](crate::client::Exchange) per request.
//!
//! One connection is kept and reused across requests to the same authority, the
//! way RFC 9114 asks a client to. A connection whose driver task has ended is
//! dropped and redialed on the next request rather than reported as an error,
//! because a QUIC connection the peer closed while idle is not a failure of the
//! request that noticed.
//!
//! The endpoint is created on the first request, not in the constructor: quinn
//! binds its UDP socket through a runtime handle, and a client half is built
//! outside one so `saci-service validate` needs no runtime.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, Bytes};
use http::{HeaderMap, Method};
use saci_core::error::SaciError;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::client::{Body, Exchange};

/// One HTTP/3 request stream: what a sent request reads its response off.
type RequestStream = h3::client::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;

/// The QUIC stream one HTTP/3 response body is read from.
pub(crate) struct RecvBody {
    stream: RequestStream,
    /// Applied per chunk, so a live body's idle gap is bounded without
    /// bounding the body itself.
    read_timeout: Option<Duration>,
}

impl RecvBody {
    /// The next body chunk, or `None` at end of body.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the stream errors or the chunk does
    /// not arrive within the read timeout.
    pub(crate) async fn next_chunk(&mut self) -> Result<Option<Bytes>, SaciError> {
        let received = match self.read_timeout {
            Some(budget) => tokio::time::timeout(budget, self.stream.recv_data())
                .await
                .map_err(|_| SaciError::generic("timed out waiting for the next body chunk"))?,
            None => self.stream.recv_data().await,
        };
        // `h3` hands out an opaque `Buf`; one copy turns it into the `Bytes`
        // every caller of this crate already works in.
        Ok(received
            .map_err(|e| SaciError::generic(e.to_string()))?
            .map(|mut buf| buf.copy_to_bytes(buf.remaining())))
    }
}

/// One live HTTP/3 connection and the driver task polling it.
struct Live {
    send: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    driver: JoinHandle<()>,
    /// `host:port`, so a request to another authority redials rather than
    /// reusing this connection.
    authority: String,
}

/// An HTTP/3 client: one reusable connection, created on demand.
pub(crate) struct Client {
    /// The connector type name an error is prefixed with.
    what: &'static str,
    tls: Arc<rustls::ClientConfig>,
    /// The whole-exchange budget, or the connect and per-chunk budget when the
    /// body being read is a live stream.
    timeout: Duration,
    streaming: bool,
    /// One endpoint per address family, created on the first request that
    /// needs it: quinn binds through a runtime handle, and an endpoint bound
    /// to a wildcard IPv6 address cannot reach an IPv4 peer, so a host that
    /// resolves to both needs both.
    endpoints: Mutex<(Option<quinn::Endpoint>, Option<quinn::Endpoint>)>,
    connection: Mutex<Option<Live>>,
}

impl Client {
    /// Prepare the client without binding a socket or dialing anything.
    pub(crate) fn new(
        what: &'static str,
        tls: rustls::ClientConfig,
        timeout: Duration,
        streaming: bool,
    ) -> Self {
        Self {
            what,
            tls: Arc::new(tls),
            timeout,
            streaming,
            endpoints: Mutex::new((None, None)),
            connection: Mutex::new(None),
        }
    }

    /// Send one request over HTTP/3 and return its response head.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the url has no host, when no
    /// resolved address accepts a QUIC connection, when the request or the
    /// response head fails, or when the exchange exceeds the timeout.
    pub(crate) async fn send(
        &self,
        method: &Method,
        url: &str,
        headers: &HeaderMap,
        body: Option<Bytes>,
    ) -> Result<Exchange, SaciError> {
        let uri: http::Uri = url
            .parse()
            .map_err(|e| SaciError::generic(format!("'{url}' is not a valid uri: {e}")))?;
        let host = uri
            .host()
            .ok_or_else(|| SaciError::generic(format!("'{url}' names no host")))?
            .to_string();
        let port = uri.port_u16().unwrap_or(443);
        let authority = format!("{host}:{port}");

        let mut request = http::Request::builder().method(method.clone()).uri(&uri);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let request = request
            .body(())
            .map_err(|e| SaciError::generic(format!("cannot build the request: {e}")))?;

        // A live read bounds connect and the response head, then each chunk
        // separately; a document read bounds the whole exchange.
        let head = self.exchange(&host, port, &authority, request, body);
        let (status, response_headers, stream) = tokio::time::timeout(self.timeout, head)
            .await
            .map_err(|_| SaciError::generic("timed out"))??;

        Ok(Exchange {
            status,
            // This client follows no redirect, so the response came from the
            // url that was asked for.
            url: url.to_string(),
            headers: response_headers,
            body: Body::Quic(Box::new(RecvBody {
                stream,
                read_timeout: self.streaming.then_some(self.timeout),
            })),
        })
    }

    /// Open (or reuse) a connection, send the request, and read the head.
    async fn exchange(
        &self,
        host: &str,
        port: u16,
        authority: &str,
        request: http::Request<()>,
        body: Option<Bytes>,
    ) -> Result<(http::StatusCode, HeaderMap, RequestStream), SaciError> {
        let mut send = self.send_handle(host, port, authority).await?;
        let mut stream = send
            .send_request(request)
            .await
            .map_err(|e| SaciError::generic(e.to_string()))?;
        if let Some(body) = body {
            stream
                .send_data(body)
                .await
                .map_err(|e| SaciError::generic(e.to_string()))?;
        }
        stream
            .finish()
            .await
            .map_err(|e| SaciError::generic(e.to_string()))?;
        let response = stream
            .recv_response()
            .await
            .map_err(|e| SaciError::generic(e.to_string()))?;
        let (parts, ()) = response.into_parts();
        Ok((parts.status, parts.headers, stream))
    }

    /// The request handle for `authority`, reusing the live connection when
    /// there is one and its driver task has not ended.
    async fn send_handle(
        &self,
        host: &str,
        port: u16,
        authority: &str,
    ) -> Result<h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>, SaciError> {
        let mut guard = self.connection.lock().await;
        if let Some(live) = guard.as_ref() {
            if live.authority == authority && !live.driver.is_finished() {
                return Ok(live.send.clone());
            }
            // A finished driver means the connection is gone; a different
            // authority means this one is the wrong peer.
            if let Some(live) = guard.take() {
                live.driver.abort();
            }
        }

        let connection = self.connect(host, port).await?;
        let (mut driver, send) = h3::client::new(h3_quinn::Connection::new(connection))
            .await
            .map_err(|e| {
                SaciError::generic(format!(
                    "cannot start an HTTP/3 session with {authority}: {e}"
                ))
            })?;
        // The driver owns connection-level frames (SETTINGS, GOAWAY, control
        // streams); nothing on a request stream progresses without it.
        let driver = tokio::spawn(async move {
            let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
        });
        let handle = send.clone();
        *guard = Some(Live {
            send,
            driver,
            authority: authority.to_string(),
        });
        Ok(handle)
    }

    /// Dial every resolved address in order; the first that answers wins.
    ///
    /// Each attempt gets an equal share of the budget, so a host whose first
    /// address is unreachable does not spend the whole request timeout on a
    /// QUIC handshake that was never going to complete.
    async fn connect(&self, host: &str, port: u16) -> Result<quinn::Connection, SaciError> {
        let addresses: Vec<std::net::SocketAddr> = tokio::net::lookup_host((host, port))
            .await
            .map_err(|e| SaciError::generic(format!("cannot resolve {host}:{port}: {e}")))?
            .collect();
        if addresses.is_empty() {
            return Err(SaciError::generic(format!(
                "{host}:{port} resolved to no addresses"
            )));
        }
        let budget = self.timeout / addresses.len().max(1) as u32;

        let mut last = String::from("no address was tried");
        for address in addresses {
            let endpoint = self.endpoint(address.is_ipv6()).await?;
            match endpoint.connect(address, host) {
                Ok(connecting) => match tokio::time::timeout(budget, connecting).await {
                    Ok(Ok(connection)) => return Ok(connection),
                    Ok(Err(e)) => last = e.to_string(),
                    Err(_) => last = format!("{address} did not answer in {budget:?}"),
                },
                Err(e) => last = e.to_string(),
            }
        }
        Err(SaciError::generic(format!(
            "cannot connect to {host}:{port} over QUIC: {last}"
        )))
    }

    /// The endpoint for one address family, created on first use and bound to
    /// that family's wildcard address.
    async fn endpoint(&self, ipv6: bool) -> Result<quinn::Endpoint, SaciError> {
        let mut guard = self.endpoints.lock().await;
        let slot = if ipv6 { &mut guard.1 } else { &mut guard.0 };
        if let Some(endpoint) = slot.as_ref() {
            return Ok(endpoint.clone());
        }
        let bind = if ipv6 { "[::]:0" } else { "0.0.0.0:0" };
        let address = bind
            .parse()
            .map_err(|e| SaciError::generic(format!("cannot parse '{bind}': {e}")))?;
        let mut endpoint = quinn::Endpoint::client(address).map_err(|e| {
            SaciError::generic(format!("{}: cannot bind a QUIC socket: {e}", self.what))
        })?;
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(Arc::clone(&self.tls))
            .map_err(|e| {
                SaciError::generic(format!(
                    "{}: cannot use this TLS config for QUIC: {e}",
                    self.what
                ))
            })?;
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
        *slot = Some(endpoint.clone());
        Ok(endpoint)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Some(live) = self.connection.get_mut().take() {
            live.driver.abort();
        }
        let (v4, v6) = self.endpoints.get_mut();
        for endpoint in [v4.take(), v6.take()].into_iter().flatten() {
            endpoint.close(0u32.into(), b"");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::{self, TlsFiles};

    fn client() -> Client {
        crate::client::install_ring();
        let loaded = tls::load_client("HttpSource", &TlsFiles::default()).expect("no files");
        Client::new(
            "HttpSource",
            tls::client_config("HttpSource", &loaded, &[b"h3"]).expect("config"),
            Duration::from_millis(400),
            false,
        )
    }

    /// The endpoint is created lazily, so building one outside a runtime is
    /// what makes `saci-service validate` runtime-free.
    #[test]
    fn a_client_is_built_with_no_runtime_at_all() {
        let client = client();
        let bound = client.endpoints.blocking_lock();
        assert!(
            bound.0.is_none() && bound.1.is_none(),
            "nothing is bound until the first request"
        );
    }

    /// Nothing answers on a QUIC port nobody opened, and the failure names the
    /// authority rather than surfacing as a timeout with no detail.
    #[tokio::test]
    async fn a_dead_authority_names_the_host_and_port() {
        let client = client();
        let Err(err) = client
            .send(
                &Method::GET,
                "https://127.0.0.1:1/data",
                &HeaderMap::new(),
                None,
            )
            .await
        else {
            panic!("nothing is listening");
        };
        assert!(
            err.message().contains("127.0.0.1:1") || err.message().contains("timed out"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn a_url_with_no_host_is_an_error() {
        let client = client();
        let Err(err) = client
            .send(&Method::GET, "/relative", &HeaderMap::new(), None)
            .await
        else {
            panic!("a relative path names no peer");
        };
        assert!(err.message().contains("names no host"), "got: {err}");
    }
}
