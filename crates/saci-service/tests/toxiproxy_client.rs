//! Checks the HTTP requests [`common::ToxiproxyClient`] puts on the wire.
//!
//! Runs without Docker: a bare `std::net::TcpListener` stands in for the
//! Toxiproxy daemon and records each request's method, path, and JSON body,
//! which are compared against the documented Toxiproxy HTTP API
//! (<https://github.com/Shopify/toxiproxy#http-api>). The chaos tests drive the
//! same client against a real daemon, but soft-skip when Docker is absent.

#![cfg(feature = "distributed-raft")]

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::time::Duration;

use common::ToxiproxyClient;
use serde_json::{Value, json};

struct RecordedRequest {
    method: String,
    path: String,
    body: Value,
}

/// Read one HTTP/1.1 request off `stream`: request line, headers (just
/// enough to find `Content-Length`), and body.
fn read_request(stream: &mut TcpStream) -> RecordedRequest {
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));

    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .expect("read request line");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();

    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).expect("read header line");
        if line == "\r\n" || line.is_empty() {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            content_length = value.trim().parse().unwrap_or(0);
        }
    }

    let mut raw_body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut raw_body).expect("read body");
    }
    let body = if raw_body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&raw_body).expect("body is valid JSON")
    };

    RecordedRequest { method, path, body }
}

/// Start a minimal stand-in Toxiproxy daemon on an ephemeral port. Records
/// exactly `expected` requests (responding `200 {}` to each) and then stops.
fn start_mock_toxiproxy(expected: usize) -> (u16, mpsc::Receiver<RecordedRequest>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock listener");
    let port = listener.local_addr().expect("local_addr").port();
    let (tx, rx) = mpsc::channel();

    std::thread::spawn(move || {
        for stream in listener.incoming().take(expected) {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            let recorded = read_request(&mut stream);
            let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}";
            let _ = stream.write_all(response);
            if tx.send(recorded).is_err() {
                break;
            }
        }
    });

    (port, rx)
}

/// Each [`ToxiproxyClient`] method must issue the method, path, and JSON body the
/// real daemon expects. Reference: <https://github.com/Shopify/toxiproxy#http-api>.
#[test]
fn toxiproxy_client_sends_documented_requests() {
    const EXPECTED_REQUESTS: usize = 11;
    let (port, rx) = start_mock_toxiproxy(EXPECTED_REQUESTS);
    let client = ToxiproxyClient::new(port);
    let recv = |rx: &mpsc::Receiver<RecordedRequest>| {
        rx.recv_timeout(Duration::from_secs(5))
            .expect("mock server received a request")
    };

    client
        .create_proxy("n0_to_n1", "127.0.0.1:9001", 20001)
        .expect("create_proxy");
    let req = recv(&rx);
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/proxies");
    assert_eq!(
        req.body,
        json!({
            "name": "n0_to_n1",
            "listen": "0.0.0.0:20001",
            "upstream": "127.0.0.1:9001",
            "enabled": true,
        })
    );

    client.add_latency("n0_to_n1", 200).expect("add_latency");
    let req = recv(&rx);
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/proxies/n0_to_n1/toxics");
    assert_eq!(
        req.body,
        json!({
            "name": "latency_upstream",
            "type": "latency",
            "stream": "upstream",
            "toxicity": 1.0,
            "attributes": { "latency": 200, "jitter": 0 },
        })
    );

    client
        .add_bandwidth("n0_to_n1", 500)
        .expect("add_bandwidth");
    let req = recv(&rx);
    assert_eq!(req.path, "/proxies/n0_to_n1/toxics");
    assert_eq!(
        req.body,
        json!({
            "name": "bandwidth_upstream",
            "type": "bandwidth",
            "stream": "upstream",
            "toxicity": 1.0,
            "attributes": { "rate": 500 },
        })
    );

    client.add_timeout("n0_to_n1", 3000).expect("add_timeout");
    let req = recv(&rx);
    assert_eq!(req.path, "/proxies/n0_to_n1/toxics");
    assert_eq!(
        req.body,
        json!({
            "name": "timeout_upstream",
            "type": "timeout",
            "stream": "upstream",
            "toxicity": 1.0,
            "attributes": { "timeout": 3000 },
        })
    );

    // reset_peer is addressed by its bare type name (not `reset_peer_upstream`)
    // so `delete_toxic(&proxy, "reset_peer")` in the chaos tests can find it.
    client
        .add_reset_peer("n0_to_n1", 100)
        .expect("add_reset_peer");
    let req = recv(&rx);
    assert_eq!(req.path, "/proxies/n0_to_n1/toxics");
    assert_eq!(
        req.body,
        json!({
            "name": "reset_peer",
            "type": "reset_peer",
            "stream": "upstream",
            "toxicity": 1.0,
            "attributes": { "timeout": 100 },
        })
    );

    client
        .add_limit_data("n0_to_n1", 4096)
        .expect("add_limit_data");
    let req = recv(&rx);
    assert_eq!(req.path, "/proxies/n0_to_n1/toxics");
    assert_eq!(
        req.body,
        json!({
            "name": "limit_data_upstream",
            "type": "limit_data",
            "stream": "upstream",
            "toxicity": 1.0,
            "attributes": { "bytes": 4096 },
        })
    );

    client.disable_proxy("n0_to_n1").expect("disable_proxy");
    let req = recv(&rx);
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/proxies/n0_to_n1");
    assert_eq!(req.body, json!({ "enabled": false }));

    client.enable_proxy("n0_to_n1").expect("enable_proxy");
    let req = recv(&rx);
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/proxies/n0_to_n1");
    assert_eq!(req.body, json!({ "enabled": true }));

    client
        .delete_toxic("n0_to_n1", "reset_peer")
        .expect("delete_toxic");
    let req = recv(&rx);
    assert_eq!(req.method, "DELETE");
    assert_eq!(req.path, "/proxies/n0_to_n1/toxics/reset_peer");

    client.delete_proxy("n0_to_n1").expect("delete_proxy");
    let req = recv(&rx);
    assert_eq!(req.method, "DELETE");
    assert_eq!(req.path, "/proxies/n0_to_n1");

    client.reset().expect("reset");
    let req = recv(&rx);
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/reset");
}
