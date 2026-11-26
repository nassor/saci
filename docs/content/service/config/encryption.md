+++
title = "Encryption and TLS"
description = "Protect SACI to SACI traffic and data at rest with TLS or mutual TLS on the transport, AEAD codec framing, and field or payload encryption."
template = "page.html"
weight = 3
+++
# Encryption and TLS

SACI separates encryption into three layers you can turn on together or independently: transport TLS and mutual TLS between nodes, AEAD encryption around an encoded codec stream or message, and AEAD encryption of specific Arrow columns or whole batches before rows leave or enter a pipeline.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 210" role="img" aria-labelledby="enc-t enc-d">
        <title id="enc-t">Three composable encryption layers between a sending SACI sink and a receiving SACI source</title>
        <desc id="enc-d">On the left, service A holds a RecordBatch inside a sink node. An optional node encrypt block seals chosen columns or the whole batch into AEAD envelopes. An optional codec encryption block seals the encoded byte stream or message. A transport tls block wraps the TCP, HTTP, OTLP, or peer socket in TLS or mutual TLS. On the right, service B reverses those layers in order: transport TLS unwraps the socket, the encrypted codec decrypts the bytes, and the source decrypt block restores the original Arrow columns.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="34" width="154" height="128" rx="8"/>
            <rect class="hd hd-data" x="0" y="34" width="154" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="46" width="154" height="8"/>
            <text class="t-lbl t-data" x="10" y="49">sink node</text>
            <text class="t-sm" x="10" y="74">1. plain RecordBatch</text>
            <rect class="row" x="8" y="84" width="138" height="30" rx="4"/>
            <text class="t-sm" x="14" y="103">encrypt (fields / payload)</text>
            <rect class="row-w" x="8" y="122" width="138" height="30" rx="4"/>
            <text class="t-sm" x="14" y="141">codec encryption</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-ctl" d="M154 98 H222" marker-end="url(#enc-c)"/>
            <rect class="blk blk-bnd" x="226" y="48" width="208" height="100" rx="8"/>
            <rect class="hd hd-bnd" x="226" y="48" width="208" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="226" y="60" width="208" height="8"/>
            <text class="t-lbl t-bnd" x="236" y="63">transport TLS / mTLS</text>
            <text class="t-sm" x="238" y="92">rustls (ring provider)</text>
            <text class="t-sm" x="238" y="112">peer, tcp, http, otlp-metrics,</text>
            <text class="t-sm" x="238" y="128">postgresql, mariadb, nats</text>
            <path class="arw arw-ctl" d="M434 98 H502" marker-end="url(#enc-c)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="506" y="34" width="154" height="128" rx="8"/>
            <rect class="hd hd-data" x="506" y="34" width="154" height="20" rx="8"/>
            <rect class="hd hd-data" x="506" y="46" width="154" height="8"/>
            <text class="t-lbl t-data" x="516" y="49">source node</text>
            <rect class="row-w" x="514" y="66" width="138" height="30" rx="4"/>
            <text class="t-sm" x="520" y="85">codec decryption</text>
            <rect class="row" x="514" y="104" width="138" height="30" rx="4"/>
            <text class="t-sm" x="520" y="123">decrypt (fields / payload)</text>
            <text class="t-sm" x="516" y="152">original Arrow types</text>
        </g>
        <g class="anim anim-4">
            <path class="ln" d="M0 182 H660"/>
            <text class="t-sm" x="0" y="200">Every envelope stores its key_id so receivers decrypt older keys during a key rotation.</text>
        </g>
        <defs>
            <marker id="enc-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data encryption on nodes and codecs</span>
        <span class="k-boundary"><i></i> transport TLS boundary</span>
    </div>
</div>

The stock `server` build includes `crypto` (`saci::crypto`) and `tls` (`saci::tls`). A custom build omitting `crypto` still parses encryption blocks and rejects them during validation naming `--features crypto`.

## 1. Declare a keyring

Add a top-level `encryption` block naming the active key id and one or more 256-bit keys. Each `key` entry reads from `env`, `file`, `hex`, or `base64`.

```kdl,name=Top-level keyring with one active key and one retired key
encryption {
    algorithm "aes-256-gcm"
    active_key "k2"
    key id="k1" file="/etc/saci/keys/k1.key"
    key id="k2" env="SACI_KEY_K2"
}
```

`algorithm` defaults to `"aes-256-gcm"` and also accepts `"chacha20-poly1305"`. Every wire envelope carries the `key_id` that produced it, so a receiver holding both `k1` and `k2` decrypts batches written before a key rotation while encrypting new batches under `active_key`. Key material never leaves the loader: `/topology` and serialized configs omit keys, secrets, and key paths.

When loading from Rust with `ServiceBuilder`, `builder.key_provider(provider)` supplies keys programmatically and wins over a keyring built from KDL.

## 2. Encrypt a whole byte format

Add an `encryption` child block inside any `codec` node to wrap that codec's encoded bytes with AEAD framing.

```kdl,name=An encrypted Parquet codec
codec "orders_parquet" format="parquet" {
    encryption enabled=#true algorithm="aes-256-gcm"
}
```

When `enabled` is `#true`, stream writes and reads use `SACS` v1 chunked AEAD framing where each chunk binds the 9-byte stream header, chunk index, and final-chunk flag as authenticated data, detecting chunk reordering or truncation. Message writes and reads (`kafka`, `nats`, `tcp`, `udp`) seal each message in a `SACI` v1 envelope. The underlying byte format (`csv`, `ndjson`, `parquet`, `avro`, or `arrow-ipc`) does not change.

## 3. Encrypt fields or whole batches on a node

Use `encrypt` on a `sink` and `decrypt` on a `source` when data needs to stay encrypted inside row-oriented stores, across `channel` or `peer` links, or on selected columns only.

`mode "fields"` encrypts the listed columns in place while keeping every other column readable for routing, partitioning, or queries. `mode "payload"` serializes the whole `RecordBatch` as an Arrow IPC stream into a single column named `__saci_encrypted_payload`.

```kdl,name=Field-level encryption into a peer sink and decryption on the receiving source
// Sender service
sink "out" connector="peer" component="Order" {
    encrypt {
        mode "fields"
        fields "customer_email" "card_last4"
        encoding "binary"
    }
    config {
        connect "127.0.0.1:9700"
        tls {
            ca_file "/etc/saci/certs/ca.crt"
            cert_file "/etc/saci/certs/sender.crt"
            key_file "/etc/saci/certs/sender.key"
        }
        field "id" type="int64" nullable=#false
        field "customer_email" type="utf8" nullable=#false
        field "card_last4" type="utf8" nullable=#true
    }
}

// Receiver service
source "in" connector="peer" component="Order" {
    decrypt {
        mode "fields"
        fields "customer_email" "card_last4"
        encoding "binary"
    }
    config {
        bind "0.0.0.0:9700"
        tls {
            cert_file "/etc/saci/certs/receiver.crt"
            key_file "/etc/saci/certs/receiver.key"
            client_ca_file "/etc/saci/certs/ca.crt"
        }
        field "id" type="int64" nullable=#false
        field "customer_email" type="utf8" nullable=#false
        field "card_last4" type="utf8" nullable=#true
    }
}
```

Two rules make this composition transparent to workflows:

1. You declare the plain column types on both nodes. During build, `ServiceBuilder` rewrites the wire `field` definitions to `binary` (or `utf8` when `encoding="base64"`) for encrypted columns, or to `__saci_encrypted_payload` in `payload` mode, before constructing the underlying connector.
2. Field names are authenticated as AAD alongside the ciphertext, so swapping ciphertext between two encrypted columns fails decryption. On decryption, values return with their exact original Arrow types and nullability.

For formats that carry text rather than raw binary bytes (such as `csv` or `ndjson`), set `encoding "base64"`. `EncryptingSink` and `DecryptingSource` also wrap outside connector self-healing, so a recreated sink or source after a reconnect continues encrypting and decrypting.

## 4. Enable transport TLS and mutual TLS

Connectors that speak TLS share `rustls` with the `ring` crypto provider and keep certificate verification on at all times. Client connectors accept `tls` with `ca_file`, `cert_file`, `key_file`, `server_name`, and `use_platform_roots`; server listeners accept `tls` with `cert_file`, `key_file`, and `client_ca_file`.

| Connector | Where TLS lives | Mutual TLS | Page |
|---|---|---|---|
| `peer` | `config.tls` on source and sink | `client_ca_file` + client `cert_file`/`key_file` | [Peer](@/service/connectors/peer.md) |
| `tcp` | `config.tls` on source and sink | `client_ca_file` + client `cert_file`/`key_file` | [TCP](@/service/connectors/tcp.md) |
| `http` / `http-listen` | `config.tls` (`cert_file`, `key_file`, optional `client_ca_file` on listener) | `client_ca_file` + client `cert_file`/`key_file` | [HTTP](@/service/connectors/http.md) |
| `otlp-metrics` | `config.tls` or `config.mode.tls` | `client_ca_file` + client `cert_file`/`key_file` | [OTLP metrics](@/service/connectors/otlp-metrics.md) |
| `postgresql` | `connection.sslmode`, `sslrootcert`, `sslcert`, `sslkey` | client cert + `verify-ca` / `verify-full` | [PostgreSQL](@/service/connectors/postgresql.md) |
| `mariadb` / MySQL | `connection.ssl_mode`, `ssl_ca` | TLS identity verification | [MariaDB](@/service/connectors/mariadb.md) |
| `nats` | `connection.tls_*` and `tls_first` | client cert + CA verification | [NATS](@/service/connectors/nats.md) |
| `s3` | HTTPS endpoint + `connection.encryption` (`sse_s3`, `sse_kms`, `sse_c`) | endpoint HTTPS | [S3](@/service/connectors/s3.md) |
| `turso` | `encryption.cipher` + `hexkey` (`aes256gcm`) | local page encryption | [Turso](@/service/connectors/turso.md) |

## 5. Validate and run an example

Validate any KDL file declaring encryption keys, codecs, or TLS blocks before starting `serve`. Runs the same on Linux, macOS and Windows (PowerShell):

```text
saci-service validate --config service.kdl --connectors-only
```

From a checkout, run the standalone `encrypted_pipelines` example showing peer mTLS alongside field-level AEAD and an encrypted Parquet file sink:

Linux/macOS:

```bash
cargo run -p saci --example encrypted_pipelines --features "crypto,connector-peer,connector-file,codec-parquet"
```

Windows (PowerShell):

```powershell
cargo run -p saci --example encrypted_pipelines --features "crypto,connector-peer,connector-file,codec-parquet"
```

That example starts a peer source and sink over ephemeral mTLS certificates, sends batches with selected encrypted fields and an encrypted Parquet file output, and exits 0 once the written records decrypt and match the input.

## Every key

### encryption

| Key | Type | Default | What it does |
|---|---|---|---|
| `algorithm` | `"aes-256-gcm"` or `"chacha20-poly1305"` | `"aes-256-gcm"` | default AEAD algorithm for codecs and nodes |
| `active_key` | string | required | key id used for new encryption |
| `key` | repeated block | at least one | keyring entries available to encrypt and decrypt |

### encryption.key

| Key | Type | Default | What it does |
|---|---|---|---|
| `id` | string | required | key identifier (`1..=64` UTF-8 bytes) written into envelopes |
| `env` | string | none | environment variable holding 64 hex chars or base64 key material |
| `file` | string | none | file holding raw 32 bytes, 64 hex chars, or base64 key material |
| `hex` | string | none | 64-character hex string |
| `base64` | string | none | base64-encoded 32-byte key |

Set one and only one of `env`, `file`, `hex`, or `base64` on each `key`.

### codec.encryption

| Key | Type | Default | What it does |
|---|---|---|---|
| `enabled` | bool | `#true` | wrap this codec in AEAD encryption |
| `algorithm` | string | service default | override `"aes-256-gcm"` or `"chacha20-poly1305"` |
| `active_key` | string | service default | override active `key` id for writes |

### sink.encrypt and source.decrypt

| Key | Type | Default | What it does |
|---|---|---|---|
| `mode` | `"fields"` or `"payload"` | `"fields"` when `fields` is set | encrypt listed columns individually or the full batch as one IPC payload column |
| `fields` | string or list | required in `"fields"` mode | column names to encrypt or decrypt (comma-separated string or list) |
| `encoding` | `"binary"` or `"base64"` | `"binary"` | wire column encoding |
| `algorithm` | string | service default | override `"aes-256-gcm"` or `"chacha20-poly1305"` |
| `active_key` | string | service default | `sink.encrypt` only; override active `key` id |

### tls (client connectors)

| Key | Type | Default | What it does |
|---|---|---|---|
| `ca_file` | string | none | PEM bundle of trusted CA certificates |
| `cert_file` | string | none | client certificate chain PEM for mutual TLS; pairs with `key_file` |
| `key_file` | string | none | client private key PEM; pairs with `cert_file` |
| `server_name` | string | derived from URL or `connect` | optional SNI and hostname/IP override |
| `use_platform_roots` | bool | `#true` | keep OS platform trust roots alongside `ca_file`; `#false` trusts `ca_file` alone |

### tls (server listeners)

| Key | Type | Default | What it does |
|---|---|---|---|
| `cert_file` | string | required | server certificate chain PEM |
| `key_file` | string | required | server private key PEM |
| `client_ca_file` | string | none | PEM bundle of client CAs to require mutual TLS |

## When it refuses to start

| Message | What to change |
|---|---|
| `... was built without the 'crypto' feature; rebuild with --features crypto` | enable `crypto` (or use `server`) |
| `encryption: at least one 'key' entry is required` | add at least one `key` child inside `encryption` |
| `encryption: active_key '{id}' is not declared in 'key' entries` | declare that `id`, or change `active_key` |
| `encryption: duplicate key id '{id}'` | give each `key` a distinct `id` |
| `encryption.key '{id}': specify exactly one of 'env', 'file', 'hex', or 'base64'` | set a single key source |
| `source ... decrypt: 'active_key' is not allowed on source decrypt` | remove `active_key` from `source.decrypt` (reads `key_id` from envelopes) |
| `... 'fields' cannot be set when mode="payload"` | drop `fields`, or set `mode "fields"` |
| `HttpListenSource config.tls['ca_file'] is not a listener key: ...` | use `client_ca_file` for mTLS on a listener |

## Next

- [Workflows and links](@/service/config/workflows.md) for node syntax in a workflow graph.
- [Peer](@/service/connectors/peer.md) and [TCP](@/service/connectors/tcp.md) for SACI to SACI streaming links.
- [Formats](@/service/formats/_index.md) for byte codecs.
- [Crates and features](@/library/reference/crates.md) for the `crypto` and `tls` Cargo features and Rust API modules.
