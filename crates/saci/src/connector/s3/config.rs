//! Serde-derived configuration for the S3 source and sink.
//!
//! Every struct and enum here carries `#[serde(deny_unknown_fields)]`: a key
//! the connector cannot honour is a configuration error, not something to drop
//! silently. `object_store`'s `AmazonS3Builder` is string-keyed, so the
//! passthrough `options` table carries every option that is not a named key
//! here, and an unrecognised key there is an error too.
//!
//! [`ConnectionConfig::build_store`] constructs the client and is the one
//! place either half builds one. It is synchronous and opens no connection, so
//! `saci-service validate` and a `serve` that cannot reach the endpoint stay
//! load-time-safe: the first request happens inside `next_batch`/`write_batch`.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use serde::Deserialize;

use crate::connector::SchemaFrom;
use crate::error::SaciError;

/// Typed server-side encryption configuration for S3 objects.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum S3EncryptionConfig {
    /// Server-side encryption with Amazon S3 managed keys (`AES256`).
    SseS3,
    /// Server-side encryption with AWS KMS keys (`sse:kms`).
    SseKms {
        /// Optional KMS key ARN or alias; omitted uses the bucket default key.
        #[serde(default)]
        kms_key_id: Option<String>,
    },
    /// Server-side encryption with customer-provided 256-bit keys (`sse-c`).
    SseC {
        /// Base64-encoded 32-byte key. Mutually exclusive with `customer_key_file`.
        #[serde(default)]
        customer_key_base64: Option<String>,
        /// Path to a file containing the 32-byte raw or base64-encoded key.
        #[serde(default)]
        customer_key_file: Option<String>,
    },
}

/// Endpoint, credentials and client options shared by both halves.
#[derive(Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    /// Bucket both halves address. Required; `object_store` has no bucket
    /// creation API, so the bucket must already exist.
    pub bucket: String,
    /// Service endpoint, for example `http://127.0.0.1:9000`. Absent uses the
    /// AWS endpoint derived from `region`.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Region. Absent leaves `object_store`'s own `us-east-1` default, which is
    /// what most S3-compatible services ignore anyway.
    #[serde(default)]
    pub region: Option<String>,
    /// Static access key. Absent relies on `from_env` or the service's own
    /// credential resolution.
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// Static secret key, paired with `access_key_id`.
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Session token for a short-lived credential, paired with the two above.
    #[serde(default)]
    pub session_token: Option<String>,
    /// Permit plain HTTP. Required for a local S3-compatible server.
    #[serde(default)]
    pub allow_http: bool,
    /// Address the bucket as `bucket.host` instead of `host/bucket`. Off by
    /// default: path style is what an S3-compatible service on a bare host or
    /// IP needs.
    #[serde(default)]
    pub virtual_hosted_style: bool,
    /// Send unsigned requests and fetch no credentials, for a public bucket.
    #[serde(default)]
    pub skip_signature: bool,
    /// Seed the builder from the `AWS_*` environment variables first, which is
    /// how the IMDS, ECS, EKS and web-identity credential paths are reached.
    #[serde(default)]
    pub from_env: bool,
    /// Typed server-side encryption settings (`sse_s3`, `sse_kms`, `sse_c`).
    #[serde(default)]
    pub encryption: Option<Box<S3EncryptionConfig>>,
    /// Every remaining `object_store` S3 or HTTP-client option, by its own
    /// name: `unsigned_payload`, `checksum_algorithm`, `conditional_put`,
    /// `copy_if_not_exists`, `request_payer`, `disable_tagging`, `s3_express`,
    /// `sse_kms_key_id`, `role_arn`, `web_identity_token_file`, `timeout`,
    /// `connect_timeout`, `pool_idle_timeout`, `proxy_url`,
    /// `allow_invalid_certificates`, `http2_only`, `user_agent`, ... An
    /// unrecognised key is a configuration error naming it.
    #[serde(default)]
    pub options: BTreeMap<String, String>,
}

impl ConnectionConfig {
    /// Build the object client both halves share.
    ///
    /// Synchronous and opens no connection: construction never touches the
    /// network, so a `serve` that cannot reach the endpoint fails on the first
    /// `next_batch`/`write_batch`, not at load time.
    ///
    /// `what` names the half in error messages ("S3Source"/"S3Sink").
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when an `options` key is not an
    /// `object_store` option, or when the builder rejects the combined
    /// settings.
    pub fn build_store(&self, what: &str) -> Result<Arc<dyn ObjectStore>, SaciError> {
        let mut builder = if self.from_env {
            AmazonS3Builder::from_env()
        } else {
            AmazonS3Builder::new()
        };
        // The passthrough first, so a named key below wins over the same key
        // spelled into `options`.
        for (key, value) in &self.options {
            let key = AmazonS3ConfigKey::from_str(key).map_err(|e| {
                SaciError::configuration(format!("{what}: unknown connection option '{key}': {e}"))
            })?;
            builder = builder.with_config(key, value);
        }
        builder = builder.with_bucket_name(&self.bucket);
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }
        if let Some(access_key_id) = &self.access_key_id {
            builder = builder.with_access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = &self.secret_access_key {
            builder = builder.with_secret_access_key(secret_access_key);
        }
        if let Some(session_token) = &self.session_token {
            builder = builder.with_token(session_token);
        }
        if let Some(encryption) = &self.encryption {
            builder = apply_s3_encryption(what, builder, encryption.as_ref())?;
        }
        let store = builder
            .with_allow_http(self.allow_http)
            .with_virtual_hosted_style_request(self.virtual_hosted_style)
            .with_skip_signature(self.skip_signature)
            .build()
            .map_err(|e| SaciError::configuration(format!("{what}: {e}")))?;
        Ok(Arc::new(store))
    }
}

fn apply_s3_encryption(
    what: &str,
    mut builder: AmazonS3Builder,
    encryption: &S3EncryptionConfig,
) -> Result<AmazonS3Builder, SaciError> {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;

    let set_key =
        |b: AmazonS3Builder, key_name: &str, val: &str| -> Result<AmazonS3Builder, SaciError> {
            let key = AmazonS3ConfigKey::from_str(key_name).map_err(|e| {
                SaciError::configuration(format!("{what}: internal S3 key error: {e}"))
            })?;
            Ok(b.with_config(key, val))
        };

    match encryption {
        S3EncryptionConfig::SseS3 => set_key(builder, "aws_server_side_encryption", "AES256"),
        S3EncryptionConfig::SseKms { kms_key_id } => {
            builder = set_key(builder, "aws_server_side_encryption", "aws:kms")?;
            if let Some(id) = kms_key_id {
                if id.trim().is_empty() {
                    return Err(SaciError::configuration(format!(
                        "{what}: connection.encryption.kms_key_id must not be empty"
                    )));
                }
                builder = set_key(builder, "aws_sse_kms_key_id", id.trim())?;
            }
            Ok(builder)
        }
        S3EncryptionConfig::SseC {
            customer_key_base64,
            customer_key_file,
        } => {
            let b64 = match (customer_key_base64, customer_key_file) {
                (Some(inline), None) => {
                    let decoded = STANDARD.decode(inline.trim()).map_err(|e| {
                        SaciError::configuration(format!(
                            "{what}: connection.encryption.customer_key_base64 is invalid base64: {e}"
                        ))
                    })?;
                    if decoded.len() != 32 {
                        return Err(SaciError::configuration(format!(
                            "{what}: connection.encryption.customer_key_base64 must decode to 32 bytes, got {}",
                            decoded.len()
                        )));
                    }
                    inline.trim().to_string()
                }
                (None, Some(path)) => {
                    let raw = std::fs::read(path).map_err(|e| {
                        SaciError::configuration(format!(
                            "{what}: cannot read connection.encryption.customer_key_file '{path}': {e}"
                        ))
                    })?;
                    if raw.len() == 32 {
                        STANDARD.encode(raw)
                    } else {
                        let text = std::str::from_utf8(&raw).map_err(|_| {
                            SaciError::configuration(format!(
                                "{what}: connection.encryption.customer_key_file '{path}' must be 32 raw bytes or base64"
                            ))
                        })?;
                        let trimmed = text.trim();
                        let decoded = STANDARD.decode(trimmed).map_err(|e| {
                            SaciError::configuration(format!(
                                "{what}: connection.encryption.customer_key_file '{path}' is invalid base64: {e}"
                            ))
                        })?;
                        if decoded.len() != 32 {
                            return Err(SaciError::configuration(format!(
                                "{what}: connection.encryption.customer_key_file '{path}' must decode to 32 bytes, got {}",
                                decoded.len()
                            )));
                        }
                        trimmed.to_string()
                    }
                }
                _ => {
                    return Err(SaciError::configuration(format!(
                        "{what}: connection.encryption (sse_c) requires exactly one of 'customer_key_base64' or 'customer_key_file'"
                    )));
                }
            };
            builder = set_key(builder, "aws_server_side_encryption", "sse-c")?;
            set_key(builder, "aws_sse_customer_key_base64", &b64)
        }
    }
}

/// Configuration for [`crate::connector::s3::source::S3Source`].
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct S3SourceConfig {
    /// Endpoint, credentials and client options.
    pub connection: ConnectionConfig,
    /// Path prefix listed, `orders/2026` style. Empty lists the whole bucket.
    /// Handed verbatim to the service's list API, so it is a byte prefix:
    /// `orders` also matches `orders-old/a.csv`. Name prefixes with a trailing
    /// slash to stay inside one directory.
    #[serde(default)]
    pub prefix: String,
    /// Where the Arrow schema handed to the format comes from.
    #[serde(default)]
    pub schema_from: SchemaFrom,
}

/// Configuration for [`crate::connector::s3::sink::S3Sink`].
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct S3SinkConfig {
    /// Endpoint, credentials and client options.
    pub connection: ConnectionConfig,
    /// Path prefix every written object lands under.
    #[serde(default)]
    pub prefix: String,
    /// Appended to the generated key, `.csv` style. The format is never
    /// inferred, so nothing is appended by default.
    #[serde(default)]
    pub suffix: String,
    /// The row/byte/age thresholds that close the open object.
    #[serde(default)]
    pub flush: Flush,
}

/// How much this sink accumulates, or how long it waits, before it writes the
/// open object to S3.
///
/// Whichever threshold fires first closes the open object, uploads it, and
/// opens the next one. `0` disables a threshold; all three at `0` means the
/// sink writes exactly one object, at `finish`.
///
/// `max_rows` and `max_bytes` are checked once per `write_batch`, after the
/// batch is written. `max_age_ms` is driven by the sink's own ticker task, so a
/// sink that stops receiving batches still writes what it holds — that is the
/// point of the knob, and a lazy check inside `write_batch` would not do it.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Flush {
    /// Rows accumulated into the open object.
    #[serde(default = "default_flush_max_rows")]
    pub max_rows: usize,
    /// Encoded bytes accumulated into the open object. A format that buffers
    /// internally (parquet's row group) reports less than it has accepted, so
    /// this is a floor on the object size, not a ceiling. Non-zero by default
    /// because the open object lives in memory.
    #[serde(default = "default_flush_max_bytes")]
    pub max_bytes: usize,
    /// Wall-clock milliseconds since the open object took its first batch.
    #[serde(default = "default_flush_max_age_ms")]
    pub max_age_ms: u64,
}

// The `Default` impl returns the same three values as the serde defaults, so
// an absent `flush` node and an empty one agree.
impl Default for Flush {
    fn default() -> Self {
        Self {
            max_rows: default_flush_max_rows(),
            max_bytes: default_flush_max_bytes(),
            max_age_ms: default_flush_max_age_ms(),
        }
    }
}

fn default_flush_max_rows() -> usize {
    100_000
}

fn default_flush_max_bytes() -> usize {
    134_217_728
}

fn default_flush_max_age_ms() -> u64 {
    60_000
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::connector::{ConfigValue, from_kdl_str};

    fn config(raw: &str) -> ConfigValue {
        from_kdl_str(raw).expect("parse test config")
    }

    #[test]
    fn the_documented_defaults_land() {
        assert_eq!(
            Flush::default(),
            Flush {
                max_rows: 100_000,
                max_bytes: 134_217_728,
                max_age_ms: 60_000,
            }
        );
        assert_eq!(SchemaFrom::default(), SchemaFrom::Config);
        let sink = S3SinkConfig::deserialize(config(r#"connection { bucket "b" }"#))
            .expect("sink config parses");
        assert_eq!(sink.flush, Flush::default());
        assert_eq!(sink.prefix, "");
        assert_eq!(sink.suffix, "");
        let src = S3SourceConfig::deserialize(config(r#"connection { bucket "b" }"#))
            .expect("source config parses");
        assert_eq!(src.schema_from, SchemaFrom::Config);
        assert_eq!(src.prefix, "");
    }

    #[test]
    fn the_field_key_belongs_to_the_factory_not_the_structs() {
        let text = "connection { bucket \"b\" }\nfield \"id\" type=\"int64\"";
        assert!(S3SourceConfig::deserialize(config(text)).is_err());
        assert!(S3SinkConfig::deserialize(config(text)).is_err());
    }

    #[test]
    fn deny_unknown_fields_rejects_a_typo() {
        let err = S3SinkConfig::deserialize(config("connection { bucket \"b\" }\nbogus_key \"x\""))
            .expect_err("unknown key must be rejected");
        assert!(err.to_string().contains("bogus_key"));
        let err = ConnectionConfig::deserialize(config("bucket \"b\"\nbogus 1"))
            .expect_err("unknown connection key must be rejected");
        assert!(err.to_string().contains("bogus"));
    }

    #[test]
    fn an_unknown_connection_option_is_a_configuration_error_naming_it() {
        let cfg = ConnectionConfig::deserialize(config(
            "bucket \"b\"\noptions {\n    bogus_key \"v\"\n}",
        ))
        .expect("config parses");
        let err = cfg.build_store("S3Source").expect_err("unknown option");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("bogus_key"));
    }

    #[test]
    fn schema_from_parses_snake_case_names() {
        let cfg = S3SourceConfig::deserialize(config(
            "connection { bucket \"b\" }\nschema_from \"payload\"",
        ))
        .expect("source config parses");
        assert_eq!(cfg.schema_from, SchemaFrom::Payload);
    }

    #[test]
    fn build_store_makes_no_request() {
        let cfg = ConnectionConfig::deserialize(config(
            "bucket \"b\"\nendpoint \"http://127.0.0.1:1\"\nallow_http #true",
        ))
        .expect("config parses");
        cfg.build_store("S3Source")
            .expect("construction is request-free");
    }

    #[test]
    fn typed_s3_encryption_modes_parse_and_validate() {
        for kdl in [
            r#"bucket "b"
               encryption kind="sse_s3""#,
            r#"bucket "b"
               encryption kind="sse_kms" kms_key_id="arn:aws:kms:us-east-1:123:key/abc""#,
            r#"bucket "b"
               encryption kind="sse_c" customer_key_base64="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=""#,
        ] {
            let cfg = ConnectionConfig::deserialize(config(kdl)).expect("parses");
            cfg.build_store("S3Sink").expect("valid encryption builds");
        }

        let bad = ConnectionConfig::deserialize(config(
            r#"bucket "b"
               encryption kind="sse_c" customer_key_base64="AAAA""#,
        ))
        .expect("parses");
        let err = bad.build_store("S3Sink").expect_err("short key refused");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("32 bytes"), "got: {err}");
    }
}
