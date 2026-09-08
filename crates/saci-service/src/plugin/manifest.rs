//! The JSON manifest a plugin returns from `describe`, parsed and validated.
//!
//! This half of the loader touches no FFI, so every rule it enforces is unit
//! testable against a byte string. The manifest mirrors the WIT
//! `pipeline-descriptor` field for field, with each component's Arrow IPC
//! schema base64 encoded because base64 is what every non-Rust processor can emit
//! from a generated constant without linking Arrow.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use saci_core::{SaciError, SaciResult};
use serde::Deserialize;

/// What a plugin says about itself.
///
/// `deny_unknown_fields` matches the config module's convention: a key the host
/// cannot honour is a load error, not a silent drop.
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct PluginManifest {
    /// Pipeline identity, authoritative. The host supplies no name of its own.
    pub name: String,
    /// The plugin crate's own version string.
    pub version: String,
    /// Whether the plugin carries state across batches in the checkpoint blob.
    pub stateful: bool,
    /// Lowercase 8 character hex of the FNV-1a hash over the component schemas.
    pub schema_fingerprint: String,
    /// Every component the plugin registers, sorted by name.
    pub components: Vec<ManifestComponent>,
}

/// One component's name and schema.
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestComponent {
    /// Component name, matching what the plugin registers in its dataset.
    pub name: String,
    /// Standard base64 with padding of a schema only Arrow IPC stream.
    pub arrow_schema_ipc_base64: String,
}

impl PluginManifest {
    /// Decode and validate the bytes `describe` produced.
    ///
    /// Everything a malformed plugin can put here is rejected with a message
    /// naming the offending field, because the alternative is a confusing
    /// failure several layers later in dataset registration.
    pub(crate) fn parse(json: &[u8]) -> SaciResult<Self> {
        let text = std::str::from_utf8(json).map_err(|e| {
            SaciError::configuration(format!("plugin manifest is not valid UTF-8: {e}"))
        })?;

        let manifest: Self = serde_json::from_str(text).map_err(|e| {
            SaciError::configuration(format!("plugin manifest is not valid JSON: {e}"))
        })?;

        manifest.validate()?;
        Ok(manifest)
    }

    /// Reject a manifest that parses but cannot describe a usable pipeline.
    fn validate(&self) -> SaciResult<()> {
        if self.name.is_empty() {
            return Err(SaciError::configuration(
                "plugin manifest field `name` is empty".to_string(),
            ));
        }

        if !is_fingerprint(&self.schema_fingerprint) {
            return Err(SaciError::configuration(format!(
                "plugin manifest field `schema_fingerprint` is `{}`, expected 8 lowercase hex characters",
                self.schema_fingerprint
            )));
        }

        if self.components.is_empty() {
            return Err(SaciError::configuration(format!(
                "plugin `{}` declares no components",
                self.name
            )));
        }

        for (index, component) in self.components.iter().enumerate() {
            if component.name.is_empty() {
                return Err(SaciError::configuration(format!(
                    "plugin `{}` component at index {index} has an empty name",
                    self.name
                )));
            }
            if self.components[..index]
                .iter()
                .any(|earlier| earlier.name == component.name)
            {
                return Err(SaciError::configuration(format!(
                    "plugin `{}` declares component `{}` twice",
                    self.name, component.name
                )));
            }
        }

        Ok(())
    }

    /// Base64 decode every component's Arrow IPC schema.
    ///
    /// An empty decode result is an error rather than a skipped component. The
    /// Rust processor SDK writes empty bytes when a schema fails to serialise, so
    /// this is where that failure becomes a clean load-time message instead of
    /// a component silently missing from the template dataset.
    pub(crate) fn decode_components(&self) -> SaciResult<Vec<(String, Vec<u8>)>> {
        let mut decoded = Vec::with_capacity(self.components.len());

        for component in &self.components {
            let bytes = STANDARD
                .decode(component.arrow_schema_ipc_base64.as_bytes())
                .map_err(|e| {
                    SaciError::configuration(format!(
                        "plugin `{}` component `{}` has an invalid base64 schema: {e}",
                        self.name, component.name
                    ))
                })?;

            if bytes.is_empty() {
                return Err(SaciError::configuration(format!(
                    "plugin `{}` component `{}` has an empty Arrow IPC schema",
                    self.name, component.name
                )));
            }

            decoded.push((component.name.clone(), bytes));
        }

        Ok(decoded)
    }
}

/// Whether `value` is exactly 8 lowercase hex characters.
fn is_fingerprint(value: &str) -> bool {
    value.len() == 8
        && value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Base64 of `b"schema"`, enough for the rules this module enforces: only
    /// the runtime asks Arrow to parse the bytes.
    const SCHEMA_B64: &str = "c2NoZW1h";

    fn manifest_json(components: &str) -> String {
        format!(
            r#"{{"name":"p","version":"0.1.0","stateful":true,
                 "schema_fingerprint":"d52f95a6","components":[{components}]}}"#
        )
    }

    fn component(name: &str, schema: &str) -> String {
        format!(r#"{{"name":"{name}","arrow_schema_ipc_base64":"{schema}"}}"#)
    }

    #[test]
    fn parses_a_well_formed_manifest() {
        let json = manifest_json(&component("Counter", SCHEMA_B64));
        let manifest = PluginManifest::parse(json.as_bytes()).expect("parse");

        assert_eq!(manifest.name, "p");
        assert_eq!(manifest.version, "0.1.0");
        assert!(manifest.stateful);
        assert_eq!(manifest.schema_fingerprint, "d52f95a6");

        let decoded = manifest.decode_components().expect("decode");
        assert_eq!(decoded, vec![("Counter".to_string(), b"schema".to_vec())]);
    }

    #[test]
    fn rejects_an_unknown_field() {
        let json = r#"{"name":"p","version":"0.1.0","stateful":false,
                       "schema_fingerprint":"00000000","components":[],"extra":1}"#;
        let err = PluginManifest::parse(json.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(err.contains("extra"), "{err}");
    }

    #[test]
    fn rejects_invalid_utf8() {
        let err = PluginManifest::parse(&[0xff, 0xfe])
            .unwrap_err()
            .to_string();
        assert!(err.contains("UTF-8"), "{err}");
    }

    #[test]
    fn rejects_an_empty_name() {
        let json = manifest_json(&component("Counter", SCHEMA_B64))
            .replace(r#""name":"p""#, r#""name":"""#);
        let err = PluginManifest::parse(json.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(err.contains("`name` is empty"), "{err}");
    }

    #[test]
    fn rejects_a_fingerprint_that_is_not_eight_lowercase_hex() {
        for bad in ["D52F95A6", "d52f95a", "d52f95a6f", "zzzzzzzz"] {
            let json = manifest_json(&component("Counter", SCHEMA_B64)).replace("d52f95a6", bad);
            let err = PluginManifest::parse(json.as_bytes())
                .unwrap_err()
                .to_string();
            assert!(err.contains("schema_fingerprint"), "{bad}: {err}");
        }
    }

    #[test]
    fn rejects_an_empty_component_list() {
        let json = manifest_json("");
        let err = PluginManifest::parse(json.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(err.contains("no components"), "{err}");
    }

    #[test]
    fn rejects_a_duplicate_component_name() {
        let json = manifest_json(&format!(
            "{},{}",
            component("Counter", SCHEMA_B64),
            component("Counter", SCHEMA_B64)
        ));
        let err = PluginManifest::parse(json.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(err.contains("twice"), "{err}");
    }

    #[test]
    fn rejects_an_empty_component_name() {
        let json = manifest_json(&component("", SCHEMA_B64));
        let err = PluginManifest::parse(json.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(err.contains("empty name"), "{err}");
    }

    #[test]
    fn rejects_invalid_base64() {
        let json = manifest_json(&component("Counter", "not base64!"));
        let manifest = PluginManifest::parse(json.as_bytes()).expect("parse");
        let err = manifest.decode_components().unwrap_err().to_string();
        assert!(err.contains("Counter"), "{err}");
        assert!(err.contains("invalid base64"), "{err}");
    }

    #[test]
    fn rejects_an_empty_decoded_schema() {
        let json = manifest_json(&component("Counter", ""));
        let manifest = PluginManifest::parse(json.as_bytes()).expect("parse");
        let err = manifest.decode_components().unwrap_err().to_string();
        assert!(err.contains("Counter"), "{err}");
        assert!(err.contains("empty Arrow IPC schema"), "{err}");
    }
}
