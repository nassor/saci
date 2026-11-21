//! The `window` block on a processor node.

use serde::{Deserialize, Serialize};

use crate::error::{SaciError, SaciResult};

/// A windowing declaration on a processor node: which windows the host
/// assumes the processor's merging logic works in, and what it tracks for
/// them.
///
/// The geometry lives on the node itself, tagged by `kind`, because a KDL
/// property table is a flat map and the spec's own `kind` tag is one key of
/// it. `key_fields` is a child node: a KDL property cannot repeat, and a child
/// with one argument is a scalar while one with several is an array, which is
/// exactly the one-or-many shape a key list needs.
///
/// ```kdl
/// wasm "windowed" path="pipelines/windowed.wasm" {
///     window kind="tumbling" size_ms=30000 offset_ms=0
///            time_field="timestamp_ms" allowed_lateness_ms=5000 {
///         key_fields "category"
///     }
/// }
/// ```
///
/// The host uses the declaration to validate the node's inbound streams
/// (every delivered component must carry `time_field`), to track the
/// node's event-time watermark across batches, and to describe the node in
/// the dashboard. The processor itself implements the merging logic and
/// reads the same geometry back through `get-config` under the `window.*`
/// keys, which the builder injects into the node's config table; the two
/// sides of the contract cannot drift because there is only one source of
/// truth.
///
/// Declared in every build, like the `wasm` and `plugin` node kinds that
/// carry it. The geometry is
/// [`crate::window::WindowSpec`], which sits outside the engine's
/// `windows` feature for exactly this reason, so a binary built without the
/// windowing engine still parses the block and
/// [`validate_build_capabilities`](crate::service::validation::validate_build_capabilities)
/// names the `--features windows` flag that would serve it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WindowConfig {
    /// The window geometry (tumbling, sliding or session).
    #[serde(flatten)]
    pub spec: crate::window::WindowSpec,
    /// The event-time column every component delivered to this processor
    /// must carry. Supports `Int64` milliseconds and the Arrow timestamp
    /// types.
    pub time_field: String,
    /// Grouping key columns; empty for a global window.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub key_fields: Vec<String>,
    /// How many milliseconds past the watermark a late row is still accepted
    /// by the processor's windowing logic. Defaults to 0.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub allowed_lateness_ms: i64,
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

impl WindowConfig {
    /// A declaration of `spec` over `time_field`, with no key columns (a
    /// global window) and no allowed lateness.
    ///
    /// Set [`key_fields`](Self::key_fields) and
    /// [`allowed_lateness_ms`](Self::allowed_lateness_ms) directly.
    pub fn new(spec: crate::window::WindowSpec, time_field: impl Into<String>) -> Self {
        Self {
            spec,
            time_field: time_field.into(),
            key_fields: Vec::new(),
            allowed_lateness_ms: 0,
        }
    }

    /// Whether the declaration is geometrically sane and names a non-empty
    /// time field with a non-negative lateness budget.
    pub fn validate(&self) -> SaciResult<()> {
        self.spec.validate()?;
        if self.time_field.is_empty() {
            return Err(SaciError::configuration(
                "window time_field must not be empty",
            ));
        }
        if self.allowed_lateness_ms < 0 {
            return Err(SaciError::configuration(format!(
                "window allowed_lateness_ms must be >= 0, got {}",
                self.allowed_lateness_ms
            )));
        }
        Ok(())
    }

    /// The declaration as `window.*` config keys, for injection into the
    /// processor node's `config` table so `get-config` answers them.
    pub fn config_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::with_capacity(6);
        let (kind, size, slide, offset, gap) = match &self.spec {
            crate::window::WindowSpec::Tumbling { size_ms, offset_ms } => {
                ("tumbling", Some(*size_ms), None, Some(*offset_ms), None)
            }
            crate::window::WindowSpec::Sliding {
                size_ms,
                slide_ms,
                offset_ms,
            } => (
                "sliding",
                Some(*size_ms),
                Some(*slide_ms),
                Some(*offset_ms),
                None,
            ),
            crate::window::WindowSpec::Session { gap_ms } => {
                ("session", None, None, None, Some(*gap_ms))
            }
        };
        pairs.push(("window.kind".to_string(), kind.to_string()));
        if let Some(size) = size {
            pairs.push(("window.size_ms".to_string(), size.to_string()));
        }
        if let Some(slide) = slide {
            pairs.push(("window.slide_ms".to_string(), slide.to_string()));
        }
        if let Some(offset) = offset {
            pairs.push(("window.offset_ms".to_string(), offset.to_string()));
        }
        if let Some(gap) = gap {
            pairs.push(("window.gap_ms".to_string(), gap.to_string()));
        }
        pairs.push(("window.time_field".to_string(), self.time_field.clone()));
        pairs.push(("window.key_fields".to_string(), self.key_fields.join(",")));
        pairs.push((
            "window.allowed_lateness_ms".to_string(),
            self.allowed_lateness_ms.to_string(),
        ));
        pairs
    }
}

impl<'de> Deserialize<'de> for WindowConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{Error as _, MapAccess, Visitor};

        struct WindowConfigVisitor;

        impl<'de> Visitor<'de> for WindowConfigVisitor {
            type Value = WindowConfig;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(
                    "a window node: kind, geometry, time_field, optional key_fields and \
                     allowed_lateness_ms",
                )
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                const KNOWN: &[&str] = &[
                    "kind",
                    "size_ms",
                    "slide_ms",
                    "offset_ms",
                    "gap_ms",
                    "time_field",
                    "allowed_lateness_ms",
                    "key_fields",
                ];
                let mut kind: Option<String> = None;
                let mut size_ms: Option<i64> = None;
                let mut slide_ms: Option<i64> = None;
                let mut offset_ms: Option<i64> = None;
                let mut gap_ms: Option<i64> = None;
                let mut time_field: Option<String> = None;
                let mut allowed_lateness_ms: Option<i64> = None;
                let mut key_fields: Vec<String> = Vec::new();

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "kind" => kind = Some(map.next_value()?),
                        "size_ms" => size_ms = Some(map.next_value()?),
                        "slide_ms" => slide_ms = Some(map.next_value()?),
                        "offset_ms" => offset_ms = Some(map.next_value()?),
                        "gap_ms" => gap_ms = Some(map.next_value()?),
                        "time_field" => time_field = Some(map.next_value()?),
                        "allowed_lateness_ms" => allowed_lateness_ms = Some(map.next_value()?),
                        "key_fields" => {
                            #[derive(Deserialize)]
                            #[serde(untagged)]
                            enum OneOrMany {
                                One(String),
                                Many(Vec<String>),
                            }
                            key_fields = match map.next_value::<OneOrMany>()? {
                                OneOrMany::One(one) => vec![one],
                                OneOrMany::Many(many) => many,
                            };
                        }
                        other => return Err(A::Error::unknown_field(other, KNOWN)),
                    }
                }

                let kind = kind.ok_or_else(|| A::Error::missing_field("kind"))?;
                let spec = match kind.as_str() {
                    "tumbling" => {
                        if slide_ms.is_some() {
                            return Err(A::Error::custom(
                                "slide_ms is only valid on a sliding window",
                            ));
                        }
                        if gap_ms.is_some() {
                            return Err(A::Error::custom(
                                "gap_ms is only valid on a session window",
                            ));
                        }
                        crate::window::WindowSpec::Tumbling {
                            size_ms: size_ms.ok_or_else(|| A::Error::missing_field("size_ms"))?,
                            offset_ms: offset_ms.unwrap_or(0),
                        }
                    }
                    "sliding" => {
                        if gap_ms.is_some() {
                            return Err(A::Error::custom(
                                "gap_ms is only valid on a session window",
                            ));
                        }
                        crate::window::WindowSpec::Sliding {
                            size_ms: size_ms.ok_or_else(|| A::Error::missing_field("size_ms"))?,
                            slide_ms: slide_ms
                                .ok_or_else(|| A::Error::missing_field("slide_ms"))?,
                            offset_ms: offset_ms.unwrap_or(0),
                        }
                    }
                    "session" => {
                        if size_ms.is_some() {
                            return Err(A::Error::custom(
                                "size_ms is only valid on a tumbling or sliding window",
                            ));
                        }
                        if slide_ms.is_some() {
                            return Err(A::Error::custom(
                                "slide_ms is only valid on a sliding window",
                            ));
                        }
                        if offset_ms.is_some() {
                            return Err(A::Error::custom(
                                "offset_ms is only valid on a tumbling or sliding window",
                            ));
                        }
                        crate::window::WindowSpec::Session {
                            gap_ms: gap_ms.ok_or_else(|| A::Error::missing_field("gap_ms"))?,
                        }
                    }
                    other => {
                        return Err(A::Error::unknown_variant(
                            other,
                            &["tumbling", "sliding", "session"],
                        ));
                    }
                };

                Ok(WindowConfig {
                    spec,
                    time_field: time_field.ok_or_else(|| A::Error::missing_field("time_field"))?,
                    key_fields,
                    allowed_lateness_ms: allowed_lateness_ms.unwrap_or(0),
                })
            }
        }

        deserializer.deserialize_map(WindowConfigVisitor)
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::service::config::test_support::{parse, workflow_kdl};

    // ── Windowing: the `window` block on processor nodes ────────────────────

    #[test]
    fn window_block_parses_with_one_and_many_key_fields() {
        let raw = workflow_kdl(
            r#"
wasm "p" path="p.wasm" {
    window kind="tumbling" size_ms=30000 offset_ms=500 time_field="timestamp_ms" allowed_lateness_ms=5000 {
        key_fields "category"
        key_fields "region"
    }
}
"#,
        );
        let cfg = parse(&raw).expect("parse");
        let window = cfg.workflows[0].wasm[0]
            .window
            .as_ref()
            .expect("window block");
        assert_eq!(
            window.spec,
            crate::window::WindowSpec::tumbling(30_000).with_offset(500)
        );
        assert_eq!(window.time_field, "timestamp_ms");
        assert_eq!(window.key_fields, vec!["category", "region"]);
        assert_eq!(window.allowed_lateness_ms, 5_000);
        cfg.validate().expect("a sane window block validates");

        // A single key_fields child is a scalar, not an array: the one-or-many
        // deserializer must accept both shapes.
        let raw = workflow_kdl(
            r#"
wasm "p" path="p.wasm" {
    window kind="session" gap_ms=10000 time_field="ts" {
        key_fields "category"
    }
}
"#,
        );
        let cfg = parse(&raw).expect("parse");
        let window = cfg.workflows[0].wasm[0]
            .window
            .as_ref()
            .expect("window block");
        assert_eq!(window.spec, crate::window::WindowSpec::session(10_000));
        assert_eq!(window.key_fields, vec!["category"]);
        assert_eq!(window.allowed_lateness_ms, 0, "lateness defaults to zero");
    }

    #[test]
    fn window_unknown_key_is_rejected() {
        let raw = workflow_kdl(
            r#"
wasm "p" path="p.wasm" {
    window kind="tumbling" size_ms=1000 time_field="ts" bogus=1
}
"#,
        );
        let err = parse(&raw).expect_err("an unknown window key must not parse");
        assert!(err.contains("bogus"), "got: {err}");
    }

    #[test]
    fn window_unknown_kind_is_rejected() {
        let raw = workflow_kdl(
            r#"
wasm "p" path="p.wasm" {
    window kind="hourly" size_ms=1000 time_field="ts"
}
"#,
        );
        let err = parse(&raw).expect_err("an unknown window kind must not parse");
        assert!(err.contains("hourly"), "got: {err}");
    }

    #[test]
    fn window_missing_time_field_is_rejected() {
        let raw = workflow_kdl(
            r#"
wasm "p" path="p.wasm" {
    window kind="tumbling" size_ms=1000
}
"#,
        );
        let err = parse(&raw).expect_err("time_field is mandatory");
        assert!(err.contains("time_field"), "got: {err}");
    }

    #[test]
    fn window_geometry_key_wrong_for_kind_is_rejected() {
        let raw = workflow_kdl(
            r#"
wasm "p" path="p.wasm" {
    window kind="session" gap_ms=1000 size_ms=500 time_field="ts"
}
"#,
        );
        let err = parse(&raw).expect_err("size_ms is not a session geometry");
        assert!(err.contains("size_ms"), "got: {err}");
    }

    #[test]
    fn window_invalid_geometry_is_rejected_at_validate() {
        let raw = workflow_kdl(
            r#"
wasm "p" path="p.wasm" {
    window kind="sliding" size_ms=1000 slide_ms=2000 time_field="ts"
}
"#,
        );
        let cfg = parse(&raw).expect("parse");
        let err = cfg
            .validate()
            .expect_err("slide > size is nonsense and must be rejected")
            .to_string();
        assert!(err.contains("window is invalid"), "got: {err}");
        assert!(err.contains("slide_ms"), "got: {err}");
    }

    #[test]
    fn window_config_pairs_cover_the_geometry() {
        let window = WindowConfig {
            spec: crate::window::WindowSpec::sliding(60_000, 10_000).with_offset(500),
            time_field: "timestamp_ms".to_string(),
            key_fields: vec!["category".to_string(), "region".to_string()],
            allowed_lateness_ms: 5_000,
        };
        let pairs = window.config_pairs();
        let get = |key: &str| {
            pairs
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| panic!("missing {key}"))
        };
        assert_eq!(get("window.kind"), "sliding");
        assert_eq!(get("window.size_ms"), "60000");
        assert_eq!(get("window.slide_ms"), "10000");
        assert_eq!(get("window.offset_ms"), "500");
        assert_eq!(get("window.time_field"), "timestamp_ms");
        assert_eq!(get("window.key_fields"), "category,region");
        assert_eq!(get("window.allowed_lateness_ms"), "5000");

        // The injected keys must not clobber a key the operator already wrote.
        let mut spec = std::collections::HashMap::new();
        spec.insert("window.kind".to_string(), "custom".to_string());
        for (key, value) in pairs {
            spec.entry(key).or_insert(value);
        }
        assert_eq!(spec.get("window.kind").map(String::as_str), Some("custom"));
    }

    #[test]
    fn window_block_is_rejected_in_cluster_mode() {
        let raw = r#"
mode "cluster"

node id=1 data_dir="/tmp/saci"

cluster {
    bootstrap #true
    peer id=1 addr="127.0.0.1:9000"
}

workflow "w" {
    wasm "p" path="p.wasm" {
        window kind="tumbling" size_ms=1000 time_field="ts"
    }
}
"#;
        let cfg = parse(raw).expect("parse");
        let err = cfg
            .validate()
            .expect_err("cluster mode cannot honour a window block");
        assert!(err.to_string().contains("window"), "got: {err}");
    }
}
