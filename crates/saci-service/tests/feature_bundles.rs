//! The three invariants the `default` and `all` feature bundles must hold.
//!
//! Both bundles are hand-written arrays in `crates/saci-service/Cargo.toml`,
//! and cargo says nothing about a feature left out of one: a connector added
//! without touching `all` compiles, registers and runs, and
//! `--features all` quietly does not carry it. Nor does anything stop a
//! connector that needs an installed broker or database from landing in
//! `default`, which would make `cargo install saci-service` produce a binary
//! that works only once a server is installed and running. The third rule
//! pushes the other way and keeps `conformance` out of `all`, since that key
//! loosens a wire-format refusal instead of adding a capability. These tests
//! fail on all three.
//!
//! The manifest arrives through `include_str!` and is parsed below, so this
//! file needs no TOML dependency, no Docker and no running service. It runs in
//! the `default` nextest profile like any other fast test, next to
//! `connector_matrix.rs`'s `dimensions_cover_the_registry`.
//!
//! `all`'s asserted set is every key in `[features]` minus `default`, `all`
//! and `conformance`, derived from the manifest rather than hand-listed, so it
//! needs no list to maintain and covers a feature that does not exist yet.

use std::collections::BTreeSet;

/// `crates/saci-service/Cargo.toml` itself, read at compile time.
const MANIFEST: &str = include_str!("../Cargo.toml");

/// The only connectors that may appear in `default`: each needs nothing
/// installed and nothing already running.
///
/// A channel lives inside the process, a file is local disk, a redb file is
/// local disk with an embedded key/value store on top of it, HTTP is a client
/// with no server of its own, TCP is a raw socket, and a `saci` peer is
/// another saci-service: nothing to install, no toolchain, and its source
/// half binds a socket the same way the TCP one does. HTTP, TCP and `saci`
/// speak the network and qualify anyway, because they are generic primitives
/// tied to no server product.
///
/// `default_bundle_requires_no_installed_service` asserts every `connector-*`
/// feature reachable from `default` is one of these, rather than that a list
/// of known-bad connectors is absent. Stated the other way round, a connector
/// needing a broker fails the moment it lands in `default`, with no
/// denylist to remember to extend; the burden is on the connector that wants
/// to be default, which is where it belongs.
const SELF_CONTAINED_CONNECTORS: [&str; 6] = [
    "connector-channel",
    "connector-file",
    "connector-http",
    "connector-redb",
    "connector-saci",
    "connector-tcp",
];

/// The two bundle keys, which are the question rather than part of the answer.
///
/// `all` cannot be required to list itself, and `default` is deliberately a
/// subset of it: every connector that needs an installed service is opt-in.
const BUNDLES: [&str; 2] = ["default", "all"];

/// The one `[features]` key `all` neither carries nor is asked to carry.
///
/// `conformance` enables `arrow-ipc/lz4`, which makes the host accept a
/// compressed record batch the wire format otherwise rejects, so it relaxes a
/// refusal rather than adding a capability. The corpus generator in
/// `examples/conformance/conformance_vectors.rs` is the only thing that needs
/// it, and it asks for it by name. Two rules hang off this one key:
/// `all_bundle_lists_every_feature` exempts it from `all`'s completeness
/// check, and `all_bundle_excludes_the_conformance_switch` fails if it ever
/// lands in `all`.
const NOT_A_CAPABILITY: &str = "conformance";

/// Every feature in the manifest's `[features]` section, as `(key, members)`
/// in manifest order.
///
/// Lines are classified by their first character once trimmed. A `#` line is
/// a comment, and that section is heavily commented, inside its arrays as well
/// as between them. A `"` line is one member of the array the preceding key
/// opened. Anything else holding an `=` is a new feature key, whose members
/// begin on that same line. A `]` line closes an array and carries nothing.
/// That is the whole grammar this one section uses, so nothing here is a TOML
/// parser.
///
/// # Panics
///
/// If the manifest has no `[features]` section, or a member line appears
/// before any key.
fn features() -> Vec<(&'static str, Vec<&'static str>)> {
    let mut lines = MANIFEST.lines();
    assert!(
        lines.any(|line| line.trim() == "[features]"),
        "crates/saci-service/Cargo.toml has no [features] section; the parser in this file \
         expects one table header on its own line",
    );

    let mut features: Vec<(&str, Vec<&str>)> = Vec::new();
    for line in lines {
        let line = line.trim();
        if line.starts_with('[') {
            break;
        }
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('"') {
            let open = features
                .last_mut()
                .expect("a feature member line follows the key whose array it belongs to");
            open.1.extend(quoted(line));
        } else if let Some((key, rest)) = line.split_once('=') {
            features.push((key.trim(), quoted(rest).collect()));
        }
    }
    features
}

/// Every double-quoted substring of `line`, in order.
fn quoted(line: &'static str) -> impl Iterator<Item = &'static str> {
    line.split('"').skip(1).step_by(2)
}

/// The members of the `name = [...]` bundle.
///
/// # Panics
///
/// If `name` is not a feature key.
fn bundle<'a>(features: &'a [(&'static str, Vec<&'static str>)], name: &str) -> &'a [&'static str] {
    features
        .iter()
        .find(|(key, _)| *key == name)
        .map(|(_, members)| members.as_slice())
        .unwrap_or_else(|| {
            panic!(
                "crates/saci-service/Cargo.toml's [features] section declares no `{name}` \
                 feature, so the bundle this test guards does not exist"
            )
        })
}

/// Everything enabling `root` turns on, following group features
/// transitively.
///
/// A worklist over the parsed `(key, members)` map. A name already in the
/// result is skipped, which both keeps the walk linear and makes a group
/// cycle terminate instead of hanging. A name that is not a feature key of
/// this crate (`dep:axum`, `saci-core/io`, `saci-core?/windows`) has no
/// members to expand, so it stays a leaf.
///
/// `default` names its connectors directly today, so the walk changes
/// nothing about what
/// `default_bundle_requires_no_installed_service` sees right now. It is what
/// keeps that rule true the day a connector arrives through an intermediate
/// feature instead: `service` and every `connector-*` key already carry
/// member lists of their own, so one added edge is all it would take.
///
/// A verbatim copy of the same walker in
/// `crates/saci/tests/feature_bundles.rs`, as `features`, `quoted` and
/// `bundle` above it already are. The two files parse different manifests
/// through their own `include_str!`, they are integration tests in two
/// separate crates, and this workspace has no test-support crate to lift
/// eighteen lines into; adding one as a published workspace member to hold
/// them is a worse trade than the copy.
fn reachable(features: &[(&'static str, Vec<&'static str>)], root: &str) -> BTreeSet<&'static str> {
    let mut reached = BTreeSet::new();
    let mut worklist: Vec<&'static str> = bundle(features, root).to_vec();
    while let Some(name) = worklist.pop() {
        if !reached.insert(name) {
            continue;
        }
        if let Some((_, members)) = features.iter().find(|(key, _)| *key == name) {
            worklist.extend(members.iter().copied());
        }
    }
    reached
}

/// `all` means every capability the crate carries, so every feature the
/// manifest declares must be in it, except the two bundle keys themselves and
/// `conformance`.
///
/// Nothing else enforces this. A tenth connector registers its factories and
/// runs from a config on its own feature, and `--features all` still builds a
/// binary that rejects it, with no build error anywhere to say so. A new
/// capability alongside `inspector`, `plugin` and `service-cluster` goes
/// missing the same way, which is why the check is a set difference over every
/// key rather than a filter on the connector and transformer names.
#[test]
fn all_bundle_lists_every_feature() {
    let features = features();
    let all = bundle(&features, "all");

    let expected: Vec<&str> = features
        .iter()
        .map(|(key, _)| *key)
        .filter(|key| !BUNDLES.contains(key) && *key != NOT_A_CAPABILITY)
        .collect();
    assert!(
        !expected.is_empty(),
        "parsed no feature key besides {BUNDLES:?} and `{NOT_A_CAPABILITY}` out of \
         crates/saci-service/Cargo.toml's [features] section; this file's parser is what to fix, \
         not the manifest",
    );

    let missing: Vec<&str> = expected
        .iter()
        .copied()
        .filter(|key| !all.contains(key))
        .collect();
    let present = expected.len() - missing.len();
    assert!(
        missing.is_empty(),
        "crates/saci-service/Cargo.toml's `all` list is missing {missing:?}; it names {present} \
         of the {} features the crate declares besides `default`, `all` and \
         `{NOT_A_CAPABILITY}`. `all` is the build that can run any config, so add each missing \
         feature to the `all = [...]` array in that file.",
        expected.len(),
    );
}

/// `all` must leave `conformance` out, because it relaxes a refusal rather
/// than adding a capability.
///
/// Its own test rather than one more assert inside
/// `all_bundle_lists_every_feature`: that rule fails when a feature is missing
/// from `all`, this one fails when a feature is present, and a reader seeing
/// which of the two went red should know which mistake was made without
/// opening either body.
#[test]
fn all_bundle_excludes_the_conformance_switch() {
    let features = features();
    let all = bundle(&features, "all");

    assert!(
        features.iter().any(|(key, _)| *key == NOT_A_CAPABILITY),
        "this test and `all_bundle_lists_every_feature` both name `{NOT_A_CAPABILITY}` exactly, \
         and crates/saci-service/Cargo.toml's [features] section no longer declares it; rename it \
         here too, or both rules stop covering that key",
    );

    assert!(
        !all.contains(&NOT_A_CAPABILITY),
        "crates/saci-service/Cargo.toml's `all` list carries `{NOT_A_CAPABILITY}`, so remove it \
         from `all = [...]`. That feature enables `arrow-ipc/lz4`, which makes the host accept a \
         compressed record batch the wire format otherwise rejects, and cargo features are \
         additive: one crate asking for `saci-service/{NOT_A_CAPABILITY}` loosens every other \
         consumer of saci-service in the same build. The corpus generator, the \
         `conformance_vectors` example, is the only thing that needs it, and it asks for it by \
         name. `--all-features` enables it whatever this list says, so this rule governs the \
         `--features all` spelling and is no guarantee that the refusal cannot be switched off.",
    );
}

/// A connector belongs in `default` only when nothing has to be installed or
/// already running for it to work.
///
/// `cargo install saci-service` must produce a binary that works on a bare
/// machine, so every connector that needs a specific broker, database or
/// object store is opt-in.
///
/// Asked as "is every connector `default` reaches self-contained", not as "is
/// each of these five known-bad connectors absent". The denylist form passed
/// for a sixth connector nobody had added to it yet, which is the one case
/// where the rule has to hold on its own; the allowlist form fails until
/// somebody justifies the newcomer by naming it in
/// [`SELF_CONTAINED_CONNECTORS`]. It also reads `default`'s transitive
/// closure rather than its literal array, so a connector arriving through
/// `service` or through another `connector-*` key counts too.
#[test]
fn default_bundle_requires_no_installed_service() {
    let features = features();

    for connector in SELF_CONTAINED_CONNECTORS {
        assert!(
            features.iter().any(|(key, _)| *key == connector),
            "this test allows `{connector}` into `default`, but \
             crates/saci-service/Cargo.toml's [features] section no longer declares it; rename \
             it here too, or the allowlist keeps blessing a feature that does not exist while \
             saying nothing about the one that replaced it",
        );
    }

    let reached = reachable(&features, "default");
    let offenders: Vec<&str> = reached
        .iter()
        .copied()
        .filter(|name| features.iter().any(|(key, _)| key == name))
        .filter(|name| name.starts_with("connector-"))
        .filter(|name| !SELF_CONTAINED_CONNECTORS.contains(name))
        .collect();
    assert!(
        offenders.is_empty(),
        "crates/saci-service/Cargo.toml's `default` bundle reaches {offenders:?}. A connector \
         may be default only when nothing has to be installed or already running for it to \
         work: `cargo install saci-service` has to produce a working binary with no broker, \
         database or object store to install and start first, and no extra build toolchain. \
         Move it out of `default = [...]` and leave it opt-in through `all` or its own \
         feature. If it genuinely needs nothing installed, add it to \
         SELF_CONTAINED_CONNECTORS in this file and say in its doc comment why it qualifies. \
         Note this reads the transitive closure, so the connector may have arrived through \
         another feature's member list rather than through `default` directly.",
    );
}
