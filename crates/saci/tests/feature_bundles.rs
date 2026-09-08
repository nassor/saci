//! The invariant the `all` feature bundle must hold: it reaches every feature
//! this facade declares.
//!
//! `all` is a hand-written array in `crates/saci/Cargo.toml`, and it names the
//! `connectors` and `transformers` groups instead of the ten features behind
//! them. Cargo says nothing about a feature left out of a group: a
//! `connector-x` added to this crate without touching `connectors` compiles
//! and works on its own feature, and `--features all` quietly does not carry
//! it. The same hole swallows a new capability feature that nobody adds to
//! `all` at all. This test fails on both.
//!
//! So the check expands `all` through the feature graph rather than reading its
//! array: `all` reaches `connectors`, which reaches `connector-file`. A member
//! that is not a declared feature key of this crate (`dep:saci-core`,
//! `saci-core/io`, `saci-core?/windows`) is a leaf and contributes nothing
//! further. The expansion carries a visited set, so a manifest whose groups
//! name each other cannot hang the test.
//!
//! The asserted set is every key in `[features]` minus the two bundles
//! themselves, so it needs no list to maintain and covers a capability feature
//! that does not exist yet. `default` is a subset by design, and `all` cannot
//! be asked to reach itself.
//!
//! The manifest arrives through `include_str!` and is parsed below, so this
//! file needs no TOML dependency and no feature of its own: it reads text, and
//! passes under `--no-default-features` like it does with the default feature
//! set. It is the facade's counterpart to
//! `crates/saci-service/tests/feature_bundles.rs`, which checks the host
//! binary's `all` and `default` arrays, where every feature is listed directly
//! and no group stands in between.
//!
//! The test does not assert `all`'s full contents in order. A test rewritten
//! every time a feature moves inside the array teaches nothing and gets edited
//! on autopilot.

use std::collections::BTreeSet;

/// `crates/saci/Cargo.toml` itself, read at compile time.
const MANIFEST: &str = include_str!("../Cargo.toml");

/// The two bundle keys, which are the question rather than part of the answer.
///
/// `all` cannot be required to reach itself, and `default` is deliberately a
/// subset of it: the facade defaults to `engine` alone. Every other key in
/// `[features]` is a capability `all` must carry, with no exception list.
const BUNDLES: [&str; 2] = ["default", "all"];

/// Every feature in the manifest's `[features]` section, as `(key, members)`
/// in manifest order.
///
/// Lines are classified by their first character once trimmed. A `#` line is
/// a comment, and that section is heavily commented, between its arrays as
/// well as inside them. A `"` line is one member of the array the preceding
/// key opened. Anything else holding an `=` is a new feature key, whose
/// members begin on that same line. A `]` line closes an array and carries
/// nothing. That is the whole grammar this one section uses, so nothing here
/// is a TOML parser.
///
/// # Panics
///
/// If the manifest has no `[features]` section, or a member line appears
/// before any key.
fn features() -> Vec<(&'static str, Vec<&'static str>)> {
    let mut lines = MANIFEST.lines();
    assert!(
        lines.any(|line| line.trim() == "[features]"),
        "crates/saci/Cargo.toml has no [features] section; the parser in this file expects one \
         table header on its own line",
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
                "crates/saci/Cargo.toml's [features] section declares no `{name}` feature, so the \
                 bundle this test guards does not exist"
            )
        })
}

/// Everything enabling `root` turns on, following group features transitively.
///
/// A worklist over the parsed `(key, members)` map. A name already in the
/// result is skipped, which both keeps the walk linear and makes a group cycle
/// terminate instead of hanging. A name that is not a feature key of this
/// crate (`dep:saci-core`, `saci-core/io`, `saci-core?/windows`) has no
/// members to expand, so it stays a leaf.
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

/// `all` means every capability the facade carries, so enabling it must reach
/// every other feature the crate declares.
///
/// Nothing else enforces this, and the indirection is what hides the mistake:
/// `all` names `connectors` and `transformers`, so a sixth connector left out
/// of the `connectors` group is absent from `all` with no build error anywhere
/// to say so, while the feature itself still builds and registers. A new
/// capability feature alongside `processor` and `plugin` goes missing the same
/// way, which is why the check is a set difference over every key rather than
/// a filter on the connector and transformer names.
#[test]
fn all_bundle_reaches_every_feature() {
    let features = features();

    let expected: Vec<&str> = features
        .iter()
        .map(|(key, _)| *key)
        .filter(|key| !BUNDLES.contains(key))
        .collect();
    assert!(
        !expected.is_empty(),
        "parsed no feature key besides {BUNDLES:?} out of crates/saci/Cargo.toml's [features] \
         section; this file's parser is what to fix, not the manifest",
    );

    let reached = reachable(&features, "all");
    let missing: Vec<&str> = expected
        .iter()
        .copied()
        .filter(|key| !reached.contains(key))
        .collect();
    let present = expected.len() - missing.len();
    assert!(
        missing.is_empty(),
        "crates/saci/Cargo.toml's `all` feature does not reach {missing:?}; it reaches {present} \
         of the {} features the crate declares besides `default` and `all` themselves. A \
         connector or transformer feature reaches `all` through its group, since `all` names \
         `connectors` and `transformers` rather than each feature, so the fix for one of those is \
         normally adding it to the `connectors = [...]` or `transformers = [...]` array in that \
         file rather than to `all = [...]` directly. Any other capability feature goes into \
         `all = [...]` itself.",
        expected.len(),
    );
}
