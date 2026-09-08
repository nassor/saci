//! `crates/saci-service/wit/pipeline.wit` is a vendored, byte-for-byte copy of
//! `crates/saci-processor/wit/pipeline.wit`, the canonical WIT package. See
//! `crates/saci-service/wit/README.md` for why the copy exists rather than a
//! `bindgen!` reference across the crate boundary.
//!
//! This only runs inside the monorepo checkout, where the sibling crate is
//! reachable: a fresh clone of just `saci-service`, or a copy built from a
//! published crates.io tarball, has no `saci-processor` directory to compare
//! against, so the test prints `SKIP:` and passes rather than failing on an
//! absent, unrelated crate.

use std::path::Path;

#[test]
fn wit_vendored_copy_matches_saci_processor() {
    let vendored = Path::new(env!("CARGO_MANIFEST_DIR")).join("wit/pipeline.wit");
    let canonical =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../saci-processor/wit/pipeline.wit");

    let Ok(canonical_bytes) = std::fs::read(&canonical) else {
        println!(
            "SKIP: {} not found (not a monorepo checkout)",
            canonical.display()
        );
        return;
    };
    let vendored_bytes =
        std::fs::read(&vendored).expect("crates/saci-service/wit/pipeline.wit must exist");

    assert_eq!(
        vendored_bytes, canonical_bytes,
        "crates/saci-service/wit/pipeline.wit has drifted from crates/saci-processor/wit/pipeline.wit; \
         re-copy the canonical file (see crates/saci-service/wit/README.md)"
    );
}
