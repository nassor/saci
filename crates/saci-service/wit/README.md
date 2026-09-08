# Vendored WIT package

`pipeline.wit` in this directory is a byte-for-byte copy of
`crates/saci-processor/wit/pipeline.wit`, the canonical `saci:pipeline@0.3.0`
WIT package `saci-processor` owns.

The copy exists because `wasmtime::component::bindgen!` in
`../src/wasm/bindings.rs` needs a WIT file inside this crate's own directory:
`cargo package`/`publish` never includes files that live outside the package
being packaged, so a `path: "../saci-processor/wit"` reference would silently
drop out of a published `saci-service` tarball and fail to compile from it.

`tests/wit_vendored.rs`'s `wit_vendored_copy_matches_saci_processor` asserts
the two files stay byte-identical whenever both are present in the same
checkout (a fresh `git clone` of just this crate, or a downloaded published
copy, has no `saci-processor` sibling to compare against, so the test skips
rather than fails there). Regenerate this copy by hand after any change to
`saci-processor`'s WIT package.
