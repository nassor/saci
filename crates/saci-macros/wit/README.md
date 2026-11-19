# Vendored WIT package

`pipeline.wit` here is a byte-for-byte copy of `crates/saci/wit/pipeline.wit`, the canonical
`saci:pipeline@0.4.0` WIT package that the `saci` crate owns.

The copy exists because `#[processor]` in `../src/lib.rs` embeds the WIT text with `include_str!`
and splices it into `wit_bindgen::generate!({ inline: ... })` in the caller's crate. `cargo
package` never includes files outside the package, so a `../../saci/wit/pipeline.wit` path would
drop out of the published `saci-macros` tarball and fail to compile from it.

`wit_vendored_copy_matches_saci` in `tests/wit_vendored.rs` asserts the two files stay identical
whenever both exist in one checkout. A clone of this crate alone, or a published copy, has no
`saci` sibling, and the test skips there.

After any change to the canonical package, refresh the copy from the repository root.

Linux/macOS:

```bash
cp crates/saci/wit/pipeline.wit crates/saci-macros/wit/pipeline.wit
```

Windows (PowerShell):

```powershell
Copy-Item crates\saci\wit\pipeline.wit crates\saci-macros\wit\pipeline.wit
```
