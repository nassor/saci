# saci-sdk

One package per language: the processor authoring package for that language,
with the Arrow IPC codec inside it. The codec reads and mutates the SACI host
to processor wire format using only the language's standard library, so a
processor decodes a batch without an Arrow dependency that has to survive its
componentizer. The codec stays internal to the SDK, so each package keeps its
original module and namespace name.

The format itself is specified in
[the wire format reference](https://nassor.github.io/saci/library/reference/wire-format/).
A sixth language can reimplement it from there.

## Coordinates

All five ship in lockstep at the version in `VERSION`: one wire format, one
version. The Kotlin KSP symbol processor (`saci-sdk-kt-ksp`) and the C# source
generator (`Saci.Sdk.Generators`) run at build time only. The generator is
packed inside `Saci.Sdk`, and the KSP processor publishes into the same Maven
repository as `saci-sdk-kt`.

| Language | Directory | Coordinate | Codec import |
|---|---|---|---|
| Go | `saci-sdk-go` | `github.com/nassor/saci/packages/saci-sdk-go` | subpackage `arrowipc` |
| Python | `saci-sdk-py` | `saci-sdk` | `saci_sdk.arrow_ipc` |
| TypeScript | `saci-sdk-ts` | `@nassor/saci-sdk` | `./arrow_ipc.ts` (internal) |
| Kotlin | `saci-sdk-kt` (+ KSP `saci-sdk-kt-ksp`) | `io.github.nassor:saci-sdk-kt` | `io.github.nassor.saci.arrowipc` |
| C# | `saci-sdk-cs` (+ generator `Saci.Sdk.Generators`) | `Saci.Sdk` | `Saci.ArrowIpc` |

## Install

Everything but Go is a GitHub Release asset of the `sdk-v0.1.0` tag. Go
resolves through the module proxy from the `packages/saci-sdk-go/v0.1.0` tag.
Neither tag exists on GitHub yet and no release has been cut, so these commands
fail until the Release procedure below runs. `cargo xtask pack-sdk` builds every
artifact into `target/arrow-ipc-dist/`, and the commands show the names and
coordinates the release will serve.

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
# Go
go get github.com/nassor/saci/packages/saci-sdk-go@v0.1.0

# Python
pip install saci_sdk-0.1.0-py3-none-any.whl

# TypeScript
npm install ./nassor-saci-sdk-0.1.0.tgz

# C#
dotnet nuget add source <download-dir> -n saci-local
dotnet add package Saci.Sdk --version 0.1.0
```

Kotlin resolves from a static Maven repository served by the docs site, with
the KSP processor alongside the runtime:

```kotlin
repositories {
    maven("https://nassor.github.io/saci/maven")
    mavenCentral()
}

dependencies {
    implementation("io.github.nassor:saci-sdk-kt:0.1.0")
    add("kspWasmWasi", "io.github.nassor:saci-sdk-kt-ksp:0.1.0")
}
```

## Tests

Every suite reads `examples/polyglot/generated/`, so run the emitter first. It
runs the same on Linux, macOS and Windows (PowerShell), from the repository
root:

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
cargo run -p saci-service --features wasm --example polyglot_schema_emit -- emit
```

The five suites differ per shell. Linux/macOS:

```bash
cd packages/saci-sdk-go && go test ./...
cd packages/saci-sdk-py && PYTHONPATH=src python -m unittest discover -s tests
cd packages/saci-sdk-ts && npm ci && npm run typecheck && npm run build && npm test
cd packages/saci-sdk-kt && gradle jvmTest
cd packages/saci-sdk-cs && dotnet test tests
```

Windows (PowerShell):

```powershell
cd packages\saci-sdk-go; go test ./...
$env:PYTHONPATH = "src"; cd packages\saci-sdk-py; python -m unittest discover -s tests
cd packages\saci-sdk-ts; npm ci; npm run typecheck; npm run build; npm test
cd packages\saci-sdk-kt; gradle jvmTest
cd packages\saci-sdk-cs; dotnet test tests
```

Each suite covers the SDK and its codec, including the shared conformance
corpus at `packages/arrow-ipc-conformance/`.

## Release

1. Bump `VERSION`, the five manifests that carry a version
   (`saci-sdk-py/pyproject.toml`, `saci-sdk-ts/package.json`,
   `saci-sdk-kt/build.gradle.kts`, `saci-sdk-kt-ksp/build.gradle.kts`,
   `saci-sdk-cs/Saci.Sdk.csproj`), the KSP processor's runtime dependency
   `implementation("io.github.nassor:saci-sdk-kt:...")` in
   `saci-sdk-kt-ksp/build.gradle.kts`, and the Kotlin stage's
   `implementation("io.github.nassor:saci-sdk-kt:...")` and
   `add("kspWasmWasi", "io.github.nassor:saci-sdk-kt-ksp:...")` lines in
   `examples/polyglot/stages/kotlin-fee/build.gradle.kts`. Go carries no manifest
   version: its version is the git tag.
2. `cargo xtask pack-sdk`. It asserts the version declarations agree with
   `VERSION`, builds every artifact into `target/arrow-ipc-dist/`, and writes the
   Kotlin publications into `docs/static/maven/`.
3. Commit, including `docs/static/maven/**`, and push to `main`. That is what
   makes the Maven repository live: `docs.yml` redeploys the Pages site and Zola
   copies `docs/static/` verbatim.
4. Tag `sdk-v<version>` and push the tag. `release-sdk.yml` packs again, creates
   the release with the assets, and pushes the `packages/saci-sdk-go/v<version>`
   tag the Go module proxy serves.

## License

This subtree is Apache-2.0, per `LICENSE-APACHE`. The rest of the repository, the
engine crates included, is AGPL-3.0-only.
