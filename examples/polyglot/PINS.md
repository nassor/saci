# Toolchain pins, polyglot example

Six processor components, each with its own language and componentization
toolchain. These are the versions CI installs in the `Polyglot Processors` job
and the versions `cargo xtask polyglot` is written against.
`crates/saci-processor/PINS.md` covers the Rust-side and host-side pins; this
file covers everything the five non-Rust stages need.

## Required tools

| Tool | Version | Language runtime | Install |
| ---- | ------- | ---------------- | ------- |
| `cargo` (Rust stage) | n/a | Rust 1.95.0 | none: `cargo build --target wasm32-wasip2` links a component itself |
| `wasm-tools`       | 1.246.2 | n/a                       | `cargo install wasm-tools --locked --version 1.246.2` |
| Rust target        | `wasm32-wasip2` | n/a               | `rustup target add wasm32-wasip2` |
| `componentize-go`  | 0.4.1  | Go 1.25.5+ (verified on 1.26.3) | `go install github.com/bytecodealliance/componentize-go@v0.4.1` |
| `componentize-py`  | 0.25.0 | Python 3.10+ (verified on 3.14) | `pip install componentize-py==0.25.0` |
| `@bytecodealliance/jco` | 1.30.0 | Node 24.12+ (CI: 24) | `npm install -g @bytecodealliance/jco@1.30.0` |
| `typescript` | 5.9.3 | with `@types/node` 24.10.1 | `npm install --save-dev typescript@5.9.3 @types/node@24.10.1` |
| Gradle             | 8.14.4+ | JDK 21 (verified on Temurin 21.0.12) | <https://gradle.org/install/> |
| `wit-bindgen` (Kotlin fork) | branch `kotlin`, reports 0.57.1 | Kotlin 2.4.0 | `cargo install wit-bindgen-cli --git https://github.com/Kotlin/wit-bindgen --branch kotlin` |
| .NET SDK           | 10 (verified on 10.0.400) | n/a        | <https://dotnet.microsoft.com/download/dotnet/10.0> |

Kotlin itself is not installed: `examples/polyglot/stages/kotlin-fee/build.gradle.kts`
pins Kotlin 2.4.0 and Gradle fetches the compiler. The C# stage needs no
`dotnet workload`; its first build downloads wasi-sdk 29.0, about 535 MB, into
`~/.wasi-sdk/`.

Build everything with `cargo xtask polyglot`. Every tool the requested stages
need is checked before any work, so a machine missing one toolchain hears about
it in a second rather than after the other five stages have compiled. Each has
its own exit code (3 wasm-tools, 4 Go, 5 componentize-go, 6 componentize-py,
7 Node/npm, 8 no artifact, 9 Gradle, 10 wit-bindgen, 11 dotnet, 12 curl). The
Rust stage has no check of its own: it is plain cargo.

## The WASI preview 1 adapter

The Kotlin stage is the only one that needs a separate artifact:
`wasi_snapshot_preview1.reactor.wasm` from the
[wasmtime v48.0.1 release](https://github.com/bytecodealliance/wasmtime/releases/tag/v48.0.1),
matching the wasmtime the workspace pins. Kotlin's Gradle build emits a WASI
preview 1 core module, and `wasm-tools component new --adapt` needs the adapter
to turn it into a component. `cargo xtask polyglot` downloads it once into the
gitignored `examples/polyglot/generated/`; `SACI_WASI_ADAPTER` points at a
different copy.

## SIMD

Only the Rust stage carries wasm SIMD. The workspace `.cargo/config.toml` sets
`-C target-feature=+simd128` for `wasm32-wasip2`, so `settle-rs` ships `simd128`
in its core module's `target_features` section. The other five toolchains expose
no equivalent setting today:

| Stage | Toolchain | Why no SIMD knob |
| ----- | --------- | ---------------- |
| `validate-go` | componentize-go 0.4.1 | Go's compiler accepts exactly two `GOWASM` values, `satconv` and `signext`, both permanent no-ops. Any other value is rejected. |
| `enrich-py` | componentize-py 0.25.0 | Ships a prebuilt WASI CPython inside a compiled native extension. No project file, config or environment variable reaches the build. |
| `score-ts` | jco 1.30.0, componentize-js 0.22.0 | StarlingMonkey ships as a fixed prebuilt engine wasm. The upstream binary lists eight target features and `simd128` is not among them. |
| `fee-kt` | Kotlin 2.4.0 `wasmWasi` | Kotlin/Wasm targets WasmGC, which has no vector types or intrinsics to lower, and no Gradle or compiler flag exposes Binaryen's SIMD passes. |
| `tier-cs` | .NET 10, ILCompiler.LLVM 10.0.0-rc.1.26306.1 | `WasmEnableSIMD` belongs to the Mono browser-wasm pipeline and is inert here: setting it yields a byte-identical artifact. `IlcInstructionSet` reaches `ilc` but rejects a valid x64 name such as `avx2` exactly as it rejects garbage, so its instruction-set table holds no wasm entries. |

`System.Runtime.Intrinsics.Wasm.PackedSimd` exists for C#, but it is a
hand-written intrinsics API rather than a build setting, so using it means
rewriting the stage's algorithms. `tier-cs` does not use it.

## SDK packages

The five non-Rust stages consume `saci-sdk-*`, one zero-ceremony authoring SDK
per language in `packages/`, at the version in `packages/VERSION`. Each SDK also
carries the Arrow IPC codec, so there is nothing else to resolve. In-repo builds
resolve the SDK locally, not from a release:

- Go: a `replace` in `stages/go-validate/go.mod`, re-applied by the task
  after `componentize-go bindings` rewrites the file.
- Python: `packages/saci-sdk-py/src` on componentize-py's `-p` list.
- TypeScript: a `file:` link in `stages/ts-score/package.json`, so the task
  runs `npm run build` in the package first to produce `dist/`.
- Kotlin: `mavenLocal()`, so the task runs `gradle publishToMavenLocal` in
  `packages/saci-sdk-kt` and then `packages/saci-sdk-kt-ksp` before the stage
  build.
- C#: a `ProjectReference` from `tier-cs.csproj`, with the generator
  referenced by a second `ProjectReference` using `OutputItemType="Analyzer"`.

Bumping `packages/VERSION` means bumping the five manifests that carry a version
(`saci-sdk-py/pyproject.toml`, `saci-sdk-ts/package.json`,
`saci-sdk-kt/build.gradle.kts`, `saci-sdk-kt-ksp/build.gradle.kts`,
`saci-sdk-cs/Saci.Sdk.csproj`) and the Kotlin stage's
`implementation("io.github.nassor:saci-sdk-kt:...")` and
`add("kspWasmWasi", "io.github.nassor:saci-sdk-kt-ksp:...")` lines.
`cargo xtask pack-sdk` asserts they all agree.

## Why these five and not others

They are the languages with maintained, single-command or documented WASI 0.2
component toolchains. `componentize-go` is the Bytecode Alliance's current Go
recommendation; the TinyGo component page carries a "not currently being
maintained" banner pointing at it. Kotlin 2.4.0 is the first release with
Component Model support, and C# reaches it through `componentize-dotnet`. C via
`wasi-sdk` plus `wit-bindgen c` is the next cheapest addition; the wire format is
documented language-neutrally in `docs/content/library/reference/wire-format.md`, so a
seventh stage needs no host changes.

## Known caveats

### Go

- **`componentize-go` on Windows.** `go install` puts a thin wrapper on your
  PATH that downloads the real binary on first use, and it asks for
  `componentize-go-windows-amd64.tar.gz`, while the release only publishes
  `componentize-go-windows-amd64.zip`. The wrapper 404s. Workaround: download
  that `.zip` from the v0.4.1 release page and put `componentize-go.exe` on your
  PATH (overwriting the wrapper in `%GOPATH%\bin` is fine). Linux and macOS are
  unaffected, which is why CI does not need this.
- **`componentize-go bindings` owns `go.mod`.** It rewrites the file from a fixed
  template, `module wit_component` plus one `require`, every time it runs, and
  drops every other dependency. That is why
  `examples/polyglot/stages/go-validate/go.mod` is committed with that module
  name, why intra-module imports read `wit_component/<pkg>`, and why the task
  re-applies the codec `require`/`replace` with `go mod edit` between
  `bindings` and `build`. Do not "fix" it.
- **Go native tests must be scoped inside the stage.** `go test ./...` fails
  there: the generated binding packages use `//go:wasmimport`, which does not
  compile for the host target. The SDK's own tests, codec included, live in
  `packages/saci-sdk-go`, a separate module, where `go test ./...` works.

### Python

- **componentize-py's `bindings` output is IDE stubs only.** `componentize`
  regenerates the real bindings itself and never reads the files on disk; the
  build succeeds with them deleted. Generating them is worth it for type
  checking, but the step is not load-bearing.
- **componentize-py resolves imports at build time only, from `-p`.** Every
  `import` in the Python stage must be at module top level, and every directory
  it imports from must be named by a `-p` flag on the `componentize` subcommand.
  A function-local import works when you run the file with CPython and then fails
  inside the component.
- **`python -m unittest discover` breaks in the stage after the bindings step.**
  The generated `componentize_py_async_support/` package imports
  `componentize_py_runtime`, which only exists inside the component, and
  discovery imports every package it finds. The SDK's own tests, codec included,
  live in `packages/saci-sdk-py`, where discovery works.

### TypeScript

- **jco needs ES modules, a versioned import specifier, and two disable
  flags.** `"type": "module"` in `package.json`; the host-io import must be
  `'saci:pipeline/host-io@0.3.0'` (the unversioned form fails at wizer time);
  and `--disable http` must be paired with `--disable fetch-event`, or the
  component still imports `wasi:http/types` and refuses to instantiate against
  the SACI host. Do not disable `clocks`: `Date.now()` silently returns garbage
  and the stage reports timing in `run-metrics`.
- **Bundling is not optional, and a TypeScript entrypoint gets it for free.**
  StarlingMonkey's loader cannot resolve relative modules at wizer time.
  `jco componentize` bundles a `.ts` entrypoint automatically; a `.js` one
  needs an explicit `--bundle`.
- **Do not type the WIT import through tsconfig `paths`.** jco's bundler reads
  the same field, resolves `saci:pipeline/host-io@0.3.0` to the declaration file
  and fails with `[MISSING_EXPORT] "getConfig" is not exported by
  "types/interfaces/saci-pipeline-host-io.d.ts"`. The mapping belongs in an
  ambient `declare module` block, which the bundler never sees. `wit.d.ts` holds
  it.
- **Node 24.12 is the floor, set by jco rather than by TypeScript.** Type
  stripping in `node --test` lands in 22.18, but jco's `@napi-rs/lzma` declares
  `engines: ^22.20 || ^24.12 || >=25`. Below that npm silently drops the
  platform binding, because an optional dependency failing its engine check is
  skipped rather than reported, and `jco componentize` then dies on `Cannot
  find native binding`. The stage declares `"engines": { "node": ">=24.12" }`
  so an old runtime fails by name.
- **The stage imports no generated schema file.** `@nassor/saci-sdk`'s
  `component()` call declares `Order` and derives its schema fingerprint
  directly, so `score.ts` holds one import, the bare `@nassor/saci-sdk`
  specifier. No polyglot stage consumes `examples/polyglot/generated/`; the
  Quick Start and native-plugin builds are its only consumers, and `cargo run
  -p saci-service --features wasm --example polyglot_schema_emit -- emit`
  regenerates it. `erasableSyntaxOnly` applies: no enums, no namespaces, no
  parameter properties.
- **The codec compiles into the SDK's `dist/`, not as a bare specifier.** The
  absorbed codec is internal source (`src/arrow_ipc.ts`) compiled alongside
  `core.ts` and re-exported by `src/index.ts`. The merged `@nassor/saci-sdk`
  package ships an `exports` map and compiled `dist/*.js`, because jco bundles
  with Rolldown under `platform: "neutral"`, where `resolve.mainFields` is empty
  and a `main`-only package would not resolve.

### Kotlin

- **Gradle does not finish the job.** Kotlin 2.4.0's Component Model support is
  binding generation, not componentization. Three tools run around the compile:
  JetBrains' `wit-bindgen` fork writes the Kotlin bindings, Gradle produces a
  core wasm module, and `wasm-tools component embed` plus
  `wasm-tools component new --adapt` wrap it into a component. There is no
  Gradle task for any of that.
- **The binding generator is a git branch, not a release.** `wit-bindgen-cli`
  from `github.com/Kotlin/wit-bindgen` branch `kotlin` builds from source and
  reports version 0.57.1. Its output is documented as non-deterministic, so
  `src/wasmWasiMain/kotlin/bindings/` is gitignored and regenerated every build.
- **The processor object's name and package are dictated by the generator.**
  `--kotlin-imports 'impl.*'` puts `import impl.*` in the generated file, and the
  export trampoline calls `PipelineImpl.describe()` and `PipelineImpl.runBatch()`
  by those exact names.
- **KSP 2.3.11 is pinned to match Kotlin 2.4.0, and there is no 2.4.x KSP
  line.** The stage applies `id("com.google.devtools.ksp") version "2.3.11"`
  beside the Kotlin plugin. KSP's own versioning started tracking the Kotlin
  version at 2.3.0, and 2.3.11 is the newest release, so it is what pairs with
  Kotlin 2.4.0 rather than a same-numbered KSP release.
- **The KSP processor is what actually writes `impl.OrderCodec` and
  `impl.PipelineImpl`.** The stage's symbol-processing configuration is
  `kspWasmWasi`, the name Gradle derives from the `wasmWasi` target, and it
  carries `io.github.nassor:saci-sdk-kt-ksp`. Like any KSP processor it runs on
  the JVM-hosted compiler regardless of the target it is inspecting. It reads
  the stage's three annotations and generates the row accessor
  (`impl.OrderCodec`) plus the `impl.PipelineImpl` export object the previous
  caveat's trampoline resolves by name. The stage source itself is left holding
  only the annotated declarations: an `@SaciComponent` data class, an
  `@SaciTransform` function and an `@SaciProcessor` function.
- **WASI preview 1 calls trap inside the finished component.** Kotlin/Wasm's
  `wasmWasi` target reaches the outside world through preview 1 imports, and
  every one of them traps once `component new --adapt` has wrapped the module:
  `kotlin.time.TimeSource.Monotonic`, `kotlin.random.Random` and `println` are
  all unusable. A Kotlin processor may call only the imports the WIT world declares,
  which is `host-io` and nothing else. That is why the Kotlin stage reports
  `run-metrics.wall-ns` as 0 while the Go, Python and TypeScript stages report
  real timings. The failure mode is an opaque wasm trap with no log line, so
  reach for this first when a Kotlin processor dies inside `run-batch`.
- **The output needs Wasm GC, exceptions and function references.** Kotlin
  classes compile to Wasm GC types. wasmtime enables all three proposals by
  default from 47.0.0, and the workspace pins 48.0.1, so the SACI host needs no
  configuration. An older host must set `Config::wasm_gc`,
  `Config::wasm_exceptions` and `Config::wasm_function_references`.
- **The generated types are not the ones a Kotlin author would write.** A WIT
  `record` becomes a plain mutable class with `var` fields, not a data class. A
  `variant` becomes a sealed interface. `option<T>` is a nullable `T?`.
  `result<T,E>` is `kotlin.Result<T>`, and the error payload travels inside the
  generated `ComponentException`, so the failure arm is
  `Result.failure(ComponentException(Types.RunError.Permanent(message)))`.
  `list<u8>` is a boxed `kotlin.collections.List<UByte>`, lifted and lowered one
  element at a time, so the codec converts to `ByteArray` on the way in and back
  with `asUByteArray().asList()` on the way out.
- **An `internal` declaration cannot expose a private-in-file type.** The codec's
  `Batch` constructor is `internal`, so `Span` and `Field` are `internal` too.
  The compiler rejects the `private` version outright.

### C#

- **`componentize-dotnet` is a Bytecode Alliance layer, not a .NET 10 feature.**
  Microsoft's own .NET 10 release notes never mention WASI. The ILCompiler LLVM
  package is still a release candidate even though the SDK is generally
  available.
- **`nuget.config` is mandatory.** `Microsoft.DotNet.ILCompiler.LLVM` is not on
  nuget.org, so the stage ships a `nuget.config` naming the `dotnet-experimental`
  Azure DevOps feed. Without it the restore fails.
- **The host-RID compiler package is not implicit.** The LLVM ILCompiler opts
  out of the SDK's implicit host-compiler resolution, so a csproj that only
  references `BytecodeAlliance.Componentize.DotNet.Wasm.SDK` fails publish with
  "Add a PackageReference for 'runtime.win-x64.Microsoft.DotNet.ILCompiler.LLVM'
  to allow cross-compilation for wasm". The stage adds
  `runtime.$(NETCoreSdkPortableRuntimeIdentifier).Microsoft.DotNet.ILCompiler.LLVM`
  so the same csproj cross-compiles from Windows, Linux and macOS. It lives on
  the `dotnet-experimental` feed too.
- **`dotnet build` already produces a component.** The SDK runs a publish after
  the build and the NativeAOT link step embeds the component type, so there is no
  `wasm-tools component new` step. The artifact is under
  `bin/Release/net10.0/wasi-wasm/publish/`.
- **wasi-sdk auto-downloads, x86_64 only.** The first build fetches wasi-sdk
  29.0, about 535 MB, into `~/.wasi-sdk/`. The download URLs are hardcoded to
  `x86_64-{windows,linux,macos}`, so an arm64 machine cannot build this stage.
- **The generated namespace carries a package version segment.** The world
  `saci-pipeline` and the package `saci:pipeline@0.3.0` give
  `SaciPipelineWorld.wit.Exports.saci.pipeline.v0_3_0`, and the implementation
  class must be `PipelineExportsImpl` there. The shared `types` interface lands
  under `wit.Imports`, not `wit.Exports`, even though an exported function
  returns its records.
- **`result<T,E>` is an exception, not a return value.** `run-batch` returns
  `RunResult` directly and the error arm is
  `throw new WitException<RunError>(RunError.Permanent(message), 0)`.
- **The default compile glob reaches into the test and generator projects.**
  `Saci.Sdk.csproj` carries `<Compile Remove="tests/**" />` and
  `<Compile Remove="generator/**" />`, or the host-only xunit sources and the
  netstandard2.0 generator end up in the library.

## Load-bearing crate pin

`arrow-ipc = "=59.3.0"` in the workspace `Cargo.toml` is the host's Arrow IPC
implementation, and the byte layout the five `saci-sdk` packages' codecs target.
A patch bump there can change the buffer layout the Go, Python, TypeScript,
Kotlin and C# stages walk. The `polyglot_chain` integration test is the
regression gate: it asserts exact per-column values produced by all six
processors. See `crates/saci-processor/PINS.md` for the full upgrade policy.