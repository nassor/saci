// Stage 4 of the polyglot example, the Kotlin processor.
//
// `wasmWasiMain` holds the WIT bindings and three annotated declarations. Two
// published dependencies do the rest:
//
// - `io.github.nassor:saci-sdk-kt`, the authoring runtime, which also carries
//   the Arrow IPC codec. Its version is one of the manifests `cargo xtask
//   pack-sdk` holds to `packages/VERSION`.
// - `io.github.nassor:saci-sdk-kt-ksp` on `kspWasmWasi`, the symbol processor that
//   generates the row accessor and the `impl.PipelineImpl` export.
//
// Both resolve from `mavenLocal()` in an in-repo build: `cargo xtask polyglot`
// runs `gradle publishToMavenLocal` in `packages/saci-sdk-kt`, then
// `packages/saci-sdk-kt-ksp`, before this build. Resolving the real artifacts,
// Gradle module metadata plus the `wasmWasi` klib, is what proves the
// publications are consumable.
//
// The KSP configuration is `kspWasmWasi`: the name derives from the target, and a
// KSP processor runs on the JVM-hosted compiler whatever the target is, which is
// why the processor artifact is an ordinary JVM jar. KSP 2.3.11 is the newest
// release; KSP's versioning started tracking the Kotlin version at 2.3.0 and
// there is no 2.4.x line, so 2.3.11 is what pairs with Kotlin 2.4.0. See
// examples/polyglot/PINS.md.
//
// Gradle produces a core wasm module, not a component. `cargo xtask polyglot`
// runs `wit-bindgen kotlin` before this build and `wasm-tools component embed`
// plus `wasm-tools component new --adapt` after it.

@file:OptIn(ExperimentalWasmDsl::class)

import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl

plugins {
    kotlin("multiplatform") version "2.4.0"
    id("com.google.devtools.ksp") version "2.3.11"
}

repositories {
    mavenLocal()
    mavenCentral()
}

kotlin {
    wasmWasi {
        binaries.executable()
        nodejs()
    }

    sourceSets {
        val wasmWasiMain by getting {
            dependencies {
                implementation("io.github.nassor:saci-sdk-kt:0.1.0")
            }
        }
    }
}

dependencies {
    add("kspWasmWasi", "io.github.nassor:saci-sdk-kt-ksp:0.1.0")
}
