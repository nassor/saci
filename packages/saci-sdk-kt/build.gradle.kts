// The authoring runtime for Kotlin SACI processors, published for two targets.
//
// `commonMain` holds everything: the annotations a stage author writes, the
// host-capability facade, the Arrow IPC codec, and the batch runner that
// decodes, applies transforms and re-encodes. The source that runs inside a
// `wasmWasi` component is byte for byte the source `jvmTest` exercises against
// real wire bytes on the host.
//
// Nothing here references the `bindings` package. Those files are generated per
// stage by `wit-bindgen kotlin` and cannot be a library dependency, so the
// runtime reaches the host through [SaciHost] and the KSP-generated glue in the
// stage implements it over `bindings.HostIo`.
//
// The compile-time half lives in `packages/saci-sdk-kt-ksp`, a JVM-only module:
// KSP processors always run on the JVM-hosted compiler, whatever the target is.

@file:OptIn(ExperimentalWasmDsl::class)

import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl

plugins {
    kotlin("multiplatform") version "2.4.0"
    `maven-publish`
}

group = "io.github.nassor"
version = "0.1.0"

repositories {
    mavenLocal()
    mavenCentral()
}

kotlin {
    jvm()

    // A library, so no `binaries.executable()`. `nodejs()` only names the
    // environment the Kotlin plugin requires for a `wasmWasi` target.
    wasmWasi {
        nodejs()
    }

    sourceSets {
        val jvmTest by getting {
            dependencies {
                implementation(kotlin("test"))
            }
        }
    }
}

publishing {
    publications.withType<MavenPublication>().configureEach {
        pom {
            name.set("saci-sdk-kt")
            description.set(
                "Authoring runtime for SACI WebAssembly processors written in Kotlin, " +
                    "for wasmWasi and the JVM."
            )
            url.set("https://github.com/nassor/saci")
            licenses {
                license {
                    name.set("Apache-2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0")
                }
            }
        }
    }
    repositories {
        maven {
            // The Pages-served Maven repository, as in `packages/saci-sdk-kt-ksp`.
            name = "pages"
            url = uri(layout.projectDirectory.dir("../../docs/static/maven"))
        }
    }
}
