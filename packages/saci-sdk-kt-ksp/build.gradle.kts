// The compile-time half of the Kotlin SACI SDK: the KSP symbol processor that
// turns `@SaciComponent`, `@SaciTransform` and `@SaciProcessor` into the row
// accessor and the `impl.PipelineImpl` export glue.
//
// Plain `kotlin("jvm")`, not multiplatform, and that is not a simplification. A
// KSP processor is loaded by the JVM-hosted Kotlin compiler whatever the target
// of the compilation it is inspecting, so the artifact a stage puts on its
// `kspWasmWasi` configuration is an ordinary JVM jar.
//
// KSP 2.3.11 against Kotlin 2.4.0: KSP's own versioning switched to tracking the
// Kotlin version at 2.3.0, and 2.3.11 is the newest release. A spike confirmed it
// runs for a `wasmWasi` target under the Kotlin 2.4.0 plugin, resolves
// annotations out of a klib dependency, and puts its output on the
// `wasmWasiMain` compilation. See `examples/polyglot/PINS.md`.

plugins {
    kotlin("jvm") version "2.4.0"
    `maven-publish`
}

group = "io.github.nassor"
version = "0.1.0"

kotlin {
    jvmToolchain(21)
}

repositories {
    // `saci-sdk-kt` is consumed as a real published artifact: `cargo xtask
    // polyglot` runs `publishToMavenLocal` there before this build.
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("com.google.devtools.ksp:symbol-processing-api:2.3.11")

    // For `saciWireName`. The property-name to wire-name rule has one definition,
    // and the module that documents and tests it is the runtime.
    implementation("io.github.nassor:saci-sdk-kt:0.1.0")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("saci-sdk-kt-ksp")
                description.set(
                    "KSP symbol processor that generates the export glue for SACI " +
                        "WebAssembly processors written in Kotlin."
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
    }
    repositories {
        maven {
            // The Pages-served Maven repository, as in `packages/saci-sdk-kt`.
            name = "pages"
            url = uri(layout.projectDirectory.dir("../../docs/static/maven"))
        }
    }
}
