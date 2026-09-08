// Generates everything a Kotlin SACI processor would otherwise hand-write.
//
// From a `@SaciComponent` data class, a `@SaciProcessor` builder and the
// `@SaciTransform` functions it registers, this emits two files:
//
//   <pkg>/Saci<Name>Codec.kt   the row accessor: field specs, a by-name row view,
//                             and the typed decode and encode the runner uses
//   impl/SaciPipelineImpl.kt   `object PipelineImpl : Pipeline`, the WIT export
//
// It has to be generated rather than reflected. Kotlin/Wasm has no reflection at
// all — `kotlin-reflect` is JVM only — so a property name exists in a `wasmWasi`
// binary only if the build wrote it there.
//
// Package `impl` is not a choice. `cargo xtask polyglot` runs `wit-bindgen kotlin
// --kotlin-imports 'impl.*'`, and the generated export trampoline resolves
// `PipelineImpl.describe()` and `PipelineImpl.runBatch()` from that package by
// those exact names. A processor annotated in another package is refused here
// rather than left to fail as an unresolved reference inside generated bindings.

package io.github.nassor.saci.sdk.ksp

import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.processing.SymbolProcessorProvider
import com.google.devtools.ksp.symbol.ClassKind
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSAnnotation
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFile
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import com.google.devtools.ksp.symbol.KSValueParameter
import com.google.devtools.ksp.symbol.Modifier
import io.github.nassor.saci.sdk.saciWireName

private const val SDK = "io.github.nassor.saci.sdk"
private const val ARROW = "io.github.nassor.saci.arrowipc"

private const val COMPONENT_ANNOTATION = "$SDK.SaciComponent"
private const val TRANSFORM_ANNOTATION = "$SDK.SaciTransform"
private const val PROCESSOR_ANNOTATION = "$SDK.SaciProcessor"

private const val PIPELINE_TYPE = "$SDK.SaciPipeline"
private const val CONFIG_TYPE = "$SDK.SaciConfig"

/** The package `wit-bindgen kotlin --kotlin-imports 'impl.*'` resolves exports from. */
private const val EXPORT_PACKAGE = "impl"

/** One Arrow-typed field, resolved from a constructor parameter. */
private class Field(
    /** The Kotlin property name. */
    val property: String,
    /** The wire field name, `saciWireName` of [property]. */
    val wire: String,
    val kind: Kind,
    /** False for a `val`, which makes the field a processor input. */
    val mutable: Boolean,
)

/**
 * The four property types a component may declare, and how each one reaches the
 * wire.
 *
 * [reader] and [array] are what keep the generated hot path free of boxing: a
 * column of `n` rows moves through one primitive array in each direction.
 */
private enum class Kind(
    val kotlinType: String,
    /** The `SaciType` entry. */
    val arrow: String,
    /** The `Column` subclass. */
    val column: String,
    /** The `Batch` accessor that decodes the column. */
    val reader: String,
    /** The array constructor that encodes it, or null for `Array`. */
    val array: String?,
) {
    LONG("kotlin.Long", "INT64", "Int64Column", "int64s", "LongArray"),
    DOUBLE("kotlin.Double", "FLOAT64", "Float64Column", "float64s", "DoubleArray"),
    BOOLEAN("kotlin.Boolean", "BOOL", "BoolColumn", "bools", "BooleanArray"),
    STRING("kotlin.String", "UTF8", "Utf8Column", "strings", null),
    ;

    companion object {
        fun of(qualified: String): Kind? = entries.firstOrNull { it.kotlinType == qualified }
    }
}

class SaciSymbolProcessor(
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger,
) : SymbolProcessor {
    private var done = false

    override fun process(resolver: Resolver): List<KSAnnotated> {
        if (done) return emptyList()
        done = true

        val components = resolver.getSymbolsWithAnnotation(COMPONENT_ANNOTATION)
            .filterIsInstance<KSClassDeclaration>()
            .toList()
        val processors = resolver.getSymbolsWithAnnotation(PROCESSOR_ANNOTATION)
            .filterIsInstance<KSFunctionDeclaration>()
            .toList()
        val transforms = resolver.getSymbolsWithAnnotation(TRANSFORM_ANNOTATION)
            .filterIsInstance<KSFunctionDeclaration>()
            .toList()

        // A compilation with none of the three is a compilation that is not a SACI
        // processor, which is the normal state of, say, a test source set.
        if (components.isEmpty() && processors.isEmpty() && transforms.isEmpty()) {
            return emptyList()
        }

        val component = single(components, "@SaciComponent class") ?: return emptyList()
        val processor = single(processors, "@SaciProcessor function") ?: return emptyList()

        val fields = fieldsOf(component) ?: return emptyList()
        if (!checkProcessor(processor)) return emptyList()
        for (transform in transforms) checkTransform(transform, component)

        val sources = listOfNotNull(component.containingFile, processor.containingFile)
        emitCodec(component, fields, sources)
        emitGlue(component, processor, sources)
        return emptyList()
    }

    // -----------------------------------------------------------------------
    // Validation.
    // -----------------------------------------------------------------------

    private fun <T : KSAnnotated> single(found: List<T>, what: String): T? = when (found.size) {
        1 -> found[0]
        0 -> {
            logger.error("a SACI processor needs exactly one $what; this compilation has none")
            null
        }

        else -> {
            logger.error(
                "a SACI processor needs exactly one $what; this compilation has ${found.size}. " +
                    "One transform signature means one row type, so one component per processor.",
                found[0],
            )
            null
        }
    }

    /**
     * Reads the component's fields off its primary constructor.
     *
     * The constructor rather than the property list, because that is both the
     * declaration order the wire contract is defined in and the order the
     * generated `decode` has to pass arguments in.
     */
    private fun fieldsOf(component: KSClassDeclaration): List<Field>? {
        if (component.classKind != ClassKind.CLASS || Modifier.DATA !in component.modifiers) {
            logger.error(
                "@SaciComponent ${component.simpleName.asString()} must be a data class: the " +
                    "generated decode calls its constructor and the generated accessor reads " +
                    "its properties",
                component,
            )
            return null
        }

        val parameters = component.primaryConstructor?.parameters
        if (parameters.isNullOrEmpty()) {
            logger.error(
                "@SaciComponent ${component.simpleName.asString()} declares no constructor " +
                    "parameters, so it has no fields",
                component,
            )
            return null
        }

        var ok = true
        val fields = ArrayList<Field>(parameters.size)
        for (parameter in parameters) {
            val field = fieldOf(component, parameter)
            if (field == null) ok = false else fields.add(field)
        }
        if (!ok) return null

        val seen = HashSet<String>()
        for (field in fields) {
            if (!seen.add(field.wire)) {
                logger.error(
                    "@SaciComponent ${component.simpleName.asString()} maps two properties to the " +
                        "wire field \"${field.wire}\"",
                    component,
                )
                ok = false
            }
        }
        return if (ok) fields else null
    }

    private fun fieldOf(component: KSClassDeclaration, parameter: KSValueParameter): Field? {
        val name = parameter.name?.asString()
        val owner = component.simpleName.asString()
        if (name == null) {
            logger.error("@SaciComponent $owner has an unnamed constructor parameter", component)
            return null
        }
        if (!parameter.isVal && !parameter.isVar) {
            logger.error(
                "@SaciComponent $owner parameter \"$name\" is neither val nor var, so it is not a " +
                    "field: a data class field has to be a property",
                parameter,
            )
            return null
        }

        val type = parameter.type.resolve()
        val qualified = type.declaration.qualifiedName?.asString()
        val kind = qualified?.let(Kind::of)
        if (kind == null || type.isMarkedNullable) {
            logger.error(
                "@SaciComponent $owner field \"$name\" is ${qualified ?: "an unresolved type"}" +
                    "${if (type.isMarkedNullable) "?" else ""}. A SACI column is non-nullable and " +
                    "one of Long, Double, Boolean or String.",
                parameter,
            )
            return null
        }

        return Field(name, saciWireName(name), kind, parameter.isVar)
    }

    private fun checkProcessor(processor: KSFunctionDeclaration): Boolean {
        var ok = true
        val name = processor.simpleName.asString()
        if (processor.packageName.asString() != EXPORT_PACKAGE) {
            logger.error(
                "@SaciProcessor $name is in package ${processor.packageName.asString()}, but the " +
                    "export trampoline `wit-bindgen kotlin --kotlin-imports '$EXPORT_PACKAGE.*'` " +
                    "resolves PipelineImpl from package $EXPORT_PACKAGE. Move it there.",
                processor,
            )
            ok = false
        }
        if (processor.parameters.isNotEmpty()) {
            logger.error("@SaciProcessor $name must take no parameters", processor)
            ok = false
        }
        val returns = processor.returnType?.resolve()?.declaration?.qualifiedName?.asString()
        if (returns != PIPELINE_TYPE) {
            logger.error(
                "@SaciProcessor $name returns ${returns ?: "nothing"}, want $PIPELINE_TYPE",
                processor,
            )
            ok = false
        }
        return ok
    }

    private fun checkTransform(transform: KSFunctionDeclaration, component: KSClassDeclaration) {
        val name = transform.simpleName.asString()
        val want = component.qualifiedName?.asString()
        val parameters = transform.parameters
        val first = parameters.getOrNull(0)?.type?.resolve()?.declaration?.qualifiedName?.asString()
        val second = parameters.getOrNull(1)?.type?.resolve()?.declaration?.qualifiedName?.asString()
        if (parameters.size != 2 || first != want || second != CONFIG_TYPE) {
            logger.error(
                "@SaciTransform $name must be fun $name(row: $want, config: $CONFIG_TYPE), " +
                    "got (${parameters.joinToString { it.type.resolve().toString() }})",
                transform,
            )
        }
    }

    // -----------------------------------------------------------------------
    // Emission.
    // -----------------------------------------------------------------------

    private fun emitCodec(
        component: KSClassDeclaration,
        fields: List<Field>,
        sources: List<KSFile>,
    ) {
        val name = component.simpleName.asString()
        val row = component.qualifiedName!!.asString()
        val version = component.annotation(COMPONENT_ANNOTATION)?.int("version", 1) ?: 1
        val out = StringBuilder(4096)

        out.append(header())
        out.append("package ").append(component.packageName.asString()).append("\n\n")
        out.append("import $ARROW.Batch\n")
        out.append("import $ARROW.BoolColumn\n")
        out.append("import $ARROW.Column\n")
        out.append("import $ARROW.FieldSpec\n")
        out.append("import $ARROW.Float64Column\n")
        out.append("import $ARROW.Int64Column\n")
        out.append("import $ARROW.SaciType\n")
        out.append("import $ARROW.Utf8Column\n")
        out.append("import $SDK.SaciComponentCodec\n\n")

        out.append("/** The generated accessor for [").append(name).append("]. */\n")
        out.append("object ").append(name).append("Codec : SaciComponentCodec<").append(row)
            .append("> {\n")
        out.append("    override val component: String = \"").append(escape(name)).append("\"\n\n")
        out.append("    override val version: UInt = ").append(version).append("u\n\n")

        out.append("    override val fields: List<FieldSpec> = listOf(\n")
        for (field in fields) {
            out.append("        FieldSpec(\"").append(escape(field.wire)).append("\", SaciType.")
                .append(field.kind.arrow).append("),\n")
        }
        out.append("    )\n\n")

        out.append("    override fun get(row: ").append(row)
            .append(", field: String): Any = when (field) {\n")
        for (field in fields) {
            out.append("        \"").append(escape(field.wire)).append("\" -> row.")
                .append(field.property).append("\n")
        }
        out.append("        else -> error(\"component \\\"").append(escape(name))
            .append("\\\" has no field \\\"\$field\\\"\")\n")
        out.append("    }\n\n")

        out.append("    override fun set(row: ").append(row)
            .append(", field: String, value: Any) {\n")
        out.append("        when (field) {\n")
        for (field in fields.filter { it.mutable }) {
            out.append("            \"").append(escape(field.wire)).append("\" -> row.")
                .append(field.property).append(" = value as ").append(field.kind.kotlinType)
                .append("\n")
        }
        val readOnly = fields.filterNot { it.mutable }
        if (readOnly.isNotEmpty()) {
            out.append("            ")
                .append(readOnly.joinToString(", ") { "\"${escape(it.wire)}\"" })
                .append(" ->\n")
            out.append("                error(\"component \\\"").append(escape(name))
                .append("\\\" field \\\"\$field\\\" is read only\")\n\n")
        }
        out.append("            else -> error(\"component \\\"").append(escape(name))
            .append("\\\" has no field \\\"\$field\\\"\")\n")
        out.append("        }\n")
        out.append("    }\n\n")

        // Locals are prefixed so a property named `rows`, `out` or `saciRows` cannot
        // shadow the machinery around it.
        out.append("    override fun decode(batch: Batch): MutableList<").append(row).append("> {\n")
        out.append("        val saciRows = batch.rows\n")
        for (field in fields) {
            out.append("        val saciColumn_").append(field.property).append(" = batch.")
                .append(field.kind.reader).append("(\"").append(escape(field.wire)).append("\")\n")
        }
        out.append("\n        val saciOut = ArrayList<").append(row).append(">(saciRows)\n")
        out.append("        for (saciRow in 0 until saciRows) {\n")
        out.append("            saciOut.add(\n")
        out.append("                ").append(row).append("(\n")
        for (field in fields) {
            out.append("                    saciColumn_").append(field.property).append("[saciRow],\n")
        }
        out.append("                )\n")
        out.append("            )\n")
        out.append("        }\n")
        out.append("        return saciOut\n")
        out.append("    }\n\n")

        out.append("    override fun encode(rows: List<").append(row)
            .append(">): Array<Column> {\n")
        out.append("        val count = rows.size\n")
        out.append("        return arrayOf(\n")
        for (field in fields) {
            val values = if (field.kind.array == null) {
                "Array(count) { rows[it].${field.property} }"
            } else {
                "${field.kind.array}(count) { rows[it].${field.property} }"
            }
            out.append("            ").append(field.kind.column).append("(\"")
                .append(escape(field.wire)).append("\", ").append(values).append("),\n")
        }
        out.append("        )\n")
        out.append("    }\n")
        out.append("}\n")

        write(out, component.packageName.asString(), "Saci${name}Codec", sources)
    }

    private fun emitGlue(
        component: KSClassDeclaration,
        processor: KSFunctionDeclaration,
        sources: List<KSFile>,
    ) {
        val annotation = processor.annotation(PROCESSOR_ANNOTATION)
        val name = annotation?.string("name") ?: ""
        val version = annotation?.string("version") ?: ""
        val logTarget = annotation?.string("logTarget") ?: ""
        val codec = "${component.packageName.asString()}.${component.simpleName.asString()}Codec"
        val build = "${processor.packageName.asString()}.${processor.simpleName.asString()}"

        val out = StringBuilder(4096)
        out.append(header())
        out.append(
            """
            package $EXPORT_PACKAGE

            import bindings.HostIo
            import bindings.Pipeline
            import bindings.Types
            import bindings.runtime.ComponentException
            import $SDK.SaciHost
            import $SDK.SaciLogLevel
            import $SDK.SaciRunner

            /** Bridges [SaciHost] onto the generated `host-io` import. */
            private object WitHost : SaciHost {
                override fun log(level: SaciLogLevel, target: String, message: String) {
                    HostIo.log(
                        when (level) {
                            SaciLogLevel.TRACE -> HostIo.LogLevel.TRACE
                            SaciLogLevel.DEBUG -> HostIo.LogLevel.DEBUG
                            SaciLogLevel.INFO -> HostIo.LogLevel.INFO
                            SaciLogLevel.WARN -> HostIo.LogLevel.WARN
                            SaciLogLevel.ERROR -> HostIo.LogLevel.ERROR
                        },
                        target,
                        message,
                    )
                }

                override fun metric(name: String, value: Double) {
                    HostIo.metric(name, value)
                }

                override fun config(key: String): String? = HostIo.getConfig(key)
            }

            /**
             * The `saci:pipeline/pipeline` export.
             *
             * Nothing throws past this boundary. An exception reaching the generated
             * trampoline traps the instance and the host sees an opaque wasm trap
             * instead of a reason, so every failure folds into `run-error::permanent`,
             * which the WIT contract designates for bad input shape and processor
             * bugs. `schema-mismatch` must never come out of `run-batch`.
             *
             * `wall-ns` is 0 unconditionally. Kotlin/Wasm reaches the outside world
             * through WASI preview 1 imports and every one of them traps once
             * `wasm-tools component new --adapt` has wrapped the module, so no clock
             * is reachable from here.
             */
            object PipelineImpl : Pipeline {
                private val runner = SaciRunner(
                    $build().copy(name = ${quote(name)}, version = ${quote(version)}),
                    $codec,
                    WitHost,
                    ${quote(logTarget)},
                )

                override fun describe(): Types.PipelineDescriptor {
                    val schema = try {
                        runner.schemaIpcWit()
                    } catch (e: Throwable) {
                        HostIo.log(
                            HostIo.LogLevel.ERROR,
                            runner.logTarget,
                            "encode the ${'$'}{runner.component} schema: ${'$'}{e.message}",
                        )
                        emptyList<UByte>()
                    }
                    return Types.PipelineDescriptor(
                        runner.pipeline.name,
                        runner.pipeline.version,
                        listOf(Types.ComponentDescriptor(runner.component, schema)),
                        false,
                        runner.fingerprint,
                    )
                }

                override fun runBatch(
                    input: List<UByte>,
                    prior: List<UByte>?,
                ): Result<Types.RunResult> {
                    try {
                        val result = runner.runBatch(input)
                        return Result.success(
                            Types.RunResult(
                                result.toWit(),
                                null,
                                Types.RunMetrics(
                                    0uL,
                                    result.rowsIn.toULong(),
                                    result.rowsOut.toULong(),
                                    result.systemsRun.toUInt(),
                                    0u,
                                ),
                                null,
                            )
                        )
                    } catch (e: Throwable) {
                        val message = e.message ?: e.toString()
                        HostIo.log(
                            HostIo.LogLevel.ERROR,
                            runner.logTarget,
                            "${'$'}{runner.pipeline.name}: ${'$'}message",
                        )
                        return Result.failure(ComponentException(Types.RunError.Permanent(message)))
                    }
                }
            }

            """.trimIndent()
        )

        write(out, EXPORT_PACKAGE, "SaciPipelineImpl", sources)
    }

    private fun write(text: CharSequence, packageName: String, fileName: String, sources: List<KSFile>) {
        codeGenerator
            .createNewFile(Dependencies(true, *sources.toTypedArray()), packageName, fileName)
            .use { it.write(text.toString().toByteArray()) }
    }
}

private fun header(): String =
    "// @generated by saci-sdk-kt-ksp from @SaciComponent, @SaciTransform and " +
        "@SaciProcessor — do not edit.\n\n"

/** Renders [text] as a Kotlin string literal. */
private fun quote(text: String): String = "\"${escape(text)}\""

private fun escape(text: String): String {
    val out = StringBuilder(text.length + 2)
    for (char in text) when (char) {
        '\\' -> out.append("\\\\")
        '"' -> out.append("\\\"")
        '$' -> out.append("\\$")
        '\n' -> out.append("\\n")
        '\r' -> out.append("\\r")
        '\t' -> out.append("\\t")
        else -> out.append(char)
    }
    return out.toString()
}

private fun KSAnnotated.annotation(qualified: String): KSAnnotation? = annotations.firstOrNull {
    it.annotationType.resolve().declaration.qualifiedName?.asString() == qualified
}

private fun KSAnnotation.string(name: String, fallback: String = ""): String =
    arguments.firstOrNull { it.name?.asString() == name }?.value as? String ?: fallback

private fun KSAnnotation.int(name: String, fallback: Int): Int =
    arguments.firstOrNull { it.name?.asString() == name }?.value as? Int ?: fallback

class SaciSymbolProcessorProvider : SymbolProcessorProvider {
    override fun create(environment: SymbolProcessorEnvironment): SymbolProcessor =
        SaciSymbolProcessor(environment.codeGenerator, environment.logger)
}
