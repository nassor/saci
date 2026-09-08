// The run-time half of the SDK: what the generated glue calls once per batch.
//
// A batch is decoded into row objects, every transform runs over every row, and
// the rows are re-encoded into a fresh stream. That is more work than the
// byte-mutating stages do, and it buys the two things in-place mutation cannot:
// a `Utf8` output column, and a row count that may shrink.
//
// Everything a processor's identity is made of is derived, not embedded. The
// component's `arrow-schema-ipc` bytes come out of the codec's schema writer and
// the fingerprint out of [saciFingerprint], both from the same generated
// [SaciComponentCodec.fields] list, so a field added to the row type moves the
// schema, the fingerprint and the wire bytes together and cannot move only one.

package io.github.nassor.saci.sdk

import io.github.nassor.saci.arrowipc.Batch
import io.github.nassor.saci.arrowipc.Column
import io.github.nassor.saci.arrowipc.FieldSpec
import io.github.nassor.saci.arrowipc.SACI_ALIVE_COMPONENT
import io.github.nassor.saci.arrowipc.SACI_ALIVE_FIELD
import io.github.nassor.saci.arrowipc.SaciStream
import io.github.nassor.saci.arrowipc.SaciStreamWriter
import io.github.nassor.saci.arrowipc.saciSchemaStream

/** FNV-1a 32-bit offset basis, 2166136261, as the `Int` holding those bits. */
private const val FNV_OFFSET: Int = -2128831035

private const val FNV_PRIME = 16777619

/**
 * `pipeline-descriptor.schema-fingerprint` for one component.
 *
 * FNV-1a over the component name, the version's four little-endian bytes, then
 * every field name in declaration order, rendered as eight lowercase hex digits.
 * Names and versions only: adding a field changes it, changing a field's type
 * does not.
 *
 * This is the one value a non-Rust processor is normally told to embed as a
 * generated constant rather than recompute. Recomputing it is safe here because
 * the input is the generated field list the same build derived the wire schema
 * from: there is no second definition to drift from. The polyglot driver and the
 * `polyglot_chain` integration test still compare it against the live Rust value.
 */
fun saciFingerprint(component: String, version: UInt, fields: List<String>): String {
    var hash = FNV_OFFSET

    fun mix(bytes: ByteArray) {
        for (byte in bytes) hash = (hash xor (byte.toInt() and 0xFF)) * FNV_PRIME
    }

    mix(component.encodeToByteArray())
    val bits = version.toInt()
    for (i in 0..3) hash = (hash xor ((bits ushr (i * 8)) and 0xFF)) * FNV_PRIME
    for (field in fields) mix(field.encodeToByteArray())

    return (hash.toLong() and 0xFFFFFFFFL).toString(16).padStart(8, '0')
}

/**
 * The wire field name for a Kotlin property name: `usdAmountDisplay` becomes
 * `usd_amount_display`.
 *
 * An underscore goes before an upper-case letter that ends a word, which is one
 * that follows a lower-case letter or a digit, or one that starts a word inside
 * an acronym by being followed by a lower-case letter. So `httpURL` becomes
 * `http_url` and `urlPath` becomes `url_path`, and a property already written in
 * snake_case passes through unchanged.
 */
fun saciWireName(property: String): String {
    val out = StringBuilder(property.length + 4)
    for (index in property.indices) {
        val char = property[index]
        if (char !in 'A'..'Z') {
            out.append(char)
            continue
        }
        val previous = if (index == 0) null else property[index - 1]
        val next = if (index + 1 < property.length) property[index + 1] else null
        val endsWord = previous != null && (previous in 'a'..'z' || previous in '0'..'9')
        val startsWord = previous != null && previous in 'A'..'Z' && next != null && next in 'a'..'z'
        if (endsWord || startsWord) out.append('_')
        out.append(char + 32)
    }
    return out.toString()
}

/**
 * The generated accessor for one component's row type.
 *
 * Generated rather than reflected: Kotlin/Wasm has no reflection, so a property
 * name only exists in a `wasmWasi` binary if the build wrote it there.
 *
 * [decode] and [encode] are the hot path and are generated fully typed, so a
 * batch of `n` rows costs one primitive array per column and no per-cell boxing.
 * [get] and [set] are the by-name row view, for code that has a field name
 * rather than a property; [set] refuses a `val` property, because a read-only
 * field is a processor input and rewriting it would not survive the round trip
 * through the constructor that [decode] uses.
 */
interface SaciComponentCodec<R : Any> {
    /** The segment's `__saci_component` label. */
    val component: String

    /** The segment's `__saci_schema_version` value. */
    val version: UInt

    /** Wire fields in declaration order, which is schema and buffer-walk order. */
    val fields: List<FieldSpec>

    /** One row's value for the field named [field]. */
    fun get(row: R, field: String): Any

    /** Overwrites one row's [field]. Read-only fields are refused. */
    fun set(row: R, field: String, value: Any)

    /** Every row of [batch], in row order. */
    fun decode(batch: Batch): MutableList<R>

    /** [rows] as one column per [fields] entry, in that order. */
    fun encode(rows: List<R>): Array<Column>
}

/**
 * A processor's transforms, plus the identity the host sees.
 *
 * [of] is what a stage author calls, and it knows only the transforms; [name] and
 * [version] come from `@SaciProcessor`, which the generated glue reads and applies
 * with [copy]. Keeping them on this type rather than beside it means the runner
 * has one object to ask about the processor's identity.
 */
data class SaciPipeline(
    val name: String = "",
    val version: String = "",
    val systems: List<(Any, SaciConfig) -> Unit>,
) {
    companion object {
        /**
         * Registers [transforms], in order.
         *
         * `R` is inferred from the function references, so every transform in one
         * call has to take the same row type, which is the same single component
         * the generated codec covers.
         */
        @Suppress("UNCHECKED_CAST")
        fun <R : Any> of(vararg transforms: (R, SaciConfig) -> Unit): SaciPipeline =
            SaciPipeline(systems = transforms.map { it as (Any, SaciConfig) -> Unit })
    }
}

/** What one `run-batch` produced, before the WIT record is built around it. */
class SaciBatchResult(
    val output: ByteArray,
    val rowsIn: Int,
    val rowsOut: Int,
    val systemsRun: Int,
) {
    /** [output] in the shape `run-result.output` wants. */
    @OptIn(ExperimentalUnsignedTypes::class)
    fun toWit(): List<UByte> = output.asUByteArray().asList()
}

/**
 * Runs one processor over one batch.
 *
 * Constructed once, by the generated `impl.PipelineImpl`, and reused for every
 * batch: [fingerprint] and [schemaIpc] are derived once, and nothing else here
 * holds state across a call. That is what `stateful: false` promises the host,
 * and it is also all the host allows — it builds a fresh wasmtime `Store` per
 * call, so linear memory never survives a batch boundary anyway.
 */
class SaciRunner<R : Any>(
    val pipeline: SaciPipeline,
    private val codec: SaciComponentCodec<R>,
    private val host: SaciHost,
    logTarget: String = "",
) {
    /** The `tracing` target the per-batch log line is bridged to. */
    val logTarget: String = logTarget.ifEmpty { pipeline.name }

    /** The component this processor declares. */
    val component: String get() = codec.component

    /** `pipeline-descriptor.schema-fingerprint`. */
    val fingerprint: String =
        saciFingerprint(codec.component, codec.version, codec.fields.map { it.name })

    /**
     * `component-descriptor.arrow-schema-ipc`.
     *
     * Lazy so a malformed generated field list surfaces inside `describe`, where
     * the glue can log it, rather than during the object initialiser, where it
     * would reach the host as an opaque trap.
     */
    val schemaIpc: ByteArray by lazy(LazyThreadSafetyMode.NONE) { saciSchemaStream(codec.fields) }

    /**
     * [schemaIpc] in the shape `component-descriptor.arrow-schema-ipc` wants.
     *
     * The conversion lives here rather than in the generated glue so the glue
     * needs no `ExperimentalUnsignedTypes` opt-in of its own.
     */
    @OptIn(ExperimentalUnsignedTypes::class)
    fun schemaIpcWit(): List<UByte> = schemaIpc.asUByteArray().asList()

    /** Runs the pipeline over the WIT `list<u8>` the export glue delivers. */
    fun runBatch(input: List<UByte>): SaciBatchResult =
        runBatch(ByteArray(input.size) { input[it].toByte() })

    /**
     * Decodes [input], applies every transform, and re-encodes.
     *
     * Systems run outermost: each transform sees every row before the next one
     * starts, which is what makes a later transform able to read what an earlier
     * one wrote.
     *
     * The `__alive` bitmap is read and written back unchanged. It is the stream's
     * row bound rather than a per-component mask, so a component that shrinks
     * stays within it and the host keeps its own view of which rows exist.
     */
    fun runBatch(input: ByteArray): SaciBatchResult {
        val stream = SaciStream.parse(input)
        val batch = stream.component(codec.component)
        val alive = stream.component(SACI_ALIVE_COMPONENT).bools(SACI_ALIVE_FIELD)
        val rows = codec.decode(batch)

        val config = SaciConfig(host)
        for (system in pipeline.systems) {
            for (row in rows) system(row, config)
        }
        config.flush()

        val output = SaciStreamWriter()
            .writeComponent(codec.component, codec.version, *codec.encode(rows))
            .writeAlive(alive)
            .toBytes()

        host.log(
            SaciLogLevel.INFO,
            logTarget,
            "${pipeline.name}: ran ${pipeline.systems.size} systems over ${rows.size} of " +
                "${alive.size} rows, resolving ${config.resolvedKeys} config keys",
        )

        return SaciBatchResult(output, batch.rows, rows.size, pipeline.systems.size)
    }
}
