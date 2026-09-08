// Runs the shared conformance corpus at `packages/arrow-ipc-conformance`, so
// this codec accepts and refuses exactly what the four sibling codecs accept
// and refuse.
//
// The corpus is the contract; the wording is not. Each reject case names a
// reason code, and [REASONS] maps that code to one substring of this codec's
// own phrasing, so a case added to the corpus costs one row there and nothing
// else. A rejection must also arrive as [ArrowIpcException]: a native
// `IndexOutOfBoundsException` reaching a caller is indistinguishable from a bug
// in the codec, which is the whole point of the reject half of the suite.
//
// The manifest is read with the same hand-rolled reader [ArrowIpcTest] uses for
// the fixture, because a package whose premise is "standard library only" should
// not pull a JSON dependency into its own test classpath.

import io.github.nassor.saci.arrowipc.ArrowIpcException
import io.github.nassor.saci.arrowipc.Batch
import io.github.nassor.saci.arrowipc.SaciStream
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * One row per reason code the corpus declares, each holding a substring of the
 * message this codec raises for it.
 *
 * Two reasons cover two vectors apiece and so map to the shared part of two
 * messages: `truncated_stream` to the prefix both framing refusals carry, and
 * `extra_message` to the clause both segment-tail refusals end with.
 *
 * `type_mismatch` maps to the connective of `field "x" is Utf8, not Int`,
 * because the two type names around it vary per case. Nothing else in the codec
 * phrases a refusal that way.
 */
private val REASONS = mapOf(
    "trailing_bytes" to "trail the stream terminator",
    "truncated_stream" to "truncated stream",
    "truncated_message" to "metadata bytes",
    "bad_continuation" to "continuation marker missing",
    "empty_segment" to "is empty",
    "first_message_not_schema" to "opens with header_type",
    "second_message_not_record_batch" to "second message has header_type",
    "dictionary_batch" to "dictionary batch",
    "compressed_batch" to "body is compressed",
    "extra_message" to "want one Schema and one RecordBatch",
    "bad_row_count" to "record batch length is",
    "nodes_field_mismatch" to "field nodes",
    "buffer_overruns_body" to "spans",
    "missing_component_key" to "__saci_component",
    "unknown_component" to "no segment declares component",
    "unknown_field" to "has no field",
    "type_mismatch" to ", not ",
    "row_out_of_range" to "is out of range for field",
    "variable_width_write" to "writes fixed-width",
)

class ConformanceTest {
    private val manifest = findManifest()
    private val corpus = parseJson(manifest.readText()).asObj("manifest")
    private val cases = corpus.arr("cases").map { it.asObj("case") }

    @Test
    fun theCorpusIsTheOneThisHarnessWasWrittenFor() {
        assertEquals(1, corpus.int("format_version"), "unexpected corpus format version")
        assertTrue(cases.isNotEmpty(), "$manifest lists no cases")
    }

    /**
     * The reason table and the corpus must name the same reasons. A reason added
     * upstream then fails here, naming the single row to add.
     */
    @Test
    fun everyCorpusReasonIsMapped() {
        val declared = corpus.arr("reasons").map { it.asStr("reason") }.toSet()
        assertEquals(declared, REASONS.keys, "the reason table and the corpus disagree")
    }

    @Test
    fun acceptedVectorsReadBackExactly() {
        var seen = 0
        for (case in cases) {
            if (case.str("expect") != "accept") continue
            seen++
            val name = case.str("name")
            val accept = case.obj("accept")

            val stream = SaciStream.parse(vectorBytes(case, name))
            assertEquals(
                accept.arr("components").map { it.asStr("$name component name") },
                stream.componentNames(),
                "$name: component list",
            )

            val batch = stream.component(accept.str("component"))
            assertEquals(accept.int("rows"), batch.rows, "$name: row count")
            for ((field, spec) in accept.obj("columns")) {
                assertColumn(name, batch, field, spec.asObj("$name column \"$field\""))
            }
        }
        assertTrue(seen > 0, "the corpus lists no accept case")
    }

    @Test
    fun rejectedVectorsRaiseThisCodecsOwnError() {
        var seen = 0
        for (case in cases) {
            if (case.str("expect") != "reject") continue
            seen++
            val name = case.str("name")
            val reason = case.str("reason")
            val want = REASONS[reason] ?: fail("$name: reason \"$reason\" has no substring")
            val bytes = vectorBytes(case, name)

            val error = assertFailsWith<ArrowIpcException>("$name ($reason) was not refused") {
                exercise(bytes, case["op"])
            }
            val message = error.message ?: fail("$name ($reason): the exception carried no message")
            assertTrue(
                message.contains(want),
                "$name ($reason): \"$message\" does not contain \"$want\"",
            )
        }
        assertTrue(seen > 0, "the corpus lists no reject case")
    }

    /**
     * A case without an `op` is refused somewhere in the parse, the segment
     * labels or the batch walk, so it runs all three. A case with an `op` parses
     * cleanly and is refused by the call.
     */
    private fun exercise(bytes: ByteArray, op: Any?) {
        val stream = SaciStream.parse(bytes)
        if (op == null) {
            stream.componentNames()
            stream.component(corpus.str("component"))
            return
        }
        val spec = op.asObj("op")
        val component = spec.str("component")
        when (val kind = spec.str("kind")) {
            "component" -> stream.component(component)
            "column" -> readColumn(stream.component(component), spec)
            "set" -> writeValue(stream.component(component), spec)
            else -> fail("op kind \"$kind\" is not one this harness runs")
        }
    }

    private fun readColumn(batch: Batch, spec: Map<String, Any?>) {
        val field = spec.str("field")
        when (val type = spec.str("type")) {
            "int64" -> batch.int64s(field)
            "float64" -> batch.float64s(field)
            "utf8" -> batch.strings(field)
            "bool" -> batch.bools(field)
            else -> fail("column type \"$type\" is not one this codec reads")
        }
    }

    private fun writeValue(batch: Batch, spec: Map<String, Any?>) {
        val field = spec.str("field")
        val row = spec.int("row")
        when (val type = spec.str("type")) {
            "int64" -> batch.setInt64(field, row, spec.num("value").toLong())
            "float64" -> batch.setFloat64(field, row, spec.num("value"))
            "bool" -> batch.setBool(field, row, spec.bool("value"))
            // This codec offers no variable-width setter by design, so the write
            // a processor would actually attempt is a fixed-width setter aimed at the
            // Utf8 column, and that is the refusal the case is about.
            "utf8" -> batch.setFloat64(field, row, 0.0)
            else -> fail("set type \"$type\" is not one this codec writes")
        }
    }

    private fun assertColumn(
        case: String,
        batch: Batch,
        field: String,
        spec: Map<String, Any?>,
    ) {
        val want = spec.arr("values")
        when (val type = spec.str("type")) {
            "int64" -> {
                val got = batch.int64s(field)
                assertEquals(want.size, got.size, "$case: \"$field\" length")
                for (row in want.indices) {
                    assertEquals(
                        want[row].asNum("$case \"$field\"[$row]").toLong(),
                        got[row],
                        "$case: \"$field\"[$row]",
                    )
                }
            }
            "float64" -> {
                val got = batch.float64s(field)
                assertEquals(want.size, got.size, "$case: \"$field\" length")
                for (row in want.indices) {
                    // Exact, on the bits: these are round-tripped values, never
                    // computed ones, so a tolerance would hide a decoding bug.
                    val value = want[row].asNum("$case \"$field\"[$row]")
                    assertEquals(
                        value.toRawBits(),
                        got[row].toRawBits(),
                        "$case: \"$field\"[$row] = ${got[row]}, want $value",
                    )
                }
            }
            "utf8" -> {
                val got = batch.strings(field)
                assertEquals(want.size, got.size, "$case: \"$field\" length")
                for (row in want.indices) {
                    assertEquals(
                        want[row].asStr("$case \"$field\"[$row]"),
                        got[row],
                        "$case: \"$field\"[$row]",
                    )
                }
            }
            "bool" -> {
                val got = batch.bools(field)
                assertEquals(want.size, got.size, "$case: \"$field\" length")
                for (row in want.indices) {
                    assertEquals(
                        want[row].asBool("$case \"$field\"[$row]"),
                        got[row],
                        "$case: \"$field\"[$row]",
                    )
                }
            }
            else -> fail("$case: column type \"$type\" is not one this codec reads")
        }
    }

    /** A vector path is relative to the manifest, and a missing one is a failure. */
    private fun vectorBytes(case: Map<String, Any?>, name: String): ByteArray {
        val file = File(manifest.parentFile, case.str("vector"))
        assertTrue(file.isFile, "$name: corpus vector $file is missing")
        return file.readBytes()
    }
}

/**
 * Walks up from the Gradle project directory, which is this module's working
 * directory when a test runs, to the corpus.
 *
 * A missing corpus fails every case here rather than skipping them: a
 * conformance suite that quietly runs nothing is worse than no suite at all.
 */
private fun findManifest(): File {
    var dir: File? = File("").absoluteFile
    while (dir != null) {
        val candidate = File(dir, "packages/arrow-ipc-conformance/manifest.json")
        if (candidate.isFile) return candidate
        dir = dir.parentFile
    }
    fail("no packages/arrow-ipc-conformance/manifest.json above ${File("").absolutePath}")
}

// The manifest is untyped once decoded, so every read names what it wanted and
// fails the case instead of throwing a cast error with no context.

@Suppress("UNCHECKED_CAST")
private fun Any?.asObj(what: String): Map<String, Any?> =
    this as? Map<String, Any?> ?: fail("$what is not a JSON object")

@Suppress("UNCHECKED_CAST")
private fun Any?.asArr(what: String): List<Any?> =
    this as? List<Any?> ?: fail("$what is not a JSON array")

private fun Any?.asStr(what: String): String =
    this as? String ?: fail("$what is not a JSON string")

private fun Any?.asNum(what: String): Double =
    this as? Double ?: fail("$what is not a JSON number")

private fun Any?.asBool(what: String): Boolean =
    this as? Boolean ?: fail("$what is not a JSON boolean")

private fun Map<String, Any?>.obj(key: String): Map<String, Any?> = this[key].asObj("\"$key\"")

private fun Map<String, Any?>.arr(key: String): List<Any?> = this[key].asArr("\"$key\"")

private fun Map<String, Any?>.str(key: String): String = this[key].asStr("\"$key\"")

private fun Map<String, Any?>.num(key: String): Double = this[key].asNum("\"$key\"")

private fun Map<String, Any?>.bool(key: String): Boolean = this[key].asBool("\"$key\"")

private fun Map<String, Any?>.int(key: String): Int = this[key].asNum("\"$key\"").toInt()
