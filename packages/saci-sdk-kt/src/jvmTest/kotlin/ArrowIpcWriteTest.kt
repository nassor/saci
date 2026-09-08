// Encodes streams with [SaciStreamWriter] and decodes them with the reader in the
// same module, on the JVM, with no WebAssembly involved.
//
// The reader is a strict decoder of the documented format: it rejects a wrong
// buffer-slot count, a body a buffer overruns, a segment with bytes past its
// end-of-stream marker, and a schema with no `__saci_component` label. Feeding it
// the writer's output is therefore a real conformance check on the writer's
// flatbuffers and framing, not just a self-consistency loop.
//
// The headline case re-encodes `generated/fixture_input.saci`, arrow-rs's own
// output, and compares every column of the result against
// `generated/fixture_input.json`. That covers eleven of the canonical `Order`
// schema's twelve fields, all four Arrow types, and the trailing `__alive` bitmap
// in one pass.

import io.github.nassor.saci.arrowipc.ArrowIpcException
import io.github.nassor.saci.arrowipc.BoolColumn
import io.github.nassor.saci.arrowipc.Column
import io.github.nassor.saci.arrowipc.FieldSpec
import io.github.nassor.saci.arrowipc.Float64Column
import io.github.nassor.saci.arrowipc.Int64Column
import io.github.nassor.saci.arrowipc.SaciStream
import io.github.nassor.saci.arrowipc.SaciStreamWriter
import io.github.nassor.saci.arrowipc.SaciType
import io.github.nassor.saci.arrowipc.Utf8Column
import io.github.nassor.saci.arrowipc.saciSchemaStream
import java.io.File
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Eleven of the canonical `Order` schema's twelve fields, in wire order, without `usd_amount_display`. */
private val ORDER_SCHEMA = listOf(
    FieldSpec("id", SaciType.INT64),
    FieldSpec("region", SaciType.UTF8),
    FieldSpec("currency", SaciType.UTF8),
    FieldSpec("amount", SaciType.FLOAT64),
    FieldSpec("valid", SaciType.BOOL),
    FieldSpec("usd_amount", SaciType.FLOAT64),
    FieldSpec("risk_score", SaciType.FLOAT64),
    FieldSpec("flagged", SaciType.BOOL),
    FieldSpec("fee", SaciType.FLOAT64),
    FieldSpec("review_tier", SaciType.INT64),
    FieldSpec("settlement", SaciType.UTF8),
)

private const val WRITE_TOLERANCE = 1e-9

private val generatedDir = File("../../examples/polyglot/generated")

class ArrowIpcWriteTest {
    @Test
    fun everyTypeRoundTripsThroughTheWriter() {
        val ids = longArrayOf(1, 2, 3, -4, Long.MAX_VALUE)
        val amounts = doubleArrayOf(0.0, -1.5, 1e300, 3.141592653589793, -0.0)
        val flags = booleanArrayOf(true, false, false, true, true)
        val tags = arrayOf("eu", "", "us-east", "\u00e9\u00e0\u4e16\u754c", "x")

        val bytes = SaciStreamWriter()
            .writeComponent(
                "Sample",
                1u,
                Int64Column("id", ids),
                Float64Column("amount", amounts),
                BoolColumn("flag", flags),
                Utf8Column("tag", tags),
            )
            .writeAlive(BooleanArray(5) { true })
            .toBytes()

        val stream = SaciStream.parse(bytes)
        assertEquals(listOf("Sample", "__alive"), stream.componentNames())

        val batch = stream.component("Sample")
        assertEquals(listOf("id", "amount", "flag", "tag"), batch.fieldNames())
        assertEquals(5, batch.rows)
        assertContentEquals(ids, batch.int64s("id"))
        assertContentEquals(flags, batch.bools("flag"))
        assertEquals(tags.toList(), batch.strings("tag"))
        val readAmounts = batch.float64s("amount")
        for (row in amounts.indices) {
            assertEquals(
                amounts[row].toRawBits(),
                readAmounts[row].toRawBits(),
                "amount[$row] lost its exact bits",
            )
        }

        assertContentEquals(BooleanArray(5) { true }, stream.component("__alive").bools("alive"))
    }

    /**
     * Re-encodes arrow-rs's own fixture and checks the result against the JSON
     * ground truth, column by column.
     */
    @Test
    fun theRealFixtureReEncodesToAnEquivalentStream() {
        val original = SaciStream.parse(generated("fixture_input.saci").readBytes())
        val source = original.component("Order")
        val alive = original.component("__alive").bools("alive")

        val bytes = SaciStreamWriter()
            .writeComponent("Order", 1u, *columnsOf(ORDER_SCHEMA) { name, type ->
                when (type) {
                    SaciType.INT64 -> Int64Column(name, source.int64s(name))
                    SaciType.FLOAT64 -> Float64Column(name, source.float64s(name))
                    SaciType.BOOL -> BoolColumn(name, source.bools(name))
                    SaciType.UTF8 -> Utf8Column(name, source.strings(name).toTypedArray())
                }
            })
            .writeAlive(alive)
            .toBytes()

        val expected = jsonRows(generated("fixture_input.json").readText())
        val batch = SaciStream.parse(bytes).component("Order")
        assertEquals(ORDER_SCHEMA.map { it.name }, batch.fieldNames())
        assertEquals(expected.size, batch.rows)

        for (field in ORDER_SCHEMA) {
            when (field.type) {
                SaciType.INT64 -> {
                    val got = batch.int64s(field.name)
                    for ((row, want) in expected.withIndex()) {
                        assertEquals(
                            (want[field.name] as Double).toLong(),
                            got[row],
                            "${field.name}[$row]",
                        )
                    }
                }

                SaciType.FLOAT64 -> {
                    val got = batch.float64s(field.name)
                    for ((row, want) in expected.withIndex()) {
                        val value = want[field.name] as Double
                        assertTrue(
                            abs(value - got[row]) <= WRITE_TOLERANCE,
                            "${field.name}[$row] = ${got[row]}, want $value",
                        )
                    }
                }

                SaciType.BOOL -> {
                    val got = batch.bools(field.name)
                    for ((row, want) in expected.withIndex()) {
                        assertEquals(want[field.name] as Boolean, got[row], "${field.name}[$row]")
                    }
                }

                SaciType.UTF8 -> {
                    val got = batch.strings(field.name)
                    for ((row, want) in expected.withIndex()) {
                        assertEquals(want[field.name] as String, got[row], "${field.name}[$row]")
                    }
                }
            }
        }
    }

    /**
     * A filtering processor emits fewer rows than it read, which is the one
     * shape in-place mutation cannot produce.
     */
    @Test
    fun reEncodingWithFewerRowsShrinksTheBatch() {
        val five = SaciStreamWriter()
            .writeComponent(
                "Sample",
                1u,
                Int64Column("id", longArrayOf(10, 11, 12, 13, 14)),
                Utf8Column("tag", arrayOf("a", "b", "c", "d", "e")),
            )
            .writeAlive(BooleanArray(5) { true })
            .toBytes()

        val wide = SaciStream.parse(five).component("Sample")
        assertEquals(5, wide.rows)

        // Keep the first three rows, the way a predicate would.
        val ids = wide.int64s("id")
        val tags = wide.strings("tag")
        val threeIds = LongArray(3) { ids[it] }
        val threeTags = Array(3) { tags[it] }

        val three = SaciStreamWriter()
            .writeComponent("Sample", 1u, Int64Column("id", threeIds), Utf8Column("tag", threeTags))
            .writeAlive(BooleanArray(3) { true })
            .toBytes()

        val narrow = SaciStream.parse(three).component("Sample")
        assertEquals(3, narrow.rows)
        assertContentEquals(longArrayOf(10, 11, 12), narrow.int64s("id"))
        assertEquals(listOf("a", "b", "c"), narrow.strings("tag"))
        assertTrue(three.size < five.size, "the shorter batch produced a longer stream")
    }

    @Test
    fun aZeroRowComponentIsWritable() {
        val bytes = SaciStreamWriter()
            .writeComponent("Sample", 1u, Int64Column("id", LongArray(0)), Utf8Column("tag", emptyArray()))
            .writeAlive(BooleanArray(4) { true })
            .toBytes()

        val batch = SaciStream.parse(bytes).component("Sample")
        assertEquals(0, batch.rows)
        assertContentEquals(LongArray(0), batch.int64s("id"))
        assertEquals(emptyList(), batch.strings("tag"))
    }

    @Test
    fun componentSegmentsAreOrderedByName() {
        val bytes = SaciStreamWriter()
            .writeComponent("Zeta", 1u, Int64Column("id", longArrayOf(1)))
            .writeComponent("Alpha", 1u, Int64Column("id", longArrayOf(2)))
            .writeAlive(booleanArrayOf(true))
            .toBytes()

        assertEquals(listOf("Alpha", "Zeta", "__alive"), SaciStream.parse(bytes).componentNames())
    }

    /**
     * `__saci_schema_version` is what a peer processor compares against its own
     * registration, and the `__alive` segment must not carry one: the reader
     * exposes neither, so this reads the metadata out of the raw bytes.
     */
    @Test
    fun theSchemaVersionLabelsComponentsAndNotTheAliveSegment() {
        val bytes = SaciStreamWriter()
            .writeComponent("Sample", 7u, Int64Column("id", longArrayOf(1)))
            .writeAlive(booleanArrayOf(true))
            .toBytes()

        val text = bytes.toString(Charsets.ISO_8859_1)
        assertEquals(1, countOf(text, "__saci_schema_version"), "one versioned segment expected")
        assertEquals(2, countOf(text, "__saci_component"), "Sample and __alive both need a label")
        assertTrue(text.contains("Sample"), "the component label is missing")
        assertTrue(text.contains("__alive"), "the alive label is missing")
    }

    /** `metadata_len` includes the flatbuffer's padding to eight bytes. */
    @Test
    fun everyMessageDeclaresAnEightAlignedMetadataLength() {
        val bytes = SaciStreamWriter()
            .writeComponent("Sample", 1u, Utf8Column("tag", arrayOf("abc", "de")))
            .writeAlive(booleanArrayOf(true, true))
            .toBytes()

        // Segment length prefix, then the first message's continuation word.
        val schemaLen = le32(bytes, 8)
        assertEquals(0, schemaLen % 8, "schema metadata_len is $schemaLen")
        val batchAt = 4 + 8 + schemaLen
        assertEquals(0xFFFFFFFFL, le32(bytes, batchAt).toLong() and 0xFFFFFFFFL)
        val batchLen = le32(bytes, batchAt + 4)
        assertEquals(0, batchLen % 8, "record batch metadata_len is $batchLen")
    }

    @Test
    fun theWriterRefusesShapesTheHostWouldReadAsCorruption() {
        assertMessage("column \"other\" holds 1 rows") {
            SaciStreamWriter().writeComponent(
                "Sample",
                1u,
                Int64Column("id", longArrayOf(1, 2)),
                Int64Column("other", longArrayOf(1)),
            )
        }
        assertMessage("declares field \"id\" twice") {
            SaciStreamWriter().writeComponent(
                "Sample",
                1u,
                Int64Column("id", longArrayOf(1)),
                Int64Column("id", longArrayOf(2)),
            )
        }
        assertMessage("declares no columns") {
            SaciStreamWriter().writeComponent("Sample", 1u)
        }
        assertMessage("written by writeAlive") {
            SaciStreamWriter().writeComponent("__alive", 1u, BoolColumn("alive", booleanArrayOf(true)))
        }
        assertMessage("is already written") {
            SaciStreamWriter()
                .writeComponent("Sample", 1u, Int64Column("id", longArrayOf(1)))
                .writeComponent("Sample", 1u, Int64Column("id", longArrayOf(1)))
        }
        assertMessage("call writeAlive") {
            SaciStreamWriter().writeComponent("Sample", 1u, Int64Column("id", longArrayOf(1))).toBytes()
        }
        assertMessage("more than the 2 bits") {
            SaciStreamWriter()
                .writeComponent("Sample", 1u, Int64Column("id", longArrayOf(1, 2, 3)))
                .writeAlive(booleanArrayOf(true, true))
                .toBytes()
        }
    }

    /**
     * The descriptor's bytes are a schema-only stream with no `custom_metadata`,
     * so splicing them into a stream produces an unlabelled, batchless segment.
     * Reaching that refusal proves both halves: the Schema message parses, and it
     * carries no component label.
     */
    @Test
    fun theDescriptorSchemaStreamIsSchemaOnlyAndUnlabelled() {
        val schema = saciSchemaStream(ORDER_SCHEMA)
        assertEquals(0xFFFFFFFFL, le32(schema, 0).toLong() and 0xFFFFFFFFL)
        val metadataLen = le32(schema, 4)
        assertEquals(0, metadataLen % 8)
        assertEquals(8 + metadataLen + 8, schema.size, "a schema-only stream carries no body")
        assertEquals(0xFFFFFFFFL, le32(schema, 8 + metadataLen).toLong() and 0xFFFFFFFFL)
        assertEquals(0, le32(schema, 12 + metadataLen), "the stream must end with metadata_len 0")

        val framed = ByteArray(4 + schema.size + 4)
        for (i in 0..3) framed[i] = ((schema.size shr (i * 8)) and 0xFF).toByte()
        schema.copyInto(framed, 4)
        assertMessage("no custom_metadata") { SaciStream.parse(framed).componentNames() }

        assertMessage("declares no fields") { saciSchemaStream(emptyList()) }
    }

    private fun assertMessage(fragment: String, body: () -> Unit) {
        val error = assertFailsWith<ArrowIpcException>(block = body)
        assertTrue(
            error.message!!.contains(fragment),
            "want a message containing \"$fragment\", got \"${error.message}\"",
        )
    }
}

private fun generated(name: String) = File(generatedDir, name)

private fun le32(buf: ByteArray, at: Int): Int =
    (buf[at].toInt() and 0xFF) or
        ((buf[at + 1].toInt() and 0xFF) shl 8) or
        ((buf[at + 2].toInt() and 0xFF) shl 16) or
        ((buf[at + 3].toInt() and 0xFF) shl 24)

private fun countOf(text: String, needle: String): Int {
    var count = 0
    var at = text.indexOf(needle)
    while (at >= 0) {
        count++
        at = text.indexOf(needle, at + needle.length)
    }
    return count
}

/** Builds one [Column] per [schema] entry through [make], preserving order. */
private fun columnsOf(
    schema: List<FieldSpec>,
    make: (String, SaciType) -> Column,
): Array<Column> = Array(schema.size) { make(schema[it].name, schema[it].type) }

/** Reuses [parseJson] from [ArrowIpcTest] rather than adding a JSON dependency. */
@Suppress("UNCHECKED_CAST")
private fun jsonRows(text: String): List<Map<String, Any?>> =
    (parseJson(text) as List<Any?>).map { it as Map<String, Any?> }
