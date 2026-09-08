// Decodes the real generated fixture with the same codec the component runs, on
// the JVM, with no WebAssembly involved.
//
// When a value comes out wrong in the chain, the useful signal is which
// language's codec is misreading bytes. These cases answer that for Kotlin: they
// compare every column of `generated/fixture_input.saci` against
// `generated/fixture_input.json`, then exercise the three in-place setters and
// the refusals that keep a byte-mutating processor honest.

import io.github.nassor.saci.arrowipc.ArrowIpcException
import io.github.nassor.saci.arrowipc.Batch
import io.github.nassor.saci.arrowipc.SaciStream
import io.github.nassor.saci.arrowipc.decodeBase64
import java.io.File
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Field order is load-bearing: it feeds the fingerprint and the buffer walk. */
private val FIELD_ORDER = listOf(
    "id",
    "region",
    "currency",
    "amount",
    "valid",
    "usd_amount",
    "usd_amount_display",
    "risk_score",
    "flagged",
    "fee",
    "review_tier",
    "settlement",
)

private const val TOLERANCE = 1e-9

private val generated = File("../../examples/polyglot/generated")

private fun generated(name: String) = File(generated, name)

class ArrowIpcTest {
    private val raw: ByteArray = generated("fixture_input.saci").readBytes()
    private val expected: List<Map<String, Any?>> = parseJsonRows(
        generated("fixture_input.json").readText()
    )

    private fun order(bytes: ByteArray = raw) = SaciStream.parse(bytes).let { it to it.component("Order") }

    @Test
    fun fixtureCarriesTheOrderAndAliveSegments() {
        val stream = SaciStream.parse(raw)
        assertEquals(listOf("Order", "__alive"), stream.componentNames())
        assertEquals(expected.size, stream.component("__alive").rows)
    }

    @Test
    fun schemaFieldOrderIsTheDocumentedOne() {
        val (_, batch) = order()
        assertEquals(FIELD_ORDER, batch.fieldNames())
    }

    @Test
    fun everyColumnMatchesTheJsonGroundTruth() {
        val (_, batch) = order()
        assertEquals(expected.size, batch.rows)

        val ids = batch.int64s("id")
        val tiers = batch.int64s("review_tier")
        val regions = batch.strings("region")
        val currencies = batch.strings("currency")
        val settlements = batch.strings("settlement")
        val displays = batch.strings("usd_amount_display")
        val amounts = batch.float64s("amount")
        val usd = batch.float64s("usd_amount")
        val risk = batch.float64s("risk_score")
        val fees = batch.float64s("fee")
        val valid = batch.bools("valid")
        val flagged = batch.bools("flagged")

        for ((row, want) in expected.withIndex()) {
            assertEquals(want.long("id"), ids[row], "id[$row]")
            assertEquals(want.long("review_tier"), tiers[row], "review_tier[$row]")
            assertEquals(want.string("region"), regions[row], "region[$row]")
            assertEquals(want.string("currency"), currencies[row], "currency[$row]")
            assertEquals(want.string("settlement"), settlements[row], "settlement[$row]")
            assertEquals(
                want.string("usd_amount_display"),
                displays[row],
                "usd_amount_display[$row]",
            )
            assertClose("amount[$row]", want.double("amount"), amounts[row])
            assertClose("usd_amount[$row]", want.double("usd_amount"), usd[row])
            assertClose("risk_score[$row]", want.double("risk_score"), risk[row])
            assertClose("fee[$row]", want.double("fee"), fees[row])
            assertEquals(want.bool("valid"), valid[row], "valid[$row]")
            assertEquals(want.bool("flagged"), flagged[row], "flagged[$row]")
        }
    }

    @Test
    fun setFloat64OnFeeRoundTripsThroughAReparse() {
        val (stream, batch) = order()
        batch.setFloat64("fee", 0, 1.32)
        batch.setFloat64("fee", 2, 54.4)

        val (_, reparsed) = order(stream.buf.copyOf())
        val fees = reparsed.float64s("fee")
        assertClose("fee[0]", 1.32, fees[0])
        assertClose("fee[2]", 54.4, fees[2])
        for (row in listOf(1, 3, 4, 5)) assertClose("fee[$row]", 0.0, fees[row])
        assertUntouchedExcept(reparsed, "fee")
    }

    @Test
    fun setInt64OnReviewTierRoundTripsThroughAReparse() {
        val (stream, batch) = order()
        batch.setInt64("review_tier", 3, 2)
        batch.setInt64("review_tier", 5, 1)

        val (_, reparsed) = order(stream.buf.copyOf())
        assertContentEquals(
            longArrayOf(0, 0, 0, 2, 0, 1),
            reparsed.int64s("review_tier"),
        )
        assertUntouchedExcept(reparsed, "review_tier")
    }

    /**
     * An `Int64` write must touch exactly the eight bytes of its slot. The
     * expected count is derived from the value rather than hardcoded: anything
     * more means the framing, a flatbuffer or the `__alive` segment moved.
     */
    @Test
    fun setInt64ChangesOnlyItsOwnEightBytes() {
        val (stream, batch) = order()
        batch.setInt64("review_tier", 4, 0x0102030405060708L)

        var changed = 0
        for (i in raw.indices) if (raw[i] != stream.buf[i]) changed++
        assertEquals(8, changed, "an Int64 write touched $changed bytes")
        assertEquals(raw.size, stream.buf.size)
    }

    @Test
    fun setBoolSetThenClearReproducesTheInput() {
        val (stream, batch) = order()
        batch.setBool("flagged", 3, true)
        batch.setBool("flagged", 3, false)
        assertContentEquals(raw, stream.buf)
    }

    @Test
    fun settersRefuseTheVariableLengthColumn() {
        for (write in listOf<(Batch) -> Unit>(
            { it.setFloat64("settlement", 0, 1.0) },
            { it.setInt64("settlement", 0, 1) },
        )) {
            val (stream, batch) = order()
            val error = assertFailsWith<ArrowIpcException> { write(batch) }
            assertTrue(
                error.message!!.contains("fixed-width"),
                "unexpected message: ${error.message}",
            )
            assertContentEquals(raw, stream.buf, "a refused write still mutated the buffer")
        }
    }

    @Test
    fun settersRefuseATypeMismatchAndAnOutOfRangeRow() {
        val (_, batch) = order()
        assertFailsWith<ArrowIpcException> { batch.setFloat64("valid", 0, 1.0) }
        assertFailsWith<ArrowIpcException> { batch.setInt64("fee", 0, 1) }
        assertFailsWith<ArrowIpcException> { batch.setInt64("review_tier", batch.rows, 1) }
        assertFailsWith<ArrowIpcException> { batch.int64s("nope") }
    }

    @Test
    fun componentRefusesANameTheStreamDoesNotCarry() {
        val stream = SaciStream.parse(raw)
        val error = assertFailsWith<ArrowIpcException> { stream.component("Nope") }
        assertTrue(
            error.message!!.contains("no segment declares component"),
            "unexpected message: ${error.message}",
        )
    }

    /**
     * The one segment-tail shape the shared corpus does not reach: too few
     * bytes left for an end-of-stream marker. Built by re-framing the fixture's
     * first segment four bytes short, so its marker is half present.
     */
    @Test
    fun aSegmentTailTooShortForAMarkerIsRefused() {
        val declared = (0..3).sumOf { (raw[it].toInt() and 0xFF) shl (it * 8) }
        val short = declared - 4
        val reframed = ByteArray(4 + short + 4)
        for (i in 0..3) reframed[i] = ((short shr (i * 8)) and 0xFF).toByte()
        raw.copyInto(reframed, 4, 4, 4 + short)

        val error = assertFailsWith<ArrowIpcException> {
            SaciStream.parse(reframed).component("Order")
        }
        assertTrue(
            error.message!!.contains("want one Schema and one RecordBatch"),
            "unexpected message: ${error.message}",
        )
    }

    @Test
    fun truncatedInputIsAnErrorNotACrash() {
        assertFailsWith<ArrowIpcException> { SaciStream.parse(raw.copyOf(raw.size - 1)) }
        assertFailsWith<ArrowIpcException> { SaciStream.parse(ByteArray(2)) }
        assertFailsWith<ArrowIpcException> { SaciStream.parse(ByteArray(4)) }
    }

    @Test
    fun decodeBase64HandlesPaddingAndRejectsGarbage() {
        assertContentEquals("hello".encodeToByteArray(), decodeBase64("aGVsbG8="))
        assertContentEquals(ByteArray(0), decodeBase64(""))
        assertFailsWith<ArrowIpcException> { decodeBase64("not base64!") }
    }

    /** Every column except [mutated] must still equal the JSON ground truth. */
    private fun assertUntouchedExcept(batch: Batch, mutated: String) {
        for (name in FIELD_ORDER) {
            if (name == mutated) continue
            for ((row, want) in expected.withIndex()) {
                val label = "$name[$row]"
                when (name) {
                    "id", "review_tier" -> assertEquals(want.long(name), batch.int64s(name)[row], label)
                    "region", "currency", "usd_amount_display", "settlement" ->
                        assertEquals(want.string(name), batch.strings(name)[row], label)
                    "valid", "flagged" -> assertEquals(want.bool(name), batch.bools(name)[row], label)
                    else -> assertClose(label, want.double(name), batch.float64s(name)[row])
                }
            }
        }
    }
}

private fun assertClose(label: String, want: Double, got: Double) {
    assertTrue(abs(want - got) <= TOLERANCE, "$label = $got, want $want")
}

private fun Map<String, Any?>.long(key: String): Long = (this[key] as Double).toLong()

private fun Map<String, Any?>.double(key: String): Double = this[key] as Double

private fun Map<String, Any?>.bool(key: String): Boolean = this[key] as Boolean

private fun Map<String, Any?>.string(key: String): String = this[key] as String

/** Parses the flat `[{...}]` shape `fixture_input.json` has. */
private fun parseJsonRows(text: String): List<Map<String, Any?>> {
    @Suppress("UNCHECKED_CAST")
    return parseJson(text) as List<Map<String, Any?>>
}

/**
 * Parses one JSON document into `Map`, `List`, `String`, `Double`, `Boolean`
 * and null.
 *
 * Hand rolled so this module's only Gradle dependency stays `kotlin("test")`,
 * the same reason the codec decodes base64 by hand. It covers everything
 * serde_json writes for a `Vec<Order>` and for the conformance manifest:
 * objects, arrays, strings with the basic escapes, numbers, booleans and null.
 */
internal fun parseJson(text: String): Any? {
    val parser = JsonParser(text)
    val value = parser.value()
    parser.skipSpace()
    require(parser.done()) { "trailing JSON at offset ${parser.pos}" }
    return value
}

private class JsonParser(private val text: String) {
    var pos = 0
        private set

    fun done() = pos >= text.length

    fun skipSpace() {
        while (pos < text.length && text[pos].isWhitespace()) pos++
    }

    fun value(): Any? {
        skipSpace()
        return when (val ch = text[pos]) {
            '{' -> obj()
            '[' -> array()
            '"' -> string()
            't' -> literal("true", true)
            'f' -> literal("false", false)
            'n' -> literal("null", null)
            else -> if (ch == '-' || ch.isDigit()) number() else error("unexpected '$ch' at $pos")
        }
    }

    private fun obj(): Map<String, Any?> {
        val out = LinkedHashMap<String, Any?>()
        pos++
        skipSpace()
        if (text[pos] == '}') { pos++; return out }
        while (true) {
            skipSpace()
            val key = string()
            skipSpace()
            require(text[pos] == ':') { "expected ':' at $pos" }
            pos++
            out[key] = value()
            skipSpace()
            when (text[pos++]) {
                ',' -> continue
                '}' -> return out
                else -> error("expected ',' or '}' at ${pos - 1}")
            }
        }
    }

    private fun array(): List<Any?> {
        val out = ArrayList<Any?>()
        pos++
        skipSpace()
        if (text[pos] == ']') { pos++; return out }
        while (true) {
            out.add(value())
            skipSpace()
            when (text[pos++]) {
                ',' -> continue
                ']' -> return out
                else -> error("expected ',' or ']' at ${pos - 1}")
            }
        }
    }

    private fun string(): String {
        require(text[pos] == '"') { "expected a string at $pos" }
        pos++
        val out = StringBuilder()
        while (text[pos] != '"') {
            if (text[pos] == '\\') {
                pos++
                out.append(
                    when (val esc = text[pos]) {
                        'n' -> '\n'
                        't' -> '\t'
                        'r' -> '\r'
                        'b' -> '\b'
                        'f' -> '\u000c'
                        'u' -> text.substring(pos + 1, pos + 5).toInt(16).toChar().also { pos += 4 }
                        else -> esc
                    }
                )
            } else {
                out.append(text[pos])
            }
            pos++
        }
        pos++
        return out.toString()
    }

    private fun number(): Double {
        val start = pos
        while (pos < text.length && (text[pos].isDigit() || text[pos] in "-+.eE")) pos++
        return text.substring(start, pos).toDouble()
    }

    private fun <T> literal(word: String, value: T): T {
        require(text.startsWith(word, pos)) { "expected $word at $pos" }
        pos += word.length
        return value
    }
}
