// Exercises the SDK against real wire bytes on the JVM, with no WebAssembly
// involved.
//
// The runner is the whole point of the module, so most of these cases drive
// [SaciRunner.runBatch] end to end: a stream written by the codec's writer goes in,
// transforms mutate row objects, and a stream the codec's reader accepts comes
// out. The fingerprint cases pin the two values the cross-language contract
// names, one of which the canonical Rust `Order` also produces.

import io.github.nassor.saci.arrowipc.BoolColumn
import io.github.nassor.saci.arrowipc.Column
import io.github.nassor.saci.arrowipc.Float64Column
import io.github.nassor.saci.arrowipc.Int64Column
import io.github.nassor.saci.arrowipc.SaciStream
import io.github.nassor.saci.arrowipc.SaciStreamWriter
import io.github.nassor.saci.arrowipc.SaciType
import io.github.nassor.saci.arrowipc.Utf8Column
import io.github.nassor.saci.sdk.SaciConfig
import io.github.nassor.saci.sdk.SaciLogLevel
import io.github.nassor.saci.sdk.SaciPipeline
import io.github.nassor.saci.sdk.SaciRunner
import io.github.nassor.saci.sdk.saciFingerprint
import io.github.nassor.saci.sdk.saciWireName
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

private const val TOLERANCE = 1e-12

/** Five rows, three regions, the last one invalid. */
private val SEED = listOf(
    Order(1, "emea", "EUR", 100.0, valid = true, usdAmount = 110.0),
    Order(2, "emea", "GBP", 50.0, valid = true, usdAmount = 62.5),
    Order(3, "apac", "JPY", 1_000_000.0, valid = true, usdAmount = 6_700.0),
    Order(4, "amer", "USD", 60_000.0, valid = true, usdAmount = 60_000.0),
    Order(5, "emea", "EUR", 0.0, valid = false, usdAmount = 0.0),
)

private fun streamOf(rows: List<Order>, alive: Int = rows.size): ByteArray =
    SaciStreamWriter()
        .writeComponent("Order", 1u, *OrderCodec.encode(rows))
        .writeAlive(BooleanArray(alive) { true })
        .toBytes()

/** The transform the polyglot Kotlin stage is built around. */
private fun fee(row: Order, config: SaciConfig) {
    row.fee = if (row.valid) row.usdAmount * config.double("fee_${row.region}", 0.0) else 0.0
    if (row.valid) {
        config.metric("fee.charged_rows", 1.0)
        config.metric("fee.total_usd", row.fee)
    }
}

class SdkTest {
    private val rates = mapOf("fee_emea" to "0.01", "fee_apac" to "0.02", "fee_amer" to "0.005")

    // -----------------------------------------------------------------------
    // Identity.
    // -----------------------------------------------------------------------

    @Test
    fun theFingerprintMatchesTheCrossLanguageVectors() {
        assertEquals(
            "f6405a7b",
            saciFingerprint("Order", 1u, OrderCodec.fields.map { it.name }),
            "the twelve-field Order fingerprint",
        )
        assertEquals("43623dda", saciFingerprint("X", 1u, listOf("x")))
    }

    /** Names and versions only: a type change must not move the fingerprint. */
    @Test
    fun theFingerprintIgnoresTypesAndFollowsFieldOrder() {
        val names = OrderCodec.fields.map { it.name }
        assertEquals(saciFingerprint("Order", 1u, names), saciFingerprint("Order", 1u, names))
        assertTrue(saciFingerprint("Order", 2u, names) != saciFingerprint("Order", 1u, names))
        assertTrue(
            saciFingerprint("Order", 1u, names.reversed()) != saciFingerprint("Order", 1u, names),
            "field order must be load bearing",
        )
        assertTrue(saciFingerprint("Order", 1u, names + "extra") != saciFingerprint("Order", 1u, names))
    }

    @Test
    fun wireNamesFollowTheDocumentedRule() {
        assertEquals("id", saciWireName("id"))
        assertEquals("usd_amount", saciWireName("usdAmount"))
        assertEquals("usd_amount_display", saciWireName("usdAmountDisplay"))
        assertEquals("risk_score", saciWireName("riskScore"))
        assertEquals("review_tier", saciWireName("reviewTier"))
        assertEquals("http_url", saciWireName("httpURL"))
        assertEquals("url_path", saciWireName("URLPath"))
        assertEquals("amount2_usd", saciWireName("amount2Usd"))
        assertEquals("already_snake", saciWireName("already_snake"))
    }

    @Test
    fun theRunnerDerivesItsIdentityFromTheGeneratedFields() {
        val runner = runner()
        assertEquals("Order", runner.component)
        assertEquals("f6405a7b", runner.fingerprint)
        assertEquals("polyglot-fee-kt", runner.pipeline.name)
        assertEquals("0.1.0", runner.pipeline.version)
        assertEquals("polyglot::fee_kt", runner.logTarget)

        val schema = runner.schemaIpc.toString(Charsets.ISO_8859_1)
        for (field in OrderCodec.fields) {
            assertTrue(schema.contains(field.name), "the schema omits \"${field.name}\"")
        }
        assertTrue(
            !schema.contains("__saci_component"),
            "descriptor bytes must carry no custom_metadata",
        )
    }

    @Test
    fun anEmptyLogTargetFallsBackToTheProcessorName() {
        val runner = SaciRunner(
            SaciPipeline.of(::fee).copy(name = "bare", version = "0.0.1"),
            OrderCodec,
            RecordingHost(),
        )
        assertEquals("bare", runner.logTarget)
    }

    // -----------------------------------------------------------------------
    // The batch round trip.
    // -----------------------------------------------------------------------

    @Test
    fun aBatchRoundTripsThroughTheRowModel() {
        val host = RecordingHost(rates)
        val runner = runner(host)
        val result = runner.runBatch(streamOf(SEED))

        assertEquals(5, result.rowsIn)
        assertEquals(5, result.rowsOut)
        assertEquals(1, result.systemsRun)

        val batch = SaciStream.parse(result.output).component("Order")
        assertEquals(OrderCodec.fields.map { it.name }, batch.fieldNames())
        assertEquals(5, batch.rows)

        val fees = batch.float64s("fee")
        assertClose(110.0 * 0.01, fees[0], "fee[0]")
        assertClose(62.5 * 0.01, fees[1], "fee[1]")
        assertClose(6_700.0 * 0.02, fees[2], "fee[2]")
        assertClose(60_000.0 * 0.005, fees[3], "fee[3]")
        assertClose(0.0, fees[4], "fee[4]")

        // Every untouched column survives the decode and re-encode.
        assertContentEquals(longArrayOf(1, 2, 3, 4, 5), batch.int64s("id"))
        assertEquals(listOf("emea", "emea", "apac", "amer", "emea"), batch.strings("region"))
        assertEquals(listOf("EUR", "GBP", "JPY", "USD", "EUR"), batch.strings("currency"))
        assertContentEquals(booleanArrayOf(true, true, true, true, false), batch.bools("valid"))
        assertContentEquals(BooleanArray(5), batch.bools("flagged"))
        assertEquals(List(5) { "" }, batch.strings("settlement"))
        assertContentEquals(
            BooleanArray(5) { true },
            SaciStream.parse(result.output).component("__alive").bools("alive"),
        )
    }

    /** One `get-config` per distinct key, and two metric calls whatever the rows. */
    @Test
    fun theHostIsCalledOncePerDistinctKeyAndOncePerMetric() {
        val host = RecordingHost(rates)
        runner(host).runBatch(streamOf(SEED))

        assertEquals(
            listOf("fee_emea", "fee_apac", "fee_amer"),
            host.configKeys,
            "three regions over five rows",
        )
        assertEquals(
            listOf("fee.charged_rows", "fee.total_usd"),
            host.metrics.keys.toList(),
            "two counters, in first-touched order",
        )
        assertEquals(4.0, host.metrics.getValue("fee.charged_rows"))
        assertClose(
            110.0 * 0.01 + 62.5 * 0.01 + 6_700.0 * 0.02 + 60_000.0 * 0.005,
            host.metrics.getValue("fee.total_usd"),
            "fee.total_usd",
        )
        assertEquals(1, host.logs.size)
        assertEquals(SaciLogLevel.INFO, host.levels[0])
        assertEquals("polyglot::fee_kt", host.targets[0])
        assertTrue(host.logs[0].startsWith("polyglot-fee-kt:"), host.logs[0])
    }

    /** An absent config key falls back to the default rather than failing. */
    @Test
    fun aMissingRateChargesNothing() {
        val host = RecordingHost(mapOf("fee_emea" to "0.01"))
        runner(host).runBatch(streamOf(SEED))
        assertEquals(listOf("fee_emea", "fee_apac", "fee_amer"), host.configKeys)
        assertClose(
            110.0 * 0.01 + 62.5 * 0.01,
            host.metrics.getValue("fee.total_usd"),
            "fee.total_usd",
        )
    }

    /** An unparseable value is a misconfiguration, and folds into the default. */
    @Test
    fun anUnparseableRateChargesNothing() {
        val host = RecordingHost(mapOf("fee_emea" to "cheap"))
        runner(host).runBatch(streamOf(SEED))
        assertClose(0.0, host.metrics.getValue("fee.total_usd"), "fee.total_usd")
    }

    /** Systems run outermost, so a later one reads what an earlier one wrote. */
    @Test
    fun transformsRunInRegistrationOrderOverTheWholeBatch() {
        val order = ArrayList<String>()
        val first: (Order, SaciConfig) -> Unit = { row, _ ->
            order.add("first:${row.id}")
            row.reviewTier = row.id
        }
        val second: (Order, SaciConfig) -> Unit = { row, _ ->
            order.add("second:${row.id}")
            row.settlement = "tier-${row.reviewTier}"
        }

        val host = RecordingHost()
        val result = SaciRunner(
            SaciPipeline.of(first, second).copy(name = "two", version = "1"),
            OrderCodec,
            host,
        ).runBatch(streamOf(SEED))

        assertEquals(
            listOf("first:1", "first:2", "first:3", "first:4", "first:5") +
                listOf("second:1", "second:2", "second:3", "second:4", "second:5"),
            order,
        )
        val batch = SaciStream.parse(result.output).component("Order")
        assertEquals(
            listOf("tier-1", "tier-2", "tier-3", "tier-4", "tier-5"),
            batch.strings("settlement"),
        )
        assertEquals(2, result.systemsRun)
    }

    /**
     * A `Utf8` output and a shrinking row count are the two things in-place
     * mutation cannot do, so they are the reason this SDK re-encodes.
     */
    @Test
    fun aUtf8OutputAndAShorterBatchSurviveTheReEncode() {
        val label: (Order, SaciConfig) -> Unit = { row, _ ->
            row.usdAmountDisplay = "USD ${row.usdAmount}"
        }
        val host = RecordingHost()
        val runner = SaciRunner(
            SaciPipeline.of(label).copy(name = "label", version = "1"),
            OrderCodec,
            host,
        )

        val input = streamOf(SEED.take(3), alive = 5)
        val result = runner.runBatch(input)
        assertEquals(3, result.rowsOut)

        val batch = SaciStream.parse(result.output).component("Order")
        assertEquals(3, batch.rows)
        assertEquals(
            listOf("USD 110.0", "USD 62.5", "USD 6700.0"),
            batch.strings("usd_amount_display"),
        )
        // The bitmap is the stream's row bound and passes through untouched.
        assertEquals(5, SaciStream.parse(result.output).component("__alive").rows)
    }

    @Test
    fun aZeroRowBatchIsAValidBatch() {
        val host = RecordingHost(rates)
        val result = runner(host).runBatch(streamOf(emptyList(), alive = 4))
        assertEquals(0, result.rowsOut)
        assertEquals(0, SaciStream.parse(result.output).component("Order").rows)
        assertTrue(host.metrics.isEmpty(), "no row contributed, so no counter was touched")
    }

    // -----------------------------------------------------------------------
    // Refusals.
    // -----------------------------------------------------------------------

    @Test
    fun theRunnerRefusesAStreamWithoutItsComponent() {
        val other = SaciStreamWriter()
            .writeComponent("Ping", 1u, Int64Column("id", longArrayOf(1)))
            .writeAlive(booleanArrayOf(true))
            .toBytes()
        val error = assertFailsWith<Exception> { runner().runBatch(other) }
        assertTrue(
            error.message!!.contains("no segment declares component \"Order\""),
            "unexpected message: ${error.message}",
        )
    }

    @Test
    fun theByNameRowViewReadsEveryFieldAndRefusesAReadOnlyWrite() {
        val row = SEED[0].copy()
        for (field in OrderCodec.fields) {
            val value: Any = OrderCodec.get(row, field.name)
            when (field.type) {
                SaciType.INT64 -> assertTrue(value is Long, "${field.name} is ${value::class}")
                SaciType.FLOAT64 -> assertTrue(value is Double, "${field.name} is ${value::class}")
                SaciType.BOOL -> assertTrue(value is Boolean, "${field.name} is ${value::class}")
                SaciType.UTF8 -> assertTrue(value is String, "${field.name} is ${value::class}")
            }
        }

        OrderCodec.set(row, "fee", 7.5)
        assertEquals(7.5, OrderCodec.get(row, "fee"))

        val error = assertFailsWith<IllegalStateException> { OrderCodec.set(row, "id", 9L) }
        assertTrue(error.message!!.contains("read only"), error.message!!)
        assertFailsWith<IllegalStateException> { OrderCodec.get(row, "nope") }
    }

    /** The generated column order must be the generated field order. */
    @Test
    fun theEncodedColumnOrderMatchesTheFieldOrder() {
        val columns: Array<Column> = OrderCodec.encode(SEED)
        assertEquals(OrderCodec.fields, columns.map { it.spec })
        for (column in columns) assertEquals(SEED.size, column.rows)
    }

    private fun runner(host: RecordingHost = RecordingHost(rates)) = SaciRunner(
        SaciPipeline.of(::fee).copy(name = "polyglot-fee-kt", version = "0.1.0"),
        OrderCodec,
        host,
        "polyglot::fee_kt",
    )

    private fun assertClose(want: Double, got: Double, label: String) {
        assertTrue(abs(want - got) <= TOLERANCE, "$label = $got, want $want")
    }

}
