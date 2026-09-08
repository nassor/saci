// The row type and accessor `packages/saci-sdk-kt-ksp` generates, written out by
// hand.
//
// Two jobs. It is the fixture every runner test needs, and it is the reference
// for the generator: what KSP emits for the polyglot stage's `Order` must be this
// shape, so a change to the generated template that this file does not follow
// shows up as a compile error here rather than as a broken component.
//
// The twelve fields and their order are the cross-language `Order` contract, and
// the fingerprint in [SdkTest] is computed over exactly this order.

import io.github.nassor.saci.arrowipc.Batch
import io.github.nassor.saci.arrowipc.BoolColumn
import io.github.nassor.saci.arrowipc.Column
import io.github.nassor.saci.arrowipc.FieldSpec
import io.github.nassor.saci.arrowipc.Float64Column
import io.github.nassor.saci.arrowipc.Int64Column
import io.github.nassor.saci.arrowipc.SaciType
import io.github.nassor.saci.arrowipc.Utf8Column
import io.github.nassor.saci.sdk.SaciComponentCodec
import io.github.nassor.saci.sdk.SaciHost
import io.github.nassor.saci.sdk.SaciLogLevel

data class Order(
    val id: Long,
    val region: String,
    val currency: String,
    val amount: Double,
    var valid: Boolean = false,
    var usdAmount: Double = 0.0,
    var usdAmountDisplay: String = "",
    var riskScore: Double = 0.0,
    var flagged: Boolean = false,
    var fee: Double = 0.0,
    var reviewTier: Long = 0,
    var settlement: String = "",
)

object OrderCodec : SaciComponentCodec<Order> {
    override val component: String = "Order"

    override val version: UInt = 1u

    override val fields: List<FieldSpec> = listOf(
        FieldSpec("id", SaciType.INT64),
        FieldSpec("region", SaciType.UTF8),
        FieldSpec("currency", SaciType.UTF8),
        FieldSpec("amount", SaciType.FLOAT64),
        FieldSpec("valid", SaciType.BOOL),
        FieldSpec("usd_amount", SaciType.FLOAT64),
        FieldSpec("usd_amount_display", SaciType.UTF8),
        FieldSpec("risk_score", SaciType.FLOAT64),
        FieldSpec("flagged", SaciType.BOOL),
        FieldSpec("fee", SaciType.FLOAT64),
        FieldSpec("review_tier", SaciType.INT64),
        FieldSpec("settlement", SaciType.UTF8),
    )

    override fun get(row: Order, field: String): Any = when (field) {
        "id" -> row.id
        "region" -> row.region
        "currency" -> row.currency
        "amount" -> row.amount
        "valid" -> row.valid
        "usd_amount" -> row.usdAmount
        "usd_amount_display" -> row.usdAmountDisplay
        "risk_score" -> row.riskScore
        "flagged" -> row.flagged
        "fee" -> row.fee
        "review_tier" -> row.reviewTier
        "settlement" -> row.settlement
        else -> error("component \"Order\" has no field \"$field\"")
    }

    override fun set(row: Order, field: String, value: Any) {
        when (field) {
            "valid" -> row.valid = value as Boolean
            "usd_amount" -> row.usdAmount = value as Double
            "usd_amount_display" -> row.usdAmountDisplay = value as String
            "risk_score" -> row.riskScore = value as Double
            "flagged" -> row.flagged = value as Boolean
            "fee" -> row.fee = value as Double
            "review_tier" -> row.reviewTier = value as Long
            "settlement" -> row.settlement = value as String
            "id", "region", "currency", "amount" ->
                error("component \"Order\" field \"$field\" is read only")

            else -> error("component \"Order\" has no field \"$field\"")
        }
    }

    override fun decode(batch: Batch): MutableList<Order> {
        val rows = batch.rows
        val id = batch.int64s("id")
        val region = batch.strings("region")
        val currency = batch.strings("currency")
        val amount = batch.float64s("amount")
        val valid = batch.bools("valid")
        val usdAmount = batch.float64s("usd_amount")
        val usdAmountDisplay = batch.strings("usd_amount_display")
        val riskScore = batch.float64s("risk_score")
        val flagged = batch.bools("flagged")
        val fee = batch.float64s("fee")
        val reviewTier = batch.int64s("review_tier")
        val settlement = batch.strings("settlement")

        val out = ArrayList<Order>(rows)
        for (row in 0 until rows) {
            out.add(
                Order(
                    id[row],
                    region[row],
                    currency[row],
                    amount[row],
                    valid[row],
                    usdAmount[row],
                    usdAmountDisplay[row],
                    riskScore[row],
                    flagged[row],
                    fee[row],
                    reviewTier[row],
                    settlement[row],
                )
            )
        }
        return out
    }

    override fun encode(rows: List<Order>): Array<Column> {
        val count = rows.size
        return arrayOf(
            Int64Column("id", LongArray(count) { rows[it].id }),
            Utf8Column("region", Array(count) { rows[it].region }),
            Utf8Column("currency", Array(count) { rows[it].currency }),
            Float64Column("amount", DoubleArray(count) { rows[it].amount }),
            BoolColumn("valid", BooleanArray(count) { rows[it].valid }),
            Float64Column("usd_amount", DoubleArray(count) { rows[it].usdAmount }),
            Utf8Column("usd_amount_display", Array(count) { rows[it].usdAmountDisplay }),
            Float64Column("risk_score", DoubleArray(count) { rows[it].riskScore }),
            BoolColumn("flagged", BooleanArray(count) { rows[it].flagged }),
            Float64Column("fee", DoubleArray(count) { rows[it].fee }),
            Int64Column("review_tier", LongArray(count) { rows[it].reviewTier }),
            Utf8Column("settlement", Array(count) { rows[it].settlement }),
        )
    }
}

/** Records every host call, so a test can assert what crossed the boundary. */
class RecordingHost(private val values: Map<String, String> = emptyMap()) : SaciHost {
    val logs = ArrayList<String>()
    val levels = ArrayList<SaciLogLevel>()
    val targets = ArrayList<String>()
    val metrics = LinkedHashMap<String, Double>()
    val configKeys = ArrayList<String>()

    override fun log(level: SaciLogLevel, target: String, message: String) {
        levels.add(level)
        targets.add(target)
        logs.add(message)
    }

    override fun metric(name: String, value: Double) {
        metrics[name] = value
    }

    override fun config(key: String): String? {
        configKeys.add(key)
        return values[key]
    }
}
