// Stage 4 of the polyglot example, the Kotlin processor: the whole thing.
//
// It reads `valid`, `region` and `usd_amount` and writes `fee`. It is the only
// stage that reads a `Utf8` column to drive a decision: the fee rate comes from
// the config key `fee_<region>`, so a region string in the data selects a value
// the host injected.
//
// # Where the rest of it went
//
// There is no `describe`, no Arrow IPC handling, no schema constant, no
// fingerprint constant, no `PipelineImpl`, and no error mapping. `packages/
// saci-sdk-kt-ksp` is a KSP symbol processor: it reads the three annotations
// below at compile time and generates `impl.OrderCodec`, the typed row accessor,
// and `impl.PipelineImpl`, the `saci:pipeline/pipeline` export that folds every
// failure into `run-error::permanent`. `packages/saci-sdk-kt` is the runtime it
// calls: decode the batch, run the transforms, re-encode, report.
//
// Generated rather than reflected because Kotlin/Wasm has no reflection at all.
// `kotlin-reflect` is JVM only, so a property name only exists in this component
// if the build wrote it there.
//
// The generated glue lands in package `impl` because `cargo xtask polyglot` runs
// `wit-bindgen kotlin --kotlin-imports 'impl.*'`, and the export trampoline in
// `bindings/InternalSaciPipeline.kt` resolves `PipelineImpl.describe()` and
// `PipelineImpl.runBatch()` from there by those exact names. So this file is in
// `impl` too, which is what the SDK's processor checks for.
//
// # What the SDK does that in-place mutation cannot
//
// The other byte-mutating stages hand back the input buffer with some fixed-width
// values overwritten. This one decodes rows, mutates them and writes a fresh
// stream, which costs a re-encode and buys the two things the mutating pattern
// rules out: a `Utf8` output column, and a row count that may shrink. `Order`
// carries `usd_amount_display`, a `Utf8` output, so that matters here.
//
// # Why `wall-ns` is zero
//
// Kotlin/Wasm's `wasmWasi` target reaches the outside world through WASI preview
// 1 imports, and `wasm-tools component new --adapt` routes those through the
// preview 1 adapter, where every one of them traps: `kotlin.time.TimeSource.
// Monotonic`, `kotlin.random.Random` and `println` are all unusable here. A
// Kotlin processor may call exactly the imports the WIT world declares, which is
// `host-io` and nothing else, so the SDK reports no timing. The Go, Python and
// TypeScript stages do report real values.

package impl

import io.github.nassor.saci.sdk.SaciComponent
import io.github.nassor.saci.sdk.SaciConfig
import io.github.nassor.saci.sdk.SaciPipeline
import io.github.nassor.saci.sdk.SaciProcessor
import io.github.nassor.saci.sdk.SaciTransform

/**
 * The `Order` component, as the chain's other five processors see it.
 *
 * Property order is schema order, and schema order feeds the buffer walk and the
 * schema fingerprint, so this list is the cross-language contract and reordering
 * it is a wire change. A `val` is an input the earlier stages wrote; a `var` is
 * an output some stage in the chain may write. Wire names are the snake_case of
 * these names, so `usdAmount` is `usd_amount`.
 */
@SaciComponent
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

/**
 * Charges every valid row its region's fee rate.
 *
 * [SaciConfig.double] memoises each key it resolves for the length of the batch,
 * so a per-row lookup here is one `get-config` call per distinct region rather
 * than one per row. An absent or unparseable rate folds into the `0.0` default:
 * the WIT contract hands config over as strings and gives the host no way to see
 * why a batch failed, so a misconfigured region charges nothing and shows up in
 * `fee.charged_rows` instead of taking the batch down.
 *
 * [SaciConfig.metric] accumulates, and the runtime reports each counter once when
 * the batch ends, so these two lines are two host calls per batch however many
 * rows contributed.
 */
@SaciTransform
fun fee(row: Order, config: SaciConfig) {
    row.fee = if (row.valid) row.usdAmount * config.double("fee_${row.region}", 0.0) else 0.0
    if (row.valid) {
        config.metric("fee.charged_rows", 1.0)
        config.metric("fee.total_usd", row.fee)
    }
}

/**
 * The pipeline: one transform, stateless, no checkpoint.
 *
 * The name and version become `pipeline-descriptor.name` and `.version`, and the
 * third argument is the `tracing` target the runtime's per-batch summary line is
 * bridged to.
 */
@SaciProcessor("polyglot-fee-kt", "0.1.0", "polyglot::fee_kt")
fun build(): SaciPipeline = SaciPipeline.of(::fee)

/**
 * Required by `binaries.executable()`, and never called.
 *
 * The host drives this component through the `pipeline` export, so the entry
 * point exists only to satisfy the Kotlin/Wasm executable link step.
 */
fun main() {
}
