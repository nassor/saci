// The processor's side of the WIT `host-io` interface.
//
// The generated `bindings` package cannot be a library dependency: `wit-bindgen
// kotlin` writes it into each stage's own source tree, its output is documented
// as non-deterministic, and two copies of package `bindings` on one compilation
// would be a redeclaration. So the runtime talks to [SaciHost] and the
// KSP-generated glue in the stage implements it over `bindings.HostIo` in four
// lines. That also makes every path in this module testable on the JVM against a
// recording fake, which is where the round-trip tests run.

package io.github.nassor.saci.sdk

/** Mirrors WIT `host-io.log-level`. */
enum class SaciLogLevel { TRACE, DEBUG, INFO, WARN, ERROR }

/**
 * The three capabilities a processor may reach for during `run-batch`.
 *
 * There is nothing else: no filesystem, no network, no clock. A Kotlin/Wasm
 * processor cannot even reach `kotlin.time.TimeSource.Monotonic`, because
 * Kotlin's WASI preview 1 imports trap once `wasm-tools component new --adapt`
 * has wrapped the module.
 */
interface SaciHost {
    fun log(level: SaciLogLevel, target: String, message: String)

    fun metric(name: String, value: Double)

    /** WIT `get-config`, which hands values over as strings. */
    fun config(key: String): String?
}

/**
 * The per-batch handle a transform is given.
 *
 * Both of its jobs are about not crossing the component boundary more often than
 * the work needs. `get-config` and `metric` are host calls, and a transform runs
 * once per row, so [double] memoises every key it resolves and [metric]
 * accumulates into a counter the runner flushes once when the batch ends. A
 * per-region fee rate therefore costs one host call per distinct region per
 * batch, and two metric counters cost two host calls per batch however many rows
 * contributed to them.
 *
 * One instance per batch: the memo is only sound for as long as the config the
 * host injected cannot have changed under it.
 */
class SaciConfig internal constructor(private val host: SaciHost) {
    private val numbers = HashMap<String, Double>()
    private val counters = LinkedHashMap<String, Double>()

    /**
     * A host-injected numeric config value, or [default] when the key is absent
     * or does not parse as a number.
     *
     * The WIT contract leaves numeric parsing to the processor and offers no way
     * to tell "absent" from "unparseable" to the host, so both fold into
     * [default] rather than failing a batch the operator cannot see the reason
     * for. A transform that must distinguish them reads [SaciHost.config].
     */
    fun double(key: String, default: Double): Double =
        numbers.getOrPut(key) { host.config(key)?.toDoubleOrNull() ?: default }

    /** Adds [value] to the named counter, reported once when the batch ends. */
    fun metric(name: String, value: Double) {
        counters[name] = (counters[name] ?: 0.0) + value
    }

    /** Reports every accumulated counter, in first-touched order. */
    internal fun flush() {
        for ((name, value) in counters) host.metric(name, value)
    }

    /** Distinct config keys this batch resolved, for the runner's log line. */
    internal val resolvedKeys: Int get() = numbers.size
}
