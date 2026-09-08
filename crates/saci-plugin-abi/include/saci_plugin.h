/*
 * saci_plugin.h -- the C ABI a SACI native plugin exports.
 *
 * A native plugin is a shared library the host loads with dlopen or
 * LoadLibrary. It mirrors the saci:pipeline@0.3.0 WIT world a WebAssembly processor
 * implements: two entry points, four vtable calls, three host callbacks, Arrow
 * IPC bytes as the data plane, one opaque checkpoint blob for state that
 * crosses a batch boundary.
 *
 * The Rust definition of these types is crates/saci-plugin-abi/src/lib.rs, and
 * its tests assert the sizes and alignments below. Change one side and the
 * other fails `cargo test -p saci-plugin-abi`.
 *
 *
 * OWNERSHIP
 *
 *   SaciSlice          borrowed. Valid for the call that received it, no
 *                     longer. The one exception is the slice get_config
 *                     writes: it stays valid for the life of the instance,
 *                     because host config is immutable once built.
 *   SaciBuffer         owned by whoever allocated it. Every buffer a plugin
 *                     writes goes back to free_buffer with ptr, len and cap
 *                     unchanged. The host never frees plugin memory, and never
 *                     hands a plugin a buffer the host allocated.
 *   SaciPluginV1       host-allocated. saci_plugin_v1 fills a caller-provided
 *                     struct, so nothing allocates the vtable and nothing
 *                     frees it. destroy releases only `instance`.
 *   SaciHostV1         host-owned, and kept alive for the whole life of the
 *                     instance, because the plugin stores the pointer.
 *
 * Every function pointer below is nullable. The host zero-fills SaciPluginV1
 * before calling saci_plugin_v1 and then checks each slot, so a plugin that
 * forgets one gets a clean load-time error instead of a jump through null. A
 * plugin should check the SaciHostV1 slots for the same reason.
 *
 *
 * THREADING
 *
 * The host never calls into one instance concurrently. Successive calls may
 * arrive on different OS threads, so no state may be thread-affine across
 * calls. A Go plugin gets its own scheduler, a JVM plugin its own isolate;
 * neither may assume the caller's thread identity is stable.
 *
 *
 * UNWINDING
 *
 * No panic and no exception may cross the boundary. Wrap every exported body:
 * catch_unwind in Rust, recover() in Go, try/catch in C# and Kotlin.
 *
 * describe and run_batch are declared extern "C-unwind" on the Rust side
 * (crates/saci-plugin-abi/src/lib.rs), which is why their C prototypes below
 * are otherwise unchanged: an unwind can cross a C-unwind boundary as a
 * defined, catchable control-flow path, so a Rust host wrapping the call in
 * catch_unwind can turn an unguarded Rust plugin's panic into an error
 * instead of a process abort. free_buffer and destroy stay plain extern "C".
 *
 * This helps a Rust plugin only, and only if it skips its own guard. It does
 * nothing for a Go panic or a .NET/Kotlin exception: neither is a Rust
 * unwind, and one reaching a Rust catch_unwind is defined to abort regardless
 * of the extern ABI, because Rust cannot safely resume an exception whose
 * cleanup semantics it does not understand. It does nothing for a plugin
 * built with panic = "abort", and nothing for memory corruption. Guard your
 * own exported bodies; do not rely on the host.
 *
 *
 * NO PREEMPTION
 *
 * A plugin runs in-process with full host privileges. There is no equivalent of
 * the wasmtime epoch deadline that bounds a WebAssembly processor: a plugin that
 * wedges wedges its caller, and one that corrupts memory takes the host with
 * it. The host's only integrity gate is the optional sha3_256 digest in the
 * service config. Plugin paths are operator-trusted.
 *
 *
 * COMPILING A PLUGIN
 *
 *   Rust    crate-type = ["cdylib"]; use saci_plugin::export_plugin!
 *           cargo build --release -p my-plugin
 *
 *   Go      //export saci_abi_version and //export saci_plugin_v1 under cgo
 *           go build -buildmode=c-shared -o my_plugin.so .
 *           The Go runtime installs signal handlers and starts its GC at load
 *           time, and GOMAXPROCS competes with the host's tokio and rayon
 *           pools. cgo cannot call a C function pointer directly, so calling
 *           back into SaciHostV1 needs a small static shim in the preamble.
 *
 *   C#      <PublishAot>true</PublishAot> plus
 *           [UnmanagedCallersOnly(EntryPoint = "saci_abi_version")]
 *           dotnet publish -r linux-x64 -c Release
 *           An UnmanagedCallersOnly method cannot throw across the boundary,
 *           so each needs its own try/catch mapping to SaciStatus.
 *
 *   Kotlin  GraalVM @CEntryPoint(name = "saci_abi_version")
 *           native-image --shared
 *           A CEntryPoint takes an IsolateThread, so create one isolate in
 *           saci_plugin_v1 and attach per call.
 *
 * The data plane is the same Arrow IPC framing a WebAssembly processor exchanges,
 * specified in docs/content/library/reference/wire-format.md. The five packaged codecs
 * under packages/arrow-ipc-* read and write it with no Arrow dependency.
 */

#ifndef SACI_PLUGIN_H
#define SACI_PLUGIN_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

 * behaves exactly as before. Minor 2 appended routes/has_routes to
 * SaciRunResult; a minor-1 plugin leaves them zeroed, which the host reads as
 * "no routing decision", so it loads and behaves exactly as before. */
#define SACI_ABI_VERSION 0x00010002u

/* SaciStatus values. OK is zero. */
#define SACI_STATUS_OK 0
#define SACI_STATUS_RETRYABLE 1
#define SACI_STATUS_PERMANENT 2
/* Reserved for load time. run_batch must never return it; the host folds a
 * mid-batch schema mismatch into the permanent path. */
#define SACI_STATUS_SCHEMA_MISMATCH 3

/* Log levels for SaciHostV1.log, in WIT host-io::log-level order. */
#define SACI_LOG_TRACE 0u
#define SACI_LOG_DEBUG 1u
#define SACI_LOG_INFO 2u
#define SACI_LOG_WARN 3u
#define SACI_LOG_ERROR 4u

typedef int32_t SaciStatus;

/* sizeof 16, alignof 8 */
typedef struct {
    const uint8_t *ptr;
    size_t len;
} SaciSlice;

/* sizeof 24, alignof 8 */
typedef struct {
    uint8_t *ptr;
    size_t len;
    size_t cap;
} SaciBuffer;

/* sizeof 32, alignof 8. Mirrors the WIT run-metrics record. */
typedef struct {
    uint64_t wall_ns;
    uint64_t rows_in;
    uint64_t rows_out;
    uint32_t systems_run;
    uint32_t retries;
} SaciRunMetrics;

/* sizeof 120, alignof 8 */
typedef struct {
    /* Arrow IPC bytes for the mutated dataset. Plugin-owned. */
    SaciBuffer output;
    /* The plugin's new state blob. Plugin-owned. Read only when
     * has_checkpoint is non-zero. */
    SaciBuffer checkpoint;
    /* Non-zero when checkpoint carries a blob the host must persist. Zero means
     * stateless for this batch, which is not the same as a present but empty
     * blob. */
    int32_t has_checkpoint;
    SaciRunMetrics metrics;
    /* UTF-8 JSON array of branch names this batch's output is delivered to.
     * Plugin-owned. Read only when has_routes is non-zero. A null buffer means
     * no routing decision (legacy multicast). */
    SaciBuffer routes;
    /* Non-zero when routes carries a JSON list. */
    int32_t has_routes;
} SaciRunResult;

/* Host capabilities, callable only while a vtable call is in progress.
 * sizeof 32, alignof 8. */
typedef struct {
    void *ctx;
    /* level is one of the SACI_LOG_* constants; anything else is treated as
     * SACI_LOG_INFO. */
    void (*log)(void *ctx, uint32_t level, SaciSlice target, SaciSlice message);
    void (*metric)(void *ctx, SaciSlice name, double value);
    /* Returns 1 and writes *out when the key is present, 0 and leaves *out
     * untouched when absent. The written slice stays valid for the life of the
     * instance. */
    int32_t (*get_config)(void *ctx, SaciSlice key, SaciSlice *out);
} SaciHostV1;

/* The plugin's vtable. sizeof 40, alignof 8. */
typedef struct {
    /* Opaque plugin state, released by destroy. A stateless plugin may leave
     * this null. */
    void *instance;

    /* Identity and component schemas, once at load. Writes plugin-owned UTF-8
     * JSON into *manifest_json:
     *
     *   {
     *     "name": "settle-go",
     *     "version": "0.1.0",
     *     "stateful": false,
     *     "schema_fingerprint": "d52f95a6",
     *     "components": [
     *       { "name": "Order", "arrow_schema_ipc_base64": "/////zgCAAAQ..." }
     *     ]
     *   }
     *
     * Every field is required and unknown fields are rejected. The schema bytes
     * are one schema-only Arrow IPC stream per component, base64 with padding.
     * schema_fingerprint is lowercase 8-char hex, and the host recomputes it
     * from the decoded schemas and refuses a mismatch. On failure, write a UTF-8
     * message into *err and return a non-OK status. */
    SaciStatus (*describe)(void *instance, SaciBuffer *manifest_json, SaciBuffer *err);

    /* Run one batch. input is Arrow IPC bytes. prior is the blob this plugin
     * returned last batch, readable only when has_prior is non-zero. On success
     * fill *out and return SACI_STATUS_OK. On failure write a UTF-8 message into
     * *err and return SACI_STATUS_RETRYABLE or SACI_STATUS_PERMANENT. */
    SaciStatus (*run_batch)(void *instance,
                           SaciSlice input,
                           SaciSlice prior,
                           int32_t has_prior,
                           SaciRunResult *out,
                           SaciBuffer *err);

    /* Release a buffer this plugin allocated. A null buffer is a no-op. */
    void (*free_buffer)(void *instance, SaciBuffer buffer);

    /* Release instance. Called once, last. */
    void (*destroy)(void *instance);
} SaciPluginV1;

/* The two symbols a plugin exports, and nothing else. */

/* Must return SACI_ABI_VERSION. The host calls this first and refuses the
 * library on a major mismatch or a minor newer than its own. */
uint32_t saci_abi_version(void);

/* Fill *out. host stays valid for the life of the instance. Return
 * SACI_STATUS_OK on success, or a non-OK status to refuse the load. */
SaciStatus saci_plugin_v1(const SaciHostV1 *host, SaciPluginV1 *out);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* SACI_PLUGIN_H */
