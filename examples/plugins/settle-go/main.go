// Command settle-go is the Go reference implementation of the SACI native plugin
// ABI, a shared library the host loads with dlopen instead of a WebAssembly
// component it instantiates.
//
// It operates on the same `Order` component the polyglot WASM example uses, so
// the two processor paths are directly comparable. It reads `amount` and
// `currency`, converts each order through a host supplied rate, and writes
// `review_tier`, the settlement disposition:
//
//	0  clear to settle
//	1  over the escalation threshold, hold for manual review
//	2  nothing to settle, the converted amount is not positive
//
// Two config keys steer it. `settle.escalate_above` is the tier 1 threshold,
// default 10000. `settle.rate_<CURRENCY>` is the multiplier for one currency,
// default 1.0, so a value the host injected selects the rate for each row from
// the row's own data.
//
// # Why review_tier and not settlement
//
// `settlement` is the schema's only Utf8 column. Overwriting a variable width
// value moves every following offset and forces a rewrite of the RecordBatch
// flatbuffer, which the codec refuses; docs/content/library/reference/wire-format.md
// covers why. `review_tier` is the schema's only Int64 output, so it is the
// fixed width slot this plugin can overwrite in place.
//
// # The C boundary
//
// Four rules the host relies on, all of them from
// crates/saci-plugin-abi/include/saci_plugin.h:
//
//   - Every buffer handed to the host is C.malloc'd with cap equal to len and
//     comes back through saci_go_free_buffer. A Go pointer would let the
//     collector move or reclaim bytes the host still holds.
//   - Every exported body recovers. A Go panic crossing an extern "C" frame
//     aborts the process, and the host has no way to report why.
//   - cgo cannot call a C function pointer, so every call into SaciHostV1 and
//     the vtable fill itself go through the shims in the preamble.
//   - Per instance state hangs off the cookie the host holds as
//     SaciPluginV1.instance. A c-shared library has one Go runtime per process,
//     so a package variable would make two instances share one host vtable.
package main

/*
#cgo CFLAGS: -I${SRCDIR}/../../../crates/saci-plugin-abi/include

#include <stdlib.h>
#include <stdint.h>

// saci_plugin.h is the authority for every type here, but it declares
// saci_plugin_v1 as taking `const SaciHostV1 *`. cgo emits its own declaration
// for an //export function and has no way to spell const, and C treats
// `SaciHostV1 *` and `const SaciHostV1 *` as conflicting types once both land in
// one translation unit, which they do: cgo copies this preamble into
// _cgo_export.h. Renaming the header's prototype out of the way leaves the
// types intact and lets cgo own the definition. The host resolves the symbol by
// name, so nothing about the ABI changes.
#define saci_plugin_v1 saci_plugin_v1_prototype
#include "saci_plugin.h"
#undef saci_plugin_v1

// The four vtable entry points, implemented in Go below. cgo writes the same
// declarations into _cgo_export.h, which this file's own preamble may not
// include, so the shim reaches them through a compatible re-declaration.
SaciStatus saci_go_describe(void *instance, SaciBuffer *manifest_json, SaciBuffer *err);
SaciStatus saci_go_run_batch(void *instance, SaciSlice input, SaciSlice prior,
                           int32_t has_prior, SaciRunResult *out, SaciBuffer *err);
void saci_go_free_buffer(void *instance, SaciBuffer buffer);
void saci_go_destroy(void *instance);

// A file using //export must keep its preamble to declarations, because cgo
// copies the preamble into more than one C output file. `static inline` respects
// that: every copy has internal linkage, so nothing collides at link time, and
// an unused copy draws no warning.

// saci_shim_fill installs the vtable. Taking the address of an //export function
// is the one thing cgo cannot express, so C does it.
static inline void saci_shim_fill(SaciPluginV1 *out, void *instance) {
    out->instance = instance;
    out->describe = saci_go_describe;
    out->run_batch = saci_go_run_batch;
    out->free_buffer = saci_go_free_buffer;
    out->destroy = saci_go_destroy;
}

// The three host callbacks. Each slot is nullable, so each shim checks before
// it jumps.
static inline void saci_shim_log(const SaciHostV1 *host, uint32_t level,
                                SaciSlice target, SaciSlice message) {
    if (host != NULL && host->log != NULL) {
        host->log(host->ctx, level, target, message);
    }
}

static inline void saci_shim_metric(const SaciHostV1 *host, SaciSlice name, double value) {
    if (host != NULL && host->metric != NULL) {
        host->metric(host->ctx, name, value);
    }
}

static inline int32_t saci_shim_get_config(const SaciHostV1 *host, SaciSlice key, SaciSlice *out) {
    if (host == NULL || host->get_config == NULL) {
        return 0;
    }
    return host->get_config(host->ctx, key, out);
}
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	arrowipc "github.com/nassor/saci/packages/saci-sdk-go/arrowipc"
)

const (
	// pluginName is the manifest name the host treats as this pipeline's
	// identity, and the prefix on every message it logs.
	pluginName    = "settle-go"
	pluginVersion = "0.1.0"

	componentName = "Order"

	fieldAmount     = "amount"
	fieldCurrency   = "currency"
	fieldReviewTier = "review_tier"

	// escalateAboveKey is the converted amount at or above which a row holds
	// for manual review.
	escalateAboveKey     = "settle.escalate_above"
	escalateAboveDefault = 10000.0

	// ratePrefix plus the row's currency names the multiplier that converts
	// `amount` into the unit the threshold is written in.
	ratePrefix  = "settle.rate_"
	rateDefault = 1.0

	tierClear     = 0
	tierReview    = 1
	tierUnsettled = 2

	// blockedMetric counts the rows that did not clear, tier 1 and tier 2
	// together.
	blockedMetric = "settle.blocked_rows"
)

// Statuses from saci_plugin.h. Nothing here is retryable: every failure is a bad
// input shape, a bad config value or a plugin bug, and the same bytes with the
// same config would fail again.
const (
	statusOK        = C.SaciStatus(C.SACI_STATUS_OK)
	statusPermanent = C.SaciStatus(C.SACI_STATUS_PERMANENT)
)

// manifest is the JSON `describe` writes. Every field is required and the host
// rejects a key it does not know.
type manifest struct {
	Name              string              `json:"name"`
	Version           string              `json:"version"`
	Stateful          bool                `json:"stateful"`
	SchemaFingerprint string              `json:"schema_fingerprint"`
	Components        []manifestComponent `json:"components"`
}

type manifestComponent struct {
	Name string `json:"name"`
	// SchemaIPCBase64 is one schema only Arrow IPC stream, base64 with padding.
	SchemaIPCBase64 string `json:"arrow_schema_ipc_base64"`
}

// instance is the state behind one SaciPluginV1.instance cookie.
//
// The host keeps its SaciHostV1 alive for the whole life of the instance, so
// holding the pointer is safe. It is held per cookie rather than per process
// because two loads of this library share one Go runtime.
type instance struct {
	host *C.SaciHostV1
}

var (
	instancesMu sync.Mutex
	instances   = map[uintptr]*instance{}
)

func lookupInstance(cookie unsafe.Pointer) (*instance, error) {
	instancesMu.Lock()
	defer instancesMu.Unlock()
	inst, ok := instances[uintptr(cookie)]
	if !ok {
		return nil, fmt.Errorf("%s: no instance registered for cookie %#x", pluginName, uintptr(cookie))
	}
	return inst, nil
}

// ---------------------------------------------------------------------------
// The two symbols the host looks up.
// ---------------------------------------------------------------------------

//export saci_abi_version
func saci_abi_version() C.uint32_t {
	return C.uint32_t(C.SACI_ABI_VERSION)
}

//export saci_plugin_v1
func saci_plugin_v1(host *C.SaciHostV1, out *C.SaciPluginV1) (status C.SaciStatus) {
	// saci_plugin_v1 has no error buffer, so a refused load is a status and
	// nothing more. The host names the library and the version it read.
	defer func() {
		if r := recover(); r != nil {
			status = statusPermanent
		}
	}()

	if out == nil {
		return statusPermanent
	}

	// One byte of C memory, used only as an identity the host can hand back.
	// This plugin keeps no per batch state, so there is nothing else in it.
	cookie := C.malloc(1)

	instancesMu.Lock()
	instances[uintptr(cookie)] = &instance{host: host}
	instancesMu.Unlock()

	C.saci_shim_fill(out, cookie)
	return statusOK
}

// ---------------------------------------------------------------------------
// The vtable.
// ---------------------------------------------------------------------------

//export saci_go_describe
func saci_go_describe(cookie unsafe.Pointer, manifestJSON *C.SaciBuffer, errOut *C.SaciBuffer) (status C.SaciStatus) {
	defer func() {
		if r := recover(); r != nil {
			status = failure(errOut, "%s: panic in describe: %v", pluginName, r)
		}
	}()

	if manifestJSON == nil {
		return failure(errOut, "%s: describe called with a null manifest buffer", pluginName)
	}
	if _, err := lookupInstance(cookie); err != nil {
		return failure(errOut, "%v", err)
	}

	// The schema bytes reach the manifest as the generated base64 constant, so
	// nothing here encodes a flatbuffer. Decoding once proves the constant is
	// well formed, and names this plugin rather than the host when it is not.
	if _, err := arrowipc.DecodeBase64(OrderSchemaIPCBase64); err != nil {
		return failure(errOut, "%s: OrderSchemaIPCBase64 does not decode: %v", pluginName, err)
	}

	body, err := json.Marshal(manifest{
		Name:              pluginName,
		Version:           pluginVersion,
		Stateful:          false,
		SchemaFingerprint: OrderFingerprint,
		Components: []manifestComponent{{
			Name:            componentName,
			SchemaIPCBase64: OrderSchemaIPCBase64,
		}},
	})
	if err != nil {
		return failure(errOut, "%s: encode manifest: %v", pluginName, err)
	}

	buf, err := cBuffer(body)
	if err != nil {
		return failure(errOut, "%s: manifest buffer: %v", pluginName, err)
	}
	*manifestJSON = buf
	return statusOK
}

//export saci_go_run_batch
func saci_go_run_batch(
	cookie unsafe.Pointer,
	input C.SaciSlice,
	prior C.SaciSlice,
	hasPrior C.int32_t,
	out *C.SaciRunResult,
	errOut *C.SaciBuffer,
) (status C.SaciStatus) {
	defer func() {
		if r := recover(); r != nil {
			status = failure(errOut, "%s: panic in run_batch: %v", pluginName, r)
		}
	}()

	started := time.Now()

	if out == nil {
		return failure(errOut, "%s: run_batch called with a null result", pluginName)
	}
	inst, err := lookupInstance(cookie)
	if err != nil {
		return failure(errOut, "%v", err)
	}

	// This plugin declares stateful:false, so the host has no blob to replay
	// and `prior` is never read. hasPrior is what would gate that read: a zero
	// flag is a cold start whatever the slice holds.
	if hasPrior != 0 {
		inst.log(C.SACI_LOG_DEBUG, fmt.Sprintf(
			"%s: ignoring a %d byte checkpoint, this plugin is stateless", pluginName, uint64(prior.len)))
	}

	stream, batch, blocked, err := settle(goBytes(input), inst.config)
	if err != nil {
		inst.log(C.SACI_LOG_ERROR, err.Error())
		return failure(errOut, "%v", err)
	}

	output, err := cBuffer(stream.Buf)
	if err != nil {
		return failure(errOut, "%s: output buffer: %v", pluginName, err)
	}
	elapsed := time.Since(started)

	inst.metric(blockedMetric, float64(blocked))
	inst.log(C.SACI_LOG_INFO, fmt.Sprintf("%s: %d of %d rows cleared to settle",
		pluginName, batch.Rows-blocked, batch.Rows))

	// Last statement before the success return, so no failure path can leave
	// the host a filled result it will not free.
	rows := C.uint64_t(batch.Rows)
	*out = C.SaciRunResult{
		output:         output,
		checkpoint:     C.SaciBuffer{},
		has_checkpoint: 0,
		metrics: C.SaciRunMetrics{
			wall_ns:     C.uint64_t(elapsed.Nanoseconds()),
			rows_in:     rows,
			rows_out:    rows,
			systems_run: 1,
			retries:     0,
		},
	}
	return statusOK
}

//export saci_go_free_buffer
func saci_go_free_buffer(cookie unsafe.Pointer, buffer C.SaciBuffer) {
	// A recovered panic has nowhere to be reported here, and unwinding out of
	// an extern "C" frame aborts the host, so it is recovered and dropped. The
	// only work is one null check and one free.
	defer func() { _ = recover() }()

	if buffer.ptr != nil {
		C.free(unsafe.Pointer(buffer.ptr))
	}
}

//export saci_go_destroy
func saci_go_destroy(cookie unsafe.Pointer) {
	defer func() { _ = recover() }()

	instancesMu.Lock()
	delete(instances, uintptr(cookie))
	instancesMu.Unlock()

	if cookie != nil {
		C.free(cookie)
	}
}

// ---------------------------------------------------------------------------
// The batch, in plain Go.
// ---------------------------------------------------------------------------

// settle assigns every row of the Order batch a review tier and returns the
// mutated stream.
//
// The rate keys off the row's own `currency`, so each distinct currency costs
// one host config lookup and the rest come from the map.
func settle(input []byte, cfg func(string) (string, bool)) (*arrowipc.Stream, *arrowipc.Batch, int, error) {
	escalateAbove, err := configFloat(cfg, escalateAboveKey, escalateAboveDefault)
	if err != nil {
		return nil, nil, 0, err
	}

	stream, err := arrowipc.Parse(input)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("%s: parse input stream: %w", pluginName, err)
	}
	batch, err := stream.Component(componentName)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("%s: locate %s batch: %w", pluginName, componentName, err)
	}
	amounts, err := batch.Float64s(fieldAmount)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("%s: read %s.%s: %w", pluginName, componentName, fieldAmount, err)
	}
	currencies, err := batch.Strings(fieldCurrency)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("%s: read %s.%s: %w", pluginName, componentName, fieldCurrency, err)
	}
	if len(amounts) < batch.Rows || len(currencies) < batch.Rows {
		return nil, nil, 0, fmt.Errorf(
			"%s: %s declares %d rows but carries %d amounts and %d currencies",
			pluginName, componentName, batch.Rows, len(amounts), len(currencies))
	}

	rates := make(map[string]float64)
	blocked := 0
	for row := range batch.Rows {
		currency := currencies[row]
		rate, seen := rates[currency]
		if !seen {
			rate, err = configFloat(cfg, ratePrefix+currency, rateDefault)
			if err != nil {
				return nil, nil, 0, err
			}
			rates[currency] = rate
		}

		// `converted > 0` rather than `converted <= 0`, so a NaN amount lands
		// on the unsettleable arm instead of clearing.
		converted := amounts[row] * rate
		tier := int64(tierClear)
		switch {
		case !(converted > 0):
			tier = tierUnsettled
		case converted >= escalateAbove:
			tier = tierReview
		}
		if tier != tierClear {
			blocked++
		}

		if err := batch.SetInt64(fieldReviewTier, row, tier); err != nil {
			return nil, nil, 0, fmt.Errorf("%s: write %s.%s row %d: %w",
				pluginName, componentName, fieldReviewTier, row, err)
		}
	}

	return stream, batch, blocked, nil
}

// configFloat reads one host config value as a float. The ABI hands config over
// as strings, so an unparseable value is a misconfiguration worth failing the
// batch for rather than silently defaulting.
func configFloat(cfg func(string) (string, bool), key string, fallback float64) (float64, error) {
	raw, ok := cfg(key)
	if !ok {
		return fallback, nil
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: config %s=%q is not a number: %w", pluginName, key, raw, err)
	}
	return value, nil
}

// ---------------------------------------------------------------------------
// The host callbacks.
// ---------------------------------------------------------------------------

func (i *instance) log(level C.uint32_t, message string) {
	target, freeTarget := cSlice(pluginName)
	defer freeTarget()
	body, freeBody := cSlice(message)
	defer freeBody()
	C.saci_shim_log(i.host, level, target, body)
}

func (i *instance) metric(name string, value float64) {
	key, freeKey := cSlice(name)
	defer freeKey()
	C.saci_shim_metric(i.host, key, C.double(value))
}

// config reads one host config key.
//
// The slice the host writes stays valid for the life of the instance, but it is
// copied into a Go string anyway: the value is small, and a Go string cannot
// alias C memory the host may reuse.
func (i *instance) config(key string) (string, bool) {
	name, freeName := cSlice(key)
	defer freeName()

	var value C.SaciSlice
	if C.saci_shim_get_config(i.host, name, &value) == 0 {
		return "", false
	}
	if value.ptr == nil || value.len == 0 {
		return "", true
	}
	return string(unsafe.Slice((*byte)(unsafe.Pointer(value.ptr)), int(value.len))), true
}

// ---------------------------------------------------------------------------
// Buffers.
// ---------------------------------------------------------------------------

// cBuffer copies b into C memory with cap equal to len and hands the host the
// only reference. Every buffer this plugin returns comes from here and goes
// back through saci_go_free_buffer.
//
// cgo routes C.malloc through a helper that never returns nil and crashes the
// process out of memory, so the only failure left is a caller with nothing to
// send.
func cBuffer(b []byte) (C.SaciBuffer, error) {
	if len(b) == 0 {
		return C.SaciBuffer{}, fmt.Errorf("refusing to hand the host an empty buffer")
	}
	p := C.malloc(C.size_t(len(b)))
	copy(unsafe.Slice((*byte)(p), len(b)), b)
	return C.SaciBuffer{
		ptr: (*C.uint8_t)(p),
		len: C.size_t(len(b)),
		cap: C.size_t(len(b)),
	}, nil
}

// cSlice copies s into C memory and returns a borrowed slice over it plus its
// release.
//
// The bytes cannot be Go managed: the cgo rules forbid handing C a Go pointer,
// and the collector is free to move Go memory while the host reads it.
func cSlice(s string) (C.SaciSlice, func()) {
	if len(s) == 0 {
		return C.SaciSlice{}, func() {}
	}
	p := C.malloc(C.size_t(len(s)))
	copy(unsafe.Slice((*byte)(p), len(s)), s)
	return C.SaciSlice{ptr: (*C.uint8_t)(p), len: C.size_t(len(s))}, func() { C.free(p) }
}

// goBytes views the host's borrowed input without copying. arrowipc.Parse
// copies immediately, so the view never outlives the call that owns it.
func goBytes(s C.SaciSlice) []byte {
	if s.ptr == nil || s.len == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(s.ptr)), int(s.len))
}

// failure writes a message into the host's error buffer and returns the
// permanent status. A buffer that could not be allocated leaves the host to
// report the bare status.
func failure(errOut *C.SaciBuffer, format string, args ...any) C.SaciStatus {
	if errOut != nil {
		if buf, err := cBuffer([]byte(fmt.Sprintf(format, args...))); err == nil {
			*errOut = buf
		}
	}
	return statusPermanent
}

// main is never called. A c-shared library still needs package main to carry
// the exported symbols.
func main() {}
