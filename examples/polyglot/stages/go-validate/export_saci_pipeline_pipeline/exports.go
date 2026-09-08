// Package export_saci_pipeline_pipeline implements the `saci:pipeline/pipeline`
// export for stage 1 of the polyglot example, the **Go** processor.
//
// It reads `amount` and writes `valid`, the gate every later stage keys off:
// the Python stage converts only valid rows, the Kotlin stage charges a fee only
// on valid rows, and the Rust stage rejects the invalid ones outright.
//
// The whole stage is the [Order] struct, one transform function, and the two
// export wrappers below. The SDK derives the Arrow schema and the schema
// fingerprint from the struct, decodes the host's stream into `Order` values,
// runs the transform, and re-encodes. Nothing here parses a flatbuffer, embeds a
// generated schema constant, or addresses a column by name.
//
// The package name and the `wit_component/...` import paths are not a choice:
// `componentize-go bindings` generates `wit_exports.go`, which imports this
// package by that exact path and calls exactly Describe and RunBatch. It also
// rewrites go.mod's module line to `wit_component`, which is why the SDK reaches
// the host through an interface this file implements rather than importing the
// generated bindings itself.
//
// # Why nothing here panics
//
// A Go panic inside a component traps the instance, and the host then sees an
// opaque wasm trap instead of a reason. Every failure path is therefore folded
// into `run-error::permanent`, which the WIT contract designates for bad input
// shape and processor bugs; `schema-mismatch` must never come out of `run-batch`.
package export_saci_pipeline_pipeline

import (
	witTypes "go.bytecodealliance.org/pkg/wit/types"

	saci "github.com/nassor/saci/packages/saci-sdk-go"
	hostio "wit_component/saci_pipeline_host_io"
	"wit_component/saci_pipeline_types"
)

const (
	// stageName is what the driver and the integration test expect back from
	// describe(); the host keys config and checkpoint compatibility on it.
	stageName    = "polyglot-validate-go"
	stageVersion = "0.1.0"

	// logTarget is the tracing target the host bridges host-io::log onto.
	logTarget = "validate"

	// minAmountKey is read from `pipeline.wasm.config` in the service TOML, or
	// from the driver's config map. Absent means "no floor".
	minAmountKey     = "min_amount"
	minAmountDefault = 0.0

	// invalidRowsMetric counts the rows this stage rejected, summed over the
	// batch and reported once.
	invalidRowsMetric = "validate.invalid_rows"
)

// Order is the chain's row type: these twelve columns in this order are the
// cross-language contract every stage agrees on, and the order the schema
// fingerprint is computed in.
//
// The struct is the schema. The `saci` tags spell the wire names out rather than
// leaning on the SDK's lower snake case default, because these names are shared
// with five other languages and a Go rename must not silently move a column.
type Order struct {
	ID               int64   `saci:"id"`
	Region           string  `saci:"region"`
	Currency         string  `saci:"currency"`
	Amount           float64 `saci:"amount"`
	Valid            bool    `saci:"valid"`
	UsdAmount        float64 `saci:"usd_amount"`
	UsdAmountDisplay string  `saci:"usd_amount_display"`
	RiskScore        float64 `saci:"risk_score"`
	Flagged          bool    `saci:"flagged"`
	Fee              float64 `saci:"fee"`
	ReviewTier       int64   `saci:"review_tier"`
	Settlement       string  `saci:"settlement"`
}

// stage is the processor. It is a package-level var so the schema derivation and
// the descriptor encoding happen once, at instantiation, rather than on the
// first batch.
var stage = saci.New(stageName, stageVersion,
	saci.Transform("validate", func(row *Order, cfg saci.Config) error {
		minAmount, err := cfg.Float64(minAmountKey, minAmountDefault)
		if err != nil {
			return err
		}
		row.Valid = row.Amount > minAmount

		// Counted unconditionally, so a batch with nothing to reject still
		// reports a zero rather than dropping the series.
		invalid := 0.0
		if !row.Valid {
			invalid = 1
		}
		cfg.Count(invalidRowsMetric, invalid)
		return nil
	}),
).Bind(host{})

// host bridges the SDK onto the generated host-io bindings.
//
// The SDK cannot import them: `componentize-go bindings` regenerates them into
// whichever stage module it is run in, always under the module name
// `wit_component`, so the import path is not unique across stages. The log
// target is supplied here for the same reason it is a constant, it names this
// stage to the host's tracing subscriber.
type host struct{}

func (host) GetConfig(key string) (string, bool) {
	value := hostio.GetConfig(key)
	if !value.IsSome() {
		return "", false
	}
	return value.Some(), true
}

func (host) Log(level saci.LogLevel, message string) {
	hostio.Log(hostio.LogLevel(level), logTarget, message)
}

func (host) Metric(name string, value float64) { hostio.Metric(name, value) }

// Describe reports the stage identity and the one component it operates on.
func Describe() saci_pipeline_types.PipelineDescriptor {
	descriptor := stage.Describe()

	components := make([]saci_pipeline_types.ComponentDescriptor, len(descriptor.Components))
	for i, c := range descriptor.Components {
		components[i] = saci_pipeline_types.ComponentDescriptor{
			Name:           c.Name,
			ArrowSchemaIpc: c.ArrowSchemaIPC,
		}
	}
	return saci_pipeline_types.PipelineDescriptor{
		Name:              descriptor.Name,
		Version:           descriptor.Version,
		Components:        components,
		Stateful:          descriptor.Stateful,
		SchemaFingerprint: descriptor.SchemaFingerprint,
	}
}

// RunBatch marks every row whose `amount` clears the configured floor as valid.
//
// `prior` is ignored and `checkpoint` is none: this stage keeps no state across
// batches, which is what `stateful: false` in Describe promises the host.
func RunBatch(input []uint8, prior witTypes.Option[[]uint8]) witTypes.Result[saci_pipeline_types.RunResult, saci_pipeline_types.RunError] {
	_ = prior

	outcome, err := stage.RunBatch(input)
	if err != nil {
		// Nothing this stage can hit is worth a host retry: the same bytes and
		// the same config would fail again.
		message := err.Error()
		hostio.Log(hostio.LogLevelError, logTarget, stageName+": "+message)
		return witTypes.Err[saci_pipeline_types.RunResult, saci_pipeline_types.RunError](
			saci_pipeline_types.MakeRunErrorPermanent(message),
		)
	}

	return witTypes.Ok[saci_pipeline_types.RunResult, saci_pipeline_types.RunError](saci_pipeline_types.RunResult{
		Output:     outcome.Output,
		Checkpoint: witTypes.None[[]uint8](),
		Metrics: saci_pipeline_types.RunMetrics{
			WallNs:     outcome.Metrics.WallNs,
			RowsIn:     outcome.Metrics.RowsIn,
			RowsOut:    outcome.Metrics.RowsOut,
			SystemsRun: outcome.Metrics.SystemsRun,
			Retries:    0,
		},
		// The host multicasts to every downstream link, which is this chain's
		// only routing.
		Routes: witTypes.None[[]string](),
	})
}
