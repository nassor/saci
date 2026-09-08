// Package saci turns a row struct and a transform function into a SACI
// processor.
//
// A stage declares one struct per component, tags its fields with their wire
// names, and hands [New] one [Transform] per system. The SDK derives the Arrow
// schema and the schema fingerprint from the struct by reflection, decodes the
// host's Arrow IPC stream into row values, runs the transforms in registration
// order, and re-encodes what they changed:
//
//	type Order struct {
//		ID     int64   `saci:"id"`
//		Amount float64 `saci:"amount"`
//		Valid  bool    `saci:"valid"`
//	}
//
//	var stage = saci.New("validate", "0.1.0",
//		saci.Transform("validate", func(row *Order, cfg saci.Config) error {
//			floor, err := cfg.Float64("min_amount", 0)
//			if err != nil {
//				return err
//			}
//			row.Valid = row.Amount > floor
//			return nil
//		}),
//	)
//
// # Why the WIT types are not in this package
//
// `componentize-go bindings` generates a stage's bindings into the stage's own
// module, and it names that module `wit_component` from a fixed template. Every
// Go stage therefore has its own `wit_component/saci_pipeline_types`, and an SDK
// that imported one could serve exactly one stage. The generated packages also
// import through `//go:wasmimport`, which does not compile for a host target, so
// an SDK that depended on them could not be unit tested at all.
//
// This package therefore speaks plain Go types, [Descriptor] and [Outcome], and
// reaches the host through [Host]. A stage's `describe` and `run-batch` exports
// are the only place the generated types appear.
//
// # Failure
//
// A Go panic inside a component traps the instance and the host sees a wasm trap
// with no reason, so [Processor.RunBatch] returns an error for everything it can
// refuse and the stage folds it into `run-error::permanent`. Authoring mistakes
// are the exception: a row type this SDK cannot map panics from [Transform], at
// construction time, long before a batch arrives.
package saci

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	arrowipc "github.com/nassor/saci/packages/saci-sdk-go/arrowipc"
)

// LogLevel is `saci:pipeline/host-io.log-level`, in the WIT declaration order the
// generated bindings use as their numeric values.
type LogLevel uint8

const (
	LogTrace LogLevel = iota
	LogDebug
	LogInfo
	LogWarn
	LogError
)

// Host is what a processor may call on its host, which is the whole of
// `saci:pipeline/host-io`: no filesystem, no network, no clock.
//
// A stage implements it over its generated bindings and hands it to
// [Processor.Bind]. The log target is the adapter's to choose, because it is a
// stage-level naming decision the host bridges onto a tracing target.
type Host interface {
	// GetConfig reads a config value the host injected, reporting whether the
	// key was set at all.
	GetConfig(key string) (string, bool)
	// Log writes one line to the host log.
	Log(level LogLevel, message string)
	// Metric records one observation of a named metric.
	Metric(name string, value float64)
}

// Config is the batch-scoped handle a transform receives.
//
// It is the only channel a transform has to the host, and it is deliberately
// narrow: a transform reads its configuration and counts what it saw, and the
// processor reports both once the batch is done.
type Config interface {
	// Float64 reads a float64 config value. An unset or blank key yields the
	// fallback; a value that is not a float64 is an error, because a
	// misconfigured floor silently defaulting to zero is worse than a refused
	// batch.
	Float64(key string, fallback float64) (float64, error)

	// Count adds to a named counter.
	//
	// The processor sums the deltas and reports one metric observation per
	// counter after the last system, so a per-row call costs one addition rather
	// than one host call. Calling it with a zero delta registers the counter, so
	// a batch that saw nothing still reports a zero.
	Count(name string, delta float64)
}

// ComponentSchema is one entry of [Descriptor.Components].
type ComponentSchema struct {
	Name string
	// ArrowSchemaIPC is a schema-only Arrow IPC stream, which is what
	// `component-descriptor.arrow-schema-ipc` holds.
	ArrowSchemaIPC []byte
}

// Descriptor is what a processor reports from `describe`.
type Descriptor struct {
	Name              string
	Version           string
	Components        []ComponentSchema
	Stateful          bool
	SchemaFingerprint string
}

// Metrics is what `run-metrics` reports, minus the retry count: nothing in this
// SDK retries, so a stage reports zero for it.
type Metrics struct {
	WallNs     uint64
	RowsIn     uint64
	RowsOut    uint64
	SystemsRun uint32
}

// Outcome is what one batch produced.
type Outcome struct {
	// Output is the re-encoded wire stream, ready to return as
	// `run-result.output`.
	Output  []byte
	Metrics Metrics
}

// Processor is a named, versioned pipeline of systems.
type Processor struct {
	name    string
	version string
	systems []System
	host    Host

	// components are the systems' row types, deduplicated and sorted by name.
	// That order is the descriptor's and the fingerprint's, and it matches the
	// order the host's own producer writes segments in.
	components []component

	descriptor    Descriptor
	descriptorErr error
}

// New builds a processor from its systems, in the order they run.
func New(name, version string, systems ...System) *Processor {
	p := &Processor{
		name:       name,
		version:    version,
		systems:    systems,
		host:       silentHost{},
		components: componentsOf(systems),
	}
	p.descriptor, p.descriptorErr = describe(name, version, p.components)
	return p
}

// Bind attaches the host bindings and returns the processor, so a stage can
// declare one as a package-level var.
//
// A processor with no host runs its transforms and drops its logs and metrics,
// which is what a unit test wants and what keeps [Processor.RunBatch] free of
// nil checks.
func (p *Processor) Bind(h Host) *Processor {
	if h != nil {
		p.host = h
	}
	return p
}

// Describe reports the processor's identity, its components' Arrow schemas and
// its schema fingerprint.
//
// `describe` has no error arm in the WIT world. A schema that failed to encode
// is reported to the host log and leaves that component's bytes empty, which the
// host turns into a load-time failure naming the component. That is a far better
// diagnostic than a trap, and unreachable in practice: every schema this SDK
// encodes was already validated when its row type was derived.
func (p *Processor) Describe() Descriptor {
	if p.descriptorErr != nil {
		p.host.Log(LogError, fmt.Sprintf("%s: %v", p.name, p.descriptorErr))
	}
	return p.descriptor
}

// RunBatch decodes the input stream, runs every system over it, and re-encodes.
//
// Components no system touched are forwarded byte for byte, including the
// `__alive` bitmap: re-deriving that bitmap would mark every row alive and
// resurrect the rows the host killed.
func (p *Processor) RunBatch(input []byte) (Outcome, error) {
	started := time.Now()

	stream, err := arrowipc.Parse(input)
	if err != nil {
		return Outcome{}, fmt.Errorf("parse input stream: %w", err)
	}
	segments, err := stream.RawSegments()
	if err != nil {
		return Outcome{}, fmt.Errorf("split input stream: %w", err)
	}

	run := &batch{
		stream:  stream,
		cfg:     &config{host: p.host},
		decoded: make(map[string]*decoded, len(p.components)),
	}
	for _, system := range p.systems {
		if err := system.run(run); err != nil {
			return Outcome{}, fmt.Errorf("system %s: %w", system.Name(), err)
		}
	}

	w := arrowipc.NewWriter()
	rows := 0
	for _, seg := range segments {
		if entry, ok := run.decoded[seg.Component]; ok {
			if err := entry.encode(w); err != nil {
				return Outcome{}, fmt.Errorf("encode component %s: %w", seg.Component, err)
			}
			rows = max(rows, seg.Rows)
			continue
		}
		if err := w.CopySegment(seg); err != nil {
			return Outcome{}, fmt.Errorf("forward component %s: %w", seg.Component, err)
		}
	}
	output := w.Bytes()

	p.report(run.cfg, rows)
	return Outcome{
		Output: output,
		Metrics: Metrics{
			WallNs:     uint64(time.Since(started).Nanoseconds()),
			RowsIn:     uint64(rows),
			RowsOut:    uint64(rows),
			SystemsRun: uint32(len(p.systems)),
		},
	}, nil
}

// report flushes the batch's counters and logs one summary line.
//
// One metric call per counter rather than one per row, and one log line per
// batch rather than per system: both cross the component boundary, and a
// per-row call would dominate the batch.
func (p *Processor) report(cfg *config, rows int) {
	for _, c := range cfg.counters {
		p.host.Metric(c.name, c.value)
	}

	var line strings.Builder
	fmt.Fprintf(&line, "%s: rows=%d systems=%d", p.name, rows, len(p.systems))
	for _, r := range cfg.reads {
		fmt.Fprintf(&line, " %s=%g", r.key, r.value)
	}
	for _, c := range cfg.counters {
		fmt.Fprintf(&line, " %s=%g", c.name, c.value)
	}
	p.host.Log(LogInfo, line.String())
}

// describe builds the descriptor once, at construction time, so a stage's
// `describe` export is a field read.
func describe(name, version string, components []component) (Descriptor, error) {
	out := Descriptor{
		Name:    name,
		Version: version,
		// Stateless: this SDK offers no checkpoint channel, so a stage built on
		// it returns `none` for `run-result.checkpoint` and must not promise the
		// host otherwise.
		Stateful:          false,
		Components:        make([]ComponentSchema, len(components)),
		SchemaFingerprint: fingerprint(components),
	}

	var err error
	for i, c := range components {
		schema, encodeErr := arrowipc.EncodeSchema(c.fields)
		if encodeErr != nil && err == nil {
			err = fmt.Errorf("encode schema of component %s: %w", c.name, encodeErr)
		}
		out.Components[i] = ComponentSchema{Name: c.name, ArrowSchemaIPC: schema}
	}
	return out, err
}

// componentsOf collects the systems' components, deduplicated and sorted by
// name.
//
// Two systems naming one component with different fields is an authoring
// mistake with no correct answer: the descriptor can hold one schema per
// component and the fingerprint hashes one field list.
func componentsOf(systems []System) []component {
	var out []component
	for _, s := range systems {
		spec := s.component()
		known := false
		for _, seen := range out {
			if seen.name != spec.name {
				continue
			}
			known = true
			if !sameFields(seen.fields, spec.fields) {
				panic(fmt.Sprintf("saci: system %s declares component %s with a different schema", s.Name(), spec.name))
			}
		}
		if !known {
			out = append(out, spec)
		}
	}
	// Insertion sort: a processor has a handful of components, and this keeps
	// the descriptor and the fingerprint independent of registration order.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].name < out[j-1].name; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func sameFields(a, b []arrowipc.SchemaField) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// batch is one [Processor.RunBatch] call's mutable state.
type batch struct {
	stream *arrowipc.Stream
	cfg    *config

	// decoded holds the row slice of every component a system touched, keyed by
	// component name. The second system to reach a component sees the first
	// one's writes, and the component is encoded once, after the last system.
	decoded map[string]*decoded
}

// decoded is one component's rows and the closure that writes them back. The
// closure carries the row type, which the map cannot.
type decoded struct {
	rows   any
	encode func(*arrowipc.Writer) error
}

// config is the batch-scoped [Config].
type config struct {
	host Host

	// reads and counters keep insertion order so the summary line is stable
	// across batches. A processor has a handful of each, so the linear lookups
	// beat a map plus a sort.
	reads    []reading
	counters []counter
}

type reading struct {
	key   string
	value float64
}

type counter struct {
	name  string
	value float64
}

func (c *config) Float64(key string, fallback float64) (float64, error) {
	value := fallback
	if raw, ok := c.host.GetConfig(key); ok {
		if text := strings.TrimSpace(raw); text != "" {
			parsed, err := strconv.ParseFloat(text, 64)
			if err != nil {
				return 0, fmt.Errorf("config %s=%q is not a float64", key, text)
			}
			value = parsed
		}
	}

	for i := range c.reads {
		if c.reads[i].key == key {
			c.reads[i].value = value
			return value, nil
		}
	}
	c.reads = append(c.reads, reading{key: key, value: value})
	return value, nil
}

func (c *config) Count(name string, delta float64) {
	for i := range c.counters {
		if c.counters[i].name == name {
			c.counters[i].value += delta
			return
		}
	}
	c.counters = append(c.counters, counter{name: name, value: delta})
}

// silentHost is the host a processor has before [Processor.Bind].
type silentHost struct{}

func (silentHost) GetConfig(string) (string, bool) { return "", false }
func (silentHost) Log(LogLevel, string)            {}
func (silentHost) Metric(string, float64)          {}
