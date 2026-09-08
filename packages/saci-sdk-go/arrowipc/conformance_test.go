package arrowipc_test

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"testing"

	arrowipc "github.com/nassor/saci/packages/saci-sdk-go/arrowipc"
)

// conformanceDir holds the corpus every codec under packages/arrow-ipc-* runs:
// one manifest, one binary vector per parse-level case, and a language-neutral
// reason code per refusal.
//
// Unlike the polyglot fixtures the rest of this file's tests use, the corpus is
// committed. Nothing below skips: a conformance suite that quietly covers zero
// cases is worse than no suite, because it reports green either way.
const conformanceDir = "../../arrow-ipc-conformance"

// conformanceFormatVersion is the manifest shape this harness reads. A bump
// means the manifest grew or moved something, and failing on it is what stops a
// stale harness from reading a new corpus as if nothing had changed.
const conformanceFormatVersion = 1

// conformanceHint is the command that regenerates the corpus.
const conformanceHint = "regenerate with `cargo run -p saci-service --features conformance --example conformance_vectors -- emit`"

// reasonSubstring maps a corpus reason code to the fragment this codec's own
// message carries.
//
// The reason code is the contract and the wording is deliberately local to each
// language, so this table is the entire translation between the two. Adding a
// corpus case costs exactly one row here and nothing else.
var reasonSubstring = map[string]string{
	"trailing_bytes":                  "trail the stream terminator",
	"truncated_stream":                "truncated stream",
	"truncated_message":               "metadata bytes",
	"bad_continuation":                "continuation marker missing",
	"empty_segment":                   "is empty",
	"first_message_not_schema":        "opens with header_type",
	"second_message_not_record_batch": "second message has header_type",
	"dictionary_batch":                "dictionary batch",
	"compressed_batch":                "body is compressed",
	"extra_message":                   "want one Schema and one RecordBatch",
	"bad_row_count":                   "record batch length is",
	"nodes_field_mismatch":            "field nodes for",
	"buffer_overruns_body":            "spans [",
	"missing_component_key":           "__saci_component",
	"unknown_component":               "no segment declares component",
	"unknown_field":                   "has no field",
	"type_mismatch":                   ", not ",
	"row_out_of_range":                "is out of range for field",
	"variable_width_write":            "writes fixed-width values only",
}

type conformanceManifest struct {
	FormatVersion int               `json:"format_version"`
	Component     string            `json:"component"`
	Reasons       []string          `json:"reasons"`
	Cases         []conformanceCase `json:"cases"`

	// dir is the manifest's own directory. Every `vector` path in the manifest
	// resolves against it, so the corpus can move as a unit.
	dir string
}

type conformanceCase struct {
	Name   string             `json:"name"`
	Vector string             `json:"vector"`
	Expect string             `json:"expect"`
	Reason string             `json:"reason"`
	Accept *conformanceAccept `json:"accept"`
	Op     *conformanceOp     `json:"op"`
}

type conformanceAccept struct {
	Components []string                     `json:"components"`
	Component  string                       `json:"component"`
	Rows       int                          `json:"rows"`
	Columns    map[string]conformanceColumn `json:"columns"`
}

type conformanceColumn struct {
	Type   string            `json:"type"`
	Values []json.RawMessage `json:"values"`
}

type conformanceOp struct {
	Kind      string          `json:"kind"`
	Component string          `json:"component"`
	Field     string          `json:"field"`
	Type      string          `json:"type"`
	Row       int             `json:"row"`
	Value     json.RawMessage `json:"value"`
}

// TestConformanceCorpus runs every case in the shared corpus.
func TestConformanceCorpus(t *testing.T) {
	m := loadConformanceManifest(t)
	if len(m.Cases) == 0 {
		t.Fatalf("%s carries no cases (%s)", m.dir, conformanceHint)
	}
	for _, c := range m.Cases {
		t.Run(c.Name, func(t *testing.T) {
			defer conformanceGuard(t, c.Name)
			runConformanceCase(t, m, c)
		})
	}
}

// TestConformanceReasonsAreMapped keeps reasonSubstring and the corpus in step.
//
// A reason with no row would otherwise reach the harness as an unmapped case,
// and a row with no reason means the corpus dropped a case this codec still
// thinks it covers. Both are silent losses of coverage, which is the failure
// mode a conformance suite exists to prevent.
func TestConformanceReasonsAreMapped(t *testing.T) {
	m := loadConformanceManifest(t)
	for _, reason := range m.Reasons {
		if _, ok := reasonSubstring[reason]; !ok {
			t.Errorf("corpus reason %q has no row in reasonSubstring", reason)
		}
	}
	for _, reason := range slices.Sorted(maps.Keys(reasonSubstring)) {
		if !slices.Contains(m.Reasons, reason) {
			t.Errorf("reasonSubstring maps %q, which the corpus no longer lists", reason)
		}
	}
}

// TestSegmentTailTooShortForMarker covers the one branch of the segment-tail
// rule no corpus vector reaches.
//
// Both `extra_message` vectors leave at least a whole end-of-stream marker
// after the record batch. A segment whose declared length cuts that marker
// short has surplus bytes just the same, and reading them as a message would
// report a truncated prefix, which names the wrong defect. The stream is
// derived here rather than committed because it is one edit away from the
// corpus baseline, and inventing a corpus vector is not this package's call.
func TestSegmentTailTooShortForMarker(t *testing.T) {
	m := loadConformanceManifest(t)
	baseline := conformanceBaseline(t, m)

	// Re-frame segment 0 on its own, four bytes short, so its trailing marker
	// is half present and nothing follows the stream terminator.
	segLen := int(binary.LittleEndian.Uint32(baseline))
	if segLen < 12 || 4+segLen > len(baseline) {
		t.Fatalf("baseline segment 0 declares %d bytes of %d", segLen, len(baseline))
	}
	short := segLen - 4
	stream := binary.LittleEndian.AppendUint32(nil, uint32(short))
	stream = append(stream, baseline[4:4+short]...)
	stream = binary.LittleEndian.AppendUint32(stream, 0)

	s, err := arrowipc.Parse(stream)
	if err != nil {
		t.Fatalf("Parse of a well-framed stream: %v", err)
	}
	_, err = s.Component(m.Component)
	var codecErr *arrowipc.Error
	if !errors.As(err, &codecErr) {
		t.Fatalf("Component refused with %T, want *arrowipc.Error: %v", err, err)
	}
	for _, want := range []string{reasonSubstring["extra_message"], "too few for an end-of-stream marker"} {
		if !strings.Contains(codecErr.Error(), want) {
			t.Fatalf("Component says %q, want a message containing %q", codecErr, want)
		}
	}
}

// conformanceBaseline returns the corpus's one accepted stream.
func conformanceBaseline(t *testing.T, m conformanceManifest) []byte {
	t.Helper()
	for _, c := range m.Cases {
		if c.Expect == "accept" {
			return readConformanceVector(t, m, c)
		}
	}
	t.Fatalf("%s carries no accept case to derive from", m.dir)
	return nil
}

// conformanceGuard turns a panic into a failure naming the case.
//
// A panic escaping the codec is itself a conformance failure, not a crash the
// suite should die on: the contract is that malformed input comes back as an
// *arrowipc.Error, and a processor cannot turn a runtime fault into the WIT
// `permanent(string)` reason its host is waiting for.
func conformanceGuard(t *testing.T, name string) {
	if r := recover(); r != nil {
		t.Errorf("case %s panicked, want an error: %v\n%s", name, r, debug.Stack())
	}
}

func runConformanceCase(t *testing.T, m conformanceManifest, c conformanceCase) {
	t.Helper()
	data := readConformanceVector(t, m, c)
	switch c.Expect {
	case "accept":
		if c.Accept == nil {
			t.Fatalf("case %s expects accept but carries no accept block", c.Name)
		}
		runConformanceAccept(t, *c.Accept, data)
	case "reject":
		want, ok := reasonSubstring[c.Reason]
		if !ok {
			t.Fatalf("case %s carries reason %q, which reasonSubstring does not map", c.Name, c.Reason)
		}
		assertRefused(t, c, want, conformanceReject(t, m, c, data))
	default:
		t.Fatalf("case %s has expect %q, want accept or reject", c.Name, c.Expect)
	}
}

func runConformanceAccept(t *testing.T, spec conformanceAccept, data []byte) {
	t.Helper()
	stream, err := arrowipc.Parse(data)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	components, err := stream.Components()
	if err != nil {
		t.Fatalf("Components: %v", err)
	}
	if !slices.Equal(components, spec.Components) {
		t.Fatalf("Components() = %v, want %v", components, spec.Components)
	}
	batch, err := stream.Component(spec.Component)
	if err != nil {
		t.Fatalf("Component(%q): %v", spec.Component, err)
	}
	if batch.Rows != spec.Rows {
		t.Fatalf("Rows = %d, want %d", batch.Rows, spec.Rows)
	}
	for _, name := range slices.Sorted(maps.Keys(spec.Columns)) {
		assertConformanceColumn(t, batch, name, spec.Columns[name])
	}
}

// conformanceReject performs the case's operation and returns whatever refused.
//
// A case with no `op` is a parse-level case: the vector itself is malformed, so
// the refusal comes out of Parse, out of listing the components, or out of
// addressing the one the manifest names.
func conformanceReject(t *testing.T, m conformanceManifest, c conformanceCase, data []byte) error {
	t.Helper()
	stream, err := arrowipc.Parse(data)
	if err != nil {
		return err
	}
	if c.Op == nil {
		if _, err := stream.Components(); err != nil {
			return err
		}
		_, err := stream.Component(m.Component)
		return err
	}
	batch, err := stream.Component(c.Op.Component)
	if err != nil || c.Op.Kind == "component" {
		return err
	}
	switch c.Op.Kind {
	case "column":
		return readConformanceColumn(t, batch, c.Op.Field, c.Op.Type)
	case "set":
		return writeConformanceValue(t, batch, *c.Op)
	}
	t.Fatalf("case %s has op kind %q, which the harness does not run", c.Name, c.Op.Kind)
	return nil
}

func assertRefused(t *testing.T, c conformanceCase, want string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("case %s (%s) succeeded, want a refusal", c.Name, c.Reason)
	}
	// The type matters as much as the refusal. An escaped native error tells a
	// caller nothing about whether the stream was bad or this codec is.
	var codecErr *arrowipc.Error
	if !errors.As(err, &codecErr) {
		t.Fatalf("case %s (%s) refused with %T, want *arrowipc.Error: %v", c.Name, c.Reason, err, err)
	}
	if !strings.Contains(codecErr.Error(), want) {
		t.Fatalf("case %s (%s) says %q, want a message containing %q", c.Name, c.Reason, codecErr, want)
	}
}

func assertConformanceColumn(t *testing.T, b *arrowipc.Batch, name string, col conformanceColumn) {
	t.Helper()
	switch col.Type {
	case "int64":
		got, err := b.Int64s(name)
		if err != nil {
			t.Fatalf("Int64s(%q): %v", name, err)
		}
		assertConformanceValues(t, name, got, col.Values)
	case "float64":
		got, err := b.Float64s(name)
		if err != nil {
			t.Fatalf("Float64s(%q): %v", name, err)
		}
		// Exact, not within a tolerance: these are bit patterns the host wrote
		// and this codec read back, not values anything computed, so a
		// difference of one ulp is a decode bug rather than rounding.
		assertConformanceValues(t, name, got, col.Values)
	case "utf8":
		got, err := b.Strings(name)
		if err != nil {
			t.Fatalf("Strings(%q): %v", name, err)
		}
		assertConformanceValues(t, name, got, col.Values)
	case "bool":
		got, err := b.Bools(name)
		if err != nil {
			t.Fatalf("Bools(%q): %v", name, err)
		}
		assertConformanceValues(t, name, got, col.Values)
	default:
		t.Fatalf("column %q has type %q, which the harness does not read", name, col.Type)
	}
}

func assertConformanceValues[T comparable](t *testing.T, name string, got []T, raw []json.RawMessage) {
	t.Helper()
	want := make([]T, len(raw))
	for i := range raw {
		if err := json.Unmarshal(raw[i], &want[i]); err != nil {
			t.Fatalf("column %q value %d: %v", name, i, err)
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("column %q = %v, want %v", name, got, want)
	}
}

func readConformanceColumn(t *testing.T, b *arrowipc.Batch, field, typ string) error {
	t.Helper()
	switch typ {
	case "int64":
		_, err := b.Int64s(field)
		return err
	case "float64":
		_, err := b.Float64s(field)
		return err
	case "utf8":
		_, err := b.Strings(field)
		return err
	case "bool":
		_, err := b.Bools(field)
		return err
	}
	t.Fatalf("column op asks for type %q, which the harness does not read", typ)
	return nil
}

// writeConformanceValue performs a `set` op.
//
// There is no string writer for a utf8 op to dispatch to, and that absence is
// exactly the rule the corpus checks: an in-place write to a variable-width
// column would move every later offset and both buffer lengths in the
// RecordBatch metadata. SetFloat64 is the call a caller makes when they mistake
// `settlement` for a number, and the writer refuses it by type before the
// type-mismatch message, so the op's string value has nothing to convert into.
func writeConformanceValue(t *testing.T, b *arrowipc.Batch, op conformanceOp) error {
	t.Helper()
	switch op.Type {
	case "int64":
		var v int64
		decodeConformanceValue(t, op, &v)
		return b.SetInt64(op.Field, op.Row, v)
	case "float64":
		var v float64
		decodeConformanceValue(t, op, &v)
		return b.SetFloat64(op.Field, op.Row, v)
	case "bool":
		var v bool
		decodeConformanceValue(t, op, &v)
		return b.SetBool(op.Field, op.Row, v)
	case "utf8":
		return b.SetFloat64(op.Field, op.Row, 0)
	}
	t.Fatalf("set op asks for type %q, which the harness does not write", op.Type)
	return nil
}

func decodeConformanceValue(t *testing.T, op conformanceOp, into any) {
	t.Helper()
	if err := json.Unmarshal(op.Value, into); err != nil {
		t.Fatalf("set op on %q carries value %s: %v", op.Field, op.Value, err)
	}
}

func loadConformanceManifest(t *testing.T) conformanceManifest {
	t.Helper()
	dir, err := filepath.Abs(conformanceDir)
	if err != nil {
		t.Fatalf("resolve %s: %v", conformanceDir, err)
	}
	path := filepath.Join(dir, "manifest.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v (%s)", path, err, conformanceHint)
	}
	var m conformanceManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if m.FormatVersion != conformanceFormatVersion {
		t.Fatalf("%s is format_version %d, this harness reads %d", path, m.FormatVersion, conformanceFormatVersion)
	}
	m.dir = dir
	return m
}

func readConformanceVector(t *testing.T, m conformanceManifest, c conformanceCase) []byte {
	t.Helper()
	path := filepath.Join(m.dir, filepath.FromSlash(c.Vector))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("case %s: read %s: %v (%s)", c.Name, path, err, conformanceHint)
	}
	return data
}
