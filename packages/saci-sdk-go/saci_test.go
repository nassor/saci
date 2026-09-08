package saci

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"

	arrowipc "github.com/nassor/saci/packages/saci-sdk-go/arrowipc"
)

// logLine is one host log call, kept whole so a test can assert on the level.
type logLine struct {
	level   LogLevel
	message string
}

// testHost records what a processor reported, which is the only way to check the
// observability the WIT world offers.
type testHost struct {
	config  map[string]string
	logs    []logLine
	metrics map[string]float64
}

func newTestHost(config map[string]string) *testHost {
	return &testHost{config: config, metrics: map[string]float64{}}
}

func (h *testHost) GetConfig(key string) (string, bool) {
	value, ok := h.config[key]
	return value, ok
}

func (h *testHost) Log(level LogLevel, message string) {
	h.logs = append(h.logs, logLine{level: level, message: message})
}

func (h *testHost) Metric(name string, value float64) { h.metrics[name] = value }

// Ledger is a second component no system touches, so it exercises the
// pass-through path.
type Ledger struct {
	ID    int64
	Total float64
}

// orderSpec is the derived `Order` component the test helpers encode against.
func orderSpec() component { return componentOf(reflect.TypeFor[Order]()) }

// sampleOrders is three rows whose amounts straddle a floor of 100.
func sampleOrders() []Order {
	return []Order{
		{ID: 1, Region: "emea", Currency: "EUR", Amount: 250},
		{ID: 2, Region: "apac", Currency: "JPY", Amount: 50},
		{ID: 3, Region: "amer", Currency: "USD", Amount: 100.5},
	}
}

// inputStream builds what the host hands a processor: one `Order` segment, one
// untouched `Ledger` segment, and an alive bitmap with a dead row.
func inputStream(t *testing.T, orders []Order) []byte {
	t.Helper()
	w := arrowipc.NewWriter()
	if err := w.WriteComponent("Order", 1, columnsOf(orders, orderSpec())...); err != nil {
		t.Fatalf("WriteComponent(Order): %v", err)
	}
	if err := w.WriteComponent("Ledger", 1,
		arrowipc.Int64Column{Name: "id", Values: []int64{9, 8, 7}},
		arrowipc.Float64Column{Name: "total", Values: []float64{1.5, 2.5, 3.5}},
	); err != nil {
		t.Fatalf("WriteComponent(Ledger): %v", err)
	}
	if err := w.WriteAlive([]bool{true, false, true}); err != nil {
		t.Fatalf("WriteAlive: %v", err)
	}
	return w.Bytes()
}

// onlyOrders is an input stream carrying nothing but `Order` and the bitmap.
func onlyOrders(t *testing.T, orders []Order) []byte {
	t.Helper()
	w := arrowipc.NewWriter()
	if err := w.WriteComponent("Order", 1, columnsOf(orders, orderSpec())...); err != nil {
		t.Fatalf("WriteComponent: %v", err)
	}
	return w.Bytes()
}

// decodeOrders reads the `Order` component back out of a processor's output.
func decodeOrders(t *testing.T, output []byte) []Order {
	t.Helper()
	stream, err := arrowipc.Parse(output)
	if err != nil {
		t.Fatalf("Parse output: %v", err)
	}
	batch, err := stream.Component("Order")
	if err != nil {
		t.Fatalf("Component(Order): %v", err)
	}
	rows, err := decode[Order](batch, orderSpec())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return rows
}

func rawSegments(t *testing.T, stream []byte) []arrowipc.RawSegment {
	t.Helper()
	parsed, err := arrowipc.Parse(stream)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	segments, err := parsed.RawSegments()
	if err != nil {
		t.Fatalf("RawSegments: %v", err)
	}
	return segments
}

// TestRunBatchWritesEveryColumn runs the validate rule the polyglot chain's Go
// stage carries, and checks the columns nobody wrote survive untouched.
func TestRunBatchWritesEveryColumn(t *testing.T) {
	host := newTestHost(map[string]string{"min_amount": "100"})
	stage := New("validate", "0.1.0", Transform("validate", func(row *Order, cfg Config) error {
		floor, err := cfg.Float64("min_amount", 0)
		if err != nil {
			return err
		}
		row.Valid = row.Amount > floor
		row.Settlement = "settled"
		invalid := 0.0
		if !row.Valid {
			invalid = 1
		}
		cfg.Count("validate.invalid_rows", invalid)
		return nil
	})).Bind(host)

	outcome, err := stage.RunBatch(inputStream(t, sampleOrders()))
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}

	rows := decodeOrders(t, outcome.Output)
	if len(rows) != 3 {
		t.Fatalf("output holds %d rows, want 3", len(rows))
	}
	for i, want := range []bool{true, false, true} {
		if rows[i].Valid != want {
			t.Errorf("row %d valid = %v, want %v", i, rows[i].Valid, want)
		}
	}
	for i, want := range sampleOrders() {
		got := rows[i]
		if got.ID != want.ID || got.Region != want.Region || got.Currency != want.Currency || got.Amount != want.Amount {
			t.Errorf("row %d = %+v, want the input's id, region, currency and amount", i, got)
		}
		if got.Settlement != "settled" {
			t.Errorf("row %d settlement = %q, want the written value", i, got.Settlement)
		}
	}

	if outcome.Metrics.RowsIn != 3 || outcome.Metrics.RowsOut != 3 {
		t.Errorf("metrics = %+v, want three rows in and out", outcome.Metrics)
	}
	if outcome.Metrics.SystemsRun != 1 {
		t.Errorf("systems run = %d, want 1", outcome.Metrics.SystemsRun)
	}

	// One metric call carrying the batch's total, not one call per row.
	if got, want := host.metrics["validate.invalid_rows"], 1.0; got != want {
		t.Errorf("validate.invalid_rows = %v, want %v", got, want)
	}
	if len(host.logs) != 1 {
		t.Fatalf("host received %d log lines, want one batch summary: %v", len(host.logs), host.logs)
	}
	if host.logs[0].level != LogInfo {
		t.Errorf("summary logged at level %d, want %d (info)", host.logs[0].level, LogInfo)
	}
	summary := host.logs[0].message
	for _, want := range []string{"validate:", "rows=3", "systems=1", "min_amount=100", "validate.invalid_rows=1"} {
		if !strings.Contains(summary, want) {
			t.Errorf("summary %q is missing %q", summary, want)
		}
	}
}

// TestRunBatchForwardsUntouchedSegments checks the pass-through: a component no
// system reads, and the alive bitmap, come out byte for byte, while the one a
// system wrote is re-encoded.
func TestRunBatchForwardsUntouchedSegments(t *testing.T) {
	input := inputStream(t, sampleOrders())
	stage := New("validate", "0.1.0", Transform("validate", func(row *Order, cfg Config) error {
		row.Valid = true
		return nil
	}))

	outcome, err := stage.RunBatch(input)
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}

	before := rawSegments(t, input)
	after := rawSegments(t, outcome.Output)
	if len(before) != len(after) {
		t.Fatalf("output holds %d segments, input held %d", len(after), len(before))
	}
	for i := range before {
		if before[i].Component != after[i].Component {
			t.Fatalf("segment %d is %q, want %q", i, after[i].Component, before[i].Component)
		}
		identical := bytes.Equal(before[i].IPC, after[i].IPC)
		if before[i].Component == "Order" {
			if identical {
				t.Errorf("the written component passed through unchanged, want a re-encode")
			}
			continue
		}
		if !identical {
			t.Errorf("segment %q was rewritten, want it forwarded byte for byte", before[i].Component)
		}
	}

	// The forwarded bitmap still holds the host's dead row.
	parsed, err := arrowipc.Parse(outcome.Output)
	if err != nil {
		t.Fatalf("Parse output: %v", err)
	}
	batch, err := parsed.Component("__alive")
	if err != nil {
		t.Fatalf("Component(__alive): %v", err)
	}
	bits, err := batch.Bools("alive")
	if err != nil {
		t.Fatalf("Bools(alive): %v", err)
	}
	if len(bits) != 3 || bits[1] {
		t.Errorf("alive = %v, want row 1 still dead", bits)
	}
}

// TestRunBatchNoopIsAFixedPoint checks that decoding and re-encoding a component
// without changing a value reproduces the segment exactly, which is what makes a
// re-encoding stage safe to put anywhere in a chain.
func TestRunBatchNoopIsAFixedPoint(t *testing.T) {
	input := inputStream(t, sampleOrders())
	stage := New("noop", "0.1.0", Transform("noop", func(row *Order, cfg Config) error { return nil }))

	outcome, err := stage.RunBatch(input)
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}
	if !bytes.Equal(outcome.Output, input) {
		t.Errorf("output is %d bytes, input was %d, want them identical", len(outcome.Output), len(input))
	}
}

// TestRunBatchSystemOrder checks systems run in registration order and share one
// decoded copy of the component, so the second sees the first's writes.
func TestRunBatchSystemOrder(t *testing.T) {
	var ran []string
	stage := New("chain", "0.1.0",
		Transform("first", func(row *Order, cfg Config) error {
			ran = append(ran, "first")
			row.RiskScore = 1
			return nil
		}),
		Transform("second", func(row *Order, cfg Config) error {
			ran = append(ran, "second")
			if row.RiskScore != 1 {
				return errors.New("second system did not see the first system's write")
			}
			row.RiskScore = 2
			return nil
		}),
	)

	outcome, err := stage.RunBatch(inputStream(t, sampleOrders()))
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}
	want := []string{"first", "first", "first", "second", "second", "second"}
	if strings.Join(ran, ",") != strings.Join(want, ",") {
		t.Errorf("systems ran %v, want %v", ran, want)
	}
	for i, row := range decodeOrders(t, outcome.Output) {
		if row.RiskScore != 2 {
			t.Errorf("row %d risk_score = %v, want the second system's write", i, row.RiskScore)
		}
	}
	if outcome.Metrics.SystemsRun != 2 {
		t.Errorf("systems run = %d, want 2", outcome.Metrics.SystemsRun)
	}
}

// TestRunBatchRefusals covers every error a batch comes back with. All of them
// map to `run-error::permanent` at the stage boundary, so the message is the
// whole diagnostic and has to name where the failure was.
func TestRunBatchRefusals(t *testing.T) {
	orders := sampleOrders()
	valid := inputStream(t, orders)

	cases := []struct {
		name  string
		stage *Processor
		input []byte
		want  string
	}{{
		name:  "transform error",
		stage: New("s", "1", Transform("boom", func(row *Order, cfg Config) error { return errors.New("no") })),
		input: valid,
		want:  "system boom: row 0: no",
	}, {
		name: "unparseable config",
		stage: New("s", "1", Transform("cfg", func(row *Order, cfg Config) error {
			_, err := cfg.Float64("min_amount", 0)
			return err
		})).Bind(newTestHost(map[string]string{"min_amount": "many"})),
		input: valid,
		want:  `config min_amount="many" is not a float64`,
	}, {
		name:  "component absent from the input",
		stage: New("s", "1", Transform("ledger", func(row *Ledger, cfg Config) error { return nil })),
		input: onlyOrders(t, orders),
		want:  "read component Ledger",
	}, {
		name:  "malformed stream",
		stage: New("s", "1", Transform("noop", func(row *Order, cfg Config) error { return nil })),
		input: []byte{1, 2, 3},
		want:  "parse input stream",
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			outcome, err := c.stage.RunBatch(c.input)
			if err == nil {
				t.Fatalf("RunBatch accepted the batch, want a refusal naming %q", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %q, want it to name %q", err, c.want)
			}
			if outcome.Output != nil {
				t.Error("a refused batch returned output, want none")
			}
		})
	}
}

// TestRunBatchWithoutHost checks a processor nobody bound still runs, which is
// what makes a transform unit-testable without the generated bindings.
func TestRunBatchWithoutHost(t *testing.T) {
	stage := New("validate", "0.1.0", Transform("validate", func(row *Order, cfg Config) error {
		floor, err := cfg.Float64("min_amount", 100)
		if err != nil {
			return err
		}
		row.Valid = row.Amount > floor
		cfg.Count("validate.invalid_rows", 0)
		return nil
	}))

	outcome, err := stage.RunBatch(inputStream(t, sampleOrders()))
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}
	rows := decodeOrders(t, outcome.Output)
	for i, want := range []bool{true, false, true} {
		if rows[i].Valid != want {
			t.Errorf("row %d valid = %v, want %v from the fallback floor", i, rows[i].Valid, want)
		}
	}
}

// TestDescribe checks the descriptor a host loads a processor from: identity, one
// schema-only Arrow IPC stream per component sorted by name, and the fingerprint
// the host recomputes for itself.
func TestDescribe(t *testing.T) {
	stage := New("polyglot-validate-go", "0.1.0",
		Transform("validate", func(row *Order, cfg Config) error { return nil }),
		Transform("ledger", func(row *Ledger, cfg Config) error { return nil }),
	)
	descriptor := stage.Describe()

	if descriptor.Name != "polyglot-validate-go" || descriptor.Version != "0.1.0" {
		t.Errorf("descriptor identity = %q %q", descriptor.Name, descriptor.Version)
	}
	if descriptor.Stateful {
		t.Error("descriptor is stateful, want stateless")
	}
	if len(descriptor.Components) != 2 {
		t.Fatalf("descriptor holds %d components, want 2", len(descriptor.Components))
	}
	if descriptor.Components[0].Name != "Ledger" || descriptor.Components[1].Name != "Order" {
		t.Errorf("components = %q, %q, want them sorted by name", descriptor.Components[0].Name, descriptor.Components[1].Name)
	}

	// Both components hash into one fingerprint, in that same sorted order.
	want := fingerprint([]component{
		componentOf(reflect.TypeFor[Ledger]()),
		orderSpec(),
	})
	if descriptor.SchemaFingerprint != want {
		t.Errorf("fingerprint = %s, want %s", descriptor.SchemaFingerprint, want)
	}

	for _, c := range descriptor.Components {
		if len(c.ArrowSchemaIPC) < 16 {
			t.Fatalf("component %s carries %d schema bytes", c.Name, len(c.ArrowSchemaIPC))
		}
		if binary.LittleEndian.Uint32(c.ArrowSchemaIPC) != 0xFFFFFFFF {
			t.Errorf("component %s schema does not open with the continuation marker", c.Name)
		}
		metaLen := int(binary.LittleEndian.Uint32(c.ArrowSchemaIPC[4:]))
		if want := 8 + metaLen + 8; len(c.ArrowSchemaIPC) != want {
			t.Errorf("component %s schema is %d bytes, want %d for one message and the marker", c.Name, len(c.ArrowSchemaIPC), want)
		}
	}
}

// outerOrderTransform closes over the package-level `Order`, so a test can
// register it beside a locally declared type of the same name.
func outerOrderTransform() System {
	return Transform("outer", func(row *Order, cfg Config) error { return nil })
}

// TestNewRejectsClashingComponents is why a component's schema is derived once:
// two row types sharing a name have no descriptor that could describe both.
func TestNewRejectsClashingComponents(t *testing.T) {
	type Order struct {
		ID int64 `saci:"id"`
	}
	defer func() {
		raised := recover()
		if raised == nil {
			t.Fatal("New accepted two schemas for one component, want a panic")
		}
		if message, _ := raised.(string); !strings.Contains(message, "different schema") {
			t.Errorf("panic = %v, want it to name the clash", raised)
		}
	}()
	New("clash", "1",
		outerOrderTransform(),
		Transform("inner", func(row *Order, cfg Config) error { return nil }),
	)
}
