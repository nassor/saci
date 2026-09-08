package arrowipc_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	arrowipc "github.com/nassor/saci/packages/saci-sdk-go/arrowipc"
)

// writeOne encodes a single-component stream and parses it straight back, which
// is the contract the writer exists for: what it emits, the reader reads.
func writeOne(t *testing.T, name string, rows int, columns ...arrowipc.Column) *arrowipc.Batch {
	t.Helper()
	w := arrowipc.NewWriter()
	if err := w.WriteComponent(name, 1, columns...); err != nil {
		t.Fatalf("WriteComponent: %v", err)
	}
	if err := w.WriteAlive(alive(rows)); err != nil {
		t.Fatalf("WriteAlive: %v", err)
	}
	stream, err := arrowipc.Parse(w.Bytes())
	if err != nil {
		t.Fatalf("Parse of written stream: %v", err)
	}
	batch, err := stream.Component(name)
	if err != nil {
		t.Fatalf("Component(%q): %v", name, err)
	}
	return batch
}

func alive(rows int) []bool {
	bits := make([]bool, rows)
	for i := range bits {
		bits[i] = true
	}
	return bits
}

// TestWriteRoundTripEveryType covers all four Arrow types the codec writes,
// with the value shapes the encodings can get wrong: a negative integer, a
// non-integral float, a bitmap that spills past one byte, and strings that are
// empty or multi-byte.
func TestWriteRoundTripEveryType(t *testing.T) {
	ids := []int64{0, -1, 9007199254740993, 42, 7, 8, 9, 10, 11}
	amounts := []float64{0, -0.5, 1e300, 0.1, 2, 3, 4, 5, 6}
	flags := []bool{true, false, true, true, false, false, false, true, true}
	labels := []string{"", "eu-west", "üñî", "a", "bb", "ccc", "dddd", "e", "ffffff"}

	batch := writeOne(t, "Wide", len(ids),
		arrowipc.Int64Column{Name: "id", Values: ids},
		arrowipc.Float64Column{Name: "amount", Values: amounts},
		arrowipc.BoolColumn{Name: "flagged", Values: flags},
		arrowipc.Utf8Column{Name: "label", Values: labels},
	)

	if batch.Rows != len(ids) {
		t.Fatalf("Rows = %d, want %d", batch.Rows, len(ids))
	}
	for i, want := range ids {
		if got := ints(t, batch, "id")[i]; got != want {
			t.Errorf("id[%d] = %d, want %d", i, got, want)
		}
	}
	for i, want := range amounts {
		if got := floats(t, batch, "amount")[i]; got != want {
			t.Errorf("amount[%d] = %v, want %v", i, got, want)
		}
	}
	for i, want := range flags {
		if got := bools(t, batch, "flagged")[i]; got != want {
			t.Errorf("flagged[%d] = %v, want %v", i, got, want)
		}
	}
	for i, want := range labels {
		if got := strs(t, batch, "label")[i]; got != want {
			t.Errorf("label[%d] = %q, want %q", i, got, want)
		}
	}
}

// TestWriteFieldOrderIsSchemaOrder pins the one property the fingerprint and the
// buffer walk both rest on.
func TestWriteFieldOrderIsSchemaOrder(t *testing.T) {
	batch := writeOne(t, "Ordered", 1,
		arrowipc.Utf8Column{Name: "third", Values: []string{"c"}},
		arrowipc.Int64Column{Name: "first", Values: []int64{1}},
		arrowipc.BoolColumn{Name: "second", Values: []bool{true}},
	)
	for want, name := range []string{"third", "first", "second"} {
		got, err := batch.FieldIndex(name)
		if err != nil {
			t.Fatalf("FieldIndex(%q): %v", name, err)
		}
		if got != want {
			t.Errorf("FieldIndex(%q) = %d, want %d", name, got, want)
		}
	}
}

// TestWriteEmptyBatch covers an empty partition, where every buffer is zero
// length except a Utf8 column's offsets, which still carries its one terminating
// entry.
func TestWriteEmptyBatch(t *testing.T) {
	batch := writeOne(t, "Order", 0,
		arrowipc.Int64Column{Name: "id", Values: nil},
		arrowipc.Float64Column{Name: "amount", Values: nil},
		arrowipc.BoolColumn{Name: "valid", Values: nil},
		arrowipc.Utf8Column{Name: "region", Values: nil},
	)
	if batch.Rows != 0 {
		t.Fatalf("Rows = %d, want 0", batch.Rows)
	}
	if got := ints(t, batch, "id"); len(got) != 0 {
		t.Errorf("id = %v, want no values", got)
	}
	if got := floats(t, batch, "amount"); len(got) != 0 {
		t.Errorf("amount = %v, want no values", got)
	}
	if got := bools(t, batch, "valid"); len(got) != 0 {
		t.Errorf("valid = %v, want no values", got)
	}
	if got := strs(t, batch, "region"); len(got) != 0 {
		t.Errorf("region = %v, want no values", got)
	}
}

// TestWriteRowsShrink is the case in-place mutation cannot serve: a second batch
// for the same component, shorter than the first, replacing it rather than
// appending a second segment under the same name.
func TestWriteRowsShrink(t *testing.T) {
	w := arrowipc.NewWriter()
	if err := w.WriteAlive(alive(5)); err != nil {
		t.Fatalf("WriteAlive: %v", err)
	}
	if err := w.WriteComponent("Order", 1,
		arrowipc.Int64Column{Name: "id", Values: []int64{1, 2, 3, 4, 5}},
		arrowipc.Utf8Column{Name: "region", Values: []string{"a", "b", "c", "d", "e"}},
	); err != nil {
		t.Fatalf("WriteComponent five rows: %v", err)
	}
	if err := w.WriteComponent("Order", 1,
		arrowipc.Int64Column{Name: "id", Values: []int64{7, 8, 9}},
		arrowipc.Utf8Column{Name: "region", Values: []string{"x", "yy", "zzz"}},
	); err != nil {
		t.Fatalf("WriteComponent three rows: %v", err)
	}

	stream, err := arrowipc.Parse(w.Bytes())
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, err := stream.Components(); err != nil || len(got) != 2 {
		t.Fatalf("Components() = %v, %v, want the component and the alive bitmap", got, err)
	}
	batch, err := stream.Component("Order")
	if err != nil {
		t.Fatalf("Component(Order): %v", err)
	}
	if batch.Rows != 3 {
		t.Fatalf("Rows = %d, want 3", batch.Rows)
	}
	if got := ints(t, batch, "id"); got[0] != 7 || got[2] != 9 {
		t.Errorf("id = %v, want the three-row batch", got)
	}
	if got := strs(t, batch, "region"); got[2] != "zzz" {
		t.Errorf("region = %v, want the three-row batch", got)
	}
}

// TestWriteAliveSegment checks the trailing segment is a readable component in
// its own right, and that a writer given none synthesises an all-alive one
// sized to its longest component.
func TestWriteAliveSegment(t *testing.T) {
	w := arrowipc.NewWriter()
	if err := w.WriteComponent("Order", 1,
		arrowipc.Int64Column{Name: "id", Values: []int64{1, 2, 3}},
	); err != nil {
		t.Fatalf("WriteComponent: %v", err)
	}

	stream, err := arrowipc.Parse(w.Bytes())
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	components, err := stream.Components()
	if err != nil {
		t.Fatalf("Components: %v", err)
	}
	if len(components) != 2 || components[0] != "Order" || components[1] != "__alive" {
		t.Fatalf("components = %v, want [Order __alive]", components)
	}
	batch, err := stream.Component("__alive")
	if err != nil {
		t.Fatalf("Component(__alive): %v", err)
	}
	if batch.Rows != 3 {
		t.Fatalf("alive Rows = %d, want 3", batch.Rows)
	}
	for i, live := range bools(t, batch, "alive") {
		if !live {
			t.Errorf("synthesised alive[%d] = false, want true", i)
		}
	}

	// An explicit bitmap with dead rows survives.
	if err := w.WriteAlive([]bool{true, false, true, true}); err != nil {
		t.Fatalf("WriteAlive: %v", err)
	}
	stream, err = arrowipc.Parse(w.Bytes())
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	batch, err = stream.Component("__alive")
	if err != nil {
		t.Fatalf("Component(__alive): %v", err)
	}
	if got := bools(t, batch, "alive"); batch.Rows != 4 || got[1] {
		t.Errorf("alive = %v of %d rows, want four bits with row 1 dead", got, batch.Rows)
	}
}

// TestWriteRefusals covers every validation the Write calls carry, because
// [arrowipc.Writer.Bytes] cannot report one.
func TestWriteRefusals(t *testing.T) {
	cases := []struct {
		name string
		want string
		run  func(w *arrowipc.Writer) error
	}{{
		name: "empty component name",
		want: "component name is empty",
		run: func(w *arrowipc.Writer) error {
			return w.WriteComponent("", 1, arrowipc.Int64Column{Name: "id", Values: []int64{1}})
		},
	}, {
		name: "alive through WriteComponent",
		want: "write it with WriteAlive",
		run: func(w *arrowipc.Writer) error {
			return w.WriteComponent("__alive", 1, arrowipc.BoolColumn{Name: "alive", Values: []bool{true}})
		},
	}, {
		name: "no columns",
		want: "declares no columns",
		run: func(w *arrowipc.Writer) error {
			return w.WriteComponent("Order", 1)
		},
	}, {
		name: "ragged columns",
		want: "holds 2 rows",
		run: func(w *arrowipc.Writer) error {
			return w.WriteComponent("Order", 1,
				arrowipc.Int64Column{Name: "id", Values: []int64{1, 2}},
				arrowipc.BoolColumn{Name: "valid", Values: []bool{true}},
			)
		},
	}, {
		name: "duplicate field",
		want: "declares field \"id\" twice",
		run: func(w *arrowipc.Writer) error {
			return w.WriteComponent("Order", 1,
				arrowipc.Int64Column{Name: "id", Values: []int64{1}},
				arrowipc.Int64Column{Name: "id", Values: []int64{2}},
			)
		},
	}, {
		name: "unnamed field",
		want: "field 0 is unnamed",
		run: func(w *arrowipc.Writer) error {
			return w.WriteComponent("Order", 1, arrowipc.Int64Column{Values: []int64{1}})
		},
	}, {
		name: "component longer than the alive bitmap",
		want: "more than the 1 bits of the alive bitmap",
		run: func(w *arrowipc.Writer) error {
			if err := w.WriteAlive([]bool{true}); err != nil {
				return err
			}
			return w.WriteComponent("Order", 1, arrowipc.Int64Column{Name: "id", Values: []int64{1, 2}})
		},
	}, {
		name: "alive bitmap shorter than a component",
		want: "more than the 1 bits of the alive bitmap",
		run: func(w *arrowipc.Writer) error {
			if err := w.WriteComponent("Order", 1, arrowipc.Int64Column{Name: "id", Values: []int64{1, 2}}); err != nil {
				return err
			}
			return w.WriteAlive([]bool{true})
		},
	}, {
		name: "copied segment without a component",
		want: "declares no component",
		run: func(w *arrowipc.Writer) error {
			return w.CopySegment(arrowipc.RawSegment{IPC: []byte{1}})
		},
	}, {
		name: "empty copied segment",
		want: "is empty",
		run: func(w *arrowipc.Writer) error {
			return w.CopySegment(arrowipc.RawSegment{Component: "Order"})
		},
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.run(arrowipc.NewWriter())
			if err == nil {
				t.Fatalf("write accepted, want a refusal naming %q", c.want)
			}
			var codecErr *arrowipc.Error
			if !errors.As(err, &codecErr) {
				t.Fatalf("err is %T, want *arrowipc.Error", err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %q, want it to name %q", err, c.want)
			}
		})
	}
}

// TestEncodeSchemaFraming checks the descriptor stream a processor reports: one
// Schema message, the end-of-stream marker, and nothing else.
func TestEncodeSchemaFraming(t *testing.T) {
	fields := []arrowipc.SchemaField{
		{Name: "id", Type: arrowipc.TypeInt64},
		{Name: "region", Type: arrowipc.TypeUtf8},
		{Name: "amount", Type: arrowipc.TypeFloat64},
		{Name: "valid", Type: arrowipc.TypeBool},
	}
	out, err := arrowipc.EncodeSchema(fields)
	if err != nil {
		t.Fatalf("EncodeSchema: %v", err)
	}
	if len(out) < 16 {
		t.Fatalf("descriptor stream is %d bytes, too short to hold a message", len(out))
	}
	if got := binary.LittleEndian.Uint32(out); got != 0xFFFFFFFF {
		t.Errorf("first word = %#x, want the continuation marker", got)
	}
	metaLen := int(binary.LittleEndian.Uint32(out[4:]))
	if metaLen%8 != 0 {
		t.Errorf("metadata_len = %d, want a multiple of 8", metaLen)
	}
	if want := 8 + metaLen + 8; len(out) != want {
		t.Errorf("stream is %d bytes, want %d for one message and the marker", len(out), want)
	}
	tail := out[8+metaLen:]
	if binary.LittleEndian.Uint32(tail) != 0xFFFFFFFF || binary.LittleEndian.Uint32(tail[4:]) != 0 {
		t.Errorf("stream does not end with the end-of-stream marker: % x", tail)
	}

	if _, err := arrowipc.EncodeSchema(nil); err == nil {
		t.Error("EncodeSchema(nil) accepted, want a refusal")
	}
	if _, err := arrowipc.EncodeSchema([]arrowipc.SchemaField{{Name: "x", Type: arrowipc.ColumnType(99)}}); err == nil {
		t.Error("EncodeSchema accepted an unknown type, want a refusal")
	}
}

// orderFields is the twelve-field `Order` schema the generated fixtures carry,
// in declaration order.
//
// The order comes from fieldOrder, so the two stay in step; a name missing from
// this table would encode as type_type 0 and the writer would refuse it, which
// is what makes a forgotten column loud rather than silent.
func orderFields() []arrowipc.SchemaField {
	types := map[string]arrowipc.ColumnType{
		"id":                 arrowipc.TypeInt64,
		"region":             arrowipc.TypeUtf8,
		"currency":           arrowipc.TypeUtf8,
		"amount":             arrowipc.TypeFloat64,
		"valid":              arrowipc.TypeBool,
		"usd_amount":         arrowipc.TypeFloat64,
		"usd_amount_display": arrowipc.TypeUtf8,
		"risk_score":         arrowipc.TypeFloat64,
		"flagged":            arrowipc.TypeBool,
		"fee":                arrowipc.TypeFloat64,
		"review_tier":        arrowipc.TypeInt64,
		"settlement":         arrowipc.TypeUtf8,
	}
	fields := make([]arrowipc.SchemaField, len(fieldOrder))
	for i, name := range fieldOrder {
		if _, ok := types[name]; !ok {
			panic("writer_test: fieldOrder names " + name + ", which orderFields has no type for")
		}
		fields[i] = arrowipc.SchemaField{Name: name, Type: types[name]}
	}
	return fields
}

// TestReEncodeFixtureRoundTrip decodes the host's own fixture batch, re-encodes
// every column and reads all twelve back.
//
// The fixture is arrow-rs output, so this is the one test whose input the
// encoder did not produce. What comes out the far side has to agree with the
// JSON ground truth column for column, which is the same bar the reader's own
// tests clear.
func TestReEncodeFixtureRoundTrip(t *testing.T) {
	wire, rows := fixture(t)
	_, batch := orderBatch(t, wire)

	w := arrowipc.NewWriter()
	if err := w.WriteComponent("Order", 1, orderColumns(t, batch)...); err != nil {
		t.Fatalf("WriteComponent: %v", err)
	}
	if err := w.WriteAlive(alive(batch.Rows)); err != nil {
		t.Fatalf("WriteAlive: %v", err)
	}
	encoded := w.Bytes()

	// The re-encoded body is smaller than the host's: arrow-rs pads every
	// buffer to 64 bytes and this codec to the format's minimum of 8, which the
	// same six rows show plainly.
	if got, host := len(recordBatchBody(t, segmentOf(t, encoded, 0))), len(recordBatchBody(t, segmentOf(t, wire, 0))); got%8 != 0 || got > host {
		t.Errorf("re-encoded body is %d bytes against the host's %d, want a multiple of 8 no larger", got, host)
	}

	// The values survive the round trip, which is what the body comparison
	// cannot show on its own: identical bytes described by a different
	// flatbuffer would still decode wrong.
	stream, err := arrowipc.Parse(encoded)
	if err != nil {
		t.Fatalf("Parse of re-encoded stream: %v", err)
	}
	reread, err := stream.Component("Order")
	if err != nil {
		t.Fatalf("Component(Order): %v", err)
	}
	assertColumns(t, reread, rows)
}

// TestOrderContractRoundTrip encodes the `Order` ground truth this package
// tests against and reads all twelve columns back.
//
// It needs no generated fixture, so it is what keeps [fieldOrder], the `order`
// struct and [orderFields] in step while the emitter's output is being
// regenerated: a column added to one and forgotten in another fails here rather
// than skipping.
func TestOrderContractRoundTrip(t *testing.T) {
	want := []order{
		{ID: 1, Region: "emea", Currency: "EUR", Amount: 100, Valid: true, UsdAmount: 110, UsdAmountDisplay: "$110.00", RiskScore: 0.5, Flagged: false, Fee: 1.1, ReviewTier: 0, Settlement: "settled"},
		{ID: 2, Region: "emea", Currency: "EUR", Amount: -5, Valid: false, UsdAmount: 1, UsdAmountDisplay: "", RiskScore: 0, Flagged: true, Fee: 0, ReviewTier: 2, Settlement: ""},
		{ID: 3, Region: "apac", Currency: "JPY", Amount: 1e6, Valid: true, UsdAmount: 6800, UsdAmountDisplay: "$6,800.00", RiskScore: 0.9, Flagged: true, Fee: 68, ReviewTier: 1, Settlement: "held"},
	}

	w := arrowipc.NewWriter()
	if err := w.WriteComponent("Order", 1, orderRowColumns(want)...); err != nil {
		t.Fatalf("WriteComponent: %v", err)
	}
	if err := w.WriteAlive(alive(len(want))); err != nil {
		t.Fatalf("WriteAlive: %v", err)
	}

	stream, err := arrowipc.Parse(w.Bytes())
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	batch, err := stream.Component("Order")
	if err != nil {
		t.Fatalf("Component(Order): %v", err)
	}

	for want, name := range fieldOrder {
		got, err := batch.FieldIndex(name)
		if err != nil {
			t.Fatalf("FieldIndex(%q): %v", name, err)
		}
		if got != want {
			t.Errorf("FieldIndex(%q) = %d, want %d", name, got, want)
		}
	}
	assertColumns(t, batch, want)
}

// orderRowColumns turns the ground truth's rows into columns, in schema order.
//
// The switch is on the wire name rather than the type, because that is what
// binds a column of the schema to a field of the struct; a name in [fieldOrder]
// with no case here is a column the ground truth cannot describe.
func orderRowColumns(rows []order) []arrowipc.Column {
	columns := make([]arrowipc.Column, 0, len(fieldOrder))
	ints := func(pick func(order) int64) []int64 {
		out := make([]int64, len(rows))
		for i, r := range rows {
			out[i] = pick(r)
		}
		return out
	}
	floats := func(pick func(order) float64) []float64 {
		out := make([]float64, len(rows))
		for i, r := range rows {
			out[i] = pick(r)
		}
		return out
	}
	bools := func(pick func(order) bool) []bool {
		out := make([]bool, len(rows))
		for i, r := range rows {
			out[i] = pick(r)
		}
		return out
	}
	strings := func(pick func(order) string) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = pick(r)
		}
		return out
	}

	for _, name := range fieldOrder {
		switch name {
		case "id":
			columns = append(columns, arrowipc.Int64Column{Name: name, Values: ints(func(r order) int64 { return r.ID })})
		case "region":
			columns = append(columns, arrowipc.Utf8Column{Name: name, Values: strings(func(r order) string { return r.Region })})
		case "currency":
			columns = append(columns, arrowipc.Utf8Column{Name: name, Values: strings(func(r order) string { return r.Currency })})
		case "amount":
			columns = append(columns, arrowipc.Float64Column{Name: name, Values: floats(func(r order) float64 { return r.Amount })})
		case "valid":
			columns = append(columns, arrowipc.BoolColumn{Name: name, Values: bools(func(r order) bool { return r.Valid })})
		case "usd_amount":
			columns = append(columns, arrowipc.Float64Column{Name: name, Values: floats(func(r order) float64 { return r.UsdAmount })})
		case "usd_amount_display":
			columns = append(columns, arrowipc.Utf8Column{Name: name, Values: strings(func(r order) string { return r.UsdAmountDisplay })})
		case "risk_score":
			columns = append(columns, arrowipc.Float64Column{Name: name, Values: floats(func(r order) float64 { return r.RiskScore })})
		case "flagged":
			columns = append(columns, arrowipc.BoolColumn{Name: name, Values: bools(func(r order) bool { return r.Flagged })})
		case "fee":
			columns = append(columns, arrowipc.Float64Column{Name: name, Values: floats(func(r order) float64 { return r.Fee })})
		case "review_tier":
			columns = append(columns, arrowipc.Int64Column{Name: name, Values: ints(func(r order) int64 { return r.ReviewTier })})
		case "settlement":
			columns = append(columns, arrowipc.Utf8Column{Name: name, Values: strings(func(r order) string { return r.Settlement })})
		default:
			panic("writer_test: fieldOrder names " + name + ", which orderRowColumns cannot build")
		}
	}
	return columns
}

// orderColumns decodes every `Order` column and hands it back as a writable one.
func orderColumns(t *testing.T, b *arrowipc.Batch) []arrowipc.Column {
	t.Helper()
	columns := make([]arrowipc.Column, 0, len(fieldOrder))
	for _, f := range orderFields() {
		switch f.Type {
		case arrowipc.TypeInt64:
			columns = append(columns, arrowipc.Int64Column{Name: f.Name, Values: ints(t, b, f.Name)})
		case arrowipc.TypeFloat64:
			columns = append(columns, arrowipc.Float64Column{Name: f.Name, Values: floats(t, b, f.Name)})
		case arrowipc.TypeBool:
			columns = append(columns, arrowipc.BoolColumn{Name: f.Name, Values: bools(t, b, f.Name)})
		case arrowipc.TypeUtf8:
			columns = append(columns, arrowipc.Utf8Column{Name: f.Name, Values: strs(t, b, f.Name)})
		}
	}
	return columns
}

// TestCopySegmentIsByteIdentical checks the pass-through path: a stream taken
// apart and reassembled unchanged is the same bytes.
func TestCopySegmentIsByteIdentical(t *testing.T) {
	wire, _ := fixture(t)
	stream, err := arrowipc.Parse(wire)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	segments, err := stream.RawSegments()
	if err != nil {
		t.Fatalf("RawSegments: %v", err)
	}
	if len(segments) < 2 {
		t.Fatalf("fixture holds %d segments, want a component and the alive bitmap", len(segments))
	}
	if segments[len(segments)-1].Component != "__alive" {
		t.Errorf("last segment is %q, want __alive", segments[len(segments)-1].Component)
	}
	for _, seg := range segments {
		if seg.Rows == 0 {
			t.Errorf("segment %q reports no rows", seg.Component)
		}
	}

	w := arrowipc.NewWriter()
	for _, seg := range segments {
		if err := w.CopySegment(seg); err != nil {
			t.Fatalf("CopySegment(%q): %v", seg.Component, err)
		}
	}
	if got := w.Bytes(); !bytes.Equal(got, wire) {
		t.Errorf("reassembled stream is %d bytes, input was %d", len(got), len(wire))
	}
}

// TestCopySegmentKeepsDeadRows is why the alive bitmap is forwarded rather than
// rebuilt: a rebuilt one marks every row alive and resurrects the rows the host
// killed.
func TestCopySegmentKeepsDeadRows(t *testing.T) {
	source := arrowipc.NewWriter()
	if err := source.WriteComponent("Order", 1, arrowipc.Int64Column{Name: "id", Values: []int64{1, 2, 3}}); err != nil {
		t.Fatalf("WriteComponent: %v", err)
	}
	if err := source.WriteAlive([]bool{true, false, true}); err != nil {
		t.Fatalf("WriteAlive: %v", err)
	}
	stream, err := arrowipc.Parse(source.Bytes())
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	segments, err := stream.RawSegments()
	if err != nil {
		t.Fatalf("RawSegments: %v", err)
	}

	w := arrowipc.NewWriter()
	for _, seg := range segments {
		if seg.Component == "Order" {
			if err := w.WriteComponent("Order", 1, arrowipc.Int64Column{Name: "id", Values: []int64{4, 5, 6}}); err != nil {
				t.Fatalf("WriteComponent: %v", err)
			}
			continue
		}
		if err := w.CopySegment(seg); err != nil {
			t.Fatalf("CopySegment(%q): %v", seg.Component, err)
		}
	}

	out, err := arrowipc.Parse(w.Bytes())
	if err != nil {
		t.Fatalf("Parse of output: %v", err)
	}
	batch, err := out.Component("__alive")
	if err != nil {
		t.Fatalf("Component(__alive): %v", err)
	}
	if got := bools(t, batch, "alive"); got[1] {
		t.Errorf("alive = %v, want row 1 still dead", got)
	}
}

// segmentOf returns segment i of a wire stream, length prefix excluded.
func segmentOf(t *testing.T, wire []byte, want int) []byte {
	t.Helper()
	pos := 0
	for i := 0; ; i++ {
		if pos+4 > len(wire) {
			t.Fatalf("stream holds no segment %d", want)
		}
		length := int(binary.LittleEndian.Uint32(wire[pos:]))
		pos += 4
		if length == 0 {
			t.Fatalf("stream holds no segment %d", want)
		}
		if i == want {
			return wire[pos : pos+length]
		}
		pos += length
	}
}

// recordBatchBody returns the body of a segment's second message.
//
// Framing only: the continuation marker and the metadata length are enough to
// step over the Schema message and to find where the record batch body starts,
// with no flatbuffer reading at all.
func recordBatchBody(t *testing.T, segment []byte) []byte {
	t.Helper()
	pos := 0
	for range 2 {
		if pos+8 > len(segment) {
			t.Fatal("segment holds fewer than two messages")
		}
		if binary.LittleEndian.Uint32(segment[pos:]) != 0xFFFFFFFF {
			t.Fatalf("offset %d is not a message: continuation marker missing", pos)
		}
		metaLen := int(binary.LittleEndian.Uint32(segment[pos+4:]))
		if metaLen == 0 || metaLen%8 != 0 {
			t.Fatalf("message at %d declares %d metadata bytes, want a non-zero multiple of 8", pos, metaLen)
		}
		pos += 8 + metaLen
	}
	// Everything up to the trailing end-of-stream marker is the body.
	if len(segment)-pos < 8 {
		t.Fatal("segment ends before its end-of-stream marker")
	}
	return segment[pos : len(segment)-8]
}
