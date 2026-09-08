package arrowipc_test

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"testing"

	arrowipc "github.com/nassor/saci/packages/saci-sdk-go/arrowipc"
)

// generatedDir holds the emitter's output. It is gitignored, so tests that need
// it skip rather than fail when it is absent.
const generatedDir = "../../../examples/polyglot/generated"

// emitHint is the command that produces the fixtures.
const emitHint = "run `cargo run -p saci-service --features wasm --example polyglot_schema_emit -- emit` first"

// tolerance for float comparisons: 100.0 * 1.10 is not exactly 110.0, and the
// same tolerance is used by every stage of the chain.
const tolerance = 1e-6

// order mirrors examples/polyglot/generated/fixture_input.json, which is the
// ground truth for what the codec must decode out of the wire bytes.
type order struct {
	ID               int64   `json:"id"`
	Region           string  `json:"region"`
	Currency         string  `json:"currency"`
	Amount           float64 `json:"amount"`
	Valid            bool    `json:"valid"`
	UsdAmount        float64 `json:"usd_amount"`
	UsdAmountDisplay string  `json:"usd_amount_display"`
	RiskScore        float64 `json:"risk_score"`
	Flagged          bool    `json:"flagged"`
	Fee              float64 `json:"fee"`
	ReviewTier       int64   `json:"review_tier"`
	Settlement       string  `json:"settlement"`
}

// fieldOrder is load-bearing: it is the order the fingerprint is computed in and
// the order the buffer walk assigns slots in.
var fieldOrder = []string{
	"id", "region", "currency", "amount", "valid",
	"usd_amount", "usd_amount_display", "risk_score", "flagged", "fee",
	"review_tier", "settlement",
}

func readGenerated(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(generatedDir, name))
	if errors.Is(err, fs.ErrNotExist) {
		t.Skipf("%s/%s is absent: %s", generatedDir, name, emitHint)
	}
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return raw
}

// fixture returns the wire bytes and the JSON ground truth for the same rows.
func fixture(t *testing.T) ([]byte, []order) {
	t.Helper()
	wire := readGenerated(t, "fixture_input.saci")
	var rows []order
	if err := json.Unmarshal(readGenerated(t, "fixture_input.json"), &rows); err != nil {
		t.Fatalf("decode fixture_input.json: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("fixture_input.json holds no rows")
	}
	return wire, rows
}

func orderBatch(t *testing.T, wire []byte) (*arrowipc.Stream, *arrowipc.Batch) {
	t.Helper()
	stream, err := arrowipc.Parse(wire)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	batch, err := stream.Component("Order")
	if err != nil {
		t.Fatalf("Component(Order): %v", err)
	}
	return stream, batch
}

func ints(t *testing.T, b *arrowipc.Batch, name string) []int64 {
	t.Helper()
	v, err := b.Int64s(name)
	if err != nil {
		t.Fatalf("Int64s(%q): %v", name, err)
	}
	return v
}

func floats(t *testing.T, b *arrowipc.Batch, name string) []float64 {
	t.Helper()
	v, err := b.Float64s(name)
	if err != nil {
		t.Fatalf("Float64s(%q): %v", name, err)
	}
	return v
}

func bools(t *testing.T, b *arrowipc.Batch, name string) []bool {
	t.Helper()
	v, err := b.Bools(name)
	if err != nil {
		t.Fatalf("Bools(%q): %v", name, err)
	}
	return v
}

func strs(t *testing.T, b *arrowipc.Batch, name string) []string {
	t.Helper()
	v, err := b.Strings(name)
	if err != nil {
		t.Fatalf("Strings(%q): %v", name, err)
	}
	return v
}

// assertColumns checks all twelve columns against the JSON ground truth.
func assertColumns(t *testing.T, b *arrowipc.Batch, want []order) {
	t.Helper()
	if b.Rows != len(want) {
		t.Fatalf("Rows = %d, want %d", b.Rows, len(want))
	}
	ids := ints(t, b, "id")
	tiers := ints(t, b, "review_tier")
	regions := strs(t, b, "region")
	currencies := strs(t, b, "currency")
	settlements := strs(t, b, "settlement")
	amounts := floats(t, b, "amount")
	usd := floats(t, b, "usd_amount")
	usdDisplay := strs(t, b, "usd_amount_display")
	risk := floats(t, b, "risk_score")
	fees := floats(t, b, "fee")
	valid := bools(t, b, "valid")
	flagged := bools(t, b, "flagged")

	for i, w := range want {
		if ids[i] != w.ID {
			t.Errorf("row %d id = %d, want %d", i, ids[i], w.ID)
		}
		if regions[i] != w.Region {
			t.Errorf("row %d region = %q, want %q", i, regions[i], w.Region)
		}
		if currencies[i] != w.Currency {
			t.Errorf("row %d currency = %q, want %q", i, currencies[i], w.Currency)
		}
		if settlements[i] != w.Settlement {
			t.Errorf("row %d settlement = %q, want %q", i, settlements[i], w.Settlement)
		}
		if math.Abs(amounts[i]-w.Amount) > tolerance {
			t.Errorf("row %d amount = %v, want %v", i, amounts[i], w.Amount)
		}
		if math.Abs(usd[i]-w.UsdAmount) > tolerance {
			t.Errorf("row %d usd_amount = %v, want %v", i, usd[i], w.UsdAmount)
		}
		if usdDisplay[i] != w.UsdAmountDisplay {
			t.Errorf("row %d usd_amount_display = %q, want %q", i, usdDisplay[i], w.UsdAmountDisplay)
		}
		if math.Abs(risk[i]-w.RiskScore) > tolerance {
			t.Errorf("row %d risk_score = %v, want %v", i, risk[i], w.RiskScore)
		}
		if valid[i] != w.Valid {
			t.Errorf("row %d valid = %v, want %v", i, valid[i], w.Valid)
		}
		if flagged[i] != w.Flagged {
			t.Errorf("row %d flagged = %v, want %v", i, flagged[i], w.Flagged)
		}
		if math.Abs(fees[i]-w.Fee) > tolerance {
			t.Errorf("row %d fee = %v, want %v", i, fees[i], w.Fee)
		}
		if tiers[i] != w.ReviewTier {
			t.Errorf("row %d review_tier = %d, want %d", i, tiers[i], w.ReviewTier)
		}
	}
}

func TestFixtureDecodesEveryColumn(t *testing.T) {
	wire, want := fixture(t)
	_, batch := orderBatch(t, wire)
	assertColumns(t, batch, want)
}

// TestFixtureProducesDocumentedValidColumn pins the fixture's amounts to the
// `valid` column every later stage of the chain is documented to receive. The
// stage rule lives in the processor package, which cannot compile for the host
// target, so the fixture and the documented expectation are compared here.
func TestFixtureProducesDocumentedValidColumn(t *testing.T) {
	wire, _ := fixture(t)
	_, batch := orderBatch(t, wire)
	amounts := floats(t, batch, "amount")

	const minAmount = 0.0 // the min_amount config default
	want := []bool{true, false, true, true, false, true}
	if len(amounts) != len(want) {
		t.Fatalf("fixture has %d rows, the documented chain expects %d", len(amounts), len(want))
	}
	for row, amount := range amounts {
		if got := amount > minAmount; got != want[row] {
			t.Errorf("row %d: amount %v > %v = %v, documented valid is %v", row, amount, minAmount, got, want[row])
		}
	}
}

func TestFieldOrderMatchesSchema(t *testing.T) {
	wire, _ := fixture(t)
	_, batch := orderBatch(t, wire)
	for want, name := range fieldOrder {
		got, err := batch.FieldIndex(name)
		if err != nil {
			t.Fatalf("FieldIndex(%q): %v", name, err)
		}
		if got != want {
			t.Errorf("FieldIndex(%q) = %d, want %d", name, got, want)
		}
	}
}

// changedBytes counts how many of the eight little-endian bytes of two 64-bit
// values differ. An in-place write only rewrites those.
func changedBytes(before, after uint64) int {
	var b, a [8]byte
	binary.LittleEndian.PutUint64(b[:], before)
	binary.LittleEndian.PutUint64(a[:], after)
	n := 0
	for i := range b {
		if b[i] != a[i] {
			n++
		}
	}
	return n
}

// bitmap packs up to eight bools the way Arrow does, LSB first. The fixture is
// six rows, so one byte holds the whole valid column.
func bitmap(values []bool) byte {
	var mask byte
	for i, v := range values {
		if v {
			mask |= 1 << (uint(i) & 7)
		}
	}
	return mask
}

func validOf(rows []order) []bool {
	out := make([]bool, len(rows))
	for i, r := range rows {
		out[i] = r.Valid
	}
	return out
}

// TestSetRoundTrip checks the mutation contract: the written values read back
// after a fresh parse of the mutated buffer, every other column still matches
// the ground truth, and nothing outside the three target value buffers moved,
// byte for byte.
func TestSetRoundTrip(t *testing.T) {
	wire, want := fixture(t)
	stream, batch := orderBatch(t, wire)

	wantUsd := []float64{110.0, 1.0, 6800.0, 60000.0, 2.0, 20000.0}
	wantValid := []bool{true, false, true, true, false, true}
	// review_tier is the schema's only Int64 output, written by the C# stage
	// alone. A full-width payload and a negative one prove SetInt64 writes all
	// eight little-endian bytes rather than a truncated or unsigned value.
	wantTier := []int64{0, 1, 2, 0x0102030405060708, -1, 3}
	if len(wantUsd) != batch.Rows {
		t.Fatalf("fixture has %d rows, test expects %d", batch.Rows, len(wantUsd))
	}
	for row := range batch.Rows {
		if err := batch.SetFloat64("usd_amount", row, wantUsd[row]); err != nil {
			t.Fatalf("SetFloat64(usd_amount, %d): %v", row, err)
		}
		if err := batch.SetBool("valid", row, wantValid[row]); err != nil {
			t.Fatalf("SetBool(valid, %d): %v", row, err)
		}
		if err := batch.SetInt64("review_tier", row, wantTier[row]); err != nil {
			t.Fatalf("SetInt64(review_tier, %d): %v", row, err)
		}
	}

	mutated := stream.Buf
	if len(mutated) != len(wire) {
		t.Fatalf("mutation resized the stream: %d bytes, want %d", len(mutated), len(wire))
	}
	// The mutation may only touch the bytes the new values differ in: the six
	// usd_amount doubles, the six review_tier int64s and the one bitmap byte
	// holding all six valid bits. Anything more means the framing, a flatbuffer
	// or the __alive segment moved. The expected count is derived from the
	// values, not hard-coded.
	wantDiff := 0
	for row, v := range wantUsd {
		wantDiff += changedBytes(math.Float64bits(want[row].UsdAmount), math.Float64bits(v))
	}
	for row, v := range wantTier {
		wantDiff += changedBytes(uint64(want[row].ReviewTier), uint64(v))
	}
	if bitmap(wantValid) != bitmap(validOf(want)) {
		wantDiff++
	}
	diff := 0
	for i := range mutated {
		if mutated[i] != wire[i] {
			diff++
		}
	}
	if diff != wantDiff {
		t.Errorf("%d bytes changed, want exactly %d", diff, wantDiff)
	}

	_, reparsed := orderBatch(t, mutated)
	gotUsd := floats(t, reparsed, "usd_amount")
	gotValid := bools(t, reparsed, "valid")
	gotTier := ints(t, reparsed, "review_tier")
	for row := range reparsed.Rows {
		if math.Abs(gotUsd[row]-wantUsd[row]) > tolerance {
			t.Errorf("row %d usd_amount = %v, want %v", row, gotUsd[row], wantUsd[row])
		}
		if gotValid[row] != wantValid[row] {
			t.Errorf("row %d valid = %v, want %v", row, gotValid[row], wantValid[row])
		}
		if gotTier[row] != wantTier[row] {
			t.Errorf("row %d review_tier = %d, want %d", row, gotTier[row], wantTier[row])
		}
	}

	// Neighbouring columns must be exactly what they were: patch the ground
	// truth with the three written columns and re-check all twelve.
	for i := range want {
		want[i].UsdAmount = wantUsd[i]
		want[i].Valid = wantValid[i]
		want[i].ReviewTier = wantTier[i]
	}
	assertColumns(t, reparsed, want)
}

func TestComponentAbsent(t *testing.T) {
	wire, _ := fixture(t)
	stream, err := arrowipc.Parse(wire)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := stream.Component("Nope"); err == nil {
		t.Fatal("Component(Nope) succeeded, want an error")
	}
}

func TestSetOnUtf8Rejected(t *testing.T) {
	wire, _ := fixture(t)
	stream, batch := orderBatch(t, wire)
	before := make([]byte, len(stream.Buf))
	copy(before, stream.Buf)

	if err := batch.SetFloat64("settlement", 0, 1.0); err == nil {
		t.Fatal("SetFloat64 on the Utf8 settlement column succeeded, want an error")
	}
	if err := batch.SetInt64("settlement", 0, 1); err == nil {
		t.Fatal("SetInt64 on the Utf8 settlement column succeeded, want an error")
	}
	for i := range stream.Buf {
		if stream.Buf[i] != before[i] {
			t.Fatalf("rejected write still changed byte %d", i)
		}
	}
}

func TestSetRejectsTypeAndRange(t *testing.T) {
	wire, _ := fixture(t)
	_, batch := orderBatch(t, wire)

	if err := batch.SetFloat64("valid", 0, 1.0); err == nil {
		t.Error("SetFloat64 on a Bool column succeeded, want an error")
	}
	if err := batch.SetBool("amount", 0, true); err == nil {
		t.Error("SetBool on a Float64 column succeeded, want an error")
	}
	if err := batch.SetFloat64("usd_amount", batch.Rows, 1.0); err == nil {
		t.Error("SetFloat64 past the last row succeeded, want an error")
	}
	if err := batch.SetFloat64("usd_amount", -1, 1.0); err == nil {
		t.Error("SetFloat64 at row -1 succeeded, want an error")
	}
	if err := batch.SetFloat64("nope", 0, 1.0); err == nil {
		t.Error("SetFloat64 on an unknown field succeeded, want an error")
	}
	if err := batch.SetInt64("amount", 0, 1); err == nil {
		t.Error("SetInt64 on a Float64 column succeeded, want an error")
	}
	if err := batch.SetInt64("review_tier", batch.Rows, 1); err == nil {
		t.Error("SetInt64 past the last row succeeded, want an error")
	}
	if err := batch.SetInt64("review_tier", -1, 1); err == nil {
		t.Error("SetInt64 at row -1 succeeded, want an error")
	}
	if err := batch.SetInt64("nope", 0, 1); err == nil {
		t.Error("SetInt64 on an unknown field succeeded, want an error")
	}
	if _, err := batch.Float64s("region"); err == nil {
		t.Error("Float64s on a Utf8 column succeeded, want an error")
	}
	if _, err := batch.Strings("amount"); err == nil {
		t.Error("Strings on a Float64 column succeeded, want an error")
	}
}

func TestParseRejectsMalformedFraming(t *testing.T) {
	wire, _ := fixture(t)

	if _, err := arrowipc.Parse(nil); err == nil {
		t.Error("Parse(nil) succeeded, want an error")
	}
	if _, err := arrowipc.Parse([]byte{4, 0, 0, 0}); err == nil {
		t.Error("Parse of a segment length with no segment succeeded, want an error")
	}
	if _, err := arrowipc.Parse(wire[:len(wire)-1]); err == nil {
		t.Error("Parse of a truncated stream succeeded, want an error")
	}
	if _, err := arrowipc.Parse(append(append([]byte{}, wire...), 0)); err == nil {
		t.Error("Parse of a stream with trailing bytes succeeded, want an error")
	}

	// A segment whose first message lost its continuation marker is not an
	// Arrow IPC stream, and must be reported rather than read past.
	corrupt := append([]byte{}, wire...)
	binary.LittleEndian.PutUint32(corrupt[4:], 0)
	stream, err := arrowipc.Parse(corrupt)
	if err != nil {
		t.Fatalf("Parse of a framing-valid stream: %v", err)
	}
	if _, err := stream.Component("Order"); err == nil {
		t.Error("Component on a segment with no continuation marker succeeded, want an error")
	}
}

// TestDecodeBase64 covers the one encoding helper a processor needs: its generated
// schema constant is base64, and nothing else in the package decodes it.
func TestDecodeBase64(t *testing.T) {
	out, err := arrowipc.DecodeBase64("aGVsbG8=")
	if err != nil {
		t.Fatalf("DecodeBase64 of a valid vector: %v", err)
	}
	if string(out) != "hello" {
		t.Errorf("DecodeBase64(%q) = %q, want %q", "aGVsbG8=", out, "hello")
	}
	if _, err := arrowipc.DecodeBase64("not base64!"); err == nil {
		t.Error("DecodeBase64 of a non-base64 input succeeded, want an error")
	}
}
