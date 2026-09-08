package arrowipc

// Metadata-level tests for the encoder.
//
// These live inside the package because the properties they check are inside the
// flatbuffers: the metadata version, the padding of the metadata block, the
// field nodes, and every buffer's declared span. The exported reader deliberately
// hides all of it, and re-deriving a flatbuffer reader in an external test would
// leave the encoder checked against a second hand-rolled parser rather than the
// one the host uses.

import (
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// walkMessage returns one framed message's root table, body offset and declared
// body length, after checking the framing rules the wire format fixes.
func walkMessage(t *testing.T, buf []byte, pos int) (fbTable, int, int) {
	t.Helper()
	if binary.LittleEndian.Uint32(buf[pos:]) != continuation {
		t.Fatalf("offset %d does not open a message", pos)
	}
	metaLen := int(binary.LittleEndian.Uint32(buf[pos+4:]))
	if metaLen == 0 {
		t.Fatalf("offset %d is the end-of-stream marker, want a message", pos)
	}
	if metaLen%8 != 0 {
		t.Errorf("message at %d declares %d metadata bytes, want a multiple of 8", pos, metaLen)
	}

	fb := fbBuf(buf[pos+8 : pos+8+metaLen])
	root, err := fb.root()
	if err != nil {
		t.Fatalf("message at %d: %v", pos, err)
	}
	// MetadataVersion is an i16 and V5 is 4, so its low byte is the whole
	// value on a little-endian wire.
	version, err := root.u8(msgVersionID, 0)
	if err != nil {
		t.Fatalf("message at %d version: %v", pos, err)
	}
	if version != uint8(metadataVersionV5) {
		t.Errorf("message at %d declares MetadataVersion %d, want %d (V5)", pos, version, metadataVersionV5)
	}
	// A zero bodyLength is the field's default, and arrow-rs leaves it out
	// rather than writing it, so absent reads as zero here too.
	bodyLen, err := root.i64(msgBodyLength, 0)
	if err != nil {
		t.Fatalf("message at %d bodyLength: %v", pos, err)
	}
	if bodyLen%8 != 0 {
		t.Errorf("message at %d declares a %d-byte body, want a multiple of 8", pos, bodyLen)
	}
	return root, pos + 8 + metaLen, int(bodyLen)
}

func header(t *testing.T, root fbTable, wantType uint8) fbTable {
	t.Helper()
	got, err := root.u8(msgHeaderType, 0)
	if err != nil {
		t.Fatalf("header_type: %v", err)
	}
	if got != wantType {
		t.Fatalf("header_type = %d, want %d", got, wantType)
	}
	head, ok, err := root.child(msgHeader)
	if err != nil || !ok {
		t.Fatalf("header: %v, present %v", err, ok)
	}
	return head
}

// TestEncodedSegmentMetadata checks everything the exported reader takes on
// trust: that the two messages are framed and padded correctly, that every field
// gets a node reporting the batch's row count and no nulls, and that every
// buffer's declared span is eight-byte aligned and inside the body.
func TestEncodedSegmentMetadata(t *testing.T) {
	rows := 9
	columns := []Column{
		Int64Column{Name: "id", Values: make([]int64, rows)},
		Utf8Column{Name: "region", Values: []string{"a", "", "ccc", "d", "e", "f", "g", "h", "iiiii"}},
		Float64Column{Name: "amount", Values: make([]float64, rows)},
		BoolColumn{Name: "valid", Values: make([]bool, rows)},
	}
	// Two buffer slots per fixed-width field, three for the Utf8 one.
	wantBuffers := 2*3 + 3

	w := NewWriter()
	if err := w.WriteComponent("Order", 7, columns...); err != nil {
		t.Fatalf("WriteComponent: %v", err)
	}
	segment := firstSegment(t, w.Bytes())

	schemaRoot, schemaBody, schemaBodyLen := walkMessage(t, segment, 0)
	if schemaBodyLen != 0 {
		t.Errorf("schema message declares a %d-byte body, want 0", schemaBodyLen)
	}
	schema := header(t, schemaRoot, headerSchema)
	assertMetadata(t, schema, map[string]string{componentKey: "Order", versionKey: "7"})
	assertFields(t, schema, []SchemaField{
		{Name: "id", Type: TypeInt64},
		{Name: "region", Type: TypeUtf8},
		{Name: "amount", Type: TypeFloat64},
		{Name: "valid", Type: TypeBool},
	})

	batchRoot, body, bodyLen := walkMessage(t, segment, schemaBody)
	batch := header(t, batchRoot, headerRecordBatch)
	if batch.has(batchCompressionID) {
		t.Error("record batch declares compression, want it absent")
	}
	if got, err := batch.i64(batchLengthID, -1); err != nil || got != int64(rows) {
		t.Errorf("record batch length = %d (%v), want %d", got, err, rows)
	}
	if want := body + bodyLen + 8; want != len(segment) {
		t.Errorf("segment is %d bytes, want %d for the body plus the end-of-stream marker", len(segment), want)
	}

	nodes, ok, err := batch.vector(batchNodesID)
	if err != nil || !ok {
		t.Fatalf("nodes: %v, present %v", err, ok)
	}
	if nodes.count != len(columns) {
		t.Fatalf("record batch carries %d field nodes for %d fields", nodes.count, len(columns))
	}
	for i := range nodes.count {
		at, err := nodes.inline(i, fieldNodeSize)
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		length, _ := nodes.buf.i64(at)
		nulls, _ := nodes.buf.i64(at + 8)
		if length != int64(rows) || nulls != 0 {
			t.Errorf("node %d = {length %d, null_count %d}, want {%d, 0}", i, length, nulls, rows)
		}
	}

	buffers, ok, err := batch.vector(batchBuffersID)
	if err != nil || !ok {
		t.Fatalf("buffers: %v, present %v", err, ok)
	}
	if buffers.count != wantBuffers {
		t.Fatalf("record batch carries %d buffers, want %d", buffers.count, wantBuffers)
	}
	end := 0
	for i := range buffers.count {
		at, err := buffers.inline(i, bufferSize)
		if err != nil {
			t.Fatalf("buffer %d: %v", i, err)
		}
		off, _ := buffers.buf.i64(at)
		length, _ := buffers.buf.i64(at + 8)
		if off%8 != 0 {
			t.Errorf("buffer %d starts at %d, want an eight-byte boundary", i, off)
		}
		if off < int64(end) {
			t.Errorf("buffer %d starts at %d, overlapping the previous buffer ending at %d", i, off, end)
		}
		if off+length > int64(bodyLen) {
			t.Errorf("buffer %d spans [%d,%d) of a %d-byte body", i, off, off+length, bodyLen)
		}
		end = int(off + length)
	}
	// The validity, offsets and values buffers of the last field must reach the
	// body's end, or the encoder declared a body longer than it filled.
	if align8(end) != bodyLen {
		t.Errorf("buffers end at %d of a %d-byte body", end, bodyLen)
	}
}

// TestDescriptorSchemaAgreesWithHost re-encodes the schema arrow-rs generated
// for the canonical `Order` component and compares what the host reads out of
// both streams.
//
// The field list is read out of the generated file rather than written here, so
// the test follows the canonical `Order` through a column being added instead of
// pinning a contract that lives in the emitter. What it checks is the encoder:
// hand arrow-rs's own field list back to EncodeSchema and the host must see the
// same names, the same types and the same absent metadata.
//
// The bytes differ. The Rust flatbuffers builder deduplicates vtables and this
// one does not, which costs about seventy bytes and changes every offset, so the
// comparison is at the level the host consumes.
func TestDescriptorSchemaAgreesWithHost(t *testing.T) {
	assertSchemaReEncodes(t, "arrow-rs", readFixture(t, "order_schema.ipc"))
}

// TestDescriptorSchemaAnyFieldCount runs the same comparison over a twelve-field
// schema this package builds itself.
//
// It is a guard on the test above, not a second opinion on arrow-rs: the input is
// this encoder's own output, so what it proves is that the comparison is field
// count agnostic. The generated `order_schema.ipc` holds those same twelve fields,
// and neither test pins that count, so a new column does not turn the check red.
func TestDescriptorSchemaAnyFieldCount(t *testing.T) {
	twelve, err := EncodeSchema([]SchemaField{
		{Name: "id", Type: TypeInt64},
		{Name: "region", Type: TypeUtf8},
		{Name: "currency", Type: TypeUtf8},
		{Name: "amount", Type: TypeFloat64},
		{Name: "valid", Type: TypeBool},
		{Name: "usd_amount", Type: TypeFloat64},
		{Name: "usd_amount_display", Type: TypeUtf8},
		{Name: "risk_score", Type: TypeFloat64},
		{Name: "flagged", Type: TypeBool},
		{Name: "fee", Type: TypeFloat64},
		{Name: "review_tier", Type: TypeInt64},
		{Name: "settlement", Type: TypeUtf8},
	})
	if err != nil {
		t.Fatalf("EncodeSchema: %v", err)
	}
	assertSchemaReEncodes(t, "twelve-field", twelve)
}

// assertSchemaReEncodes reads a descriptor stream's field list, encodes that list
// again, and checks a host reads the same schema out of either stream.
func assertSchemaReEncodes(t *testing.T, source string, hostBytes []byte) {
	t.Helper()

	hostRoot, hostBody, hostBodyLen := walkMessage(t, hostBytes, 0)
	if hostBodyLen != 0 {
		t.Errorf("%s schema declares a %d-byte body, want 0", source, hostBodyLen)
	}
	if want := hostBody + 8; want != len(hostBytes) {
		t.Errorf("%s stream is %d bytes, want %d for one message and the marker", source, len(hostBytes), want)
	}
	fields := readFields(t, header(t, hostRoot, headerSchema))
	if len(fields) == 0 {
		t.Fatalf("%s schema declares no fields", source)
	}
	t.Logf("%s declares %d fields: %v", source, len(fields), fields)

	got, err := EncodeSchema(fields)
	if err != nil {
		t.Fatalf("EncodeSchema: %v", err)
	}

	for _, c := range []struct {
		name  string
		bytes []byte
	}{{source, hostBytes}, {"this codec", got}} {
		root, body, bodyLen := walkMessage(t, c.bytes, 0)
		if bodyLen != 0 {
			t.Errorf("%s schema declares a %d-byte body, want 0", c.name, bodyLen)
		}
		if want := body + 8; want != len(c.bytes) {
			t.Errorf("%s stream is %d bytes, want %d for one message and the marker", c.name, len(c.bytes), want)
		}
		schema := header(t, root, headerSchema)
		assertFields(t, schema, fields)
		// A descriptor schema carries no `__saci_component`: it describes the
		// component, it is not a segment of one.
		if _, ok, err := schema.vector(schemaMetadataID); err != nil || ok {
			t.Errorf("%s schema carries custom_metadata (%v), want none", c.name, err)
		}
	}
}

// readFields lifts a Schema's fields vector into the encoder's own field list.
func readFields(t *testing.T, schema fbTable) []SchemaField {
	t.Helper()
	vec, ok, err := schema.vector(schemaFieldsID)
	if err != nil || !ok {
		t.Fatalf("fields: %v, present %v", err, ok)
	}
	out := make([]SchemaField, vec.count)
	for i := range vec.count {
		field, err := vec.table(i)
		if err != nil {
			t.Fatalf("field %d: %v", i, err)
		}
		name, ok, err := field.str(fieldNameID)
		if err != nil || !ok {
			t.Fatalf("field %d name: %v, present %v", i, err, ok)
		}
		typ, err := field.u8(fieldTypeTypeID, 0)
		if err != nil {
			t.Fatalf("field %q type_type: %v", name, err)
		}
		out[i] = SchemaField{Name: name, Type: ColumnType(typ)}
	}
	return out
}

// assertFields checks a Schema's fields vector, including each type's payload.
func assertFields(t *testing.T, schema fbTable, want []SchemaField) {
	t.Helper()
	vec, ok, err := schema.vector(schemaFieldsID)
	if err != nil || !ok {
		t.Fatalf("fields: %v, present %v", err, ok)
	}
	if vec.count != len(want) {
		t.Fatalf("schema declares %d fields, want %d", vec.count, len(want))
	}
	for i, f := range want {
		field, err := vec.table(i)
		if err != nil {
			t.Fatalf("field %d: %v", i, err)
		}
		name, _, err := field.str(fieldNameID)
		if err != nil {
			t.Fatalf("field %d name: %v", i, err)
		}
		if name != f.Name {
			t.Errorf("field %d is %q, want %q", i, name, f.Name)
		}
		typ, err := field.u8(fieldTypeTypeID, 0)
		if err != nil {
			t.Fatalf("field %q type_type: %v", f.Name, err)
		}
		if ColumnType(typ) != f.Type {
			t.Errorf("field %q is %s, want %s", f.Name, ColumnType(typ), f.Type)
		}

		payload, ok, err := field.child(fieldTypeID)
		if err != nil || !ok {
			t.Fatalf("field %q type payload: %v, present %v", f.Name, err, ok)
		}
		switch f.Type {
		case TypeInt64:
			// bitWidth is an i32; its low byte carries 64 on a little-endian wire.
			if width, err := payload.u8(intBitWidthID, 0); err != nil || width != 64 {
				t.Errorf("field %q bitWidth = %d (%v), want 64", f.Name, width, err)
			}
			if signed, err := payload.u8(intSignedID, 0); err != nil || signed != 1 {
				t.Errorf("field %q is_signed = %d (%v), want 1", f.Name, signed, err)
			}
		case TypeFloat64:
			if p, err := payload.u8(floatPrecisionID, 0); err != nil || int16(p) != precisionDouble {
				t.Errorf("field %q precision = %d (%v), want %d (DOUBLE)", f.Name, p, err, precisionDouble)
			}
		}
	}
}

// assertMetadata checks a Schema's custom_metadata holds exactly want.
func assertMetadata(t *testing.T, schema fbTable, want map[string]string) {
	t.Helper()
	vec, ok, err := schema.vector(schemaMetadataID)
	if err != nil || !ok {
		t.Fatalf("custom_metadata: %v, present %v", err, ok)
	}
	got := make(map[string]string, vec.count)
	for i := range vec.count {
		kv, err := vec.table(i)
		if err != nil {
			t.Fatalf("custom_metadata[%d]: %v", i, err)
		}
		key, _, err := kv.str(kvKeyID)
		if err != nil {
			t.Fatalf("custom_metadata[%d] key: %v", i, err)
		}
		value, _, err := kv.str(kvValueID)
		if err != nil {
			t.Fatalf("custom_metadata[%d] value: %v", i, err)
		}
		got[key] = value
	}
	if len(got) != len(want) {
		t.Fatalf("custom_metadata = %v, want %v", got, want)
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("custom_metadata[%q] = %q, want %q", key, got[key], value)
		}
	}
}

// firstSegment strips the length prefix off a stream's first segment.
func firstSegment(t *testing.T, stream []byte) []byte {
	t.Helper()
	if len(stream) < 4 {
		t.Fatal("stream is too short to hold a segment")
	}
	length := int(binary.LittleEndian.Uint32(stream))
	if length == 0 || 4+length > len(stream) {
		t.Fatalf("stream declares a %d-byte first segment of %d bytes", length, len(stream))
	}
	return stream[4 : 4+length]
}

// The generated fixture directory and the command that fills it. These repeat
// what arrowipc_test.go's generatedDir and emitHint hold, because that file is
// package arrowipc_test and this one is package arrowipc, so the constants
// cannot be shared. A rename of the emitter has to land in both.
const (
	fixtureDir  = "../../../examples/polyglot/generated"
	fixtureHint = "run `cargo run -p saci-service --features wasm --example polyglot_schema_emit -- emit` first"
)

// readFixture reads a generated file, skipping when the emitter has not run.
func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir, name))
	if errors.Is(err, fs.ErrNotExist) {
		t.Skipf("%s is absent: %s", name, fixtureHint)
	}
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return raw
}
