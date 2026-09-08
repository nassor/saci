// Package arrowipc reads and mutates the SACI host<->processor wire format using
// nothing but the Go standard library.
//
// Wire format, with examples/polyglot/generated/fixture_input.saci as the
// reference stream:
//
//	saci_stream := segment* terminator
//	segment    := u32le segment_len ++ arrow_ipc_stream[segment_len]
//	terminator := u32le 0x00000000
//	message    := u32le 0xFFFFFFFF ++ u32le metadata_len
//	           ++ flatbuffer[metadata_len] ++ body[bodyLength]
//
// One segment per registered component ordered by component name, then an
// `__alive` bitmap segment. Each segment is a standalone Arrow IPC stream: one
// Schema message, one RecordBatch message, then an end-of-stream marker.
// `metadata_len` already includes the flatbuffer's padding to 8 bytes, and the
// next message starts at align8(body_start + bodyLength).
//
// # What this codec cannot do
//
// It never *writes* a flatbuffer. Overwriting a fixed-width value slot is a read
// of the flatbuffer metadata plus a byte write into the body, which is all the
// standard library is needed for. SetFloat64 and SetBool therefore accept
// fixed-width fields only: changing a Utf8 value would shift every following
// offset and force a rewrite of the RecordBatch metadata. `settlement`, the
// chain's one variable-length output, belongs to the Rust stage instead, which
// has a real Arrow writer.
//
// The trailing `__alive` segment is never parsed and never touched: the host
// marks every row of a batch alive, and a processor that can neither add nor remove
// rows cannot change that. Those bytes pass through byte-identical, as does
// every flatbuffer and every framing word.
//
// Malformed input yields an [Error], never a panic and never a
// standard-library error: this code runs inside a component whose only failure
// channel is the WIT `permanent(string)` arm, and a Go panic there traps the
// instance instead of reporting the reason.
package arrowipc

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
)

// Error is the only error this package returns.
//
// The type is what lets a caller separate a refused input from a bug in the
// codec. A bare `encoding/base64` error or an index-out-of-range panic reaching
// a processor would leave it unable to tell "the host handed me bytes I cannot
// read" from "this codec is broken", and those two need different answers: the
// first is a `permanent` run error naming the stream, the second is a bug
// report.
type Error struct {
	msg string
}

func (e *Error) Error() string { return e.msg }

// errf builds an [Error]. Every refusal in this package goes through it, so
// there is no path on which the type can be forgotten.
//
// Causes are formatted into the message rather than chained: every cause is
// already an *Error, so a chain would nest one type inside itself and buy a
// caller nothing that the text does not already carry.
func errf(format string, args ...any) *Error {
	return &Error{msg: fmt.Sprintf(format, args...)}
}

// Framing and Arrow discriminants.
const (
	// continuation prefixes every IPC message; a metadata length of 0 right
	// after it is the end-of-stream marker.
	continuation uint32 = 0xFFFFFFFF

	// Message.header_type values.
	headerSchema      uint8 = 1
	headerDictionary  uint8 = 2
	headerRecordBatch uint8 = 3

	// Field.type_type values this codec understands. Anything else in a
	// component segment is rejected rather than guessed at.
	typeInt   uint8 = 2
	typeFloat uint8 = 3
	typeUtf8  uint8 = 5
	typeBool  uint8 = 6

	// Inline FlatBuffers struct sizes: FieldNode{i64,i64}, Buffer{i64,i64}.
	fieldNodeSize = 16
	bufferSize    = 16

	// componentKey names the Schema custom_metadata entry the host writes to
	// label a segment. A segment without it is not addressable, so its absence
	// is an error rather than a skip.
	componentKey = "__saci_component"
)

// FlatBuffers vtable field ids, from Arrow's Message.fbs and Schema.fbs. A
// union occupies two consecutive slots (discriminant, then payload), which is
// where Field.type_type = 2 comes from.
const (
	msgHeaderType = 1
	msgHeader     = 2
	msgBodyLength = 3

	schemaFieldsID   = 1
	schemaMetadataID = 2

	fieldNameID     = 0
	fieldTypeTypeID = 2

	batchLengthID      = 0
	batchNodesID       = 1
	batchBuffersID     = 2
	batchCompressionID = 3

	kvKeyID   = 0
	kvValueID = 1
)

// DecodeBase64 decodes standard base64 with padding (RFC 4648 section 4).
//
// A processor that embeds its component schema as a generated constant needs this
// and nothing else from an encoding library.
func DecodeBase64(text string) ([]byte, error) {
	out, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return nil, errf("decode base64: %v", err)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Stream: segment framing.
// ---------------------------------------------------------------------------

// Stream is a parsed SACI wire-format stream.
//
// Buf is the processor-owned mutable copy of the input: every Set call writes into
// it, and it is what the processor hands back to the host as `run-result.output`.
type Stream struct {
	Buf      []byte
	segments []segment
}

// segment locates one embedded Arrow IPC stream inside Buf.
type segment struct {
	start int // absolute offset of the segment's first IPC message
	end   int // absolute offset one past the segment's last byte
}

// Parse splits input into segments.
//
// The input is copied. The slice the generated export glue hands a processor aliases
// memory pinned only for the duration of the call, so owning a copy is what
// makes in-place mutation and returning the buffer safe.
func Parse(input []byte) (*Stream, error) {
	buf := make([]byte, len(input))
	copy(buf, input)
	s := &Stream{Buf: buf}

	pos := 0
	for {
		if pos+4 > len(buf) {
			return nil, errf("truncated stream: no segment length at offset %d of %d bytes", pos, len(buf))
		}
		segLen := int(binary.LittleEndian.Uint32(buf[pos:]))
		pos += 4
		if segLen == 0 {
			break
		}
		if segLen < 0 || pos+segLen > len(buf) {
			return nil, errf("truncated stream: segment at offset %d declares %d bytes, %d remain", pos-4, segLen, len(buf)-pos)
		}
		s.segments = append(s.segments, segment{start: pos, end: pos + segLen})
		pos += segLen
	}
	if len(s.segments) == 0 {
		return nil, errf("stream declares no segments")
	}
	if pos != len(buf) {
		return nil, errf("%d bytes trail the stream terminator", len(buf)-pos)
	}
	return s, nil
}

// Components lists the component each segment declares, in segment order.
//
// A processor that addresses components it already knows by name never needs this.
// A conformance harness and a diagnostic dump both do, and neither should have
// to reach into the segment table to get it.
func (s *Stream) Components() ([]string, error) {
	out := make([]string, len(s.segments))
	for i := range s.segments {
		sc, err := s.schemaOf(i)
		if err != nil {
			return nil, err
		}
		out[i] = sc.component
	}
	return out, nil
}

// Component returns the batch of the segment whose Schema metadata declares the
// given component name.
func (s *Stream) Component(name string) (*Batch, error) {
	for i := range s.segments {
		sc, err := s.schemaOf(i)
		if err != nil {
			return nil, err
		}
		if sc.component != name {
			continue
		}
		batch, err := s.batch(s.segments[i], sc, name)
		if err != nil {
			return nil, errf("segment %d (%s): %v", i, name, err)
		}
		return batch, nil
	}
	return nil, errf("no segment declares component %q", name)
}

// segmentSchema is one segment's validated opening message: the framing, the
// Schema table itself, and the component name its metadata declares.
type segmentSchema struct {
	msg       message
	header    fbTable
	component string
}

// schemaOf checks that segment i opens with a Schema message carrying a
// component label.
//
// Both entry points that touch a segment go through here, so a segment that is
// empty, opens with the wrong message, or declares no component is refused
// identically whether the caller wanted that one component or only the list.
func (s *Stream) schemaOf(i int) (segmentSchema, error) {
	seg := s.segments[i]
	msg, err := s.message(seg.start, seg.end)
	if err != nil {
		return segmentSchema{}, errf("segment %d: %v", i, err)
	}
	if !msg.present {
		return segmentSchema{}, errf("segment %d is empty", i)
	}
	if msg.headerType != headerSchema {
		return segmentSchema{}, errf("segment %d opens with header_type %d, want %d (Schema)", i, msg.headerType, headerSchema)
	}
	header, ok, err := msg.root.child(msgHeader)
	if err != nil {
		return segmentSchema{}, errf("segment %d schema header: %v", i, err)
	}
	if !ok {
		return segmentSchema{}, errf("segment %d schema message carries no header", i)
	}
	name, err := componentOf(header)
	if err != nil {
		return segmentSchema{}, errf("segment %d: %v", i, err)
	}
	return segmentSchema{msg: msg, header: header, component: name}, nil
}

// componentOf reads the `__saci_component` label out of a Schema's
// custom_metadata.
func componentOf(schema fbTable) (string, error) {
	meta, ok, err := schema.vector(schemaMetadataID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errf("schema has no custom_metadata, so no %q label", componentKey)
	}
	for i := range meta.count {
		kv, err := meta.table(i)
		if err != nil {
			return "", errf("custom_metadata[%d]: %v", i, err)
		}
		key, _, err := kv.str(kvKeyID)
		if err != nil {
			return "", errf("custom_metadata[%d] key: %v", i, err)
		}
		if key != componentKey {
			continue
		}
		value, ok, err := kv.str(kvValueID)
		if err != nil {
			return "", errf("%s value: %v", componentKey, err)
		}
		if !ok {
			return "", errf("%s metadata entry has no value", componentKey)
		}
		return value, nil
	}
	return "", errf("schema custom_metadata has no %q key", componentKey)
}

// ---------------------------------------------------------------------------
// Message framing.
// ---------------------------------------------------------------------------

// message is one framed Arrow IPC message inside a segment.
type message struct {
	present    bool // false for the end-of-stream marker
	root       fbTable
	headerType uint8
	body       int // absolute offset of the message body in Stream.Buf
	bodyLen    int
	next       int // absolute offset of the following message
}

func (s *Stream) message(pos, limit int) (message, error) {
	if pos+8 > limit {
		return message{}, errf("truncated message prefix at offset %d", pos)
	}
	if binary.LittleEndian.Uint32(s.Buf[pos:]) != continuation {
		return message{}, errf("offset %d is not an IPC message: continuation marker missing", pos)
	}
	metaLen := int(binary.LittleEndian.Uint32(s.Buf[pos+4:]))
	if metaLen == 0 {
		return message{}, nil // end-of-stream
	}
	if metaLen < 0 || pos+8+metaLen > limit {
		return message{}, errf("message at offset %d declares %d metadata bytes, %d remain", pos, metaLen, limit-pos-8)
	}

	fb := fbBuf(s.Buf[pos+8 : pos+8+metaLen])
	root, err := fb.root()
	if err != nil {
		return message{}, errf("message at offset %d: %v", pos, err)
	}
	headerType, err := root.u8(msgHeaderType, 0)
	if err != nil {
		return message{}, errf("message at offset %d header_type: %v", pos, err)
	}
	rawBodyLen, err := root.i64(msgBodyLength, 0)
	if err != nil {
		return message{}, errf("message at offset %d bodyLength: %v", pos, err)
	}
	body := pos + 8 + metaLen
	bodyLen, err := asLength(rawBodyLen, "bodyLength")
	if err != nil {
		return message{}, err
	}
	if body+bodyLen > limit {
		return message{}, errf("message at offset %d declares a %d-byte body, %d remain", pos, bodyLen, limit-body)
	}
	return message{
		present:    true,
		root:       root,
		headerType: headerType,
		body:       body,
		bodyLen:    bodyLen,
		next:       body + align8(bodyLen),
	}, nil
}

// endOfSegment checks that nothing follows the record batch but, at most, the
// end-of-stream marker.
//
// The marker is optional because a segment's declared length is what bounds it;
// bytes after the marker are not, because the writer that produced them
// disagreed with the length it wrote.
func (s *Stream) endOfSegment(pos, end int) error {
	if pos == end {
		return nil
	}
	// Too short to be a marker, so it cannot be anything this format defines.
	// Reading it as a message would report a truncated prefix, which names the
	// wrong defect: these bytes are surplus, not cut off.
	if end-pos < 8 {
		return errf("segment carries %d bytes after its record batch, too few for an end-of-stream marker, want one Schema and one RecordBatch", end-pos)
	}
	tail, err := s.message(pos, end)
	if err != nil {
		return err
	}
	if tail.present {
		return errf("segment carries a third message with header_type %d, want one Schema and one RecordBatch", tail.headerType)
	}
	if pos+8 != end {
		return errf("segment carries %d bytes after its end-of-stream marker, want one Schema and one RecordBatch", end-pos-8)
	}
	return nil
}

func align8(n int) int { return (n + 7) & ^7 }

// asLength narrows an on-the-wire i64 to int, rejecting the values that would
// otherwise turn into an out-of-range slice index.
func asLength(v int64, what string) (int, error) {
	if v < 0 || v > int64(^uint(0)>>1) {
		return 0, errf("%s is %d, which is not a usable length", what, v)
	}
	return int(v), nil
}

// asRowCount narrows the declared RecordBatch row count.
//
// The bound is the format's, not the platform's: a Utf8 column addresses its
// values buffer with i32 offsets, so a batch wider than an i32 cannot be
// described here at all. asLength alone would not catch it, because on a 64-bit
// build its ceiling is i64's, and a row count of i64 max would then flow into
// every later length computation.
func asRowCount(v int64) (int, error) {
	if v < 0 || v > math.MaxInt32 {
		return 0, errf("record batch length is %d, which is not a usable length", v)
	}
	return int(v), nil
}

// ---------------------------------------------------------------------------
// Batch: columns of one component segment.
// ---------------------------------------------------------------------------

// span is one Arrow buffer, resolved to absolute offsets in Stream.Buf.
type span struct {
	off int
	len int
}

// field is a schema field paired with the buffers the RecordBatch assigned it.
// The validity span is resolved and then never read: every `Order` field is
// non-nullable, arrow-rs still emits an all-ones validity bitmap, and an
// in-place value write therefore never has to touch it. Resolving it anyway is
// the point, because a validity buffer that leaves the body is a malformed
// batch whether or not this codec would have looked at those bytes.
type field struct {
	name     string
	typ      uint8
	validity span
	offsets  span // Utf8 only
	values   span
}

// Batch is the RecordBatch of one component segment, addressable by field name.
type Batch struct {
	// Rows is the RecordBatch row count.
	Rows int

	component string
	buf       []byte // aliases Stream.Buf: Set calls land in the stream
	fields    []field
}

func (s *Stream) batch(seg segment, sc segmentSchema, name string) (*Batch, error) {
	fields, err := schemaFieldsOf(sc.header)
	if err != nil {
		return nil, err
	}

	rb, err := s.message(sc.msg.next, seg.end)
	if err != nil {
		return nil, err
	}
	if !rb.present {
		return nil, errf("segment ends after its schema, with no record batch")
	}
	if rb.headerType != headerRecordBatch {
		if rb.headerType == headerDictionary {
			return nil, errf("segment carries a dictionary batch, which this codec does not support")
		}
		return nil, errf("second message has header_type %d, want %d (RecordBatch)", rb.headerType, headerRecordBatch)
	}
	rbHeader, ok, err := rb.root.child(msgHeader)
	if err != nil {
		return nil, errf("record batch header: %v", err)
	}
	if !ok {
		return nil, errf("record batch message carries no header")
	}
	// Body compression would make every value offset below meaningless.
	if rbHeader.has(batchCompressionID) {
		return nil, errf("record batch body is compressed, which this codec does not support")
	}
	// A segment is exactly one Schema, one RecordBatch, and an optional
	// end-of-stream marker. Anything else and a reader would silently drop data
	// it was handed, which is worse than refusing the stream.
	if err := s.endOfSegment(rb.next, seg.end); err != nil {
		return nil, err
	}

	rawRows, err := rbHeader.i64(batchLengthID, 0)
	if err != nil {
		return nil, errf("record batch length: %v", err)
	}
	rows, err := asRowCount(rawRows)
	if err != nil {
		return nil, err
	}

	nodes, ok, err := rbHeader.vector(batchNodesID)
	if err != nil {
		return nil, errf("record batch nodes: %v", err)
	}
	if !ok || nodes.count != len(fields) {
		return nil, errf("record batch has %d field nodes for %d schema fields", nodes.count, len(fields))
	}
	if _, err := nodes.inline(nodes.count-1, fieldNodeSize); nodes.count > 0 && err != nil {
		return nil, errf("record batch nodes: %v", err)
	}
	buffers, ok, err := rbHeader.vector(batchBuffersID)
	if err != nil {
		return nil, errf("record batch buffers: %v", err)
	}
	if !ok {
		return nil, errf("record batch carries no buffers vector")
	}

	// Buffer slots are assigned by walking the schema in field order; the slot
	// count is fixed by type_type, never inferred from a buffer's length.
	next := 0
	take := func(fieldName string) (span, error) {
		if next >= buffers.count {
			return span{}, errf("field %q needs buffer slot %d, record batch has %d", fieldName, next, buffers.count)
		}
		s, err := buffers.buffer(next, rb.body, rb.bodyLen)
		next++
		return s, err
	}
	for i := range fields {
		f := &fields[i]
		if f.validity, err = take(f.name); err != nil {
			return nil, err
		}
		switch f.typ {
		case typeInt, typeFloat, typeBool:
		case typeUtf8:
			if f.offsets, err = take(f.name); err != nil {
				return nil, err
			}
		default:
			return nil, errf("field %q has unsupported type_type %d", f.name, f.typ)
		}
		if f.values, err = take(f.name); err != nil {
			return nil, err
		}
	}
	if next != buffers.count {
		return nil, errf("schema consumes %d buffer slots, record batch carries %d", next, buffers.count)
	}

	return &Batch{Rows: rows, component: name, buf: s.Buf, fields: fields}, nil
}

// schemaFieldsOf reads field names and type discriminants in schema order,
// which is also buffer-walk order.
func schemaFieldsOf(schema fbTable) ([]field, error) {
	vec, ok, err := schema.vector(schemaFieldsID)
	if err != nil {
		return nil, errf("schema fields: %v", err)
	}
	if !ok {
		return nil, errf("schema carries no fields vector")
	}
	fields := make([]field, vec.count)
	for i := range vec.count {
		t, err := vec.table(i)
		if err != nil {
			return nil, errf("schema field %d: %v", i, err)
		}
		name, ok, err := t.str(fieldNameID)
		if err != nil {
			return nil, errf("schema field %d name: %v", i, err)
		}
		if !ok {
			return nil, errf("schema field %d has no name", i)
		}
		typ, err := t.u8(fieldTypeTypeID, 0)
		if err != nil {
			return nil, errf("schema field %q type_type: %v", name, err)
		}
		fields[i] = field{name: name, typ: typ}
	}
	return fields, nil
}

// FieldIndex returns the schema position of a field.
func (b *Batch) FieldIndex(name string) (int, error) {
	for i := range b.fields {
		if b.fields[i].name == name {
			return i, nil
		}
	}
	return 0, errf("component %q has no field %q", b.component, name)
}

// Int64s decodes an Int64 column.
func (b *Batch) Int64s(name string) ([]int64, error) {
	f, err := b.reader(name, typeInt, 8)
	if err != nil {
		return nil, err
	}
	out := make([]int64, b.Rows)
	for i := range out {
		out[i] = int64(binary.LittleEndian.Uint64(b.buf[f.values.off+i*8:]))
	}
	return out, nil
}

// Float64s decodes a Float64 column.
func (b *Batch) Float64s(name string) ([]float64, error) {
	f, err := b.reader(name, typeFloat, 8)
	if err != nil {
		return nil, err
	}
	out := make([]float64, b.Rows)
	for i := range out {
		out[i] = math.Float64frombits(binary.LittleEndian.Uint64(b.buf[f.values.off+i*8:]))
	}
	return out, nil
}

// Bools decodes a Boolean column from its LSB-first bitmap.
func (b *Batch) Bools(name string) ([]bool, error) {
	f, err := b.field(name, typeBool)
	if err != nil {
		return nil, err
	}
	if err := b.needBits(f); err != nil {
		return nil, err
	}
	out := make([]bool, b.Rows)
	for i := range out {
		out[i] = b.buf[f.values.off+i/8]>>(uint(i)&7)&1 == 1
	}
	return out, nil
}

// Strings decodes a Utf8 column through its i32 offsets buffer.
func (b *Batch) Strings(name string) ([]string, error) {
	f, err := b.field(name, typeUtf8)
	if err != nil {
		return nil, err
	}
	if need := (b.Rows + 1) * 4; f.offsets.len < need {
		return nil, errf("field %q offsets buffer holds %d bytes, need %d for %d rows", name, f.offsets.len, need, b.Rows)
	}
	out := make([]string, b.Rows)
	for i := range out {
		start := int(int32(binary.LittleEndian.Uint32(b.buf[f.offsets.off+i*4:])))
		end := int(int32(binary.LittleEndian.Uint32(b.buf[f.offsets.off+(i+1)*4:])))
		if start < 0 || end < start || end > f.values.len {
			return nil, errf("field %q row %d offsets [%d,%d) escape its %d-byte values buffer", name, i, start, end, f.values.len)
		}
		out[i] = string(b.buf[f.values.off+start : f.values.off+end])
	}
	return out, nil
}

// SetInt64 overwrites one Int64 value in place.
func (b *Batch) SetInt64(name string, row int, v int64) error {
	f, err := b.writer(name, typeInt, row, 8)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(b.buf[f.values.off+row*8:], uint64(v))
	return nil
}

// SetFloat64 overwrites one Float64 value in place.
func (b *Batch) SetFloat64(name string, row int, v float64) error {
	f, err := b.writer(name, typeFloat, row, 8)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(b.buf[f.values.off+row*8:], math.Float64bits(v))
	return nil
}

// SetBool overwrites one bit of a Boolean column's bitmap in place.
func (b *Batch) SetBool(name string, row int, v bool) error {
	f, err := b.field(name, typeBool)
	if err != nil {
		return err
	}
	if err := b.checkRow(name, row); err != nil {
		return err
	}
	if err := b.needBits(f); err != nil {
		return err
	}
	mask := byte(1) << (uint(row) & 7)
	at := f.values.off + row/8
	if v {
		b.buf[at] |= mask
	} else {
		b.buf[at] &= ^mask
	}
	return nil
}

// field resolves a name and checks its Arrow type.
func (b *Batch) field(name string, want uint8) (*field, error) {
	i, err := b.FieldIndex(name)
	if err != nil {
		return nil, err
	}
	f := &b.fields[i]
	if f.typ != want {
		return nil, errf("field %q is %s, not %s", name, typeName(f.typ), typeName(want))
	}
	return f, nil
}

// reader resolves a fixed-width field and checks its values buffer covers every
// row.
func (b *Batch) reader(name string, want uint8, width int) (*field, error) {
	f, err := b.field(name, want)
	if err != nil {
		return nil, err
	}
	if need := b.Rows * width; f.values.len < need {
		return nil, errf("field %q values buffer holds %d bytes, need %d for %d rows", name, f.values.len, need, b.Rows)
	}
	return f, nil
}

// writer is reader plus a row bound and the variable-length refusal. A Utf8
// write would move every following offset, so it is rejected by type before the
// type-mismatch message, which would otherwise read as if a Float64 column of
// that name were merely missing.
func (b *Batch) writer(name string, want uint8, row, width int) (*field, error) {
	i, err := b.FieldIndex(name)
	if err != nil {
		return nil, err
	}
	if t := b.fields[i].typ; t != typeInt && t != typeFloat && t != typeBool {
		return nil, errf("field %q is %s: this codec writes fixed-width values only, because a variable-length write would have to rebuild the offsets buffer and the RecordBatch metadata", name, typeName(t))
	}
	if err := b.checkRow(name, row); err != nil {
		return nil, err
	}
	return b.reader(name, want, width)
}

func (b *Batch) checkRow(name string, row int) error {
	if row < 0 || row >= b.Rows {
		return errf("row %d is out of range for field %q of %d rows", row, name, b.Rows)
	}
	return nil
}

func (b *Batch) needBits(f *field) error {
	if need := (b.Rows + 7) / 8; f.values.len < need {
		return errf("field %q bitmap holds %d bytes, need %d for %d rows", f.name, f.values.len, need, b.Rows)
	}
	return nil
}

func typeName(typ uint8) string {
	switch typ {
	case typeInt:
		return "Int"
	case typeFloat:
		return "FloatingPoint"
	case typeUtf8:
		return "Utf8"
	case typeBool:
		return "Bool"
	default:
		return fmt.Sprintf("type_type %d", typ)
	}
}

// ---------------------------------------------------------------------------
// FlatBuffers reader: just enough for Arrow's Message, Schema, Field,
// RecordBatch and KeyValue tables.
// ---------------------------------------------------------------------------

// fbBuf is one FlatBuffers-encoded Arrow metadata message. Every read is bounds
// checked: these bytes come from outside the component.
type fbBuf []byte

func (b fbBuf) bounds(off, n int) error {
	if off < 0 || n < 0 || off+n > len(b) {
		return errf("read of %d bytes at %d exceeds %d-byte metadata", n, off, len(b))
	}
	return nil
}

func (b fbBuf) u8(off int) (uint8, error) {
	if err := b.bounds(off, 1); err != nil {
		return 0, err
	}
	return b[off], nil
}

func (b fbBuf) u32(off int) (uint32, error) {
	if err := b.bounds(off, 4); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b[off:]), nil
}

func (b fbBuf) i64(off int) (int64, error) {
	if err := b.bounds(off, 8); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b[off:])), nil
}

// root follows the buffer's leading uoffset to the root table.
func (b fbBuf) root() (fbTable, error) {
	off, err := b.u32(0)
	if err != nil {
		return fbTable{}, err
	}
	return b.table(int(off))
}

// table reads the table header at pos: a signed offset back to its vtable.
func (b fbBuf) table(pos int) (fbTable, error) {
	soff, err := b.u32(pos)
	if err != nil {
		return fbTable{}, errf("table at %d: %v", pos, err)
	}
	vt := pos - int(int32(soff))
	if err := b.bounds(vt, 4); err != nil {
		return fbTable{}, errf("vtable of table at %d: %v", pos, err)
	}
	vtLen := int(binary.LittleEndian.Uint16(b[vt:]))
	if vtLen < 4 {
		return fbTable{}, errf("table at %d has a %d-byte vtable", pos, vtLen)
	}
	if err := b.bounds(vt, vtLen); err != nil {
		return fbTable{}, errf("vtable of table at %d: %v", pos, err)
	}
	return fbTable{buf: b, pos: pos, vt: vt, vtLen: vtLen}, nil
}

type fbTable struct {
	buf   fbBuf
	pos   int
	vt    int
	vtLen int
}

// slot returns the field's offset from the table position, or 0 for an absent
// field. FlatBuffers encodes absence as a zero vtable entry or as a vtable too
// short to hold the id. The vtable bounds were checked in table(), so this
// cannot fail.
func (t fbTable) slot(id int) int {
	off := 4 + id*2
	if off+2 > t.vtLen {
		return 0
	}
	return int(binary.LittleEndian.Uint16(t.buf[t.vt+off:]))
}

func (t fbTable) has(id int) bool { return t.slot(id) != 0 }

func (t fbTable) u8(id int, def uint8) (uint8, error) {
	slot := t.slot(id)
	if slot == 0 {
		return def, nil
	}
	return t.buf.u8(t.pos + slot)
}

func (t fbTable) i64(id int, def int64) (int64, error) {
	slot := t.slot(id)
	if slot == 0 {
		return def, nil
	}
	return t.buf.i64(t.pos + slot)
}

// child resolves a uoffset field to the table it points at.
func (t fbTable) child(id int) (fbTable, bool, error) {
	slot := t.slot(id)
	if slot == 0 {
		return fbTable{}, false, nil
	}
	at := t.pos + slot
	off, err := t.buf.u32(at)
	if err != nil {
		return fbTable{}, false, err
	}
	child, err := t.buf.table(at + int(off))
	if err != nil {
		return fbTable{}, false, err
	}
	return child, true, nil
}

func (t fbTable) str(id int) (string, bool, error) {
	slot := t.slot(id)
	if slot == 0 {
		return "", false, nil
	}
	at := t.pos + slot
	off, err := t.buf.u32(at)
	if err != nil {
		return "", false, err
	}
	head := at + int(off)
	n, err := t.buf.u32(head)
	if err != nil {
		return "", false, err
	}
	if err := t.buf.bounds(head+4, int(n)); err != nil {
		return "", false, err
	}
	return string(t.buf[head+4 : head+4+int(n)]), true, nil
}

func (t fbTable) vector(id int) (fbVector, bool, error) {
	slot := t.slot(id)
	if slot == 0 {
		return fbVector{}, false, nil
	}
	at := t.pos + slot
	off, err := t.buf.u32(at)
	if err != nil {
		return fbVector{}, false, err
	}
	head := at + int(off)
	n, err := t.buf.u32(head)
	if err != nil {
		return fbVector{}, false, err
	}
	count, err := asLength(int64(n), "vector length")
	if err != nil {
		return fbVector{}, false, err
	}
	// Every vector this codec reads has elements of at least four bytes: a
	// uoffset for fields and custom_metadata, sixteen for FieldNode and Buffer.
	// Refusing a count the remaining metadata could not hold is what keeps a
	// corrupt four-byte length out of `make`, where a billion elements is a
	// fatal out-of-memory rather than a returned error.
	if follow := len(t.buf) - (head + 4); count > follow/4 {
		return fbVector{}, false, errf("vector at %d declares %d elements, %d bytes of metadata follow it", head, count, follow)
	}
	return fbVector{buf: t.buf, start: head + 4, count: count}, true, nil
}

type fbVector struct {
	buf   fbBuf
	start int
	count int
}

// table resolves element i of a vector of tables.
func (v fbVector) table(i int) (fbTable, error) {
	at := v.start + i*4
	off, err := v.buf.u32(at)
	if err != nil {
		return fbTable{}, err
	}
	return v.buf.table(at + int(off))
}

// inline returns the position of inline struct element i.
func (v fbVector) inline(i, size int) (int, error) {
	at := v.start + i*size
	if i < 0 || i >= v.count {
		return 0, errf("element %d is out of range for a %d-element vector", i, v.count)
	}
	if err := v.buf.bounds(at, size); err != nil {
		return 0, err
	}
	return at, nil
}

// buffer reads inline Buffer{i64 offset, i64 length} element i and resolves it
// against the message body. Buffer.offset is body-relative.
func (v fbVector) buffer(i, body, bodyLen int) (span, error) {
	at, err := v.inline(i, bufferSize)
	if err != nil {
		return span{}, err
	}
	off, err := v.buf.i64(at)
	if err != nil {
		return span{}, err
	}
	length, err := v.buf.i64(at + 8)
	if err != nil {
		return span{}, err
	}
	if off < 0 || length < 0 || off > int64(bodyLen) || length > int64(bodyLen)-off {
		return span{}, errf("buffer %d spans [%d,%d) of a %d-byte body", i, off, off+length, bodyLen)
	}
	return span{off: body + int(off), len: int(length)}, nil
}
