// Encoding side of the SACI wire format: a [Writer] that produces the segment
// stream [Parse] reads, and an [EncodeSchema] for the schema-only stream a
// processor reports in `component-descriptor.arrow-schema-ipc`.
//
// The package doc comment explains what the in-place mutation path cannot do: a
// processor that must write a Utf8 column, drop a row, or emit a component the
// host did not send has to encode a RecordBatch flatbuffer. This file is that
// encoder. It shares the reader's field id constants and its hand-rolled
// approach, so the whole codec is still standard library only.
//
// Refusals come back as an [Error] and only from the Write calls. [Writer.Bytes]
// cannot fail, because every segment is encoded and validated when it is
// written.

package arrowipc

import (
	"encoding/binary"
	"math"
	"strconv"
)

// Values the reader never has to know, because the host always writes them the
// same way and the reader takes the flatbuffer defaults for the rest.
const (
	// metadataVersionV5 is `MetadataVersion::V5`. `IpcWriteOptions::default()`
	// on the host fixes it, and arrow-rs refuses to read a V4 stream that
	// declares V5 buffer layout, so this is written explicitly rather than
	// left to the field's default of V1.
	metadataVersionV5 int16 = 4

	// int64BitWidth and precisionDouble are the only `Int` and
	// `FloatingPoint` shapes the wire format carries.
	int64BitWidth   int32 = 64
	precisionDouble int16 = 2

	// versionKey holds a component's decimal u32 schema version. The host
	// writes it on every component segment and reads it back to decide whether
	// a processor's registration still matches.
	versionKey = "__saci_schema_version"

	// aliveComponent is the trailing liveness segment, whose one Boolean
	// column bounds every other component's row count. The host rejects a
	// stream without it.
	aliveComponent = "__alive"
	aliveField     = "alive"
)

// FlatBuffers field ids the writer needs on top of the reader's set. A union
// occupies two slots, so `Field.type` sits at 3, one past `type_type`.
const (
	msgVersionID = 0

	fieldTypeID = 3

	intBitWidthID = 0
	intSignedID   = 1

	floatPrecisionID = 0
)

// Vtable lengths, which are also the smallest each table can be: FlatBuffers
// stores a vtable long enough to reach the highest id actually written, and a
// reader treats an id at or past the vtable's end as absent. Writing exactly
// what the wire format uses keeps the metadata small without a trimming pass.
const (
	msgSlots    = 4 // version, header_type, header, bodyLength
	schemaSlots = 3 // endianness, fields, custom_metadata
	fieldSlots  = 4 // name, nullable, type_type, type
	batchSlots  = 3 // length, nodes, buffers
	kvSlots     = 2 // key, value
	intSlots    = 2 // bitWidth, is_signed
	floatSlots  = 1 // precision
)

// ---------------------------------------------------------------------------
// Columns.
// ---------------------------------------------------------------------------

// ColumnType is a column's Arrow `Field.type_type`, narrowed to the four types
// the SACI wire format carries.
//
// The names are the concrete Arrow types rather than the union arm names. The
// arms are `Int` and `FloatingPoint` at any bit width; this codec writes the
// 64-bit signed and double-precision variants and nothing else, so naming the
// arm would promise a generality the encoder does not have.
type ColumnType uint8

const (
	TypeInt64   = ColumnType(typeInt)
	TypeFloat64 = ColumnType(typeFloat)
	TypeUtf8    = ColumnType(typeUtf8)
	TypeBool    = ColumnType(typeBool)
)

// String reports the Arrow union arm name, which is what the reader's error
// messages use.
func (t ColumnType) String() string { return typeName(uint8(t)) }

// SchemaField is one field of a component schema.
//
// The declaration order is the schema. It fixes the fields vector, the field
// node order, the buffer walk and the schema fingerprint, so two field lists
// that differ only in order describe two different components.
type SchemaField struct {
	Name string
	Type ColumnType
}

// Column is one column of a batch: a [SchemaField] plus its values.
//
// The interface is closed by an unexported method. A fifth Arrow type is a
// change to the wire format, to all five language codecs and to the host, never
// something a caller adds from outside this package.
type Column interface {
	// Field is the schema entry this column contributes.
	Field() SchemaField
	// Rows is the value count.
	Rows() int

	// encode appends this column's buffer slots to a record batch body, in
	// schema slot order.
	encode(b *bodyBuilder)
}

// Int64Column is a non-nullable Int64 column.
type Int64Column struct {
	Name   string
	Values []int64
}

func (c Int64Column) Field() SchemaField { return SchemaField{Name: c.Name, Type: TypeInt64} }
func (c Int64Column) Rows() int          { return len(c.Values) }

func (c Int64Column) encode(b *bodyBuilder) {
	b.validity(len(c.Values))
	dst := b.buf[b.reserve(len(c.Values)*8):]
	for i, v := range c.Values {
		binary.LittleEndian.PutUint64(dst[i*8:], uint64(v))
	}
}

// Float64Column is a non-nullable Float64 column.
type Float64Column struct {
	Name   string
	Values []float64
}

func (c Float64Column) Field() SchemaField { return SchemaField{Name: c.Name, Type: TypeFloat64} }
func (c Float64Column) Rows() int          { return len(c.Values) }

func (c Float64Column) encode(b *bodyBuilder) {
	b.validity(len(c.Values))
	dst := b.buf[b.reserve(len(c.Values)*8):]
	for i, v := range c.Values {
		binary.LittleEndian.PutUint64(dst[i*8:], math.Float64bits(v))
	}
}

// BoolColumn is a non-nullable Boolean column, bit-packed LSB first.
type BoolColumn struct {
	Name   string
	Values []bool
}

func (c BoolColumn) Field() SchemaField { return SchemaField{Name: c.Name, Type: TypeBool} }
func (c BoolColumn) Rows() int          { return len(c.Values) }

func (c BoolColumn) encode(b *bodyBuilder) {
	b.validity(len(c.Values))
	dst := b.buf[b.reserve(bitmapBytes(len(c.Values))):]
	for i, v := range c.Values {
		if v {
			dst[i/8] |= 1 << (uint(i) & 7)
		}
	}
}

// Utf8Column is a non-nullable Utf8 column.
type Utf8Column struct {
	Name   string
	Values []string
}

func (c Utf8Column) Field() SchemaField { return SchemaField{Name: c.Name, Type: TypeUtf8} }
func (c Utf8Column) Rows() int          { return len(c.Values) }

func (c Utf8Column) encode(b *bodyBuilder) {
	rows := len(c.Values)
	b.validity(rows)
	// Both spans are claimed before either is written: a reserve can move the
	// body, so a slice taken across one would be a write into the old array.
	offsets := b.reserve((rows + 1) * 4)
	values := b.reserve(c.bytes())
	at := 0
	for i, s := range c.Values {
		binary.LittleEndian.PutUint32(b.buf[offsets+i*4:], uint32(at))
		at += copy(b.buf[values+at:], s)
	}
	binary.LittleEndian.PutUint32(b.buf[offsets+rows*4:], uint32(at))
}

// bytes is the values buffer size, which is also the last offset.
func (c Utf8Column) bytes() int {
	total := 0
	for _, s := range c.Values {
		total += len(s)
	}
	return total
}

func bitmapBytes(rows int) int { return (rows + 7) / 8 }

// ---------------------------------------------------------------------------
// Writer.
// ---------------------------------------------------------------------------

// Writer accumulates the segments of one wire-format stream.
//
// The name is not `Stream`, which the wire grammar would suggest, because
// [Stream] is the reader and one package cannot declare that name twice.
//
// Segments come out in the order they were first written, with the `__alive`
// segment last, which is the layout the host's own producer emits. Writing a
// component name twice replaces that segment in place rather than appending a
// second one: a processor that re-encodes a shrinking batch, or writes the same
// component from two systems, must not hand the host two segments claiming the
// same name.
type Writer struct {
	segments []writeSegment

	// alive holds the encoded `__alive` segment, either built by [Writer.WriteAlive]
	// or copied verbatim from an input stream. aliveRows is its bit length, or
	// -1 while it is unset.
	alive     []byte
	aliveRows int
}

// writeSegment is one encoded segment, ready but for its length prefix.
type writeSegment struct {
	name string
	rows int
	ipc  []byte
}

// NewWriter returns an empty [Writer].
func NewWriter() *Writer { return &Writer{aliveRows: -1} }

// WriteComponent encodes one component segment: a Schema message labelled with
// the component name and version, and a RecordBatch message holding the columns.
//
// Every column is non-nullable and must hold the same number of values, because
// a RecordBatch has one row count and this codec writes no null bitmaps beyond
// the all-ones one Arrow requires in the validity slot.
func (w *Writer) WriteComponent(name string, version uint32, columns ...Column) error {
	if name == "" {
		return errf("component name is empty")
	}
	if name == aliveComponent {
		return errf("component %q is the liveness segment: write it with WriteAlive", aliveComponent)
	}
	if len(columns) == 0 {
		return errf("component %q declares no columns", name)
	}

	rows := columns[0].Rows()
	fields := make([]SchemaField, len(columns))
	for i, c := range columns {
		if c.Rows() != rows {
			return errf("component %q column %q holds %d rows, column %q holds %d", name, columns[0].Field().Name, rows, c.Field().Name, c.Rows())
		}
		fields[i] = c.Field()
		if s, ok := c.(Utf8Column); ok && s.bytes() > math.MaxInt32 {
			return errf("component %q column %q holds %d bytes, which an i32 offset cannot address", name, s.Name, s.bytes())
		}
	}
	if err := checkFields("component "+strconv.Quote(name), fields); err != nil {
		return err
	}
	if err := w.checkRows(name, rows); err != nil {
		return err
	}

	meta := []keyValue{
		{key: componentKey, value: name},
		{key: versionKey, value: strconv.FormatUint(uint64(version), 10)},
	}
	w.put(writeSegment{name: name, rows: rows, ipc: encodeSegment(fields, meta, columns, rows)})
	return nil
}

// WriteAlive encodes the trailing `__alive` segment.
//
// The bitmap is the stream's row bound: the host refuses a component holding
// more rows than there are bits here, so this is the one segment whose length
// constrains the others.
func (w *Writer) WriteAlive(bits []bool) error {
	if len(bits) > math.MaxInt32 {
		return errf("alive bitmap holds %d bits, which is not a usable row count", len(bits))
	}
	for _, seg := range w.segments {
		if seg.rows > len(bits) {
			return errf("component %q holds %d rows, more than the %d bits of the alive bitmap", seg.name, seg.rows, len(bits))
		}
	}

	fields := []SchemaField{{Name: aliveField, Type: TypeBool}}
	meta := []keyValue{{key: componentKey, value: aliveComponent}}
	columns := []Column{BoolColumn{Name: aliveField, Values: bits}}
	w.alive = encodeSegment(fields, meta, columns, len(bits))
	w.aliveRows = len(bits)
	return nil
}

// CopySegment appends an input segment verbatim.
//
// This is how a processor forwards what it does not touch. Re-encoding a
// component from decoded values would drop any Arrow type this codec cannot
// write, and re-deriving the alive bitmap would resurrect the rows the host
// marked dead, so the bytes pass through instead.
//
// The segment aliases the [Stream] it came from until [Writer.Bytes] copies it
// out.
func (w *Writer) CopySegment(seg RawSegment) error {
	if seg.Component == "" {
		return errf("copied segment declares no component")
	}
	if len(seg.IPC) == 0 {
		return errf("copied segment %q is empty", seg.Component)
	}
	if seg.Component == aliveComponent {
		for _, s := range w.segments {
			if s.rows > seg.Rows {
				return errf("component %q holds %d rows, more than the %d bits of the alive bitmap", s.name, s.rows, seg.Rows)
			}
		}
		w.alive = seg.IPC
		w.aliveRows = seg.Rows
		return nil
	}
	if err := w.checkRows(seg.Component, seg.Rows); err != nil {
		return err
	}
	w.put(writeSegment{name: seg.Component, rows: seg.Rows, ipc: seg.IPC})
	return nil
}

// Bytes assembles the stream: every component segment, the `__alive` segment,
// then the terminator, each segment behind its own u32 length prefix.
//
// A stream with no alive segment gets an all-alive bitmap sized to its longest
// component, because that is what a processor building a stream from scratch
// means and the host rejects a stream without one.
func (w *Writer) Bytes() []byte {
	alive := w.alive
	if alive == nil {
		rows := 0
		for _, seg := range w.segments {
			rows = max(rows, seg.rows)
		}
		bits := make([]bool, rows)
		for i := range bits {
			bits[i] = true
		}
		fields := []SchemaField{{Name: aliveField, Type: TypeBool}}
		meta := []keyValue{{key: componentKey, value: aliveComponent}}
		alive = encodeSegment(fields, meta, []Column{BoolColumn{Name: aliveField, Values: bits}}, rows)
	}

	total := 4 + 4 + len(alive)
	for _, seg := range w.segments {
		total += 4 + len(seg.ipc)
	}

	out := make([]byte, 0, total)
	for _, seg := range w.segments {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(seg.ipc)))
		out = append(out, seg.ipc...)
	}
	out = binary.LittleEndian.AppendUint32(out, uint32(len(alive)))
	out = append(out, alive...)
	return binary.LittleEndian.AppendUint32(out, 0)
}

// put replaces the segment of the same name, or appends a new one.
func (w *Writer) put(seg writeSegment) {
	for i := range w.segments {
		if w.segments[i].name == seg.name {
			w.segments[i] = seg
			return
		}
	}
	w.segments = append(w.segments, seg)
}

// checkRows refuses a component the alive bitmap cannot cover.
func (w *Writer) checkRows(name string, rows int) error {
	if rows > math.MaxInt32 {
		return errf("component %q holds %d rows, which an i32 offset cannot address", name, rows)
	}
	if w.aliveRows >= 0 && rows > w.aliveRows {
		return errf("component %q holds %d rows, more than the %d bits of the alive bitmap", name, rows, w.aliveRows)
	}
	return nil
}

// checkFields refuses a schema a reader could not address unambiguously.
//
// `subject` names what is being described, because the same rules guard a wire
// segment's component and a descriptor's bare schema.
func checkFields(subject string, fields []SchemaField) error {
	for i, f := range fields {
		if f.Name == "" {
			return errf("%s field %d is unnamed", subject, i)
		}
		switch f.Type {
		case TypeInt64, TypeFloat64, TypeUtf8, TypeBool:
		default:
			return errf("%s field %q has type_type %d, which this codec cannot write", subject, f.Name, uint8(f.Type))
		}
		for _, prior := range fields[:i] {
			if prior.Name == f.Name {
				return errf("%s declares field %q twice", subject, f.Name)
			}
		}
	}
	return nil
}

// EncodeSchema returns a schema-only Arrow IPC stream: one Schema message and
// the end-of-stream marker, with no batches.
//
// This is what `component-descriptor.arrow-schema-ipc` holds, and it is not a
// segment. It carries no length prefix, no terminator and no custom_metadata:
// the host parses it with `StreamReader::schema()` to build its template
// dataset, and the `__saci_component` label that every wire segment needs would
// travel into that template as a stray metadata key.
func EncodeSchema(fields []SchemaField) ([]byte, error) {
	if len(fields) == 0 {
		return nil, errf("schema declares no fields")
	}
	if err := checkFields("schema", fields); err != nil {
		return nil, err
	}
	msg := encodeSchemaMessage(fields, nil)
	out := make([]byte, 0, len(msg)+8)
	out = append(out, msg...)
	return appendEndOfStream(out), nil
}

// ---------------------------------------------------------------------------
// Segment and message framing.
// ---------------------------------------------------------------------------

// keyValue is one Schema custom_metadata entry.
type keyValue struct {
	key   string
	value string
}

// encodeSegment builds one standalone Arrow IPC stream: a Schema message, a
// RecordBatch message, then the end-of-stream marker.
//
// Both messages are framed with a metadata length already padded to eight
// bytes, so the record batch body starts eight-byte aligned relative to the
// segment and every buffer offset inside it stays aligned too. arrow-rs on the
// host is the real consumer and realigns a body it has to, at the cost of a
// copy of the whole batch.
func encodeSegment(fields []SchemaField, meta []keyValue, columns []Column, rows int) []byte {
	body := bodyBuilder{}
	for _, c := range columns {
		c.encode(&body)
	}

	schema := encodeSchemaMessage(fields, meta)
	batch := encodeBatchMessage(rows, len(fields), len(body.buf), body.buffers)

	out := make([]byte, 0, len(schema)+len(batch)+len(body.buf)+8)
	out = append(out, schema...)
	out = append(out, batch...)
	out = append(out, body.buf...)
	return appendEndOfStream(out)
}

// appendEndOfStream writes the continuation marker followed by a zero metadata
// length, which is how an Arrow IPC stream ends.
func appendEndOfStream(out []byte) []byte {
	out = binary.LittleEndian.AppendUint32(out, continuation)
	return binary.LittleEndian.AppendUint32(out, 0)
}

// encodeSchemaMessage frames a Schema message. Its body is empty, so the frame
// is the whole message.
func encodeSchemaMessage(fields []SchemaField, meta []keyValue) []byte {
	w := newFBWriter(128 + 96*len(fields) + 64*len(meta))
	w.finish(w.messageTable(headerSchema, w.schemaTable(fields, meta), 0))
	return frame(w.bytes())
}

// encodeBatchMessage frames a RecordBatch message. The body it describes is
// appended by the caller, which is what already built it.
func encodeBatchMessage(rows, nodeCount, bodyLen int, buffers []bufferSpan) []byte {
	w := newFBWriter(128 + fieldNodeSize*nodeCount + bufferSize*len(buffers))
	w.finish(w.messageTable(headerRecordBatch, w.recordBatchTable(rows, nodeCount, buffers), int64(bodyLen)))
	return frame(w.bytes())
}

// frame prefixes a metadata flatbuffer with the continuation marker and its own
// length, which is already a multiple of eight.
func frame(fb []byte) []byte {
	out := make([]byte, 0, 8+len(fb))
	out = binary.LittleEndian.AppendUint32(out, continuation)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(fb)))
	return append(out, fb...)
}

// ---------------------------------------------------------------------------
// Record batch body.
// ---------------------------------------------------------------------------

// bufferSpan is one Arrow buffer's body-relative offset and its exact length,
// padding excluded, which is what the RecordBatch `Buffer` struct records.
type bufferSpan struct {
	off int
	len int
}

// bodyBuilder lays out one record batch body.
//
// Every buffer starts on an eight-byte boundary and is padded up to one, so the
// body's total length is a multiple of eight and equals the `bodyLength` the
// Message declares.
type bodyBuilder struct {
	buf     []byte
	buffers []bufferSpan
}

// reserve claims the next buffer slot, records its span and returns its offset
// into the body.
//
// It hands back an offset rather than a slice because it can move the body: a
// slice taken before another reserve would alias the array the growth left
// behind, and every write through it would be lost.
func (b *bodyBuilder) reserve(n int) int {
	off := len(b.buf)
	b.buffers = append(b.buffers, bufferSpan{off: off, len: n})

	end := off + align8(n)
	if end > cap(b.buf) {
		grown := make([]byte, off, max(end, 2*cap(b.buf)))
		copy(grown, b.buf)
		b.buf = grown
	}
	b.buf = b.buf[:end]
	clear(b.buf[off:end])
	return off
}

// validity fills the all-ones bitmap Arrow expects in every field's first
// buffer slot.
//
// arrow-rs writes this buffer for a non-nullable column with no nulls rather
// than an empty one, and sets every bit of every byte including the ones past
// the last row. A reader that trusts the null count never looks, and one that
// does not now finds every row marked present.
func (b *bodyBuilder) validity(rows int) {
	n := bitmapBytes(rows)
	off := b.reserve(n)
	dst := b.buf[off : off+n]
	for i := range dst {
		dst[i] = 0xFF
	}
}

// ---------------------------------------------------------------------------
// Raw segments: pass-through of what a processor does not touch.
// ---------------------------------------------------------------------------

// RawSegment is one segment of a parsed stream, addressable without decoding
// its columns.
type RawSegment struct {
	// Component is the name the segment's Schema custom_metadata declares.
	Component string
	// Rows is its RecordBatch row count.
	Rows int
	// IPC is the segment's Arrow IPC stream, length prefix excluded. It aliases
	// the [Stream] buffer it was read from.
	IPC []byte
}

// RawSegments lists every segment of the stream, in stream order.
//
// A processor re-encoding one component hands the rest of these to
// [Writer.CopySegment] unchanged, which is both cheaper than a decode and
// re-encode and the only way to preserve a column whose Arrow type this codec
// does not write.
func (s *Stream) RawSegments() ([]RawSegment, error) {
	out := make([]RawSegment, len(s.segments))
	for i := range s.segments {
		sc, err := s.schemaOf(i)
		if err != nil {
			return nil, err
		}
		rows, err := s.segmentRows(s.segments[i], sc)
		if err != nil {
			return nil, errf("segment %d (%s): %v", i, sc.component, err)
		}
		out[i] = RawSegment{
			Component: sc.component,
			Rows:      rows,
			IPC:       s.Buf[s.segments[i].start:s.segments[i].end],
		}
	}
	return out, nil
}

// segmentRows reads a segment's row count out of its RecordBatch header without
// resolving a single buffer, which is all a pass-through caller needs.
func (s *Stream) segmentRows(seg segment, sc segmentSchema) (int, error) {
	rb, err := s.message(sc.msg.next, seg.end)
	if err != nil {
		return 0, err
	}
	if !rb.present {
		return 0, errf("segment ends after its schema, with no record batch")
	}
	if rb.headerType != headerRecordBatch {
		return 0, errf("second message has header_type %d, want %d (RecordBatch)", rb.headerType, headerRecordBatch)
	}
	header, ok, err := rb.root.child(msgHeader)
	if err != nil {
		return 0, errf("record batch header: %v", err)
	}
	if !ok {
		return 0, errf("record batch message carries no header")
	}
	raw, err := header.i64(batchLengthID, 0)
	if err != nil {
		return 0, errf("record batch length: %v", err)
	}
	return asRowCount(raw)
}

// ---------------------------------------------------------------------------
// FlatBuffers writer: just enough for Arrow's Message, Schema, Field,
// RecordBatch and KeyValue tables.
//
// FlatBuffers builds back to front. Content occupies buf[head:], offset() is
// the distance from the buffer's end, and every uoffset is stored relative to
// its own position, so a table can only be written after everything it points
// at. It does neither vtable deduplication nor trailing-slot trimming: both buy
// bytes in a metadata message that is a few hundred bytes long, and both cost a
// pass a reader cannot tell apart from their absence.
// ---------------------------------------------------------------------------

type fbWriter struct {
	buf  []byte
	head int

	// minalign is the widest field written so far. It starts at eight rather
	// than one so that the finished buffer's length is a multiple of eight,
	// which is what lets it be framed as a padded metadata block and still keep
	// its i64 fields aligned.
	minalign int

	// vtable holds the offsets of the fields written for the table under
	// construction, indexed by field id. Zero is an absent field.
	vtable []int
	objEnd int
}

func newFBWriter(size int) *fbWriter {
	return &fbWriter{buf: make([]byte, size), head: size, minalign: 8}
}

// offset is the number of bytes written, which is also every object's address.
func (w *fbWriter) offset() int { return len(w.buf) - w.head }

func (w *fbWriter) bytes() []byte { return w.buf[w.head:] }

// grow doubles the buffer, keeping the content at its end.
func (w *fbWriter) grow() {
	used := w.offset()
	size := max(2*len(w.buf), 64)
	grown := make([]byte, size)
	copy(grown[size-used:], w.buf[w.head:])
	w.buf = grown
	w.head = size - used
}

// prep pads so that a field of the given size, written after `additional`
// bytes, lands on its own alignment, and grows the buffer to hold all of it.
func (w *fbWriter) prep(size, additional int) {
	if size > w.minalign {
		w.minalign = size
	}
	pad := -(w.offset() + additional) & (size - 1)
	for w.head <= pad+size+additional {
		w.grow()
	}
	for range pad {
		w.head--
		w.buf[w.head] = 0
	}
}

func (w *fbWriter) placeU8(v uint8) {
	w.head--
	w.buf[w.head] = v
}

func (w *fbWriter) placeU16(v uint16) {
	w.head -= 2
	binary.LittleEndian.PutUint16(w.buf[w.head:], v)
}

func (w *fbWriter) placeU32(v uint32) {
	w.head -= 4
	binary.LittleEndian.PutUint32(w.buf[w.head:], v)
}

func (w *fbWriter) placeU64(v uint64) {
	w.head -= 8
	binary.LittleEndian.PutUint64(w.buf[w.head:], v)
}

// prependOffset writes a uoffset to an object already in the buffer.
func (w *fbWriter) prependOffset(off int) {
	w.prep(4, 0)
	w.placeU32(uint32(w.offset() + 4 - off))
}

// startObject begins a table with room for `slots` field ids.
func (w *fbWriter) startObject(slots int) {
	w.vtable = make([]int, slots)
	w.objEnd = w.offset()
}

// slot records that the field just written belongs to this id.
func (w *fbWriter) slot(id int) { w.vtable[id] = w.offset() }

func (w *fbWriter) slotU8(id int, v, def uint8) {
	if v == def {
		return
	}
	w.prep(1, 0)
	w.placeU8(v)
	w.slot(id)
}

func (w *fbWriter) slotBool(id int, v, def bool) {
	if v == def {
		return
	}
	w.prep(1, 0)
	w.placeU8(1)
	w.slot(id)
}

func (w *fbWriter) slotI16(id int, v, def int16) {
	if v == def {
		return
	}
	w.prep(2, 0)
	w.placeU16(uint16(v))
	w.slot(id)
}

func (w *fbWriter) slotI32(id int, v, def int32) {
	if v == def {
		return
	}
	w.prep(4, 0)
	w.placeU32(uint32(v))
	w.slot(id)
}

// slotI64 writes an i64 field whatever its value.
//
// `Message.bodyLength` and `RecordBatch.length` both default to zero, and both
// are what a reader dispatches on, so an empty batch reads better as an
// explicit zero than as an absent field a reader has to know the default of.
func (w *fbWriter) slotI64(id int, v int64) {
	w.prep(8, 0)
	w.placeU64(uint64(v))
	w.slot(id)
}

func (w *fbWriter) slotOffset(id, off int) {
	if off == 0 {
		return
	}
	w.prependOffset(off)
	w.slot(id)
}

// endObject writes the table's vtable and returns the table's offset.
func (w *fbWriter) endObject() int {
	// The soffset back to the vtable is patched in once the vtable's position
	// is known, so its space is claimed first.
	w.prep(4, 0)
	w.placeU32(0)
	object := w.offset()

	for i := len(w.vtable) - 1; i >= 0; i-- {
		off := 0
		if w.vtable[i] != 0 {
			off = object - w.vtable[i]
		}
		w.prep(2, 0)
		w.placeU16(uint16(off))
	}
	w.prep(2, 0)
	w.placeU16(uint16(object - w.objEnd))
	w.prep(2, 0)
	w.placeU16(uint16((len(w.vtable) + 2) * 2))

	binary.LittleEndian.PutUint32(w.buf[len(w.buf)-object:], uint32(int32(w.offset()-object)))
	return object
}

// startVector claims space for a vector's elements and its length prefix.
func (w *fbWriter) startVector(elemSize, n, align int) {
	w.prep(4, elemSize*n)
	w.prep(align, elemSize*n)
}

// endVector writes the element count that heads a vector.
func (w *fbWriter) endVector(n int) int {
	w.prep(4, 0)
	w.placeU32(uint32(n))
	return w.offset()
}

// createString writes a null-terminated FlatBuffers string.
func (w *fbWriter) createString(s string) int {
	w.prep(4, len(s)+1)
	w.placeU8(0)
	w.head -= len(s)
	copy(w.buf[w.head:], s)
	return w.endVector(len(s))
}

// offsetVector writes a vector of uoffsets to tables already in the buffer.
func (w *fbWriter) offsetVector(offsets []int) int {
	w.startVector(4, len(offsets), 4)
	for i := len(offsets) - 1; i >= 0; i-- {
		w.prependOffset(offsets[i])
	}
	return w.endVector(len(offsets))
}

// finish writes the buffer's root uoffset, after which [fbWriter.bytes] is the
// complete flatbuffer and its length is a multiple of eight.
func (w *fbWriter) finish(root int) {
	w.prep(w.minalign, 4)
	w.prependOffset(root)
}

// ---------------------------------------------------------------------------
// Arrow tables.
// ---------------------------------------------------------------------------

// messageTable writes the Message envelope every IPC message opens with.
func (w *fbWriter) messageTable(headerType uint8, header int, bodyLen int64) int {
	w.startObject(msgSlots)
	w.slotI16(msgVersionID, metadataVersionV5, 0)
	w.slotU8(msgHeaderType, headerType, 0)
	w.slotOffset(msgHeader, header)
	w.slotI64(msgBodyLength, bodyLen)
	return w.endObject()
}

// schemaTable writes a Schema and everything it points at.
func (w *fbWriter) schemaTable(fields []SchemaField, meta []keyValue) int {
	fieldOffsets := make([]int, len(fields))
	for i, f := range fields {
		fieldOffsets[i] = w.fieldTable(f)
	}
	metaOffsets := make([]int, len(meta))
	for i, kv := range meta {
		metaOffsets[i] = w.keyValueTable(kv)
	}

	fieldsVector := w.offsetVector(fieldOffsets)
	metaVector := 0
	if len(meta) > 0 {
		metaVector = w.offsetVector(metaOffsets)
	}

	w.startObject(schemaSlots)
	w.slotOffset(schemaFieldsID, fieldsVector)
	w.slotOffset(schemaMetadataID, metaVector)
	return w.endObject()
}

// fieldTable writes one Field. `nullable` is left at its default of false: the
// wire format's columns are non-nullable by convention, and the reader never
// looks at the flag.
func (w *fbWriter) fieldTable(f SchemaField) int {
	name := w.createString(f.Name)
	typ := w.typeTable(f.Type)

	w.startObject(fieldSlots)
	w.slotOffset(fieldNameID, name)
	w.slotU8(fieldTypeTypeID, uint8(f.Type), 0)
	w.slotOffset(fieldTypeID, typ)
	return w.endObject()
}

// typeTable writes the union payload the field's type_type selects.
func (w *fbWriter) typeTable(t ColumnType) int {
	switch t {
	case TypeInt64:
		w.startObject(intSlots)
		w.slotI32(intBitWidthID, int64BitWidth, 0)
		w.slotBool(intSignedID, true, false)
		return w.endObject()
	case TypeFloat64:
		w.startObject(floatSlots)
		w.slotI16(floatPrecisionID, precisionDouble, 0)
		return w.endObject()
	default:
		// Utf8 and Bool carry no parameters, so their tables are empty. They
		// are still written: the union payload is what makes type_type
		// meaningful, and arrow-rs rejects a field without one.
		w.startObject(0)
		return w.endObject()
	}
}

func (w *fbWriter) keyValueTable(kv keyValue) int {
	key := w.createString(kv.key)
	value := w.createString(kv.value)

	w.startObject(kvSlots)
	w.slotOffset(kvKeyID, key)
	w.slotOffset(kvValueID, value)
	return w.endObject()
}

// recordBatchTable writes a RecordBatch header. `compression` is left absent,
// which is what tells a reader the body is raw.
func (w *fbWriter) recordBatchTable(rows, nodeCount int, buffers []bufferSpan) int {
	nodes := w.fieldNodeVector(nodeCount, rows)
	bufferVector := w.bufferVector(buffers)

	w.startObject(batchSlots)
	w.slotI64(batchLengthID, int64(rows))
	w.slotOffset(batchNodesID, nodes)
	w.slotOffset(batchBuffersID, bufferVector)
	return w.endObject()
}

// fieldNodeVector writes one FieldNode per field. Every node reports the same
// row count and a null count of zero, because every column this codec writes is
// non-nullable and fully populated.
func (w *fbWriter) fieldNodeVector(n, rows int) int {
	w.startVector(fieldNodeSize, n, 8)
	for range n {
		w.prep(8, fieldNodeSize)
		w.placeU64(0)
		w.placeU64(uint64(rows))
	}
	return w.endVector(n)
}

// bufferVector writes the body-relative span of every buffer slot, in the order
// the schema walk assigned them.
func (w *fbWriter) bufferVector(buffers []bufferSpan) int {
	w.startVector(bufferSize, len(buffers), 8)
	for i := len(buffers) - 1; i >= 0; i-- {
		w.prep(8, bufferSize)
		w.placeU64(uint64(buffers[i].len))
		w.placeU64(uint64(buffers[i].off))
	}
	return w.endVector(len(buffers))
}
