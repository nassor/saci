// The TypeScript side of the SACI host to processor wire format.
//
// Two halves, and most processors need only the first. A stage that rewrites
// fixed-width cells locates its component's segment, reads the columns it
// needs, overwrites value bytes in place and hands the same array back: both
// flatbuffers, the `__alive` segment and the stream framing pass through
// untouched, so [`SaciStream`] is a flatbuffer *reader*. A stage that changes a
// row count, or writes a `Utf8` column, cannot do that — a different string
// length moves every buffer after it — and reaches for [`SaciStreamWriter`],
// which encodes whole segments, flatbuffers included. Neither half needs
// anything beyond the JavaScript standard library.
//
// The reader ignores the `__alive` bitmap: liveness is the host's bookkeeping,
// and a processor that can neither add nor remove rows cannot change it. The
// writer has to state it, because the host rejects a stream that carries no
// `__alive` segment.
//
// Framing, with `examples/polyglot/generated/fixture_input.saci` as the
// reference stream:
//
//   saci_stream := segment* terminator
//   segment    := u32le segment_len ++ arrow_ipc_stream[segment_len]
//   terminator := u32le 0x00000000
//   message    := u32le 0xffffffff ++ u32le metadata_len
//              ++ flatbuffer[metadata_len] ++ body[body_length]
//
// `metadata_len` already covers the flatbuffer's padding to 8 bytes; the body
// starts right after it and the next message starts at
// `body_start + align8(body_length)`.
//
// Every malformed-input path throws [`ArrowIpcError`] with a precise message.
// This is the codec's own API; the processor is what maps a throw onto the WIT
// `run-error::permanent` arm, because a processor must never trap.

/**
 * Every refusal this codec raises: malformed wire bytes, or a write it will not
 * perform.
 *
 * A dedicated subclass rather than a plain `Error` is what lets a caller tell a
 * rejected stream from a bug in the codec. A native `RangeError` escaping from
 * a bad declared length would otherwise be indistinguishable from a deliberate
 * refusal, and the two want opposite responses.
 */
export class ArrowIpcError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ArrowIpcError';
  }
}

/** Arrow IPC message prefix; also the first word of the end-of-stream marker. */
const CONTINUATION = 0xffffffff;

/** `Message.header_type` discriminants (Arrow `Message.fbs`). */
const HEADER_SCHEMA = 1;
const HEADER_DICTIONARY = 2;
const HEADER_RECORD_BATCH = 3;

/** `Field.type_type` union discriminants (Arrow `Schema.fbs`). */
const TYPE_INT = 2;
const TYPE_FLOAT = 3;
const TYPE_UTF8 = 5;
const TYPE_BOOL = 6;

/**
 * The Arrow types this codec understands: `Int`, `FloatingPoint`, `Utf8` and
 * `Bool`.
 *
 * Narrowing the byte read out of `Field.type_type` to this union is what lets
 * the two lookup tables below be total: a type the codec cannot describe is
 * rejected at parse time rather than surfacing as an `undefined` slot count
 * halfway through a buffer walk.
 */
export type ArrowType =
  | typeof TYPE_INT
  | typeof TYPE_FLOAT
  | typeof TYPE_UTF8
  | typeof TYPE_BOOL;

function isArrowType(value: number): value is ArrowType {
  return value === TYPE_INT || value === TYPE_FLOAT || value === TYPE_UTF8 || value === TYPE_BOOL;
}

// Buffer slots a field consumes in `RecordBatch.buffers`, walked in schema
// order. Fixed by the Arrow type, never inferred from nullability or from a
// buffer's length: arrow-rs emits the validity slot with `ceil(n/8)` bytes of
// set bits even when the field is non-nullable.
const BUFFER_SLOTS = {
  [TYPE_INT]: 2, // validity, values
  [TYPE_FLOAT]: 2, // validity, values
  [TYPE_BOOL]: 2, // validity, values
  [TYPE_UTF8]: 3, // validity, i32 offsets, values
} as const satisfies Record<ArrowType, number>;

/** Arrow type names, for error messages only. */
const TYPE_NAMES = {
  [TYPE_INT]: 'Int',
  [TYPE_FLOAT]: 'FloatingPoint',
  [TYPE_UTF8]: 'Utf8',
  [TYPE_BOOL]: 'Bool',
} as const satisfies Record<ArrowType, string>;

/** Schema custom-metadata key naming the component a segment carries. */
const COMPONENT_KEY = '__saci_component';

/** Bytes per row of a fixed-width 64-bit column. */
const WIDTH_64 = 8;

// Largest row count this codec accepts, shared by all five codecs. A `Utf8`
// column addresses its values buffer with i32 offsets, so a batch wider than
// this cannot be described by the wire format at all, whatever the reader's
// word size. Compared as a BigInt: `Number` loses precision above 2^53, so an
// i64-max row count narrows to something that no longer looks out of range.
const MAX_ROWS = 0x7fff_ffffn;

// `fatal` turns invalid UTF-8 into a throw instead of replacement characters:
// a mis-parsed offsets buffer should fail loudly, not yield mojibake.
const UTF8 = new TextDecoder('utf-8', { fatal: true });

/** One field of a component's schema, with the buffer run it owns. */
interface Field {
  readonly name: string;
  readonly typeType: ArrowType;
  /** Index of this field's first slot in `RecordBatch.buffers`. */
  firstBuffer: number;
}

/**
 * A byte range: absolute inside the stream for the reader, relative to its
 * message body for the writer.
 */
interface Span {
  readonly start: number;
  readonly length: number;
}

/** A decoded Arrow IPC message header and the position of the message after it. */
interface Message {
  readonly headerType: number;
  /** Absolute position of the header table. */
  readonly header: number;
  readonly bodyStart: number;
  readonly bodyLength: number;
  /** Absolute position the next message starts at. */
  readonly next: number;
}

/** One length-prefixed Arrow IPC stream inside the SACI stream. */
interface Segment {
  readonly start: number;
  readonly end: number;
  readonly component: string;
  readonly schema: Message;
  /** Parsed on first access by [`SaciStream.component`]. */
  batch?: Batch;
}

/** Round `n` up to the next multiple of 8 without 32-bit bitwise truncation. */
function align8(n: number): number {
  const rem = n % 8;
  return rem === 0 ? n : n + (8 - rem);
}

// ---------------------------------------------------------------------------
// FlatBuffers reader
//
// A buffer's root table sits at the uoffset32 stored in its first four bytes.
// A table at absolute position `t` starts with an soffset32; its vtable lives
// at `t - soffset`. The vtable is `u16 vtable_len`, `u16 table_len`, then one
// u16 per field id. A zero offset, or a field id past `vtable_len`, means the
// field is absent.
// ---------------------------------------------------------------------------

/** Absolute position of field `id`'s slot in `table`, or 0 when absent. */
function fbField(view: DataView, table: number, id: number): number {
  const vtable = table - view.getInt32(table, true);
  const vtableLen = view.getUint16(vtable, true);
  // A vtable shorter than its own two-u16 header is not a vtable: reading a
  // field slot out of it would index whatever bytes happen to follow.
  if (vtableLen < 4) {
    throw new ArrowIpcError(
      `arrow ipc: vtable at ${vtable} declares ${vtableLen} bytes, less than its 4-byte header`,
    );
  }
  const slot = 4 + id * 2;
  if (slot >= vtableLen) {
    return 0;
  }
  const offset = view.getUint16(vtable + slot, true);
  return offset === 0 ? 0 : table + offset;
}

/** Follow the uoffset stored at `slot` to the absolute position it names. */
function fbDeref(view: DataView, slot: number): number {
  return slot + view.getUint32(slot, true);
}

/** Read the string referenced by `slot`. */
function fbString(view: DataView, bytes: Uint8Array, slot: number): string {
  const at = fbDeref(view, slot);
  const len = view.getUint32(at, true);
  return UTF8.decode(bytes.subarray(at + 4, at + 4 + len));
}

/** `{ start, count }` of the vector referenced by `slot`; `start` skips the count. */
function fbVector(view: DataView, slot: number): { start: number; count: number } {
  const at = fbDeref(view, slot);
  return { start: at + 4, count: view.getUint32(at, true) };
}

// ---------------------------------------------------------------------------
// Arrow IPC message framing
// ---------------------------------------------------------------------------

/**
 * Read the message header at `pos`, bounded by `end`.
 *
 * Returns `null` at the end-of-stream marker.
 */
function readMessage(view: DataView, pos: number, end: number): Message | null {
  if (pos + 8 > end) {
    throw new ArrowIpcError(`arrow ipc: truncated message prefix at ${pos}`);
  }
  const continuation = view.getUint32(pos, true);
  if (continuation !== CONTINUATION) {
    throw new ArrowIpcError(
      `arrow ipc: expected continuation 0xffffffff at ${pos}, found 0x${continuation.toString(16)}`,
    );
  }
  const metadataLen = view.getUint32(pos + 4, true);
  if (metadataLen === 0) {
    return null;
  }
  const flatbuffer = pos + 8;
  const bodyStart = flatbuffer + metadataLen;
  if (bodyStart > end) {
    throw new ArrowIpcError(
      `arrow ipc: message at ${pos} claims ${metadataLen} metadata bytes, past the segment end`,
    );
  }
  const message = flatbuffer + view.getUint32(flatbuffer, true);
  const headerTypeSlot = fbField(view, message, 1);
  const headerSlot = fbField(view, message, 2);
  if (headerSlot === 0) {
    throw new ArrowIpcError(`arrow ipc: message at ${pos} has no header`);
  }
  const bodyLengthSlot = fbField(view, message, 3);
  const bodyLength = bodyLengthSlot === 0 ? 0 : Number(view.getBigInt64(bodyLengthSlot, true));
  if (bodyLength < 0 || bodyStart + bodyLength > end) {
    throw new ArrowIpcError(
      `arrow ipc: message at ${pos} claims a ${bodyLength}-byte body, past the segment end`,
    );
  }
  return {
    headerType: headerTypeSlot === 0 ? 0 : view.getUint8(headerTypeSlot),
    header: fbDeref(view, headerSlot),
    bodyStart,
    bodyLength,
    next: bodyStart + align8(bodyLength),
  };
}

/**
 * Parse a segment's leading Schema message and pull `__saci_component` out of
 * its custom metadata.
 */
function scanSegment(
  view: DataView,
  bytes: Uint8Array,
  start: number,
  end: number,
): { component: string; schema: Message } {
  const schema = readMessage(view, start, end);
  // A segment whose whole body is the end-of-stream marker is its own failure:
  // it carries nothing at all, which is a different producer bug from a segment
  // that opens with the wrong kind of message.
  if (schema === null) {
    throw new ArrowIpcError(
      `saci stream: segment at ${start} is empty: no Schema message before the end-of-stream marker`,
    );
  }
  if (schema.headerType !== HEADER_SCHEMA) {
    throw new ArrowIpcError(`saci stream: segment at ${start} does not start with a Schema message`);
  }
  const metadataSlot = fbField(view, schema.header, 2);
  if (metadataSlot !== 0) {
    const entries = fbVector(view, metadataSlot);
    for (let i = 0; i < entries.count; i += 1) {
      const keyValue = fbDeref(view, entries.start + i * 4);
      const keySlot = fbField(view, keyValue, 0);
      const valueSlot = fbField(view, keyValue, 1);
      if (keySlot === 0 || valueSlot === 0) {
        continue;
      }
      if (fbString(view, bytes, keySlot) === COMPONENT_KEY) {
        return { component: fbString(view, bytes, valueSlot), schema };
      }
    }
  }
  throw new ArrowIpcError(
    `saci stream: segment at ${start} carries no "${COMPONENT_KEY}" schema metadata`,
  );
}

// ---------------------------------------------------------------------------
// Batch
// ---------------------------------------------------------------------------

/** Look up a field descriptor by name. */
function fieldOf(batch: Batch, name: string): Field {
  const field = batch.fields.get(name);
  if (field === undefined) {
    throw new ArrowIpcError(
      `arrow ipc: component "${batch.component}" has no field "${name}" (has: ${[...batch.fields.keys()].join(', ')})`,
    );
  }
  return field;
}

/** Resolve buffer slot `index` to an absolute `{ start, length }` in the body. */
function bufferAt(batch: Batch, index: number): Span {
  const at = batch.buffersStart + index * 16;
  const offset = Number(batch.view.getBigInt64(at, true));
  const length = Number(batch.view.getBigInt64(at + 8, true));
  if (offset < 0 || length < 0 || offset + length > batch.bodyLength) {
    throw new ArrowIpcError(
      `arrow ipc: buffer ${index} (offset ${offset}, length ${length}) escapes the ${batch.bodyLength}-byte body`,
    );
  }
  return { start: batch.bodyStart + offset, length };
}

/**
 * Values buffer of `field`, checked against the row count.
 *
 * `slot` is the offset within the field's slot run: 1 is the values buffer for
 * a fixed-width type and the i32 offsets buffer for `Utf8`.
 */
function valuesBuffer(batch: Batch, field: Field, slot: number, minBytes: number): Span {
  const buffer = bufferAt(batch, field.firstBuffer + slot);
  if (buffer.length < minBytes) {
    throw new ArrowIpcError(
      `arrow ipc: field "${field.name}" needs ${minBytes} bytes for ${batch.rows} rows, buffer ${field.firstBuffer + slot} holds ${buffer.length}`,
    );
  }
  return buffer;
}

/** Reject a read/write against a field whose Arrow type is not the expected one. */
function expectType(batch: Batch, name: string, wanted: ArrowType): Field {
  const field = fieldOf(batch, name);
  if (field.typeType !== wanted) {
    throw new ArrowIpcError(
      `arrow ipc: field "${name}" is ${TYPE_NAMES[field.typeType]}, not ${TYPE_NAMES[wanted]}`,
    );
  }
  return field;
}

/** Reject an out-of-range row index. */
function checkRow(batch: Batch, name: string, row: number): void {
  if (!Number.isInteger(row) || row < 0 || row >= batch.rows) {
    throw new ArrowIpcError(
      `arrow ipc: row ${row} is out of range for field "${name}" in a ${batch.rows}-row batch`,
    );
  }
}

/**
 * One component's RecordBatch, addressed by field name.
 *
 * Reads copy out of the underlying stream; writes go straight back into it.
 * Validity buffers are not consulted: a non-nullable field has an all-ones
 * bitmap, and an in-place value write never has to touch it.
 */
export class Batch {
  readonly view: DataView;
  readonly bytes: Uint8Array;
  readonly component: string;
  /** Row count from `RecordBatch.length`. */
  readonly rows: number;
  readonly fields: ReadonlyMap<string, Field>;
  readonly buffersStart: number;
  readonly bodyStart: number;
  readonly bodyLength: number;

  constructor(
    stream: SaciStream,
    component: string,
    rows: number,
    fields: ReadonlyMap<string, Field>,
    buffersStart: number,
    body: Span,
  ) {
    this.view = stream.view;
    this.bytes = stream.bytes;
    this.component = component;
    this.rows = rows;
    this.fields = fields;
    this.buffersStart = buffersStart;
    this.bodyStart = body.start;
    this.bodyLength = body.length;
  }

  /** Field names in schema order. */
  fieldNames(): string[] {
    return [...this.fields.keys()];
  }

  /** Read an `Int64` column. Values are `BigInt`: a JS number cannot hold the full range. */
  int64s(name: string): bigint[] {
    const field = expectType(this, name, TYPE_INT);
    const buffer = valuesBuffer(this, field, 1, this.rows * WIDTH_64);
    const out = new Array<bigint>(this.rows);
    for (let row = 0; row < this.rows; row += 1) {
      out[row] = this.view.getBigInt64(buffer.start + row * WIDTH_64, true);
    }
    return out;
  }

  /** Read a `Float64` column. */
  float64s(name: string): number[] {
    const field = expectType(this, name, TYPE_FLOAT);
    const buffer = valuesBuffer(this, field, 1, this.rows * WIDTH_64);
    const out = new Array<number>(this.rows);
    for (let row = 0; row < this.rows; row += 1) {
      out[row] = this.view.getFloat64(buffer.start + row * WIDTH_64, true);
    }
    return out;
  }

  /** Read a `Boolean` column out of its LSB-first bitmap. */
  bools(name: string): boolean[] {
    const field = expectType(this, name, TYPE_BOOL);
    const buffer = valuesBuffer(this, field, 1, Math.ceil(this.rows / 8));
    const out = new Array<boolean>(this.rows);
    for (let row = 0; row < this.rows; row += 1) {
      const byte = this.bytes[buffer.start + (row >> 3)];
      out[row] = (byte & (1 << (row & 7))) !== 0;
    }
    return out;
  }

  /** Read a `Utf8` column via its i32 offsets buffer. */
  strings(name: string): string[] {
    const field = expectType(this, name, TYPE_UTF8);
    const offsets = valuesBuffer(this, field, 1, (this.rows + 1) * 4);
    const values = bufferAt(this, field.firstBuffer + 2);
    const out = new Array<string>(this.rows);
    for (let row = 0; row < this.rows; row += 1) {
      const from = this.view.getInt32(offsets.start + row * 4, true);
      const to = this.view.getInt32(offsets.start + (row + 1) * 4, true);
      if (from < 0 || to < from || to > values.length) {
        throw new ArrowIpcError(
          `arrow ipc: field "${name}" row ${row} offsets [${from}, ${to}) escape its ${values.length}-byte values buffer`,
        );
      }
      out[row] = UTF8.decode(this.bytes.subarray(values.start + from, values.start + to));
    }
    return out;
  }

  /** Overwrite one `Int64` cell in place. */
  setInt64(name: string, row: number, value: bigint): void {
    const field = writable(this, name, TYPE_INT);
    checkRow(this, name, row);
    const buffer = valuesBuffer(this, field, 1, this.rows * WIDTH_64);
    this.view.setBigInt64(buffer.start + row * WIDTH_64, value, true);
  }

  /** Overwrite one `Float64` cell in place. */
  setFloat64(name: string, row: number, value: number): void {
    const field = writable(this, name, TYPE_FLOAT);
    checkRow(this, name, row);
    const buffer = valuesBuffer(this, field, 1, this.rows * WIDTH_64);
    this.view.setFloat64(buffer.start + row * WIDTH_64, value, true);
  }

  /** Overwrite one `Boolean` cell in place, flipping a single bit. */
  setBool(name: string, row: number, value: boolean): void {
    const field = writable(this, name, TYPE_BOOL);
    checkRow(this, name, row);
    const buffer = valuesBuffer(this, field, 1, Math.ceil(this.rows / 8));
    const at = buffer.start + (row >> 3);
    const mask = 1 << (row & 7);
    this.bytes[at] = value ? this.bytes[at] | mask : this.bytes[at] & ~mask;
  }
}

/**
 * Field a `set*` call may target.
 *
 * Variable-length columns are refused outright: widening a `Utf8` cell would
 * mean rewriting its offsets buffer, its values buffer and the RecordBatch
 * flatbuffer that describes both. Only the Rust stage, which has a real Arrow
 * writer, writes `settlement`.
 */
function writable(batch: Batch, name: string, wanted: ArrowType): Field {
  const field = fieldOf(batch, name);
  if (BUFFER_SLOTS[field.typeType] === 3) {
    throw new ArrowIpcError(
      `arrow ipc: field "${name}" is variable-length ${TYPE_NAMES[field.typeType]}; in-place writes are limited to fixed-width columns`,
    );
  }
  return expectType(batch, name, wanted);
}

/** Parse a segment's Schema + RecordBatch messages into an addressable [`Batch`]. */
function parseBatch(stream: SaciStream, segment: Segment): Batch {
  const { view, bytes } = stream;
  const fields = new Map<string, Field>();
  let fieldCount = 0;
  const fieldsSlot = fbField(view, segment.schema.header, 1);
  if (fieldsSlot !== 0) {
    const vector = fbVector(view, fieldsSlot);
    fieldCount = vector.count;
    for (let i = 0; i < vector.count; i += 1) {
      const table = fbDeref(view, vector.start + i * 4);
      const nameSlot = fbField(view, table, 0);
      if (nameSlot === 0) {
        throw new ArrowIpcError(`arrow ipc: schema field ${i} has no name`);
      }
      const name = fbString(view, bytes, nameSlot);
      const typeSlot = fbField(view, table, 2);
      const typeType = typeSlot === 0 ? 0 : view.getUint8(typeSlot);
      if (!isArrowType(typeType)) {
        throw new ArrowIpcError(`arrow ipc: field "${name}" has unsupported type_type ${typeType}`);
      }
      fields.set(name, { name, typeType, firstBuffer: 0 });
    }
  }

  const batch = readMessage(view, segment.schema.next, segment.end);
  if (batch === null) {
    throw new ArrowIpcError(
      `saci stream: segment "${segment.component}" has no RecordBatch message after its schema`,
    );
  }
  // A dictionary batch is named on its own. It is a plausible thing for a
  // writer to emit, and reporting it as a generic wrong header type sends the
  // reader hunting for a corrupt stream instead of an unsupported encoding.
  if (batch.headerType === HEADER_DICTIONARY) {
    throw new ArrowIpcError(
      `saci stream: segment "${segment.component}" carries a dictionary batch, which is not supported`,
    );
  }
  if (batch.headerType !== HEADER_RECORD_BATCH) {
    throw new ArrowIpcError(
      `saci stream: segment "${segment.component}" has header type ${batch.headerType} after its schema; expected a RecordBatch`,
    );
  }

  // A segment is one Schema, one RecordBatch, and at most the 8-byte
  // end-of-stream marker. Anything past that is data this reader would drop
  // without saying so.
  if (batch.next < segment.end) {
    const trailing = readMessage(view, batch.next, segment.end);
    if (trailing !== null || batch.next + 8 !== segment.end) {
      throw new ArrowIpcError(
        `saci stream: segment "${segment.component}" carries an extra message after its RecordBatch`,
      );
    }
  }

  if (fbField(view, batch.header, 3) !== 0) {
    throw new ArrowIpcError(
      `saci stream: segment "${segment.component}" declares body compression, which is not supported`,
    );
  }
  const buffersSlot = fbField(view, batch.header, 2);
  if (buffersSlot === 0) {
    throw new ArrowIpcError(
      `saci stream: segment "${segment.component}" RecordBatch has no buffers vector`,
    );
  }
  const buffers = fbVector(view, buffersSlot);

  // Bound the declared row count while it is still an i64: it is the loop bound
  // and the allocation size of every getter, so a negative or absurd value has
  // to be refused here rather than deep inside one.
  const lengthSlot = fbField(view, batch.header, 0);
  const declaredRows = lengthSlot === 0 ? 0n : view.getBigInt64(lengthSlot, true);
  if (declaredRows < 0n || declaredRows > MAX_ROWS) {
    throw new ArrowIpcError(
      `saci stream: segment "${segment.component}" RecordBatch claims ${declaredRows} rows, which is not a usable row count`,
    );
  }
  const rows = Number(declaredRows);

  // One FieldNode per schema field, in the same order. A nodes vector of a
  // different length means the writer and this reader disagree about the
  // schema, and the positional buffer walk below would then hand a field
  // another field's buffers.
  const nodesSlot = fbField(view, batch.header, 1);
  const nodeCount = nodesSlot === 0 ? 0 : fbVector(view, nodesSlot).count;
  if (nodeCount !== fieldCount) {
    throw new ArrowIpcError(
      `arrow ipc: schema has ${fieldCount} fields but the RecordBatch declares ${nodeCount} nodes`,
    );
  }

  // Buffer slots are positional: walk the fields in schema order and hand each
  // one the next run of slots its type consumes.
  let bufIdx = 0;
  for (const field of fields.values()) {
    field.firstBuffer = bufIdx;
    bufIdx += BUFFER_SLOTS[field.typeType];
    if (bufIdx > buffers.count) {
      throw new ArrowIpcError(
        `arrow ipc: field "${field.name}" needs buffer slots ${field.firstBuffer}..${bufIdx - 1}, but the RecordBatch declares only ${buffers.count}`,
      );
    }
  }
  if (bufIdx !== buffers.count) {
    throw new ArrowIpcError(
      `arrow ipc: schema consumes ${bufIdx} buffer slots but the RecordBatch declares ${buffers.count}`,
    );
  }

  const parsed = new Batch(stream, segment.component, rows, fields, buffers.start, {
    start: batch.bodyStart,
    length: batch.bodyLength,
  });
  // Every declared span is checked now, not when a getter reaches for one. The
  // validity slots are the reason: no getter reads one, so a lazy check leaves
  // a third of the batch's buffers never bounds-checked at all.
  for (let i = 0; i < buffers.count; i += 1) {
    bufferAt(parsed, i);
  }
  return parsed;
}

// ---------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------

/**
 * A parsed SACI host to processor stream.
 *
 * The stream borrows `bytes` and mutates it in place. The host hands
 * `run-batch` a freshly allocated array per call, so copying it would be
 * waste. Callers that need the original must copy before constructing.
 */
export class SaciStream {
  readonly bytes: Uint8Array;
  readonly view: DataView;
  private readonly segments: Segment[];

  constructor(bytes: Uint8Array) {
    // The static type is a promise, not a check: `run-batch` is called across
    // the component boundary, so this constructor is a trust boundary and
    // validates at run time too.
    //
    // `instanceof Uint8Array` is wrong here: componentize-js lifts `list<u8>`
    // into a Uint8Array allocated by the generated bindings, which live in a
    // different realm. Same shape, different intrinsics, so `instanceof` and
    // `constructor === Uint8Array` are both false inside the component.
    // `ArrayBuffer.isView` plus the element width is the realm-agnostic check,
    // and every operation below (DataView, subarray, indexing, TextDecoder)
    // accepts a cross-realm view.
    if (!ArrayBuffer.isView(bytes) || bytes.BYTES_PER_ELEMENT !== 1) {
      throw new ArrowIpcError('saci stream: expected a byte view (Uint8Array)');
    }
    this.bytes = bytes;
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    this.segments = [];

    // Component names come out of every segment up front: a segment without
    // `__saci_component` is malformed whether or not this stage reads it.
    let pos = 0;
    for (;;) {
      if (pos + 4 > bytes.byteLength) {
        throw new ArrowIpcError(`saci stream: truncated at ${pos}, before the segment terminator`);
      }
      const len = this.view.getUint32(pos, true);
      pos += 4;
      if (len === 0) {
        break;
      }
      const end = pos + len;
      if (end > bytes.byteLength) {
        throw new ArrowIpcError(
          `saci stream: truncated at ${pos}: segment claims ${len} bytes, only ${bytes.byteLength - pos} remain`,
        );
      }
      const scanned = scanSegment(this.view, bytes, pos, end);
      this.segments.push({ start: pos, end, component: scanned.component, schema: scanned.schema });
      pos = end;
    }

    // The terminator ends the stream. Bytes after it are payload the producer
    // wrote and this reader would never look at, which is a disagreement about
    // the format rather than a harmless tail.
    if (pos !== bytes.byteLength) {
      throw new ArrowIpcError(
        `saci stream: ${bytes.byteLength - pos} bytes follow the stream terminator at ${pos - 4}`,
      );
    }
  }

  /** Component names carried by this stream, in segment order. */
  componentNames(): string[] {
    return this.segments.map((segment) => segment.component);
  }

  /** The named segment, or a refusal naming what the stream does carry. */
  private segmentOf(name: string): Segment {
    const segment = this.segments.find((candidate) => candidate.component === name);
    if (segment === undefined) {
      throw new ArrowIpcError(
        `saci stream: no segment for component "${name}" (present: ${this.componentNames().join(', ') || 'none'})`,
      );
    }
    return segment;
  }

  /** The named component's batch. Throws when the stream carries no such segment. */
  component(name: string): Batch {
    const segment = this.segmentOf(name);
    if (segment.batch === undefined) {
      segment.batch = parseBatch(this, segment);
    }
    return segment.batch;
  }

  /**
   * The named segment's Arrow IPC stream, without its length prefix.
   *
   * A view into this stream's bytes, not a copy: it exists so a stage that
   * re-encodes with [`SaciStreamWriter`] can pass a component it does not
   * declare through untouched, via [`SaciStreamWriter.writeSegment`]. Re-encoding
   * such a component is not possible — a processor cannot know a foreign
   * schema's version — and dropping it would delete host state.
   */
  segmentBytes(name: string): Uint8Array {
    const segment = this.segmentOf(name);
    return this.bytes.subarray(segment.start, segment.end);
  }

  /** The stream's bytes, including every in-place mutation. */
  toBytes(): Uint8Array {
    return this.bytes;
  }
}

/**
 * Decode a base64 string to bytes.
 *
 * `atob` is the one base64 primitive both Node and StarlingMonkey expose, so
 * the processor decodes its generated schema constant with it and needs no
 * dependency.
 */
export function decodeBase64(text: string): Uint8Array {
  let raw: string;
  try {
    raw = atob(text);
  } catch (cause) {
    // `atob` raises a DOMException, which is not this codec's error type.
    throw new ArrowIpcError(`arrow ipc: not valid base64: ${String(cause)}`);
  }
  const out = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i += 1) {
    out[i] = raw.charCodeAt(i);
  }
  return out;
}

// ---------------------------------------------------------------------------
// FlatBuffers writer
//
// A flatbuffer is built back to front: every nested object is finished before
// the table that points at it, and a "position" below is a byte count from the
// buffer's end rather than an index into it. That is what makes the encoding
// single-pass — a table's field slots are known by the time its vtable is
// written — and it is why `createString`, `createOffsetVector` and
// `writeTypeTable` are all called before the `startTable` that references
// their results.
//
// Vtables are not deduplicated. The Arrow writer here emits at most a few
// dozen tables per message, so the shared-vtable search would cost more than
// the handful of bytes it saves.
// ---------------------------------------------------------------------------

/** Arrow `MetadataVersion.V5`, the only metadata version SACI reads or writes. */
const METADATA_V5 = 4;

/** `FloatingPoint.precision` discriminant for `DOUBLE`. */
const PRECISION_DOUBLE = 2;

/** Schema custom-metadata key holding a component's decimal `u32` schema version. */
const SCHEMA_VERSION_KEY = '__saci_schema_version';

/** Component name of the trailing liveness segment. */
const ALIVE_COMPONENT = '__alive';

/** The liveness segment's single `Boolean` column. */
const ALIVE_FIELD = 'alive';

/** Largest `u32`, the range of a schema version. */
const MAX_VERSION = 0xffff_ffff;

/** Largest i32, the range of a `Utf8` values-buffer offset. */
const MAX_OFFSET = 0x7fff_ffff;

const UTF8_ENCODER = new TextEncoder();

/** The four column types a SACI component may declare, as this codec names them. */
export type ColumnType = 'int64' | 'float64' | 'bool' | 'utf8';

/** Arrow `Field.type_type` for each column type. */
const ARROW_TYPE = {
  int64: TYPE_INT,
  float64: TYPE_FLOAT,
  bool: TYPE_BOOL,
  utf8: TYPE_UTF8,
} as const satisfies Record<ColumnType, ArrowType>;

class FlatBuilder {
  private bytes: Uint8Array;
  private view: DataView;
  /** Unused bytes at the front: the encoding grows towards index 0. */
  private space: number;
  /** Widest scalar written so far, which is what the root offset aligns to. */
  private minAlign = 1;
  /** Slot positions of the table under construction, by field id; 0 is absent. */
  private slots: number[] = [];
  /** Position the current table's first field was written at. */
  private tableStart = 0;
  /** Element count of the vector under construction. */
  private vectorElems = 0;

  constructor(capacity: number) {
    this.bytes = new Uint8Array(capacity);
    this.view = new DataView(this.bytes.buffer);
    this.space = capacity;
  }

  /** Bytes written so far; also the position just past the last byte written. */
  private position(): number {
    return this.bytes.length - this.space;
  }

  /** Double the buffer, keeping the encoding flush against its end. */
  private grow(): void {
    const old = this.bytes;
    const grown = new Uint8Array(old.length * 2);
    grown.set(old, old.length);
    this.bytes = grown;
    this.view = new DataView(grown.buffer);
    this.space += old.length;
  }

  /**
   * Make room for a `width`-wide scalar that will sit `additional` bytes before
   * the current position, padding so that scalar lands `width`-aligned.
   */
  private prep(width: number, additional: number): void {
    if (width > this.minAlign) {
      this.minAlign = width;
    }
    const pad = -(this.position() + additional) & (width - 1);
    while (this.space < pad + width + additional) {
      this.grow();
    }
    for (let i = 0; i < pad; i += 1) {
      this.space -= 1;
      this.bytes[this.space] = 0;
    }
  }

  int8(value: number): void {
    this.prep(1, 0);
    this.space -= 1;
    this.view.setUint8(this.space, value);
  }

  int16(value: number): void {
    this.prep(2, 0);
    this.space -= 2;
    this.view.setInt16(this.space, value, true);
  }

  int32(value: number): void {
    this.prep(4, 0);
    this.space -= 4;
    this.view.setInt32(this.space, value, true);
  }

  int64(value: bigint): void {
    this.prep(8, 0);
    this.space -= 8;
    this.view.setBigInt64(this.space, value, true);
  }

  /** A uoffset from here back to `target`. */
  private uoffset(target: number): void {
    this.prep(4, 0);
    const value = this.position() + 4 - target;
    this.space -= 4;
    this.view.setInt32(this.space, value, true);
  }

  /** Open a table with `fields` vtable slots. Nested objects must already be built. */
  startTable(fields: number): void {
    this.slots = new Array<number>(fields).fill(0);
    this.tableStart = this.position();
  }

  slotInt8(id: number, value: number): void {
    this.int8(value);
    this.slots[id] = this.position();
  }

  slotInt16(id: number, value: number): void {
    this.int16(value);
    this.slots[id] = this.position();
  }

  slotInt32(id: number, value: number): void {
    this.int32(value);
    this.slots[id] = this.position();
  }

  slotInt64(id: number, value: bigint): void {
    this.int64(value);
    this.slots[id] = this.position();
  }

  slotOffset(id: number, target: number): void {
    this.uoffset(target);
    this.slots[id] = this.position();
  }

  /** Write the table's vtable and return the table's position. */
  endTable(): number {
    this.int32(0); // placeholder for the soffset to the vtable
    const table = this.position();
    // Trailing absent slots are dropped: a reader treats a field id at or past
    // `vtable_len` as absent, which is what the codec's own `fbField` does.
    let used = this.slots.length;
    while (used > 0 && this.slots[used - 1] === 0) {
      used -= 1;
    }
    for (let id = used - 1; id >= 0; id -= 1) {
      this.int16(this.slots[id] === 0 ? 0 : table - this.slots[id]);
    }
    this.int16(table - this.tableStart);
    this.int16((used + 2) * 2);
    this.view.setInt32(this.bytes.length - table, this.position() - table, true);
    return table;
  }

  /** Reserve an inline vector of `elems` `elemSize`-wide elements. */
  private startVector(elemSize: number, elems: number, align: number): void {
    this.vectorElems = elems;
    this.prep(4, elemSize * elems);
    this.prep(align, elemSize * elems);
  }

  /** Write the element count and return the vector's position. */
  private endVector(): number {
    this.int32(this.vectorElems);
    return this.position();
  }

  createString(text: string): number {
    const encoded = UTF8_ENCODER.encode(text);
    // FlatBuffers strings carry a NUL past their declared length so that a
    // reader may hand the bytes straight to C.
    this.int8(0);
    this.startVector(1, encoded.length, 1);
    this.space -= encoded.length;
    this.bytes.set(encoded, this.space);
    return this.endVector();
  }

  /** Vector of uoffsets to already-built tables, in `targets` order. */
  createOffsetVector(targets: readonly number[]): number {
    this.startVector(4, targets.length, 4);
    for (let i = targets.length - 1; i >= 0; i -= 1) {
      this.uoffset(targets[i]);
    }
    return this.endVector();
  }

  /**
   * `count` inline `FieldNode { i64 length, i64 null_count }` structs.
   *
   * Every node is identical: this writer emits non-nullable fields only, so
   * each holds `rows` values and no nulls.
   */
  fieldNodes(count: number, rows: number): number {
    const length = BigInt(rows);
    this.startVector(16, count, 8);
    for (let i = 0; i < count; i += 1) {
      this.int64(0n); // null_count
      this.int64(length);
    }
    return this.endVector();
  }

  /** Inline `Buffer { i64 offset, i64 length }` structs, in slot order. */
  bufferSpans(spans: readonly Span[]): number {
    this.startVector(16, spans.length, 8);
    for (let i = spans.length - 1; i >= 0; i -= 1) {
      this.int64(BigInt(spans[i].length));
      this.int64(BigInt(spans[i].start));
    }
    return this.endVector();
  }

  /** Write the root uoffset and return the finished flatbuffer. */
  finish(root: number): Uint8Array {
    this.prep(this.minAlign, 4);
    this.uoffset(root);
    return this.bytes.subarray(this.space);
  }
}

// ---------------------------------------------------------------------------
// Arrow message writers
// ---------------------------------------------------------------------------

/** A column's name and type, which is all a Schema message needs. */
export interface SchemaField {
  readonly name: string;
  readonly type: ColumnType;
}

/** The Arrow `Type` union member for `type`, as its own table. */
function writeTypeTable(fb: FlatBuilder, type: ColumnType): number {
  switch (type) {
    case 'int64':
      fb.startTable(2); // bitWidth, is_signed
      fb.slotInt32(0, 64);
      fb.slotInt8(1, 1);
      return fb.endTable();
    case 'float64':
      fb.startTable(1); // precision
      fb.slotInt16(0, PRECISION_DOUBLE);
      return fb.endTable();
    default:
      // `Utf8` and `Bool` are parameterless: an empty table, not an absent one.
      fb.startTable(0);
      return fb.endTable();
  }
}

/** Wrap a header table in a `Message` and finish the flatbuffer. */
function writeMessage(
  fb: FlatBuilder,
  headerType: number,
  header: number,
  bodyLength: number,
): Uint8Array {
  fb.startTable(4); // version, header_type, header, bodyLength
  fb.slotInt16(0, METADATA_V5);
  fb.slotInt8(1, headerType);
  fb.slotOffset(2, header);
  fb.slotInt64(3, BigInt(bodyLength));
  return fb.finish(fb.endTable());
}

/**
 * A Schema message flatbuffer: one `Field` per column plus SACI custom metadata.
 *
 * `endianness` is left absent, which a reader takes as its default of little,
 * and every field is left non-nullable for the same reason.
 */
function writeSchemaMessage(
  fields: readonly SchemaField[],
  metadata: readonly (readonly [string, string])[],
): Uint8Array {
  const fb = new FlatBuilder(1024);

  const fieldTables = new Array<number>(fields.length);
  for (let i = 0; i < fields.length; i += 1) {
    const field = fields[i];
    const name = fb.createString(field.name);
    const type = writeTypeTable(fb, field.type);
    fb.startTable(4); // name, nullable, type_type, type
    fb.slotOffset(0, name);
    fb.slotInt8(2, ARROW_TYPE[field.type]);
    fb.slotOffset(3, type);
    fieldTables[i] = fb.endTable();
  }
  const fieldVector = fb.createOffsetVector(fieldTables);

  const metaTables = new Array<number>(metadata.length);
  for (let i = 0; i < metadata.length; i += 1) {
    const key = fb.createString(metadata[i][0]);
    const value = fb.createString(metadata[i][1]);
    fb.startTable(2); // key, value
    fb.slotOffset(0, key);
    fb.slotOffset(1, value);
    metaTables[i] = fb.endTable();
  }
  const metaVector = metaTables.length === 0 ? 0 : fb.createOffsetVector(metaTables);

  fb.startTable(3); // endianness, fields, custom_metadata
  fb.slotOffset(1, fieldVector);
  if (metaVector !== 0) {
    fb.slotOffset(2, metaVector);
  }
  const schema = fb.endTable();
  return writeMessage(fb, HEADER_SCHEMA, schema, 0);
}

/** A RecordBatch message flatbuffer describing `spans` inside a `bodyLength`-byte body. */
function writeRecordBatchMessage(
  rows: number,
  fieldCount: number,
  spans: readonly Span[],
  bodyLength: number,
): Uint8Array {
  const fb = new FlatBuilder(512);
  const nodes = fb.fieldNodes(fieldCount, rows);
  const buffers = fb.bufferSpans(spans);
  fb.startTable(3); // length, nodes, buffers ('compression' stays absent)
  fb.slotInt64(0, BigInt(rows));
  fb.slotOffset(1, nodes);
  fb.slotOffset(2, buffers);
  const batch = fb.endTable();
  return writeMessage(fb, HEADER_RECORD_BATCH, batch, bodyLength);
}

// ---------------------------------------------------------------------------
// Columns
// ---------------------------------------------------------------------------

/**
 * One column of a component, values included.
 *
 * Built by [`Int64Column`], [`Float64Column`], [`BoolColumn`] and
 * [`Utf8Column`] rather than by hand: the tag has to agree with the value type,
 * and a factory per type is what lets the compiler enforce that.
 */
export type Column =
  | { readonly type: 'int64'; readonly name: string; readonly values: readonly (bigint | number)[] }
  | { readonly type: 'float64'; readonly name: string; readonly values: readonly number[] }
  | { readonly type: 'bool'; readonly name: string; readonly values: readonly boolean[] }
  | { readonly type: 'utf8'; readonly name: string; readonly values: readonly string[] };

/**
 * An `Int64` column.
 *
 * Values may be `bigint` or `number`: the reader hands back `bigint` because a
 * JS number cannot hold the full range, but a producer whose values came from
 * arithmetic has numbers, and refusing them would push the conversion into
 * every caller.
 */
export function Int64Column(name: string, values: readonly (bigint | number)[]): Column {
  return { type: 'int64', name, values };
}

/** A `Float64` column. */
export function Float64Column(name: string, values: readonly number[]): Column {
  return { type: 'float64', name, values };
}

/** A `Boolean` column, written as an LSB-first bitmap. */
export function BoolColumn(name: string, values: readonly boolean[]): Column {
  return { type: 'bool', name, values };
}

/** A `Utf8` column, written as i32 offsets plus UTF-8 bytes. */
export function Utf8Column(name: string, values: readonly string[]): Column {
  return { type: 'utf8', name, values };
}

/** Narrow one `Int64` value to the range Arrow can carry, or refuse it. */
function int64Of(name: string, row: number, value: bigint | number): bigint {
  if (typeof value === 'number') {
    // `BigInt(1.5)` throws a RangeError, which is not this codec's error type,
    // and a silent truncation would corrupt the column instead.
    if (!Number.isInteger(value)) {
      throw new ArrowIpcError(`arrow ipc: field "${name}" row ${row} value ${value} is not an integer`);
    }
    return BigInt(value);
  }
  // `setBigInt64` wraps modulo 2^64 rather than throwing, so an out-of-range
  // value would land in the buffer as a different number entirely.
  if (BigInt.asIntN(64, value) !== value) {
    throw new ArrowIpcError(`arrow ipc: field "${name}" row ${row} value ${value} does not fit in Int64`);
  }
  return value;
}

/** A RecordBatch body: the bytes, and the span of every buffer slot inside them. */
interface Body {
  readonly bytes: Uint8Array;
  readonly spans: Span[];
}

/**
 * Encode `columns` into one RecordBatch body.
 *
 * Buffer slots follow [`BUFFER_SLOTS`]: a validity bitmap first, then the
 * values, and for `Utf8` the i32 offsets between them. Each slot starts on an
 * 8-byte boundary and declares its exact unpadded length, which is what
 * arrow-rs emits and what the reader above bounds-checks.
 *
 * The validity bitmap is all ones. It is not strictly needed — every field here
 * is non-nullable and declares `null_count: 0`, so arrow-rs never reads it —
 * but the slot is positional, and a zero-length one would move every buffer
 * after it.
 */
function writeBody(rows: number, columns: readonly Column[]): Body {
  const validityBytes = Math.ceil(rows / 8);
  const lengths: number[] = [];
  // `Utf8` value bytes are needed twice, to size the body and to fill it, and
  // TextEncoder cannot answer the first question without doing the work.
  const encoded = new Map<number, Uint8Array[]>();

  for (let i = 0; i < columns.length; i += 1) {
    const column = columns[i];
    lengths.push(validityBytes);
    switch (column.type) {
      case 'int64':
      case 'float64':
        lengths.push(rows * WIDTH_64);
        break;
      case 'bool':
        lengths.push(validityBytes);
        break;
      case 'utf8': {
        const parts = new Array<Uint8Array>(rows);
        let total = 0;
        for (let row = 0; row < rows; row += 1) {
          parts[row] = UTF8_ENCODER.encode(column.values[row]);
          total += parts[row].length;
        }
        // A `Utf8` column addresses its values buffer with i32 offsets, so past
        // this the offsets would wrap into negative numbers rather than fail.
        if (total > MAX_OFFSET) {
          throw new ArrowIpcError(
            `arrow ipc: field "${column.name}" holds ${total} bytes of text, past the i32 offset limit`,
          );
        }
        encoded.set(i, parts);
        lengths.push((rows + 1) * 4, total);
        break;
      }
    }
  }

  const spans = new Array<Span>(lengths.length);
  let at = 0;
  for (let slot = 0; slot < lengths.length; slot += 1) {
    spans[slot] = { start: at, length: lengths[slot] };
    at = align8(at + lengths[slot]);
  }

  const bytes = new Uint8Array(at);
  const view = new DataView(bytes.buffer);
  let slot = 0;
  for (let i = 0; i < columns.length; i += 1) {
    const column = columns[i];
    const validity = spans[slot];
    bytes.fill(0xff, validity.start, validity.start + validity.length);
    slot += 1;
    const values = spans[slot];
    slot += 1;
    switch (column.type) {
      case 'int64':
        for (let row = 0; row < rows; row += 1) {
          const value = int64Of(column.name, row, column.values[row]);
          view.setBigInt64(values.start + row * WIDTH_64, value, true);
        }
        break;
      case 'float64':
        for (let row = 0; row < rows; row += 1) {
          view.setFloat64(values.start + row * WIDTH_64, column.values[row], true);
        }
        break;
      case 'bool':
        // Zero-initialised, so only set bits need writing.
        for (let row = 0; row < rows; row += 1) {
          if (column.values[row]) {
            bytes[values.start + (row >> 3)] |= 1 << (row & 7);
          }
        }
        break;
      case 'utf8': {
        // Set above for exactly the `Utf8` columns, keyed by the same index.
        const parts = encoded.get(i) as Uint8Array[];
        const data = spans[slot];
        slot += 1;
        let cursor = 0;
        for (let row = 0; row < rows; row += 1) {
          view.setInt32(values.start + row * 4, cursor, true);
          bytes.set(parts[row], data.start + cursor);
          cursor += parts[row].length;
        }
        view.setInt32(values.start + rows * 4, cursor, true);
        break;
      }
    }
  }

  return { bytes, spans };
}

// ---------------------------------------------------------------------------
// Segment framing
// ---------------------------------------------------------------------------

/** Bytes one message occupies: the 8-byte prefix plus the padded flatbuffer. */
function frameSize(flatbuffer: Uint8Array): number {
  return 8 + align8(flatbuffer.length);
}

/**
 * Write one message prefix and its flatbuffer at `at`, returning the position
 * its body starts at.
 *
 * `metadata_len` covers the flatbuffer's padding to 8 bytes, which is what puts
 * the body — and therefore the next message — on an 8-byte boundary.
 */
function writeFrame(out: Uint8Array, view: DataView, at: number, flatbuffer: Uint8Array): number {
  const padded = align8(flatbuffer.length);
  view.setUint32(at, CONTINUATION, true);
  view.setUint32(at + 4, padded, true);
  out.set(flatbuffer, at + 8);
  return at + 8 + padded;
}

/** One complete Arrow IPC stream: Schema, RecordBatch, end-of-stream marker. */
function buildSegment(schema: Uint8Array, batch: Uint8Array, body: Uint8Array): Uint8Array {
  const out = new Uint8Array(frameSize(schema) + frameSize(batch) + body.length + 8);
  const view = new DataView(out.buffer);
  let at = writeFrame(out, view, 0, schema);
  at = writeFrame(out, view, at, batch);
  out.set(body, at);
  // End-of-stream: the continuation word followed by a zero metadata length,
  // which the zero-filled tail already provides.
  view.setUint32(at + body.length, CONTINUATION, true);
  return out;
}

/** Reject a column set that cannot describe a batch. */
function checkColumns(component: string, columns: readonly Column[]): number {
  if (columns.length === 0) {
    throw new ArrowIpcError(`saci stream: component "${component}" needs at least one column`);
  }
  const rows = columns[0].values.length;
  const seen = new Set<string>();
  for (const column of columns) {
    if (column.values.length !== rows) {
      throw new ArrowIpcError(
        `saci stream: component "${component}" column "${column.name}" holds ${column.values.length} rows, not ${rows} like "${columns[0].name}"`,
      );
    }
    if (seen.has(column.name)) {
      throw new ArrowIpcError(
        `saci stream: component "${component}" declares column "${column.name}" twice`,
      );
    }
    seen.add(column.name);
  }
  if (BigInt(rows) > MAX_ROWS) {
    throw new ArrowIpcError(
      `saci stream: component "${component}" claims ${rows} rows, which is not a usable row count`,
    );
  }
  return rows;
}

/**
 * A *schema-only* Arrow IPC stream: one Schema message and the end-of-stream
 * marker, no batches and no custom metadata.
 *
 * This is exactly `component-descriptor.arrow-schema-ipc` in the WIT contract,
 * which is why it is separate from a segment's schema: the host reads it with
 * `StreamReader::schema()` to build its template dataset, and a segment's
 * schema additionally carries `__saci_component`, which does not belong in a
 * descriptor.
 */
export function schemaIpc(fields: readonly SchemaField[]): Uint8Array {
  if (fields.length === 0) {
    throw new ArrowIpcError('arrow ipc: a schema needs at least one field');
  }
  const seen = new Set<string>();
  for (const field of fields) {
    if (seen.has(field.name)) {
      throw new ArrowIpcError(`arrow ipc: schema declares field "${field.name}" twice`);
    }
    seen.add(field.name);
  }
  const message = writeSchemaMessage(fields, []);
  const out = new Uint8Array(frameSize(message) + 8);
  const view = new DataView(out.buffer);
  const at = writeFrame(out, view, 0, message);
  view.setUint32(at, CONTINUATION, true);
  return out;
}

// ---------------------------------------------------------------------------
// Stream writer
// ---------------------------------------------------------------------------

/**
 * A SACI host to processor stream under construction.
 *
 * Each [`writeComponent`](SaciStreamWriter.writeComponent) call encodes one
 * complete segment — Schema message, RecordBatch message, end-of-stream marker
 * — so a stage that changes a row count or a string length hands back a stream
 * the host parses with the same `StreamReader` it uses for its own output.
 *
 * The `__alive` segment is not optional: the host reads it as the dataset's row
 * count and rejects a stream without one, so [`toBytes`](SaciStreamWriter.toBytes)
 * refuses to produce bytes until [`writeAlive`](SaciStreamWriter.writeAlive) has
 * been called.
 */
export class SaciStreamWriter {
  /** Component segments and passed-through segments, in call order. */
  private readonly segments: Uint8Array[] = [];
  /** Row counts of the built segments, checked against the `__alive` length. */
  private readonly rowCounts: { readonly component: string; readonly rows: number }[] = [];
  private aliveSegment: Uint8Array | undefined;
  private aliveRows = 0;

  /**
   * Encode one component as a segment.
   *
   * `version` is the component's schema version, which the host reads back out
   * of `__saci_schema_version`; every column must hold the same number of rows,
   * and the column order is the schema order.
   */
  writeComponent(name: string, version: number, ...columns: readonly Column[]): void {
    if (name.length === 0) {
      throw new ArrowIpcError('saci stream: a component name cannot be empty');
    }
    if (name === ALIVE_COMPONENT) {
      throw new ArrowIpcError(
        `saci stream: "${ALIVE_COMPONENT}" is the liveness segment; write it with writeAlive`,
      );
    }
    if (!Number.isInteger(version) || version < 0 || version > MAX_VERSION) {
      throw new ArrowIpcError(
        `saci stream: component "${name}" schema version ${version} is not a u32`,
      );
    }
    const rows = checkColumns(name, columns);
    const body = writeBody(rows, columns);
    const schema = writeSchemaMessage(columns, [
      [COMPONENT_KEY, name],
      [SCHEMA_VERSION_KEY, String(version)],
    ]);
    const batch = writeRecordBatchMessage(rows, columns.length, body.spans, body.bytes.length);
    this.segments.push(buildSegment(schema, batch, body.bytes));
    this.rowCounts.push({ component: name, rows });
  }

  /**
   * Encode the trailing `__alive` segment.
   *
   * Its length is the dataset's row count: the host takes it as the bound every
   * component's row count is validated against.
   */
  writeAlive(bits: readonly boolean[]): void {
    if (this.aliveSegment !== undefined) {
      throw new ArrowIpcError('saci stream: the liveness segment is already written');
    }
    const column = BoolColumn(ALIVE_FIELD, bits);
    const rows = checkColumns(ALIVE_COMPONENT, [column]);
    const body = writeBody(rows, [column]);
    // Only `__saci_component`: the liveness segment has no schema version,
    // because it is the host's own bookkeeping rather than a component.
    const schema = writeSchemaMessage([column], [[COMPONENT_KEY, ALIVE_COMPONENT]]);
    const batch = writeRecordBatchMessage(rows, 1, body.spans, body.bytes.length);
    this.aliveSegment = buildSegment(schema, batch, body.bytes);
    this.aliveRows = rows;
  }

  /**
   * Append an already-encoded segment verbatim.
   *
   * This is how a stage preserves a component it does not declare. Re-encoding
   * one is not an option: a processor cannot know a foreign component's schema
   * version, and dropping it would silently delete host state, so the segment's
   * own bytes — obtainable from [`SaciStream.segmentBytes`] — are copied through
   * unread.
   */
  writeSegment(segment: Uint8Array): void {
    if (!ArrayBuffer.isView(segment) || segment.BYTES_PER_ELEMENT !== 1) {
      throw new ArrowIpcError('saci stream: expected a byte view (Uint8Array)');
    }
    if (segment.byteLength === 0) {
      throw new ArrowIpcError('saci stream: an empty segment carries no schema');
    }
    this.segments.push(segment);
  }

  /**
   * The finished stream: every component segment, the `__alive` segment, then
   * the terminator.
   *
   * Segment lengths are computed here rather than tracked, so a writer reused
   * after a change still frames what it holds now.
   */
  toBytes(): Uint8Array {
    const alive = this.aliveSegment;
    if (alive === undefined) {
      throw new ArrowIpcError(
        `saci stream: no "${ALIVE_COMPONENT}" segment; call writeAlive before toBytes`,
      );
    }
    // A component longer than the liveness bitmap is corruption on the host
    // side: the bitmap is the dataset's row bound. Fewer rows is legitimate — a
    // windowing stage reduces N rows to M.
    for (const entry of this.rowCounts) {
      if (entry.rows > this.aliveRows) {
        throw new ArrowIpcError(
          `saci stream: component "${entry.component}" holds ${entry.rows} rows, more than the ${this.aliveRows}-row liveness bitmap`,
        );
      }
    }

    let size = 4; // terminator
    for (const segment of this.segments) {
      size += 4 + segment.byteLength;
    }
    size += 4 + alive.byteLength;

    const out = new Uint8Array(size);
    const view = new DataView(out.buffer);
    let at = 0;
    for (const segment of this.segments) {
      view.setUint32(at, segment.byteLength, true);
      out.set(segment, at + 4);
      at += 4 + segment.byteLength;
    }
    view.setUint32(at, alive.byteLength, true);
    out.set(alive, at + 4);
    // The terminating zero length is already there: `out` is zero-filled.
    return out;
  }
}
