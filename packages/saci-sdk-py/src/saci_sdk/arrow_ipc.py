"""Arrow IPC codec for the SACI host<->processor wire format, stdlib only.

`pyarrow` has no `wasm32-wasi` wheel, so this package implements the slice of the
format a processor needs, in both directions.

**Reading and patching.** A processor locates its component's segment, reads the
columns it consumes, overwrites fixed-width value bytes in place, and hands the
input buffer back. Framing, both flatbuffers, and every column it did not touch
are returned byte-identical. `set_int64`, `set_float64` and `set_bool` are that
whole surface: a `Utf8` value cannot be patched, because a different string
length moves the values buffer, invalidates the offsets buffer, and invalidates
the `Buffer` entries in the RecordBatch flatbuffer that describe both.

**Writing.** `SaciStream()` builds an envelope out of column values instead:
`write_component` encodes a real Schema and RecordBatch flatbuffer pair, so a
processor may write a string column, or return fewer rows than it was handed. A
parsed stream takes the same calls and keeps every segment it was not asked to
replace exactly as it arrived, so pass-through stays byte-identical.

The `__alive` segment is parsed (so a malformed stream is still rejected) but
never consulted by a patching processor: one that can neither add nor remove
rows cannot change which rows are alive.

Format reference: `docs/content/library/reference/wire-format.md`. The flatbuffers field
ids below come from Arrow's `Message.fbs` and `Schema.fbs`.
"""

import base64
import struct

# --------------------------------------------------------------------------
# Format constants.
# --------------------------------------------------------------------------

#: Prefix of every Arrow IPC message in the encapsulated (post-0.15) format.
_CONTINUATION = 0xFFFFFFFF

_HEADER_SCHEMA = 1
_HEADER_DICTIONARY_BATCH = 2
_HEADER_RECORD_BATCH = 3

# `Message` field ids.
_MSG_VERSION = 0
_MSG_HEADER_TYPE = 1
_MSG_HEADER = 2
_MSG_BODY_LENGTH = 3

# `Schema` field ids.
_SCHEMA_FIELDS = 1
_SCHEMA_CUSTOM_METADATA = 2

# `Field` field ids.
_FIELD_NAME = 0
_FIELD_TYPE_TYPE = 2
_FIELD_TYPE = 3

# `KeyValue` field ids.
_KV_KEY = 0
_KV_VALUE = 1

# `RecordBatch` field ids.
_RB_LENGTH = 0
_RB_NODES = 1
_RB_BUFFERS = 2
_RB_COMPRESSION = 3

# `Type` union discriminants this codec understands.
TYPE_INT = 2
TYPE_FLOAT = 3
TYPE_UTF8 = 5
TYPE_BOOL = 6

_TYPE_NAMES = {
    TYPE_INT: "Int",
    TYPE_FLOAT: "FloatingPoint",
    TYPE_UTF8: "Utf8",
    TYPE_BOOL: "Bool",
}

#: Buffer slots each Arrow type consumes, in schema order. Fixed by the type,
#: never by nullability: arrow-rs emits the validity slot even when the field is
#: non-nullable and the batch has no nulls.
_BUFFER_SLOTS = {
    TYPE_INT: 2,  # validity, values
    TYPE_FLOAT: 2,  # validity, values
    TYPE_UTF8: 3,  # validity, i32 offsets, values
    TYPE_BOOL: 2,  # validity, bit-packed values
}

#: Largest row count this codec accepts as a length. A `Utf8` column addresses
#: its values with i32 offsets, so a wider batch cannot be described by the
#: format at all, and every sibling codec narrows the declared count to a
#: 32-bit index the same way.
_MAX_ROWS = 2**31 - 1

#: Schema metadata key carrying the saci-core component name.
_COMPONENT_KEY = "__saci_component"

#: Schema metadata key carrying that component's decimal `u32` version. Absent
#: on the `__alive` segment, which has no version of its own.
_SCHEMA_VERSION_KEY = "__saci_schema_version"

#: Component name of the trailing liveness segment, and the name of its single
#: `Boolean` column. Every component's row count is bounded by its length.
ALIVE_COMPONENT = "__alive"
ALIVE_FIELD = "alive"

_U8 = struct.Struct("<B")
_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")
_I16 = struct.Struct("<h")
_I32 = struct.Struct("<i")
_I64 = struct.Struct("<q")
_F64 = struct.Struct("<d")

# --------------------------------------------------------------------------
# Bounds-checked primitives.
#
# Every read goes through these so that a truncated or hostile buffer raises
# ValueError instead of `struct.error` or IndexError: the WIT contract says a
# processor maps bad input to `permanent(string)`, never to a trap.
# --------------------------------------------------------------------------


def _read(spec, buf, pos):
    """Read one little-endian scalar, rejecting out-of-range positions."""
    if pos < 0 or pos + spec.size > len(buf):
        raise ValueError(
            "arrow ipc: {} B read at {} is outside the {} B stream".format(
                spec.size, pos, len(buf)
            )
        )
    return spec.unpack_from(buf, pos)[0]


def _bounds(buf, pos, length):
    """Reject a span that leaves the buffer."""
    if pos < 0 or length < 0 or pos + length > len(buf):
        raise ValueError(
            "arrow ipc: span ({}, {}) is outside the {} B stream".format(
                pos, length, len(buf)
            )
        )


class _Table:
    """A FlatBuffers table addressed absolutely inside the stream buffer.

    FlatBuffers offsets are all self-relative, so absolute positions work
    unchanged as long as the root uoffset is read at the flatbuffer's start.
    """

    __slots__ = ("buf", "pos", "_vtable", "_vtable_len")

    def __init__(self, buf, pos):
        vtable = pos - _read(_I32, buf, pos)
        vtable_len = _read(_U16, buf, vtable)
        if vtable_len < 4:
            raise ValueError(
                "arrow ipc: vtable at {} is {} B, minimum is 4".format(vtable, vtable_len)
            )
        self.buf = buf
        self.pos = pos
        self._vtable = vtable
        self._vtable_len = vtable_len

    def _slot(self, field_id):
        """Byte offset of `field_id` from the table start; 0 when absent."""
        entry = 4 + 2 * field_id
        if entry + 2 > self._vtable_len:
            return 0
        return _read(_U16, self.buf, self._vtable + entry)

    def has(self, field_id):
        return self._slot(field_id) != 0

    def scalar(self, field_id, spec, default):
        offset = self._slot(field_id)
        if offset == 0:
            return default
        return _read(spec, self.buf, self.pos + offset)

    def _indirect(self, field_id):
        """Absolute target of the uoffset in `field_id`; -1 when absent."""
        offset = self._slot(field_id)
        if offset == 0:
            return -1
        at = self.pos + offset
        return at + _read(_U32, self.buf, at)

    def table(self, field_id):
        at = self._indirect(field_id)
        return None if at < 0 else _Table(self.buf, at)

    def string(self, field_id):
        at = self._indirect(field_id)
        if at < 0:
            return None
        length = _read(_U32, self.buf, at)
        _bounds(self.buf, at + 4, length)
        # UnicodeDecodeError subclasses ValueError, so invalid UTF-8 already
        # lands on the caller's rejection path.
        return bytes(self.buf[at + 4 : at + 4 + length]).decode("utf-8")

    def vector(self, field_id, stride=4):
        """`(first element position, count)`; `(-1, 0)` when absent.

        `stride` is the element width: 4 for a vector of offsets, 16 for one of
        the inline `Buffer` and `FieldNode` structs. The whole span is checked
        here, so a count read off a hostile flatbuffer cannot drive a
        four-billion-iteration walk before its first element fails.
        """
        at = self._indirect(field_id)
        if at < 0:
            return -1, 0
        count = _read(_U32, self.buf, at)
        _bounds(self.buf, at + 4, stride * count)
        return at + 4, count


def _vector_table(buf, start, index):
    """Table `index` of a FlatBuffers vector of offsets starting at `start`."""
    at = start + 4 * index
    return _Table(buf, at + _read(_U32, buf, at))


# --------------------------------------------------------------------------
# Stream, segment and message walking.
# --------------------------------------------------------------------------


def _split_segments(buf):
    """Every `(start, length)` in the SACI envelope, framing validated up front.

    `segment* terminator`, where a segment is a u32le length followed by that
    many bytes and the terminator is a u32le zero.

    The whole envelope is walked before any segment is parsed, so a stream that
    is truncated or that carries bytes past its terminator is reported as such
    instead of as whatever the first intact segment happens to contain.
    """
    total = len(buf)
    segments = []
    pos = 0
    while True:
        if pos + 4 > total:
            raise ValueError(
                "arrow ipc: stream is truncated: no zero-length terminator in "
                "{} B".format(total)
            )
        length = _read(_U32, buf, pos)
        pos += 4
        if length == 0:
            if pos != total:
                raise ValueError(
                    "arrow ipc: {} B follow the stream terminator at {}".format(
                        total - pos, pos - 4
                    )
                )
            return segments
        if pos + length > total:
            raise ValueError(
                "arrow ipc: stream is truncated: segment at {} declares {} B "
                "but only {} B remain".format(pos - 4, length, total - pos)
            )
        segments.append((pos, length))
        pos += length


def _iter_messages(buf, start, length):
    """Yield `(header_type, header_table, body_start, body_length)`.

    A segment ends either exactly on its last message body or on one
    end-of-stream marker, which is what `Dataset::write_ipc` emits. Anything
    else inside the declared length is data a reader would silently drop, so it
    is refused instead.
    """
    pos = start
    end = start + length
    while pos < end:
        if pos + 8 > end:
            raise ValueError(
                "arrow ipc: message at {} needs 8 B of framing but the segment "
                "ends at {}".format(pos, end)
            )
        if _read(_U32, buf, pos) != _CONTINUATION:
            raise ValueError(
                "arrow ipc: message at {} lacks the 0xffffffff continuation marker".format(pos)
            )
        metadata_len = _read(_U32, buf, pos + 4)
        if metadata_len == 0:
            if pos + 8 != end:
                raise ValueError(
                    "arrow ipc: segment at {} carries an extra {} B past its "
                    "end-of-stream marker".format(start, end - pos - 8)
                )
            return
        flatbuffer = pos + 8
        if flatbuffer + metadata_len > end:
            raise ValueError(
                "arrow ipc: message at {} declares {} B of metadata but the "
                "segment ends at {}".format(pos, metadata_len, end)
            )
        message = _Table(buf, flatbuffer + _read(_U32, buf, flatbuffer))
        header = message.table(_MSG_HEADER)
        if header is None:
            raise ValueError("arrow ipc: message at {} carries no header table".format(pos))
        body_length = message.scalar(_MSG_BODY_LENGTH, _I64, 0)
        # `metadata_len` already covers the flatbuffer's padding to 8 bytes.
        body_start = flatbuffer + metadata_len
        if body_length < 0 or body_start + body_length > end:
            raise ValueError(
                "arrow ipc: message at {} declares a {} B body that overruns "
                "its segment".format(pos, body_length)
            )
        yield message.scalar(_MSG_HEADER_TYPE, _U8, 0), header, body_start, body_length
        pos = body_start + ((body_length + 7) & ~7)


def _parse_schema(schema):
    """Return `(custom_metadata, [(field name, type_type)])`."""
    fields = []
    start, count = schema.vector(_SCHEMA_FIELDS)
    for i in range(count):
        field = _vector_table(schema.buf, start, i)
        name = field.string(_FIELD_NAME)
        if name is None:
            raise ValueError("arrow ipc: schema field {} has no name".format(i))
        type_type = field.scalar(_FIELD_TYPE_TYPE, _U8, 0)
        if type_type not in _BUFFER_SLOTS:
            raise ValueError(
                "arrow ipc: field {!r} has arrow type id {}, which this codec "
                "does not implement".format(name, type_type)
            )
        fields.append((name, type_type))

    metadata = {}
    start, count = schema.vector(_SCHEMA_CUSTOM_METADATA)
    for i in range(count):
        entry = _vector_table(schema.buf, start, i)
        key = entry.string(_KV_KEY)
        if key is not None:
            metadata[key] = entry.string(_KV_VALUE) or ""
    return metadata, fields


def _parse_record_batch(record_batch, fields, body_start, body_length):
    """Resolve the batch's buffer table into absolute stream positions."""
    if record_batch.has(_RB_COMPRESSION):
        raise ValueError("arrow ipc: compressed record batches are not supported")

    rows = record_batch.scalar(_RB_LENGTH, _I64, 0)
    if rows < 0 or rows > _MAX_ROWS:
        raise ValueError(
            "arrow ipc: record batch declares {} rows, which is not a usable "
            "length".format(rows)
        )

    # One `FieldNode` per schema field, in schema order. A short nodes vector
    # slides every later field onto another field's buffers, so the mismatch is
    # fatal even though this codec reads its values through the buffers alone.
    nodes = record_batch.vector(_RB_NODES, 16)[1]
    if nodes != len(fields):
        raise ValueError(
            "arrow ipc: record batch carries {} field nodes but the schema "
            "declares {} fields".format(nodes, len(fields))
        )

    start, count = record_batch.vector(_RB_BUFFERS, 16)
    buffers = []
    for i in range(count):
        at = start + 16 * i
        offset = _read(_I64, record_batch.buf, at)
        length = _read(_I64, record_batch.buf, at + 8)
        # Buffer offsets are relative to the message body.
        if offset < 0 or length < 0 or offset + length > body_length:
            raise ValueError(
                "arrow ipc: buffer {} spans ({}, {}), overrunning the {} B "
                "message body".format(i, offset, length, body_length)
            )
        buffers.append((body_start + offset, length))

    first_slot = []
    used = 0
    for _name, type_type in fields:
        first_slot.append(used)
        used += _BUFFER_SLOTS[type_type]
    if used != count:
        raise ValueError(
            "arrow ipc: schema needs {} buffer slots but the record batch "
            "carries {}".format(used, count)
        )
    return Batch(record_batch.buf, rows, fields, first_slot, buffers)


def _parse_segment(buf, start, length):
    """Return `(component name, Batch)` for one Arrow IPC stream.

    A message's header type decides what it is, never its position, so a
    segment whose messages are out of order is refused for what it carries
    rather than for where the walk happened to stop.
    """
    fields = None
    metadata = None
    parsed = None
    for header_type, header, body_start, body_length in _iter_messages(buf, start, length):
        if parsed is not None:
            raise ValueError(
                "arrow ipc: segment at {} carries an extra message after its "
                "record batch".format(start)
            )
        if header_type == _HEADER_SCHEMA:
            if fields is not None:
                raise ValueError(
                    "arrow ipc: segment at {} carries an extra schema message".format(start)
                )
            metadata, fields = _parse_schema(header)
        elif header_type == _HEADER_RECORD_BATCH:
            if fields is None:
                raise ValueError("arrow ipc: record batch precedes its schema message")
            name = metadata.get(_COMPONENT_KEY)
            if not name:
                raise ValueError(
                    "arrow ipc: segment at {} has no {} schema metadata".format(
                        start, _COMPONENT_KEY
                    )
                )
            parsed = name, _parse_record_batch(header, fields, body_start, body_length)
        elif header_type == _HEADER_DICTIONARY_BATCH:
            raise ValueError(
                "arrow ipc: dictionary batches are not supported; segment at {} "
                "carries one".format(start)
            )
        else:
            raise ValueError(
                "arrow ipc: message header type {} is not supported (expected "
                "1=Schema or 3=RecordBatch)".format(header_type)
            )
    if parsed is None:
        if fields is None:
            raise ValueError(
                "arrow ipc: segment at {} is empty: it carries no schema message".format(start)
            )
        raise ValueError("arrow ipc: segment at {} has no record batch".format(start))
    return parsed


# --------------------------------------------------------------------------
# Writing: a minimal FlatBuffers builder.
#
# A processor that writes a string, drops a row or adds one cannot patch the
# input's flatbuffers, because every buffer length the RecordBatch message
# declares moves. Encoding a segment needs a writer, and `flatbuffers` has no
# `wasm32-wasi` wheel either, so this is the slice of that library Arrow's two
# message types need.
#
# FlatBuffers are built back to front: a child is written before the parent that
# points at it, and every offset counts backwards from the buffer's end. This
# builder therefore prepends, `offset()` is the distance from the head to that
# end, and the algorithms mirror the reference implementation's `Builder`.
# --------------------------------------------------------------------------


class _Builder:
    """Back-to-front FlatBuffers writer for one message."""

    __slots__ = ("_tail", "_minalign", "_vtable", "_table_end")

    def __init__(self):
        self._tail = bytearray()
        self._minalign = 1
        self._vtable = None
        self._table_end = 0

    # -- primitives --------------------------------------------------------

    def offset(self):
        """Distance from the head to the buffer's end: every offset's origin."""
        return len(self._tail)

    def _prep(self, size, additional):
        """Pad so `additional` bytes, then a `size`-wide field, land aligned."""
        if size > self._minalign:
            self._minalign = size
        pad = -(len(self._tail) + additional) & (size - 1)
        if pad:
            self._tail[0:0] = bytes(pad)

    def _prepend(self, spec, value):
        self._tail[0:0] = spec.pack(value)

    def _scalar(self, spec, value):
        self._prep(spec.size, 0)
        self._prepend(spec, value)

    def element(self, target):
        """One uoffset pointing at an object already written."""
        self._prep(_U32.size, 0)
        self._prepend(_U32, self.offset() + _U32.size - target)

    def struct(self, first, second):
        """One inline 16 B struct of two i64s: `Buffer` and `FieldNode` both."""
        self._prep(8, 16)
        self._prepend(_I64, second)
        self._prepend(_I64, first)

    # -- strings, vectors, tables ------------------------------------------

    def string(self, text):
        raw = text.encode("utf-8")
        # The NUL terminator is not counted by the length prefix, but it is
        # what the reference writer emits and readers rely on.
        self._prep(_U32.size, len(raw) + 1)
        self._tail[0:0] = raw + b"\x00"
        return self.end_vector(len(raw))

    def start_vector(self, width, count, alignment):
        self._prep(_U32.size, width * count)
        self._prep(alignment, width * count)

    def end_vector(self, count):
        self._prepend(_U32, count)
        return self.offset()

    def offset_vector(self, targets):
        """A vector of uoffsets, in `targets` order once read back."""
        self.start_vector(_U32.size, len(targets), _U32.size)
        for target in reversed(targets):
            self.element(target)
        return self.end_vector(len(targets))

    def start_table(self, slots):
        self._vtable = [0] * slots
        self._table_end = self.offset()

    def add_scalar(self, slot, spec, value):
        self._scalar(spec, value)
        self._vtable[slot] = self.offset()

    def add_offset(self, slot, target):
        self.element(target)
        self._vtable[slot] = self.offset()

    def end_table(self):
        """Write the vtable and return the table's own offset.

        A table starts with a signed offset to its vtable, so the slot is
        reserved first and patched once the vtable's position is known. Slots
        left at their default carry a zero entry, and trailing ones are dropped
        entirely: a field id at or beyond `vtable_len` reads as absent.
        """
        self._scalar(_I32, 0)
        table = self.offset()
        vtable = self._vtable
        self._vtable = None
        while vtable and vtable[-1] == 0:
            vtable.pop()
        for value in reversed(vtable):
            self._prepend(_U16, table - value if value else 0)
        self._prepend(_U16, table - self._table_end)
        self._prepend(_U16, 2 * (len(vtable) + 2))
        _I32.pack_into(self._tail, len(self._tail) - table, self.offset() - table)
        return table

    def finish(self, root):
        """The finished flatbuffer: the root uoffset, then every object."""
        self._prep(self._minalign, _U32.size)
        self.element(root)
        return bytes(self._tail)


# --------------------------------------------------------------------------
# Writing: columns, messages, segments.
#
# The public column types live here rather than with the reader, because what
# they are is the writer's input vocabulary.
# --------------------------------------------------------------------------

#: `Message.version`: the metadata version arrow-rs writes and reads. Not a
#: flatbuffers default (that would be V1), so it is always written out.
_METADATA_VERSION_V5 = 4

#: `Int` and `FloatingPoint` payload field ids, and the only values SACI uses:
#: a signed 64-bit integer and an IEEE-754 double.
_INT_BIT_WIDTH = 0
_INT_IS_SIGNED = 1
_INT64_BITS = 64
_FP_PRECISION = 0
_PRECISION_DOUBLE = 2

#: Framing that closes every Arrow IPC stream: a continuation marker followed
#: by a zero-length message.
_END_OF_STREAM = _U32.pack(_CONTINUATION) + _U32.pack(0)


class Int64Column:
    """An `Int64` column: one Python `int` per row, 8 LE bytes each."""

    __slots__ = ("name", "values")

    type_type = TYPE_INT

    def __init__(self, name, values):
        self.name = name
        self.values = values

    def _buffers(self, rows, validity):
        try:
            return validity, struct.pack("<{}q".format(rows), *self.values)
        except struct.error as exc:
            raise ValueError(
                "arrow ipc: column {!r} holds a value an Int64 cannot: {}".format(
                    self.name, exc
                )
            ) from None


class Float64Column:
    """A `FloatingPoint` column of doubles: one per row, 8 LE bytes each."""

    __slots__ = ("name", "values")

    type_type = TYPE_FLOAT

    def __init__(self, name, values):
        self.name = name
        self.values = values

    def _buffers(self, rows, validity):
        try:
            return validity, struct.pack("<{}d".format(rows), *self.values)
        except struct.error as exc:
            raise ValueError(
                "arrow ipc: column {!r} holds a value a Float64 cannot: {}".format(
                    self.name, exc
                )
            ) from None


class BoolColumn:
    """A `Bool` column, bit-packed LSB first: row `i` is bit `i & 7` of byte `i >> 3`."""

    __slots__ = ("name", "values")

    type_type = TYPE_BOOL

    def __init__(self, name, values):
        self.name = name
        self.values = values

    def _buffers(self, rows, validity):
        packed = bytearray((rows + 7) // 8)
        for row, value in enumerate(self.values):
            if value:
                packed[row >> 3] |= 1 << (row & 7)
        return validity, bytes(packed)


class Utf8Column:
    """A `Utf8` column: `rows + 1` i32 LE offsets into one UTF-8 values buffer."""

    __slots__ = ("name", "values")

    type_type = TYPE_UTF8

    def __init__(self, name, values):
        self.name = name
        self.values = values

    def _buffers(self, rows, validity):
        values = bytearray()
        offsets = [0]
        for row, text in enumerate(self.values):
            if not isinstance(text, str):
                raise ValueError(
                    "arrow ipc: column {!r} row {} is {}, and a Utf8 column holds "
                    "str".format(self.name, row, type(text).__name__)
                )
            values += text.encode("utf-8")
            if len(values) > _MAX_ROWS:
                raise ValueError(
                    "arrow ipc: column {!r} needs more than {} B of values, which "
                    "i32 offsets cannot address".format(self.name, _MAX_ROWS)
                )
            offsets.append(len(values))
        return (
            validity,
            struct.pack("<{}i".format(rows + 1), *offsets),
            bytes(values),
        )


def _check_columns(columns):
    """Reject anything that is not a distinctly named column up front."""
    if not columns:
        raise ValueError("arrow ipc: a segment needs at least one column")
    seen = set()
    for column in columns:
        if getattr(column, "type_type", None) not in _BUFFER_SLOTS:
            raise ValueError(
                "arrow ipc: {!r} is not a column; use Int64Column, Float64Column, "
                "BoolColumn or Utf8Column".format(column)
            )
        name = column.name
        if not isinstance(name, str) or not name:
            raise ValueError(
                "arrow ipc: a column name must be a non-empty string, not {!r}".format(name)
            )
        if name in seen:
            raise ValueError("arrow ipc: column {!r} is declared twice".format(name))
        seen.add(name)


def _column_rows(columns):
    """The row count every column agrees on."""
    _check_columns(columns)
    rows = len(columns[0].values)
    for column in columns[1:]:
        if len(column.values) != rows:
            raise ValueError(
                "arrow ipc: column {!r} has {} rows but {!r} has {}".format(
                    column.name, len(column.values), columns[0].name, rows
                )
            )
    if rows > _MAX_ROWS:
        raise ValueError(
            "arrow ipc: {} rows is not a usable length; i32 offsets cap a batch "
            "at {}".format(rows, _MAX_ROWS)
        )
    return rows


def _encode_body(columns, rows):
    """`(body bytes, [(buffer offset, length)])` for one RecordBatch.

    Every span starts on an 8-byte boundary and the body is padded to one, so
    `bodyLength` is exactly the byte count written and the reader's walk to the
    next message lands on its continuation marker.

    Every field is non-nullable with a zero null count, and arrow-rs still emits
    the validity slot for one, so every column shares one all-ones bitmap.
    """
    validity = b"\xff" * ((rows + 7) // 8)
    body = bytearray()
    buffers = []
    for column in columns:
        for raw in column._buffers(rows, validity):
            pad = -len(body) & 7
            if pad:
                body += bytes(pad)
            buffers.append((len(body), len(raw)))
            body += raw
    pad = -len(body) & 7
    if pad:
        body += bytes(pad)
    return bytes(body), buffers


def _type_table(builder, type_type):
    """The `Type` union payload for a column type. `Utf8` and `Bool` carry none."""
    if type_type == TYPE_INT:
        builder.start_table(2)
        builder.add_scalar(_INT_BIT_WIDTH, _I32, _INT64_BITS)
        builder.add_scalar(_INT_IS_SIGNED, _U8, 1)
        return builder.end_table()
    if type_type == TYPE_FLOAT:
        builder.start_table(1)
        builder.add_scalar(_FP_PRECISION, _I16, _PRECISION_DOUBLE)
        return builder.end_table()
    builder.start_table(0)
    return builder.end_table()


def _message(builder, header_type, header, body_length):
    """Wrap a header table in a `Message` and finish the flatbuffer."""
    builder.start_table(4)
    builder.add_scalar(_MSG_VERSION, _I16, _METADATA_VERSION_V5)
    builder.add_scalar(_MSG_HEADER_TYPE, _U8, header_type)
    builder.add_offset(_MSG_HEADER, header)
    if body_length:
        builder.add_scalar(_MSG_BODY_LENGTH, _I64, body_length)
    return builder.finish(builder.end_table())


def _schema_message(columns, metadata):
    """One Schema message: the columns in order, then `metadata` in order.

    `nullable` is left at its `false` default on every field, which is what the
    all-ones validity bitmap and the zero null counts in the batch say too.
    """
    builder = _Builder()
    fields = []
    for column in columns:
        name = builder.string(column.name)
        payload = _type_table(builder, column.type_type)
        builder.start_table(4)
        builder.add_offset(_FIELD_NAME, name)
        builder.add_scalar(_FIELD_TYPE_TYPE, _U8, column.type_type)
        builder.add_offset(_FIELD_TYPE, payload)
        fields.append(builder.end_table())

    entries = []
    for key, value in metadata:
        key_offset = builder.string(key)
        value_offset = builder.string(value)
        builder.start_table(2)
        builder.add_offset(_KV_KEY, key_offset)
        builder.add_offset(_KV_VALUE, value_offset)
        entries.append(builder.end_table())

    field_vector = builder.offset_vector(fields)
    metadata_vector = builder.offset_vector(entries) if entries else 0
    builder.start_table(3)
    builder.add_offset(_SCHEMA_FIELDS, field_vector)
    if metadata_vector:
        builder.add_offset(_SCHEMA_CUSTOM_METADATA, metadata_vector)
    return _message(builder, _HEADER_SCHEMA, builder.end_table(), 0)


def _record_batch_message(columns, rows, buffers, body_length):
    """One RecordBatch message describing an already encoded body."""
    builder = _Builder()
    builder.start_vector(16, len(buffers), 8)
    for offset, length in reversed(buffers):
        builder.struct(offset, length)
    buffer_vector = builder.end_vector(len(buffers))

    builder.start_vector(16, len(columns), 8)
    for _column in columns:
        # `FieldNode`: every row of every column is valid.
        builder.struct(rows, 0)
    node_vector = builder.end_vector(len(columns))

    builder.start_table(3)
    builder.add_scalar(_RB_LENGTH, _I64, rows)
    builder.add_offset(_RB_NODES, node_vector)
    builder.add_offset(_RB_BUFFERS, buffer_vector)
    return _message(builder, _HEADER_RECORD_BATCH, builder.end_table(), body_length)


def _frame(flatbuffer, body=b""):
    """One encapsulated message: framing, padded flatbuffer, then the body."""
    pad = -len(flatbuffer) & 7
    out = bytearray()
    out += _U32.pack(_CONTINUATION)
    # `metadata_len` covers that padding, which is what puts the body on an
    # 8-byte boundary relative to the message's start.
    out += _U32.pack(len(flatbuffer) + pad)
    out += flatbuffer
    out += bytes(pad)
    out += body
    return out


def _encode_segment(name, version, columns):
    """`(rows, segment bytes)`: one component as a standalone Arrow IPC stream.

    `version` is the component's decimal `u32` schema version, or `None` for the
    `__alive` segment, which carries no version.
    """
    rows = _column_rows(columns)
    body, buffers = _encode_body(columns, rows)
    metadata = [(_COMPONENT_KEY, name)]
    if version is not None:
        metadata.append((_SCHEMA_VERSION_KEY, str(version)))
    segment = _frame(_schema_message(columns, metadata))
    segment += _frame(_record_batch_message(columns, rows, buffers, len(body)), body)
    segment += _END_OF_STREAM
    return rows, bytes(segment)


# --------------------------------------------------------------------------
# Public surface.
# --------------------------------------------------------------------------


class Batch:
    """One component's columns, backed by the stream's mutable buffer."""

    __slots__ = ("rows", "_buf", "_names", "_types", "_first_slot", "_index", "_buffers")

    def __init__(self, buf, rows, fields, first_slot, buffers):
        #: Row count declared by the RecordBatch message.
        self.rows = rows
        self._buf = buf
        self._names = [name for name, _type in fields]
        self._types = [type_type for _name, type_type in fields]
        self._first_slot = first_slot
        self._index = {name: i for i, name in enumerate(self._names)}
        self._buffers = buffers

    @property
    def field_names(self):
        return list(self._names)

    # -- lookup helpers ----------------------------------------------------

    def _field(self, name, expected):
        index = self._index.get(name)
        if index is None:
            raise ValueError(
                "arrow ipc: no field named {!r} (have {})".format(
                    name, ", ".join(self._names)
                )
            )
        actual = self._types[index]
        if actual != expected:
            raise ValueError(
                "arrow ipc: field {!r} is {}, not the requested {}".format(
                    name, _TYPE_NAMES[actual], _TYPE_NAMES[expected]
                )
            )
        return index

    def _values(self, name, expected):
        """Values buffer of a fixed-width field: slot 1 of its slot group."""
        index = self._field(name, expected)
        return self._buffers[self._first_slot[index] + 1]

    def _writable(self, name, expected):
        """Like `_values`, but rejects variable-length fields explicitly."""
        index = self._index.get(name)
        if index is not None and self._types[index] == TYPE_UTF8:
            raise ValueError(
                "arrow ipc: {!r} is Utf8; an in-place processor cannot write a "
                "variable-length column".format(name)
            )
        return self._values(name, expected)

    def _require(self, name, have, need):
        if have < need:
            raise ValueError(
                "arrow ipc: field {!r} has a {} B buffer where {} rows need "
                "{} B".format(name, have, self.rows, need)
            )

    def _require_row(self, name, row):
        if row < 0 or row >= self.rows:
            raise ValueError(
                "arrow ipc: field {!r} row {} is outside the batch's {} rows".format(
                    name, row, self.rows
                )
            )

    # -- readers -----------------------------------------------------------

    def int64s(self, field):
        offset, length = self._values(field, TYPE_INT)
        self._require(field, length, 8 * self.rows)
        return list(struct.unpack_from("<{}q".format(self.rows), self._buf, offset))

    def float64s(self, field):
        offset, length = self._values(field, TYPE_FLOAT)
        self._require(field, length, 8 * self.rows)
        return list(struct.unpack_from("<{}d".format(self.rows), self._buf, offset))

    def bools(self, field):
        offset, length = self._values(field, TYPE_BOOL)
        self._require(field, length, (self.rows + 7) // 8)
        buf = self._buf
        # Bit-packed, LSB first.
        return [bool(buf[offset + (i >> 3)] >> (i & 7) & 1) for i in range(self.rows)]

    def strings(self, field):
        index = self._field(field, TYPE_UTF8)
        slot = self._first_slot[index]
        offsets_at, offsets_len = self._buffers[slot + 1]
        values_at, values_len = self._buffers[slot + 2]
        self._require(field, offsets_len, 4 * (self.rows + 1))
        offsets = struct.unpack_from("<{}i".format(self.rows + 1), self._buf, offsets_at)
        out = []
        for row in range(self.rows):
            begin, end = offsets[row], offsets[row + 1]
            if begin < 0 or end < begin or end > values_len:
                raise ValueError(
                    "arrow ipc: field {!r} row {} spans ({}, {}) outside its "
                    "{} B values buffer".format(field, row, begin, end, values_len)
                )
            out.append(bytes(self._buf[values_at + begin : values_at + end]).decode("utf-8"))
        return out

    # -- in-place writers --------------------------------------------------

    def set_int64(self, field, row, value):
        offset, length = self._writable(field, TYPE_INT)
        self._require(field, length, 8 * self.rows)
        self._require_row(field, row)
        value = int(value)
        # `pack_into` raises `struct.error` outside the i64 range, and a caller
        # cannot tell that from a bug in this codec.
        if not -(2**63) <= value < 2**63:
            raise ValueError(
                "arrow ipc: field {!r} cannot hold {}, which is outside the "
                "Int64 range".format(field, value)
            )
        _I64.pack_into(self._buf, offset + 8 * row, value)

    def set_float64(self, field, row, value):
        offset, length = self._writable(field, TYPE_FLOAT)
        self._require(field, length, 8 * self.rows)
        self._require_row(field, row)
        _F64.pack_into(self._buf, offset + 8 * row, float(value))

    def set_bool(self, field, row, value):
        offset, length = self._writable(field, TYPE_BOOL)
        self._require(field, length, (self.rows + 7) // 8)
        self._require_row(field, row)
        at = offset + (row >> 3)
        mask = 1 << (row & 7)
        if value:
            self._buf[at] |= mask
        else:
            self._buf[at] &= 0xFF ^ mask


class SaciStream:
    """A SACI batch envelope: parsed, patched in place, or built from scratch.

    `SaciStream(data)` parses an envelope and owns a mutable copy of it, so a
    `Batch` can patch fixed-width values in place. `SaciStream()` starts empty and
    is filled by `write_component` and `write_alive`.

    Both accept the writes. A parsed stream that is only patched serializes back
    byte for byte; one whose segments are rewritten keeps every segment it was
    not asked to replace exactly as it arrived, patches included.
    """

    __slots__ = ("_buf", "_batches", "_spans", "_written")

    def __init__(self, data=None):
        self._buf = None if data is None else bytearray(data)
        self._batches = {}
        #: Component name -> `(start, length)` of its segment in `_buf`.
        self._spans = {}
        #: Component name -> `(rows, segment bytes)` written over the parse.
        self._written = {}
        if data is None:
            return
        for start, length in _split_segments(self._buf):
            name, batch = _parse_segment(self._buf, start, length)
            self._batches[name] = batch
            self._spans[name] = (start, length)

    @property
    def component_names(self):
        if not self._written:
            return sorted(self._batches)
        return sorted(set(self._batches) | set(self._written))

    def component(self, name):
        """The named component's batch, or `ValueError` if the stream has none."""
        batch = self._batches.get(name)
        if batch is None:
            if name in self._written:
                raise ValueError(
                    "arrow ipc: component {!r} was written, not parsed; parse "
                    "to_bytes() to read it back".format(name)
                )
            raise ValueError(
                "arrow ipc: no segment for component {!r} (have {})".format(
                    name, ", ".join(self.component_names)
                )
            )
        return batch

    # -- writers -----------------------------------------------------------

    def write_component(self, name, version, *columns):
        """Encode one component's segment, replacing any segment of that name.

        `version` is the component's schema version, the `u32` the host reads
        back out of `__saci_schema_version`. Every column must carry the same
        number of rows, and the schema is written in the order given.
        """
        if not isinstance(name, str) or not name:
            raise ValueError(
                "arrow ipc: a component name must be a non-empty string, not "
                "{!r}".format(name)
            )
        if name == ALIVE_COMPONENT:
            raise ValueError(
                "arrow ipc: {!r} is the liveness segment; write it with "
                "write_alive".format(ALIVE_COMPONENT)
            )
        if isinstance(version, bool) or not isinstance(version, int) or not 0 <= version < 2**32:
            raise ValueError(
                "arrow ipc: schema version {!r} is not a u32".format(version)
            )
        self._written[name] = _encode_segment(name, version, columns)

    def write_alive(self, bits):
        """Encode the `__alive` segment: one `Boolean` column named `alive`.

        Its length is the row bound every component in the envelope is checked
        against, so it is written last and must be at least as long as the
        longest component.
        """
        self._written[ALIVE_COMPONENT] = _encode_segment(
            ALIVE_COMPONENT, None, (BoolColumn(ALIVE_FIELD, bits),)
        )

    def to_bytes(self):
        """The whole envelope: every segment, the bitmap, then the terminator."""
        if not self._written:
            if self._buf is None:
                raise ValueError(
                    "arrow ipc: an empty stream cannot be serialized; write a "
                    "component and the __alive bitmap first"
                )
            # Nothing was re-encoded, so the parse plus its in-place patches is
            # the answer, byte for byte.
            return bytes(self._buf)

        alive = self._segment(ALIVE_COMPONENT)
        if alive is None:
            raise ValueError(
                "arrow ipc: stream has no {} segment; call write_alive".format(
                    ALIVE_COMPONENT
                )
            )
        alive_rows, alive_bytes = alive
        names = set(self._batches) | set(self._written)
        names.discard(ALIVE_COMPONENT)

        out = bytearray()
        for name in sorted(names):
            rows, segment = self._segment(name)
            # A component may hold fewer rows than the bitmap; more of them is
            # corruption, and the host refuses such a stream outright.
            if rows > alive_rows:
                raise ValueError(
                    "arrow ipc: component {!r} has {} rows but the {} bitmap has "
                    "{}".format(name, rows, ALIVE_COMPONENT, alive_rows)
                )
            out += _U32.pack(len(segment))
            out += segment
        out += _U32.pack(len(alive_bytes))
        out += alive_bytes
        out += _U32.pack(0)
        return bytes(out)

    def _segment(self, name):
        """`(rows, segment bytes)`: what was written, else what was parsed."""
        written = self._written.get(name)
        if written is not None:
            return written
        span = self._spans.get(name)
        if span is None:
            return None
        start, length = span
        return self._batches[name].rows, bytes(self._buf[start : start + length])


def schema_ipc(*columns):
    """The schema-only Arrow IPC stream a processor declares a component with.

    `component-descriptor.arrow-schema-ipc` is what a `StreamWriter` opened on
    the schema and finished with no batches emits: one Schema message and the
    end-of-stream marker. It carries no `__saci_component` metadata, because the
    host pairs the field list with the name in the descriptor record; the
    segments a processor writes carry their own. Only column names and types are
    read here, so the values may be empty.
    """
    _check_columns(columns)
    return bytes(_frame(_schema_message(columns, ())) + _END_OF_STREAM)


def decode_base64(encoded):
    """Decode standard base64 with padding (RFC 4648 section 4).

    Lives here so a processor that embeds its component schema as a generated
    constant needs one wire-format import and no `base64` of its own;
    `binascii.Error` subclasses `ValueError`, so a corrupt constant lands on the
    same rejection path as a corrupt stream.
    """
    return base64.b64decode(encoded)
