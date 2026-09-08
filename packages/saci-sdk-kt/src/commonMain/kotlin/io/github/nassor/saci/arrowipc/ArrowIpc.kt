// Reads and mutates the SACI host to processor wire format using nothing but the
// Kotlin standard library.
//
// Wire format, with examples/polyglot/generated/fixture_input.saci as the
// reference stream:
//
//   saci_stream := segment* terminator
//   segment    := u32le segment_len ++ arrow_ipc_stream[segment_len]
//   terminator := u32le 0x00000000
//   message    := u32le 0xFFFFFFFF ++ u32le metadata_len
//              ++ flatbuffer[metadata_len] ++ body[bodyLength]
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
// standard library is needed for. The setters therefore accept fixed-width
// fields only: changing a Utf8 value would shift every following offset and
// force a rewrite of the RecordBatch metadata.
//
// The trailing `__alive` segment is never parsed and never touched: the host
// marks every row of a batch alive, and a processor that can neither add nor remove
// rows cannot change that. Those bytes pass through byte identical, as does
// every flatbuffer and every framing word.
//
// This file lives in `commonMain`, so the same source compiles for the
// `wasmWasi` processor and for the JVM test that decodes a real fixture. Failures
// are [ArrowIpcException], never an index out of bounds: the bytes arrive from
// outside the component, and the export layer needs a message to put in
// `run-error::permanent`.

package io.github.nassor.saci.arrowipc

/** Every failure this codec raises, with a message the processor can report. */
class ArrowIpcException(message: String) : Exception(message)

internal fun fail(message: String): Nothing = throw ArrowIpcException(message)

// Framing and Arrow discriminants. These are `internal` rather than file
// private because `ArrowIpcWrite.kt` encodes exactly what this file decodes: one
// definition of each field id, shared by the reader and the writer.

/** Prefixes every IPC message; a metadata length of 0 after it ends the stream. */
internal const val CONTINUATION = 0xFFFFFFFFL

internal const val HEADER_SCHEMA = 1
private const val HEADER_DICTIONARY = 2
internal const val HEADER_RECORD_BATCH = 3

/** `Field.type_type` values this codec understands. Anything else is rejected. */
internal const val TYPE_INT = 2
internal const val TYPE_FLOAT = 3
internal const val TYPE_UTF8 = 5
internal const val TYPE_BOOL = 6

/** Inline FlatBuffers struct sizes: `FieldNode{i64,i64}`, `Buffer{i64,i64}`. */
private const val FIELD_NODE_SIZE = 16
internal const val BUFFER_SIZE = 16

/**
 * Names the Schema `custom_metadata` entry the host writes to label a segment.
 * A segment without it is not addressable, so its absence is an error rather
 * than a skip.
 */
internal const val COMPONENT_KEY = "__saci_component"

// FlatBuffers vtable field ids, from Arrow's Message.fbs and Schema.fbs. A union
// occupies two consecutive slots (discriminant, then payload), which is where
// Field.type_type = 2 comes from.

internal const val MSG_HEADER_TYPE = 1
internal const val MSG_HEADER = 2
internal const val MSG_BODY_LENGTH = 3

internal const val SCHEMA_FIELDS_ID = 1
internal const val SCHEMA_METADATA_ID = 2

internal const val FIELD_NAME_ID = 0
internal const val FIELD_TYPE_TYPE_ID = 2

internal const val BATCH_LENGTH_ID = 0
internal const val BATCH_NODES_ID = 1
internal const val BATCH_BUFFERS_ID = 2
private const val BATCH_COMPRESSION_ID = 3

internal const val KV_KEY_ID = 0
internal const val KV_VALUE_ID = 1

internal fun align8(n: Int): Int = (n + 7) and 7.inv()

/**
 * Narrows an on-the-wire length to `Int`, rejecting the values that would
 * otherwise turn into an out-of-range index.
 */
private fun asLength(value: Long, what: String): Int {
    if (value < 0 || value > Int.MAX_VALUE) fail("$what is $value, which is not a usable length")
    return value.toInt()
}

private fun ByteArray.u32At(at: Int): Long =
    (this[at].toLong() and 0xFF) or
        ((this[at + 1].toLong() and 0xFF) shl 8) or
        ((this[at + 2].toLong() and 0xFF) shl 16) or
        ((this[at + 3].toLong() and 0xFF) shl 24)

private fun ByteArray.i64At(at: Int): Long {
    var acc = 0L
    for (i in 7 downTo 0) acc = (acc shl 8) or (this[at + i].toLong() and 0xFF)
    return acc
}

private fun ByteArray.putI64At(at: Int, value: Long) {
    for (i in 0..7) this[at + i] = ((value ushr (i * 8)) and 0xFF).toByte()
}

private const val BASE64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

private val base64Reverse = IntArray(128) { -1 }.also { table ->
    BASE64_ALPHABET.forEachIndexed { index, ch -> table[ch.code] = index }
}

/**
 * Standard base64 with `=` padding (RFC 4648 section 4), decode only.
 *
 * Hand rolled so a processor depends on nothing outside the language, the same
 * reason the rest of this file exists. A processor that embeds its component schema
 * as a generated constant runs this once, at load.
 */
fun decodeBase64(text: String): ByteArray {
    // RFC 4648 pads to a multiple of four. Without that the output length
    // computed below is short and the loop writes past the array.
    if (text.length % 4 != 0) {
        fail("base64 input is ${text.length} characters, which is not a multiple of four")
    }
    val out = ByteArray(text.length / 4 * 3)
    var written = 0
    var acc = 0
    var bits = 0
    for (ch in text) {
        if (ch == '=') break
        val value = if (ch.code < 128) base64Reverse[ch.code] else -1
        if (value < 0) fail("base64 input holds the invalid character '$ch'")
        acc = (acc shl 6) or value
        bits += 6
        if (bits >= 8) {
            bits -= 8
            out[written++] = ((acc shr bits) and 0xFF).toByte()
        }
    }
    return out.copyOf(written)
}

// ---------------------------------------------------------------------------
// Stream: segment framing.
// ---------------------------------------------------------------------------

/** One Arrow buffer, resolved to absolute offsets in [SaciStream.buf]. */
internal class Span(val off: Int, val len: Int)

internal val EMPTY_SPAN = Span(0, 0)

/**
 * A schema field paired with the buffers the RecordBatch assigned it.
 *
 * The validity span is resolved but unused: arrow-rs emits an all-ones validity
 * bitmap for a non-nullable field, and an in-place value write therefore never
 * has to touch it.
 */
internal class Field(val name: String, val type: Int) {
    var validity: Span = EMPTY_SPAN
    var offsets: Span = EMPTY_SPAN
    var values: Span = EMPTY_SPAN
}

/** One framed Arrow IPC message inside a segment. */
private class Message(
    /** False for the end-of-stream marker. */
    val present: Boolean,
    val root: FbTable?,
    val headerType: Int,
    /** Absolute offset of the message body in [SaciStream.buf]. */
    val body: Int,
    val bodyLen: Int,
    /** Absolute offset of the following message. */
    val next: Int,
)

private val END_OF_STREAM = Message(false, null, 0, 0, 0, 0)

/**
 * A parsed SACI wire-format stream.
 *
 * [buf] is the processor-owned mutable copy of the input: every setter writes into
 * it, and it is what the processor hands back to the host as `run-result.output`.
 */
class SaciStream private constructor(val buf: ByteArray, private val segments: List<Segment>) {
    private class Segment(val start: Int, val end: Int)

    companion object {
        /**
         * Splits [input] into segments.
         *
         * The input is copied. The list the generated export glue hands a processor
         * is built from memory pinned only for the duration of the call, so
         * owning a copy is what makes in-place mutation and returning the buffer
         * safe.
         */
        fun parse(input: ByteArray): SaciStream {
            val buf = input.copyOf()
            val segments = ArrayList<Segment>()
            var pos = 0
            while (true) {
                if (buf.size - pos < 4) {
                    fail("truncated stream: no segment length at offset $pos of ${buf.size} bytes")
                }
                // Held as a Long and compared against what is left: a u32
                // segment length can exceed Int.MAX_VALUE, and adding one to an
                // offset would wrap into a negative index instead of failing.
                val segLen = buf.u32At(pos)
                pos += 4
                if (segLen == 0L) break
                if (segLen > (buf.size - pos).toLong()) {
                    fail(
                        "truncated stream: segment at offset ${pos - 4} declares " +
                            "$segLen bytes, ${buf.size - pos} remain"
                    )
                }
                val end = pos + segLen.toInt()
                segments.add(Segment(pos, end))
                pos = end
            }
            if (segments.isEmpty()) fail("stream declares no segments")
            if (pos != buf.size) fail("${buf.size - pos} bytes trail the stream terminator")
            return SaciStream(buf, segments)
        }

        /** Parses the WIT `list<u8>` the export glue delivers. */
        fun parse(input: List<UByte>): SaciStream =
            parse(ByteArray(input.size) { input[it].toByte() })
    }

    /** The mutated buffer, in the shape `run-result.output` wants. */
    @OptIn(ExperimentalUnsignedTypes::class)
    fun toWit(): List<UByte> = buf.asUByteArray().asList()

    /** Component labels of every segment, in stream order. */
    fun componentNames(): List<String> = segments.indices.map { componentAt(it).second }

    /**
     * The batch of the segment whose Schema metadata declares [name].
     */
    fun component(name: String): Batch {
        for (index in segments.indices) {
            val (schema, declared) = componentAt(index)
            if (declared != name) continue
            val header = requireHeader(schema, "segment $index schema")
            return batch(segments[index], schema, header, name)
        }
        fail("no segment declares component \"$name\"")
    }

    /** Parses one segment's Schema message and reads its component label. */
    private fun componentAt(index: Int): Pair<Message, String> {
        val seg = segments[index]
        val schema = message(seg.start, seg.end)
        if (!schema.present) fail("segment $index is empty")
        if (schema.headerType != HEADER_SCHEMA) {
            fail(
                "segment $index opens with header_type ${schema.headerType}, " +
                    "want $HEADER_SCHEMA (Schema)"
            )
        }
        val header = requireHeader(schema, "segment $index schema")
        return schema to componentOf(header, index)
    }

    private fun requireHeader(msg: Message, what: String): FbTable =
        msg.root?.child(MSG_HEADER) ?: fail("$what message carries no header")

    /** Reads the `__saci_component` label out of a Schema's `custom_metadata`. */
    private fun componentOf(schema: FbTable, index: Int): String {
        val meta = schema.vector(SCHEMA_METADATA_ID)
            ?: fail("segment $index schema has no custom_metadata, so no \"$COMPONENT_KEY\" label")
        for (i in 0 until meta.count) {
            val kv = meta.table(i)
            if (kv.str(KV_KEY_ID) != COMPONENT_KEY) continue
            return kv.str(KV_VALUE_ID) ?: fail("$COMPONENT_KEY metadata entry has no value")
        }
        fail("segment $index schema custom_metadata has no \"$COMPONENT_KEY\" key")
    }

    private fun message(pos: Int, limit: Int): Message {
        if (limit - pos < 8) fail("truncated message prefix at offset $pos")
        if (buf.u32At(pos) != CONTINUATION) {
            fail("offset $pos is not an IPC message: continuation marker missing")
        }
        // Same reason as the segment length: a u32 metadata length is compared
        // against the room left, never added to an offset first.
        val declared = buf.u32At(pos + 4)
        if (declared == 0L) return END_OF_STREAM
        val room = limit - pos - 8
        if (declared > room.toLong()) {
            fail("message at offset $pos declares $declared metadata bytes, $room remain")
        }
        val metaLen = declared.toInt()

        val fb = FbBuf(buf, pos + 8, metaLen)
        val root = fb.root()
        val headerType = root.u8(MSG_HEADER_TYPE, 0)
        val body = pos + 8 + metaLen
        val bodyLen = asLength(root.i64(MSG_BODY_LENGTH, 0), "bodyLength")
        if (bodyLen > limit - body) {
            fail(
                "message at offset $pos declares a $bodyLen-byte body, " +
                    "${limit - body} remain"
            )
        }
        return Message(true, root, headerType, body, bodyLen, body + align8(bodyLen))
    }

    private fun batch(seg: Segment, schema: Message, header: FbTable, name: String): Batch {
        val fields = schemaFieldsOf(header)

        val rb = message(schema.next, seg.end)
        if (!rb.present) fail("segment ends after its schema, with no record batch")
        if (rb.headerType != HEADER_RECORD_BATCH) {
            if (rb.headerType == HEADER_DICTIONARY) {
                fail("segment carries a dictionary batch, which this codec does not support")
            }
            fail(
                "second message has header_type ${rb.headerType}, " +
                    "want $HEADER_RECORD_BATCH (RecordBatch)"
            )
        }
        val rbHeader = requireHeader(rb, "record batch")
        // Body compression would make every value offset below meaningless.
        if (rbHeader.has(BATCH_COMPRESSION_ID)) {
            fail("record batch body is compressed, which this codec does not support")
        }

        // A segment is exactly Schema, RecordBatch, end-of-stream marker, and
        // the declared segment length covers those three exactly. Anything past
        // the record batch is data this codec would otherwise drop in silence.
        var tail = rb.next
        // A tail too short to hold a marker is named separately: there is no
        // marker in that case, so reporting bytes after one would describe
        // something that is not there.
        if (tail < seg.end && seg.end - tail < 8) {
            fail(
                "segment carries ${seg.end - tail} bytes after its record batch, " +
                    "too few for an end-of-stream marker, " +
                    "want one Schema and one RecordBatch"
            )
        }
        if (tail < seg.end) {
            val trailer = message(tail, seg.end)
            if (trailer.present) {
                fail(
                    "segment carries a third message with header_type " +
                        "${trailer.headerType}, want one Schema and one RecordBatch"
                )
            }
            tail += 8
        }
        if (tail != seg.end) {
            fail(
                "segment carries ${seg.end - tail} bytes after its end-of-stream " +
                    "marker, want one Schema and one RecordBatch"
            )
        }

        val rows = asLength(rbHeader.i64(BATCH_LENGTH_ID, 0), "record batch length")

        val nodes = rbHeader.vector(BATCH_NODES_ID)
        if (nodes == null || nodes.count != fields.size) {
            fail(
                "record batch has ${nodes?.count ?: 0} field nodes " +
                    "for ${fields.size} schema fields"
            )
        }
        if (nodes.count > 0) nodes.inline(nodes.count - 1, FIELD_NODE_SIZE)
        val buffers = rbHeader.vector(BATCH_BUFFERS_ID)
            ?: fail("record batch carries no buffers vector")

        // Buffer slots are assigned by walking the schema in field order; the
        // slot count is fixed by type_type, never inferred from a length.
        var next = 0
        fun take(fieldName: String): Span {
            if (next >= buffers.count) {
                fail("field \"$fieldName\" needs buffer slot $next, record batch has ${buffers.count}")
            }
            return buffers.buffer(next++, rb.body, rb.bodyLen)
        }
        for (field in fields) {
            field.validity = take(field.name)
            when (field.type) {
                TYPE_INT, TYPE_FLOAT, TYPE_BOOL -> {}
                TYPE_UTF8 -> field.offsets = take(field.name)
                else -> fail("field \"${field.name}\" has unsupported type_type ${field.type}")
            }
            field.values = take(field.name)
        }
        if (next != buffers.count) {
            fail("schema consumes $next buffer slots, record batch carries ${buffers.count}")
        }

        return Batch(rows, name, buf, fields)
    }

    /**
     * Field names and type discriminants in schema order, which is also
     * buffer-walk order.
     */
    private fun schemaFieldsOf(schema: FbTable): List<Field> {
        val vec = schema.vector(SCHEMA_FIELDS_ID) ?: fail("schema carries no fields vector")
        return (0 until vec.count).map { i ->
            val table = vec.table(i)
            val name = table.str(FIELD_NAME_ID) ?: fail("schema field $i has no name")
            Field(name, table.u8(FIELD_TYPE_TYPE_ID, 0))
        }
    }
}

// ---------------------------------------------------------------------------
// Batch: columns of one component segment.
// ---------------------------------------------------------------------------

/** The RecordBatch of one component segment, addressable by field name. */
class Batch internal constructor(
    /** The RecordBatch row count. */
    val rows: Int,
    private val component: String,
    /** Aliases [SaciStream.buf], so setter writes land in the stream. */
    private val buf: ByteArray,
    private val fields: List<Field>,
) {
    /** Field names in schema order. */
    fun fieldNames(): List<String> = fields.map { it.name }

    /** The schema position of [name]. */
    fun fieldIndex(name: String): Int {
        val index = fields.indexOfFirst { it.name == name }
        if (index < 0) fail("component \"$component\" has no field \"$name\"")
        return index
    }

    /** Decodes an `Int64` column. */
    fun int64s(name: String): LongArray {
        val field = reader(name, TYPE_INT, 8)
        return LongArray(rows) { buf.i64At(field.values.off + it * 8) }
    }

    /** Decodes a `Float64` column. */
    fun float64s(name: String): DoubleArray {
        val field = reader(name, TYPE_FLOAT, 8)
        return DoubleArray(rows) { Double.fromBits(buf.i64At(field.values.off + it * 8)) }
    }

    /** Decodes a `Boolean` column from its LSB-first bitmap. */
    fun bools(name: String): BooleanArray {
        val field = field(name, TYPE_BOOL)
        needBits(field)
        return BooleanArray(rows) {
            (buf[field.values.off + it / 8].toInt() shr (it and 7)) and 1 == 1
        }
    }

    /** Decodes a `Utf8` column through its i32 offsets buffer. */
    fun strings(name: String): List<String> {
        val field = field(name, TYPE_UTF8)
        // Widened: `rows` is bounded only by the check it feeds, so the product
        // must not wrap before it is compared.
        val need = (rows.toLong() + 1) * 4
        if (field.offsets.len < need) {
            fail(
                "field \"$name\" offsets buffer holds ${field.offsets.len} bytes, " +
                    "need $need for $rows rows"
            )
        }
        return (0 until rows).map { row ->
            val start = buf.u32At(field.offsets.off + row * 4).toInt()
            val end = buf.u32At(field.offsets.off + (row + 1) * 4).toInt()
            if (start < 0 || end < start || end > field.values.len) {
                fail(
                    "field \"$name\" row $row offsets [$start,$end) escape its " +
                        "${field.values.len}-byte values buffer"
                )
            }
            buf.decodeToString(field.values.off + start, field.values.off + end)
        }
    }

    /**
     * Overwrites one `Int64` value in place.
     *
     * `review_tier` is the schema's only `Int64` output, so this is the one
     * setter the C# stage needs and the only integer write the example makes.
     */
    fun setInt64(name: String, row: Int, value: Long) {
        val field = writer(name, TYPE_INT, row, 8)
        buf.putI64At(field.values.off + row * 8, value)
    }

    /** Overwrites one `Float64` value in place. */
    fun setFloat64(name: String, row: Int, value: Double) {
        val field = writer(name, TYPE_FLOAT, row, 8)
        buf.putI64At(field.values.off + row * 8, value.toRawBits())
    }

    /** Overwrites one bit of a `Boolean` column's bitmap in place. */
    fun setBool(name: String, row: Int, value: Boolean) {
        val field = field(name, TYPE_BOOL)
        checkRow(name, row)
        needBits(field)
        val mask = 1 shl (row and 7)
        val at = field.values.off + row / 8
        val current = buf[at].toInt() and 0xFF
        buf[at] = (if (value) current or mask else current and mask.inv()).toByte()
    }

    /** Resolves a name and checks its Arrow type. */
    private fun field(name: String, want: Int): Field {
        val field = fields[fieldIndex(name)]
        if (field.type != want) {
            fail("field \"$name\" is ${typeName(field.type)}, not ${typeName(want)}")
        }
        return field
    }

    /**
     * [field] plus a check that the values buffer covers every row.
     */
    private fun reader(name: String, want: Int, width: Int): Field {
        val field = field(name, want)
        // Widened for the same reason as the offsets check in `strings`.
        val need = rows.toLong() * width
        if (field.values.len < need) {
            fail(
                "field \"$name\" values buffer holds ${field.values.len} bytes, " +
                    "need $need for $rows rows"
            )
        }
        return field
    }

    /**
     * [reader] plus a row bound and the variable-length refusal.
     *
     * A Utf8 write would move every following offset, so it is rejected by type
     * before the type-mismatch message, which would otherwise read as if a
     * fixed-width column of that name were merely missing.
     */
    private fun writer(name: String, want: Int, row: Int, width: Int): Field {
        val type = fields[fieldIndex(name)].type
        if (type != TYPE_INT && type != TYPE_FLOAT && type != TYPE_BOOL) {
            fail(
                "field \"$name\" is ${typeName(type)}: this codec writes fixed-width " +
                    "values only, because a variable-length write would have to rebuild " +
                    "the offsets buffer and the RecordBatch metadata"
            )
        }
        checkRow(name, row)
        return reader(name, want, width)
    }

    private fun checkRow(name: String, row: Int) {
        if (row < 0 || row >= rows) {
            fail("row $row is out of range for field \"$name\" of $rows rows")
        }
    }

    private fun needBits(field: Field) {
        // Widened for the same reason as the offsets check in `strings`.
        val need = (rows.toLong() + 7) / 8
        if (field.values.len < need) {
            fail(
                "field \"${field.name}\" bitmap holds ${field.values.len} bytes, " +
                    "need $need for $rows rows"
            )
        }
    }
}

private fun typeName(type: Int): String = when (type) {
    TYPE_INT -> "Int"
    TYPE_FLOAT -> "FloatingPoint"
    TYPE_UTF8 -> "Utf8"
    TYPE_BOOL -> "Bool"
    else -> "type_type $type"
}

// ---------------------------------------------------------------------------
// FlatBuffers reader: just enough for Arrow's Message, Schema, Field,
// RecordBatch and KeyValue tables.
// ---------------------------------------------------------------------------

/**
 * One FlatBuffers-encoded Arrow metadata message, as a window into the stream
 * buffer. Every read is bounds checked: these bytes come from outside the
 * component.
 */
private class FbBuf(val buf: ByteArray, val base: Int, val size: Int) {
    fun bounds(off: Int, n: Int) {
        if (off < 0 || n < 0 || off > size - n) {
            fail("read of $n bytes at $off exceeds $size-byte metadata")
        }
    }

    fun u8(off: Int): Int {
        bounds(off, 1)
        return buf[base + off].toInt() and 0xFF
    }

    fun u16(off: Int): Int {
        bounds(off, 2)
        return (buf[base + off].toInt() and 0xFF) or ((buf[base + off + 1].toInt() and 0xFF) shl 8)
    }

    fun u32(off: Int): Long {
        bounds(off, 4)
        return buf.u32At(base + off)
    }

    fun i64(off: Int): Long {
        bounds(off, 8)
        return buf.i64At(base + off)
    }

    fun string(off: Int, n: Int): String {
        bounds(off, n)
        return buf.decodeToString(base + off, base + off + n)
    }

    /** Follows the buffer's leading uoffset to the root table. */
    fun root(): FbTable = table(asLength(u32(0), "root offset"))

    /** Reads the table header at [pos]: a signed offset back to its vtable. */
    fun table(pos: Int): FbTable {
        val soff = u32(pos).toInt()
        val vt = pos - soff
        bounds(vt, 4)
        val vtLen = u16(vt)
        if (vtLen < 4) fail("table at $pos has a $vtLen-byte vtable")
        bounds(vt, vtLen)
        return FbTable(this, pos, vt, vtLen)
    }
}

private class FbTable(val fb: FbBuf, val pos: Int, val vt: Int, val vtLen: Int) {
    /**
     * The field's offset from the table position, or 0 for an absent field.
     * FlatBuffers encodes absence as a zero vtable entry or as a vtable too
     * short to hold the id. The vtable bounds were checked in `table`, so this
     * cannot fail.
     */
    fun slot(id: Int): Int {
        val off = 4 + id * 2
        if (off + 2 > vtLen) return 0
        return fb.u16(vt + off)
    }

    fun has(id: Int): Boolean = slot(id) != 0

    fun u8(id: Int, default: Int): Int {
        val slot = slot(id)
        return if (slot == 0) default else fb.u8(pos + slot)
    }

    fun i64(id: Int, default: Long): Long {
        val slot = slot(id)
        return if (slot == 0) default else fb.i64(pos + slot)
    }

    /** Resolves a uoffset field to the table it points at. */
    fun child(id: Int): FbTable? {
        val slot = slot(id)
        if (slot == 0) return null
        val at = pos + slot
        return fb.table(at + asLength(fb.u32(at), "child offset"))
    }

    fun str(id: Int): String? {
        val slot = slot(id)
        if (slot == 0) return null
        val at = pos + slot
        val head = at + asLength(fb.u32(at), "string offset")
        val n = asLength(fb.u32(head), "string length")
        return fb.string(head + 4, n)
    }

    fun vector(id: Int): FbVector? {
        val slot = slot(id)
        if (slot == 0) return null
        val at = pos + slot
        val head = at + asLength(fb.u32(at), "vector offset")
        val count = asLength(fb.u32(head), "vector length")
        return FbVector(fb, head + 4, count)
    }
}

private class FbVector(val fb: FbBuf, val start: Int, val count: Int) {
    /** Resolves element [i] of a vector of tables. */
    fun table(i: Int): FbTable {
        val at = start + i * 4
        return fb.table(at + asLength(fb.u32(at), "vector element offset"))
    }

    /** The position of inline struct element [i]. */
    fun inline(i: Int, size: Int): Int {
        if (i < 0 || i >= count) {
            fail("element $i is out of range for a $count-element vector")
        }
        val at = start + i * size
        fb.bounds(at, size)
        return at
    }

    /**
     * Reads inline `Buffer{i64 offset, i64 length}` element [i] and resolves it
     * against the message body. `Buffer.offset` is body relative.
     */
    fun buffer(i: Int, body: Int, bodyLen: Int): Span {
        val at = inline(i, BUFFER_SIZE)
        val off = fb.i64(at)
        val len = fb.i64(at + 8)
        if (off < 0 || len < 0 || off > bodyLen || len > bodyLen - off) {
            fail("buffer $i spans [$off,${off + len}) of a $bodyLen-byte body")
        }
        return Span(body + off.toInt(), len.toInt())
    }
}
