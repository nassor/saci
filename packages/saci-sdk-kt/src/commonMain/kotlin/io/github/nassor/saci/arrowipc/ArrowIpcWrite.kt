// Writes the SACI host to processor wire format from typed columns, using nothing
// but the Kotlin standard library.
//
// [ArrowIpc.kt] reads that format and mutates fixed-width values in place, which
// is all a processor needs as long as it never changes a length. This file is the
// other half: a real FlatBuffers *writer*, so a processor may also write a `Utf8`
// column, drop rows, or emit a component the input did not carry.
//
// The format is the one `ArrowIpc.kt` documents and `crates/saci-core`'s
// `dataset/ipc.rs` produces, down to the field ids this file shares with the
// reader:
//
//   saci_stream := segment* terminator
//   segment    := u32le segment_len ++ arrow_ipc_stream[segment_len]
//   terminator := u32le 0x00000000
//   message    := u32le 0xFFFFFFFF ++ u32le metadata_len
//              ++ flatbuffer[metadata_len] ++ body[bodyLength]
//
// Component segments come first, ordered by component name, then the `__alive`
// bitmap segment, then the terminator. Each component segment is one Schema
// message plus one RecordBatch message plus an end-of-stream marker.
//
// # Two invariants worth naming
//
// `bodyLength` is the *padded* total of the body's buffers, not the sum of their
// declared lengths. arrow-rs's `StreamReader` reads exactly `bodyLength` bytes and
// then expects the next continuation word, so trailing padding has to be inside
// the count rather than skipped after it. Every buffer therefore starts at an
// 8-aligned body offset and the body ends 8-aligned, which also makes the
// reader's `align8(bodyLength)` step a no-op.
//
// A component may hold fewer rows than the `__alive` bitmap — a filtering
// processor emits fewer rows than it read — but never more. [SaciStreamWriter.toBytes]
// refuses that, because the host reads it as corruption rather than as a shrink.

package io.github.nassor.saci.arrowipc

/** The Arrow types a SACI component column may have. */
enum class SaciType { INT64, FLOAT64, BOOL, UTF8 }

/** One field of a component schema: its wire name and its Arrow type. */
data class FieldSpec(val name: String, val type: SaciType)

/**
 * One column of a RecordBatch to be written: a [FieldSpec] plus every row's
 * value.
 *
 * Sealed and typed per Arrow type rather than generic over a boxed value, so a
 * column of `n` rows costs one primitive array and the encode loop never boxes.
 */
sealed class Column(val spec: FieldSpec) {
    /** Rows this column carries; every column of one component must agree. */
    abstract val rows: Int

    val name: String get() = spec.name
}

/** An `Int64` column. */
class Int64Column(name: String, val values: LongArray) : Column(FieldSpec(name, SaciType.INT64)) {
    /** Widens an `Int` column, which is what a row-index or tier field usually is. */
    constructor(name: String, values: IntArray) :
        this(name, LongArray(values.size) { values[it].toLong() })

    override val rows: Int get() = values.size
}

/** A `Float64` column. */
class Float64Column(name: String, val values: DoubleArray) :
    Column(FieldSpec(name, SaciType.FLOAT64)) {
    override val rows: Int get() = values.size
}

/** A `Boolean` column, bit-packed on the way out. */
class BoolColumn(name: String, val values: BooleanArray) : Column(FieldSpec(name, SaciType.BOOL)) {
    override val rows: Int get() = values.size
}

/** A `Utf8` column. */
class Utf8Column(name: String, val values: Array<String>) : Column(FieldSpec(name, SaciType.UTF8)) {
    override val rows: Int get() = values.size
}

// Writer-only FlatBuffers field ids and enum values. The ids the reader also
// uses live in ArrowIpc.kt.

private const val MSG_VERSION_ID = 0

/** `MetadataVersion.V5`, the only version arrow-rs writes or accepts. */
private const val METADATA_VERSION_V5 = 4

private const val FIELD_TYPE_ID = 3

private const val INT_BITWIDTH_ID = 0
private const val INT_SIGNED_ID = 1

private const val FLOAT_PRECISION_ID = 0

/** `Precision.DOUBLE`. */
private const val PRECISION_DOUBLE = 2

/** Holds the component's decimal `u32` schema version; absent on `__alive`. */
private const val SCHEMA_VERSION_KEY = "__saci_schema_version"

/**
 * The trailing liveness segment's `__saci_component` label.
 *
 * Public because a processor that re-encodes rather than mutating in place has
 * to read that segment and write it back, and the label is the only way to
 * address it.
 */
const val SACI_ALIVE_COMPONENT = "__alive"

/** The single non-nullable `Boolean` column the `__alive` segment carries. */
const val SACI_ALIVE_FIELD = "alive"

/** `FieldNode` and `Buffer` are both two `i64`s. */
private const val STRUCT_SIZE = BUFFER_SIZE

private fun putLe32(buf: ByteArray, at: Int, value: Int) {
    for (i in 0..3) buf[at + i] = ((value ushr (i * 8)) and 0xFF).toByte()
}

private fun putLe64(buf: ByteArray, at: Int, value: Long) {
    for (i in 0..7) buf[at + i] = ((value ushr (i * 8)) and 0xFF).toByte()
}

/** The `Field.type_type` discriminant for [type]. */
private fun discriminant(type: SaciType): Int = when (type) {
    SaciType.INT64 -> TYPE_INT
    SaciType.FLOAT64 -> TYPE_FLOAT
    SaciType.BOOL -> TYPE_BOOL
    SaciType.UTF8 -> TYPE_UTF8
}

/** Buffer slots [type] occupies: validity and values, plus offsets for `Utf8`. */
private fun slotsOf(type: SaciType): Int = if (type == SaciType.UTF8) 3 else 2

private fun requireFields(fields: List<FieldSpec>, what: String) {
    if (fields.isEmpty()) fail("$what declares no fields")
    for (i in fields.indices) {
        if (fields[i].name.isEmpty()) fail("$what field $i has no name")
        for (j in 0 until i) {
            if (fields[i].name == fields[j].name) {
                fail("$what declares field \"${fields[i].name}\" twice")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry points.
// ---------------------------------------------------------------------------

/**
 * Builds a SACI wire-format stream out of typed columns.
 *
 * Not [SaciStream]: that type is a parsed stream whose [SaciStream.buf] is aliased
 * by every [Batch] handed out of it, so appending to it would leave those
 * batches pointing at a stale array. A writer owns nothing but finished segment
 * bytes, and [toBytes] is what the two types meet at.
 *
 * Segments are encoded as they are added and concatenated in
 * `__saci_component`-name order by [toBytes], so the order of the calls does not
 * matter.
 */
class SaciStreamWriter {
    private class Segment(val name: String, val rows: Int, val bytes: ByteArray)

    private val components = ArrayList<Segment>()
    private var alive: ByteArray? = null
    private var aliveRows = 0

    /**
     * Encodes one component segment.
     *
     * [version] lands in the schema's `__saci_schema_version` metadata, which is
     * what a peer processor compares against its own registration. Column order
     * is schema order, which is also the buffer-walk order the reader assumes
     * and the order the schema fingerprint is computed over.
     */
    fun writeComponent(name: String, version: UInt, vararg columns: Column): SaciStreamWriter {
        if (name.isEmpty()) fail("a component name must not be empty")
        if (name == SACI_ALIVE_COMPONENT) {
            fail("\"$SACI_ALIVE_COMPONENT\" is written by writeAlive, not by writeComponent")
        }
        if (components.any { it.name == name }) fail("component \"$name\" is already written")
        if (columns.isEmpty()) fail("component \"$name\" declares no columns")

        val rows = columns[0].rows
        for (column in columns) {
            if (column.rows != rows) {
                fail(
                    "component \"$name\" column \"${column.name}\" holds ${column.rows} rows, " +
                        "\"${columns[0].name}\" holds $rows"
                )
            }
        }
        requireFields(columns.map { it.spec }, "component \"$name\"")

        components.add(Segment(name, rows, segment(name, version, columns, rows)))
        return this
    }

    /**
     * Encodes the trailing `__alive` segment: one non-nullable `Boolean` column
     * named `alive`, and no schema version.
     *
     * Its bit count is the stream's row bound, so this is also what every
     * component's row count is checked against.
     */
    fun writeAlive(bits: BooleanArray): SaciStreamWriter {
        if (alive != null) fail("the \"$SACI_ALIVE_COMPONENT\" segment is already written")
        aliveRows = bits.size
        alive = segment(SACI_ALIVE_COMPONENT, null, arrayOf(BoolColumn(SACI_ALIVE_FIELD, bits)), bits.size)
        return this
    }

    /** The finished stream: component segments by name, `__alive`, terminator. */
    fun toBytes(): ByteArray {
        val aliveSegment = alive
            ?: fail("a SACI stream must carry an \"$SACI_ALIVE_COMPONENT\" segment; call writeAlive")

        val ordered = components.sortedBy { it.name }
        for (segment in ordered) {
            if (segment.rows > aliveRows) {
                fail(
                    "component \"${segment.name}\" holds ${segment.rows} rows, more than the " +
                        "$aliveRows bits of \"$SACI_ALIVE_COMPONENT\""
                )
            }
        }

        var total = 4L
        for (segment in ordered) total += 4L + segment.bytes.size
        total += 4L + aliveSegment.size
        if (total > Int.MAX_VALUE) fail("the stream would be $total bytes, which is not addressable")

        val out = ByteArray(total.toInt())
        var at = 0
        for (segment in ordered) at = putSegment(out, at, segment.bytes)
        at = putSegment(out, at, aliveSegment)
        // The terminator is a zero segment length, already zero in `out`.
        return out
    }

    /** The finished stream in the shape `run-result.output` wants. */
    @OptIn(ExperimentalUnsignedTypes::class)
    fun toWit(): List<UByte> = toBytes().asUByteArray().asList()

    private fun putSegment(out: ByteArray, at: Int, bytes: ByteArray): Int {
        putLe32(out, at, bytes.size)
        bytes.copyInto(out, at + 4)
        return at + 4 + bytes.size
    }
}

/**
 * The schema-only Arrow IPC stream `component-descriptor.arrow-schema-ipc` holds:
 * one Schema message and an end-of-stream marker, with no batches.
 *
 * No `custom_metadata`: the host reads this with `StreamReader::schema()` to
 * build its template dataset, and a `__saci_component` entry would describe a
 * segment that is not there. These bytes are therefore not a wire schema and
 * must never be spliced into a stream.
 */
fun saciSchemaStream(fields: List<FieldSpec>): ByteArray {
    requireFields(fields, "schema")
    val writer = FbWriter()
    val schema = writer.schemaTable(fields, emptyList())
    val metadata = writer.finish(writer.messageTable(HEADER_SCHEMA, schema, 0L))

    val padded = align8(metadata.size)
    val out = ByteArray(8 + padded + 8)
    putMessage(out, 0, metadata, padded, null)
    putLe32(out, 8 + padded, CONTINUATION.toInt())
    // metadata_len 0 in the next word, already zero, ends the stream.
    return out
}

// ---------------------------------------------------------------------------
// Segment and message assembly.
// ---------------------------------------------------------------------------

/**
 * One component segment: Schema message, RecordBatch message, end-of-stream
 * marker. [version] is null for `__alive`, which carries no schema version.
 */
private fun segment(
    name: String,
    version: UInt?,
    columns: Array<out Column>,
    rows: Int,
): ByteArray {
    val metadata = ArrayList<Pair<String, String>>(2)
    metadata.add(COMPONENT_KEY to name)
    if (version != null) metadata.add(SCHEMA_VERSION_KEY to version.toString())

    val schemaWriter = FbWriter()
    val schemaTable = schemaWriter.schemaTable(columns.map { it.spec }, metadata)
    val schemaFb = schemaWriter.finish(
        schemaWriter.messageTable(HEADER_SCHEMA, schemaTable, 0L)
    )

    val body = body(name, columns, rows)
    val batchWriter = FbWriter()
    val batchTable = batchWriter.recordBatchTable(rows, columns.size, body)
    val batchFb = batchWriter.finish(
        batchWriter.messageTable(HEADER_RECORD_BATCH, batchTable, body.bytes.size.toLong())
    )

    val schemaMeta = align8(schemaFb.size)
    val batchMeta = align8(batchFb.size)
    val out = ByteArray(8 + schemaMeta + 8 + batchMeta + body.bytes.size + 8)

    var at = putMessage(out, 0, schemaFb, schemaMeta, null)
    at = putMessage(out, at, batchFb, batchMeta, body.bytes)
    putLe32(out, at, CONTINUATION.toInt())
    // metadata_len 0 in the following word, already zero, ends the segment.
    return out
}

/**
 * Frames one message into [out] and returns the offset of the next one.
 *
 * [padded] is `metadata_len`, the flatbuffer's length rounded up to 8 so the
 * body starts 8-aligned after the 8-byte prefix. [body] is already 8-aligned in
 * length, so no padding follows it.
 */
private fun putMessage(
    out: ByteArray,
    at: Int,
    metadata: ByteArray,
    padded: Int,
    body: ByteArray?,
): Int {
    putLe32(out, at, CONTINUATION.toInt())
    putLe32(out, at + 4, padded)
    metadata.copyInto(out, at + 8)
    // The metadata padding is already zero in `out`.
    val bodyAt = at + 8 + padded
    if (body == null) return bodyAt
    body.copyInto(out, bodyAt)
    return bodyAt + body.size
}

/** A RecordBatch body plus the `Buffer{offset,length}` entries that describe it. */
private class Body(val bytes: ByteArray, val offsets: LongArray, val lengths: LongArray)

/**
 * Lays out and fills one RecordBatch body.
 *
 * Two passes: the first sizes every buffer slot and 8-aligns its offset, the
 * second writes values into the single allocation that produces. Padding bytes
 * and the `Bool` bits past the row count are left at the zero a fresh
 * `ByteArray` already holds.
 */
private fun body(component: String, columns: Array<out Column>, rows: Int): Body {
    val slots = columns.sumOf { slotsOf(it.spec.type) }
    val offsets = LongArray(slots)
    val lengths = LongArray(slots)
    val validityLen = (rows + 7) / 8

    // Kept from the sizing pass: the values buffer length is the encoded byte
    // count, so encoding twice would be the alternative.
    val encoded = arrayOfNulls<Array<ByteArray>>(columns.size)

    var slot = 0
    var end = 0L
    fun place(len: Long) {
        if (len < 0 || end + len > Int.MAX_VALUE) {
            fail("component \"$component\" needs a body larger than ${Int.MAX_VALUE} bytes")
        }
        offsets[slot] = end
        lengths[slot] = len
        slot++
        end = align8((end + len).toInt()).toLong()
    }

    for ((index, column) in columns.withIndex()) {
        place(validityLen.toLong())
        when (column) {
            is Int64Column, is Float64Column -> place(rows.toLong() * 8)
            is BoolColumn -> place(validityLen.toLong())
            is Utf8Column -> {
                val bytes = Array(rows) { column.values[it].encodeToByteArray() }
                encoded[index] = bytes
                place((rows.toLong() + 1) * 4)
                place(bytes.sumOf { it.size.toLong() })
            }
        }
    }

    val out = ByteArray(end.toInt())
    slot = 0
    for ((index, column) in columns.withIndex()) {
        // arrow-rs emits an all-ones validity bitmap even for a non-nullable
        // field, and the reader's buffer walk counts on the slot being there.
        val validityAt = offsets[slot++].toInt()
        for (byte in 0 until validityLen) out[validityAt + byte] = 0xFF.toByte()

        when (column) {
            is Int64Column -> {
                val at = offsets[slot++].toInt()
                for (row in 0 until rows) putLe64(out, at + row * 8, column.values[row])
            }

            is Float64Column -> {
                val at = offsets[slot++].toInt()
                for (row in 0 until rows) {
                    putLe64(out, at + row * 8, column.values[row].toRawBits())
                }
            }

            is BoolColumn -> {
                val at = offsets[slot++].toInt()
                for (row in 0 until rows) {
                    if (!column.values[row]) continue
                    val byte = at + row / 8
                    out[byte] = (out[byte].toInt() or (1 shl (row and 7))).toByte()
                }
            }

            is Utf8Column -> {
                val offsetsAt = offsets[slot++].toInt()
                val valuesAt = offsets[slot++].toInt()
                val bytes = encoded[index]!!
                var cursor = 0
                for (row in 0 until rows) {
                    putLe32(out, offsetsAt + row * 4, cursor)
                    bytes[row].copyInto(out, valuesAt + cursor)
                    cursor += bytes[row].size
                }
                putLe32(out, offsetsAt + rows * 4, cursor)
            }
        }
    }

    return Body(out, offsets, lengths)
}

// ---------------------------------------------------------------------------
// Arrow metadata tables.
// ---------------------------------------------------------------------------

/** `Message`, wrapping a Schema or RecordBatch header. */
private fun FbWriter.messageTable(headerType: Int, header: Int, bodyLength: Long): Int {
    startTable(5)
    addI16(MSG_VERSION_ID, METADATA_VERSION_V5, 0)
    addU8(MSG_HEADER_TYPE, headerType, 0)
    addChild(MSG_HEADER, header)
    addI64(MSG_BODY_LENGTH, bodyLength, 0L)
    return endTable()
}

/**
 * `Schema`, with `endianness` left at its `Little` default.
 *
 * [metadata] becomes `custom_metadata`, which is where `__saci_component` and
 * `__saci_schema_version` live; an empty list omits the vector entirely.
 */
private fun FbWriter.schemaTable(
    fields: List<FieldSpec>,
    metadata: List<Pair<String, String>>,
): Int {
    val fieldTables = IntArray(fields.size) { fieldTable(fields[it]) }
    val fieldsVector = offsetVector(fieldTables)

    var metadataVector = 0
    if (metadata.isNotEmpty()) {
        val entries = IntArray(metadata.size) { keyValue(metadata[it].first, metadata[it].second) }
        metadataVector = offsetVector(entries)
    }

    startTable(3)
    addChild(SCHEMA_FIELDS_ID, fieldsVector)
    addChild(SCHEMA_METADATA_ID, metadataVector)
    return endTable()
}

/**
 * `Field`.
 *
 * `nullable` is left absent, which FlatBuffers reads as its `false` default:
 * every SACI column is non-nullable, and the all-ones validity bitmap the body
 * carries says the same thing a second time.
 */
private fun FbWriter.fieldTable(spec: FieldSpec): Int {
    val name = string(spec.name)
    val type = typeTable(spec.type)
    startTable(7)
    addChild(FIELD_NAME_ID, name)
    addU8(FIELD_TYPE_TYPE_ID, discriminant(spec.type), 0)
    addChild(FIELD_TYPE_ID, type)
    return endTable()
}

/** The `Field.type` payload table for [type]; `Utf8` and `Bool` are empty. */
private fun FbWriter.typeTable(type: SaciType): Int = when (type) {
    SaciType.INT64 -> {
        startTable(2)
        addI32(INT_BITWIDTH_ID, 64, 0)
        addBool(INT_SIGNED_ID, true, false)
        endTable()
    }

    SaciType.FLOAT64 -> {
        startTable(1)
        addI16(FLOAT_PRECISION_ID, PRECISION_DOUBLE, 0)
        endTable()
    }

    SaciType.BOOL, SaciType.UTF8 -> {
        startTable(0)
        endTable()
    }
}

private fun FbWriter.keyValue(key: String, value: String): Int {
    val keyOffset = string(key)
    val valueOffset = string(value)
    startTable(2)
    addChild(KV_KEY_ID, keyOffset)
    addChild(KV_VALUE_ID, valueOffset)
    return endTable()
}

/**
 * `RecordBatch`, with `compression` absent.
 *
 * Every field is non-nullable with an all-ones validity bitmap, so every
 * `FieldNode.null_count` is 0. Struct fields are written last to first, which is
 * how FlatBuffers lays an inline struct out.
 */
private fun FbWriter.recordBatchTable(rows: Int, fields: Int, body: Body): Int {
    val nodes = structVector(fields) {
        putLong(0L)
        putLong(rows.toLong())
    }
    val buffers = structVector(body.offsets.size) { index ->
        putLong(body.lengths[index])
        putLong(body.offsets[index])
    }

    startTable(3)
    addI64(BATCH_LENGTH_ID, rows.toLong(), 0L)
    addChild(BATCH_NODES_ID, nodes)
    addChild(BATCH_BUFFERS_ID, buffers)
    return endTable()
}

// ---------------------------------------------------------------------------
// FlatBuffers writer: just enough for the tables above.
// ---------------------------------------------------------------------------

/**
 * Encodes one FlatBuffers buffer back to front, the way the reference
 * implementations do: children are finished before their parents, and a table's
 * vtable is written after its fields.
 *
 * No vtable deduplication. Arrow metadata messages are small and written once
 * per batch, and a duplicate vtable costs bytes rather than correctness.
 */
private class FbWriter(initial: Int = 1024) {
    private var buf = ByteArray(initial)

    /** Unused bytes at the front of [buf]; the encoding grows downwards. */
    private var space = initial

    /** The largest scalar written, which is what the root offset is aligned to. */
    private var minAlign = 1

    private var vtable = IntArray(8)
    private var vtableFields = 0
    private var tableStart = 0
    private var open = false
    private var vectorElements = 0

    /** Bytes encoded so far, which is also every offset's frame of reference. */
    private val offset: Int get() = buf.size - space

    fun finish(root: Int): ByteArray {
        if (open) fail("flatbuffer finished with an object still open")
        prep(minAlign, 4)
        addOffset(root)
        return buf.copyOfRange(space, buf.size)
    }

    // Tables.

    fun startTable(fields: Int) {
        if (open) fail("flatbuffer object started inside another one")
        if (vtable.size < fields) vtable = IntArray(fields)
        vtable.fill(0, 0, fields)
        vtableFields = fields
        tableStart = offset
        open = true
    }

    fun endTable(): Int {
        if (!open) fail("flatbuffer table ended without a start")
        // Placeholder for the signed offset back to the vtable, patched below
        // once the vtable's position is known.
        addInt(0)
        val tableLocation = offset

        var field = vtableFields - 1
        while (field >= 0 && vtable[field] == 0) field--
        val used = field + 1
        while (field >= 0) {
            addShort(if (vtable[field] != 0) tableLocation - vtable[field] else 0)
            field--
        }
        addShort(tableLocation - tableStart)
        // The two u16 header slots plus one per field the vtable still declares.
        addShort((used + 2) * 2)

        putLe32At(buf.size - tableLocation, offset - tableLocation)
        open = false
        return tableLocation
    }

    fun addBool(id: Int, value: Boolean, default: Boolean) {
        if (value != default) {
            addByte(if (value) 1 else 0)
            slot(id)
        }
    }

    fun addU8(id: Int, value: Int, default: Int) {
        if (value != default) {
            addByte(value)
            slot(id)
        }
    }

    fun addI16(id: Int, value: Int, default: Int) {
        if (value != default) {
            prep(2, 0)
            putShortAt(value)
            slot(id)
        }
    }

    fun addI32(id: Int, value: Int, default: Int) {
        if (value != default) {
            addInt(value)
            slot(id)
        }
    }

    fun addI64(id: Int, value: Long, default: Long) {
        if (value != default) {
            prep(8, 0)
            putLong(value)
            slot(id)
        }
    }

    /** Adds a uoffset field; offset 0 means the child is absent. */
    fun addChild(id: Int, target: Int) {
        if (target != 0) {
            addOffset(target)
            slot(id)
        }
    }

    // Strings and vectors.

    /** A null-terminated FlatBuffers string, returning its offset. */
    fun string(text: String): Int {
        val bytes = text.encodeToByteArray()
        addByte(0)
        startVector(1, bytes.size, 1)
        space -= bytes.size
        bytes.copyInto(buf, space)
        return endVector()
    }

    /** A vector of uoffsets to [targets], in [targets] order. */
    fun offsetVector(targets: IntArray): Int {
        startVector(4, targets.size, 4)
        for (index in targets.indices.reversed()) addOffset(targets[index])
        return endVector()
    }

    /**
     * A vector of [count] inline two-`i64` structs. [write] emits one element,
     * last declared field first, and is called from the last index down so the
     * finished vector reads in index order.
     */
    fun structVector(count: Int, write: (Int) -> Unit): Int {
        startVector(STRUCT_SIZE, count, 8)
        for (index in count - 1 downTo 0) write(index)
        return endVector()
    }

    /** Writes an `i64` with no alignment prep; only valid inside a struct vector. */
    fun putLong(value: Long) {
        space -= 8
        putLe64(buf, space, value)
    }

    private fun startVector(elementSize: Int, elements: Int, alignment: Int) {
        if (open) fail("flatbuffer object started inside another one")
        vectorElements = elements
        prep(4, elementSize * elements)
        prep(alignment, elementSize * elements)
        open = true
    }

    private fun endVector(): Int {
        // The element count's four bytes were reserved by `startVector`.
        space -= 4
        putLe32(buf, space, vectorElements)
        open = false
        return offset
    }

    // Scalars.

    private fun addByte(value: Int) {
        prep(1, 0)
        buf[--space] = value.toByte()
    }

    private fun addShort(value: Int) {
        prep(2, 0)
        putShortAt(value)
    }

    private fun addInt(value: Int) {
        prep(4, 0)
        space -= 4
        putLe32(buf, space, value)
    }

    private fun addOffset(target: Int) {
        prep(4, 0)
        // Read after `prep`, which may have padded, and before the write moves
        // the head: a uoffset is relative to the slot that holds it.
        val value = offset - target + 4
        space -= 4
        putLe32(buf, space, value)
    }

    private fun putShortAt(value: Int) {
        space -= 2
        buf[space] = (value and 0xFF).toByte()
        buf[space + 1] = ((value shr 8) and 0xFF).toByte()
    }

    private fun putLe32At(at: Int, value: Int) = putLe32(buf, at, value)

    /** Records that field [id] was written at the current offset. */
    private fun slot(id: Int) {
        vtable[id] = offset
    }

    /**
     * Aligns the write head to [size] and makes room for [size] + [additional]
     * more bytes.
     */
    private fun prep(size: Int, additional: Int) {
        if (size > minAlign) minAlign = size
        val padding = (-(offset + additional)) and (size - 1)
        grow(padding + size + additional)
        for (i in 0 until padding) buf[--space] = 0
    }

    private fun grow(want: Int) {
        while (space < want) {
            val old = buf.size
            if (old > Int.MAX_VALUE / 2) fail("flatbuffer exceeds the addressable size")
            val next = ByteArray(old * 2)
            // The encoding lives at the tail, so it moves to the new tail and
            // the freed space appears at the front.
            buf.copyInto(next, old, 0, old)
            space += old
            buf = next
        }
    }
}
