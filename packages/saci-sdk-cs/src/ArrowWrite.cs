// Writes the SACI host<->processor wire format that ArrowIpc.cs reads.
//
// What this adds over in-place mutation
//
// ArrowIpc.cs can overwrite a fixed-width value slot because that is a byte write
// into an existing body. It cannot write a string: a different length moves every
// following offset, the values buffer length, and the `Buffer` entries inside the
// RecordBatch flatbuffer. This file encodes both flatbuffers from scratch, so a
// processor can rewrite any column, drop rows, or emit a component the input
// never carried.
//
// The encoder targets exactly what arrow-rs `StreamWriter` with
// `IpcWriteOptions::default()` produces, because the host reads these bytes with
// arrow-rs: MetadataVersion V5, 8-byte alignment, no compression, one Schema
// message and one RecordBatch message per segment followed by an end-of-stream
// marker. `docs/content/library/reference/wire-format.md` is the specification; the field
// ids and type discriminants come from the same constants the reader uses, so the
// two halves cannot drift.
//
// Non-nullable everywhere
//
// Every field is written with `nullable` absent, which FlatBuffers reads as false,
// and every FieldNode carries `null_count: 0`. The validity slot is still emitted
// with a real all-ones bitmap, because arrow-rs emits one and the slot count per
// type is fixed rather than inferred.
//
// FlatBuffers by hand
//
// FlatBuilder below is the back-to-front builder the format requires: children
// before parents, tables closed before the table that points at them. It does not
// deduplicate vtables, which is legal and costs a few bytes per message. Only the
// seven Arrow metadata tables this format uses are reachable from it.

using System.Buffers.Binary;
using System.Globalization;
using System.Text;

namespace Saci.ArrowIpc;

// ---------------------------------------------------------------------------
// Columns: the values one component segment is built from.
// ---------------------------------------------------------------------------

/// <summary>One column of a component segment: a field name, an Arrow type, and
/// the values for every row.</summary>
/// <remarks>Column order is the schema: the fields vector is written in the order
/// the columns are passed to <see cref="SaciStream.WriteComponent"/>, and that is
/// also the order the buffer walk assigns slots in.</remarks>
public abstract class Column
{
    /// <summary>Not public: the four concrete columns below are the whole set the
    /// wire format has types for.</summary>
    internal Column(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        Name = name;
    }

    /// <summary>The field name written into the schema.</summary>
    public string Name { get; }

    /// <summary>The row count. Every column of a component must agree.</summary>
    public abstract int Length { get; }

    /// <summary>The `Field.type_type` discriminant.</summary>
    internal abstract byte ArrowType { get; }

    /// <summary>Buffer slots after the validity bitmap: one for a fixed-width
    /// type, two for Utf8.</summary>
    internal abstract int PayloadCount { get; }

    internal abstract int PayloadLength(int payload);

    internal abstract void WritePayload(int payload, Span<byte> dest);

    /// <summary>Total buffer slots, validity included.</summary>
    internal int Slots => 1 + PayloadCount;

    internal int SlotLength(int slot) =>
        slot == 0 ? (Length + 7) / 8 : PayloadLength(slot - 1);

    internal void WriteSlot(int slot, Span<byte> dest)
    {
        if (slot != 0)
        {
            WritePayload(slot - 1, dest);
            return;
        }
        // All-ones validity, trailing bits of the last byte left clear. Every row
        // is valid, and `null_count: 0` means no reader consults these bytes.
        dest.Fill(0xFF);
        int tail = Length & 7;
        if (tail != 0)
        {
            dest[^1] = (byte)((1 << tail) - 1);
        }
    }
}

/// <summary>An Int64 column: 8 bytes little-endian per row.</summary>
public sealed class Int64Column(string name, long[] values) : Column(name)
{
    private readonly long[] _values = values ?? throw new ArgumentNullException(nameof(values));

    public override int Length => _values.Length;

    internal override byte ArrowType => ArrowIpc.TypeInt;

    internal override int PayloadCount => 1;

    internal override int PayloadLength(int payload) => _values.Length * 8;

    internal override void WritePayload(int payload, Span<byte> dest)
    {
        for (int i = 0; i < _values.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(i * 8, 8), _values[i]);
        }
    }
}

/// <summary>A Float64 column: IEEE-754, 8 bytes little-endian per row.</summary>
public sealed class Float64Column(string name, double[] values) : Column(name)
{
    private readonly double[] _values = values ?? throw new ArgumentNullException(nameof(values));

    public override int Length => _values.Length;

    internal override byte ArrowType => ArrowIpc.TypeFloat;

    internal override int PayloadCount => 1;

    internal override int PayloadLength(int payload) => _values.Length * 8;

    internal override void WritePayload(int payload, Span<byte> dest)
    {
        for (int i = 0; i < _values.Length; i++)
        {
            BinaryPrimitives.WriteDoubleLittleEndian(dest.Slice(i * 8, 8), _values[i]);
        }
    }
}

/// <summary>A Boolean column: bit-packed LSB-first, ceil(rows/8) bytes.</summary>
public sealed class BoolColumn(string name, bool[] values) : Column(name)
{
    private readonly bool[] _values = values ?? throw new ArgumentNullException(nameof(values));

    public override int Length => _values.Length;

    internal override byte ArrowType => ArrowIpc.TypeBool;

    internal override int PayloadCount => 1;

    internal override int PayloadLength(int payload) => (_values.Length + 7) / 8;

    internal override void WritePayload(int payload, Span<byte> dest)
    {
        dest.Clear();
        for (int i = 0; i < _values.Length; i++)
        {
            if (_values[i])
            {
                dest[i >> 3] |= (byte)(1 << (i & 7));
            }
        }
    }
}

/// <summary>A Utf8 column: rows+1 i32 little-endian offsets, then the UTF-8
/// bytes.</summary>
public sealed class Utf8Column : Column
{
    private readonly string[] _values;

    /// <summary>Monotonic i32 offsets, rows+1 of them, computed once so the body
    /// plan and the body write agree without encoding the strings twice.</summary>
    private readonly int[] _offsets;

    public Utf8Column(string name, string[] values) : base(name)
    {
        ArgumentNullException.ThrowIfNull(values);
        _values = values;
        _offsets = new int[values.Length + 1];
        int at = 0;
        for (int i = 0; i < values.Length; i++)
        {
            string text = values[i]
                ?? throw new ArrowIpcException(
                    $"field \"{name}\" row {i} is null, and a Utf8 column is non-nullable");
            at = checked(at + Encoding.UTF8.GetByteCount(text));
            _offsets[i + 1] = at;
        }
    }

    public override int Length => _values.Length;

    internal override byte ArrowType => ArrowIpc.TypeUtf8;

    internal override int PayloadCount => 2;

    internal override int PayloadLength(int payload) =>
        payload == 0 ? _offsets.Length * 4 : _offsets[^1];

    internal override void WritePayload(int payload, Span<byte> dest)
    {
        if (payload == 0)
        {
            for (int i = 0; i < _offsets.Length; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(i * 4, 4), _offsets[i]);
            }
            return;
        }
        for (int i = 0; i < _values.Length; i++)
        {
            int start = _offsets[i];
            Encoding.UTF8.GetBytes(_values[i], dest[start.._offsets[i + 1]]);
        }
    }
}

// ---------------------------------------------------------------------------
// Segment encoder.
// ---------------------------------------------------------------------------

/// <summary>Encodes one length-prefixed segment, or the schema-only stream a
/// processor reports in its descriptor.</summary>
internal static class ArrowEncode
{
    /// <summary>MetadataVersion.V5, the only version this format uses.</summary>
    private const short MetadataV5 = 4;

    /// <summary>Precision.DOUBLE.</summary>
    private const short PrecisionDouble = 2;

    private const int Int64BitWidth = 64;

    // Vtable slot counts, from Arrow's Message.fbs and Schema.fbs. A table may
    // leave trailing slots empty but must never claim fewer than the ids it fills.
    private const int MessageFields = 5;
    private const int SchemaFields = 4;
    private const int FieldFields = 7;
    private const int RecordBatchFields = 5;
    private const int KeyValueFields = 2;
    private const int IntFields = 2;
    private const int FloatFields = 1;

    // The field ids the reader does not need, so ArrowIpc does not declare them.
    private const int MsgVersionId = 0;
    private const int FieldTypeId = 3;
    private const int IntBitWidthId = 0;
    private const int IntSignedId = 1;
    private const int FloatPrecisionId = 0;

    /// <summary>Encodes `u32le segment_len ++ Schema ++ RecordBatch ++
    /// end-of-stream`.</summary>
    /// <param name="component">The `__saci_component` label.</param>
    /// <param name="version">The `__saci_schema_version` value, or null to omit the
    /// entry as the `__alive` segment does.</param>
    /// <param name="columns">Fields in schema order.</param>
    /// <param name="rows">The row count every column agreed on.</param>
    internal static byte[] Segment(string component, uint? version, Column[] columns, out int rows)
    {
        rows = RequireOneShape(component, columns);

        // Body plan first: the RecordBatch flatbuffer cannot be encoded before
        // every buffer's offset and length is known, and bodyLength is the padded
        // total, which is what Message.bodyLength must report.
        int slots = 0;
        for (int i = 0; i < columns.Length; i++)
        {
            slots += columns[i].Slots;
        }
        long[] offsets = new long[slots];
        long[] lengths = new long[slots];
        int bodyLength = 0;
        int slot = 0;
        for (int i = 0; i < columns.Length; i++)
        {
            Column column = columns[i];
            for (int s = 0; s < column.Slots; s++)
            {
                int length = column.SlotLength(s);
                offsets[slot] = bodyLength;
                lengths[slot] = length;
                // Every buffer starts 8-byte aligned inside the body.
                bodyLength = ArrowIpc.Align8(checked(bodyLength + length));
                slot++;
            }
        }

        byte[] schemaFb = SchemaMessage(component, version, columns);
        byte[] batchFb = BatchMessage(rows, columns.Length, offsets, lengths, bodyLength);
        int schemaMeta = ArrowIpc.Align8(schemaFb.Length);
        int batchMeta = ArrowIpc.Align8(batchFb.Length);

        // Two message prefixes, both flatbuffers padded to 8, the body, and the
        // eight-byte end-of-stream marker.
        int segmentLength = checked(8 + schemaMeta + 8 + batchMeta + bodyLength + 8);
        byte[] output = new byte[checked(4 + segmentLength)];
        BinaryPrimitives.WriteUInt32LittleEndian(output.AsSpan(0, 4), (uint)segmentLength);

        int at = PutMessage(output, 4, schemaFb, schemaMeta);
        int body = PutMessage(output, at, batchFb, batchMeta);
        slot = 0;
        for (int i = 0; i < columns.Length; i++)
        {
            Column column = columns[i];
            for (int s = 0; s < column.Slots; s++)
            {
                column.WriteSlot(s, output.AsSpan(body + (int)offsets[slot], (int)lengths[slot]));
                slot++;
            }
        }

        // The marker is a continuation word plus a zero metadata length; the
        // trailing zeros are already there.
        BinaryPrimitives.WriteUInt32LittleEndian(
            output.AsSpan(body + bodyLength, 4), ArrowIpc.Continuation);
        return output;
    }

    /// <summary>Encodes the schema-only stream `component-descriptor.arrow-schema-ipc`
    /// carries: one Schema message with no SACI metadata, then end-of-stream.</summary>
    internal static byte[] SchemaStream(Column[] columns)
    {
        byte[] fb = SchemaMessage(component: null, version: null, columns);
        int metaLen = ArrowIpc.Align8(fb.Length);
        byte[] output = new byte[8 + metaLen + 8];
        PutMessage(output, 0, fb, metaLen);
        BinaryPrimitives.WriteUInt32LittleEndian(output.AsSpan(8 + metaLen, 4), ArrowIpc.Continuation);
        return output;
    }

    /// <summary>Writes one message prefix and its flatbuffer, and returns the
    /// offset of the body.</summary>
    /// <remarks><paramref name="metaLen"/> already includes the flatbuffer's
    /// padding to 8 bytes, so the body starts 8-byte aligned. The padding bytes
    /// are left as the zeros the array came with.</remarks>
    private static int PutMessage(byte[] dest, int at, byte[] fb, int metaLen)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(dest.AsSpan(at, 4), ArrowIpc.Continuation);
        BinaryPrimitives.WriteUInt32LittleEndian(dest.AsSpan(at + 4, 4), (uint)metaLen);
        fb.CopyTo(dest.AsSpan(at + 8, fb.Length));
        return at + 8 + metaLen;
    }

    /// <summary>Rejects a column set that cannot be one RecordBatch.</summary>
    private static int RequireOneShape(string component, Column[] columns)
    {
        if (columns.Length == 0)
        {
            throw new ArrowIpcException(
                $"component \"{component}\" has no columns, and the fields vector is the schema");
        }
        int rows = columns[0].Length;
        for (int i = 0; i < columns.Length; i++)
        {
            if (columns[i].Length != rows)
            {
                throw new ArrowIpcException(
                    $"component \"{component}\" column \"{columns[i].Name}\" holds "
                    + $"{columns[i].Length} rows, \"{columns[0].Name}\" holds {rows}");
            }
            for (int j = 0; j < i; j++)
            {
                if (columns[j].Name == columns[i].Name)
                {
                    throw new ArrowIpcException(
                        $"component \"{component}\" declares field \"{columns[i].Name}\" twice");
                }
            }
        }
        return rows;
    }

    // -----------------------------------------------------------------------
    // Arrow metadata tables.
    // -----------------------------------------------------------------------

    private static byte[] SchemaMessage(string? component, uint? version, Column[] columns)
    {
        FlatBuilder fb = new();
        int schema = SchemaTable(fb, columns, component, version);
        fb.StartTable(MessageFields);
        fb.AddShort(MsgVersionId, MetadataV5);
        fb.AddByte(ArrowIpc.MsgHeaderType, ArrowIpc.HeaderSchema);
        fb.AddOffset(ArrowIpc.MsgHeader, schema);
        fb.AddLong(ArrowIpc.MsgBodyLength, 0);
        return fb.Finish(fb.EndTable());
    }

    private static byte[] BatchMessage(
        int rows, int nodes, long[] offsets, long[] lengths, long bodyLength)
    {
        FlatBuilder fb = new();

        // Inline struct vectors, elements written back to front like every other
        // FlatBuffers vector.
        fb.StartVector(ArrowIpc.FieldNodeSize, nodes, 8);
        for (int i = nodes - 1; i >= 0; i--)
        {
            fb.PutStruct(rows, 0);
        }
        int nodesVec = fb.EndVector();

        fb.StartVector(ArrowIpc.BufferSize, offsets.Length, 8);
        for (int i = offsets.Length - 1; i >= 0; i--)
        {
            fb.PutStruct(offsets[i], lengths[i]);
        }
        int buffersVec = fb.EndVector();

        fb.StartTable(RecordBatchFields);
        fb.AddLong(ArrowIpc.BatchLengthId, rows);
        fb.AddOffset(ArrowIpc.BatchNodesId, nodesVec);
        fb.AddOffset(ArrowIpc.BatchBuffersId, buffersVec);
        // `compression` left absent: a present entry is what the reader rejects.
        int batch = fb.EndTable();

        fb.StartTable(MessageFields);
        fb.AddShort(MsgVersionId, MetadataV5);
        fb.AddByte(ArrowIpc.MsgHeaderType, ArrowIpc.HeaderRecordBatch);
        fb.AddOffset(ArrowIpc.MsgHeader, batch);
        fb.AddLong(ArrowIpc.MsgBodyLength, bodyLength);
        return fb.Finish(fb.EndTable());
    }

    /// <summary>Builds the Schema table: the fields vector in column order, plus
    /// the SACI custom_metadata unless this is a descriptor schema.</summary>
    private static int SchemaTable(
        FlatBuilder fb, Column[] columns, string? component, uint? version)
    {
        int[] fields = new int[columns.Length];
        for (int i = 0; i < columns.Length; i++)
        {
            // Children before parents: the name string and the type table must be
            // complete before the Field table that points at them opens.
            int name = fb.CreateString(columns[i].Name);
            int type = TypeTable(fb, columns[i].ArrowType);
            fb.StartTable(FieldFields);
            fb.AddOffset(ArrowIpc.FieldNameId, name);
            // `nullable` left absent, which reads as false.
            fb.AddByte(ArrowIpc.FieldTypeTypeId, columns[i].ArrowType);
            fb.AddOffset(FieldTypeId, type);
            fields[i] = fb.EndTable();
        }
        int fieldsVec = fb.CreateOffsetVector(fields);

        int metadataVec = 0;
        if (component is not null)
        {
            metadataVec = version is null
                ? MetadataVector(fb, [(ArrowIpc.ComponentKey, component)])
                : MetadataVector(fb, [
                    (ArrowIpc.ComponentKey, component),
                    (ArrowIpc.SchemaVersionKey, version.Value.ToString(CultureInfo.InvariantCulture)),
                ]);
        }

        fb.StartTable(SchemaFields);
        // `endianness` left absent, which reads as Little.
        fb.AddOffset(ArrowIpc.SchemaFieldsId, fieldsVec);
        if (metadataVec != 0)
        {
            fb.AddOffset(ArrowIpc.SchemaMetadataId, metadataVec);
        }
        return fb.EndTable();
    }

    /// <summary>The Type union payload for one discriminant. Utf8 and Bool are
    /// empty tables; the union slot still has to point at one.</summary>
    private static int TypeTable(FlatBuilder fb, byte type)
    {
        switch (type)
        {
            case ArrowIpc.TypeInt:
                fb.StartTable(IntFields);
                fb.AddInt(IntBitWidthId, Int64BitWidth);
                fb.AddBool(IntSignedId, true);
                return fb.EndTable();
            case ArrowIpc.TypeFloat:
                fb.StartTable(FloatFields);
                fb.AddShort(FloatPrecisionId, PrecisionDouble);
                return fb.EndTable();
            default:
                fb.StartTable(0);
                return fb.EndTable();
        }
    }

    private static int MetadataVector(FlatBuilder fb, (string Key, string Value)[] entries)
    {
        int[] offsets = new int[entries.Length];
        for (int i = 0; i < entries.Length; i++)
        {
            int key = fb.CreateString(entries[i].Key);
            int value = fb.CreateString(entries[i].Value);
            fb.StartTable(KeyValueFields);
            fb.AddOffset(ArrowIpc.KvKeyId, key);
            fb.AddOffset(ArrowIpc.KvValueId, value);
            offsets[i] = fb.EndTable();
        }
        return fb.CreateOffsetVector(offsets);
    }
}

// ---------------------------------------------------------------------------
// FlatBuffers writer: the seven Arrow metadata tables and nothing more.
// ---------------------------------------------------------------------------

/// <summary>Builds one FlatBuffers buffer back to front.</summary>
/// <remarks>Offsets are counted from the end of the buffer, which is what keeps
/// them stable while the buffer grows toward the front. A caller must close every
/// nested object before opening the one that references it, and must not create a
/// string, vector or table while a table is open.</remarks>
internal sealed class FlatBuilder
{
    private byte[] _buf;

    /// <summary>Bytes still free at the front. Data occupies `[_space, Length)`.</summary>
    private int _space;

    /// <summary>The largest alignment any field asked for, which the finished
    /// buffer's start has to satisfy.</summary>
    private int _minAlign = 1;

    /// <summary>Field offsets of the open table, indexed by field id.</summary>
    private int[] _vtable = [];

    private int _vtableSize;
    private int _objectStart;
    private int _vectorElements;

    internal FlatBuilder(int capacity = 512)
    {
        _buf = new byte[capacity];
        _space = capacity;
    }

    /// <summary>Bytes written so far, counted from the end of the buffer.</summary>
    private int Offset => _buf.Length - _space;

    /// <summary>Writes the root offset and returns the finished buffer.</summary>
    internal byte[] Finish(int root)
    {
        Prep(_minAlign, 4);
        AddOffset(root);
        byte[] result = new byte[Offset];
        Array.Copy(_buf, _space, result, 0, result.Length);
        return result;
    }

    // -----------------------------------------------------------------------
    // Tables.
    // -----------------------------------------------------------------------

    /// <summary>Opens a table with <paramref name="fields"/> vtable slots.</summary>
    internal void StartTable(int fields)
    {
        if (_vtable.Length < fields)
        {
            _vtable = new int[fields];
        }
        Array.Clear(_vtable, 0, fields);
        _vtableSize = fields;
        _objectStart = Offset;
    }

    /// <summary>Closes the open table and returns its offset.</summary>
    internal int EndTable()
    {
        AddInt(0); // placeholder for the soffset back to the vtable
        int table = Offset;
        for (int id = _vtableSize - 1; id >= 0; id--)
        {
            AddShort((short)(_vtable[id] != 0 ? table - _vtable[id] : 0));
        }
        AddShort((short)(table - _objectStart)); // inline size of the table
        AddShort((short)((_vtableSize + 2) * 2)); // byte length of the vtable
        BinaryPrimitives.WriteInt32LittleEndian(
            _buf.AsSpan(_buf.Length - table, 4), Offset - table);
        return table;
    }

    internal void AddBool(int id, bool value) => AddByte(id, value ? (byte)1 : (byte)0);

    internal void AddByte(int id, byte value)
    {
        Prep(1, 0);
        _buf[--_space] = value;
        _vtable[id] = Offset;
    }

    internal void AddShort(int id, short value)
    {
        AddShort(value);
        _vtable[id] = Offset;
    }

    internal void AddInt(int id, int value)
    {
        AddInt(value);
        _vtable[id] = Offset;
    }

    internal void AddLong(int id, long value)
    {
        Prep(8, 0);
        _space -= 8;
        BinaryPrimitives.WriteInt64LittleEndian(_buf.AsSpan(_space, 8), value);
        _vtable[id] = Offset;
    }

    internal void AddOffset(int id, int target)
    {
        AddOffset(target);
        _vtable[id] = Offset;
    }

    // -----------------------------------------------------------------------
    // Strings, vectors and inline structs.
    // -----------------------------------------------------------------------

    /// <summary>Writes a null-terminated UTF-8 string and returns its offset.</summary>
    internal int CreateString(string text)
    {
        int n = Encoding.UTF8.GetByteCount(text);
        Prep(1, 0);
        _buf[--_space] = 0;
        StartVector(1, n, 1);
        _space -= n;
        Encoding.UTF8.GetBytes(text, _buf.AsSpan(_space, n));
        return EndVector();
    }

    /// <summary>Writes a vector of uoffsets to already-written objects.</summary>
    internal int CreateOffsetVector(int[] targets)
    {
        StartVector(4, targets.Length, 4);
        for (int i = targets.Length - 1; i >= 0; i--)
        {
            AddOffset(targets[i]);
        }
        return EndVector();
    }

    /// <summary>Reserves room for <paramref name="count"/> elements of
    /// <paramref name="elemSize"/> bytes.</summary>
    internal void StartVector(int elemSize, int count, int alignment)
    {
        _vectorElements = count;
        int payload = checked(elemSize * count);
        Prep(4, payload);
        Prep(alignment, payload);
    }

    /// <summary>Writes the element count and returns the vector's offset.</summary>
    internal int EndVector()
    {
        _space -= 4;
        BinaryPrimitives.WriteInt32LittleEndian(_buf.AsSpan(_space, 4), _vectorElements);
        return Offset;
    }

    /// <summary>Writes one inline `{i64, i64}` struct, which covers both Arrow
    /// FieldNode and Arrow Buffer.</summary>
    internal void PutStruct(long first, long second)
    {
        Prep(8, 16);
        _space -= 16;
        BinaryPrimitives.WriteInt64LittleEndian(_buf.AsSpan(_space, 8), first);
        BinaryPrimitives.WriteInt64LittleEndian(_buf.AsSpan(_space + 8, 8), second);
    }

    // -----------------------------------------------------------------------
    // Primitives.
    // -----------------------------------------------------------------------

    private void AddShort(short value)
    {
        Prep(2, 0);
        _space -= 2;
        BinaryPrimitives.WriteInt16LittleEndian(_buf.AsSpan(_space, 2), value);
    }

    private void AddInt(int value)
    {
        Prep(4, 0);
        _space -= 4;
        BinaryPrimitives.WriteInt32LittleEndian(_buf.AsSpan(_space, 4), value);
    }

    /// <summary>Writes a forward uoffset from the slot it lands in to
    /// <paramref name="target"/>.</summary>
    private void AddOffset(int target)
    {
        Prep(4, 0);
        int value = Offset - target + 4;
        _space -= 4;
        BinaryPrimitives.WriteInt32LittleEndian(_buf.AsSpan(_space, 4), value);
    }

    /// <summary>Pads so that a <paramref name="size"/>-byte field followed by
    /// <paramref name="additional"/> bytes lands aligned, and grows the buffer if
    /// the three together do not fit.</summary>
    private void Prep(int size, int additional)
    {
        if (size > _minAlign)
        {
            _minAlign = size;
        }
        int pad = (~(Offset + additional) + 1) & (size - 1);
        Grow(pad + size + additional);
        for (int i = 0; i < pad; i++)
        {
            _buf[--_space] = 0;
        }
    }

    /// <summary>Doubles the buffer until <paramref name="need"/> bytes are free,
    /// keeping the written data at the end.</summary>
    private void Grow(int need)
    {
        while (_space < need)
        {
            int old = _buf.Length;
            byte[] next = new byte[checked(old * 2)];
            Array.Copy(_buf, _space, next, _space + old, old - _space);
            _space += old;
            _buf = next;
        }
    }
}
