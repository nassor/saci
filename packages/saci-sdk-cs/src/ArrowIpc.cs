// Reads and mutates the SACI host<->processor wire format using nothing but the .NET
// base class library. ArrowWrite.cs encodes it.
//
// Wire format, with examples/polyglot/generated/fixture_input.saci as the
// reference stream:
//
//     saci_stream := segment* terminator
//     segment    := u32le segment_len ++ arrow_ipc_stream[segment_len]
//     terminator := u32le 0x00000000
//     message    := u32le 0xFFFFFFFF ++ u32le metadata_len
//                ++ flatbuffer[metadata_len] ++ body[bodyLength]
//
// One segment per registered component ordered by component name, then an
// `__alive` bitmap segment. Each segment is a standalone Arrow IPC stream: one
// Schema message, one RecordBatch message, then an end-of-stream marker.
// `metadata_len` already includes the flatbuffer's padding to 8 bytes, and the
// next message starts at align8(body_start + bodyLength).
//
// In-place mutation and what it cannot do
//
// This file never writes a flatbuffer. Overwriting a fixed-width value slot is a
// read of the flatbuffer metadata plus a byte write into the body, which is all
// the base class library is needed for. SetInt64, SetFloat64 and SetBool
// therefore accept fixed-width fields only: changing a Utf8 value would shift
// every following offset and force a rewrite of the RecordBatch metadata. A
// processor that has to do that builds a fresh stream instead, with the
// SaciStream() writer in ArrowWrite.cs.
//
// The `__alive` segment is addressable but never mutated: the host marks every
// row of a batch alive, and a processor that can neither add nor remove rows cannot
// change that. Those bytes pass through byte-identical, as does every flatbuffer
// and every framing word.
//
// Malformed input raises ArrowIpcException, never an IndexOutOfRangeException:
// this code runs inside a component whose only failure channel is the WIT
// `permanent(string)` arm, and an escaping runtime exception traps the instance
// instead of reporting the reason. Every read of externally supplied bytes is
// bounds checked before it happens.

using System.Buffers.Binary;
using System.Text;

namespace Saci.ArrowIpc;

/// <summary>Malformed wire bytes, or a write this codec refuses to perform.</summary>
public sealed class ArrowIpcException : Exception
{
    internal ArrowIpcException(string message) : base(message) { }
}

/// <summary>Framing constants, Arrow discriminants and FlatBuffers vtable ids.</summary>
public static class ArrowIpc
{
    /// <summary>Prefixes every IPC message; a zero metadata length right after it
    /// is the end-of-stream marker.</summary>
    internal const uint Continuation = 0xFFFFFFFF;

    // Message.header_type values.
    internal const byte HeaderSchema = 1;
    internal const byte HeaderDictionary = 2;
    internal const byte HeaderRecordBatch = 3;

    // Field.type_type values this codec understands. Anything else in a
    // component segment is rejected rather than guessed at.
    internal const byte TypeInt = 2;
    internal const byte TypeFloat = 3;
    internal const byte TypeUtf8 = 5;
    internal const byte TypeBool = 6;

    // Inline FlatBuffers struct sizes: FieldNode{i64,i64}, Buffer{i64,i64}.
    internal const int FieldNodeSize = 16;
    internal const int BufferSize = 16;

    /// <summary>Names the Schema custom_metadata entry the host writes to label a
    /// segment. A segment without it is not addressable, so its absence is an
    /// error rather than a skip.</summary>
    internal const string ComponentKey = "__saci_component";

    /// <summary>Names the Schema custom_metadata entry holding a component's
    /// decimal u32 schema version. Absent on the `__alive` segment.</summary>
    internal const string SchemaVersionKey = "__saci_schema_version";

    /// <summary>The pseudo-component the dataset's liveness bitmap travels
    /// under, always the last segment of a stream.</summary>
    public const string AliveComponent = "__alive";

    /// <summary>Its single non-nullable Boolean field. Its bit length is the
    /// stream's row bound: a component may hold fewer rows, never more.</summary>
    public const string AliveField = "alive";

    // FlatBuffers vtable field ids, from Arrow's Message.fbs and Schema.fbs. A
    // union occupies two consecutive slots (discriminant, then payload), which is
    // where Field.type_type = 2 comes from.
    internal const int MsgHeaderType = 1;
    internal const int MsgHeader = 2;
    internal const int MsgBodyLength = 3;

    internal const int SchemaFieldsId = 1;
    internal const int SchemaMetadataId = 2;

    internal const int FieldNameId = 0;
    internal const int FieldTypeTypeId = 2;

    internal const int BatchLengthId = 0;
    internal const int BatchNodesId = 1;
    internal const int BatchBuffersId = 2;
    internal const int BatchCompressionId = 3;

    internal const int KvKeyId = 0;
    internal const int KvValueId = 1;

    /// <summary>Decodes standard base64 with padding (RFC 4648 section 4).</summary>
    /// <remarks>A processor that embeds its component schema as a generated constant
    /// needs this and nothing else from an encoding library, and gets the same
    /// failure type as a corrupt stream.</remarks>
    /// <exception cref="ArrowIpcException">The input is not valid base64.</exception>
    public static byte[] DecodeBase64(string text)
    {
        try
        {
            return Convert.FromBase64String(text);
        }
        catch (FormatException e)
        {
            throw new ArrowIpcException($"decode base64: {e.Message}");
        }
    }

    /// <summary>Encodes the schema-only Arrow IPC stream a processor reports as
    /// `component-descriptor.arrow-schema-ipc`: one Schema message, no
    /// RecordBatch, then the end-of-stream marker.</summary>
    /// <remarks>Only each column's name and Arrow type is read, so empty columns
    /// are the cheapest way to describe a schema. These bytes carry no
    /// `__saci_component` metadata: the host parses them with
    /// `StreamReader::schema()` to build its template dataset, and the label
    /// belongs on a wire segment instead. Passing them to a reader that expects a
    /// segment therefore fails.</remarks>
    public static byte[] SchemaIpcStream(params Column[] columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        return ArrowEncode.SchemaStream(columns);
    }

    internal static int Align8(int n) => (n + 7) & ~7;

    /// <summary>Narrows an on-the-wire i64 to int, rejecting the values that would
    /// otherwise turn into an out-of-range index.</summary>
    internal static int AsLength(long v, string what)
    {
        if (v < 0 || v > int.MaxValue)
        {
            throw new ArrowIpcException($"{what} is {v}, which is not a usable length");
        }
        return (int)v;
    }

    internal static string TypeName(byte type) => type switch
    {
        TypeInt => "Int",
        TypeFloat => "FloatingPoint",
        TypeUtf8 => "Utf8",
        TypeBool => "Bool",
        _ => $"type_type {type}",
    };
}

// ---------------------------------------------------------------------------
// Stream: segment framing.
// ---------------------------------------------------------------------------

/// <summary>A SACI wire-format stream, either parsed from bytes or built from
/// columns.</summary>
/// <remarks>The two modes are disjoint. <see cref="SaciStream(byte[])"/> parses a
/// stream the host handed over: <see cref="Component"/> addresses its segments and
/// <see cref="Buffer"/> is the mutable buffer every Set call writes into, ready to
/// be returned as `run-result.output`. <see cref="SaciStream()"/> starts an empty
/// stream that <see cref="WriteComponent"/> and <see cref="WriteAlive"/> fill and
/// <see cref="ToBytes"/> serialises, which is what a processor that changes a
/// string, drops rows, or emits a component the input never carried has to do.
/// Calling a reader on a written stream, or a writer on a parsed one, is an
/// error rather than a silent no-op.</remarks>
public sealed class SaciStream
{
    private readonly byte[] _buf;
    private readonly Segment[] _segments;

    /// <summary>Segments written so far, in the order they were first written.
    /// Non-null exactly in write mode.</summary>
    private readonly List<Encoded>? _written;

    /// <summary>The encoded `__alive` segment, which always goes last.</summary>
    private byte[]? _alive;

    private int _aliveRows;

    /// <summary>The mutable buffer backing every column, ready to be returned as
    /// `run-result.output`. Empty in write mode, where <see cref="ToBytes"/>
    /// produces the bytes instead.</summary>
    public byte[] Buffer => _buf;

    /// <summary>Parses <paramref name="owned"/> in place, taking ownership.</summary>
    /// <remarks>The array is not copied. The generated export glue already
    /// marshals `list&lt;u8&gt;` into a fresh managed array, so a second copy would
    /// only double a batch's footprint inside the processor.</remarks>
    public SaciStream(byte[] owned)
    {
        ArgumentNullException.ThrowIfNull(owned);
        _buf = owned;

        List<Segment> segments = [];
        int pos = 0;
        while (true)
        {
            if (pos > _buf.Length - 4)
            {
                throw new ArrowIpcException(
                    $"truncated stream: no segment length at offset {pos} of {_buf.Length} bytes");
            }
            int segLen = ArrowIpc.AsLength(
                BinaryPrimitives.ReadUInt32LittleEndian(_buf.AsSpan(pos, 4)), "segment length");
            pos += 4;
            if (segLen == 0)
            {
                break;
            }
            if (segLen > _buf.Length - pos)
            {
                throw new ArrowIpcException(
                    $"truncated stream: segment at offset {pos - 4} declares {segLen} bytes, "
                    + $"{_buf.Length - pos} remain");
            }
            segments.Add(new Segment(pos, pos + segLen));
            pos += segLen;
        }
        if (segments.Count == 0)
        {
            throw new ArrowIpcException("stream declares no segments");
        }
        if (pos != _buf.Length)
        {
            throw new ArrowIpcException($"{_buf.Length - pos} bytes trail the stream terminator");
        }
        _segments = [.. segments];
    }

    /// <summary>An empty stream to write component segments into.</summary>
    public SaciStream()
    {
        _buf = [];
        _segments = [];
        _written = [];
    }

    /// <summary>Encodes one component segment, replacing any segment already
    /// written under <paramref name="name"/>.</summary>
    /// <remarks>Re-writing a component recomputes its `segment_len`, so a
    /// processor may hand back fewer rows than it read. The column order is the
    /// schema. <paramref name="version"/> becomes the segment's
    /// `__saci_schema_version`, which is what the host compares against its own
    /// registration.</remarks>
    /// <exception cref="ArrowIpcException">The columns disagree on their row
    /// count, repeat a field name, or are empty; or this stream was parsed rather
    /// than created for writing.</exception>
    public void WriteComponent(string name, uint version, params Column[] columns)
    {
        List<Encoded> written = RequireWrite();
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(columns);
        if (name == ArrowIpc.AliveComponent)
        {
            throw new ArrowIpcException(
                $"\"{ArrowIpc.AliveComponent}\" is written by WriteAlive, not WriteComponent");
        }
        byte[] bytes = ArrowEncode.Segment(name, version, columns, out int rows);
        Put(written, name, rows, bytes);
    }

    /// <summary>Encodes the trailing `__alive` segment from the dataset's liveness
    /// bitmap.</summary>
    /// <remarks>Its bit length is the stream's row bound: a component may hold
    /// fewer rows, never more. A processor that neither adds nor removes rows
    /// passes the bitmap it read straight back.</remarks>
    public void WriteAlive(bool[] bits)
    {
        RequireWrite();
        ArgumentNullException.ThrowIfNull(bits);
        _alive = ArrowEncode.Segment(
            ArrowIpc.AliveComponent,
            version: null,
            [new BoolColumn(ArrowIpc.AliveField, bits)],
            out _aliveRows);
    }

    /// <summary>Copies the segment <paramref name="source"/> declares for
    /// <paramref name="component"/> across verbatim.</summary>
    /// <remarks>This is how a processor forwards a component it does not operate
    /// on: re-encoding it would be a lossless but pointless round trip, and
    /// dropping it would lose the host's data, because the host replaces the whole
    /// partition dataset with what `run-batch` returns.</remarks>
    public void WriteSegmentFrom(SaciStream source, string component)
    {
        List<Encoded> written = RequireWrite();
        ArgumentNullException.ThrowIfNull(source);
        if (source._written is not null)
        {
            throw new ArrowIpcException(
                "WriteSegmentFrom copies from a parsed stream, and the source was created "
                + "for writing");
        }
        // Parsing the batch is what bounds the copied segment's row count against
        // the alive bitmap, and it rejects a segment this codec could not read back.
        int rows = source.Component(component).Rows;
        Segment segment = source._segments[source.IndexOf(component)];
        int length = segment.End - segment.Start;
        byte[] bytes = new byte[4 + length];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0, 4), (uint)length);
        Array.Copy(source._buf, segment.Start, bytes, 4, length);
        Put(written, component, rows, bytes);
    }

    /// <summary>Serialises every written component segment, then `__alive`, then
    /// the terminator.</summary>
    /// <exception cref="ArrowIpcException">No bitmap was written, or a component
    /// holds more rows than the bitmap has bits.</exception>
    public byte[] ToBytes()
    {
        List<Encoded> written = RequireWrite();
        if (_alive is null)
        {
            throw new ArrowIpcException(
                $"stream has no \"{ArrowIpc.AliveComponent}\" segment: call WriteAlive "
                + "before ToBytes");
        }
        int total = _alive.Length + 4; // bitmap plus the u32 zero terminator
        for (int i = 0; i < written.Count; i++)
        {
            if (written[i].Rows > _aliveRows)
            {
                throw new ArrowIpcException(
                    $"component \"{written[i].Name}\" holds {written[i].Rows} rows, more than "
                    + $"the {_aliveRows}-row \"{ArrowIpc.AliveComponent}\" bitmap");
            }
            total += written[i].Bytes.Length;
        }

        byte[] output = new byte[total];
        int at = 0;
        for (int i = 0; i < written.Count; i++)
        {
            written[i].Bytes.CopyTo(output.AsSpan(at));
            at += written[i].Bytes.Length;
        }
        _alive.CopyTo(output.AsSpan(at));
        // The terminator is the four trailing zero bytes the array came with.
        return output;
    }

    /// <summary>One encoded segment, length prefix included.</summary>
    private sealed class Encoded(string name, int rows, byte[] bytes)
    {
        internal readonly string Name = name;
        internal int Rows = rows;
        internal byte[] Bytes = bytes;
    }

    /// <summary>Adds a segment, or replaces the one already written under that
    /// name in place so stream order does not depend on how often a component was
    /// rewritten.</summary>
    private static void Put(List<Encoded> written, string name, int rows, byte[] bytes)
    {
        for (int i = 0; i < written.Count; i++)
        {
            if (written[i].Name != name)
            {
                continue;
            }
            written[i].Rows = rows;
            written[i].Bytes = bytes;
            return;
        }
        written.Add(new Encoded(name, rows, bytes));
    }

    private List<Encoded> RequireWrite() =>
        _written
        ?? throw new ArrowIpcException(
            "this stream was parsed from bytes; only a stream from SaciStream() can be written");

    private void RequireRead()
    {
        if (_written is not null)
        {
            throw new ArrowIpcException(
                "this stream was created for writing; parse bytes with SaciStream(byte[]) to read");
        }
    }

    /// <summary>The index of the segment declaring <paramref name="name"/>.</summary>
    private int IndexOf(string name)
    {
        for (int i = 0; i < _segments.Length; i++)
        {
            if (SchemaOf(i).Component == name)
            {
                return i;
            }
        }
        throw new ArrowIpcException($"no segment declares component \"{name}\"");
    }

    /// <summary>The `__saci_component` label of every segment, in stream order.</summary>
    public string[] ComponentNames()
    {
        RequireRead();
        string[] names = new string[_segments.Length];
        for (int i = 0; i < _segments.Length; i++)
        {
            names[i] = SchemaOf(i).Component;
        }
        return names;
    }

    /// <summary>The batch of the segment whose Schema metadata declares
    /// <paramref name="name"/>.</summary>
    public ArrowBatch Component(string name)
    {
        RequireRead();
        for (int i = 0; i < _segments.Length; i++)
        {
            SegmentSchema schema = SchemaOf(i);
            if (schema.Component != name)
            {
                continue;
            }
            try
            {
                return Batch(_segments[i], schema, name);
            }
            catch (ArrowIpcException e)
            {
                throw new ArrowIpcException($"segment {i} ({name}): {e.Message}");
            }
        }
        throw new ArrowIpcException($"no segment declares component \"{name}\"");
    }

    /// <summary>One segment's leading Schema message and the component it labels.</summary>
    private readonly struct SegmentSchema(IpcMessage message, FbTable header, string component)
    {
        internal readonly IpcMessage Message = message;
        internal readonly FbTable Header = header;
        internal readonly string Component = component;
    }

    private SegmentSchema SchemaOf(int index)
    {
        Segment seg = _segments[index];
        try
        {
            IpcMessage schema = Message(seg.Start, seg.End);
            if (!schema.Present)
            {
                throw new ArrowIpcException("segment is empty");
            }
            if (schema.HeaderType != ArrowIpc.HeaderSchema)
            {
                throw new ArrowIpcException(
                    $"segment opens with header_type {schema.HeaderType}, "
                    + $"want {ArrowIpc.HeaderSchema} (Schema)");
            }
            if (!schema.Root.TryChild(ArrowIpc.MsgHeader, out FbTable header))
            {
                throw new ArrowIpcException("schema message carries no header");
            }
            return new SegmentSchema(schema, header, ComponentOf(header));
        }
        catch (ArrowIpcException e)
        {
            throw new ArrowIpcException($"segment {index}: {e.Message}");
        }
    }

    /// <summary>Reads the `__saci_component` label out of a Schema's custom_metadata.</summary>
    private static string ComponentOf(FbTable schema)
    {
        if (!schema.TryVector(ArrowIpc.SchemaMetadataId, out FbVector meta))
        {
            throw new ArrowIpcException(
                $"schema has no custom_metadata, so no \"{ArrowIpc.ComponentKey}\" label");
        }
        for (int i = 0; i < meta.Count; i++)
        {
            FbTable kv = meta.Table(i);
            if (kv.Str(ArrowIpc.KvKeyId) != ArrowIpc.ComponentKey)
            {
                continue;
            }
            return kv.Str(ArrowIpc.KvValueId)
                ?? throw new ArrowIpcException(
                    $"{ArrowIpc.ComponentKey} metadata entry has no value");
        }
        throw new ArrowIpcException(
            $"schema custom_metadata has no \"{ArrowIpc.ComponentKey}\" key");
    }

    /// <summary>Locates one embedded Arrow IPC stream inside <see cref="Buffer"/>.</summary>
    private readonly struct Segment(int start, int end)
    {
        /// <summary>Absolute offset of the segment's first IPC message.</summary>
        internal readonly int Start = start;

        /// <summary>Absolute offset one past the segment's last byte.</summary>
        internal readonly int End = end;
    }

    // -----------------------------------------------------------------------
    // Message framing.
    // -----------------------------------------------------------------------

    /// <summary>One framed Arrow IPC message inside a segment.</summary>
    private readonly struct IpcMessage(FbTable root, byte headerType, int body, int bodyLen, int next)
    {
        /// <summary>False for the end-of-stream marker.</summary>
        internal readonly bool Present = true;
        internal readonly FbTable Root = root;
        internal readonly byte HeaderType = headerType;

        /// <summary>Absolute offset of the message body in the stream buffer.</summary>
        internal readonly int Body = body;
        internal readonly int BodyLen = bodyLen;

        /// <summary>Absolute offset of the following message.</summary>
        internal readonly int Next = next;
    }

    private IpcMessage Message(int pos, int limit)
    {
        if (pos < 0 || pos > limit - 8)
        {
            throw new ArrowIpcException($"truncated message prefix at offset {pos}");
        }
        if (BinaryPrimitives.ReadUInt32LittleEndian(_buf.AsSpan(pos, 4)) != ArrowIpc.Continuation)
        {
            throw new ArrowIpcException(
                $"offset {pos} is not an IPC message: continuation marker missing");
        }
        int metaLen = ArrowIpc.AsLength(
            BinaryPrimitives.ReadUInt32LittleEndian(_buf.AsSpan(pos + 4, 4)), "metadata length");
        if (metaLen == 0)
        {
            return default; // end-of-stream
        }
        if (metaLen > limit - pos - 8)
        {
            throw new ArrowIpcException(
                $"message at offset {pos} declares {metaLen} metadata bytes, "
                + $"{limit - pos - 8} remain");
        }

        FbBuf fb = new(_buf, pos + 8, metaLen);
        FbTable root = fb.Root();
        byte headerType = root.U8(ArrowIpc.MsgHeaderType, 0);
        int body = pos + 8 + metaLen;
        int bodyLen = ArrowIpc.AsLength(root.I64(ArrowIpc.MsgBodyLength, 0), "bodyLength");
        if (bodyLen > limit - body)
        {
            throw new ArrowIpcException(
                $"message at offset {pos} declares a {bodyLen}-byte body, {limit - body} remain");
        }
        return new IpcMessage(root, headerType, body, bodyLen, body + ArrowIpc.Align8(bodyLen));
    }

    // -----------------------------------------------------------------------
    // Batch assembly.
    // -----------------------------------------------------------------------

    private ArrowBatch Batch(Segment seg, SegmentSchema schema, string name)
    {
        FieldBuffers[] columns = SchemaFieldsOf(schema.Header);

        IpcMessage rb = Message(schema.Message.Next, seg.End);
        if (!rb.Present)
        {
            throw new ArrowIpcException("segment ends after its schema, with no record batch");
        }
        if (rb.HeaderType != ArrowIpc.HeaderRecordBatch)
        {
            throw new ArrowIpcException(rb.HeaderType == ArrowIpc.HeaderDictionary
                ? "segment carries a dictionary batch, which this codec does not support"
                : $"second message has header_type {rb.HeaderType}, "
                  + $"want {ArrowIpc.HeaderRecordBatch} (RecordBatch)");
        }
        // Shape before contents: a genuine segment is exactly Schema, RecordBatch,
        // end-of-stream, and consumes its declared length exactly. A further message
        // is data every reader here would silently ignore.
        RequireSegmentEnds(rb.Next, seg.End);
        if (!rb.Root.TryChild(ArrowIpc.MsgHeader, out FbTable header))
        {
            throw new ArrowIpcException("record batch message carries no header");
        }
        // Body compression would make every value offset below meaningless.
        if (header.Has(ArrowIpc.BatchCompressionId))
        {
            throw new ArrowIpcException(
                "record batch body is compressed, which this codec does not support");
        }

        int rows = ArrowIpc.AsLength(header.I64(ArrowIpc.BatchLengthId, 0), "record batch length");

        if (!header.TryVector(ArrowIpc.BatchNodesId, out FbVector nodes)
            || nodes.Count != columns.Length)
        {
            throw new ArrowIpcException(
                $"record batch has {nodes.Count} field nodes for {columns.Length} schema fields");
        }
        if (nodes.Count > 0)
        {
            // Every node is inline, so validating the last one validates them all.
            nodes.Inline(nodes.Count - 1, ArrowIpc.FieldNodeSize);
        }
        if (!header.TryVector(ArrowIpc.BatchBuffersId, out FbVector buffers))
        {
            throw new ArrowIpcException("record batch carries no buffers vector");
        }

        // Buffer slots are assigned by walking the schema in field order; the slot
        // count is fixed by type_type, never inferred from a buffer's length.
        int next = 0;
        for (int i = 0; i < columns.Length; i++)
        {
            ref FieldBuffers c = ref columns[i];
            c.Validity = Take(buffers, rb, c.Name, ref next);
            switch (c.Type)
            {
                case ArrowIpc.TypeInt:
                case ArrowIpc.TypeFloat:
                case ArrowIpc.TypeBool:
                    break;
                case ArrowIpc.TypeUtf8:
                    c.Offsets = Take(buffers, rb, c.Name, ref next);
                    break;
                default:
                    throw new ArrowIpcException(
                        $"field \"{c.Name}\" has unsupported type_type {c.Type}");
            }
            c.Values = Take(buffers, rb, c.Name, ref next);
        }
        if (next != buffers.Count)
        {
            throw new ArrowIpcException(
                $"schema consumes {next} buffer slots, record batch carries {buffers.Count}");
        }

        return new ArrowBatch(rows, name, _buf, columns);
    }

    /// <summary>Refuses anything past the record batch: the segment either ends
    /// there or carries one end-of-stream marker and nothing else.</summary>
    private void RequireSegmentEnds(int pos, int end)
    {
        if (pos == end)
        {
            return;
        }
        // The marker is eight bytes: a continuation word plus a zero metadata
        // length. A tail too short to hold one is leftover data rather than a
        // message, so it never reaches the message reader, which would otherwise
        // report a truncated prefix for what is really an over-full segment.
        if (end - pos < 8)
        {
            throw new ArrowIpcException(
                $"segment carries {end - pos} bytes after its record batch, too few for an "
                + "end-of-stream marker, want one Schema and one RecordBatch");
        }
        IpcMessage extra = Message(pos, end);
        if (extra.Present)
        {
            throw new ArrowIpcException(
                $"segment carries a third message with header_type {extra.HeaderType}, "
                + "want one Schema and one RecordBatch");
        }
        pos += 8;
        if (pos != end)
        {
            throw new ArrowIpcException(
                $"segment carries {end - pos} bytes after its end-of-stream marker, "
                + "want one Schema and one RecordBatch");
        }
    }

    private static BufferSpan Take(FbVector buffers, IpcMessage rb, string field, ref int next)
    {
        if (next >= buffers.Count)
        {
            throw new ArrowIpcException(
                $"field \"{field}\" needs buffer slot {next}, record batch has {buffers.Count}");
        }
        return buffers.Buffer(next++, rb.Body, rb.BodyLen);
    }

    /// <summary>Reads field names and type discriminants in schema order, which is
    /// also buffer-walk order.</summary>
    private static FieldBuffers[] SchemaFieldsOf(FbTable schema)
    {
        if (!schema.TryVector(ArrowIpc.SchemaFieldsId, out FbVector vec))
        {
            throw new ArrowIpcException("schema carries no fields vector");
        }
        FieldBuffers[] columns = new FieldBuffers[vec.Count];
        for (int i = 0; i < vec.Count; i++)
        {
            FbTable t = vec.Table(i);
            string name = t.Str(ArrowIpc.FieldNameId)
                ?? throw new ArrowIpcException($"schema field {i} has no name");
            columns[i].Name = name;
            columns[i].Type = t.U8(ArrowIpc.FieldTypeTypeId, 0);
        }
        return columns;
    }
}

// ---------------------------------------------------------------------------
// Batch: columns of one component segment.
// ---------------------------------------------------------------------------

/// <summary>One Arrow buffer, resolved to absolute offsets in the stream buffer.</summary>
internal readonly struct BufferSpan(int off, int len)
{
    internal readonly int Off = off;
    internal readonly int Len = len;
}

/// <summary>A schema field paired with the buffers the RecordBatch assigned it.</summary>
/// <remarks>Validity is resolved but unused: arrow-rs emits an all-ones validity
/// bitmap for a non-nullable field, and an in-place value write therefore never
/// has to touch it.</remarks>
internal struct FieldBuffers
{
    internal string Name;
    internal byte Type;
    internal BufferSpan Validity;

    /// <summary>Utf8 only.</summary>
    internal BufferSpan Offsets;
    internal BufferSpan Values;
}

/// <summary>The RecordBatch of one component segment, addressable by field name.</summary>
public sealed class ArrowBatch
{
    /// <summary>The RecordBatch row count.</summary>
    public int Rows { get; }

    private readonly string _component;

    /// <summary>Aliases the stream buffer: Set calls land in the stream.</summary>
    private readonly byte[] _buf;
    private readonly FieldBuffers[] _columns;

    internal ArrowBatch(int rows, string component, byte[] buf, FieldBuffers[] columns)
    {
        Rows = rows;
        _component = component;
        _buf = buf;
        _columns = columns;
    }

    /// <summary>Field names in schema order, which is also buffer-walk order.</summary>
    public string[] FieldNames()
    {
        string[] names = new string[_columns.Length];
        for (int i = 0; i < _columns.Length; i++)
        {
            names[i] = _columns[i].Name;
        }
        return names;
    }

    /// <summary>Decodes an Int64 column.</summary>
    public long[] Int64s(string name)
    {
        ref FieldBuffers c = ref Reader(name, ArrowIpc.TypeInt, 8);
        long[] out_ = new long[Rows];
        for (int i = 0; i < out_.Length; i++)
        {
            out_[i] = BinaryPrimitives.ReadInt64LittleEndian(_buf.AsSpan(c.Values.Off + i * 8, 8));
        }
        return out_;
    }

    /// <summary>Decodes a Float64 column.</summary>
    public double[] Float64s(string name)
    {
        ref FieldBuffers c = ref Reader(name, ArrowIpc.TypeFloat, 8);
        double[] out_ = new double[Rows];
        for (int i = 0; i < out_.Length; i++)
        {
            out_[i] = BinaryPrimitives.ReadDoubleLittleEndian(_buf.AsSpan(c.Values.Off + i * 8, 8));
        }
        return out_;
    }

    /// <summary>Decodes a Boolean column from its LSB-first bitmap.</summary>
    public bool[] Bools(string name)
    {
        ref FieldBuffers c = ref Field(name, ArrowIpc.TypeBool);
        RequireBits(ref c);
        bool[] out_ = new bool[Rows];
        for (int i = 0; i < out_.Length; i++)
        {
            out_[i] = ((_buf[c.Values.Off + (i >> 3)] >> (i & 7)) & 1) == 1;
        }
        return out_;
    }

    /// <summary>Decodes a Utf8 column through its i32 offsets buffer.</summary>
    public string[] Strings(string name)
    {
        ref FieldBuffers c = ref Field(name, ArrowIpc.TypeUtf8);
        long need = ((long)Rows + 1) * 4;
        if (c.Offsets.Len < need)
        {
            throw new ArrowIpcException(
                $"field \"{name}\" offsets buffer holds {c.Offsets.Len} bytes, "
                + $"need {need} for {Rows} rows");
        }
        string[] out_ = new string[Rows];
        for (int i = 0; i < out_.Length; i++)
        {
            int start = BinaryPrimitives.ReadInt32LittleEndian(_buf.AsSpan(c.Offsets.Off + i * 4, 4));
            int end = BinaryPrimitives.ReadInt32LittleEndian(
                _buf.AsSpan(c.Offsets.Off + (i + 1) * 4, 4));
            if (start < 0 || end < start || end > c.Values.Len)
            {
                throw new ArrowIpcException(
                    $"field \"{name}\" row {i} offsets [{start},{end}) escape its "
                    + $"{c.Values.Len}-byte values buffer");
            }
            out_[i] = Encoding.UTF8.GetString(_buf, c.Values.Off + start, end - start);
        }
        return out_;
    }

    /// <summary>Overwrites one Int64 value in place.</summary>
    public void SetInt64(string name, int row, long value)
    {
        ref FieldBuffers c = ref Writer(name, ArrowIpc.TypeInt, row, 8);
        BinaryPrimitives.WriteInt64LittleEndian(_buf.AsSpan(c.Values.Off + row * 8, 8), value);
    }

    /// <summary>Overwrites one Float64 value in place.</summary>
    public void SetFloat64(string name, int row, double value)
    {
        ref FieldBuffers c = ref Writer(name, ArrowIpc.TypeFloat, row, 8);
        BinaryPrimitives.WriteDoubleLittleEndian(_buf.AsSpan(c.Values.Off + row * 8, 8), value);
    }

    /// <summary>Overwrites one bit of a Boolean column's bitmap in place.</summary>
    public void SetBool(string name, int row, bool value)
    {
        ref FieldBuffers c = ref Writer(name, ArrowIpc.TypeBool, row, 0);
        byte mask = (byte)(1 << (row & 7));
        int at = c.Values.Off + (row >> 3);
        _buf[at] = value ? (byte)(_buf[at] | mask) : (byte)(_buf[at] & ~mask);
    }

    private int FieldIndex(string name)
    {
        for (int i = 0; i < _columns.Length; i++)
        {
            if (_columns[i].Name == name)
            {
                return i;
            }
        }
        throw new ArrowIpcException($"component \"{_component}\" has no field \"{name}\"");
    }

    /// <summary>Resolves a name and checks its Arrow type.</summary>
    private ref FieldBuffers Field(string name, byte want)
    {
        ref FieldBuffers c = ref _columns[FieldIndex(name)];
        if (c.Type != want)
        {
            throw new ArrowIpcException(
                $"field \"{name}\" is {ArrowIpc.TypeName(c.Type)}, not {ArrowIpc.TypeName(want)}");
        }
        return ref c;
    }

    /// <summary>Resolves a fixed-width field and checks its values buffer covers
    /// every row.</summary>
    private ref FieldBuffers Reader(string name, byte want, int width)
    {
        ref FieldBuffers c = ref Field(name, want);
        long need = (long)Rows * width;
        if (c.Values.Len < need)
        {
            throw new ArrowIpcException(
                $"field \"{name}\" values buffer holds {c.Values.Len} bytes, "
                + $"need {need} for {Rows} rows");
        }
        return ref c;
    }

    /// <summary>Reader plus a row bound and the variable-length refusal. Width 0
    /// selects the bitmap check instead of the fixed-width one.</summary>
    /// <remarks>A Utf8 write would move every following offset, so it is rejected
    /// by type before the type-mismatch message, which would otherwise read as if a
    /// column of that name and the requested type were merely missing.</remarks>
    private ref FieldBuffers Writer(string name, byte want, int row, int width)
    {
        int i = FieldIndex(name);
        byte type = _columns[i].Type;
        if (type != ArrowIpc.TypeInt && type != ArrowIpc.TypeFloat && type != ArrowIpc.TypeBool)
        {
            throw new ArrowIpcException(
                $"field \"{name}\" is {ArrowIpc.TypeName(type)}: this codec writes fixed-width "
                + "values only, because a variable-length write would have to rebuild the "
                + "offsets buffer and the RecordBatch metadata");
        }
        RequireRow(name, row);
        if (width > 0)
        {
            return ref Reader(name, want, width);
        }
        ref FieldBuffers c = ref Field(name, want);
        RequireBits(ref c);
        return ref c;
    }

    /// <summary>The row bound every setter shares.</summary>
    private void RequireRow(string name, int row)
    {
        if (row < 0 || row >= Rows)
        {
            throw new ArrowIpcException(
                $"row {row} is out of range for field \"{name}\" of {Rows} rows");
        }
    }

    private void RequireBits(ref FieldBuffers c)
    {
        long need = ((long)Rows + 7) / 8;
        if (c.Values.Len < need)
        {
            throw new ArrowIpcException(
                $"field \"{c.Name}\" bitmap holds {c.Values.Len} bytes, "
                + $"need {need} for {Rows} rows");
        }
    }
}

// ---------------------------------------------------------------------------
// FlatBuffers reader: just enough for Arrow's Message, Schema, Field,
// RecordBatch and KeyValue tables.
// ---------------------------------------------------------------------------

/// <summary>One FlatBuffers-encoded Arrow metadata message, as a window into the
/// stream buffer. Every read is bounds checked: these bytes come from outside the
/// component.</summary>
internal readonly struct FbBuf(byte[] buf, int start, int length)
{
    private readonly byte[] _buf = buf;
    private readonly int _base = start;
    private readonly int _len = length;

    /// <summary>Throws unless <paramref name="n"/> bytes at <paramref name="off"/>
    /// lie inside the metadata window.</summary>
    internal void Require(int off, int n)
    {
        if (off < 0 || n < 0 || off > _len - n)
        {
            throw new ArrowIpcException(
                $"read of {n} bytes at {off} exceeds {_len}-byte metadata");
        }
    }

    internal byte U8(int off)
    {
        Require(off, 1);
        return _buf[_base + off];
    }

    internal ushort U16(int off)
    {
        Require(off, 2);
        return BinaryPrimitives.ReadUInt16LittleEndian(_buf.AsSpan(_base + off, 2));
    }

    internal uint U32(int off)
    {
        Require(off, 4);
        return BinaryPrimitives.ReadUInt32LittleEndian(_buf.AsSpan(_base + off, 4));
    }

    internal long I64(int off)
    {
        Require(off, 8);
        return BinaryPrimitives.ReadInt64LittleEndian(_buf.AsSpan(_base + off, 8));
    }

    internal string Utf8(int off, int n)
    {
        Require(off, n);
        return Encoding.UTF8.GetString(_buf, _base + off, n);
    }

    /// <summary>Narrows a computed FlatBuffers position to an index. uoffset_t is
    /// unsigned 32-bit, so the sum can leave the range of int even when neither
    /// term does.</summary>
    internal static int AsOffset(long at, string what)
    {
        if (at < 0 || at > int.MaxValue)
        {
            throw new ArrowIpcException($"{what} resolves to {at}, which is not a usable offset");
        }
        return (int)at;
    }

    /// <summary>Reads a vector's declared element count and bounds it by the
    /// metadata that follows the header.</summary>
    /// <remarks>The smallest FlatBuffers vector element is a four-byte offset, so a
    /// count whose elements cannot fit is corrupt. Checking it here rather than at
    /// element access is what keeps a corrupt four-byte count from reaching a
    /// per-element allocation, where it would be an out-of-memory abort instead of a
    /// reported reason.</remarks>
    internal int VectorCount(int head)
    {
        int count = ArrowIpc.AsLength(U32(head), "vector length");
        if ((long)count * 4 > _len - head - 4)
        {
            throw new ArrowIpcException(
                $"vector at {head} declares {count} elements, "
                + $"{_len - head - 4} metadata bytes follow");
        }
        return count;
    }

    /// <summary>Follows the buffer's leading uoffset to the root table.</summary>
    internal FbTable Root() => Table(AsOffset(U32(0), "root offset"));

    /// <summary>Reads the table header at <paramref name="pos"/>: a signed offset
    /// back to its vtable.</summary>
    internal FbTable Table(int pos)
    {
        int soff = (int)U32(pos);
        int vt = AsOffset((long)pos - soff, $"vtable of table at {pos}");
        int vtLen = U16(vt);
        if (vtLen < 4)
        {
            throw new ArrowIpcException($"table at {pos} has a {vtLen}-byte vtable");
        }
        Require(vt, vtLen);
        return new FbTable(this, pos, vt, vtLen);
    }
}

internal readonly struct FbTable(FbBuf buf, int pos, int vt, int vtLen)
{
    private readonly FbBuf _buf = buf;
    private readonly int _pos = pos;
    private readonly int _vt = vt;
    private readonly int _vtLen = vtLen;

    /// <summary>The field's offset from the table position, or 0 for an absent
    /// field. FlatBuffers encodes absence as a zero vtable entry or as a vtable too
    /// short to hold the id. The vtable bounds were checked when the table was
    /// resolved, so this cannot fail.</summary>
    private int Slot(int id)
    {
        int off = 4 + (id * 2);
        return off + 2 > _vtLen ? 0 : _buf.U16(_vt + off);
    }

    internal bool Has(int id) => Slot(id) != 0;

    internal byte U8(int id, byte def)
    {
        int slot = Slot(id);
        return slot == 0 ? def : _buf.U8(_pos + slot);
    }

    internal long I64(int id, long def)
    {
        int slot = Slot(id);
        return slot == 0 ? def : _buf.I64(_pos + slot);
    }

    /// <summary>Resolves a uoffset field to the table it points at.</summary>
    internal bool TryChild(int id, out FbTable child)
    {
        int slot = Slot(id);
        if (slot == 0)
        {
            child = default;
            return false;
        }
        int at = _pos + slot;
        child = _buf.Table(FbBuf.AsOffset(at + (long)_buf.U32(at), $"child at {at}"));
        return true;
    }

    /// <summary>The string field, or null when absent.</summary>
    internal string? Str(int id)
    {
        int slot = Slot(id);
        if (slot == 0)
        {
            return null;
        }
        int at = _pos + slot;
        int head = FbBuf.AsOffset(at + (long)_buf.U32(at), $"string at {at}");
        int n = ArrowIpc.AsLength(_buf.U32(head), "string length");
        return _buf.Utf8(head + 4, n);
    }

    internal bool TryVector(int id, out FbVector vec)
    {
        int slot = Slot(id);
        if (slot == 0)
        {
            vec = default;
            return false;
        }
        int at = _pos + slot;
        int head = FbBuf.AsOffset(at + (long)_buf.U32(at), $"vector at {at}");
        vec = new FbVector(_buf, head + 4, _buf.VectorCount(head));
        return true;
    }
}

internal readonly struct FbVector(FbBuf buf, int start, int count)
{
    private readonly FbBuf _buf = buf;
    private readonly int _start = start;

    internal int Count { get; } = count;

    /// <summary>Resolves element <paramref name="i"/> of a vector of tables.</summary>
    internal FbTable Table(int i)
    {
        if (i < 0 || i >= Count)
        {
            throw new ArrowIpcException(
                $"element {i} is out of range for a {Count}-element vector");
        }
        int at = _start + (i * 4);
        return _buf.Table(FbBuf.AsOffset(at + (long)_buf.U32(at), $"vector element at {at}"));
    }

    /// <summary>The position of inline struct element <paramref name="i"/>.</summary>
    internal int Inline(int i, int size)
    {
        if (i < 0 || i >= Count)
        {
            throw new ArrowIpcException(
                $"element {i} is out of range for a {Count}-element vector");
        }
        int at = _start + (i * size);
        _buf.Require(at, size);
        return at;
    }

    /// <summary>Reads inline Buffer{i64 offset, i64 length} element
    /// <paramref name="i"/> and resolves it against the message body. Buffer.offset
    /// is body-relative.</summary>
    internal BufferSpan Buffer(int i, int body, int bodyLen)
    {
        int at = Inline(i, ArrowIpc.BufferSize);
        long off = _buf.I64(at);
        long len = _buf.I64(at + 8);
        if (off < 0 || len < 0 || off > bodyLen || len > bodyLen - off)
        {
            throw new ArrowIpcException(
                $"buffer {i} spans [{off},{off + len}) of a {bodyLen}-byte body");
        }
        return new BufferSpan(body + (int)off, (int)len);
    }
}
