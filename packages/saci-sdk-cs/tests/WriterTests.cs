// Checks the segment writer against the reader in the same package, and against
// the framing rules the host's arrow-rs StreamReader enforces.
//
// The reader is the reference: every value written has to come back out of a
// freshly parsed stream, byte offsets included. The framing assertions are
// separate because a stream can round-trip through this codec while still being
// unreadable to arrow-rs, which requires 8-byte aligned metadata, an 8-byte
// aligned body, and a bodyLength that matches the bytes written exactly.

using System.Buffers.Binary;
using System.Text;

using Saci.ArrowIpc;

namespace Saci.ArrowIpc.Tests;

public class WriterTests
{
    private const string Component = "Order";
    private const string AliveComponent = "__alive";
    private const uint SchemaVersion = 1;

    /// <summary>The message prefix every Arrow IPC message opens with.</summary>
    private const uint Continuation = 0xFFFFFFFF;

    // -----------------------------------------------------------------------
    // Values.
    // -----------------------------------------------------------------------

    [Fact]
    public void RoundTripsEveryColumnType()
    {
        long[] ids = [1, -2, 0, long.MaxValue, long.MinValue];
        double[] amounts = [0.0, -1.5, 1e300, double.Epsilon, 12.5];
        bool[] flags = [true, false, false, true, true];
        string[] regions = ["EU", "", "US", "aléatoire", "日本"];

        SaciStream writer = new();
        writer.WriteComponent(
            Component,
            SchemaVersion,
            new Int64Column("id", ids),
            new Float64Column("amount", amounts),
            new BoolColumn("flagged", flags),
            new Utf8Column("region", regions));
        writer.WriteAlive([true, true, true, true, true]);

        ArrowBatch batch = new SaciStream(writer.ToBytes()).Component(Component);

        Assert.Equal(5, batch.Rows);
        Assert.Equal(["id", "amount", "flagged", "region"], batch.FieldNames());
        Assert.Equal(ids, batch.Int64s("id"));
        Assert.Equal(amounts, batch.Float64s("amount"));
        Assert.Equal(flags, batch.Bools("flagged"));
        Assert.Equal(regions, batch.Strings("region"));
    }

    [Fact]
    public void RoundTripsAnEmptyComponent()
    {
        SaciStream writer = new();
        writer.WriteComponent(Component, SchemaVersion, new Int64Column("id", []), new Utf8Column("region", []));
        writer.WriteAlive([]);

        ArrowBatch batch = new SaciStream(writer.ToBytes()).Component(Component);

        Assert.Equal(0, batch.Rows);
        Assert.Empty(batch.Int64s("id"));
        Assert.Empty(batch.Strings("region"));
    }

    /// <summary>Boolean bitmaps and validity bitmaps both have a partial trailing
    /// byte at any row count that is not a multiple of eight.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(64)]
    [InlineData(65)]
    public void PacksBooleansAcrossByteBoundaries(int rows)
    {
        bool[] bits = new bool[rows];
        for (int i = 0; i < rows; i++)
        {
            bits[i] = i % 3 == 0;
        }

        SaciStream writer = new();
        writer.WriteComponent(Component, SchemaVersion, new BoolColumn("valid", bits));
        writer.WriteAlive(bits);

        byte[] bytes = writer.ToBytes();
        SaciStream parsed = new(bytes);
        Assert.Equal(bits, parsed.Component(Component).Bools("valid"));
        Assert.Equal(bits, parsed.Component(AliveComponent).Bools("alive"));
    }

    [Fact]
    public void WritesUtf8ValuesTheReaderSlicesBackExactly()
    {
        // Every combination the offsets buffer has to survive: empty at the front,
        // empty in the middle, multi-byte, and an empty tail.
        string[] values = ["", "a", "", "ünïcødé ✓", "日本語テキスト", ""];

        SaciStream writer = new();
        writer.WriteComponent(Component, SchemaVersion, new Utf8Column("settlement", values));
        writer.WriteAlive(new bool[values.Length]);

        Assert.Equal(values, new SaciStream(writer.ToBytes()).Component(Component).Strings("settlement"));
    }

    /// <summary>The point of the writer: a processor may hand back fewer rows than
    /// it read, which in-place mutation cannot do.</summary>
    [Fact]
    public void ReWritingAComponentShrinksItsRowCount()
    {
        SaciStream writer = new();
        writer.WriteAlive([true, true, true, true, true]);
        writer.WriteComponent(
            Component,
            SchemaVersion,
            new Int64Column("id", [1, 2, 3, 4, 5]),
            new Utf8Column("region", ["a", "b", "c", "d", "e"]));
        Assert.Equal(5, new SaciStream(writer.ToBytes()).Component(Component).Rows);

        writer.WriteComponent(
            Component,
            SchemaVersion,
            new Int64Column("id", [1, 3, 5]),
            new Utf8Column("region", ["a", "c", "e"]));

        ArrowBatch batch = new SaciStream(writer.ToBytes()).Component(Component);
        Assert.Equal(3, batch.Rows);
        Assert.Equal([1L, 3L, 5L], batch.Int64s("id"));
        Assert.Equal(["a", "c", "e"], batch.Strings("region"));
    }

    [Fact]
    public void KeepsSegmentOrderWhenAComponentIsRewritten()
    {
        SaciStream writer = new();
        writer.WriteComponent("A", SchemaVersion, new Int64Column("id", [1]));
        writer.WriteComponent("B", SchemaVersion, new Int64Column("id", [2]));
        writer.WriteComponent("A", SchemaVersion, new Int64Column("id", [3]));
        writer.WriteAlive([true]);

        Assert.Equal(["A", "B", AliveComponent], new SaciStream(writer.ToBytes()).ComponentNames());
    }

    [Fact]
    public void LabelsEverySegmentWithItsComponentAndVersion()
    {
        SaciStream writer = new();
        writer.WriteComponent(Component, 4242, new Int64Column("id", [1]));
        writer.WriteAlive([true]);
        byte[] bytes = writer.ToBytes();

        Assert.Equal([Component, AliveComponent], new SaciStream(bytes).ComponentNames());
        // The version rides in the same custom_metadata vector as the label, and
        // the reader does not surface it, so this checks the bytes directly. A
        // four-digit value cannot collide with unrelated stream bytes the way a
        // single digit can.
        string text = Encoding.UTF8.GetString(bytes);
        Assert.Contains("__saci_schema_version", text, StringComparison.Ordinal);
        Assert.Contains("4242", text, StringComparison.Ordinal);
        // The alive segment carries the label and nothing else.
        Assert.Equal(1, CountOccurrences(text, "__saci_schema_version"));
        Assert.Equal(2, CountOccurrences(text, "__saci_component"));
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        int count = 0;
        for (int at = 0; (at = haystack.IndexOf(needle, at, StringComparison.Ordinal)) >= 0; at += needle.Length)
        {
            count++;
        }
        return count;
    }

    [Fact]
    public void ForwardsAnUntouchedSegmentVerbatim()
    {
        SaciStream source = new();
        source.WriteComponent("Ledger", 3, new Int64Column("total", [10, 20]), new Utf8Column("note", ["x", "y"]));
        source.WriteAlive([true, true]);
        byte[] original = source.ToBytes();

        SaciStream copy = new();
        copy.WriteSegmentFrom(new SaciStream(original), "Ledger");
        copy.WriteAlive([true, true]);
        byte[] forwarded = copy.ToBytes();

        Assert.Equal(original, forwarded);
        ArrowBatch batch = new SaciStream(forwarded).Component("Ledger");
        Assert.Equal([10L, 20L], batch.Int64s("total"));
        Assert.Equal(["x", "y"], batch.Strings("note"));
    }

    // -----------------------------------------------------------------------
    // Framing.
    // -----------------------------------------------------------------------

    /// <summary>What arrow-rs requires and this codec's reader does not check:
    /// aligned metadata, an aligned body, and a bodyLength equal to the bytes the
    /// buffers actually occupy.</summary>
    [Fact]
    public void FramesMessagesTheWayArrowRsDoes()
    {
        SaciStream writer = new();
        writer.WriteComponent(
            Component,
            SchemaVersion,
            new Int64Column("id", [1, 2, 3]),
            new Float64Column("amount", [1.0, 2.0, 3.0]),
            new BoolColumn("valid", [true, false, true]),
            new Utf8Column("region", ["EU", "US", "APAC"]));
        writer.WriteAlive([true, true, true]);
        byte[] bytes = writer.ToBytes();

        int at = 0;
        int segments = 0;
        while (true)
        {
            int segLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(at, 4));
            at += 4;
            if (segLen == 0)
            {
                break;
            }
            Assert.Equal(0, segLen % 8);
            int end = at + segLen;

            // Schema message, then RecordBatch message, then the marker.
            int pos = RequireMessage(bytes, at, "schema");
            pos = RequireMessage(bytes, pos, "record batch");
            Assert.Equal(Continuation, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pos, 4)));
            Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pos + 4, 4)));
            Assert.Equal(end, pos + 8);

            at = end;
            segments++;
        }
        Assert.Equal(2, segments);
        Assert.Equal(bytes.Length, at);
    }

    /// <summary>Validates one message prefix and returns the offset of the next
    /// message. The body length is read out of the flatbuffer's Message table by
    /// walking its vtable, which is the same walk the reader performs.</summary>
    private static int RequireMessage(byte[] bytes, int at, string what)
    {
        Assert.Equal(Continuation, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(at, 4)));
        int metaLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(at + 4, 4));
        Assert.True(metaLen > 0, $"{what} message has no metadata");
        Assert.Equal(0, metaLen % 8);

        int body = at + 8 + metaLen;
        long bodyLength = MessageBodyLength(bytes, at + 8);
        Assert.Equal(0, bodyLength % 8);
        Assert.True(body + bodyLength <= bytes.Length, $"{what} body escapes the stream");
        return body + (int)bodyLength;
    }

    /// <summary>Reads `Message.bodyLength`, field id 3, out of a flatbuffer that
    /// starts at <paramref name="start"/>.</summary>
    private static long MessageBodyLength(byte[] bytes, int start)
    {
        int root = start + (int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(start, 4));
        int vtable = root - BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(root, 4));
        int vtableLen = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(vtable, 2));
        const int bodyLengthId = 3;
        int slotAt = vtable + 4 + (bodyLengthId * 2);
        if (slotAt + 2 > vtable + vtableLen)
        {
            return 0;
        }
        int slot = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(slotAt, 2));
        return slot == 0 ? 0 : BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(root + slot, 8));
    }

    [Fact]
    public void SchemaIpcStreamIsOneSchemaMessageAndTheMarker()
    {
        byte[] stream = ArrowIpc.SchemaIpcStream(
            new Int64Column("id", []),
            new Utf8Column("region", []),
            new Float64Column("amount", []),
            new BoolColumn("valid", []));

        int next = RequireMessage(stream, 0, "schema");
        Assert.Equal(Continuation, BinaryPrimitives.ReadUInt32LittleEndian(stream.AsSpan(next, 4)));
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(stream.AsSpan(next + 4, 4)));
        Assert.Equal(stream.Length, next + 8);

        // Descriptor bytes are not a wire segment: they carry no component label.
        Assert.DoesNotContain(
            "__saci_component", Encoding.UTF8.GetString(stream), StringComparison.Ordinal);
    }

    // -----------------------------------------------------------------------
    // Refusals.
    // -----------------------------------------------------------------------

    [Fact]
    public void RejectsColumnsThatDisagreeOnRowCount()
    {
        SaciStream writer = new();
        ArrowIpcException e = Assert.Throws<ArrowIpcException>(() => writer.WriteComponent(
            Component, SchemaVersion, new Int64Column("id", [1, 2]), new BoolColumn("valid", [true])));
        Assert.Contains("holds 1 rows", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsADuplicateFieldName()
    {
        SaciStream writer = new();
        ArrowIpcException e = Assert.Throws<ArrowIpcException>(() => writer.WriteComponent(
            Component, SchemaVersion, new Int64Column("id", [1]), new Int64Column("id", [2])));
        Assert.Contains("twice", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsAComponentWithNoColumns()
    {
        SaciStream writer = new();
        Assert.Throws<ArrowIpcException>(() => writer.WriteComponent(Component, SchemaVersion));
    }

    [Fact]
    public void RejectsMoreRowsThanTheAliveBitmapHasBits()
    {
        SaciStream writer = new();
        writer.WriteComponent(Component, SchemaVersion, new Int64Column("id", [1, 2, 3]));
        writer.WriteAlive([true, true]);

        ArrowIpcException e = Assert.Throws<ArrowIpcException>(writer.ToBytes);
        Assert.Contains("more than the 2-row", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsSerialisingWithoutAnAliveBitmap()
    {
        SaciStream writer = new();
        writer.WriteComponent(Component, SchemaVersion, new Int64Column("id", [1]));

        ArrowIpcException e = Assert.Throws<ArrowIpcException>(writer.ToBytes);
        Assert.Contains("WriteAlive", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsWritingTheAliveComponentAsAComponent()
    {
        SaciStream writer = new();
        Assert.Throws<ArrowIpcException>(() =>
            writer.WriteComponent(AliveComponent, SchemaVersion, new BoolColumn("alive", [true])));
    }

    [Fact]
    public void RejectsANullUtf8Value()
    {
        Assert.Throws<ArrowIpcException>(() => new Utf8Column("region", ["EU", null!]));
    }

    [Fact]
    public void RejectsReadingAWrittenStream()
    {
        SaciStream writer = new();
        writer.WriteComponent(Component, SchemaVersion, new Int64Column("id", [1]));

        Assert.Throws<ArrowIpcException>(() => writer.Component(Component));
        Assert.Throws<ArrowIpcException>(writer.ComponentNames);
    }

    [Fact]
    public void RejectsWritingAParsedStream()
    {
        SaciStream writer = new();
        writer.WriteComponent(Component, SchemaVersion, new Int64Column("id", [1]));
        writer.WriteAlive([true]);
        SaciStream parsed = new(writer.ToBytes());

        Assert.Throws<ArrowIpcException>(() =>
            parsed.WriteComponent(Component, SchemaVersion, new Int64Column("id", [2])));
        Assert.Throws<ArrowIpcException>(parsed.ToBytes);
    }
}
