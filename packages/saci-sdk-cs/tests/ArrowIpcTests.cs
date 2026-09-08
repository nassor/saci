// Checks the codec against the emitter's own fixtures: the wire bytes must decode
// to the same rows the emitter wrote as JSON, and an in-place write must move
// exactly the value bytes it claims to and nothing else.
//
// These tests run on the host, not in wasm, so they pin the codec contract
// without a wasm runtime in the picture.

using System.Buffers.Binary;
using System.Text.Json;

using Saci.ArrowIpc;

namespace Saci.ArrowIpc.Tests;

public class ArrowIpcTests
{
    /// <summary>The command that produces the fixtures. They are gitignored, so
    /// tests that need them skip rather than fail when they are absent.</summary>
    private const string EmitHint =
        "run `cargo run -p saci-service --features wasm --example polyglot_schema_emit -- emit` first";

    private const string Component = "Order";
    private const string AliveComponent = "__alive";

    /// <summary>Tolerance for float comparisons. The fixture's amounts are exact in
    /// binary64, so this only absorbs the round trip through JSON's decimal text.</summary>
    private const double Tolerance = 1e-9;

    /// <summary>Load-bearing: this is the order the fingerprint is computed in and
    /// the order the buffer walk assigns slots in.</summary>
    private static readonly string[] FieldOrder =
    [
        "id", "region", "currency", "amount", "valid", "usd_amount", "usd_amount_display",
        "risk_score", "flagged", "fee", "review_tier", "settlement",
    ];

    // -----------------------------------------------------------------------
    // Fixtures.
    // -----------------------------------------------------------------------

    /// <summary>Mirrors one object of examples/polyglot/generated/fixture_input.json,
    /// which is the ground truth for what the codec must decode out of the wire
    /// bytes.</summary>
    private sealed record Order(
        long Id,
        string Region,
        string Currency,
        double Amount,
        bool Valid,
        double UsdAmount,
        string UsdAmountDisplay,
        double RiskScore,
        bool Flagged,
        double Fee,
        long ReviewTier,
        string Settlement);

    /// <summary>The repository root, found by walking up from the test binary until
    /// the canonical WIT file appears. Anchoring on a committed file rather than on
    /// the generated directory keeps the skip path reachable.</summary>
    private static readonly string RepoRoot = FindRepoRoot();

    private static string FindRepoRoot()
    {
        for (DirectoryInfo? dir = new(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            if (File.Exists(Path.Combine(dir.FullName, "crates", "saci-processor", "wit", "pipeline.wit")))
            {
                return dir.FullName;
            }
        }
        throw new DirectoryNotFoundException(
            $"no repository root above {AppContext.BaseDirectory}");
    }

    /// <summary>Reads one emitter output.</summary>
    /// <remarks>A missing file fails rather than skips, unlike the Go, Python and
    /// TypeScript codec tests: nothing else in this project reads the generated
    /// directory, so a fixture missing at run time means the emit step ran
    /// partially, which is worth a red test.</remarks>
    private static byte[] ReadGenerated(string name)
    {
        string path = Path.Combine(RepoRoot, "examples", "polyglot", "generated", name);
        return File.Exists(path)
            ? File.ReadAllBytes(path)
            : throw new FileNotFoundException($"{path} is absent: {EmitHint}", path);
    }

    private static byte[] FixtureBytes() => ReadGenerated("fixture_input.saci");

    /// <summary>The JSON ground truth for the same rows the wire bytes carry. Read
    /// field by field: a naming-policy mismatch would otherwise silently produce
    /// six rows of defaults that every assertion below still agrees with.</summary>
    private static Order[] ExpectedRows()
    {
        using JsonDocument doc = JsonDocument.Parse(ReadGenerated("fixture_input.json"));
        JsonElement root = doc.RootElement;
        Order[] rows = new Order[root.GetArrayLength()];
        int i = 0;
        foreach (JsonElement row in root.EnumerateArray())
        {
            rows[i++] = new Order(
                row.GetProperty("id").GetInt64(),
                row.GetProperty("region").GetString()!,
                row.GetProperty("currency").GetString()!,
                row.GetProperty("amount").GetDouble(),
                row.GetProperty("valid").GetBoolean(),
                row.GetProperty("usd_amount").GetDouble(),
                row.GetProperty("usd_amount_display").GetString()!,
                row.GetProperty("risk_score").GetDouble(),
                row.GetProperty("flagged").GetBoolean(),
                row.GetProperty("fee").GetDouble(),
                row.GetProperty("review_tier").GetInt64(),
                row.GetProperty("settlement").GetString()!);
        }
        return rows;
    }

    private static ArrowBatch OrderBatch(SaciStream stream) => stream.Component(Component);

    /// <summary>Checks all twelve columns against the JSON ground truth.</summary>
    private static void AssertColumns(ArrowBatch batch, Order[] want)
    {
        Assert.Equal(want.Length, batch.Rows);

        long[] id = batch.Int64s("id");
        string[] region = batch.Strings("region");
        string[] currency = batch.Strings("currency");
        double[] amount = batch.Float64s("amount");
        bool[] valid = batch.Bools("valid");
        double[] usd = batch.Float64s("usd_amount");
        string[] usdDisplay = batch.Strings("usd_amount_display");
        double[] risk = batch.Float64s("risk_score");
        bool[] flagged = batch.Bools("flagged");
        double[] fee = batch.Float64s("fee");
        long[] tier = batch.Int64s("review_tier");
        string[] settlement = batch.Strings("settlement");

        for (int i = 0; i < want.Length; i++)
        {
            Assert.Equal(want[i].Id, id[i]);
            Assert.Equal(want[i].Region, region[i]);
            Assert.Equal(want[i].Currency, currency[i]);
            Assert.Equal(want[i].Amount, amount[i], Tolerance);
            Assert.Equal(want[i].Valid, valid[i]);
            Assert.Equal(want[i].UsdAmount, usd[i], Tolerance);
            Assert.Equal(want[i].UsdAmountDisplay, usdDisplay[i]);
            Assert.Equal(want[i].RiskScore, risk[i], Tolerance);
            Assert.Equal(want[i].Flagged, flagged[i]);
            Assert.Equal(want[i].Fee, fee[i], Tolerance);
            Assert.Equal(want[i].ReviewTier, tier[i]);
            Assert.Equal(want[i].Settlement, settlement[i]);
        }
    }

    // -----------------------------------------------------------------------
    // Decoding.
    // -----------------------------------------------------------------------

    [Fact]
    public void FixtureDecodesEveryColumn()
    {
        SaciStream stream = new(FixtureBytes());
        AssertColumns(OrderBatch(stream), ExpectedRows());
    }

    /// <summary>Pins the stage rule to the `review_tier` column the chain is
    /// documented to hand the Rust stage.</summary>
    /// <remarks>The rule lives in the polyglot stage's TierStage.cs, which only compiles for
    /// wasi-wasm because it implements a generated WIT interface, so the documented
    /// inputs and the documented expectation are compared here instead. The upstream
    /// columns are written through the codec rather than assumed, so the rule is fed
    /// from bytes that went through a full parse.</remarks>
    [Fact]
    public void DocumentedInputsProduceDocumentedReviewTier()
    {
        // risk_score and flagged as the TypeScript stage leaves them, then the
        // review_tier the Rust stage is documented to read back.
        (double Risk, bool Flagged, long Tier)[] chain =
        [
            (0.0022, false, 0),
            (0.0, false, 0),
            (0.136, false, 0),
            (1.2, true, 2),
            (0.0, false, 0),
            (0.4, false, 1),
        ];
        const double reviewScore = 0.2; // the review_score config default

        SaciStream stream = new(FixtureBytes());
        ArrowBatch upstream = OrderBatch(stream);
        Assert.Equal(chain.Length, upstream.Rows);
        for (int row = 0; row < chain.Length; row++)
        {
            upstream.SetFloat64("risk_score", row, chain[row].Risk);
            upstream.SetBool("flagged", row, chain[row].Flagged);
        }

        SaciStream fresh = new(stream.Buffer);
        ArrowBatch batch = OrderBatch(fresh);
        bool[] flagged = batch.Bools("flagged");
        double[] risk = batch.Float64s("risk_score");
        for (int row = 0; row < chain.Length; row++)
        {
            batch.SetInt64("review_tier", row, flagged[row] ? 2 : risk[row] >= reviewScore ? 1 : 0);
        }

        Assert.Equal(
            chain.Select(c => c.Tier),
            OrderBatch(new SaciStream(fresh.Buffer)).Int64s("review_tier"));
    }

    [Fact]
    public void FieldOrderMatchesSchema()
    {
        SaciStream stream = new(FixtureBytes());
        Assert.Equal(FieldOrder, OrderBatch(stream).FieldNames());
    }

    [Fact]
    public void AliveSegmentTrailsTheComponents()
    {
        SaciStream stream = new(FixtureBytes());
        Assert.Equal(new[] { Component, AliveComponent }, stream.ComponentNames());
        Assert.Equal(ExpectedRows().Length, stream.Component(AliveComponent).Rows);
    }

    // -----------------------------------------------------------------------
    // Mutation.
    // -----------------------------------------------------------------------

    /// <summary>Counts how many of the eight little-endian bytes of two i64s differ.
    /// An in-place write only rewrites those.</summary>
    private static int ChangedBytes(long before, long after)
    {
        int changed = 0;
        for (int i = 0; i < 8; i++)
        {
            if ((byte)(before >> (i * 8)) != (byte)(after >> (i * 8)))
            {
                changed++;
            }
        }
        return changed;
    }

    /// <summary>The mutation contract: the written values read back after a fresh
    /// parse of the mutated buffer, every other column still matches the ground
    /// truth, and nothing outside the target value buffer moved, byte for byte.</summary>
    [Fact]
    public void SetInt64RoundTripsAndMovesNothingElse()
    {
        byte[] original = FixtureBytes();
        Order[] want = ExpectedRows();

        SaciStream stream = new((byte[])original.Clone());
        ArrowBatch batch = OrderBatch(stream);

        // Distinct per row, and none of them the zero the fixture starts at, so a
        // write landing on the wrong row cannot pass.
        long[] tiers = new long[batch.Rows];
        for (int row = 0; row < batch.Rows; row++)
        {
            tiers[row] = (row * 7) + 1;
            batch.SetInt64("review_tier", row, tiers[row]);
        }

        byte[] mutated = stream.Buffer;
        Assert.Equal(original.Length, mutated.Length);

        int wantDiff = 0;
        for (int row = 0; row < tiers.Length; row++)
        {
            wantDiff += ChangedBytes(want[row].ReviewTier, tiers[row]);
        }
        int gotDiff = 0;
        for (int i = 0; i < original.Length; i++)
        {
            if (original[i] != mutated[i])
            {
                gotDiff++;
            }
        }
        Assert.Equal(wantDiff, gotDiff);

        // Re-parse from scratch: the values have to be readable out of the bytes the
        // host would receive, not out of the parser state that wrote them.
        SaciStream reparsed = new(mutated);
        ArrowBatch after = OrderBatch(reparsed);
        Assert.Equal(tiers, after.Int64s("review_tier"));
        AssertColumns(after, [.. want.Select((o, row) => o with { ReviewTier = tiers[row] })]);
    }

    /// <summary>A bool bit set and cleared again has to leave the stream exactly as
    /// it arrived: the fixture's `valid` column is all false, so the two writes are
    /// each other's inverse.</summary>
    [Fact]
    public void BoolSetAndClearedReproducesInput()
    {
        byte[] original = FixtureBytes();
        SaciStream stream = new((byte[])original.Clone());
        ArrowBatch batch = OrderBatch(stream);

        for (int row = 0; row < batch.Rows; row++)
        {
            batch.SetBool("valid", row, true);
        }
        Assert.NotEqual(original, stream.Buffer);
        Assert.All(batch.Bools("valid"), v => Assert.True(v));

        for (int row = 0; row < batch.Rows; row++)
        {
            batch.SetBool("valid", row, false);
        }
        Assert.Equal(original, stream.Buffer);
    }

    /// <summary>`settlement` is a variable-length output and belongs to the Rust
    /// stage. Every fixed-width setter has to refuse it by name, and refuse it
    /// before touching anything.</summary>
    /// <remarks>`usd_amount_display` is the chain's other Utf8 output; a stage
    /// writing either one needs the segment writer, not these setters.</remarks>
    [Fact]
    public void SetOnUtf8Rejected()
    {
        byte[] original = FixtureBytes();
        SaciStream stream = new((byte[])original.Clone());
        ArrowBatch batch = OrderBatch(stream);

        foreach (Action write in new Action[]
        {
            () => batch.SetInt64("settlement", 0, 1),
            () => batch.SetFloat64("settlement", 0, 1.0),
            () => batch.SetBool("settlement", 0, true),
        })
        {
            ArrowIpcException e = Assert.Throws<ArrowIpcException>(write);
            Assert.Contains("settlement", e.Message, StringComparison.Ordinal);
            Assert.Contains("Utf8", e.Message, StringComparison.Ordinal);
            Assert.Contains("fixed-width", e.Message, StringComparison.Ordinal);
        }
        Assert.Equal(original, stream.Buffer);
    }

    [Fact]
    public void SetRejectsTypeMismatchAndRowRange()
    {
        byte[] original = FixtureBytes();
        SaciStream stream = new((byte[])original.Clone());
        ArrowBatch batch = OrderBatch(stream);

        // `review_tier` is Int64, `fee` is Float64, `valid` is Boolean.
        Assert.Contains("not FloatingPoint",
            Assert.Throws<ArrowIpcException>(() => batch.SetFloat64("review_tier", 0, 1.0)).Message,
            StringComparison.Ordinal);
        Assert.Contains("not Int",
            Assert.Throws<ArrowIpcException>(() => batch.SetInt64("fee", 0, 1)).Message,
            StringComparison.Ordinal);
        Assert.Contains("not Bool",
            Assert.Throws<ArrowIpcException>(() => batch.SetBool("review_tier", 0, true)).Message,
            StringComparison.Ordinal);

        foreach (int row in new[] { -1, batch.Rows })
        {
            Assert.Contains("out of range",
                Assert.Throws<ArrowIpcException>(() => batch.SetInt64("review_tier", row, 1)).Message,
                StringComparison.Ordinal);
        }
        Assert.Contains("no field",
            Assert.Throws<ArrowIpcException>(() => batch.SetInt64("absent", 0, 1)).Message,
            StringComparison.Ordinal);

        Assert.Equal(original, stream.Buffer);
    }

    // -----------------------------------------------------------------------
    // Base64.
    // -----------------------------------------------------------------------

    /// <summary>The one encoding helper a processor needs for its schema constant.</summary>
    [Fact]
    public void DecodeBase64HandlesPaddingAndRejectsGarbage()
    {
        Assert.Equal("hello"u8.ToArray(), ArrowIpc.DecodeBase64("aGVsbG8="));
        Assert.Equal([], ArrowIpc.DecodeBase64(""));
        Assert.Contains("decode base64",
            Assert.Throws<ArrowIpcException>(() => ArrowIpc.DecodeBase64("not base64!")).Message,
            StringComparison.Ordinal);
    }

    // -----------------------------------------------------------------------
    // Framing.
    // -----------------------------------------------------------------------

    [Fact]
    public void ParseRejectsMalformedFraming()
    {
        byte[] fixture = FixtureBytes();

        Assert.Contains("truncated stream",
            Assert.Throws<ArrowIpcException>(() => new SaciStream([1, 2, 3])).Message,
            StringComparison.Ordinal);
        Assert.Contains("declares no segments",
            Assert.Throws<ArrowIpcException>(() => new SaciStream([0, 0, 0, 0])).Message,
            StringComparison.Ordinal);

        // A segment length one byte past the end of the buffer.
        byte[] overlong = (byte[])fixture.Clone();
        overlong[0]++;
        Assert.Throws<ArrowIpcException>(() => new SaciStream(overlong));

        // Truncating the terminator leaves the last segment unaccounted for.
        Assert.Throws<ArrowIpcException>(() => new SaciStream(fixture[..^2]));

        SaciStream stream = new(fixture);
        Assert.Contains("no segment declares component",
            Assert.Throws<ArrowIpcException>(() => stream.Component("Absent")).Message,
            StringComparison.Ordinal);
    }

    /// <summary>A segment whose declared length stops inside its end-of-stream
    /// marker leaves a tail too short to be a message at all, which the shared
    /// corpus has no vector for.</summary>
    /// <remarks>Derived from a real stream rather than forged, the way the corpus
    /// generator derives its own vectors: every segment ends with an eight-byte
    /// marker, so shrinking the first segment by four leaves exactly four bytes the
    /// reader must refuse rather than hand to the message parser.</remarks>
    [Fact]
    public void SegmentEndingInsideItsMarkerIsRefused()
    {
        byte[] fixture = FixtureBytes();
        int declared = BinaryPrimitives.ReadInt32LittleEndian(fixture);
        int end = 4 + declared; // One past the first segment.
        byte[] doctored = [.. fixture[..(end - 4)], .. fixture[end..]];
        BinaryPrimitives.WriteInt32LittleEndian(doctored, declared - 4);

        ArrowIpcException e = Assert.Throws<ArrowIpcException>(
            () => new SaciStream(doctored).Component(Component));
        Assert.Contains("4 bytes after its record batch", e.Message, StringComparison.Ordinal);
        Assert.Contains("want one Schema and one RecordBatch", e.Message, StringComparison.Ordinal);
    }
}
