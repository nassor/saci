// Runs the shared corpus at packages/arrow-ipc-conformance. One manifest and one
// set of vectors pin five codecs to the same answer about which streams are valid,
// so the only per-language part is ReasonSubstrings below: the corpus fixes the
// behaviour, each codec keeps its own words. A new case upstream costs one row.
//
// A missing manifest or vector fails the run. A conformance suite that skips
// itself when its inputs are absent reports green while proving nothing.

using System.Text.Json;

using Saci.ArrowIpc;

namespace Saci.ArrowIpc.Tests;

public class ConformanceTests
{
    /// <summary>Every manifest reason code, mapped to the substring this codec's
    /// refusal carries. A reason with two vectors needs one substring both messages
    /// share, which is why `truncated_stream` and `extra_message` map to a clause
    /// rather than to a whole message.</summary>
    private static readonly Dictionary<string, string> ReasonSubstrings = new(StringComparer.Ordinal)
    {
        // Stream and message framing.
        ["trailing_bytes"] = "bytes trail the stream terminator",
        ["truncated_stream"] = "truncated stream",
        ["truncated_message"] = "metadata bytes",
        ["bad_continuation"] = "continuation marker missing",
        ["empty_segment"] = "segment is empty",

        // Segment shape: which messages a segment carries, in what order.
        ["first_message_not_schema"] = "opens with header_type",
        ["second_message_not_record_batch"] = "second message has header_type",
        ["dictionary_batch"] = "dictionary batch",
        ["extra_message"] = "want one Schema and one RecordBatch",
        ["missing_component_key"] = "__saci_component",

        // Record batch contents.
        ["compressed_batch"] = "body is compressed",
        ["bad_row_count"] = "record batch length is",
        ["nodes_field_mismatch"] = "field nodes for",
        ["buffer_overruns_body"] = "spans [",

        // API calls on a stream that parsed.
        ["unknown_component"] = "no segment declares component",
        ["unknown_field"] = "has no field",

        // The type-mismatch message is `field "x" is A, not B`, so the comma is the
        // only part of it that does not name a specific pair of types.
        ["type_mismatch"] = ", not ",
        ["row_out_of_range"] = "is out of range for field",
        ["variable_width_write"] = "writes fixed-width values only",
    };

    // -----------------------------------------------------------------------
    // Manifest.
    // -----------------------------------------------------------------------

    /// <summary>One case, with its vector resolved to an absolute path. Exactly one
    /// of <paramref name="Expect"/> and <paramref name="Reason"/> is set.</summary>
    private sealed record Case(
        string Name,
        string Vector,
        Expectation? Expect,
        string? Reason,
        Operation? Op);

    /// <summary>What an accept case must read back.</summary>
    private sealed record Expectation(
        string[] Components,
        string Component,
        int Rows,
        ColumnValues[] Columns);

    private sealed record ColumnValues(string Field, string Type, JsonElement Values);

    /// <summary>The reject cases that parse cleanly and fail on the call instead.</summary>
    private sealed record Operation(
        string Kind,
        string Component,
        string? Field,
        string? Type,
        int Row,
        JsonElement Value);

    private sealed record Manifest(string Component, string[] Reasons, Case[] Cases)
    {
        internal Case Find(string name) => Array.Find(Cases, c => c.Name == name)
            ?? throw new KeyNotFoundException($"the manifest carries no case \"{name}\"");
    }

    private static readonly Manifest Corpus = Load();

    /// <summary>Walks up from the test assembly to the committed corpus. Anchored on
    /// the manifest itself, so an absent corpus fails with the path it looked for
    /// rather than reporting an empty suite.</summary>
    private static string FindManifest()
    {
        for (DirectoryInfo? dir = new(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            string candidate = Path.Combine(
                dir.FullName, "packages", "arrow-ipc-conformance", "manifest.json");
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }
        throw new FileNotFoundException(
            "no packages/arrow-ipc-conformance/manifest.json above "
            + AppContext.BaseDirectory);
    }

    /// <summary>Reads the manifest field by field, because a case this harness does
    /// not understand has to fail rather than deserialize into defaults that every
    /// assertion below still agrees with.</summary>
    private static Manifest Load()
    {
        string path = FindManifest();
        string dir = Path.GetDirectoryName(path)!;
        using JsonDocument doc = JsonDocument.Parse(File.ReadAllBytes(path));
        JsonElement root = doc.RootElement;

        int version = root.GetProperty("format_version").GetInt32();
        if (version != 1)
        {
            throw new NotSupportedException(
                $"{path} declares format_version {version}, this harness reads 1");
        }

        List<Case> cases = [];
        foreach (JsonElement c in root.GetProperty("cases").EnumerateArray())
        {
            string name = c.GetProperty("name").GetString()!;
            // Manifest paths are `/`-separated and relative to the manifest.
            string vector = Path.Combine(
                [dir, .. c.GetProperty("vector").GetString()!.Split('/')]);
            cases.Add(c.GetProperty("expect").GetString() switch
            {
                "accept" => new Case(name, vector, ReadExpectation(c.GetProperty("accept")), null, null),
                "reject" => new Case(
                    name,
                    vector,
                    null,
                    c.GetProperty("reason").GetString()!,
                    c.TryGetProperty("op", out JsonElement op) ? ReadOperation(op) : null),
                string other => throw new NotSupportedException(
                    $"case \"{name}\" declares expect \"{other}\""),
                _ => throw new NotSupportedException($"case \"{name}\" declares no expect"),
            });
        }

        string[] reasons = [.. root.GetProperty("reasons").EnumerateArray().Select(r => r.GetString()!)];
        return new Manifest(root.GetProperty("component").GetString()!, reasons, [.. cases]);
    }

    private static Expectation ReadExpectation(JsonElement accept)
    {
        List<ColumnValues> columns = [];
        foreach (JsonProperty column in accept.GetProperty("columns").EnumerateObject())
        {
            columns.Add(new ColumnValues(
                column.Name,
                column.Value.GetProperty("type").GetString()!,
                // Cloned: the values outlive the JsonDocument they came from.
                column.Value.GetProperty("values").Clone()));
        }
        return new Expectation(
            [.. accept.GetProperty("components").EnumerateArray().Select(v => v.GetString()!)],
            accept.GetProperty("component").GetString()!,
            accept.GetProperty("rows").GetInt32(),
            [.. columns]);
    }

    private static Operation ReadOperation(JsonElement op)
    {
        string kind = op.GetProperty("kind").GetString()!;
        if (kind is not ("component" or "column" or "set"))
        {
            throw new NotSupportedException($"unsupported op kind \"{kind}\"");
        }
        return new Operation(
            kind,
            op.GetProperty("component").GetString()!,
            op.TryGetProperty("field", out JsonElement field) ? field.GetString() : null,
            op.TryGetProperty("type", out JsonElement type) ? type.GetString() : null,
            op.TryGetProperty("row", out JsonElement row) ? row.GetInt32() : 0,
            op.TryGetProperty("value", out JsonElement value) ? value.Clone() : default);
    }

    // -----------------------------------------------------------------------
    // Cases.
    // -----------------------------------------------------------------------

    /// <summary>One reported test per manifest case, named after it.</summary>
    public static TheoryData<string> CaseNames()
    {
        TheoryData<string> names = [];
        foreach (Case c in Corpus.Cases)
        {
            names.Add(c.Name);
        }
        return names;
    }

    [Theory]
    [MemberData(nameof(CaseNames))]
    public void CorpusCaseBehavesAsTheManifestSays(string name)
    {
        Case c = Corpus.Find(name);
        byte[] bytes = File.Exists(c.Vector)
            ? File.ReadAllBytes(c.Vector)
            : throw new FileNotFoundException($"case \"{name}\" has no vector at {c.Vector}", c.Vector);

        if (c.Expect is Expectation expect)
        {
            AssertAccepted(expect, bytes);
            return;
        }
        AssertRejected(c, bytes);
    }

    /// <summary>The manifest's reason list is the contract. A reason with no row in
    /// the table would assert nothing, and a row with no reason is dead.</summary>
    [Fact]
    public void SubstringTableCoversEveryManifestReason()
    {
        string[] mapped = [.. ReasonSubstrings.Keys.Order(StringComparer.Ordinal)];
        Assert.Equal(Corpus.Reasons, mapped);
    }

    // -----------------------------------------------------------------------
    // Accept.
    // -----------------------------------------------------------------------

    private static void AssertAccepted(Expectation want, byte[] bytes)
    {
        SaciStream stream = new(bytes);
        Assert.Equal(want.Components, stream.ComponentNames());
        ArrowBatch batch = stream.Component(want.Component);
        Assert.Equal(want.Rows, batch.Rows);

        foreach (ColumnValues column in want.Columns)
        {
            switch (column.Type)
            {
                case "int64":
                    Assert.Equal(Listed(column, v => v.GetInt64()), batch.Int64s(column.Field));
                    break;
                case "float64":
                    // Exact, with no tolerance: these are bit patterns round-tripped
                    // through the wire, not values this codec computed.
                    Assert.Equal(Listed(column, v => v.GetDouble()), batch.Float64s(column.Field));
                    break;
                case "bool":
                    Assert.Equal(Listed(column, v => v.GetBoolean()), batch.Bools(column.Field));
                    break;
                case "utf8":
                    Assert.Equal(Listed(column, v => v.GetString()!), batch.Strings(column.Field));
                    break;
                default:
                    throw new NotSupportedException(
                        $"column \"{column.Field}\" declares type \"{column.Type}\"");
            }
        }
    }

    private static T[] Listed<T>(ColumnValues column, Func<JsonElement, T> read)
    {
        T[] values = new T[column.Values.GetArrayLength()];
        int i = 0;
        foreach (JsonElement value in column.Values.EnumerateArray())
        {
            values[i++] = read(value);
        }
        return values;
    }

    // -----------------------------------------------------------------------
    // Reject.
    // -----------------------------------------------------------------------

    private static void AssertRejected(Case c, byte[] bytes)
    {
        string want = ReasonSubstrings.TryGetValue(c.Reason!, out string? substring)
            ? substring
            : throw new KeyNotFoundException(
                $"reason \"{c.Reason}\" has no row in this codec's substring table");

        if (c.Op is not Operation op)
        {
            AssertRefused(want, () => Parse(bytes));
            return;
        }
        // An op case parses cleanly and fails on the call, so the parse runs outside
        // the assertion: a parse failure here is a different bug, and must not read
        // as the refusal under test.
        SaciStream stream = Parse(bytes);
        AssertRefused(want, () => Invoke(stream, op));
    }

    /// <summary>The refusal contract: this codec's own exception type, carrying the
    /// substring the reason maps to. Assert.Throws matches the type exactly, so a
    /// native IndexOutOfRangeException escaping from underneath fails the case even
    /// though something was thrown.</summary>
    private static void AssertRefused(string want, Action call)
    {
        ArrowIpcException e = Assert.Throws<ArrowIpcException>(call);
        Assert.Contains(want, e.Message, StringComparison.Ordinal);
    }

    /// <summary>Everything a parse-level case has to survive: the segment framing,
    /// every segment's component label, and the batch resolve, which validates every
    /// buffer the record batch declares.</summary>
    private static SaciStream Parse(byte[] bytes)
    {
        SaciStream stream = new(bytes);
        stream.ComponentNames();
        stream.Component(Corpus.Component);
        return stream;
    }

    private static void Invoke(SaciStream stream, Operation op)
    {
        ArrowBatch batch = stream.Component(op.Component);
        switch (op.Kind)
        {
            case "component":
                break; // Resolving the component was the operation.
            case "column":
                Read(batch, op.Field!, op.Type!);
                break;
            case "set":
                Write(batch, op.Field!, op.Type!, op.Row, op.Value);
                break;
            default:
                throw new NotSupportedException($"unsupported op kind \"{op.Kind}\"");
        }
    }

    private static void Read(ArrowBatch batch, string field, string type)
    {
        switch (type)
        {
            case "int64":
                batch.Int64s(field);
                break;
            case "float64":
                batch.Float64s(field);
                break;
            case "bool":
                batch.Bools(field);
                break;
            case "utf8":
                batch.Strings(field);
                break;
            default:
                throw new NotSupportedException($"cannot read a column as \"{type}\"");
        }
    }

    private static void Write(ArrowBatch batch, string field, string type, int row, JsonElement value)
    {
        switch (type)
        {
            case "int64":
                batch.SetInt64(field, row, value.GetInt64());
                break;
            case "float64":
                batch.SetFloat64(field, row, value.GetDouble());
                break;
            case "bool":
                batch.SetBool(field, row, value.GetBoolean());
                break;
            case "utf8":
                // This codec exposes no variable-width setter, so a fixed-width
                // setter on a Utf8 field is the only way to ask for one, and is the
                // call its refusal names. The value never gets read.
                batch.SetInt64(field, row, 0);
                break;
            default:
                throw new NotSupportedException($"cannot write a column as \"{type}\"");
        }
    }
}
