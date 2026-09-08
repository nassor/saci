// Drives the generated `saci:pipeline/pipeline` export end to end: the attributes
// below are the whole processor, and every assertion here goes through
// PipelineExportsImpl, the class the generator emits.
//
// The fixture component is the polyglot example's twelve-field `Order`, so the
// fingerprint assertion is the cross-language contract: any stage declaring these
// twelve field names in this order, at schema version 1, must report f6405a7b.

using System.Globalization;
using System.Text;

using Saci.ArrowIpc;
using Saci.Sdk;

using SaciPipelineWorld;
using SaciPipelineWorld.wit.Exports.saci.pipeline.v0_3_0;
using SaciPipelineWorld.wit.Imports.saci.pipeline.v0_3_0;

[assembly: SaciProcessor("saci-sdk-fixture", "9.9.9", LogTarget = "fixture")]

namespace Saci.Sdk.Tests;

/// <summary>The fixture row. Eleven wire names come from the default snake_case
/// conversion, `usd_amount_display` included, so the multi-word case matches what
/// the polyglot stage relies on; `currency` is renamed with
/// <see cref="SaciFieldAttribute"/> so both paths are exercised.</summary>
[SaciComponent]
public sealed class Order
{
    public long Id { get; set; }

    public string Region { get; set; } = string.Empty;

    [SaciField("currency")]
    public string CurrencyCode { get; set; } = string.Empty;

    public double Amount { get; set; }

    public bool Valid { get; set; }

    public double UsdAmount { get; set; }

    public string UsdAmountDisplay { get; set; } = string.Empty;

    public double RiskScore { get; set; }

    public bool Flagged { get; set; }

    public double Fee { get; set; }

    public long ReviewTier { get; set; }

    public string Settlement { get; set; } = string.Empty;

    /// <summary>Not a wire field: no setter, so the generator skips it.</summary>
    public bool Escalated => ReviewTier > 0;
}

public static class OrderStage
{
    /// <summary>Declared first but ordered second, which is how the assertions
    /// below can tell source order from <see cref="SaciTransformAttribute.Order"/>:
    /// it renders the tier the other transform computed.</summary>
    [SaciTransform(Order = 1)]
    public static void Display(Order row)
    {
        row.UsdAmountDisplay = string.Format(
            CultureInfo.InvariantCulture, "{0:F2}/t{1}", row.UsdAmount, row.ReviewTier);
    }

    [SaciTransform]
    public static void Tier(Order row, SaciConfig config)
    {
        double reviewScore = config.GetDouble("review_score", 0.2);
        if (row.Flagged)
        {
            row.ReviewTier = 2;
            SaciHost.Count("tier.hold_rows");
        }
        else if (row.RiskScore >= reviewScore)
        {
            row.ReviewTier = 1;
            SaciHost.Count("tier.review_rows");
        }
        else
        {
            row.ReviewTier = 0;
        }
    }
}

public class PipelineExportsTests
{
    /// <summary>The twelve wire names, in the order that fixes the
    /// fingerprint.</summary>
    private static readonly string[] WireNames =
    [
        "id", "region", "currency", "amount", "valid", "usd_amount", "usd_amount_display",
        "risk_score", "flagged", "fee", "review_tier", "settlement",
    ];

    /// <summary>FNV-1a over "Order", version 1 and the twelve names above.</summary>
    private const string OrderFingerprint = "f6405a7b";

    /// <summary>A tier no transform produces, so a `review_tier` that survives the
    /// batch is visibly a transform that did not run.</summary>
    private const long UnsetTier = 9;

    public PipelineExportsTests() => WitHost.Reset();

    // -----------------------------------------------------------------------
    // describe.
    // -----------------------------------------------------------------------

    [Fact]
    public void DescribeReportsTheAssemblyAttribute()
    {
        ITypesImports.PipelineDescriptor descriptor = PipelineExportsImpl.Describe();

        Assert.Equal("saci-sdk-fixture", descriptor.name);
        Assert.Equal("9.9.9", descriptor.version);
        Assert.False(descriptor.stateful);
        Assert.Single(descriptor.components);
        Assert.Equal("Order", descriptor.components[0].name);
    }

    [Fact]
    public void DescribeComputesTheSchemaFingerprint()
    {
        Assert.Equal(OrderFingerprint, PipelineExportsImpl.Describe().schemaFingerprint);
    }

    [Fact]
    public void DescribeEmitsASchemaOnlyIpcStreamNamingEveryField()
    {
        byte[] schema = PipelineExportsImpl.Describe().components[0].arrowSchemaIpc;

        // Framed as an Arrow IPC message, and a descriptor rather than a wire
        // segment: no component label.
        Assert.Equal(0xFFu, schema[0]);
        string text = Encoding.UTF8.GetString(schema);
        Assert.DoesNotContain("__saci_component", text, StringComparison.Ordinal);
        foreach (string name in WireNames)
        {
            Assert.Contains(name, text, StringComparison.Ordinal);
        }
    }

    // -----------------------------------------------------------------------
    // run-batch.
    // -----------------------------------------------------------------------

    [Fact]
    public void RunBatchTransformsEveryRowAndPreservesEveryOtherColumn()
    {
        ITypesImports.RunResult result = PipelineExportsImpl.RunBatch(Input(), null);

        ArrowBatch batch = new SaciStream(result.output).Component("Order");
        Assert.Equal(4, batch.Rows);
        Assert.Equal(WireNames, batch.FieldNames());

        // flagged rows hold, risk_score at or above 0.2 goes to review.
        Assert.Equal([0L, 1L, 2L, 1L], batch.Int64s("review_tier"));

        // Untouched columns come back exactly, strings included.
        Assert.Equal([1L, 2L, 3L, 4L], batch.Int64s("id"));
        Assert.Equal(["EU", "US", "", "日本"], batch.Strings("region"));
        Assert.Equal(["EUR", "USD", "GBP", "JPY"], batch.Strings("currency"));
        Assert.Equal([10.0, 20.0, 30.0, 40.0], batch.Float64s("amount"));
        Assert.Equal([true, true, false, true], batch.Bools("valid"));
        Assert.Equal([0.1, 0.5, 0.9, 0.2], batch.Float64s("risk_score"));
        Assert.Equal([false, false, true, false], batch.Bools("flagged"));
        Assert.Equal([1.0, 2.0, 3.0, 4.0], batch.Float64s("fee"));
        Assert.Equal(["", "", "", ""], batch.Strings("settlement"));
    }

    /// <summary>A Utf8 column written by a transform, which in-place mutation
    /// cannot do: every following offset moves.</summary>
    [Fact]
    public void RunBatchRewritesAUtf8Column()
    {
        ITypesImports.RunResult result = PipelineExportsImpl.RunBatch(Input(), null);

        // The rendered tier is the computed one, so Tier ran before Display even
        // though Display is declared first: SaciTransform.Order decided that.
        Assert.Equal(
            ["11.00/t0", "22.00/t1", "33.00/t2", "44.00/t1"],
            new SaciStream(result.output).Component("Order").Strings("usd_amount_display"));
    }

    [Fact]
    public void RunBatchReadsConfig()
    {
        WitHost.Config["review_score"] = "0.6";

        ITypesImports.RunResult result = PipelineExportsImpl.RunBatch(Input(), null);

        // Only the flagged row escalates now: no risk score reaches 0.6.
        Assert.Equal(
            [0L, 0L, 2L, 0L],
            new SaciStream(result.output).Component("Order").Int64s("review_tier"));
    }

    [Fact]
    public void RunBatchFailsPermanentlyOnAnUnparseableConfigValue()
    {
        WitHost.Config["review_score"] = "not-a-number";

        WitException<ITypesImports.RunError> failure =
            Assert.Throws<WitException<ITypesImports.RunError>>(
                () => PipelineExportsImpl.RunBatch(Input(), null));

        Assert.Equal(ITypesImports.RunError.Tags.Permanent, failure.TypedValue.Tag);
        Assert.Contains("review_score", failure.TypedValue.AsPermanent, StringComparison.Ordinal);
    }

    [Fact]
    public void RunBatchReportsMetricsAndOneSummaryLine()
    {
        ITypesImports.RunResult result = PipelineExportsImpl.RunBatch(Input(), null);

        // One observation per counter per batch, carrying the batch total, not one
        // observation of 1 per row.
        Assert.Contains(("tier.review_rows", 2.0), WitHost.Metrics);
        Assert.Contains(("tier.hold_rows", 1.0), WitHost.Metrics);
        Assert.Equal(2, WitHost.Metrics.Count);

        (IHostIoImports.LogLevel Level, string Target, string Message) line = Assert.Single(WitHost.Logs);
        Assert.Equal(IHostIoImports.LogLevel.INFO, line.Level);
        Assert.Equal("fixture", line.Target);
        Assert.Contains("4 rows", line.Message, StringComparison.Ordinal);
        Assert.Contains("tier.review_rows=2", line.Message, StringComparison.Ordinal);
        Assert.Contains("tier.hold_rows=1", line.Message, StringComparison.Ordinal);

        Assert.Equal(4ul, result.metrics.rowsIn);
        Assert.Equal(4ul, result.metrics.rowsOut);
        Assert.Equal(2u, result.metrics.systemsRun);
        Assert.Equal(0u, result.metrics.retries);
        Assert.Null(result.checkpoint);
        Assert.Null(result.routes);
    }

    [Fact]
    public void RunBatchPreservesTheAliveBitmap()
    {
        ITypesImports.RunResult result = PipelineExportsImpl.RunBatch(Input(), null);

        Assert.Equal(
            [true, true, false, true],
            new SaciStream(result.output).Component("__alive").Bools("alive"));
    }

    /// <summary>The host replaces the whole partition dataset with this output, so
    /// a component the processor never declared has to survive the round trip.</summary>
    [Fact]
    public void RunBatchForwardsAnUndeclaredComponent()
    {
        SaciStream writer = new();
        WriteOrder(writer);
        writer.WriteComponent(
            "Ledger", 2, new Int64Column("total", [7, 8]), new Utf8Column("note", ["a", "b"]));
        writer.WriteAlive([true, true, false, true]);

        ITypesImports.RunResult result = PipelineExportsImpl.RunBatch(writer.ToBytes(), null);

        SaciStream parsed = new(result.output);
        Assert.Contains("Ledger", parsed.ComponentNames());
        ArrowBatch ledger = parsed.Component("Ledger");
        Assert.Equal([7L, 8L], ledger.Int64s("total"));
        Assert.Equal(["a", "b"], ledger.Strings("note"));
    }

    [Fact]
    public void RunBatchFailsPermanentlyWhenTheComponentIsAbsent()
    {
        SaciStream writer = new();
        writer.WriteComponent("Ledger", 2, new Int64Column("total", [7]));
        writer.WriteAlive([true]);

        WitException<ITypesImports.RunError> failure =
            Assert.Throws<WitException<ITypesImports.RunError>>(
                () => PipelineExportsImpl.RunBatch(writer.ToBytes(), null));

        Assert.Equal(ITypesImports.RunError.Tags.Permanent, failure.TypedValue.Tag);
        Assert.Contains("Order", failure.TypedValue.AsPermanent, StringComparison.Ordinal);
    }

    [Fact]
    public void RunBatchFailsPermanentlyOnMalformedInput()
    {
        WitException<ITypesImports.RunError> failure =
            Assert.Throws<WitException<ITypesImports.RunError>>(
                () => PipelineExportsImpl.RunBatch([1, 2, 3, 4, 5, 6, 7, 8], null));

        Assert.Equal(ITypesImports.RunError.Tags.Permanent, failure.TypedValue.Tag);
        Assert.NotEmpty(failure.TypedValue.AsPermanent);
        // The failure is reported to the host before it crosses the boundary.
        Assert.Contains(WitHost.Logs, l => l.Level == IHostIoImports.LogLevel.ERROR);
    }

    [Fact]
    public void SaciHostRefusesCallsOutsideABatch()
    {
        Assert.Throws<SaciProcessorException>(() => SaciHost.Count("stray"));
        Assert.Throws<SaciProcessorException>(() => SaciHost.Metric("stray", 1));
        Assert.Throws<SaciProcessorException>(() => SaciHost.Log(SaciLogLevel.Info, "stray", "no host"));
    }

    // -----------------------------------------------------------------------
    // Fixture.
    // -----------------------------------------------------------------------

    private static byte[] Input()
    {
        SaciStream writer = new();
        WriteOrder(writer);
        writer.WriteAlive([true, true, false, true]);
        return writer.ToBytes();
    }

    private static void WriteOrder(SaciStream writer) => writer.WriteComponent(
        "Order",
        1,
        new Int64Column("id", [1, 2, 3, 4]),
        new Utf8Column("region", ["EU", "US", "", "日本"]),
        new Utf8Column("currency", ["EUR", "USD", "GBP", "JPY"]),
        new Float64Column("amount", [10.0, 20.0, 30.0, 40.0]),
        new BoolColumn("valid", [true, true, false, true]),
        new Float64Column("usd_amount", [11.0, 22.0, 33.0, 44.0]),
        new Utf8Column("usd_amount_display", ["", "", "", ""]),
        new Float64Column("risk_score", [0.1, 0.5, 0.9, 0.2]),
        new BoolColumn("flagged", [false, false, true, false]),
        new Float64Column("fee", [1.0, 2.0, 3.0, 4.0]),
        new Int64Column("review_tier", [UnsetTier, UnsetTier, UnsetTier, UnsetTier]),
        new Utf8Column("settlement", ["", "", "", ""]));
}
