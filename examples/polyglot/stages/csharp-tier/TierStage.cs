// Stage 5 of the polyglot example, the C# processor: the whole thing.
//
// It reads `flagged` and `risk_score` and writes `review_tier`, the escalation
// level the Rust stage turns into a settlement decision: tier 2 holds, tier 1
// goes to review, tier 0 settles.
//
// There is no export glue here, and no generated schema constant either. Saci.Sdk's
// incremental generator reads the three attributes below and emits
// SaciPipelineWorld.wit.Exports.saci.pipeline.v0_3_0.PipelineExportsImpl into the
// same compilation, deriving the Arrow schema from Order's properties and the
// schema fingerprint from their names. Everything the old hand-written export did
// — parsing the wire format, folding failures into `run-error::permanent`,
// reporting metrics — belongs to the SDK now.
//
// The twelve properties and their order are a cross-language contract: all six
// stages of this example declare the same `Order`, and the host gates
// compatibility on the fingerprint those names produce.

using Saci.Sdk;

[assembly: SaciProcessor("polyglot-tier-cs", "0.1.0", LogTarget = "tier")]

namespace PolyglotTier
{
    /// <summary>The polyglot `Order` component. Wire names are the snake_case form
    /// of each property name, which is what the canonical Rust definition
    /// uses.</summary>
    [SaciComponent]
    public sealed class Order
    {
        public long Id { get; set; }

        public string Region { get; set; } = string.Empty;

        public string Currency { get; set; } = string.Empty;

        public double Amount { get; set; }

        public bool Valid { get; set; }

        public double UsdAmount { get; set; }

        public string UsdAmountDisplay { get; set; } = string.Empty;

        public double RiskScore { get; set; }

        public bool Flagged { get; set; }

        public double Fee { get; set; }

        public long ReviewTier { get; set; }

        public string Settlement { get; set; } = string.Empty;
    }

    public static class TierStage
    {
        /// <summary>The risk score at or above which an unflagged row still earns
        /// a look. Read from `pipeline.wasm.config` in the service TOML, or from
        /// the driver's config map.</summary>
        private const string ReviewScoreKey = "review_score";

        private const double ReviewScoreDefault = 0.2;

        // Escalation levels, in the order the Rust stage reads them.
        private const long TierClear = 0;
        private const long TierReview = 1;
        private const long TierHold = 2;

        /// <summary>Assigns one row its escalation tier.</summary>
        /// <remarks>Two counters rather than one "escalated" total: the Rust stage
        /// treats the two tiers differently, so collapsing them would hide which
        /// branch fired. They are counters rather than metrics because a transform
        /// sees one row at a time and the host wants the batch total, which is what
        /// the SDK flushes at the end of the batch.</remarks>
        [SaciTransform]
        public static void Tier(Order row, SaciConfig config)
        {
            double reviewScore = config.GetDouble(ReviewScoreKey, ReviewScoreDefault);
            if (row.Flagged)
            {
                row.ReviewTier = TierHold;
                SaciHost.Count("tier.hold_rows");
            }
            else if (row.RiskScore >= reviewScore)
            {
                row.ReviewTier = TierReview;
                SaciHost.Count("tier.review_rows");
            }
            else
            {
                row.ReviewTier = TierClear;
            }
        }
    }
}
