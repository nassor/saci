// The Quick Start's second stage, a C# processor: the whole thing.
//
// It reads `valid` and `amount` and writes `fee` and `review_tier`: the fee the
// card network charges, and the settlement decision as a tier code.
//
//   review_tier = 0  settled
//   review_tier = 1  held for manual review, amount above `hold_above`
//   review_tier = 2  rejected, the Go stage marked the row invalid
//
// Why the decision is a number and not the `settlement` text column
//
// It no longer has to be: Saci.Sdk re-encodes every column after the transform
// runs, so writing the Utf8 `settlement` field would work. The tier code stays
// because the tutorial's PostgreSQL table and its sink mapping are keyed on it,
// and the sink is what the Quick Start teaches.
//
// There is no export glue here, and no generated schema constant either. Saci.Sdk's
// incremental generator reads the three attributes below and emits
// SaciPipelineWorld.wit.Exports.saci.pipeline.v0_3_0.PipelineExportsImpl into the
// same compilation, deriving the Arrow schema from Order's properties and the
// schema fingerprint from their names.
//
// The twelve properties and their order are the same contract
// examples/polyglot/stages/csharp-tier declares: the Quick Start reuses the
// polyglot Go stage, so both halves of the pipeline have to agree on `Order` or
// the host refuses to load it.

using Saci.Sdk;

[assembly: SaciProcessor("quickstart-settle-cs", "0.1.0", LogTarget = "settle")]

namespace QuickstartSettle
{
    /// <summary>The `Order` component. Wire names are the snake_case form of each
    /// property name.</summary>
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

    public static class SettleStage
    {
        /// <summary>Fee in basis points of the authorised amount.</summary>
        private const string FeeBpsKey = "fee_bps";

        private const double FeeBpsDefault = 290.0;

        /// <summary>Flat per-authorisation component of the fee.</summary>
        private const string FeeFixedKey = "fee_fixed";

        private const double FeeFixedDefault = 0.30;

        /// <summary>Amount above which a valid authorisation is held for review.</summary>
        private const string HoldAboveKey = "hold_above";

        private const double HoldAboveDefault = 1000.0;

        // Settlement decisions, as the tier codes the sink persists.
        private const long TierSettled = 0;
        private const long TierHold = 1;
        private const long TierRejected = 2;

        /// <summary>Prices one authorisation and decides whether it settles.</summary>
        /// <remarks>Two counters rather than one "not settled" total: a held row is
        /// a queue to work through and a rejected row is a decision already taken,
        /// so collapsing them would hide which one is growing. They are counters
        /// rather than metrics because a transform sees one row at a time and the
        /// host wants the batch total, which is what the SDK flushes at the end of
        /// the batch.</remarks>
        [SaciTransform]
        public static void Settle(Order row, SaciConfig config)
        {
            double feeBps = config.GetDouble(FeeBpsKey, FeeBpsDefault);
            double feeFixed = config.GetDouble(FeeFixedKey, FeeFixedDefault);
            double holdAbove = config.GetDouble(HoldAboveKey, HoldAboveDefault);

            if (!row.Valid)
            {
                // An invalid authorisation is never charged.
                row.Fee = 0.0;
                row.ReviewTier = TierRejected;
                SaciHost.Count("settle.rejected_rows");
                return;
            }

            // Rounded to the currency's minor unit: a fee carried at full double
            // precision would not reconcile against a ledger.
            row.Fee = Math.Round(row.Amount * feeBps / 10000.0 + feeFixed, 2);
            if (row.Amount > holdAbove)
            {
                row.ReviewTier = TierHold;
                SaciHost.Count("settle.hold_rows");
            }
            else
            {
                row.ReviewTier = TierSettled;
            }
        }
    }
}
