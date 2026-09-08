// Stage 3 of the polyglot example: the **TypeScript** processor.
//
// Reads `usd_amount`, which the Python stage wrote, and produces the two risk
// columns the C# stage turns into a review tier:
//
//   risk_score = usd_amount / risk_threshold      (host config, default 50000)
//   flagged    = risk_score >= 1.0
//
// The whole stage is a component declaration and two transforms:
// `@nassor/saci-sdk` owns the descriptor, the schema fingerprint, the Arrow
// decode/encode and the `run-error` shape, so nothing here mentions a buffer, a
// flatbuffer or a generated constant.
//
// Build (jco 1.30.0, Node 24.12+):
//
//   npm install
//   npm run typecheck   # jco writes types/ from the WIT world, then tsc checks
//   npm run build
//
// The SDK carries the three jco constraints this stage used to spell out: the
// versioned `saci:pipeline/host-io@0.3.0` import specifier, the plain
// `{ tag, val }` throw componentize-js needs (it re-throws anything
// `instanceof Error`, which traps the component), and the `{ describe,
// runBatch }` export object the `saci-pipeline` world expects. `npm run build`
// still needs `--disable http` paired with `--disable fetch-event`, and must
// leave `clocks` enabled: `run-metrics.wall-ns` comes from `Date.now()`.

import { component, transform, transformBatch, processor, type InferRow } from '@nassor/saci-sdk';

/**
 * The one component this stage declares.
 *
 * Field order is the cross-language contract: all six stages declare `Order`
 * with these twelve fields in this order, and the host refuses a processor
 * whose schema fingerprint disagrees. Property names are camelCase and the wire
 * columns are snake_case; the SDK converts, so `usdAmountDisplay` addresses
 * `usd_amount_display`.
 */
const Order = component('Order', {
  id: 'i64',
  region: 'utf8',
  currency: 'utf8',
  amount: 'f64',
  valid: 'bool',
  usdAmount: 'f64',
  usdAmountDisplay: 'utf8',
  riskScore: 'f64',
  flagged: 'bool',
  fee: 'f64',
  reviewTier: 'i64',
  settlement: 'utf8',
} as const);

/** The row type the declaration implies, erased before jco ever sees it. */
type Order = InferRow<typeof Order>;

/** USD volume at which a row scores 1.0 and gets flagged. */
const DEFAULT_RISK_THRESHOLD = 50000;

/**
 * Score one row.
 *
 * `config.float` refuses a threshold that is not a positive finite number: that
 * is an operator error, not a row error, so it fails the whole batch rather
 * than silently scoring every row `Infinity`. The read is memoised per batch,
 * so asking for it per row costs one host call.
 */
const score = transform(Order, (row: Order, config) => {
  const threshold = config.float('risk_threshold', DEFAULT_RISK_THRESHOLD);
  row.riskScore = row.usdAmount / threshold;
  row.flagged = row.riskScore >= 1.0;
});

/**
 * Report what the batch did.
 *
 * A per-row transform cannot emit a batch total: one metric observation and one
 * log line per row would be the row count of each. `transformBatch` runs once,
 * after `score`, so it observes the flags that transform just wrote.
 */
const report = transformBatch(Order, (rows, config) => {
  let flaggedRows = 0;
  for (const row of rows) {
    if (row.flagged) {
      flaggedRows += 1;
    }
  }
  const threshold = config.float('risk_threshold', DEFAULT_RISK_THRESHOLD);
  config.metric('score.flagged_rows', flaggedRows);
  config.log(
    'info',
    'score',
    `scored ${rows.length} rows against threshold ${threshold}, flagged ${flaggedRows}`,
  );
});

// Stateless: the SDK returns no checkpoint, so the host persists nothing for
// this stage.
export const pipeline = processor('polyglot-score-ts', '0.1.0', score, report);
