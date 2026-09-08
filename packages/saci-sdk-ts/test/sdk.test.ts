// The SDK, exercised the way a stage uses it: declare a component, register
// transforms, run a batch, decode what came back.
//
// The processor under test is built with `makeProcessor` rather than
// `processor`, because the latter lives in `src/index.ts` behind
// `saci:pipeline/host-io@0.3.0`, a specifier only jco resolves. The stub host
// below is the same three functions, which also makes every `get-config`,
// `metric` and `log` call observable — the polyglot stage's observability is a
// behaviour, not a side note.
//
// `node --test` strips the types and runs the sources directly, so this needs
// no build step beyond the sources themselves.

import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  BoolColumn,
  Float64Column,
  Int64Column,
  SaciStream,
  SaciStreamWriter,
  Utf8Column,
  schemaIpc,
} from '../src/arrow_ipc.ts';

import {
  SaciSdkError,
  component,
  makeProcessor,
  transform,
  transformBatch,
  type HostIo,
  type InferRow,
  type LogLevel,
  type Transform,
} from '../src/core.ts';

/** The polyglot `Order`, declared exactly as the TypeScript stage declares it. */
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

type Order = InferRow<typeof Order>;

/** A recorded host call, so a test can assert on observability. */
interface Recorded {
  readonly metrics: [string, number][];
  readonly logs: [LogLevel, string, string][];
  readonly configReads: string[];
}

/** A [`HostIo`] backed by a plain config table. */
function stubHost(config: Record<string, string> = {}): HostIo & { readonly recorded: Recorded } {
  const recorded: Recorded = { metrics: [], logs: [], configReads: [] };
  return {
    recorded,
    getConfig(key) {
      recorded.configReads.push(key);
      return config[key];
    },
    metric(name, value) {
      recorded.metrics.push([name, value]);
    },
    log(level, target, message) {
      recorded.logs.push([level, target, message]);
    },
  };
}

/** A two-row `Order` stream shaped like the host's, with `usd_amount` already filled. */
function orderStream(): Uint8Array {
  const writer = new SaciStreamWriter();
  writer.writeComponent(
    'Order',
    1,
    Int64Column('id', [1n, 2n]),
    Utf8Column('region', ['eu-west', 'apac']),
    Utf8Column('currency', ['EUR', 'JPY']),
    Float64Column('amount', [100, 200]),
    BoolColumn('valid', [true, true]),
    Float64Column('usd_amount', [110, 90000]),
    Utf8Column('usd_amount_display', ['$110.00', '$90000.00']),
    Float64Column('risk_score', [0, 0]),
    BoolColumn('flagged', [false, false]),
    Float64Column('fee', [0, 0]),
    Int64Column('review_tier', [0n, 0n]),
    Utf8Column('settlement', ['', '']),
  );
  writer.writeAlive([true, true]);
  return writer.toBytes();
}

// ---------------------------------------------------------------------------
// Declaration
// ---------------------------------------------------------------------------

test('a declaration maps camelCase properties onto snake_case columns', () => {
  assert.deepStrictEqual(
    Order.columns.map((field) => field.wire),
    [
      'id',
      'region',
      'currency',
      'amount',
      'valid',
      'usd_amount',
      'usd_amount_display',
      'risk_score',
      'flagged',
      'fee',
      'review_tier',
      'settlement',
    ],
  );
  // Declaration order is wire order, and the row property keeps its own name.
  assert.deepStrictEqual(Order.columns[6], {
    key: 'usdAmountDisplay',
    wire: 'usd_amount_display',
    type: 'utf8',
  });
  assert.equal(Order.name, 'Order');
  assert.equal(Order.version, 1);
});

test('the declaration becomes the descriptor schema, types and order included', () => {
  // Byte equality against the codec's own schema-only encoder pins the type
  // mapping ('i64' to Int, 'f64' to FloatingPoint, 'bool' to Bool, 'utf8' to
  // Utf8) and the field order in one assertion.
  assert.deepStrictEqual(
    [...Order.arrowSchemaIpc],
    [
      ...schemaIpc([
        { name: 'id', type: 'int64' },
        { name: 'region', type: 'utf8' },
        { name: 'currency', type: 'utf8' },
        { name: 'amount', type: 'float64' },
        { name: 'valid', type: 'bool' },
        { name: 'usd_amount', type: 'float64' },
        { name: 'usd_amount_display', type: 'utf8' },
        { name: 'risk_score', type: 'float64' },
        { name: 'flagged', type: 'bool' },
        { name: 'fee', type: 'float64' },
        { name: 'review_tier', type: 'int64' },
        { name: 'settlement', type: 'utf8' },
      ]),
    ],
  );
});

test('a declaration the wire format cannot carry is refused', () => {
  assert.throws(() => component('', { id: 'i64' } as const), SaciSdkError);
  assert.throws(() => component('__alive', { alive: 'bool' } as const), /liveness segment/);
  assert.throws(() => component('Empty', {} as const), /declares no fields/);
  assert.throws(
    // A JavaScript caller, or an authored stage with a typo the compiler would
    // catch: either way the column type has to exist.
    () => component('Bad', { id: 'int64' } as unknown as { id: 'i64' }),
    /declares unknown type "int64"/,
  );
  assert.throws(
    () => component('Clash', { usdAmount: 'f64', usd_amount: 'f64' } as const),
    /both map to wire column "usd_amount"/,
  );
});

// ---------------------------------------------------------------------------
// Row typing, checked by the compiler
// ---------------------------------------------------------------------------

/** `true` only when `A` and `B` are the same type, both ways round. */
type Equals<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
  ? true
  : false;

test('InferRow gives the transform a fully typed row', () => {
  // Compile-time assertion: this line does not type-check unless `InferRow`
  // resolves every declared field to its value type, mutably. `tsc -p
  // tsconfig.json` is where it is enforced; the runtime assert only keeps the
  // binding from being dead code.
  const rowShapeMatches: Equals<
    Order,
    {
      id: number;
      region: string;
      currency: string;
      amount: number;
      valid: boolean;
      usdAmount: number;
      usdAmountDisplay: string;
      riskScore: number;
      flagged: boolean;
      fee: number;
      reviewTier: number;
      settlement: string;
    }
  > = true;
  assert.equal(rowShapeMatches, true);

  const row: Order = {
    id: 1,
    region: 'eu',
    currency: 'EUR',
    amount: 1,
    valid: true,
    usdAmount: 1,
    usdAmountDisplay: '$1.00',
    riskScore: 0,
    flagged: false,
    fee: 0,
    reviewTier: 0,
    settlement: '',
  };
  row.riskScore = row.usdAmount / 2;
  assert.equal(row.riskScore, 0.5);
});

// ---------------------------------------------------------------------------
// Fingerprint
// ---------------------------------------------------------------------------

test('the schema fingerprint matches the host algorithm', () => {
  const io = stubHost();
  // The value the Rust host computes for a registry holding exactly this
  // `Order` at version 1. `polyglot_chain` asserts every stage reports it.
  assert.equal(
    makeProcessor(io, 'p', '0.1.0', [transform(Order, () => {})]).describe().schemaFingerprint,
    'f6405a7b',
  );

  // A second, minimal case, so a broken hash cannot pass by coincidence.
  const single = component('X', { x: 'i64' } as const);
  assert.equal(
    makeProcessor(io, 'p', '0.1.0', [transform(single, () => {})]).describe().schemaFingerprint,
    '43623dda',
  );

  // Field names are part of it: renaming one changes the value.
  const renamed = component('X', { y: 'i64' } as const);
  assert.notEqual(
    makeProcessor(io, 'p', '0.1.0', [transform(renamed, () => {})]).describe().schemaFingerprint,
    '43623dda',
  );
});

test('describe reports the declared components and no state', () => {
  const descriptor = makeProcessor(stubHost(), 'polyglot-score-ts', '0.1.0', [
    transform(Order, () => {}),
  ]).describe();
  assert.equal(descriptor.name, 'polyglot-score-ts');
  assert.equal(descriptor.version, '0.1.0');
  assert.equal(descriptor.stateful, false);
  assert.equal(descriptor.components.length, 1);
  assert.equal(descriptor.components[0].name, 'Order');
  assert.deepStrictEqual([...descriptor.components[0].arrowSchemaIpc], [...Order.arrowSchemaIpc]);
});

// ---------------------------------------------------------------------------
// Running a batch
// ---------------------------------------------------------------------------

/** The polyglot scoring stage, transform for transform. */
function scoreTransforms(): Transform[] {
  const score = transform(Order, (row: Order, config) => {
    const threshold = config.float('risk_threshold', 50000);
    row.riskScore = row.usdAmount / threshold;
    row.flagged = row.riskScore >= 1.0;
  });
  const report = transformBatch(Order, (rows, config) => {
    const flagged = rows.filter((row) => row.flagged).length;
    config.metric('score.flagged_rows', flagged);
    config.log('info', 'score', `scored ${rows.length} rows, flagged ${flagged}`);
  });
  return [score, report];
}

test('a batch round-trips through the transforms and back onto the wire', () => {
  const io = stubHost();
  const result = makeProcessor(io, 'polyglot-score-ts', '0.1.0', scoreTransforms()).runBatch(
    orderStream(),
  );

  const batch = new SaciStream(result.output).component('Order');
  assert.equal(batch.rows, 2);
  // usd_amount / 50000, and flagged at 1.0.
  assert.deepStrictEqual(batch.float64s('risk_score'), [110 / 50000, 90000 / 50000]);
  assert.deepStrictEqual(batch.bools('flagged'), [false, true]);
  // Untouched columns survive the re-encode, including the two Utf8 ones no
  // in-place write could have preserved.
  assert.deepStrictEqual(batch.int64s('id'), [1n, 2n]);
  assert.deepStrictEqual(batch.strings('region'), ['eu-west', 'apac']);
  assert.deepStrictEqual(batch.strings('usd_amount_display'), ['$110.00', '$90000.00']);
  assert.deepStrictEqual(batch.float64s('amount'), [100, 200]);
  assert.deepStrictEqual(batch.bools('valid'), [true, true]);
  assert.deepStrictEqual(batch.int64s('review_tier'), [0n, 0n]);
  assert.deepStrictEqual(batch.strings('settlement'), ['', '']);
  assert.deepStrictEqual(new SaciStream(result.output).componentNames(), ['Order', '__alive']);
  assert.deepStrictEqual(new SaciStream(result.output).component('__alive').bools('alive'), [
    true,
    true,
  ]);

  // One metric and one log line per batch, not per row.
  assert.deepStrictEqual(io.recorded.metrics, [['score.flagged_rows', 1]]);
  assert.deepStrictEqual(io.recorded.logs, [['info', 'score', 'scored 2 rows, flagged 1']]);
  // And one config read for the whole batch, though the row transform asks per
  // row.
  assert.deepStrictEqual(io.recorded.configReads, ['risk_threshold']);

  assert.equal(result.checkpoint, undefined);
  assert.equal(result.routes, undefined);
  assert.equal(result.metrics.rowsIn, 2n);
  assert.equal(result.metrics.rowsOut, 2n);
  assert.equal(result.metrics.systemsRun, 2);
  assert.equal(result.metrics.retries, 0);
  assert.equal(typeof result.metrics.wallNs, 'bigint');
});

test('transforms run in registration order', () => {
  const doubled = transform(Order, (row: Order) => {
    row.usdAmount *= 2;
  });
  const scored = transform(Order, (row: Order) => {
    row.riskScore = row.usdAmount;
  });
  const result = makeProcessor(stubHost(), 'p', '0.1.0', [doubled, scored]).runBatch(orderStream());
  assert.deepStrictEqual(
    new SaciStream(result.output).component('Order').float64s('risk_score'),
    [220, 180000],
  );
});

test('a batch whose columns disagree with the declaration fails the batch', () => {
  // A declaration narrower than the segment would re-encode to a stream missing
  // those columns, which is host state deleted rather than a decode error.
  const narrow = component('Order', { id: 'i64', region: 'utf8' } as const);
  const processor = makeProcessor(stubHost(), 'p', '0.1.0', [transform(narrow, () => {})]);
  const failure = runError(() => processor.runBatch(orderStream()));
  assert.equal(failure.tag, 'permanent');
  assert.ok(failure.val.includes('declares [id, region] but the batch carries'), failure.val);
});

test('a batch transform may drop rows, and the component shrinks with them', () => {
  const filtered = transformBatch(Order, (rows) => {
    // A windowing stage: N rows in, fewer out. Every length in the segment has
    // to follow, which is the writer's job.
    rows.splice(1, 1);
  });
  const result = makeProcessor(stubHost(), 'p', '0.1.0', [filtered]).runBatch(orderStream());
  const output = new SaciStream(result.output);
  assert.equal(output.component('Order').rows, 1);
  assert.deepStrictEqual(output.component('Order').int64s('id'), [1n]);
  // The liveness bitmap is untouched: it is the host's row count, not the
  // component's.
  assert.deepStrictEqual(output.component('__alive').bools('alive'), [true, true]);
  assert.equal(result.metrics.rowsOut, 2n);
});

test('a host config value overrides the fallback and a bad one fails the batch', () => {
  const result = makeProcessor(stubHost({ risk_threshold: '100' }), 'p', '0.1.0', [
    transform(Order, (row: Order, config) => {
      row.riskScore = row.usdAmount / config.float('risk_threshold', 50000);
    }),
  ]).runBatch(orderStream());
  assert.deepStrictEqual(new SaciStream(result.output).component('Order').float64s('risk_score'), [
    1.1, 900,
  ]);

  for (const bad of ['0', '-1', 'many']) {
    const processor = makeProcessor(stubHost({ risk_threshold: bad }), 'p', '0.1.0', [
      transform(Order, (row: Order, config) => {
        row.riskScore = config.float('risk_threshold', 50000);
      }),
    ]);
    const failure = runError(() => processor.runBatch(orderStream()));
    assert.equal(failure.tag, 'permanent');
    assert.ok(failure.val.includes('must be a positive number'), failure.val);
  }
});

test('a component the processor never declared passes through untouched', () => {
  const writer = new SaciStreamWriter();
  writer.writeSegment(new SaciStream(orderStream()).segmentBytes('Order'));
  writer.writeComponent('Other', 7, Utf8Column('label', ['keep', 'me']));
  writer.writeAlive([true, true]);
  const input = writer.toBytes();
  const foreign = new SaciStream(input).segmentBytes('Other');

  const result = makeProcessor(stubHost(), 'p', '0.1.0', scoreTransforms()).runBatch(input);
  const output = new SaciStream(result.output);
  // Byte-identical, which is the only way to keep a schema version the
  // processor cannot know.
  assert.deepStrictEqual([...output.segmentBytes('Other')], [...foreign]);
  assert.deepStrictEqual(output.component('Other').strings('label'), ['keep', 'me']);
  assert.deepStrictEqual(output.componentNames(), ['Order', 'Other', '__alive']);
});

// ---------------------------------------------------------------------------
// Failure shape
// ---------------------------------------------------------------------------

test('every failure leaves run-batch as a permanent run-error, never an Error', () => {
  const processor = makeProcessor(stubHost(), 'p', '0.1.0', scoreTransforms());

  for (const [what, input] of [
    ['truncated stream', new Uint8Array([1, 2, 3])],
    ['empty stream', new Uint8Array(0)],
    ['a stream without the declared component', emptyStream()],
  ] as const) {
    const failure = runError(() => processor.runBatch(input));
    assert.equal(failure.tag, 'permanent', what);
    assert.notEqual(failure.val, '', what);
  }
});

/**
 * Run `call`, and require that it threw a WIT `run-error` rather than an Error.
 *
 * componentize-js lowers a thrown value into the `err` arm, but *re-throws*
 * anything `instanceof Error`, which traps the component. The shape is the
 * assertion.
 */
function runError(call: () => unknown): { tag: string; val: string } {
  let thrown: unknown;
  try {
    call();
  } catch (err) {
    thrown = err;
    if (err instanceof Error) {
      throw new Error(`threw an Error, which traps the component: ${err.message}`);
    }
  }
  if (
    thrown === null ||
    typeof thrown !== 'object' ||
    !('tag' in thrown) ||
    !('val' in thrown) ||
    typeof thrown.tag !== 'string' ||
    typeof thrown.val !== 'string'
  ) {
    throw new Error(`expected a { tag, val } run-error, got ${String(thrown)}`);
  }
  return { tag: thrown.tag, val: thrown.val };
}

/** A well-formed stream carrying a different component, so `Order` is missing. */
function emptyStream(): Uint8Array {
  const writer = new SaciStreamWriter();
  writer.writeComponent('Other', 1, Int64Column('id', [1n]));
  writer.writeAlive([true]);
  return writer.toBytes();
}

test('a processor with nothing registered is refused at build time', () => {
  const io = stubHost();
  assert.throws(() => makeProcessor(io, 'p', '0.1.0', []), /registers no transforms/);
  assert.throws(() => makeProcessor(io, '', '0.1.0', [transform(Order, () => {})]), /cannot be empty/);
  assert.throws(() => makeProcessor(io, 'p', '', [transform(Order, () => {})]), /needs a version/);
  // Two declarations of one component name would give the host two schemas for
  // one segment.
  const other = component('Order', { id: 'i64' } as const);
  assert.throws(
    () => makeProcessor(io, 'p', '0.1.0', [transform(Order, () => {}), transform(other, () => {})]),
    /declares component "Order" twice/,
  );
});
