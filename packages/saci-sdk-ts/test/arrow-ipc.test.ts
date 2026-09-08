// Ground-truth test for the codec: decode the stream the host produces and
// compare every column against the JSON the emitter wrote from the same Rust
// `Order` rows. Flatbuffer-offset bugs surface here, natively, before anything
// is componentized.
//
// `node --test` strips the types and runs the sources directly, so this needs
// no build step and no test-only transpiler.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';

import type { Batch } from '../src/arrow_ipc.ts';
import { SaciStream, decodeBase64 } from '../src/arrow_ipc.ts';

/** Every `Order` column, in schema order, as the codec decodes it. */
interface Columns {
  id: number[];
  region: string[];
  currency: string[];
  amount: number[];
  valid: boolean[];
  usd_amount: number[];
  usd_amount_display: string[];
  risk_score: number[];
  flagged: boolean[];
  fee: number[];
  review_tier: number[];
  settlement: string[];
}

/** One row of `fixture_input.json`: the same fields, one value deep. */
type OrderRow = { [K in keyof Columns]: Columns[K][number] };

const GENERATED = new URL('../../../examples/polyglot/generated/', import.meta.url);
const FIXTURE_SACI = new URL('fixture_input.saci', GENERATED);
const FIXTURE_JSON = new URL('fixture_input.json', GENERATED);

const skip =
  existsSync(FIXTURE_SACI) && existsSync(FIXTURE_JSON)
    ? false
    : 'examples/polyglot/generated is absent — run `cargo run -p saci-service --features wasm --example polyglot_schema_emit -- emit`';

/** A fresh mutable copy of the fixture, so mutation tests cannot leak into each other. */
function fixtureBytes(): Uint8Array {
  return new Uint8Array(readFileSync(FIXTURE_SACI));
}

/** Every column of a batch, read through the public codec surface. */
function snapshot(batch: Batch): Columns {
  return {
    id: batch.int64s('id').map(Number),
    region: batch.strings('region'),
    currency: batch.strings('currency'),
    amount: batch.float64s('amount'),
    valid: batch.bools('valid'),
    usd_amount: batch.float64s('usd_amount'),
    usd_amount_display: batch.strings('usd_amount_display'),
    risk_score: batch.float64s('risk_score'),
    flagged: batch.bools('flagged'),
    fee: batch.float64s('fee'),
    review_tier: batch.int64s('review_tier').map(Number),
    settlement: batch.strings('settlement'),
  };
}

test('the fixture stream carries an Order segment and the alive bitmap', { skip }, () => {
  const stream = new SaciStream(fixtureBytes());
  assert.deepStrictEqual(stream.componentNames(), ['Order', '__alive']);
});

test('every Order column decodes to the emitted JSON', { skip }, () => {
  // Pass the Buffer straight through: it is a Uint8Array view that may sit at a
  // non-zero byteOffset inside a pooled ArrayBuffer, which the codec must honour.
  const batch = new SaciStream(readFileSync(FIXTURE_SACI)).component('Order');
  const rows = JSON.parse(readFileSync(FIXTURE_JSON, 'utf8')) as OrderRow[];

  assert.strictEqual(batch.rows, rows.length);
  assert.deepStrictEqual(batch.fieldNames(), [
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
  ]);

  const columns = snapshot(batch);
  // The fixture is what the host hands stage one, and only the Rust stage
  // writes `usd_amount_display`, so every row carries the empty string. A
  // `Utf8` column whose values are all empty is worth pinning on its own: its
  // offsets buffer is six zeroes and a zero-length values buffer, which is the
  // shape a reader is most likely to mis-address.
  assert.deepStrictEqual(columns.usd_amount_display, ['', '', '', '', '', '']);

  for (const [row, expected] of rows.entries()) {
    for (const field of Object.keys(expected) as (keyof Columns)[]) {
      assert.deepStrictEqual(
        columns[field][row],
        expected[field],
        `row ${row} field ${field}: ${columns[field][row]} != ${expected[field]}`,
      );
    }
  }
});

test('int64s keeps full Int64 precision as BigInt', { skip }, () => {
  const batch = new SaciStream(fixtureBytes()).component('Order');
  assert.deepStrictEqual(batch.int64s('id'), [1n, 2n, 3n, 4n, 5n, 6n]);
});

test('in-place writes read back and leave neighbours byte-identical', { skip }, () => {
  const before = snapshot(new SaciStream(fixtureBytes()).component('Order'));

  const mutated = fixtureBytes();
  const stream = new SaciStream(mutated);
  const batch = stream.component('Order');
  batch.setFloat64('risk_score', 2, 1.75);
  batch.setBool('flagged', 2, true);
  batch.setBool('flagged', 4, true);
  batch.setBool('flagged', 4, false); // set then clear: the bit must end up back at 0

  // Re-parse the returned bytes from scratch, the same thing the host does.
  const after = snapshot(new SaciStream(stream.toBytes()).component('Order'));

  assert.strictEqual(after.risk_score[2], 1.75);
  assert.strictEqual(after.flagged[2], true);
  assert.deepStrictEqual(after.risk_score, [0, 0, 1.75, 0, 0, 0]);
  assert.deepStrictEqual(after.flagged, [false, false, true, false, false, false]);

  const untouched: (keyof Columns)[] = [
    'id',
    'region',
    'currency',
    'amount',
    'valid',
    'usd_amount',
    'usd_amount_display',
    'fee',
    'review_tier',
    'settlement',
  ];
  for (const field of untouched) {
    assert.deepStrictEqual(after[field], before[field], `field ${field} changed`);
  }
});

test('an in-place Int64 write to review_tier reads back and spares its neighbours', { skip }, () => {
  const before = snapshot(new SaciStream(fixtureBytes()).component('Order'));

  const stream = new SaciStream(fixtureBytes());
  const batch = stream.component('Order');
  // review_tier is the schema's only Int64 output and the C# stage is the only
  // stage that writes one. A full-width payload and a negative one prove all
  // eight little-endian bytes are written, signed.
  const wanted = [0n, 1n, 2n, 0x0102030405060708n, -1n, 3n];
  wanted.forEach((value, row) => batch.setInt64('review_tier', row, value));

  // Re-parse the returned bytes from scratch, the same thing the host does.
  const reparsed = new SaciStream(stream.toBytes()).component('Order');
  assert.deepStrictEqual(reparsed.int64s('review_tier'), wanted);

  const after = snapshot(reparsed);
  const names = Object.keys(before) as (keyof Columns)[];
  for (const field of names.filter((name) => name !== 'review_tier')) {
    assert.deepStrictEqual(after[field], before[field], `field ${field} changed`);
  }
});

test('an unknown component name is rejected', { skip }, () => {
  const stream = new SaciStream(fixtureBytes());
  assert.throws(() => stream.component('Widget'), /no segment for component "Widget"/);
});

test('writing a Utf8 column is rejected', { skip }, () => {
  const batch = new SaciStream(fixtureBytes()).component('Order');
  assert.throws(() => batch.setFloat64('settlement', 0, 1), /variable-length Utf8/);
  assert.throws(() => batch.setBool('settlement', 0, true), /variable-length Utf8/);
  assert.throws(() => batch.setInt64('settlement', 0, 1n), /variable-length Utf8/);
});

test('type, field-name and row-bound violations are rejected', { skip }, () => {
  const batch = new SaciStream(fixtureBytes()).component('Order');
  assert.throws(() => batch.float64s('valid'), /is Bool, not FloatingPoint/);
  assert.throws(() => batch.bools('amount'), /is FloatingPoint, not Bool/);
  assert.throws(() => batch.strings('id'), /is Int, not Utf8/);
  assert.throws(() => batch.float64s('nope'), /has no field "nope"/);
  assert.throws(() => batch.int64s('amount'), /is FloatingPoint, not Int/);
  const past = new RegExp(`row ${batch.rows} is out of range`);
  assert.throws(() => batch.setFloat64('risk_score', batch.rows, 1), past);
  assert.throws(() => batch.setInt64('review_tier', batch.rows, 1n), past);
  assert.throws(() => batch.setInt64('review_tier', -1, 1n), /row -1 is out of range/);
  assert.throws(() => batch.setFloat64('risk_score', -1, 1), /row -1 is out of range/);
});

test('malformed input throws instead of trapping', { skip }, () => {
  const truncated = fixtureBytes().subarray(0, 40);
  const claimed = new DataView(truncated.buffer, truncated.byteOffset, 4).getUint32(0, true);
  assert.throws(() => new SaciStream(truncated), new RegExp(`claims ${claimed} bytes, only 36 remain`));

  const noTerminator = fixtureBytes().subarray(0, fixtureBytes().length - 2);
  assert.throws(() => new SaciStream(noTerminator), /before the segment terminator/);

  assert.throws(() => new SaciStream(new Uint8Array([9, 0, 0, 0, 1])), /claims 9 bytes, only 1 remain/);
  // The static signature promises a Uint8Array; the host calls across the
  // component boundary, so the run-time guard has to hold anyway.
  assert.throws(() => new SaciStream([] as unknown as Uint8Array), /expected a byte view/);
});

test('decodeBase64 decodes standard base64 with padding', () => {
  assert.deepStrictEqual(decodeBase64('aGVsbG8='), new Uint8Array([104, 101, 108, 108, 111]));
  assert.deepStrictEqual(decodeBase64(''), new Uint8Array(0));
  assert.throws(() => decodeBase64('not base64!'));
});
