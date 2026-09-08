// Ground-truth test for the writer half: encode a stream, then decode it with
// the reader half and compare every value.
//
// Reader and writer are independent implementations of the same flatbuffer
// layout — one walks vtables, the other emits them — so a round trip is a real
// check rather than a tautology. The reader is also the stricter of the two: it
// rejects a segment whose buffer slots, node count, declared row count or body
// span disagree with its schema, which is exactly the class of bug a
// hand-written flatbuffer encoder produces.
//
// The host-side counterpart is `polyglot_chain`, which runs arrow-rs against
// what the TypeScript stage returns. That gate needs cargo; this one needs
// nothing but `node --test`, so it is where an encoder mistake should surface.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';

import type { Batch } from '../src/arrow_ipc.ts';
import {
  ArrowIpcError,
  BoolColumn,
  Float64Column,
  Int64Column,
  SaciStream,
  SaciStreamWriter,
  Utf8Column,
  schemaIpc,
} from '../src/arrow_ipc.ts';

/** A four-column, three-row `Order`-shaped component covering all four types. */
function writeSample(): Uint8Array {
  const writer = new SaciStreamWriter();
  writer.writeComponent(
    'Order',
    1,
    // The full Int64 range: a codec that routes values through a JS number
    // silently rounds both of these.
    Int64Column('id', [9223372036854775807n, -9223372036854775808n, 0n]),
    Utf8Column('region', ['eu-west', '', 'apac-südost 🌏']),
    Float64Column('amount', [1.5, -0.0625, 1e308]),
    BoolColumn('flagged', [true, false, true]),
  );
  writer.writeAlive([true, true, false]);
  return writer.toBytes();
}

test('every column type round-trips through the writer', () => {
  const stream = new SaciStream(writeSample());
  assert.deepStrictEqual(stream.componentNames(), ['Order', '__alive']);

  const batch = stream.component('Order');
  assert.equal(batch.rows, 3);
  // Column order is schema order, which is the order the columns were passed.
  assert.deepStrictEqual(batch.fieldNames(), ['id', 'region', 'amount', 'flagged']);
  assert.deepStrictEqual(batch.int64s('id'), [
    9223372036854775807n,
    -9223372036854775808n,
    0n,
  ]);
  assert.deepStrictEqual(batch.strings('region'), ['eu-west', '', 'apac-südost 🌏']);
  assert.deepStrictEqual(batch.float64s('amount'), [1.5, -0.0625, 1e308]);
  assert.deepStrictEqual(batch.bools('flagged'), [true, false, true]);

  // The liveness segment is a component like any other to the reader, and it is
  // what the host reads the dataset's row count out of.
  assert.deepStrictEqual(stream.component('__alive').bools('alive'), [true, true, false]);
});

test('a Utf8 column, which no in-place write can touch, is written by the writer', () => {
  // The reader refuses `setString` by design: a different string length moves
  // every buffer after it. This is the case the writer exists for.
  const inPlace = new SaciStream(writeSample());
  assert.throws(
    () => inPlace.component('Order').setInt64('region', 0, 1n),
    /variable-length Utf8/,
  );

  const writer = new SaciStreamWriter();
  writer.writeComponent('Order', 1, Utf8Column('settlement', ['T+0', 'T+2-with-a-longer-value']));
  writer.writeAlive([true, true]);
  assert.deepStrictEqual(new SaciStream(writer.toBytes()).component('Order').strings('settlement'), [
    'T+0',
    'T+2-with-a-longer-value',
  ]);
});

test('re-encoding the same component with fewer rows shrinks the batch', () => {
  const wide = new SaciStreamWriter();
  wide.writeComponent(
    'Order',
    1,
    Int64Column('id', [1, 2, 3, 4, 5]),
    Utf8Column('region', ['a', 'bb', 'ccc', 'dddd', 'eeeee']),
  );
  wide.writeAlive([true, true, true, true, true]);
  assert.equal(new SaciStream(wide.toBytes()).component('Order').rows, 5);

  // A windowing stage reduces N input rows to M output rows. Every length in
  // the segment — row count, node lengths, buffer spans, body length — has to
  // follow, which an in-place mutation cannot do.
  const narrow = new SaciStreamWriter();
  narrow.writeComponent('Order', 1, Int64Column('id', [1, 2, 3]), Utf8Column('region', ['a', 'bb', 'ccc']));
  narrow.writeAlive([true, true, true, true, true]);

  const batch = new SaciStream(narrow.toBytes()).component('Order');
  assert.equal(batch.rows, 3);
  assert.deepStrictEqual(batch.int64s('id'), [1n, 2n, 3n]);
  assert.deepStrictEqual(batch.strings('region'), ['a', 'bb', 'ccc']);
});

test('a zero-row component is a valid segment', () => {
  // A stage whose filter matched nothing still has to return the component, or
  // the host reads back a dataset that lost it.
  const writer = new SaciStreamWriter();
  writer.writeComponent('Order', 1, Int64Column('id', []), Utf8Column('region', []));
  writer.writeAlive([]);
  const batch = new SaciStream(writer.toBytes()).component('Order');
  assert.equal(batch.rows, 0);
  assert.deepStrictEqual(batch.int64s('id'), []);
  assert.deepStrictEqual(batch.strings('region'), []);
});

test('the schema-only descriptor is not a segment schema', () => {
  // `component-descriptor.arrow-schema-ipc` carries no custom metadata, so
  // reusing those bytes as a segment's schema produces a stream the host and
  // this reader both reject. The two encoders are deliberately separate.
  const descriptor = schemaIpc([
    { name: 'id', type: 'int64' },
    { name: 'region', type: 'utf8' },
    { name: 'amount', type: 'float64' },
    { name: 'flagged', type: 'bool' },
  ]);

  const framed = new Uint8Array(4 + descriptor.byteLength + 4);
  new DataView(framed.buffer).setUint32(0, descriptor.byteLength, true);
  framed.set(descriptor, 4);
  assert.throws(() => new SaciStream(framed), /carries no "__saci_component"/);

  // It is still a well-formed Arrow IPC stream: continuation word, one Schema
  // message, end-of-stream marker.
  const view = new DataView(descriptor.buffer, descriptor.byteOffset, descriptor.byteLength);
  assert.equal(view.getUint32(0, true), 0xffffffff);
  assert.equal(view.getUint32(descriptor.byteLength - 8, true), 0xffffffff);
  assert.equal(view.getUint32(descriptor.byteLength - 4, true), 0);
});

test('a segment the writer did not build passes through byte-identical', () => {
  const source = new SaciStream(writeSample());
  const foreign = source.segmentBytes('Order');

  const writer = new SaciStreamWriter();
  writer.writeSegment(foreign);
  writer.writeAlive([true, true, false]);
  const roundTripped = new SaciStream(writer.toBytes());

  assert.deepStrictEqual(roundTripped.componentNames(), ['Order', '__alive']);
  assert.deepStrictEqual([...roundTripped.segmentBytes('Order')], [...foreign]);
});

test('a stream without a liveness segment is refused', () => {
  const writer = new SaciStreamWriter();
  writer.writeComponent('Order', 1, Int64Column('id', [1]));
  assert.throws(() => writer.toBytes(), /no "__alive" segment/);
});

test('a component longer than the liveness bitmap is refused', () => {
  const writer = new SaciStreamWriter();
  writer.writeComponent('Order', 1, Int64Column('id', [1, 2, 3]));
  writer.writeAlive([true, true]);
  assert.throws(() => writer.toBytes(), /more than the 2-row liveness bitmap/);

  // Fewer rows than the bitmap is legitimate: that is a windowed component.
  const fewer = new SaciStreamWriter();
  fewer.writeComponent('Order', 1, Int64Column('id', [1]));
  fewer.writeAlive([true, true]);
  assert.equal(new SaciStream(fewer.toBytes()).component('Order').rows, 1);
});

test('the writer refuses column sets it cannot describe', () => {
  const writer = new SaciStreamWriter();
  assert.throws(
    () => writer.writeComponent('Order', 1, Int64Column('id', [1, 2]), BoolColumn('valid', [true])),
    /holds 1 rows, not 2/,
  );
  assert.throws(
    () => writer.writeComponent('Order', 1, Int64Column('id', [1]), Float64Column('id', [1])),
    /declares column "id" twice/,
  );
  assert.throws(() => writer.writeComponent('Order', 1), /needs at least one column/);
  assert.throws(
    () => writer.writeComponent('__alive', 1, BoolColumn('alive', [true])),
    /write it with writeAlive/,
  );
  assert.throws(
    () => writer.writeComponent('Order', -1, Int64Column('id', [1])),
    /schema version -1 is not a u32/,
  );
  assert.throws(() => writer.writeComponent('', 1, Int64Column('id', [1])), /cannot be empty/);

  writer.writeAlive([true]);
  assert.throws(() => writer.writeAlive([true]), /already written/);
});

test('an Int64 value the wire format cannot carry is refused, not truncated', () => {
  const writer = new SaciStreamWriter();
  assert.throws(
    () => writer.writeComponent('Order', 1, Int64Column('id', [1.5])),
    /row 0 value 1.5 is not an integer/,
  );
  // `setBigInt64` wraps silently, so this would otherwise land in the buffer as
  // a different number.
  assert.throws(
    () => writer.writeComponent('Order', 1, Int64Column('id', [9223372036854775808n])),
    /does not fit in Int64/,
  );
});

test('every refusal is an ArrowIpcError, not a native error', () => {
  const writer = new SaciStreamWriter();
  // A processor maps this class onto `run-error::permanent`; a RangeError
  // escaping from a BigInt conversion would be indistinguishable from a bug.
  assert.throws(() => writer.writeComponent('Order', 1, Int64Column('id', [0.5])), ArrowIpcError);
  assert.throws(() => writer.toBytes(), ArrowIpcError);
  assert.throws(() => writer.writeSegment(new Uint8Array(0)), ArrowIpcError);
});

// ---------------------------------------------------------------------------
// Against the host's own stream
// ---------------------------------------------------------------------------

const FIXTURE_SACI = new URL('../../../examples/polyglot/generated/fixture_input.saci', import.meta.url);

const skip = existsSync(FIXTURE_SACI)
  ? false
  : 'examples/polyglot/generated is absent — run `cargo run -p saci-service --features wasm --example polyglot_schema_emit -- emit`';

test('an arrow-rs stream re-encoded by the writer keeps every value', { skip }, () => {
  // The fixture is what arrow-rs produces, so this pins the writer against a
  // real Arrow implementation's field order, types and row count rather than
  // against the writer's own idea of them.
  const source = new SaciStream(new Uint8Array(readFileSync(FIXTURE_SACI)));
  const order = source.component('Order');
  const alive = source.component('__alive').bools('alive');

  const writer = new SaciStreamWriter();
  writer.writeComponent(
    'Order',
    1,
    Int64Column('id', order.int64s('id')),
    Utf8Column('region', order.strings('region')),
    Utf8Column('currency', order.strings('currency')),
    Float64Column('amount', order.float64s('amount')),
    BoolColumn('valid', order.bools('valid')),
    Float64Column('usd_amount', order.float64s('usd_amount')),
    Utf8Column('usd_amount_display', order.strings('usd_amount_display')),
    Float64Column('risk_score', order.float64s('risk_score')),
    BoolColumn('flagged', order.bools('flagged')),
    Float64Column('fee', order.float64s('fee')),
    Int64Column('review_tier', order.int64s('review_tier')),
    Utf8Column('settlement', order.strings('settlement')),
  );
  writer.writeAlive(alive);

  const reEncoded = new SaciStream(writer.toBytes()).component('Order');
  assert.equal(reEncoded.rows, order.rows);
  assert.deepStrictEqual(reEncoded.fieldNames(), order.fieldNames());
  for (const field of order.fieldNames()) {
    assert.deepStrictEqual(column(reEncoded, field), column(order, field));
  }
});

/** One column, read through whichever getter its Arrow type demands. */
function column(batch: Batch, name: string): readonly unknown[] {
  for (const read of [
    () => batch.int64s(name),
    () => batch.float64s(name),
    () => batch.bools(name),
    () => batch.strings(name),
  ]) {
    try {
      return read();
    } catch (err) {
      if (!(err instanceof ArrowIpcError)) {
        throw err;
      }
    }
  }
  throw new Error(`no getter matched field "${name}"`);
}
