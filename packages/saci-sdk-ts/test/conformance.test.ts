// The shared conformance corpus, run through this codec.
//
// `packages/arrow-ipc-conformance/` holds one manifest and one binary vector
// per case, and all five codecs run it, so "which streams are valid" is decided
// once rather than five times. The error *text* stays local to each language,
// which is why the reason-to-substring table below is the only per-language
// part of this file: a new corpus case costs one row here and nothing else.
//
// Nothing in here skips. A conformance suite that quietly passes because the
// corpus was not checked out is worse than no suite at all, so a missing
// manifest or vector is a hard failure with the path in the message.
//
// `node --test` strips the types and runs the sources directly, the same as the
// fixture suite next to it.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import type { Batch } from '../src/arrow_ipc.ts';
import { ArrowIpcError, SaciStream } from '../src/arrow_ipc.ts';

/**
 * The reason code each corpus case carries, mapped to the fragment of this
 * codec's message that proves the right rule fired.
 *
 * One row per reason, and `every reason in the manifest is mapped` below fails
 * the day the corpus grows one this table has not caught up with. The fragments
 * are chosen to be unique across every message the codec can produce, so a case
 * cannot pass on the strength of some other refusal.
 */
const REASON_SUBSTRING: Record<string, string | undefined> = {
  trailing_bytes: 'bytes follow the stream terminator',
  truncated_stream: 'saci stream: truncated at',
  truncated_message: 'metadata bytes, past the segment end',
  bad_continuation: 'expected continuation 0xffffffff',
  empty_segment: 'is empty: no Schema message',
  first_message_not_schema: 'does not start with a Schema message',
  second_message_not_record_batch: 'after its schema; expected a RecordBatch',
  dictionary_batch: 'carries a dictionary batch',
  compressed_batch: 'declares body compression',
  extra_message: 'carries an extra message after its RecordBatch',
  bad_row_count: 'which is not a usable row count',
  nodes_field_mismatch: 'fields but the RecordBatch declares',
  buffer_overruns_body: ') escapes the',
  missing_component_key: 'carries no "__saci_component"',
  unknown_component: 'no segment for component',
  unknown_field: 'has no field',
  type_mismatch: ', not ',
  row_out_of_range: 'is out of range for field',
  variable_width_write: 'in-place writes are limited to fixed-width',
};

type ColumnType = 'int64' | 'float64' | 'utf8' | 'bool';

/** A JSON scalar as the manifest spells it. */
type Scalar = number | string | boolean;

interface ColumnSpec {
  readonly type: ColumnType;
  readonly values: readonly Scalar[];
}

interface AcceptSpec {
  readonly components: readonly string[];
  readonly component: string;
  readonly rows: number;
  readonly columns: Readonly<Record<string, ColumnSpec>>;
}

/** The three operations a reject case can name; absent means the parse itself must fail. */
type Op =
  | { readonly kind: 'component'; readonly component: string }
  | { readonly kind: 'column'; readonly component: string; readonly field: string; readonly type: ColumnType }
  | {
      readonly kind: 'set';
      readonly component: string;
      readonly field: string;
      readonly type: ColumnType;
      readonly row: number;
      readonly value: Scalar;
    };

interface Case {
  readonly name: string;
  readonly vector: string;
  readonly expect: 'accept' | 'reject';
  readonly reason?: string;
  readonly accept?: AcceptSpec;
  readonly op?: Op;
}

interface Manifest {
  readonly component: string;
  readonly reasons: readonly string[];
  readonly cases: readonly Case[];
}

const MANIFEST_URL = new URL('../../arrow-ipc-conformance/manifest.json', import.meta.url);

/** Read a corpus file, or fail naming the path. The corpus is committed, so absence is a broken checkout. */
function corpusBytes(url: URL): Uint8Array {
  try {
    return new Uint8Array(readFileSync(url));
  } catch (cause) {
    throw new Error(
      `conformance corpus file ${fileURLToPath(url)} is unreadable, and this suite must not skip: ${String(cause)}`,
    );
  }
}

const manifest = JSON.parse(new TextDecoder().decode(corpusBytes(MANIFEST_URL))) as Manifest;

/** A vector's bytes, resolved against the manifest and freshly copied so `set` cases cannot leak. */
function vectorBytes(relative: string): Uint8Array {
  return corpusBytes(new URL(relative, MANIFEST_URL));
}

/** Read one column through the public surface, dispatching on the manifest's type name. */
function readColumn(batch: Batch, field: string, type: ColumnType): readonly unknown[] {
  switch (type) {
    case 'int64':
      return batch.int64s(field);
    case 'float64':
      return batch.float64s(field);
    case 'utf8':
      return batch.strings(field);
    case 'bool':
      return batch.bools(field);
  }
}

/** Perform one in-place write. */
function writeCell(batch: Batch, op: Extract<Op, { kind: 'set' }>): void {
  switch (op.type) {
    case 'int64':
      batch.setInt64(op.field, op.row, BigInt(op.value as number));
      return;
    case 'float64':
      batch.setFloat64(op.field, op.row, op.value as number);
      return;
    case 'bool':
      batch.setBool(op.field, op.row, op.value as boolean);
      return;
    case 'utf8':
      // No Utf8 setter exists, and that absence is the rule under test: the
      // only way to ask for an in-place write to a variable-width column is
      // through a fixed-width setter, and `writable` refuses the column before
      // the requested type is ever considered.
      batch.setFloat64(op.field, op.row, 0);
      return;
  }
}

function runOp(stream: SaciStream, op: Op): void {
  if (op.kind === 'component') {
    stream.component(op.component);
    return;
  }
  const batch = stream.component(op.component);
  if (op.kind === 'column') {
    readColumn(batch, op.field, op.type);
    return;
  }
  writeCell(batch, op);
}

/**
 * Parse a vector as far as a codec ever parses one.
 *
 * This codec decodes a segment's RecordBatch on first access, so the batch-level
 * rules — row count, nodes, buffers, compression — only run once the component
 * is addressed. A reject case that carries no `op` still has to reach them.
 */
function parseVector(bytes: Uint8Array): void {
  new SaciStream(bytes).component(manifest.component);
}

/** What was thrown, for a failure message that distinguishes a native error from a refusal. */
function describeThrown(value: unknown): string {
  const kind = value instanceof Error ? value.constructor.name : typeof value;
  return `${kind}: ${String(value)}`;
}

test('the reason table and the manifest cover each other exactly', () => {
  const unmapped = manifest.reasons.filter((reason) => REASON_SUBSTRING[reason] === undefined);
  assert.deepStrictEqual(unmapped, [], 'add one row to REASON_SUBSTRING per new reason code');

  // The reverse direction matters as much. A row the corpus no longer lists
  // means this codec still believes it covers a case that has gone away, which
  // is a silent loss of coverage rather than a visible failure.
  const stale = Object.keys(REASON_SUBSTRING)
    .filter((reason) => !manifest.reasons.includes(reason))
    .sort();
  assert.deepStrictEqual(stale, [], 'REASON_SUBSTRING maps reasons the corpus dropped');
});

for (const testCase of manifest.cases) {
  if (testCase.expect === 'accept') {
    test(`accept: ${testCase.name}`, () => {
      const spec = testCase.accept;
      assert.ok(spec !== undefined, `${testCase.name}: an accept case must carry an "accept" block`);

      const stream = new SaciStream(vectorBytes(testCase.vector));
      assert.deepStrictEqual(stream.componentNames(), [...spec.components]);

      const batch = stream.component(spec.component);
      assert.strictEqual(batch.rows, spec.rows, `${testCase.name}: row count`);

      for (const [field, column] of Object.entries(spec.columns)) {
        const actual = readColumn(batch, field, column.type);
        // Int64 columns come back as BigInt, because a JS number cannot hold
        // the full range; the manifest spells them as JSON numbers.
        const wanted: readonly unknown[] =
          column.type === 'int64' ? column.values.map((v) => BigInt(v as number)) : column.values;

        assert.strictEqual(actual.length, wanted.length, `${testCase.name}: ${field} length`);
        for (const [row, value] of wanted.entries()) {
          // float64 compares exactly. These are round-tripped bit patterns, not
          // computed values, so a tolerance would only hide a decoder bug.
          assert.strictEqual(actual[row], value, `${testCase.name}: ${field} row ${row}`);
        }
      }
    });
    continue;
  }

  test(`reject: ${testCase.name} (${testCase.reason})`, () => {
    const reason = testCase.reason;
    assert.ok(reason !== undefined, `${testCase.name}: a reject case must carry a reason`);
    const fragment = REASON_SUBSTRING[reason];
    assert.ok(fragment !== undefined, `${testCase.name}: no fragment mapped for reason "${reason}"`);

    const bytes = vectorBytes(testCase.vector);
    let thrown: unknown;
    let threw = false;
    try {
      if (testCase.op === undefined) {
        parseVector(bytes);
      } else {
        runOp(new SaciStream(bytes), testCase.op);
      }
    } catch (err) {
      threw = true;
      thrown = err;
    }

    assert.ok(threw, `${testCase.name}: the codec accepted input the corpus says it must refuse`);
    // A native RangeError or TypeError fails the case even though something was
    // thrown: a caller that cannot tell a malformed stream from a bug in the
    // codec has no way to decide whether to retry, report, or crash.
    assert.ok(
      thrown instanceof ArrowIpcError,
      `${testCase.name}: expected ArrowIpcError, got ${describeThrown(thrown)}`,
    );
    assert.ok(
      thrown.message.includes(fragment),
      `${testCase.name}: message ${JSON.stringify(thrown.message)} does not contain ${JSON.stringify(fragment)}`,
    );
  });
}
