// Everything in `@nassor/saci-sdk` that does not touch the host.
//
// A component declaration, a transform, the schema fingerprint, and the
// decode/encode pair that turns a batch into row objects and back. `index.ts`
// is the other half, and the split has exactly one cause: that file imports
// `saci:pipeline/host-io@0.3.0`, a specifier only jco resolves, so a test that
// reached it would fail on the import rather than on an assertion. With the
// host behind [`HostIo`], every line here runs under `node --test` and the WIT
// import stays in one file.
//
// The layer below is `./arrow_ipc.ts`, the codec that owns the wire format.
// This file owns the mapping from a TypeScript declaration onto it: camelCase
// property to snake_case column, `'i64' | 'f64' | 'bool' | 'utf8'` to Arrow
// type, and row objects to columns.

import {
  BoolColumn,
  Float64Column,
  Int64Column,
  SaciStream,
  SaciStreamWriter,
  Utf8Column,
  schemaIpc,
  type Column,
  type ColumnType,
} from './arrow_ipc.ts';

/**
 * Every refusal this SDK raises: a declaration it cannot map onto the wire
 * format, or a host config value it will not use.
 *
 * A dedicated subclass is what lets a caller tell a rejected declaration from a
 * bug in the SDK, the same reason `ArrowIpcError` exists one layer down. Inside
 * `run-batch` the distinction is invisible — every throw becomes
 * `run-error::permanent` — but a declaration is checked at module
 * initialisation, which is `jco componentize` time, so the message lands in a
 * build log where the name matters.
 */
export class SaciSdkError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SaciSdkError';
  }
}

/**
 * Component name of the host's liveness segment.
 *
 * Not a component: the host writes the dataset's row count as this segment's
 * single `alive` column, and a returned stream without it is rejected.
 */
const ALIVE_COMPONENT = '__alive';

/**
 * Schema version every component the SDK declares carries.
 *
 * The version is part of the schema fingerprint and of each segment's
 * `__saci_schema_version`, so it cannot be left implicit. Migrating a component
 * is a host-side concern — the registry owns the migration functions — and a
 * processor that declared a different version would only make its fingerprint
 * disagree with the host's.
 */
const SCHEMA_VERSION = 1;

/** FNV-1a 32-bit, the schema fingerprint's hash. */
const FNV_OFFSET = 2166136261;
const FNV_PRIME = 16777619;

const UTF8 = new TextEncoder();

/** The field types a component may declare. */
export type FieldType = 'i64' | 'f64' | 'bool' | 'utf8';

/** A component declaration: row property names to wire types. */
export type FieldMap = Readonly<Record<string, FieldType>>;

/** Arrow column type for each declared field type. */
const COLUMN_TYPE = {
  i64: 'int64',
  f64: 'float64',
  bool: 'bool',
  utf8: 'utf8',
} as const satisfies Record<FieldType, ColumnType>;

/** The value a row carries for one declared field type. */
type Cell<T> = T extends 'i64' | 'f64' ? number : T extends 'bool' ? boolean : string;

/**
 * The row type a transform on `S` receives.
 *
 * Erased entirely before jco runs, and worth having anyway: it is what turns
 * `row.usdAmount` into a checked `number` and a typo into a compile error,
 * without a code generator or a build step between the declaration and the
 * transform.
 */
export type InferRow<S> =
  S extends ComponentSpec<infer F> ? { -readonly [K in keyof F]: Cell<F[K]> } : never;

/** One declared field: the row property, the wire column it maps to, and its type. */
export interface SpecField {
  readonly key: string;
  readonly wire: string;
  readonly type: FieldType;
}

/**
 * A declared component: what [`component`] returns and [`transform`] reads.
 *
 * `declared` is the declaration as written, kept because [`InferRow`] reads the
 * row shape back out of it. `columns` is the same thing in declaration order
 * with wire names resolved, which is the order the segment's schema carries and
 * therefore part of the cross-language contract.
 */
export interface ComponentSpec<F extends FieldMap = FieldMap> {
  readonly name: string;
  readonly version: number;
  readonly declared: F;
  readonly columns: readonly SpecField[];
  /** This component's `component-descriptor.arrow-schema-ipc` bytes. */
  readonly arrowSchemaIpc: Uint8Array;
}

/**
 * `usdAmountDisplay` becomes `usd_amount_display`: the wire name of a row
 * property.
 *
 * Snake case is what the host's Arrow schemas use, and the C# and Kotlin SDKs
 * apply the same default, so one field spelled idiomatically in each language
 * still names one column. One underscore per uppercase letter, which is why an
 * acronym belongs in a name as `usdAmount` rather than `USDAmount`.
 */
function snakeCase(key: string): string {
  let out = '';
  for (const char of key) {
    const lower = char.toLowerCase();
    if (char === lower) {
      out += char;
      continue;
    }
    if (out.length > 0) {
      out += '_';
    }
    out += lower;
  }
  return out;
}

/**
 * Declare a component.
 *
 * Property order is wire order. The declaration is converted, checked and
 * encoded into descriptor bytes here, at module initialisation, which jco
 * snapshots into the component: nothing below runs on the hot path.
 */
export function component<const F extends FieldMap>(name: string, fields: F): ComponentSpec<F> {
  if (name.length === 0) {
    throw new SaciSdkError('saci sdk: a component name cannot be empty');
  }
  if (name === ALIVE_COMPONENT) {
    throw new SaciSdkError(`saci sdk: "${ALIVE_COMPONENT}" is the host's liveness segment`);
  }

  const columns: SpecField[] = [];
  const byWire = new Map<string, string>();
  // `Object.keys` preserves the declaration order of string keys, which is what
  // makes the field list the contract rather than an accident.
  for (const key of Object.keys(fields)) {
    const type = fields[key];
    if (COLUMN_TYPE[type] === undefined) {
      throw new SaciSdkError(
        `saci sdk: component "${name}" field "${key}" declares unknown type "${String(type)}"`,
      );
    }
    const wire = snakeCase(key);
    const clash = byWire.get(wire);
    if (clash !== undefined) {
      throw new SaciSdkError(
        `saci sdk: component "${name}" fields "${clash}" and "${key}" both map to wire column "${wire}"`,
      );
    }
    byWire.set(wire, key);
    columns.push({ key, wire, type });
  }
  if (columns.length === 0) {
    throw new SaciSdkError(`saci sdk: component "${name}" declares no fields`);
  }

  return {
    name,
    version: SCHEMA_VERSION,
    declared: fields,
    columns,
    arrowSchemaIpc: schemaIpc(
      columns.map((field) => ({ name: field.wire, type: COLUMN_TYPE[field.type] })),
    ),
  };
}

/**
 * One row of a component as the SDK holds it.
 *
 * Authored transforms never see this type: [`transform`] hands them
 * [`InferRow`], which names every field and its value type.
 */
export type Row = Record<string, number | boolean | string>;

/** One registered system: the component it reads, and what it does to a batch of rows. */
export interface Transform {
  readonly spec: ComponentSpec;
  readonly run: (rows: Row[], config: SaciConfig) => void;
}

/**
 * A per-row transform.
 *
 * Writes to `row` are what the batch returns: the SDK re-encodes the rows after
 * every transform has run, so a transform mutates plain objects and never
 * touches a buffer.
 */
export function transform<S extends ComponentSpec>(
  spec: S,
  fn: (row: InferRow<S>, config: SaciConfig) => void,
): Transform {
  return {
    spec,
    run(rows, config) {
      for (let i = 0; i < rows.length; i += 1) {
        fn(rows[i] as unknown as InferRow<S>, config);
      }
    },
  };
}

/**
 * A per-batch transform, for the work a row cannot see.
 *
 * A batch total, a metric, one log line: emitting those per row would multiply
 * them by the row count. Registered in the same list as [`transform`] and run
 * in the same order, so a batch transform placed after a row transform observes
 * its writes.
 *
 * `rows` is the batch: dropping entries shrinks the component, which the host
 * accepts as long as the result stays within the liveness bitmap.
 */
export function transformBatch<S extends ComponentSpec>(
  spec: S,
  fn: (rows: InferRow<S>[], config: SaciConfig) => void,
): Transform {
  return {
    spec,
    run(rows, config) {
      fn(rows as unknown as InferRow<S>[], config);
    },
  };
}

// ---------------------------------------------------------------------------
// Host capabilities
// ---------------------------------------------------------------------------

/** WIT `host-io.log-level`. */
export type LogLevel = 'trace' | 'debug' | 'info' | 'warn' | 'error';

/**
 * The `saci:pipeline/host-io@0.3.0` imports, as a value.
 *
 * `index.ts` binds the real ones. A test binds a stub, which is the only way to
 * exercise a processor natively: the WIT specifier does not resolve outside a
 * component.
 */
export interface HostIo {
  getConfig(key: string): string | undefined;
  metric(name: string, value: number): void;
  log(level: LogLevel, target: string, message: string): void;
}

/** The host capabilities a transform may reach for. */
export interface SaciConfig {
  /**
   * A numeric host config value, or `fallback` when the host set none.
   *
   * The value must be a positive, finite number. `get-config` hands over
   * strings and leaves parsing to the processor, and a knob that is not a usable
   * positive magnitude — a threshold, a rate, a window — is an operator error,
   * not a row error, so it fails the whole batch rather than silently scoring
   * every row `Infinity`.
   */
  float(key: string, fallback: number): number;
  /** Observe a named metric. The host records it as `saci_processor_metric`. */
  metric(name: string, value: number): void;
  /** Emit a structured log line, bridged to `tracing` on the host. */
  log(level: LogLevel, target: string, message: string): void;
}

/**
 * [`SaciConfig`] for one batch.
 *
 * Config reads are memoised because a transform runs per row and a host call
 * crosses the component boundary: `config.float('risk_threshold', 50000)` at
 * the top of a row transform costs one `get-config` per batch, not one per row.
 * The first `fallback` for a key wins, so declaring two different defaults for
 * one key is a bug the first read hides.
 */
class BatchConfig implements SaciConfig {
  private readonly io: HostIo;
  private readonly floats = new Map<string, number>();

  constructor(io: HostIo) {
    this.io = io;
  }

  float(key: string, fallback: number): number {
    const cached = this.floats.get(key);
    if (cached !== undefined) {
      return cached;
    }
    const raw = this.io.getConfig(key);
    let value = fallback;
    if (raw !== undefined) {
      value = Number(raw);
      if (!Number.isFinite(value) || value <= 0) {
        throw new SaciSdkError(
          `config "${key}" must be a positive number, got ${JSON.stringify(raw)}`,
        );
      }
    }
    this.floats.set(key, value);
    return value;
  }

  metric(name: string, value: number): void {
    this.io.metric(name, value);
  }

  log(level: LogLevel, target: string, message: string): void {
    this.io.log(level, target, message);
  }
}

// ---------------------------------------------------------------------------
// Processor
// ---------------------------------------------------------------------------

/** WIT `component-descriptor`. */
export interface ComponentDescriptor {
  name: string;
  arrowSchemaIpc: Uint8Array;
}

/** WIT `pipeline-descriptor`. */
export interface PipelineDescriptor {
  name: string;
  version: string;
  components: ComponentDescriptor[];
  stateful: boolean;
  schemaFingerprint: string;
}

/** WIT `run-metrics`. */
export interface RunMetrics {
  wallNs: bigint;
  rowsIn: bigint;
  rowsOut: bigint;
  systemsRun: number;
  retries: number;
}

/** WIT `run-result`. */
export interface RunResult {
  output: Uint8Array;
  checkpoint?: Uint8Array;
  metrics: RunMetrics;
  routes?: string[];
}

/**
 * The `saci:pipeline/pipeline@0.3.0` export surface.
 *
 * `export const pipeline = processor(...)` is what jco looks for: the world
 * exports an interface, so the entrypoint's export must be an object with one
 * method per interface function. `prior` is accepted and ignored — an SDK
 * processor is stateless, so it persists no checkpoint.
 */
export interface Processor {
  describe(): PipelineDescriptor;
  runBatch(input: Uint8Array): RunResult;
}

/** FNV-1a 32-bit over `bytes`, continuing from `hash`. */
function fnv1a(hash: number, bytes: Uint8Array): number {
  let acc = hash;
  for (const byte of bytes) {
    // `Math.imul` is the 32-bit multiply: `acc * FNV_PRIME` exceeds 2^53 and
    // would lose the low bits the hash is made of.
    acc = Math.imul(acc ^ byte, FNV_PRIME) >>> 0;
  }
  return acc;
}

/**
 * The 8-char lowercase hex `pipeline-descriptor.schema-fingerprint`.
 *
 * Component names sorted, then per component the name bytes, the version as
 * four little-endian bytes, and each field name in schema order. The host
 * computes the same walk over its own registry and refuses a processor whose
 * value disagrees, which is what makes a dropped or reordered field a load-time
 * failure instead of a mis-decoded column.
 */
function fingerprint(specs: readonly ComponentSpec[]): string {
  const version = new Uint8Array(4);
  const versionView = new DataView(version.buffer);
  let hash = FNV_OFFSET;
  for (const spec of specs) {
    hash = fnv1a(hash, UTF8.encode(spec.name));
    versionView.setUint32(0, spec.version, true);
    hash = fnv1a(hash, version);
    for (const field of spec.columns) {
      hash = fnv1a(hash, UTF8.encode(field.wire));
    }
  }
  return hash.toString(16).padStart(8, '0');
}

/** Read one component's rows out of `stream` as plain objects. */
function readRows(stream: SaciStream, spec: ComponentSpec): Row[] {
  const batch = stream.component(spec.name);

  // The batch's columns have to be the declared ones, in the declared order.
  // Re-encoding writes exactly what was declared, so a wider batch would lose
  // the extra columns silently, and a differently ordered one would return a
  // schema whose fingerprint no longer matches the host's registry. Both are
  // the declaration drifting from the host, which is a permanent failure rather
  // than something to paper over.
  const declared = spec.columns.map((field) => field.wire);
  const present = batch.fieldNames();
  if (present.length !== declared.length || present.some((name, i) => name !== declared[i])) {
    throw new SaciSdkError(
      `saci sdk: component "${spec.name}" declares [${declared.join(', ')}] but the batch carries [${present.join(', ')}]`,
    );
  }

  const rows = new Array<Row>(batch.rows);
  for (let row = 0; row < batch.rows; row += 1) {
    rows[row] = {};
  }
  // Column at a time, because that is how the codec reads: one bounds-checked
  // buffer walk per column rather than one per cell.
  for (const field of spec.columns) {
    switch (field.type) {
      case 'i64': {
        const values = batch.int64s(field.wire);
        for (let row = 0; row < rows.length; row += 1) {
          // `bigint` is what the wire carries and `number` is what a transform
          // wants to do arithmetic with. Above 2^53 this rounds, which is the
          // price of a row object whose fields are ordinary numbers.
          rows[row][field.key] = Number(values[row]);
        }
        break;
      }
      case 'f64': {
        const values = batch.float64s(field.wire);
        for (let row = 0; row < rows.length; row += 1) {
          rows[row][field.key] = values[row];
        }
        break;
      }
      case 'bool': {
        const values = batch.bools(field.wire);
        for (let row = 0; row < rows.length; row += 1) {
          rows[row][field.key] = values[row];
        }
        break;
      }
      case 'utf8': {
        const values = batch.strings(field.wire);
        for (let row = 0; row < rows.length; row += 1) {
          rows[row][field.key] = values[row];
        }
        break;
      }
    }
  }
  return rows;
}

/** Turn transformed rows back into the columns the writer encodes. */
function columnsOf(spec: ComponentSpec, rows: readonly Row[]): Column[] {
  const columns = new Array<Column>(spec.columns.length);
  for (let i = 0; i < spec.columns.length; i += 1) {
    const field = spec.columns[i];
    switch (field.type) {
      case 'i64':
      case 'f64': {
        const values = new Array<number>(rows.length);
        for (let row = 0; row < rows.length; row += 1) {
          values[row] = rows[row][field.key] as number;
        }
        // A transform that assigned a fractional value to an `i64` field is
        // refused by the codec rather than truncated here.
        columns[i] =
          field.type === 'i64' ? Int64Column(field.wire, values) : Float64Column(field.wire, values);
        break;
      }
      case 'bool': {
        const values = new Array<boolean>(rows.length);
        for (let row = 0; row < rows.length; row += 1) {
          values[row] = rows[row][field.key] as boolean;
        }
        columns[i] = BoolColumn(field.wire, values);
        break;
      }
      case 'utf8': {
        const values = new Array<string>(rows.length);
        for (let row = 0; row < rows.length; row += 1) {
          values[row] = rows[row][field.key] as string;
        }
        columns[i] = Utf8Column(field.wire, values);
        break;
      }
    }
  }
  return columns;
}

/**
 * Build a processor over `transforms`, reaching the host through `io`.
 *
 * [`processor`](../index.ts) is this with the real WIT imports bound; a test
 * passes a stub instead. The descriptor is computed once here, at module
 * initialisation, because jco snapshots an initialised module into the
 * component: `describe` then costs a lowering and nothing else.
 */
export function makeProcessor(
  io: HostIo,
  name: string,
  version: string,
  transforms: readonly Transform[],
): Processor {
  if (name.length === 0) {
    throw new SaciSdkError('saci sdk: a processor name cannot be empty');
  }
  if (version.length === 0) {
    throw new SaciSdkError(`saci sdk: processor "${name}" needs a version`);
  }
  if (transforms.length === 0) {
    throw new SaciSdkError(`saci sdk: processor "${name}" registers no transforms`);
  }

  // Declared components, sorted by name: the fingerprint is defined over a
  // sorted walk, and the descriptor lists what the host validates its sources
  // and sinks against.
  const specs: ComponentSpec[] = [];
  for (const registered of transforms) {
    if (!specs.includes(registered.spec)) {
      specs.push(registered.spec);
    }
  }
  specs.sort((left, right) => (left.name < right.name ? -1 : left.name > right.name ? 1 : 0));
  for (let i = 1; i < specs.length; i += 1) {
    // Two `component()` calls with one name would give the host two schemas for
    // one segment, and which one the fingerprint covered would be arbitrary.
    if (specs[i].name === specs[i - 1].name) {
      throw new SaciSdkError(
        `saci sdk: processor "${name}" declares component "${specs[i].name}" twice`,
      );
    }
  }

  const descriptor: PipelineDescriptor = {
    name,
    version,
    components: specs.map((spec) => ({ name: spec.name, arrowSchemaIpc: spec.arrowSchemaIpc })),
    // An SDK processor keeps no state across batches: it has no checkpoint API,
    // and the host creates a fresh store per call.
    stateful: false,
    schemaFingerprint: fingerprint(specs),
  };

  return {
    describe(): PipelineDescriptor {
      return descriptor;
    },

    runBatch(input: Uint8Array): RunResult {
      // StarlingMonkey's clock is millisecond-resolution, so a small batch
      // reports 0 ns.
      const startedMs = Date.now();
      try {
        const config = new BatchConfig(io);
        const stream = new SaciStream(input);

        const rows = new Map<ComponentSpec, Row[]>();
        for (const spec of specs) {
          rows.set(spec, readRows(stream, spec));
        }
        for (const registered of transforms) {
          registered.run(rows.get(registered.spec) as Row[], config);
        }

        const alive = stream.component(ALIVE_COMPONENT).bools('alive');
        const writer = new SaciStreamWriter();
        for (const present of stream.componentNames()) {
          if (present === ALIVE_COMPONENT) {
            continue;
          }
          const spec = specs.find((candidate) => candidate.name === present);
          if (spec === undefined) {
            // A component this processor never declared. Its bytes go through
            // untouched: re-encoding one would need a schema version the
            // processor cannot know, and dropping it would delete host state.
            writer.writeSegment(stream.segmentBytes(present));
            continue;
          }
          writer.writeComponent(
            spec.name,
            spec.version,
            ...columnsOf(spec, rows.get(spec) as Row[]),
          );
        }
        writer.writeAlive(alive);

        // The liveness bitmap is the batch's row count, whichever components
        // this processor happens to declare.
        const batchRows = BigInt(alive.length);
        return {
          output: writer.toBytes(),
          checkpoint: undefined,
          metrics: {
            wallNs: BigInt(Date.now() - startedMs) * 1_000_000n,
            rowsIn: batchRows,
            rowsOut: batchRows,
            systemsRun: transforms.length,
            retries: 0,
          },
          // `routes` absent: the host multicasts the output to every downstream
          // link. Routing is a per-batch decision no transform can express yet.
          routes: undefined,
        };
      } catch (err) {
        // componentize-js lowers a thrown value into the WIT `err` arm, but it
        // *re-throws* anything `instanceof Error`, which traps the component.
        // Hence a plain object, always, and `permanent`: a malformed batch or a
        // bad config value will not fix itself on a retry, and `run-batch` must
        // never surface `schema-mismatch`.
        throw { tag: 'permanent', val: String(err) };
      }
    },
  };
}
