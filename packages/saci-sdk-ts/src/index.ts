// Processor authoring for SACI.
//
// A stage is a component declaration, one or more transforms and one export:
//
//   const Order = component('Order', { id: 'i64', amount: 'f64' } as const);
//   type Order = InferRow<typeof Order>;
//
//   const score = transform(Order, (row: Order, config) => {
//     row.amount *= config.float('rate', 1.5);
//   });
//
//   export const pipeline = processor('my-stage', '0.1.0', score);
//
// No decorators: TC39 decorators do not survive the Oxc transform jco runs, and
// the legacy ones carry `design:type` metadata only, which cannot describe a
// field list. A declaration object is what both a bundler and a type checker
// understand, and `InferRow` turns it into the row type without a code
// generator in between.
//
// This file is the half that touches the host. It exists separately from
// `core.ts` because `saci:pipeline/host-io@0.3.0` resolves inside a component
// and nowhere else, so a test that imported it would fail on the import.

import { log, metric, getConfig } from 'saci:pipeline/host-io@0.3.0';

import { makeProcessor, type HostIo, type Processor, type Transform } from './core.js';
export * from './arrow_ipc.ts';

export {
  SaciSdkError,
  component,
  transform,
  transformBatch,
  type ComponentDescriptor,
  type ComponentSpec,
  type FieldMap,
  type FieldType,
  type InferRow,
  type LogLevel,
  type SaciConfig,
  type PipelineDescriptor,
  type Processor,
  type Row,
  type RunMetrics,
  type RunResult,
  type SpecField,
  type Transform,
} from './core.js';

/**
 * The WIT imports, bound once.
 *
 * A module-level binding rather than a lookup per call: jco snapshots an
 * initialised module into the component, so this costs nothing at run time and
 * keeps the imported names in one place.
 */
const HOST_IO: HostIo = { log, metric, getConfig };

/**
 * Build the processor a stage exports.
 *
 * `export const pipeline = processor(...)` is the shape jco looks for: the
 * `saci-pipeline` world exports an interface, so the entrypoint's export must be
 * an object with one method per interface function. Transforms run in
 * registration order.
 */
export function processor(
  name: string,
  version: string,
  ...transforms: readonly Transform[]
): Processor {
  return makeProcessor(HOST_IO, name, version, transforms);
}
