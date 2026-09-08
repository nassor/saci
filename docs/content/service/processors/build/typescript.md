+++
title = "A TypeScript processor"
description = "jco on Node 24, a component declared as an object literal, the row type inferred from it, and the four jco gotchas the SDK cannot absorb."
template = "page.html"
weight = 4
aliases = ["/processors/typescript/", "/guests/typescript/", "/guests/javascript/", "/processors/javascript/"]
+++
# A TypeScript processor

`score-ts.wasm` is a WASI 0.2 component built by `jco` from one TypeScript
file. It reads the `usd_amount` column of an `Order` row and writes
`risk_score` and `flagged`. It runs under `saci-service` against a CSV fixture,
and the two columns show up in the output file.

Every block is from `examples/polyglot/stages/ts-score/`, stage 3 of the polyglot
example. TypeScript costs one config file and buys a compile error whenever a
transform stops matching its row type.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 124" role="img" aria-labelledby="ts-title ts-desc">
        <title id="ts-title">The score stage reads one CSV file and writes two columns into another</title>
        <desc id="ts-desc">
            A FileSource reads examples/configs/fixtures/polyglot_orders.csv, whose rows carry all
            twelve Order columns. It hands the batch as Arrow IPC bytes to the WebAssembly node
            score-ts, which reads usd_amount and writes risk_score and flagged. The node hands the
            batch on to a FileSink, which writes /tmp/saci-polyglot-out.csv with those two columns
            filled in. No column is added or removed anywhere along the way.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="40" width="180" height="62" rx="8"/>
            <rect class="hd hd-data" x="0" y="40" width="180" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="52" width="180" height="8"/>
            <text class="t-lbl" x="12" y="55">polyglot_orders.csv</text>
            <text class="t-sm" x="12" y="78">FileSource, csv transformer</text>
            <text class="t-sm" x="12" y="93">twelve Order columns</text>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="240" y="40" width="180" height="62" rx="8"/>
            <rect class="hd hd-bnd" x="240" y="40" width="180" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="240" y="52" width="180" height="8"/>
            <text class="t-lbl" x="252" y="55">wasm score-ts</text>
            <text class="t-sm t-bnd" x="252" y="78">reads usd_amount</text>
            <text class="t-sm t-bnd" x="252" y="93">writes risk_score, flagged</text>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="480" y="40" width="180" height="62" rx="8"/>
            <rect class="hd hd-data" x="480" y="40" width="180" height="20" rx="8"/>
            <rect class="hd hd-data" x="480" y="52" width="180" height="8"/>
            <text class="t-lbl" x="492" y="55">saci-polyglot-out.csv</text>
            <text class="t-sm" x="492" y="78">FileSink, under /tmp</text>
            <text class="t-sm" x="492" y="93">same twelve columns</text>
        </g>
        <g class="anim anim-4">
            <text class="t-sm t-mid" x="210" y="34">Arrow IPC</text>
            <path class="arw arw-data" d="M180 70 H240" marker-end="url(#ts-arw)"/>
            <text class="t-sm t-mid" x="450" y="34">Arrow IPC</text>
            <path class="arw arw-data" d="M420 70 H480" marker-end="url(#ts-arw)"/>
        </g>
        <defs>
            <marker id="ts-arw" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane: CSV in, Arrow IPC across the links, CSV out</span>
        <span class="k-boundary"><i></i> the WebAssembly component this page builds</span>
    </div>
    <figcaption class="dgm-cap">
        A processor mutates the columns it declares and forwards the rest byte for byte, so
        the input and output files carry the same twelve columns. Only <code>risk_score</code>
        and <code>flagged</code> differ between them.
    </figcaption>
</div>

## What you need

Node 24.12 or newer; CI runs 24. jco sets that floor, not TypeScript.
`node --test` strips types from 22.18, but jco's `@napi-rs/lzma` declares
`engines: ^22.20 || ^24.12 || >=25`.

`jco` 1.30.0 does the componentizing, and `typescript` 5.9.3 with
`@types/node` 24.10.1 does the checking. The processor itself imports
`@nassor/saci-sdk`, which owns the descriptor, the Arrow decode and encode and
the `run-error` shape.

```bash,name=Install jco, TypeScript and the SDK
npm install --save-dev @bytecodealliance/jco@1.30.0 typescript@5.9.3 @types/node@24.10.1
npm install ./nassor-saci-sdk-0.1.0.tgz
```
Runs the same on Linux, macOS and Windows (PowerShell).

The npm tarball is an asset of the `sdk-v0.1.0` release, which is not cut yet, so
a build inside this repository resolves the SDK through a `file:` specifier
pointing at `packages/saci-sdk-ts` instead. The [SDK package
coordinates](@/library/reference/sdk-packages.md) list every language's name and
install command.

## 1. Create the project

`package.json` carries the two build scripts and pins the toolchain:

```json,name=package.json for the score stage
{
  "name": "polyglot-score-ts",
  "version": "0.1.0",
  "private": true,
  "description": "Stage 3 of the SACI polyglot example: scores usd_amount into risk_score/flagged.",
  "type": "module",
  "engines": {
    "node": ">=24.12"
  },
  "scripts": {
    "build": "jco componentize score.ts --wit ../../../../crates/saci-processor/wit --world-name saci-pipeline --disable http --disable fetch-event -o score-ts.wasm",
    "types": "jco types ../../../../crates/saci-processor/wit --world-name saci-pipeline -o types",
    "typecheck": "npm run types && tsc"
  },
  "dependencies": {
    "@nassor/saci-sdk": "file:../../../../packages/saci-sdk-ts"
  },
  "devDependencies": {
    "@bytecodealliance/jco": "1.30.0",
    "@types/node": "24.10.1",
    "typescript": "5.9.3"
  }
}
```

`"type": "module"` is mandatory, because jco only consumes ES modules. `--bundle`
is absent because jco bundles a TypeScript entrypoint on its own, and bundling is
not optional. `engines` restates jco's own floor so an old Node fails by name.
The `types` script writes the WIT world's TypeScript declarations into `types/`,
which is generated and never committed.

`tsconfig.json` emits nothing. `jco componentize` transpiles for the component,
so `tsc` runs as a checker only:

```json,name=tsconfig.json runs as a checker only
{
  "compilerOptions": {
    "target": "es2023",
    "lib": ["ES2023"],
    "module": "nodenext",
    "moduleResolution": "nodenext",
    "types": ["node"],
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true,
    "allowImportingTsExtensions": true,
    "erasableSyntaxOnly": true,
    "verbatimModuleSyntax": true,
    "noEmit": true,
    "skipLibCheck": true
  },
  "include": ["*.ts", "types/**/*.d.ts"]
}
```

`erasableSyntaxOnly` is the important one. It rejects enums, namespaces and
parameter properties, the TypeScript features Node cannot strip, so a source that
type-checks is a source Node can run. It is also why the SDK declares a component
with an object literal rather than decorators, which do not survive the transform
jco runs.

The versioned host-io import specifier needs a home, and a tsconfig `paths` entry
is the wrong one. `wit.d.ts` gives it one:

```ts,name=wit.d.ts types the versioned host-io import
declare module 'saci:pipeline/host-io@0.3.0' {
  export { log, metric, getConfig, type LogLevel } from './types/interfaces/saci-pipeline-host-io.js';
}
```

The stage directory now holds `package.json`, `tsconfig.json` and `wit.d.ts`.

## 2. Declare the row type

`component` takes the declaration and returns a spec. Property order is wire
order, and the SDK converts camelCase property names to snake_case columns, so
`usdAmountDisplay` addresses `usd_amount_display`. `InferRow` turns the same
declaration into the row type, so no code generator sits between the two.

```ts,name=score.ts declares the component and its row type
import { component, transform, transformBatch, processor, type InferRow } from '@nassor/saci-sdk';

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
```

`'i64' | 'f64' | 'bool' | 'utf8'` are the wire format's four types. The
declaration is checked and encoded into descriptor bytes at module
initialisation, which jco snapshots into the component. An unknown type, or two
properties colliding on one column name, is a build-time throw.

Two stages agree when they declare the same field names in the same order, and
[the wire format](@/library/reference/wire-format.md) specifies the algorithm
that turns those names into the fingerprint both stages report.

## 3. Write the transform

A `transform` runs over one row. Writes to `row` are what the batch returns,
because the SDK re-encodes the rows after every transform has run:

```ts,name=The per row scoring transform
const score = transform(Order, (row: Order, config) => {
  const threshold = config.float('risk_threshold', DEFAULT_RISK_THRESHOLD);
  row.riskScore = row.usdAmount / threshold;
  row.flagged = row.riskScore >= 1.0;
});
```

A `transformBatch` runs once over the whole batch rather than once per row. It
is registered in the same list as a `transform` and runs in the same order, so
it sees whatever an earlier one wrote. One metric observation and one log line
belong here; per row they would each be multiplied by the row count:

```ts,name=The batch report
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
```

## 4. Export it

The `saci-pipeline` world exports an interface, so jco looks for an object with
one method per interface function. `processor` returns exactly that, so the
author writes one exported binding:

```ts,name=The one export the world requires
export const pipeline = processor('polyglot-score-ts', '0.1.0', score, report);
```

Transforms run in registration order, so `score` fills the two columns before
`report` counts them. A segment the processor never declared is forwarded byte
for byte. The `__alive` bitmap is not one of those: the codec decodes it once
and rebuilds it after the transforms have run, so its bytes are re-encoded.

## 5. Build and validate

`npm run typecheck` regenerates `types/` from the WIT world and then runs `tsc`.
`npm run build` runs the `jco componentize` line from `package.json`:

```bash,name=Type check the stage then componentize it
npm run typecheck && npm run build
```

Windows (PowerShell):

```powershell
npm run typecheck; npm run build
```

The stage directory now holds `score-ts.wasm`, about 12 MB, because
StarlingMonkey ships inside it. Confirm it is a well-formed component that
imports and exports the right interfaces with
[the two verify commands](@/service/processors/build/_index.md) on the build hub.

## 6. Run it under saci-service

`examples/configs/standalone_polyglot.kdl` is a one-stage workflow: a
`FileSource` reading `examples/configs/fixtures/polyglot_orders.csv`, one `wasm`
node, and a `FileSink` writing `/tmp/saci-polyglot-out.csv`. It ships pointing at
the Python stage, so two things change to run this one. The `wasm` node's
`module` becomes this component, and its `config` carries the one key this stage
reads, `risk_threshold`:

```kdl,name=The wasm node for the score stage
wasm "score" module="examples/polyglot/build/score-ts.wasm" {
    // The USD volume at which a row scores 1.0. A string; the processor parses it.
    config risk_threshold="50000"
}

link from="csv_orders" to="score"
link from="score" to="csv_out"
```

`cargo xtask polyglot` builds all six stages into `examples/polyglot/build/`,
which is where that `module` path points. Run all three commands from the
repository root:

```bash,name=Build the processors then validate and serve
cargo xtask polyglot
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- validate \
  --config examples/configs/standalone_polyglot.kdl --strict
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- serve \
  --config examples/configs/standalone_polyglot.kdl
```

Windows (PowerShell), each command on one line:

```powershell
cargo xtask polyglot
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- validate --config examples/configs/standalone_polyglot.kdl --strict
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- serve --config examples/configs/standalone_polyglot.kdl
```

`validate` compiles the component, checks its WIT world, and walks every `link`
comparing the component names and schemas at both ends:

```text,name=Expected validate output
OK: workflow graph validated (components and schemas agree end to end)
OK: config is structurally valid
  node.id:  1
  node.name: saci-polyglot
  mode:     standalone
  workflow: polyglot
  processors: examples/polyglot/build/score-ts.wasm
  sources:  1
  sinks:    1
  http.bind: 127.0.0.1:0
  log_level: info
OK: all declared types resolved in built-in registry
```

`serve` then runs one batch of six rows and writes
`/tmp/saci-polyglot-out.csv`. The two columns this stage fills are `risk_score`
and `flagged`; the other ten arrive from the fixture unchanged. The fixture seeds
`usd_amount` at zero, because the Python stage that would write it is not in this
single-stage pipeline, so every row comes back with a zero score and `flagged`
false. Declare the Python `enrich` node ahead of this one and link the two to see
scores driven by real amounts.

## What breaks

<div class="note note-warn">
<span class="note-label">The versioned import, and why its types are not in tsconfig</span>

The host-io import specifier must carry its version,
`'saci:pipeline/host-io@0.3.0'`, not the unversioned form, which fails at wizer
time with `ReferenceError: Error loading module "saci:pipeline/host-io" ... No
such file or directory`. Typing it through a tsconfig `paths` entry then breaks
the build, because jco's bundler reads the same field, resolves the specifier to
a declaration file and reports `[MISSING_EXPORT] "getConfig" is not exported by
"types/interfaces/saci-pipeline-host-io.d.ts"`.
An ambient `declare module` in `wit.d.ts` is invisible to the bundler, so the
import stays external and still type-checks.

</div>

<div class="note note-warn">
<span class="note-label">An old Node fails as a missing native binding</span>

jco pulls in `@napi-rs/lzma`, whose `engines` are
`^22.20 || ^24.12 || >=25`. Its platform binding is an *optional* dependency,
and npm skips an optional dependency that fails its engine check without
failing the install. On Node 22.18 the tree installs cleanly and
`jco componentize` then dies on `Cannot find native binding`, pointing at an
npm bug that is not the cause. The stage declares `"engines": { "node":
">=24.12" }`, which turns that into an `EBADENGINE` line naming the version.

</div>

<div class="note note-warn">
<span class="note-label">Don't just <code>--disable http</code></span>

`--disable http` alone is not enough to drop `wasi:http`. Pair it with
`--disable fetch-event`, or the component still imports `wasi:http/types@0.2.x`
and fails to instantiate against a host that links plain WASI. In the other
direction, do **not** disable `clocks`: the SDK reads `Date.now()` for
`run-metrics.wall-ns`, and StarlingMonkey's clock is millisecond-resolution
already, so a small batch reports 0 ns.

</div>

<div class="note note-warn">
<span class="note-label">Values don't cross the boundary as themselves</span>

`wallNs`, `rowsIn` and `rowsOut` are `BigInt`, not `Number`, and the WIT
declarations say so; the SDK builds them. `list<u8>` arrives from a different
realm: componentize-js lifts it into a `Uint8Array` whose prototype is not the
local one, so `input instanceof Uint8Array` is `false` and
`input.constructor !== Uint8Array`, while `constructor.name` is still
`'Uint8Array'`. A realm-agnostic check is the only reliable one:
`ArrayBuffer.isView(x) && x.BYTES_PER_ELEMENT === 1`.

</div>

One more rule follows from how componentize-js lowers a failure. It lowers a
*thrown* value into the WIT `err` arm, but it re-throws anything that is an
`instanceof Error` instead of lowering it, which traps the component. So the SDK
throws a plain object, and your own code must never throw an `Error` out of a
transform:

```ts,name=How the SDK reports a permanent error
throw { tag: 'permanent', val: String(err) };
```

## Config, logs and state

`config` is the whole of `host-io` a transform can reach, and it carries three
methods. `config.float(key, fallback)` reads a numeric key, refusing a value that
is not a positive finite number, because a threshold that is not a usable
magnitude is an operator error rather than a row error. Reads are memoised per
batch, so asking per row costs one `get-config` call.

`config.metric(name, value)` observes a named metric, which the host records as
`saci_processor_metric`. `config.log(level, target, message)` emits a structured
log line, with `level` one of `'trace'`, `'debug'`, `'info'`, `'warn'` or
`'error'`.

`console.log` goes nowhere. The host discards a processor's stdout and stderr, so
`config.log` is the only channel out of a component, and a log line in a per-row
transform multiplies by the row count.

The state blob a processor returns is the only thing that survives to the next
batch, coming back as the next call's prior. The TypeScript SDK exposes no
checkpoint API, so a processor it builds is stateless. The descriptor reports
`stateful: false`, `prior` is accepted and ignored, and each batch starts from
its input alone.

## Next

- [The build hub](@/service/processors/build/_index.md): the two verify commands,
  and the same stage in the six-language chain.
- [The WIT contract](@/service/processors/build/wit-contract.md): every field the
  descriptor fills in, and what the host checks it against.
