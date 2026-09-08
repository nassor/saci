+++
title = "A Python processor"
description = "componentize-py, a dataclass that is the component, transforms registered while the build snapshots the CPython heap, and the two gotchas that only appear inside the component."
template = "page.html"
weight = 3
aliases = ["/processors/python/", "/guests/python/"]
+++
# A Python processor

`enrich-py.wasm` is a component that `saci-service` loads from a `wasm` node in
a KDL workflow. It reads `valid`, `currency` and `amount` from every row and
writes `usd_amount` with its display string. Every block below is stage 2 of the
polyglot example, `examples/polyglot/stages/python-enrich/`, which CI builds.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 210" role="img" aria-labelledby="py-title py-desc">
        <title id="py-title">The Python stage between a CSV source and a CSV sink</title>
        <desc id="py-desc">
            A file source reads examples/configs/fixtures/polyglot_orders.csv, six Order rows
            with twelve columns each. Those rows go to the wasm node named enrich, which loads
            enrich-py.wasm, the Python component built by componentize-py. The component fills
            the usd_amount column and its display string, leaves the other ten columns as it
            found them, and hands the rows on to a file sink that writes
            /tmp/saci-polyglot-out.csv. The workflow file standalone_polyglot.kdl supplies the
            three exchange rate keys fx_eur, fx_gbp and fx_jpy to the component through host
            config.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="30" width="170" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="30" width="170" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="42" width="170" height="8"/>
            <text class="t-lbl" x="12" y="45">csv_orders</text>
            <text class="t-sm" x="12" y="66">polyglot_orders.csv</text>
            <text class="t-sm" x="12" y="79">6 rows, 12 columns</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M170 58 H245" marker-end="url(#py-d)"/>
            <rect class="blk blk-bnd" x="245" y="30" width="170" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="245" y="30" width="170" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="245" y="42" width="170" height="8"/>
            <text class="t-lbl" x="257" y="45">enrich-py.wasm</text>
            <text class="t-sm t-bnd" x="257" y="66">Python, componentize-py</text>
            <text class="t-sm t-data" x="257" y="79">+ usd_amount</text>
            <text class="t-sm t-data" x="257" y="92">+ usd_amount_display</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M415 58 H490" marker-end="url(#py-d)"/>
            <rect class="blk blk-data" x="490" y="30" width="170" height="56" rx="8"/>
            <rect class="hd hd-data" x="490" y="30" width="170" height="20" rx="8"/>
            <rect class="hd hd-data" x="490" y="42" width="170" height="8"/>
            <text class="t-lbl" x="502" y="45">csv_out</text>
            <text class="t-sm" x="502" y="66">saci-polyglot-out.csv</text>
            <text class="t-sm" x="502" y="79">same 12 columns</text>
        </g>
        <g class="anim anim-4">
            <rect class="blk blk-ctl" x="245" y="150" width="170" height="44" rx="8"/>
            <rect class="hd hd-ctl" x="245" y="150" width="170" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="245" y="162" width="170" height="8"/>
            <text class="t-lbl" x="257" y="165">standalone_polyglot.kdl</text>
            <text class="t-sm" x="257" y="186">fx_eur fx_gbp fx_jpy</text>
            <path class="arw arw-ctl" d="M330 150 V104" marker-end="url(#py-c)"/>
        </g>
        <defs>
            <marker id="py-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="py-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> Order rows, and the columns this stage fills</span>
        <span class="k-boundary"><i></i> the WebAssembly component boundary</span>
        <span class="k-control"><i></i> the config keys the workflow file injects</span>
    </div>
    <figcaption class="dgm-cap">
        The component sees the whole row and writes two columns of it. Everything else in the
        output CSV, <code>settlement</code> and <code>fee</code> included, comes straight out
        of the input, because the stages that write those columns are not in this workflow.
    </figcaption>
</div>

## What you need

Python 3.10 or newer runs the stage source; CI verifies 3.14. `componentize-py`
0.25.0 turns it into a component, and it is the only build tool this page needs
beyond the `wasm-tools` that [the build hub](@/service/processors/build/_index.md)
installs once.

```bash,name=Install componentize-py
pip install componentize-py==0.25.0
```
Runs the same on Linux, macOS and Windows (PowerShell).

The SDK's coordinate is `saci-sdk` and it imports as `saci_sdk`. It carries the
Arrow IPC codec with it, as the internal `saci_sdk.arrow_ipc` module, so a
processor resolves one package and nothing else. Outside this repository, install
the wheel; [the SDK packages page](@/library/reference/sdk-packages.md) says where
it comes from.

```bash,name=Install the Python SDK
pip install saci_sdk-0.1.0-py3-none-any.whl
```
Runs the same on Linux, macOS and Windows (PowerShell).

Inside this repository the wheel is unnecessary. The SDK is plain Python under
`packages/saci-sdk-py/src`, and the build command names that directory directly.
`componentize-py` resolves imports once, during its pre-init snapshot, from the
directories named by `-p`, which defaults to `.`. That is the whole reason the
build command in step 5 carries `-p` twice.

## 1. Create the project

The stage is two files: a `requirements.txt` that pins the builder, and an
`app.py` that holds the row type, the transforms and the export.

```text,name=examples/polyglot/stages/python-enrich/requirements.txt
componentize-py==0.25.0
```

Generating the bindings is optional and buys you editor completion on the
generated records. Run it from the stage directory:

```bash,name=Write the binding stubs
componentize-py -d ../../../../crates/saci-processor/wit -w saci-pipeline bindings .
```

Windows (PowerShell):

```powershell
componentize-py -d ..\..\..\..\crates\saci-processor\wit -w saci-pipeline bindings .
```

The directory now holds `wit_world/`, `componentize_py_types.py`,
`componentize_py_async_support/` and `poll_loop.py` beside your two files. All of
them are gitignored here, because `pipeline.wit` is the source of truth and a
committed copy would drift from it.

<div class="note note-warn">
<span class="note-label">bindings output is stubs, nothing more</span>

The `bindings` command writes files for the IDE's benefit only. `componentize`
regenerates the real bindings itself and never reads them from disk; the build
succeeds with them deleted. It is also not idempotent, so a second run fails
with "Cannot create a file when that file already exists". `cargo xtask polyglot`
skips the step entirely for that reason.

</div>

## 2. Declare the row type

A `@dataclass` under `@saci_sdk.component` is the component. The class name is the
wire component name and the field names are the column names, both verbatim: a
component is a cross-language contract, so nothing here renames anything.

```python,name=The dataclass that is the component
from dataclasses import dataclass

import saci_sdk


@saci_sdk.component
@dataclass
class Order:
    id: int
    region: str
    currency: str
    amount: float
    valid: bool = False
    usd_amount: float = 0.0
    usd_amount_display: str = ""
    risk_score: float = 0.0
    flagged: bool = False
    fee: float = 0.0
    review_tier: int = 0
    settlement: str = ""
```

`int`, `float`, `bool` and `str` annotations become `Int64`, `Float64`,
`Boolean` and `Utf8` columns, in declaration order. That declaration order is
the wire order, so moving a field is a wire change. Two stages agree when they
declare the same field names in the same order, and
[the wire format](@/library/reference/wire-format.md) specifies the algorithm.

Every refusal, an annotation the SDK cannot map or a field with `init=False`,
is raised while the module is imported. That import is part of the build, so a
component that would encode the wrong schema fails the build instead of the
batch.

<div class="note note-warn">
<span class="note-label">Imports must sit at module top level</span>

componentize-py resolves imports at build time only. A function-local
`import` works under plain CPython, then fails inside the component with no
obvious connection to the code that moved. Keep every `import` at module scope,
in this file and in the packages it names.

</div>

## 3. Write the transform

`@saci_sdk.transform(Order)` runs once per row, with a mutable instance of the
dataclass. Whatever it leaves on the row is what gets encoded:

```python,name=The per row enrich transform
@saci_sdk.transform(Order)
def enrich(row, config):
    """Convert one order into USD, or zero a rejected one."""
    if row.valid:
        row.usd_amount = row.amount * _rate(config, row.currency)
        row.usd_amount_display = f"{row.usd_amount:.2f} USD"
    else:
        row.usd_amount = 0.0
        row.usd_amount_display = ""
```

Rejected rows are zeroed rather than converted, so a row an upstream validator
turned down carries no misleading money downstream.

The rate per currency comes from one config key each, with a fallback the stage
carries itself. An unrecognised code converts one to one, which keeps the row
visible to a later risk stage instead of collapsing it to zero:

```python,name=One config key per non-USD currency
_RATES = {
    "EUR": ("fx_eur", 1.10),
    "GBP": ("fx_gbp", 1.30),
    "JPY": ("fx_jpy", 0.0068),
}

#: Reporting currency, and the rate used for any code not listed above.
_IDENTITY_RATE = 1.0


def _rate(config, currency):
    """This batch's rate for a currency: host config, then the fallback."""
    entry = _RATES.get(currency)
    if entry is None:
        return _IDENTITY_RATE
    key, default = entry
    return config.float(key, default)
```

`@saci_sdk.batch(Order)` runs once, after every per-row transform has seen every
row, with the list of rows. A batch total belongs here, because a per-row
`metric` call would report six rows as six observations of one:

```python,name=The batch report
@saci_sdk.batch(Order)
def report(rows, config):
    total = sum(row.usd_amount for row in rows)
    converted = sum(1 for row in rows if row.valid)
    config.metric("enrich.usd_total", total)
    config.log(
        "info",
        "enrich",
        f"converted {converted} of {len(rows)} rows, {total:.2f} USD total",
    )
```

## 4. Export it

One module-level name is the whole export. `componentize-py` looks up `Pipeline`,
and `saci_sdk.processor` returns a class subclassing the generated
`wit_world.exports.Pipeline`, exactly as a hand-written processor would:

```python,name=The module level export
Pipeline = saci_sdk.processor("polyglot-enrich-py", "0.1.0", enrich, report)
```

Transforms are grouped by the component they were registered against and run in
the order given, so `enrich` sees every row before `report` sees the batch.

The descriptor and the schema bytes are built inside that call, at import time,
which is componentize-py's pre-init pass. The finished component therefore
starts with both in memory rather than deriving them on the first batch.

## 5. Build and validate

The stage source runs under plain CPython too, where `saci_sdk.LOCAL_HOST` stands
in for the host's config, log and metric calls. Driving your processor class
there before building is the fastest way to see a transform's output, and the
wire bytes are real either way, because both paths go through the same codec.

Build from the stage directory. `-p .` finds `app.py`, and the second `-p` finds
the SDK:

```bash,name=Build the component
componentize-py -d ../../../../crates/saci-processor/wit -w saci-pipeline componentize app \
    -p . -p ../../../../packages/saci-sdk-py/src -o enrich-py.wasm
```

Windows (PowerShell):

```powershell
componentize-py -d ..\..\..\..\crates\saci-processor\wit -w saci-pipeline componentize app -p . -p ..\..\..\..\packages\saci-sdk-py\src -o enrich-py.wasm
```

Do **not** pass `--stub-wasi`. The bundled CPython needs the real WASI imports,
and the host supplies them.

`enrich-py.wasm` now sits in the stage directory, carrying a whole prebuilt
CPython inside it. `cargo xtask polyglot` runs the same build and puts its copy at
`examples/polyglot/build/enrich-py.wasm`, which is the path the next step's
config names. Confirm the artifact with
[the two verify commands](@/service/processors/build/_index.md) on the build hub.

## 6. Run it under saci-service

`examples/configs/standalone_polyglot.kdl` runs this exact component end to end.
Its `wasm` node names the artifact and carries the three keys the stage reads:

```kdl,name=The wasm node in examples/configs/standalone_polyglot.kdl
// The Python stage carries the most config keys, so it exercises `get-config`.
wasm "enrich" module="examples/polyglot/build/enrich-py.wasm" {
    // FX rates the processor reads. Strings; the processor parses them.
    config fx_eur="1.10" fx_gbp="1.30" fx_jpy="0.0068"
}
```

Every path in that file is relative to the repository root, so run from there.
`validate` checks the config and the workflow graph, `serve` runs the workflow
once and exits, because the file asks for `one_shot`:

```bash,name=Build the component then run the service
cargo xtask polyglot
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- validate \
  --config examples/configs/standalone_polyglot.kdl --strict
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- serve \
  --config examples/configs/standalone_polyglot.kdl
```

Windows (PowerShell), one command per line:

```powershell
cargo xtask polyglot
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- validate --config examples/configs/standalone_polyglot.kdl --strict
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- serve --config examples/configs/standalone_polyglot.kdl
```

The run reads `examples/configs/fixtures/polyglot_orders.csv` and writes
`/tmp/saci-polyglot-out.csv`. `usd_amount` is the column this stage fills: it is
`0.0` in every input row and holds converted money in the output:

```bash,name=Read the column the stage wrote
cut -d, -f1,6 /tmp/saci-polyglot-out.csv
```

```text,name=The usd_amount column after the run
id,usd_amount
1,110.00000000000001
2,0.0
3,6800.0
4,60000.0
5,0.0
6,20000.0
```

Windows (PowerShell) prints the same six values as a table:

```powershell
Import-Csv /tmp/saci-polyglot-out.csv | Select-Object id, usd_amount
```

Rows 2 and 5 read `0.0` because the fixture seeds their `valid` as false. The
fixture seeds that column, and `settlement` as `PENDING`, because the stages
that would write them are not in this single-stage workflow.

## Config, logs and state

`config` is the whole of the host a transform can reach, and it offers three
calls. `config.float(key, default)` returns the default when the workflow
injected no value for the key, and raises `ValueError` for a value that will not
parse. It caches by key, so the per-row `_rate` lookup above costs one host call
per batch rather than one per row.

`config.metric(name, value)` records one observation, which the host exposes as
`saci_processor_metric{metric="<name>"}`. `config.log(level, target, message)`
writes one structured line, with `level` one of `trace`, `debug`, `info`, `warn`
or `error`.

A `print` in a processor goes nowhere. The host discards a processor's stdout and
stderr, so `config.log` is the only channel out of the component, and a debug
line has to go through it to be seen.

This stage is stateless: it returns no state blob and ignores the one it is
handed. That blob is the only thing that survives to the next batch, coming back
as the next call's prior, so nothing a module global holds is still there when
the following batch runs.

## Next

- [The WIT contract](@/service/processors/build/wit-contract.md): every record
  the descriptor fills in, and what the host checks it against.
- [Build your own processor](@/service/processors/build/_index.md): the verify
  commands, the six-language chain, and the other five recipes.
