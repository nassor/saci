"""Stage 2 of the polyglot example: the **Python** processor.

Reads `valid` (written by the Go stage), `currency` and `amount`, and writes
`usd_amount` with its display string:

    usd_amount = amount * rate(currency)   when valid
    usd_amount = 0.0                       otherwise

Rejected rows are zeroed rather than converted, so a row the validator dropped
carries no misleading money downstream. `usd_amount_display` is that number
formatted, and it is the one `Utf8` column no in-place processor could write: a
different string length moves the values buffer, the offsets buffer and the
`Buffer` entries describing both. This stage re-encodes its whole segment
through `saci_sdk` instead of patching bytes, so the string costs it nothing.

Exchange rates come from `pipeline.wasm.config` via `get-config`, falling back
to the constants below when a key is absent. `USD` is the reporting currency
and is never configurable. An unrecognised code converts 1:1, the conservative
choice: it keeps the row visible to the risk stage instead of silently
collapsing it to zero.

This stage is stateless: `prior` is ignored and `checkpoint` is `none`. The
host creates a fresh wasmtime `Store` per `run-batch`, so the bundled CPython
re-initialises every batch. That cost is why a hot inner loop belongs in the
Rust or Go stage rather than here.

Everything else a processor owes the WIT world — the descriptor, the schema
bytes, the fingerprint, the Arrow decode and re-encode, the error mapping — is
`saci_sdk.processor`. The dataclass below *is* the component, and the decorators
register it while componentize-py builds: that build imports this file once and
snapshots the CPython heap, so the schema is derived at build time and the
finished component starts with it in memory.

Build (from this directory):

    componentize-py -d ../../../../crates/saci-processor/wit -w saci-pipeline bindings .
    componentize-py -d ../../../../crates/saci-processor/wit -w saci-pipeline componentize app \
        -p . -p ../../../../packages/saci-sdk-py/src -o enrich-py.wasm

No `--stub-wasi`: the bundled CPython needs the real WASI imports, which the
host supplies through `wasmtime_wasi::p2::add_to_linker_sync`.

componentize-py resolves imports at build time only, and from the directories
named by `-p`, which is why the SDK's `src` is on that
list. Every import in this file is module level: a function-local import is a
runtime `ImportError` inside the component, not a build failure.
"""

from dataclasses import dataclass

import saci_sdk

#: Config key and fallback rate per currency the host may override.
_RATES = {
    "EUR": ("fx_eur", 1.10),
    "GBP": ("fx_gbp", 1.30),
    "JPY": ("fx_jpy", 0.0068),
}

#: Reporting currency, and the rate used for any code not listed above.
_IDENTITY_RATE = 1.0


@saci_sdk.component
@dataclass
class Order:
    """The component all six stages of the example share.

    Field order is the cross-language contract: it feeds the schema fingerprint
    the host checks at load time and the buffer walk every SDK codec performs.
    Each of the six stages declares this schema in its own
    language, and the six-way fingerprint check the host runs at load time keeps
    the declarations in agreement: a stage that drifts is refused.
    """

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


def _rate(config, currency):
    """This batch's rate for a currency: host config, then the fallback."""
    entry = _RATES.get(currency)
    if entry is None:
        return _IDENTITY_RATE
    key, default = entry
    return config.float(key, default)


@saci_sdk.transform(Order)
def enrich(row, config):
    """Convert one order into USD, or zero a rejected one."""
    if row.valid:
        row.usd_amount = row.amount * _rate(config, row.currency)
        row.usd_amount_display = f"{row.usd_amount:.2f} USD"
    else:
        row.usd_amount = 0.0
        row.usd_amount_display = ""


@saci_sdk.batch(Order)
def report(rows, config):
    """The batch's own numbers, reported once: `run-batch` is what the host measures."""
    total = sum(row.usd_amount for row in rows)
    converted = sum(1 for row in rows if row.valid)
    config.metric("enrich.usd_total", total)
    config.log(
        "info",
        "enrich",
        f"converted {converted} of {len(rows)} rows, {total:.2f} USD total",
    )


#: The `saci:pipeline/pipeline` export. componentize-py looks up this name.
Pipeline = saci_sdk.processor("polyglot-enrich-py", "0.1.0", enrich, report)
