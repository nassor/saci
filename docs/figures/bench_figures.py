#!/usr/bin/env python3
"""Render the comparison charts on the benchmarks page.

    python3 docs/figures/bench_figures.py

Every measurement lives in this file exactly once, as the string criterion
printed plus the same quantity in nanoseconds. The script turns those into
geometry and never reformats a measurement.

The charts are spliced into `docs/content/library/reference/benchmarks.md` between
`<!-- fig:NAME -->` and `<!-- /fig:NAME -->` markers. The next run overwrites
whatever sits between them, so change a number here and re-run rather than
editing the SVG in that file.

Chart vocabulary (`.dgm .bar-*`, `.ax`, `.grid`, `.mark`, `.t-num`, `.t-ax`)
lives in `docs/static/styles.css` section 7. Amber is the SACI/Arrow path, teal
is whatever it is measured against, violet crosses the WebAssembly boundary,
grey is time no measured part accounts for.
"""

from __future__ import annotations

import math
import re
import textwrap
from pathlib import Path

PAGE = Path(__file__).resolve().parents[1] / "content" / "library" / "reference" / "benchmarks.md"

W = 660  # every diagram on the site is authored at 660 units; see styles.css
US = 1_000  # ns
MS = 1_000_000  # ns


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------


def n(v: float) -> str:
    """Coordinate with at most one decimal, and no trailing zero."""
    s = f"{v:.1f}"
    s = s.rstrip("0").rstrip(".")
    return "0" if s in ("", "-0") else s


def txt(x: float, y: float, s: str, cls: str = "t-sm") -> str:
    return f'<text class="{cls}" x="{n(x)}" y="{n(y)}">{s}</text>'


def bar(x: float, y: float, w: float, h: float, cls: str) -> str:
    # A 2-unit floor keeps a sub-pixel measurement visible as a stub rather than
    # vanishing, which would read as "not measured".
    return (
        f'<rect class="bar {cls}" x="{n(x)}" y="{n(y)}" '
        f'width="{n(max(w, 2))}" height="{n(h)}" rx="2"/>'
    )


def vline(x: float, y0: float, y1: float, cls: str = "grid") -> str:
    return f'<path class="{cls}" d="M{n(x)} {n(y0)} V{n(y1)}"/>'


def hline(y: float, x0: float, x1: float, cls: str = "grid") -> str:
    return f'<path class="{cls}" d="M{n(x0)} {n(y)} H{n(x1)}"/>'


class Log:
    """Log scale. Bars start at `lo`, so `lo` is a floor, not zero."""

    def __init__(self, lo: float, hi: float, x0: float, x1: float) -> None:
        self.a, self.b, self.x0, self.x1 = (
            math.log10(lo),
            math.log10(hi),
            x0,
            x1,
        )

    def __call__(self, v: float) -> float:
        return self.x0 + (math.log10(v) - self.a) / (self.b - self.a) * (self.x1 - self.x0)


class Lin:
    def __init__(self, hi: float, x0: float, x1: float) -> None:
        self.hi, self.x0, self.x1 = hi, x0, x1

    def __call__(self, v: float) -> float:
        return self.x0 + v / self.hi * (self.x1 - self.x0)


def axis(scale, ticks, y0: float, y1: float, y_lab: float, zero: bool = False) -> list[str]:
    """Gridlines plus their labels. `zero` also draws the baseline at scale(0)."""
    out = []
    if zero:
        out.append(vline(scale.x0, y0, y1, "ax"))
    for v, lab in ticks:
        out.append(vline(scale(v), y0, y1, "grid"))
        out.append(txt(scale(v), y_lab, lab, "t-ax t-mid"))
    return out


def rows(items, scale, *, y0: float, pitch: float, bh: float, x_name: float = 0,
         x_val: float = 655) -> list[str]:
    """One labelled bar per item, values right-aligned in a column of their own.

    An item is a dict: `name`, optional `detail` (second gutter line), `segs`
    (a list of `(nanoseconds, class)` laid end to end), `value`, and optional
    `factor` printed under the value.
    """
    out = []
    for i, r in enumerate(items):
        y = y0 + i * pitch
        out.append(txt(x_name, y + bh / 2 + 4, r["name"], "t-lbl"))
        if r.get("detail"):
            out.append(txt(x_name, y + bh + 13, r["detail"], "t-sm"))
        acc = 0.0
        for v, cls in r["segs"]:
            x_from, acc = scale(acc), acc + v
            out.append(bar(x_from, y, scale(acc) - x_from, bh, cls))
        out.append(txt(x_val, y + bh / 2 + 4, r["value"], "t-num t-end"))
        if r.get("factor"):
            out.append(txt(x_val, y + bh + 13, r["factor"], r.get("factor_cls", "t-sm") + " t-end"))
    return out


def figure(fid: str, title: str, desc: str, body: list[str], height: float,
           key: list[tuple[str, str]] | None = None, cap: str | None = None) -> str:
    """The `.dgm` frame every diagram on the site shares."""
    out = [
        '<div class="dgm animate-in">',
        f'    <div class="dgm-scroll"><svg viewBox="0 0 {W} {n(height)}" role="img" '
        f'aria-labelledby="{fid}-t {fid}-d">',
        f'        <title id="{fid}-t">{title}</title>',
        f'        <desc id="{fid}-d">',
    ]
    out += [f"            {ln}" for ln in textwrap.wrap(" ".join(desc.split()), 86)]
    out += ["        </desc>"]
    out += [f"        {ln}" for ln in body]
    out += ["    </svg>", "    </div>"]
    if key:
        out.append('    <div class="dgm-key">')
        out += [f'        <span class="{cls}"><i></i> {label}</span>' for cls, label in key]
        out.append("    </div>")
    if cap:
        out.append('    <figcaption class="dgm-cap">')
        out += [f"        {ln}" for ln in textwrap.wrap(" ".join(cap.split()), 86)]
        out.append("    </figcaption>")
    out.append("</div>")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# 1. Batch versus stream: the item size sweep
# ---------------------------------------------------------------------------

ITEM_SIZE = [
    # k, invocations, total, total ns, per invocation, per invocation ns, vs one batch
    ("100 000", "1", "266.3 µs", 266.3 * US, "266.3 µs", 266.3 * US, "1.0×"),
    ("10 000", "10", "274.2 µs", 274.2 * US, "27.42 µs", 27.42 * US, "1.03×"),
    ("1 000", "100", "356.3 µs", 356.3 * US, "3.563 µs", 3.563 * US, "1.34×"),
    ("100", "1 000", "1.139 ms", 1.139 * MS, "1.139 µs", 1.139 * US, "4.3×"),
    ("10", "10 000", "8.736 ms", 8.736 * MS, "874 ns", 874, "32.8×"),
    ("1", "100 000", "86.44 ms", 86.44 * MS, "864 ns", 864, "324.6×"),
]


def fig_item_size() -> str:
    x_k, x_calls, pl, pr, x_val, x_fac = 78, 150, 164, 548, 604, 660
    pitch, bh = 24, 14
    body: list[str] = []

    def panel(top: float, head: str, scale, ticks, cls: str, col: int, factors: bool) -> float:
        y0 = top + 24
        y_end = y0 + (len(ITEM_SIZE) - 1) * pitch + bh
        out = [
            txt(x_k, top + 11, "ITEM SIZE k", "t-ax t-end"),
            txt(x_calls, top + 11, "CALLS", "t-ax t-end"),
            txt(pl, top + 11, head, "t-ax"),
        ]
        if factors:
            out.append(txt(x_fac, top + 11, "VS ONE BATCH", "t-ax t-end"))
        out += axis(scale, ticks, y0 - 6, y_end + 4, y_end + 18)
        for i, row in enumerate(ITEM_SIZE):
            y = y0 + i * pitch
            last = i == len(ITEM_SIZE) - 1
            out += [
                txt(x_k, y + 11, row[0], "t-lbl t-end"),
                txt(x_calls, y + 11, row[1], "t-sm t-end"),
                bar(pl, y, scale(row[col + 1]) - pl, bh, cls),
                txt(x_val, y + 11, row[col], "t-num t-data t-end" if last else "t-num t-end"),
            ]
            if factors:
                out.append(txt(x_fac, y + 11, row[6], "t-sm t-end"))
        body.extend(out)
        return y_end + 26

    after = panel(
        0,
        "TOTAL WALL TIME · LOG SCALE",
        Log(100 * US, 100 * MS, pl, pr),
        [(100 * US, "100 µs"), (1 * MS, "1 ms"), (10 * MS, "10 ms"), (100 * MS, "100 ms")],
        "bar-data",
        2,
        True,
    )
    height = panel(
        after + 8,
        "COST PER INVOCATION · LOG SCALE",
        Log(500, 500 * US, pl, pr),
        [(1 * US, "1 µs"), (10 * US, "10 µs"), (100 * US, "100 µs")],
        "bar-data-2",
        4,
        False,
    )
    return figure(
        "bs",
        "Total wall time and per-invocation cost for 100 000 rows, swept by item size",
        """
        Both panels are logarithmic, one gridline per ten-fold step. Top, total wall time for
        the same 100 000 rows: one batch of 100 000 takes 266.3 µs, ten of 10 000 take 274.2 µs
        (1.03×), a hundred of 1 000 take 356.3 µs (1.34×), a thousand of 100 take 1.139 ms
        (4.3×), ten thousand of 10 take 8.736 ms (32.8×) and 100 000 single-row items take
        86.44 ms, 324.6× the single batch. Bottom, the same runs divided by their invocation
        count: 266.3 µs, 27.42 µs, 3.563 µs, 1.139 µs, 874 ns and 864 ns. The bottom three bars
        are nearly the same length. That plateau is the fixed cost of one invocation.
        """,
        body,
        height,
        key=[
            ("k-data", "total wall time for the whole 100 000 rows"),
            ("k-data-2", "the same run, divided by its invocation count"),
        ],
        cap="""
        Both panels plot the same six runs. The upper one is what the wall clock says; the lower
        one divides it by the invocation count, which is where the fixed cost of an invocation
        stops hiding. The bottom three bars are the same length because below about a hundred
        rows an invocation costs what it costs whatever it carries.
        """,
    )


# ---------------------------------------------------------------------------
# 2. Service-level latency
# ---------------------------------------------------------------------------

LATENCY = [
    (
        "native · source → systems → sink · n = 10 000",
        "bar-data",
        "t-data",
        [("mean", "1.0 µs", 1.0 * US, False), ("p50", "1 µs", 1 * US, False),
         ("p99", "2 µs", 2 * US, True), ("max", "6 µs", 6 * US, False)],
    ),
    (
        "WASM processor · run_on_with_state · n = 1 000",
        "bar-bnd",
        "t-bnd",
        [("mean", "179.4 µs", 179.4 * US, False), ("p50", "159 µs", 159 * US, False),
         ("p99", "420 µs", 420 * US, True), ("max", "678 µs", 678 * US, False)],
    ),
]


def fig_latency() -> str:
    x_stat, pl, pr = 96, 108, 560
    scale = Log(500, 1 * MS, pl, pr)
    ticks = [(1 * US, "1 µs"), (10 * US, "10 µs"), (100 * US, "100 µs"), (1 * MS, "1 ms")]
    pitch, bh, group_gap = 15, 9, 30
    body = [txt(0, 11, "ROUND TRIP, PRODUCER TO SINK · LOG SCALE", "t-ax")]
    y = 24
    tops = []
    for name, cls, tcls, stats in LATENCY:
        body.append(txt(0, y + 10, name, "t-lbl"))
        y += 18
        tops.append((y - 4, y + len(stats) * pitch))
        for stat, label, value, hot in stats:
            body += [
                txt(x_stat, y + bh, stat, "t-sm t-end"),
                bar(pl, y, scale(value) - pl, bh, cls),
                txt(scale(value) + 6, y + bh, label, f"t-num {tcls}" if hot else "t-num"),
            ]
            y += pitch
        y += group_gap
    y_end = y - group_gap
    body = (
        body[:1]
        + axis(scale, ticks, 20, y_end + 4, y_end + 18)
        + body[1:]
    )
    return figure(
        "lat",
        "Per-item round trip latency, native path against a WebAssembly processor",
        """
        Logarithmic, one gridline per ten-fold step. Native source to systems to sink, over
        10 000 items: mean 1.0 µs, p50 1 µs, p99 2 µs, max 6 µs. The same single-row round trip
        through a WebAssembly processor calling run_on_with_state, over 1 000 items: mean 179.4 µs,
        p50 159 µs, p99 420 µs, max 678 µs. Every WASM bar sits roughly two gridlines, two
        orders of magnitude, to the right of its native counterpart.
        """,
        body,
        y_end + 26,
        key=[
            ("k-data", "native, in-process"),
            ("k-boundary", "across the WebAssembly boundary"),
        ],
        cap="""
        Timed from the producer: send one single-row batch, wait for the transformed row to
        arrive at the sink. Sample counts differ: 10 000 native, 1 000 through the processor. The
        WASM tail is the thinly sampled half of the chart.
        """,
    )


# ---------------------------------------------------------------------------
# 3. Stage cost across the dispatch threshold
# ---------------------------------------------------------------------------

STAGE_COST = [
    ("256", "inline", "4.178 µs", 4.178 * US, "16.3 ns", 16.3),
    ("512", "inline", "4.470 µs", 4.470 * US, "8.7 ns", 8.7),
    ("1 024", "inline", "5.441 µs", 5.441 * US, "5.3 ns", 5.3),
    ("4 096", "inline", "11.02 µs", 11.02 * US, "2.7 ns", 2.7),
    ("16 384", "inline", "33.82 µs", 33.82 * US, "2.1 ns", 2.1),
    ("65 536", "inline", "198.4 µs", 198.4 * US, "3.0 ns", 3.0),
    ("131 072", "dispatched", "403.8 µs", 403.8 * US, "3.1 ns", 3.1),
    ("262 144", "dispatched", "813.7 µs", 813.7 * US, "3.1 ns", 3.1),
    ("1 048 576", "dispatched", "3.219 ms", 3.219 * MS, "3.1 ns", 3.1),
]


def fig_stage_cost() -> str:
    x_rows, al, ar, x_a = 78, 92, 300, 358
    bl, br, x_b = 372, 598, 655
    total = Log(3 * US, 4 * MS, al, ar)
    perrow = Lin(17, bl, br)
    pitch, bh, y0, seam_gap = 22, 13, 46, 22
    inline = sum(1 for r in STAGE_COST if r[1] == "inline")

    def row_y(i: int) -> float:
        # The threshold is a discontinuity in the plan, so the rows open up for
        # it: the label needs somewhere to sit that is not on top of a bar.
        return y0 + i * pitch + (seam_gap if i >= inline else 0)

    y_end = row_y(len(STAGE_COST) - 1) + bh
    body = [
        txt(x_rows, 13, "ROWS", "t-ax t-end"),
        txt(al, 13, "TOTAL TIME · LOG SCALE", "t-ax"),
        txt(bl, 13, "PER ROW · LINEAR", "t-ax"),
    ]
    body += axis(
        total,
        [(10 * US, "10 µs"), (100 * US, "100 µs"), (1 * MS, "1 ms")],
        y0 - 8,
        y_end + 4,
        y_end + 18,
    )
    body += axis(
        perrow,
        [(0, "0"), (5, "5 ns"), (10, "10 ns"), (15, "15 ns")],
        y0 - 8,
        y_end + 4,
        y_end + 18,
        zero=True,
    )
    for i, (label, path, t_str, t_ns, p_str, p_ns) in enumerate(STAGE_COST):
        y = row_y(i)
        cls = "bar-data" if path == "inline" else "bar-ctl"
        body += [
            txt(x_rows, y + 10, label, "t-lbl t-end"),
            bar(al, y, total(t_ns) - al, bh, cls),
            txt(x_a, y + 10, t_str, "t-num t-end"),
            bar(bl, y, perrow(p_ns) - bl, bh, cls),
            txt(x_b, y + 10, p_str, "t-num t-end"),
        ]
    seam = row_y(inline) - seam_gap / 2 - 1
    body += [
        txt(al, seam + 4, "STAGE_INLINE_THRESHOLD · 100 000 ROWS", "t-ax t-ctl"),
        hline(seam, al + 244, br, "mark"),
    ]
    return figure(
        "sc",
        "Two-system stage cost by row count, either side of the inline dispatch threshold",
        """
        Left, total time on a logarithmic scale; right, the same runs divided by row count, on a
        linear scale from zero to 17 nanoseconds. 256 rows inline, 4.178 µs, 16.3 ns per row.
        512 inline, 4.470 µs, 8.7 ns. 1 024 inline, 5.441 µs, 5.3 ns. 4 096 inline, 11.02 µs,
        2.7 ns. 16 384 inline, 33.82 µs, 2.1 ns. 65 536 inline, 198.4 µs, 3.0 ns. 131 072
        dispatched, 403.8 µs, 3.1 ns. 262 144 dispatched, 813.7 µs, 3.1 ns. 1 048 576
        dispatched, 3.219 ms, 3.1 ns. The per-row bars collapse from 16.3 ns to about 3 ns and
        then stay there, straight through the inline-to-dispatched transition.
        """,
        body,
        y_end + 26,
        key=[
            ("k-data", "stage ran inline, one system after the other"),
            ("k-control", "stage dispatched one spawn_blocking per system"),
        ],
        cap="""
        Each row runs whichever path the threshold selects at that size, so this is the cost
        curve a deployment gets, <b>not an A/B</b>. No size here is measured both ways, so the
        chart cannot locate the crossover. What it shows is that the transition leaves no step
        in the per-row curve.
        """,
    )


# ---------------------------------------------------------------------------
# 4. TPC-H Q1
# ---------------------------------------------------------------------------


def fig_q1() -> str:
    pl, pr, x_val = 200, 560, 655
    scale = Lin(3 * MS, pl, pr)
    ticks = [(0, "0"), (1 * MS, "1 ms"), (2 * MS, "2 ms"), (3 * MS, "3 ms")]
    head = [
        {
            "name": "scalar baseline",
            "detail": "one pass over a Vec of rows",
            "segs": [(1.287 * MS, "bar-ctl")],
            "value": "1.287 ms",
            "factor": "1.0×",
        },
        {
            "name": "SACI pipeline",
            "detail": "includes pipeline construction",
            "segs": [(2.806 * MS, "bar-data")],
            "value": "2.806 ms",
            "factor": "2.18× slower",
            "factor_cls": "t-sm t-data",
        },
    ]
    parts = [
        {"name": "setup", "segs": [(212.7 * US, "bar-data-3")], "value": "212.7 µs"},
        {"name": "filter", "segs": [(35.5 * US, "bar-data-2")], "value": "35.5 µs"},
        {"name": "compute", "segs": [(303.9 * US, "bar-data-2")], "value": "303.9 µs"},
        {"name": "aggregate", "segs": [(1.474 * MS, "bar-data")], "value": "1.474 ms"},
        {"name": "unattributed", "detail": "pipeline machinery", "segs": [(0.78 * MS, "bar")],
         "value": "≈ 0.78 ms"},
    ]
    y_head, pitch_head = 24, 34
    y_parts = y_head + len(head) * pitch_head + 30
    pitch_parts, bh = 20, 13
    y_end = y_parts + (len(parts) - 1) * pitch_parts + bh + 14
    body = [txt(0, 11, "GROUP BY (returnflag, linestatus) OVER 12 COLUMNS, 1M ROWS", "t-ax")]
    body += axis(scale, ticks, 18, y_end - 2, y_end + 12, zero=True)
    body += rows(head, scale, y0=y_head, pitch=pitch_head, bh=16, x_val=x_val)
    body += [txt(0, y_parts - 10, "WHERE THE 2.806 ms GOES", "t-ax")]
    body += rows(parts, scale, y0=y_parts, pitch=pitch_parts, bh=bh, x_val=x_val)
    return figure(
        "q1",
        "TPC-H Q1: a scalar loop against the SACI pipeline, and where the pipeline time goes",
        """
        Linear scale, zero to 3 milliseconds. The scalar baseline, one pass over a Vec of row
        structs, takes 1.287 ms. The SACI pipeline takes 2.806 ms, 2.18× slower. Decomposed:
        setup 212.7 µs, filter 35.5 µs, compute 303.9 µs, aggregate 1.474 ms, and roughly
        0.78 ms that no measured stage accounts for. The aggregate bar alone is longer than the
        entire scalar baseline.
        """,
        body,
        y_end + 20,
        key=[
            ("k-control", "hand-written scalar loop"),
            ("k-data", "SACI stage"),
            ("k-mute", "time no measured stage accounts for"),
        ],
        cap="""
        The four measured stages sum to 2.026 ms against a measured 2.806 ms, so the grey bar is
        pipeline machinery no stage accounts for. That is a much larger share than Q6 pays, on
        a benchmark that rebuilds its <code>Pipeline</code> every iteration.
        """,
    )


# ---------------------------------------------------------------------------
# 5. TPC-H Q6
# ---------------------------------------------------------------------------


def fig_q6() -> str:
    pl, pr, x_val = 210, 545, 655
    scale = Lin(13 * MS, pl, pr)
    ticks = [(0, "0"), (2 * MS, "2 ms"), (4 * MS, "4 ms"), (6 * MS, "6 ms"),
             (8 * MS, "8 ms"), (10 * MS, "10 ms"), (12 * MS, "12 ms")]
    groups = [
        ("NARROW · 12-COLUMN SCHEMA", [
            {"name": "scalar", "detail": "12 columns, all touched",
             "segs": [(2.096 * MS, "bar-ctl")], "value": "2.096 ms", "factor": "1.0×"},
            {"name": "SACI", "detail": "reads 4 of the 12 columns",
             "segs": [(910.2 * US, "bar-data")], "value": "910.2 µs",
             "factor": "2.30× faster", "factor_cls": "t-sm t-data"},
        ]),
        ("WIDE · 30-COLUMN SCHEMA", [
            {"name": "scalar", "detail": "all 30 pulled per row",
             "segs": [(12.41 * MS, "bar-ctl")], "value": "12.41 ms",
             "factor": "5.92× slower", "factor_cls": "t-sm t-ctl"},
            {"name": "SACI", "detail": "reads 4 columns of 30",
             "segs": [(916.9 * US, "bar-data")], "value": "916.9 µs",
             "factor": "13.54× faster", "factor_cls": "t-sm t-data"},
        ]),
    ]
    pitch, bh = 34, 16
    body: list[str] = []
    y = 22
    saci_ys = []
    for head, items in groups:
        body.append(txt(0, y - 8, head, "t-ax"))
        body += rows(items, scale, y0=y, pitch=pitch, bh=bh, x_val=x_val)
        saci_ys.append(y + pitch + bh / 2)
        y += len(items) * pitch + 22
    y_end = y - 22
    body = axis(scale, ticks, 12, y_end + 2, y_end + 16, zero=True) + body
    # Drawn as two segments so the connector between the SACI bar ends does not
    # run through the wide scalar bar, which is a different measurement.
    x_flat = scale(916.9 * US)
    y_scalar = saci_ys[1] - pitch - bh / 2
    body += [
        vline(x_flat, saci_ys[0], y_scalar - 4, "mark"),
        vline(x_flat, y_scalar + bh + 4, saci_ys[1], "mark"),
        txt(x_flat + 7, saci_ys[1] + 3, "flat to 0.7%", "t-sm t-data"),
    ]
    return figure(
        "q6",
        "TPC-H Q6 on a 12-column and a 30-column schema, scalar loop against SACI",
        """
        Linear scale, zero to 13 milliseconds. On the narrow 12-column schema the scalar loop
        takes 2.096 ms and SACI 910.2 µs, 2.30× faster. On the wide 30-column schema, where 18
        columns are never read, the scalar loop takes 12.41 ms, 5.92× its own narrow figure,
        while SACI takes 916.9 µs, 13.54× faster than the wide scalar loop and within 0.7% of its
        own narrow figure. The two SACI bars are the same length; the two scalar bars are not.
        """,
        body,
        y_end + 24,
        key=[
            ("k-control", "hand-written scalar row loop"),
            ("k-data", "SACI pipeline"),
        ],
        cap="""
        A row-oriented pass pulls every field into cache whether the query reads it or not, so
        the scalar bar grows with the schema. <b>The two SACI bars are the same length</b>
        because the pipeline reads the four columns the predicates name and never touches the
        other 26.
        """,
    )


# ---------------------------------------------------------------------------
# 6. Slice parallelism
# ---------------------------------------------------------------------------


def fig_slices() -> str:
    pl, pr, x_val = 190, 560, 655
    scale = Lin(420 * MS, pl, pr)
    ticks = [(0, "0"), (100 * MS, "100 ms"), (200 * MS, "200 ms"),
             (300 * MS, "300 ms"), (400 * MS, "400 ms")]
    items = [
        {"name": "sequential", "detail": "plain System, one thread",
         "segs": [(399.8 * MS, "bar-ctl")], "value": "399.8 ms", "factor": "1.0×"},
        {"name": "slice-parallel", "detail": "ParallelSystem with run_slice",
         "segs": [(40.56 * MS, "bar-data")], "value": "40.56 ms",
         "factor": "9.86× on 32 logical CPUs", "factor_cls": "t-sm t-data"},
        {"name": "threshold raised", "detail": "slices gated off",
         "segs": [(399.0 * MS, "bar-ctl-2")], "value": "399.0 ms", "factor": "≈ 1.0×"},
    ]
    pitch, bh, y0 = 34, 16, 24
    y_end = y0 + (len(items) - 1) * pitch + bh + 14
    body = [txt(0, 11, "SHA3-256 OVER 1M 128-BYTE BLOBS · 128 MB IN", "t-ax")]
    body += axis(scale, ticks, 18, y_end - 2, y_end + 12, zero=True)
    body += rows(items, scale, y0=y0, pitch=pitch, bh=bh, x_val=x_val)
    return figure(
        "sp",
        "Slice parallelism on a CPU-bound hash, against the same work on one thread",
        """
        Linear scale, zero to 420 milliseconds. Sequential, a plain System on one thread:
        399.8 ms. The same work as a ParallelSystem with run_slice: 40.56 ms, a 9.86× speedup on
        32 logical CPUs. With the slice threshold raised above the row count the executor falls
        back to the whole-dataset path and the time returns to 399.0 ms, confirming the gate.
        """,
        body,
        y_end + 20,
        key=[
            ("k-control", "one thread"),
            ("k-data", "fanned out across rayon"),
            ("k-ctl-2", "fallback path, slices gated off"),
        ],
        cap="""
        Same system, same rows, same bytes: the only difference between the first two bars is
        whether <code>run_slice</code> exists. The third re-runs the parallel configuration with
        the slice threshold raised above the row count, which is why it lands back on the
        sequential bar.
        """,
    )


# ---------------------------------------------------------------------------
# 7. Arrow IPC versus postcard
# ---------------------------------------------------------------------------

IPC = [
    # rows, IPC encode, postcard encode, IPC decode, postcard decode
    ("1 row", ("4.160 µs", 4.160 * US), ("73.5 ns", 73.5), ("4.801 µs", 4.801 * US), ("40.7 ns", 40.7)),
    ("1 000", ("5.770 µs", 5.770 * US), ("31.70 µs", 31.70 * US), ("10.25 µs", 10.25 * US), ("40.02 µs", 40.02 * US)),
    ("10 000", ("31.08 µs", 31.08 * US), ("337.9 µs", 337.9 * US), ("64.44 µs", 64.44 * US), ("457.9 µs", 457.9 * US)),
    ("100 000", ("436.9 µs", 436.9 * US), ("3.807 ms", 3.807 * MS), ("865.1 µs", 865.1 * US), ("5.115 ms", 5.115 * MS)),
    ("1 000 000", ("6.961 ms", 6.961 * MS), ("40.73 ms", 40.73 * MS), ("8.915 ms", 8.915 * MS), ("54.21 ms", 54.21 * MS)),
]


def fig_ipc() -> str:
    x_rows, pl, pr = 74, 86, 520
    scale = Log(20, 100 * MS, pl, pr)
    ticks = [(100, "100 ns"), (1 * US, "1 µs"), (10 * US, "10 µs"), (100 * US, "100 µs"),
             (1 * MS, "1 ms"), (10 * MS, "10 ms")]
    pitch, bh, gap = 26, 8, 3
    body: list[str] = []

    def panel(top: float, head: str, a: int, b: int) -> float:
        y0 = top + 20
        y_end = y0 + (len(IPC) - 1) * pitch + 2 * bh + gap
        out = [txt(x_rows, top + 10, "ROWS", "t-ax t-end"), txt(pl, top + 10, head, "t-ax")]
        out += axis(scale, ticks, y0 - 6, y_end + 4, y_end + 18)
        for i, row in enumerate(IPC):
            y = y0 + i * pitch
            out.append(txt(x_rows, y + 12, row[0], "t-lbl t-end"))
            for j, (idx, cls) in enumerate(((a, "bar-data"), (b, "bar-ctl"))):
                label, value = row[idx]
                yy = y + j * (bh + gap)
                win = value == min(row[a][1], row[b][1])
                out += [
                    bar(pl, yy, scale(value) - pl, bh, cls),
                    txt(scale(value) + 6, yy + bh - 1, label,
                        "t-num" if not win else f"t-num {'t-data' if cls == 'bar-data' else 't-ctl'}"),
                ]
        body.extend(out)
        return y_end + 26

    after = panel(0, "ENCODE · LOG SCALE", 1, 2)
    height = panel(after + 6, "DECODE · LOG SCALE", 3, 4)
    return figure(
        "ipc",
        "Arrow IPC against postcard, encode and decode, one row to a million",
        """
        Both panels are logarithmic, one gridline per ten-fold step. Encode, 1 row: IPC
        4.160 µs, postcard 73.5 ns. 1 000 rows: IPC 5.770 µs, postcard 31.70 µs. 10 000: IPC
        31.08 µs, postcard 337.9 µs. 100 000: IPC 436.9 µs, postcard 3.807 ms. 1 000 000: IPC
        6.961 ms, postcard 40.73 ms. Decode, 1 row: IPC 4.801 µs, postcard 40.7 ns. 1 000: IPC
        10.25 µs, postcard 40.02 µs. 10 000: IPC 64.44 µs, postcard 457.9 µs. 100 000: IPC
        865.1 µs, postcard 5.115 ms. 1 000 000: IPC 8.915 ms, postcard 54.21 ms. The IPC bars
        barely lengthen from 1 row to 1 000; the postcard bars lengthen with every row.
        """,
        body,
        height,
        key=[
            ("k-data", "Arrow IPC"),
            ("k-control", "postcard"),
        ],
        cap="""
        Each pair is one size measured both ways, Arrow IPC above and postcard below. The pair
        worth staring at is the top one in each panel against the one under it. <b>IPC barely
        moves from 1 row to 1 000</b>, which is the shape of a fixed cost, while postcard's bar
        grows with every row it is given.
        """,
    )


# ---------------------------------------------------------------------------
# 8. Against DataFusion
# ---------------------------------------------------------------------------


def fig_datafusion() -> str:
    pl, pr, x_val = 200, 545, 655
    scale = Lin(1.35 * MS, pl, pr)
    ticks = [(0, "0"), (250 * US, "250 µs"), (500 * US, "500 µs"),
             (750 * US, "750 µs"), (1 * MS, "1 ms"), (1.25 * MS, "1.25 ms")]
    items = [
        {"name": "SACI pipeline", "detail": "3 stages + per-iteration setup",
         "segs": [(108.4 * US, "bar-data-3"), (348.3 * US, "bar-data")], "value": "456.7 µs"},
        {"name": "SACI, setup only", "detail": "paid on every iteration",
         "segs": [(108.4 * US, "bar-data-3")], "value": "108.4 µs"},
        {"name": "DataFusion, SQL", "detail": "end to end, session → execute",
         "segs": [(1.272 * MS, "bar-ctl")], "value": "1.272 ms"},
        {"name": "DataFusion", "detail": "physical plan execution alone",
         "segs": [(696.6 * US, "bar-ctl-2")], "value": "696.6 µs"},
        {"name": "DataFusion", "detail": "parse + optimise + planning",
         "segs": [(370.8 * US, "bar-ctl-2")], "value": "370.8 µs"},
        {"name": "DataFusion", "detail": "session setup",
         "segs": [(17.98 * US, "bar-ctl-3")], "value": "17.98 µs"},
    ]
    pitch, bh, y0 = 30, 14, 26
    y_end = y0 + (len(items) - 1) * pitch + bh + 14
    body = [txt(0, 11, "Q6 REVENUE SUM, 12-COLUMN SCHEMA, 1M ROWS · LINEAR", "t-ax")]
    body += axis(scale, ticks, 20, y_end - 2, y_end + 12, zero=True)
    body += rows(items, scale, y0=y0, pitch=pitch, bh=bh, x_val=x_val)
    x_saci = scale(456.7 * US)
    body += [
        vline(x_saci, y0 + pitch * 2 - 8, y_end - 2, "mark"),
        txt(x_saci + 5, y0 + pitch * 2 - 12, "SACI, 456.7 µs", "t-ax t-ctl"),
    ]
    return figure(
        "df",
        "SACI against DataFusion 55 on the same Q6, whole and decomposed",
        """
        Linear scale, zero to 1.35 milliseconds. The SACI pipeline runs Q6 in 456.7 µs, of which
        108.4 µs is per-iteration setup. DataFusion answers the same query as SQL over a MemTable
        in 1.272 ms end to end; its physical plan execution alone is 696.6 µs, parse, optimise
        and physical planning 370.8 µs, and session setup 17.98 µs. The dashed line marks the
        SACI figure: it falls short of DataFusion's execution-only bar, which is the comparison
        worth quoting.
        """,
        body,
        y_end + 20,
        key=[
            ("k-data", "SACI"),
            ("k-data-2", "SACI per-iteration setup, inside the bar above"),
            ("k-control", "DataFusion end to end"),
            ("k-ctl-2", "a measured part of that run"),
        ],
        cap="""
        DataFusion's three lower bars are <b>separate measurements, not a partition of the top
        one</b>. Registration is not timed on its own and they do not sum to the end-to-end
        figure, so they are drawn as their own bars rather than stacked. The SACI bar is stacked,
        because its 108.4 µs of setup is measured inside the 456.7 µs above it.
        """,
    )


# ---------------------------------------------------------------------------
# 9. Summary
# ---------------------------------------------------------------------------

SUMMARY = [
    ("Q6, 30-column schema", "12.41 ms → 916.9 µs", 13.54, True, "13.54× faster"),
    ("Slice parallelism", "399.8 ms → 40.56 ms", 9.86, True, "9.86× on 32 CPUs"),
    ("Checkpoint decode, 1M rows", "54.21 ms → 8.915 ms", 6.08, True, "6.08× faster"),
    ("Checkpoint encode, 1M rows", "40.73 ms → 6.961 ms", 5.85, True, "5.85× faster"),
    ("Q6, 12-column schema", "2.096 ms → 910.2 µs", 2.30, True, "2.30× faster"),
    ("Q6 as SQL, execute only", "696.6 µs → 456.7 µs", 1.53, True, "1.53× faster"),
    ("Q1, aggregation", "1.287 ms → 2.806 ms", 2.18, False, "2.18× slower"),
    ("Checkpoint decode, 1 row", "40.7 ns → 4.801 µs", 118.0, False, "118× slower"),
    ("Stream vs batch, 100k rows", "266.3 µs → 86.44 ms", 324.6, False, "324.6× wall time"),
]


def fig_summary() -> str:
    pl, pr, x_val = 195, 530, 655
    mid = (pl + pr) / 2
    per_decade = (pr - pl) / 2 / 2.6
    pitch, bh, y0 = 30, 14, 34
    y_end = y0 + (len(SUMMARY) - 1) * pitch + bh + 14
    body = [
        txt(0, 12, "BASELINE → SACI", "t-ax"),
        txt(mid, 12, "SLOWER  ·  LOG  ·  FASTER", "t-ax t-mid"),
    ]
    for dec in (1, 2):
        for sign in (-1, 1):
            body.append(vline(mid + sign * dec * per_decade, 22, y_end - 2))
            body.append(
                txt(mid + sign * dec * per_decade, y_end + 12, f"{10 ** dec}×", "t-ax t-mid")
            )
    body += [vline(mid, 22, y_end - 2, "grid grid-0"), txt(mid, y_end + 12, "1×", "t-ax t-mid")]
    for i, (name, detail, factor, faster, label) in enumerate(SUMMARY):
        y = y0 + i * pitch
        w = math.log10(factor) * per_decade
        body += [
            txt(0, y + bh / 2 + 4, name, "t-lbl"),
            txt(0, y + bh + 13, detail, "t-sm"),
            bar(mid if faster else mid - w, y, w, bh, "bar-data" if faster else "bar-ctl"),
            txt(x_val, y + bh / 2 + 4, label,
                "t-num t-end " + ("t-data" if faster else "t-ctl")),
        ]
    return figure(
        "sum",
        "Every headline result, as a ratio against the thing it was measured against",
        """
        Bars run right for faster than the baseline and left for slower, on a logarithmic scale
        with gridlines at ten and a hundred times either side of parity. Faster: Q6 on a
        30-column schema 13.54× (12.41 ms to 916.9 µs); slice parallelism 9.86× on 32 CPUs
        (399.8 ms to 40.56 ms); checkpoint decode at a million rows 6.08× (54.21 ms to
        8.915 ms); checkpoint encode 5.85× (40.73 ms to 6.961 ms); Q6 on a 12-column schema
        2.30× (2.096 ms to 910.2 µs); Q6 as SQL, execution only, 1.53× (696.6 µs to 456.7 µs).
        Slower: Q1 aggregation 2.18× (1.287 ms to 2.806 ms); checkpoint decode of a single row
        118× (40.7 ns to 4.801 µs); 100 000 rows one at a time against one batch, 324.6× the
        wall time (266.3 µs to 86.44 ms).
        """,
        body,
        y_end + 20,
        key=[
            ("k-data", "SACI ahead"),
            ("k-control", "SACI behind"),
        ],
        cap="""
        Three published results have no baseline to divide by, so they are not on the chart.
        They are Q6's column-width scaling (5.92× cost for 30 columns against flat to 0.7%),
        the native stream p99 of <b>2 µs per item</b>, and the framework floor of
        <b>247 ns per item</b>.
        """,
    )


# ---------------------------------------------------------------------------
# Splice
# ---------------------------------------------------------------------------

FIGURES = {
    "item-size": fig_item_size,
    "latency": fig_latency,
    "stage-cost": fig_stage_cost,
    "q1": fig_q1,
    "q6": fig_q6,
    "slices": fig_slices,
    "ipc": fig_ipc,
    "datafusion": fig_datafusion,
    "summary": fig_summary,
}


def main() -> None:
    text = PAGE.read_text(encoding="utf-8")
    for name, build in FIGURES.items():
        start, end = f"<!-- fig:{name} -->", f"<!-- /fig:{name} -->"
        pattern = re.compile(
            re.escape(start) + r"\n.*?\n" + re.escape(end), re.DOTALL
        )
        if not pattern.search(text):
            raise SystemExit(f"{PAGE}: no marker pair for fig:{name}")
        svg = build()
        if "\n\n" in svg:
            # A blank line inside a raw HTML block ends it, and the rest of the
            # figure would be rendered as literal markdown text.
            raise SystemExit(f"fig:{name}: blank line inside the figure")
        text = pattern.sub(lambda _m: f"{start}\n{svg}\n{end}", text, count=1)
    PAGE.write_text(text, encoding="utf-8")
    print(f"wrote {len(FIGURES)} figures into {PAGE}")


if __name__ == "__main__":
    main()
