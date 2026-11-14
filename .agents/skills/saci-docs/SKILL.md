---
name: saci-docs
description: Use with the user-level technical-writer skill when writing or editing SACI's README.md, docs/ content or templates, docs diagrams and benchmark figures, Rust doc comments, or a saci-* skill. Holds the SACI surfaces, Zola site layout, .dgm diagram classes, key-list markup and docs build commands.
---

# SACI docs

The writing rules (current state only, Diátaxis kinds, simplify, dashes, SVG diagrams, tables and
key lists, code comments, the AI pattern list) are in the user-level skill `technical-writer`
(`~/.agents/skills/technical-writer/`). Read it first. This file holds what is true only of SACI.

## Where prose lives

| Surface | What it holds | Kind |
|---|---|---|
| `README.md` | the pitch and the first command | pitch plus a how-to Quick start |
| `crates/saci/README.md`, `crates/saci-macros/README.md` | each crate's crates.io page: what it is, the dependency line, one example | pitch plus a how-to |
| `CHANGELOG.md` | what each release added, changed, fixed or removed | reference; the one surface that records history |
| `docs/content/service/**` | pages for someone running the binary | tutorial, or how-to once the reader has a running service |
| `docs/content/library/first-pipeline.md` | the embedding tutorial | tutorial |
| `docs/content/library/service/*` | extending the service | how-to |
| `docs/content/library/reference/`, `//!` and `///`, `AGENTS.md`, `.agents/skills/saci-*/SKILL.md` | exact facts | reference |
| `docs/templates/{dataset,systems,pipeline,scheduler,distributed,io,tracing}.html`, `docs/content/library/{flow-control,processor-host,windowing}.md` | concepts | explanation |

The Service area carries no reference section and no "Reference" label: exact keys sit in each
page's own `## Every key` table, and host internals sit in the Library area.

The canonical quickstart is `docs/content/service/`, `install.md` then `first-pipeline.md`; the
README "Quick start" block is its short form.

## Site layout

- `docs/`: Zola site, split into two areas by a switch in the sidebar. `content/service/` is the
  Service area: getting started (install, first pipeline), writing the config (`config/`),
  sources, sinks and formats (`connectors/`, `formats/`), processors (`processors/` plus
  `processors/build/`, one page per language), plugins (`plugins/`), operating (`operate/`).
  `content/library/` is the Library area, for someone embedding the engine: getting started, core
  concepts (dataset, systems, pipeline, scheduler, io, windowing, distributed), extending the
  service (`library/service/`), under the hood (flow-control, tracing, processor-host), and
  `library/reference/` (crates, wire format, SDK packages, benchmarks).
- The seven concept pages under `docs/content/library/{dataset,systems,pipeline,scheduler,io,distributed,tracing}.md`
  are front matter only; their prose is `docs/templates/<name>.html`. Every other page is
  markdown through `page.html`, `section.html` or `subpage.html`; `page.html` renders no title, so
  such a page opens its body with its own `#` heading.
- `docs/config.toml`: `[[extra.areas]]` is the two areas and their reading order. `base.html`
  renders the area switch, the sidebar, the breadcrumb and the previous/next pager from it, so a new
  page is added there, not in the template. The pager never crosses an area.
  `definition_list = true` is what renders the markdown key list.
- `docs/figures/bench_figures.py`: owns every chart on the benchmarks page, spliced into
  `content/library/reference/benchmarks.md` between `<!-- fig:NAME -->` and `<!-- /fig:NAME -->`
  markers. Change the numbers in the script and run `python3 docs/figures/bench_figures.py`.
- `docs/search-index.py`: builds `public/search-index.json` from the rendered HTML, split at `<h2>`
  boundaries, one record per section carrying the area it belongs to. Runs after `zola build`,
  because the concept pages keep their prose in `templates/`.
- `docs/build-local.py`: zola build plus relative-URL rewrite plus search index, for browsing
  `public/` over `file://` or a local server.

## Sources of truth

Implementation `crates/saci/src/`; tests `crates/saci/tests/` and unit tests next to the code;
workspace map `AGENTS.md`; per-area reference `.agents/skills/saci-*/SKILL.md`; examples
`examples/`; Arrow IPC conformance corpus `packages/arrow-ipc-conformance/`.

## Platforms

SACI runs on Linux, macOS and Windows, so every terminal command in the docs carries a
Linux/macOS form and a Windows (PowerShell) form, or a note that it runs the same on all three.

## Dashes

The em dash in a template title block (`{% block title %}Systems — SACI{% endblock %}`) is the site
title separator. Leave it.

## Diagrams

A page diagram is inline SVG inside the site's diagram frame:

```html
<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 150" role="img" aria-labelledby="x-t x-d">
        <title id="x-t">One sentence naming what the diagram shows.</title>
        <desc id="x-d">A description that replaces the picture for a reader who cannot see it.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="40" width="70" height="56" rx="8"/>
            <text class="t-lbl" x="12" y="76">Order</text>
        </g>
    </svg>
    </div>
</div>
```

- Style only with the `.dgm` class vocabulary in `docs/static/styles.css`.
- `-data` peach is the data plane, `-ctl` teal is the control plane, `-bnd` mauve is a boundary
  such as the WASM edge. Each plane has an accent token for area fills and an ink token for text
  and line art. `.row-r` marks a read, `.row-w` a write.
- Author at `viewBox` width 660; `.dgm-scroll` holds that as a minimum, so a wider drawing scrolls.
- Wrap each step in `<g class="anim anim-1">` through `anim-4` for the reveal animation.
- README figures are standalone files in `docs/static/*.svg`, referenced as
  `<img src="docs/static/NAME.svg" alt="..." width="660">` (relative from the README's directory).
  Site CSS does not reach them, so each carries the `<style>` block and marker `<defs>` of
  `docs/static/end-to-end.svg` verbatim (Catppuccin Latte plus a `prefers-color-scheme: dark`
  Mocha block) and uses only its classes: `canvas`, `panel`, `blk blk-d|blk-c|blk-b` with
  `hd-d|hd-c|hd-b`, `t-ttl t-sub t-lab t-step`, `f-*`, `arw s-*`, `dash`, markers `#m-n|d|c|b`.
  The first drawn element is the `canvas` frame. Name a figure after its README
  (`branching-fanout.svg`); `two-ways.svg` and `stage-plan.svg` belong to the root README, and
  the docs home page carries an inline `.dgm` copy of `two-ways`. Check a new figure in a
  headless browser in both colour schemes before committing.
- A crate README is packaged at the crate root and crates.io resolves its relative links against
  `crates/<crate>/` on GitHub, so a link that leaves the crate directory is an absolute URL.
  `crates/saci/src/lib.rs` compiles the `saci` README's Rust blocks as a doc test
  (`ReadmeDoctests`), so `cargo test -p saci --doc` fails when the example drifts; the
  `saci-macros` example is `rust,ignore` because it needs the wasm32-wasip2 target.

## Key lists

The markdown form, rendered by `definition_list = true`:

    `saci_sink_pending_rows` <small>gauge, labels `sink` only</small>
    :   one pass per sink that reports a backlog

A concept page in `docs/templates/` writes the same shape by hand as `<dl class="key-list">` with
`<dt><code>…</code> <small>…</small></dt><dd>…</dd>` entries.

## Before you finish

Walk `technical-writer`'s list, then:

- Touched `bench_figures.py`: rerun it, confirm the diff moved numbers only.
- Touched doc comments: `cargo test --workspace --all-features --doc`.
- Touched templates or content: `cd docs && zola check --skip-external-links`, then
  `python3 build-local.py` (Windows PowerShell: `cd docs; python3 build-local.py`) and open
  `docs/public/index.html`. Checking search needs a server: `python3 -m http.server -d public`,
  which runs the same on all three platforms.

## Keep this skill current

Update this file in the same change that adds or moves a docs area, a template, a site script, a
`.dgm` class or the canonical quickstart.
