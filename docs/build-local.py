#!/usr/bin/env python3
"""Build the site for `file://` browsing.

Builds the site, rewrites the sentinel base URL to per-page relative paths so
`public/` opens straight from disk, then builds the search index from the
rendered pages. Search itself needs a real server, because `fetch` refuses a
`file://` URL.

Usage:  python3 build-local.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SENTINEL = "http://__local__"

here = Path(__file__).resolve().parent
public = here / "public"


def run(*args: str) -> None:
    result = subprocess.run(args, cwd=here)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


run("zola", "build", "--base-url", SENTINEL)

count = 0
for html in public.rglob("*.html"):
    depth = len(html.parent.relative_to(public).parts)
    prefix = "./" if depth == 0 else "../" * depth
    text = html.read_text(encoding="utf-8")
    text = text.replace(SENTINEL + "/", prefix)
    # Zola HTML-escapes the slashes in `page.permalink`, used by section.html's
    # child-page cards, so the replace above never sees those.
    text = text.replace((SENTINEL + "/").replace("/", "&#x2F;"), prefix.replace("/", "&#x2F;"))
    text = text.replace(SENTINEL, prefix.rstrip("/"))
    html.write_text(text, encoding="utf-8")
    count += 1

print(f"rewrote URLs in {count} HTML files")
run(sys.executable, "search-index.py", "public")
print(f"\nOpen: file://{public / 'index.html'}")
