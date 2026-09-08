+++
title = "Plugins in other languages"
description = "What any language needs to export to be loadable as a plugin."
template = "page.html"
weight = 3
+++
# Plugins in other languages

A plugin is a shared library exporting two C symbols. Any toolchain that can produce one can
author a plugin, and `saci-service` never learns which toolchain did.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 172" role="img" aria-labelledby="pol-title pol-desc">
        <title id="pol-title">The two symbols the host looks up, and the vtable it gets back</title>
        <desc id="pol-desc">
            saci-service opens the shared library with dlopen on Unix or LoadLibrary on Windows
            and looks up two symbols by name: saci_abi_version, which reports the ABI the
            library was built against, and saci_plugin_v1, which fills a vtable the host
            allocated with four function pointers named describe, run_batch, free_buffer and
            destroy. Both sides share one address space, so no language runtime boundary
            protects either from the other.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="40" width="176" height="72" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="40" width="176" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="52" width="176" height="8"/>
            <text class="t-lbl" x="12" y="55">saci-service</text>
            <text class="t-sm" x="12" y="76">dlopen, LoadLibrary</text>
            <text class="t-sm" x="12" y="89">owns the vtable</text>
            <text class="t-sm" x="12" y="102">looks up two names</text>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="456" y="40" width="204" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="456" y="40" width="204" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="456" y="52" width="204" height="8"/>
            <text class="t-lbl" x="468" y="55">your library</text>
            <text class="t-sm t-bnd" x="468" y="76">saci_abi_version()</text>
            <text class="t-sm t-bnd" x="468" y="89">saci_plugin_v1()</text>
            <text class="t-sm" x="468" y="102">any toolchain, one C ABI</text>
        </g>
        <g class="anim anim-3">
            <text class="t-sm t-ctl t-mid" x="316" y="44">looks up both symbols by name</text>
            <path class="arw arw-ctl" d="M176 50 H456" marker-end="url(#pol-c)"/>
            <text class="t-sm t-mid" x="316" y="80">describe, run_batch,</text>
            <text class="t-sm t-mid" x="316" y="96">free_buffer, destroy</text>
            <path class="arw arw-data" d="M456 104 H176" marker-end="url(#pol-d)"/>
        </g>
        <g class="anim anim-4">
            <path class="ln" d="M0 136 H654"/>
            <text class="t-sm" x="0" y="154">One address space: no language runtime boundary protects either side from the other.</text>
        </g>
        <defs>
            <marker id="pol-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
            <marker id="pol-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> the host, and the two names it resolves</span>
        <span class="k-boundary"><i></i> your library, and the four pointers it fills in</span>
    </div>
</div>

## How each language exports the two symbols

| Language | How the symbols get exported | Build |
|---|---|---|
| **Rust** | `saci_plugin::export_plugin!` in a `cdylib` crate | `cargo build --release -p my-plugin` |
| **Go** | cgo, `//export saci_abi_version` and `//export saci_plugin_v1` | `go build -buildmode=c-shared -o my_plugin.so .` |
| **C#** | NativeAOT, `[UnmanagedCallersOnly(EntryPoint = "saci_abi_version")]` | `dotnet publish -r linux-x64 -c Release` |
| **Kotlin** | GraalVM, `@CEntryPoint(name = "saci_abi_version")` | `native-image --shared` |

Python and TypeScript cannot export a C ABI, so both stay on the
[WebAssembly processor](@/service/processors/build/_index.md) path instead.

`saci_plugin.h`, the plugin ABI header that ships in this repository, is the authority outside
Rust. It declares every struct and both entry points, and its contract comment carries the
buffer ownership rules and the threading rule.

## The rules every language obeys

**No panic and no exception crosses the boundary.** Whatever your language calls unwinding, it
must not leave an exported function. Catch it inside and return a failure status with a message
instead; a crash on the way out takes the service with it and reports nothing.

**One instance is never called concurrently.** The host makes one call at a time into a given
instance. Successive calls may arrive on different operating system threads, so state a call
leaves behind must be safe to touch from another thread, and thread-local state is not a place
to keep it.

**Every buffer you hand over stays yours to free.** The host copies out of a buffer and hands it
straight back to your `free_buffer`, so allocator ownership never crosses in either direction.
Allocate those bytes where your language cannot move or reclaim them under the host's feet.

**A managed runtime starts when the library loads.** Go, GraalVM and NativeAOT each bring one,
with their own signal handlers and their own thread pools running alongside the service's. That
runtime does not go away between batches.

**Only the checkpoint crosses a batch boundary.** The host holds one loaded instance for the
node's whole life, so your library's own memory survives a batch. Keeping state there is still
wrong: consecutive batches of one partition may land on different processes, and only the
checkpoint travels with the claim.

**One process may load your library twice.** Keep per instance state behind the `instance`
pointer the host hands back on every call, not in a global, or two nodes end up sharing one.

## Next

- [A Go plugin](@/service/plugins/go.md): all of the above worked through in one language.
- [Plugins in a workflow](@/service/plugins/_index.md): the node that loads the library, and
  what the service prints when it refuses one.
