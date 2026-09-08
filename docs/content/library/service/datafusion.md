+++
title = "SQL results as a source"
description = "A DataFusion query as a Source, streaming lazily so the filter runs in DataFusion."
template = "page.html"
weight = 3
aliases = ["/connectors/datafusion/"]
[[extra.facts]]
label = "Crate"
value = "<code>saci-connector-datafusion</code>"
[[extra.facts]]
label = "Feature"
value = "None on <code>saci-service</code>; the <code>saci</code> facade has <code>connector-datafusion</code>"
[[extra.facts]]
label = "Config type"
value = "None: it has no factory"
[[extra.facts]]
label = "Transformer key"
value = "None: rows arrive as Arrow batches"
+++
# SQL results as a source

`saci-connector-datafusion` adds one adapter, in one direction: a DataFusion query
becomes a SACI `Source`. It streams lazily, pulling from DataFusion's execution
stream one batch per `next_batch`, so a filter or aggregation runs in DataFusion
and only the result crosses into the `Dataset`.

There is no adapter in the other direction. A `Dataset` is not exposed as a
DataFusion `TableProvider`.

## The constructors

```rust,name=Three calls, and only one of them is async
use saci_connector_datafusion::DataFusionSource;

DataFusionSource::from_sql(&SessionContext, &str).await  -> Result<Self>
DataFusionSource::from_stream(SendableRecordBatchStream) -> Self
source.with_estimated_rows(rows: usize)                  -> Self
```

`from_stream` takes a stream you already hold. `with_estimated_rows` is the only
way `estimated_rows` returns `Some`: neither constructor guesses a row count,
because a lazy stream has none until it is drained.

## No factory, no feature

<div class="note">
<span class="note-label">Constraint</span>
<p>
This connector has <b>no factory and no <code>saci-service</code> feature</b>. It needs a live
<code>SessionContext</code> that the caller owns, which service config cannot express, so there is
no <code>type</code> string for it. The crate does not even depend on <code>saci-connector</code>,
so it cannot implement <code>SourceFactory</code>.
</p>
</div>

Wiring is Rust, not config: build the source, drain it into a `Dataset` with
`drain_into_dataset`, and run a `Pipeline` over that dataset.

```rust,name=From a query to a Dataset
use datafusion::prelude::SessionContext;
use saci_connector_datafusion::DataFusionSource;
use saci_core::dataset::Dataset;
use saci_core::io::drain_into_dataset;

let ctx = SessionContext::new();
ctx.register_table("sales", Arc::new(mem_table))?;

let mut src = DataFusionSource::from_sql(
    &ctx,
    "SELECT product_id, quantity, unit_price FROM sales WHERE region = 'north'",
).await?;

let mut dataset = Dataset::new();
dataset.register_raw_component("sales", src.schema());
drain_into_dataset(&mut src, &mut dataset, "sales").await?;

let mut pipeline = Pipeline::new("datafusion");
*pipeline.data_mut() = dataset;
pipeline.add_system(ComputeRevenue);
pipeline.run().await?;
```

`register_raw_component(name, schema)` is the natural pairing here: the query's
projected schema is known only after planning, so take it from the source rather
than declaring a `Component` up front.

## Worked example

`cargo run -p saci-connector-datafusion --example datafusion_interop` runs the code
above against an in-memory table. The TPC-H Q6 comparison bench is
`vs_datafusion_q6`, run through `cargo xtask bench vs_datafusion_q6`. Both run the
same on Linux, macOS and Windows (PowerShell).

[Sources & Sinks](@/library/io.md) is the trait pair this plugs into.
