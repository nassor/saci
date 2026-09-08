//! The dashboard shell: a fixed left rail plus four tabbed views.
//!
//! One poll drives everything on the Pipelines tab. `/api/snapshot` is the only
//! endpoint on this timer; traces and logs run their own, and each skips a tick
//! while the document is hidden, so a backgrounded tab stops asking.
//!
//! ## Why the tab is in the URL fragment
//!
//! The selected tab is mirrored into `location.hash`, so `/ui#logs` opens on
//! the log tail. That is what makes a view linkable from an incident note, and
//! what lets a headless browser screenshot one tab without scripting a click.
//! The hash is read once on mount and written on every change; no history
//! entry is pushed, because a tab switch is not navigation.
//!
//! ## Where a new panel goes
//!
//! Every tab is one `<section>` inside the same content column, and the column
//! sets the spacing scale. A tab is a stack of [`Card`](crate::ui::Card)s, so
//! an added panel is another card in that stack rather than a new layout.

use leptos::prelude::*;
use leptos::task::spawn_local;
use std::time::Duration;

use saci_inspector_wire::{Snapshot, Topology, WorkflowStatus};

use crate::api;
use crate::components::{DlqView, LogsView, PipelinesView, TracesView};
use crate::ui::{Badge, BadgeTone, Separator, TabBar, TabButton, Tooltip};

/// How much series history the graph's sparklines cover.
const WINDOW_SECS: u64 = 300;

/// Which tab is showing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    /// The topology graph.
    Pipelines,
    /// Trace list and waterfall.
    Traces,
    /// Log tail.
    Logs,
    /// What each workflow's dead letter queue is holding.
    DeadLetters,
}

impl Tab {
    /// The `location.hash` fragment this tab is addressed by.
    fn slug(self) -> &'static str {
        match self {
            Self::Pipelines => "pipelines",
            Self::Traces => "traces",
            Self::Logs => "logs",
            Self::DeadLetters => "dead-letters",
        }
    }

    /// The tab a fragment names, or `Pipelines` for anything else.
    fn from_slug(slug: &str) -> Self {
        match slug.trim_start_matches('#') {
            "traces" => Self::Traces,
            "logs" => Self::Logs,
            "dead-letters" => Self::DeadLetters,
            _ => Self::Pipelines,
        }
    }
}

/// The dashboard root.
#[component]
pub fn App() -> impl IntoView {
    let (topology, set_topology) = signal::<Option<Topology>>(None);
    let (snapshot, set_snapshot) = signal::<Option<Snapshot>>(None);
    // `None` also means "the service mounts no control plane", which is what
    // `/api/workflows` answering 404 reports. The workflow cards hide their
    // controls then rather than showing an error nobody can act on.
    let (workflows, set_workflows) = signal::<Option<Vec<WorkflowStatus>>>(None);
    let (error, set_error) = signal::<Option<String>>(None);
    // What the last lifecycle action did that the operator did not ask for:
    // a service-wide verb some workflows refused. Separate from `error`
    // because the one-second poll clears that one on its next success, and a
    // partial sweep has to stay readable until the next action.
    let (notice, set_notice) = signal::<Option<String>>(None);
    let (tab, set_tab) = signal(Tab::from_slug(
        &window().location().hash().unwrap_or_default(),
    ));

    // The topology is fixed for the process lifetime, so it is fetched once.
    spawn_local(async move {
        match api::topology().await {
            Ok(value) => set_topology.set(Some(value)),
            Err(message) => set_error.set(Some(message)),
        }
    });

    let poll = move || {
        spawn_local(async move {
            match api::snapshot(WINDOW_SECS).await {
                Ok(value) => {
                    set_snapshot.set(Some(value));
                    set_error.set(None);
                }
                Err(message) => set_error.set(Some(message)),
            }
            // Same tick, so a state badge and the numbers it annotates never
            // disagree by a second. A transport failure here is the one the
            // snapshot fetch already reported.
            if let Ok(value) = api::workflows().await {
                set_workflows.set(value);
            }
        });
    };
    poll();

    let handle = set_interval_with_handle(
        move || {
            if !document().hidden() {
                poll();
            }
        },
        Duration::from_millis(1000),
    )
    .expect("setInterval is available in every browser that can run WebAssembly");
    on_cleanup(move || handle.clear());

    // A verb's own answer is the freshest status there is, so it lands
    // immediately and the badge flips without waiting for a poll; the
    // follow-up re-poll is what picks up a transition that was still in
    // progress when the request returned `202`.
    let control = Callback::new(move |(id, verb): (String, &'static str)| {
        set_notice.set(None);
        spawn_local(async move {
            match api::control(&id, verb).await {
                Ok(status) => {
                    set_error.set(None);
                    set_workflows.update(|list| {
                        if let Some(list) = list.as_mut()
                            && let Some(entry) = list.iter_mut().find(|entry| entry.id == status.id)
                        {
                            *entry = status;
                        }
                    });
                }
                Err(message) => set_error.set(Some(message)),
            }
            if let Ok(value) = api::workflows().await {
                set_workflows.set(value);
            }
        });
    });

    // The same shape one workflow up: the report carries every workflow the
    // verb reached, so the whole list lands before the next poll.
    //
    // A verb some workflows refuse still moves the rest, and the request
    // succeeds. The cards show the truth but nothing points at it, so a
    // partial sweep says so once, in its own chip.
    let control_all = Callback::new(move |verb: &'static str| {
        set_notice.set(None);
        spawn_local(async move {
            match api::control_all(verb).await {
                Ok(report) => {
                    set_error.set(None);
                    if !report.refused.is_empty() {
                        let names: Vec<&str> = report
                            .refused
                            .iter()
                            .map(|refusal| refusal.id.as_str())
                            .collect();
                        set_notice
                            .set(Some(format!("{verb} all: left {} alone", names.join(", "))));
                    }
                    set_workflows.update(|list| {
                        if let Some(list) = list.as_mut() {
                            for status in report.applied {
                                if let Some(entry) =
                                    list.iter_mut().find(|entry| entry.id == status.id)
                                {
                                    *entry = status;
                                }
                            }
                        }
                    });
                }
                Err(message) => set_error.set(Some(message)),
            }
            if let Ok(value) = api::workflows().await {
                set_workflows.set(value);
            }
        });
    });

    let select = Callback::new(move |next: Tab| {
        // `replace`, not `set_hash`: writing the hash is a fragment navigation
        // and pushes a history entry, which would make Back walk the tab bar
        // instead of leaving the dashboard. Replacing the entry keeps the URL
        // addressable without owning the Back button. The `Result` is
        // discarded because `set_tab.set(next)` on the next line switches the
        // tab regardless of whether the replace succeeded, so a failure only
        // leaves the address bar stale.
        let _ = window().location().replace(&format!("#{}", next.slug()));
        set_tab.set(next);
    });

    // The fragment, not the signal, is the address of a tab, so a change from
    // anywhere else has to arrive here too: a pasted link, the Back button, a
    // headless browser navigating straight to `/ui#logs`. The guard keeps the
    // tab bar's own write from remounting the panel a second time.
    let listener = window_event_listener(leptos::ev::hashchange, move |_| {
        let next = Tab::from_slug(&window().location().hash().unwrap_or_default());
        if tab.get_untracked() != next {
            set_tab.set(next);
        }
    });
    on_cleanup(move || listener.remove());

    view! {
        <div class="flex min-h-screen bg-background text-foreground">
            <Rail topology=topology snapshot=snapshot />
            <main class="min-w-0 flex-1">
                <header class="sticky top-0 z-30 flex flex-wrap items-center gap-3 border-b border-border bg-background/95 px-6 py-3 backdrop-blur">
                    <TabBar>
                        <TabButton
                            active=Signal::derive(move || tab.get() == Tab::Pipelines)
                            on_select=Callback::new(move |()| select.run(Tab::Pipelines))
                        >
                            "Pipelines"
                        </TabButton>
                        <TabButton
                            active=Signal::derive(move || tab.get() == Tab::Traces)
                            on_select=Callback::new(move |()| select.run(Tab::Traces))
                        >
                            "Traces"
                        </TabButton>
                        <TabButton
                            active=Signal::derive(move || tab.get() == Tab::Logs)
                            on_select=Callback::new(move |()| select.run(Tab::Logs))
                        >
                            "Logs"
                        </TabButton>
                        <TabButton
                            active=Signal::derive(move || tab.get() == Tab::DeadLetters)
                            on_select=Callback::new(move |()| select.run(Tab::DeadLetters))
                        >
                            "Dead letters"
                        </TabButton>
                    </TabBar>
                    <Show when=move || error.get().is_some()>
                        <span class="flex items-center gap-2 rounded-md border border-destructive/40 px-2.5 py-1 font-mono text-xs text-destructive">
                            <span class="saci-lvl saci-lvl-error">"api"</span>
                            {move || error.get().unwrap_or_default()}
                        </span>
                    </Show>
                    <Show when=move || notice.get().is_some()>
                        <span class="flex items-center gap-2 rounded-md border border-border px-2.5 py-1 font-mono text-xs text-muted-foreground">
                            <span class="saci-lvl saci-lvl-warn">"all"</span>
                            {move || notice.get().unwrap_or_default()}
                        </span>
                    </Show>
                </header>

                <section class="px-6 py-5">
                    <Show when=move || tab.get() == Tab::Pipelines>
                        <PipelinesView
                            topology=topology
                            snapshot=snapshot
                            workflows=workflows
                            on_control=control
                            on_control_all=control_all
                        />
                    </Show>
                    <Show when=move || tab.get() == Tab::Traces>
                        <TracesView topology=topology />
                    </Show>
                    <Show when=move || tab.get() == Tab::Logs>
                        <LogsView />
                    </Show>
                    <Show when=move || tab.get() == Tab::DeadLetters>
                        <DlqView
                            on_error=Callback::new(move |message| set_error.set(Some(message)))
                            on_notice=Callback::new(move |message| set_notice.set(Some(message)))
                        />
                    </Show>
                </section>
            </main>
        </div>
    }
}

/// Node identity, readiness, uptime and buffer occupancy.
#[component]
fn Rail(
    #[prop(into)] topology: Signal<Option<Topology>>,
    #[prop(into)] snapshot: Signal<Option<Snapshot>>,
) -> impl IntoView {
    let node_id = move || {
        topology
            .get()
            .map_or_else(|| "…".to_string(), |t| t.node_id)
    };
    let mode = move || topology.get().map_or_else(|| "…".to_string(), |t| t.mode);
    let uptime = move || {
        snapshot
            .get()
            .map_or_else(|| "…".to_string(), |s| format_duration(s.uptime_secs))
    };
    let ready = move || snapshot.get().is_some_and(|s| s.ready);
    let series_value = move |name: &'static str| {
        snapshot.get().and_then(|s| {
            s.series
                .iter()
                .find(|series| series.name == name && series.attrs.is_empty())
                .map(|series| series.value)
        })
    };
    let runs = move || series_value("saci_workflow_runs_total").unwrap_or(0.0);
    let errors = move || series_value("saci_workflow_errors_total").unwrap_or(0.0);
    let buffers = move || snapshot.get().map(|s| s.buffers);
    // One accessor for four counters of two widths: `BufferStats` counts
    // occupancy as `usize` and the drop total as `u64`.
    let count = move |pick: fn(&saci_inspector_wire::BufferStats) -> u64| {
        buffers().map_or_else(|| "…".to_string(), |b| pick(&b).to_string())
    };

    view! {
        <aside class="flex w-60 shrink-0 flex-col gap-4 border-r border-border bg-card px-5 py-4">
            <div>
                <div class="text-sm font-semibold tracking-tight">"saci-service"</div>
                <div class="mt-0.5 font-mono text-xs text-muted-foreground">
                    {move || format!("node {} · {}", node_id(), mode())}
                </div>
            </div>

            <div class="flex flex-wrap gap-1.5">
                <Show
                    when=move || ready()
                    fallback=|| {
                        view! { <Badge tone=BadgeTone::Secondary>"not ready"</Badge> }
                    }
                >
                    <Badge tone=BadgeTone::Primary>"ready"</Badge>
                </Show>
                <Show when=move || { errors() > 0.0 }>
                    <Badge tone=BadgeTone::Destructive>
                        {move || format!("{} errors", errors() as u64)}
                    </Badge>
                </Show>
            </div>

            <div>
                <Heading label="service" />
                <dl class="space-y-1.5 text-xs">
                    <Fact label="uptime" value=Signal::derive(uptime) />
                    <Fact
                        label="workflow runs"
                        value=Signal::derive(move || format!("{}", runs() as u64))
                    />
                    <Fact
                        label="workflows"
                        value=Signal::derive(move || {
                            topology
                                .get()
                                .map_or_else(|| "…".to_string(), |t| t.workflows.len().to_string())
                        })
                    />
                </dl>
            </div>

            <div>
                <Heading label="retention" />
                <dl class="space-y-1.5 text-xs">
                    <Fact
                        label="spans"
                        value=Signal::derive(move || count(|b| b.spans as u64))
                    />
                    <Fact label="logs" value=Signal::derive(move || count(|b| b.logs as u64)) />
                    <Fact
                        label="samples"
                        value=Signal::derive(move || count(|b| b.samples as u64))
                    />
                    <Fact
                        label="decisions"
                        value=Signal::derive(move || count(|b| b.flow_decisions as u64))
                    />
                    <div class="flex items-baseline justify-between gap-2">
                        <dt class="text-muted-foreground">
                            <Tooltip content=Signal::derive(|| {
                                "Records the ring buffers overwrote before anyone read them. \
                                 A rising number means the tail is behind the workload."
                                    .to_string()
                            })>
                                <span class="underline decoration-dotted">"dropped"</span>
                            </Tooltip>
                        </dt>
                        <dd class="font-mono tabular-nums">
                            {move || count(|b| b.dropped)}
                        </dd>
                    </div>
                </dl>
            </div>
        </aside>
    }
}

/// One section heading in the rail, with the hairline that separates it from
/// the section above.
#[component]
fn Heading(label: &'static str) -> impl IntoView {
    view! {
        <>
            <Separator />
            <div class="mb-2 text-[0.6875rem] font-medium tracking-wide text-muted-foreground uppercase">
                {label}
            </div>
        </>
    }
}

/// One `label: value` row in the rail.
#[component]
fn Fact(label: &'static str, #[prop(into)] value: Signal<String>) -> impl IntoView {
    view! {
        <div class="flex items-baseline justify-between gap-2">
            <dt class="text-muted-foreground">{label}</dt>
            <dd class="font-mono tabular-nums">{move || value.get()}</dd>
        </div>
    }
}

/// `412s` as `6m 52s`, `9412s` as `2h 36m`.
fn format_duration(secs: u64) -> String {
    match secs {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m {}s", s / 60, s % 60),
        s => format!("{}h {}m", s / 3600, (s % 3600) / 60),
    }
}
