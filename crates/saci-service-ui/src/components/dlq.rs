//! The Dead letters tab: what each workflow's queue is holding, and the two
//! actions that empty it.
//!
//! One card per workflow that declares a `dlq` block, and one table row per
//! `(sink, reason)` group. The grouping is the server's: a reader needs to
//! know which sink is refusing and why, not which individual batch is waiting,
//! and the payload of a letter never leaves the service.
//!
//! ## Why a replay answers twice
//!
//! Both replay points sit inside a workflow pass, so a request lands whenever
//! the runner next gets there. `200` carries the report of a replay that ran;
//! `202` means the request is queued, which is the normal answer for an idle
//! or stream-mode workflow, and the tab shows it as a notice rather than an
//! error.
//!
//! ## Why purge takes two clicks
//!
//! Purging discards every letter, and nothing brings one back. The button
//! arms itself on the first click and acts on the second, so a misplaced click
//! on a card costs nothing.

use leptos::prelude::*;
use leptos::task::spawn_local;
use std::time::Duration;

use saci_inspector_wire::{DlqGroup, DlqSummary};

use crate::api;
use crate::components::logs::format_clock;
use crate::ui::{
    Badge, BadgeTone, Button, ButtonTone, Card, CardContent, CardHeader, CardTitle, Table,
    TableBody, TableCell, TableHead, TableHeader, TableRow, Tooltip,
};

/// Poll period. A queue changes when a pass fails or a replay runs, neither
/// of which is per-item, so this is slower than the snapshot's 1 Hz.
const POLL_MS: u64 = 2000;

/// The Dead letters tab.
#[component]
pub fn DlqView(
    /// Where a failed request goes.
    #[prop(into)]
    on_error: Callback<String>,
    /// Where a `202` goes: the request is queued, not refused.
    #[prop(into)]
    on_notice: Callback<String>,
) -> impl IntoView {
    // `None` also means "no workflow declares a dlq block", which is what
    // `/api/dlq` answering 404 reports.
    let (queues, set_queues) = signal::<Option<Vec<DlqSummary>>>(None);
    let (loaded, set_loaded) = signal(false);
    // Which card's purge button is armed. One at a time: arming a second
    // disarms the first.
    let (armed, set_armed) = signal::<Option<String>>(None);

    let load = move || {
        spawn_local(async move {
            match api::dlq().await {
                Ok(value) => set_queues.set(value),
                Err(message) => on_error.run(message),
            }
            set_loaded.set(true);
        });
    };
    load();

    let handle = set_interval_with_handle(
        move || {
            if !document().hidden() {
                load();
            }
        },
        Duration::from_millis(POLL_MS),
    )
    .expect("setInterval is available in every browser that can run WebAssembly");
    on_cleanup(move || handle.clear());

    // Every action re-fetches when it completes, so the card reflects what
    // the replay did without waiting out the poll.
    let replay = Callback::new(
        move |(id, sink, reason): (String, Option<String>, Option<String>)| {
            set_armed.set(None);
            spawn_local(async move {
                match api::dlq_replay(&id, sink.as_deref(), reason.as_deref()).await {
                    Ok(Some(report)) => {
                        if let Some(error) = report.error {
                            on_error.run(format!("{id}: replay ended early: {error}"));
                        }
                    }
                    Ok(None) => on_notice.run(format!(
                        "{id}: replay accepted; the workflow runs it at its next pass"
                    )),
                    Err(message) => on_error.run(message),
                }
                load();
            });
        },
    );

    let purge = Callback::new(move |id: String| {
        set_armed.set(None);
        spawn_local(async move {
            match api::dlq_purge(&id).await {
                Ok(Some(report)) => {
                    on_notice.run(format!("{id}: purged {} letters", report.purged));
                }
                Ok(None) => on_notice.run(format!(
                    "{id}: purge accepted; the workflow runs it at its next pass"
                )),
                Err(message) => on_error.run(message),
            }
            load();
        });
    });

    view! {
        <Show
            when=move || queues.get().is_some_and(|list| !list.is_empty())
            fallback=move || {
                view! {
                    <div class="flex flex-col items-start gap-2 rounded-md border border-dashed border-border px-4 py-8">
                        <p class="text-sm text-muted-foreground">
                            {move || {
                                if !loaded.get() {
                                    "Loading the dead letter queues…".to_string()
                                } else {
                                    "No workflow declares a dlq block.".to_string()
                                }
                            }}
                        </p>
                    </div>
                }
            }
        >
            <div class="flex flex-col gap-4">
                {move || {
                    queues
                        .get()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|summary| {
                            queue_card(summary, armed, set_armed, replay, purge)
                        })
                        .collect_view()
                }}
            </div>
        </Show>
    }
}

/// One workflow's queue: the header facts, the two whole-queue buttons, and
/// the group table.
fn queue_card(
    summary: DlqSummary,
    armed: ReadSignal<Option<String>>,
    set_armed: WriteSignal<Option<String>>,
    replay: Callback<(String, Option<String>, Option<String>)>,
    purge: Callback<String>,
) -> AnyView {
    let id = summary.workflow.clone();
    let facts = format!(
        "{} {}, {} {}",
        summary.letters,
        plural(summary.letters, "letter"),
        summary.rows,
        plural(summary.rows, "row")
    );
    let replay_point = summary.replay.clone();

    let replay_all = {
        let id = id.clone();
        Callback::new(move |()| replay.run((id.clone(), None, None)))
    };
    let arm_or_purge = {
        let id = id.clone();
        Callback::new(move |()| {
            if armed.get_untracked().as_deref() == Some(id.as_str()) {
                purge.run(id.clone());
            } else {
                set_armed.set(Some(id.clone()));
            }
        })
    };
    // The only reactive part of a card: everything else is rebuilt from the
    // next poll's summary, but arming the button must show at once.
    let purge_label = {
        let id = id.clone();
        move || {
            if armed.get().as_deref() == Some(id.as_str()) {
                "confirm purge"
            } else {
                "purge"
            }
        }
    };

    // Two chips and two status lines that are present or absent, not
    // reactive: a plain conditional rather than `Show`, whose children must
    // be re-callable.
    let unscanned = (!summary.known).then(|| {
        view! {
            <Tooltip content=Signal::derive(|| {
                "No replay of this process has read the store through, so these counts cover \
                 what it recorded itself."
                    .to_string()
            })>
                <Badge tone=BadgeTone::Outline>"not scanned yet"</Badge>
            </Tooltip>
        }
        .into_any()
    });
    let pending = summary
        .replay_pending
        .then(|| view! { <Badge tone=BadgeTone::Outline>"replay pending"</Badge> }.into_any());
    let schedule = summary
        .next_auto_replay_unix_ms
        .map(|at| view! { <span>{schedule_phrase(at)}</span> }.into_any());
    let last = summary
        .last_replay
        .as_ref()
        .map(|report| view! { <span>{report_phrase(report)}</span> }.into_any());

    let body = if summary.groups.is_empty() {
        view! { <p class="text-sm text-muted-foreground">"Nothing is waiting."</p> }.into_any()
    } else {
        let rows = summary
            .groups
            .iter()
            .map(|group| group_row(id.clone(), group, replay))
            .collect_view();
        view! {
            <Table>
                <TableHeader>
                    <TableRow>
                        <TableHead>"sink"</TableHead>
                        <TableHead>"reason"</TableHead>
                        <TableHead>"letters"</TableHead>
                        <TableHead>"rows"</TableHead>
                        <TableHead>"first"</TableHead>
                        <TableHead>"last"</TableHead>
                        <TableHead>"replays"</TableHead>
                        <TableHead>""</TableHead>
                    </TableRow>
                </TableHeader>
                <TableBody>{rows}</TableBody>
            </Table>
        }
        .into_any()
    };

    view! {
        <Card>
            <CardHeader>
                <div class="flex flex-wrap items-center justify-between gap-x-4 gap-y-3">
                    <div class="flex flex-wrap items-center gap-2">
                        <CardTitle>{id.clone()}</CardTitle>
                        <Badge tone=BadgeTone::Secondary>{summary.store.clone()}</Badge>
                        <span class="font-mono text-xs text-muted-foreground tabular-nums">
                            {facts}
                        </span>
                        {unscanned}
                        {pending}
                    </div>
                    <div class="flex items-center gap-2">
                        <Tooltip content=Signal::derive(move || {
                            format!("Replay every letter, at this workflow's {replay_point} point.")
                        })>
                            <Button on_click=replay_all>"replay all"</Button>
                        </Tooltip>
                        <Tooltip content=Signal::derive(|| {
                            "Discard every letter. Click twice: nothing brings one back."
                                .to_string()
                        })>
                            <Button on_click=arm_or_purge tone=ButtonTone::Destructive>
                                {purge_label}
                            </Button>
                        </Tooltip>
                    </div>
                </div>
                <div class="mt-2 flex flex-col gap-0.5 font-mono text-xs text-muted-foreground">
                    {schedule}
                    {last}
                </div>
            </CardHeader>
            <CardContent>{body}</CardContent>
        </Card>
    }
    .into_any()
}

/// One `(sink, reason)` group, with a button that replays only it.
fn group_row(
    workflow: String,
    group: &DlqGroup,
    replay: Callback<(String, Option<String>, Option<String>)>,
) -> AnyView {
    let sink = group.sink.clone();
    let reason = group.reason.clone();
    let letters = group.letters;
    let rows = group.rows;
    let first = format_clock(group.first_failed_at_unix_ms);
    let last = format_clock(group.last_failed_at_unix_ms);
    let replays = group.max_replays;
    let on_click = {
        let workflow = workflow.clone();
        let sink = sink.clone();
        let reason = reason.clone();
        Callback::new(move |()| {
            replay.run((workflow.clone(), Some(sink.clone()), Some(reason.clone())))
        })
    };

    view! {
        <TableRow>
            <TableCell>
                <span class="font-mono text-xs">{sink}</span>
            </TableCell>
            <TableCell>
                <Tooltip content=Signal::derive({
                    let reason = reason.clone();
                    move || reason.clone()
                })>
                    <span class="block max-w-[28rem] truncate font-mono text-xs">
                        {reason.clone()}
                    </span>
                </Tooltip>
            </TableCell>
            <TableCell>
                <span class="font-mono text-xs tabular-nums">{letters}</span>
            </TableCell>
            <TableCell>
                <span class="font-mono text-xs tabular-nums">{rows}</span>
            </TableCell>
            <TableCell>
                <span class="font-mono text-xs tabular-nums">{first}</span>
            </TableCell>
            <TableCell>
                <span class="font-mono text-xs tabular-nums">{last}</span>
            </TableCell>
            <TableCell>
                {if replays == 0 {
                    view! { <span class="font-mono text-xs tabular-nums">"0"</span> }.into_any()
                } else {
                    view! {
                        <Badge tone=BadgeTone::Destructive>{replays.to_string()}</Badge>
                    }
                        .into_any()
                }}
            </TableCell>
            <TableCell>
                <Button on_click=on_click>"replay"</Button>
            </TableCell>
        </TableRow>
    }
    .into_any()
}

/// `"next automatic replay at 12:34:56 utc"`.
///
/// The service's own clock stamped the deadline, and the only other clock
/// here is the browser's, which may sit either side of it. Rendering the
/// instant rather than a countdown keeps the two from disagreeing, the same
/// reason every other timestamp on the dashboard is absolute UTC.
fn schedule_phrase(at_unix_ms: u64) -> String {
    format!("next automatic replay at {} utc", format_clock(at_unix_ms))
}

/// What the last replay did, in one line.
fn report_phrase(report: &saci_inspector_wire::DlqReplayReport) -> String {
    let mut line = format!(
        "{}: {} delivered, {} retained, {} purged",
        report.trigger.as_str(),
        report.delivered,
        report.retained,
        report.purged
    );
    if report.lost > 0 {
        line.push_str(&format!(", {} lost", report.lost));
    }
    if let Some(error) = &report.error {
        line.push_str(&format!(", {error}"));
    }
    line
}

/// `noun` or its plural, for a count the card renders beside it.
fn plural(count: u64, noun: &str) -> String {
    if count == 1 {
        noun.to_string()
    } else {
        format!("{noun}s")
    }
}
