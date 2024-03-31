use crate::{
    common::Stopwatch,
    interpreter::clickhouse::{Columns, TraceType},
    interpreter::{flamegraph, ContextArc},
    view::{self, Navigation},
};
use anyhow::{anyhow, Result};
use chdig::{highlight_sql, open_graph_in_browser};
use chrono::{DateTime, Local};
// FIXME: "leaky abstractions"
use cursive::traits::*;
use cursive::views;
use futures::channel::mpsc;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum Event {
    // [filter, limit]
    UpdateProcessList(String, u64),
    // [filter, start, end, limit]
    UpdateSlowQueryLog(String, DateTime<Local>, DateTime<Local>, u64),
    // [filter, start, end, limit]
    UpdateLastQueryLog(String, DateTime<Local>, DateTime<Local>, u64),
    // (view_name, [query_ids], start, end)
    GetQueryTextLog(
        &'static str,
        Option<Vec<String>>,
        DateTime<Local>,
        Option<DateTime<Local>>,
    ),
    // [bool (true - show in TUI, false - open in browser), type, start, end]
    ShowServerFlameGraph(bool, TraceType, DateTime<Local>, DateTime<Local>),
    // (type, bool (true - show in TUI, false - open in browser), start time, end time, [query_ids])
    ShowQueryFlameGraph(
        TraceType,
        bool,
        DateTime<Local>,
        Option<DateTime<Local>>,
        Vec<String>,
    ),
    // [bool (true - show in TUI, false - open in browser), query_ids]
    ShowLiveQueryFlameGraph(bool, Vec<String>),
    UpdateSummary,
    // query_id
    KillQuery(String),
    // (database, query)
    ExecuteQuery(String, String),
    // (database, query)
    ExplainSyntax(String, String, HashMap<String, String>),
    // (database, query)
    ExplainPlan(String, String),
    // (database, query)
    ExplainPipeline(String, String),
    // (database, query)
    ExplainPipelineOpenGraphInBrowser(String, String),
    // (database, query)
    ExplainPlanIndexes(String, String),
    // TODO: support different types somehow
    // (view_name, query)
    ViewQuery(&'static str, String),
}

type ReceiverArc = Arc<Mutex<mpsc::Receiver<Event>>>;
type Sender = mpsc::Sender<Event>;

pub struct Worker {
    sender: Sender,
    receiver: ReceiverArc,
    thread: Option<thread::JoinHandle<()>>,
    paused: bool,
}

// TODO: can we simplify things with callbacks? (EnumValue(Type))
impl Worker {
    pub fn new() -> Self {
        // Here the futures::channel::mpsc::channel is used over standard std::sync::mpsc::channel,
        // since standard does not allow to configure backlog (queue max size), while in case of
        // very low --delay-interval it may fill queue with i.e. UpdateProcessList, which can be
        // quite heavy, especially with the --cluster, and this will lead to UI will not show
        // anything else until it will get to the event that is requried for that action.
        //
        // Note, by default channel reserves slot for each sender [1].
        //
        //   [1]: https://github.com/rust-lang/futures-rs/issues/403
        let (sender, receiver) = mpsc::channel::<Event>(1);
        let receiver = Arc::new(Mutex::new(receiver));

        return Worker {
            sender,
            receiver,
            thread: None,
            paused: false,
        };
    }

    pub fn start(&mut self, context: ContextArc) {
        let receiver = self.receiver.clone();
        let context = context.clone();
        self.thread = Some(std::thread::spawn(move || {
            start_tokio(context, receiver);
        }));
    }

    pub fn toggle_pause(&mut self) {
        self.paused = !self.paused;
        log::trace!(
            "Toggle pause ({})",
            if self.paused { "paused" } else { "unpaused" }
        );
    }

    pub fn is_paused(&self) -> bool {
        return self.paused;
    }

    pub fn send(&mut self, event: Event) {
        if self.paused {
            return;
        }

        log::trace!("Sending event: {:?}", event);
        // Simply ignore errors (queue is full, likely update interval is too short)
        self.sender.try_send(event.clone()).unwrap_or_else(|e| {
            log::error!(
                "Cannot send event {:?}: {} (too low --delay-interval?)",
                event,
                e
            )
        });
    }
}

#[tokio::main(flavor = "current_thread")]
async fn start_tokio(context: ContextArc, receiver: ReceiverArc) {
    let mut slow_processing = false;

    log::info!("Event worker started");

    loop {
        let result = receiver.lock().unwrap().try_next();

        // No message available.
        if result.is_err() {
            // Same as INPUT_POLL_DELAY_MS, but I hate such implementations, both should be fixed.
            thread::sleep(Duration::from_millis(30));

            continue;
        }

        let event_result = result.unwrap();
        // Channel closed.
        if event_result.is_none() {
            break;
        }

        let event = event_result.unwrap();
        log::trace!("Got event: {:?}", event);

        let mut need_clear = false;
        let cb_sink = context.lock().unwrap().cb_sink.clone();
        let options = context.lock().unwrap().options.clone();

        let update_status = |message: &str| {
            let content = message.to_string();
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.set_statusbar_content(content.to_string());
                }))
                // Ignore errors on exit
                .unwrap_or_default();
        };

        let mut status = format!("Processing {:?}...", event);
        if slow_processing {
            status.push_str(" (Processing takes too long, consider increasing --delay_interval)");
        }
        update_status(&status);

        let stopwatch = Stopwatch::start_new();
        if let Err(err) = process_event(context.clone(), event.clone(), &mut need_clear).await {
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(views::Dialog::info(err.to_string()));
                }))
                // Ignore errors on exit
                .unwrap_or_default();
        }
        update_status(&format!(
            "Processing {:?} took {} ms.",
            event,
            stopwatch.elapsed_ms(),
        ));

        // It should not be reseted, since delay_interval should be set to the maximum service
        // query duration time.
        if stopwatch.elapsed() > options.view.delay_interval {
            slow_processing = true;
        }

        cb_sink
            .send(Box::new(move |siv: &mut cursive::Cursive| {
                if need_clear {
                    siv.clear();
                }
                siv.on_event(cursive::event::Event::Refresh);
            }))
            // Ignore errors on exit
            .unwrap_or_default();
    }

    log::info!("Event worker finished");
}

async fn render_flamegraph(tui: bool, cb_sink: cursive::CbSink, block: Columns) -> Result<()> {
    if tui {
        cb_sink
            .send(Box::new(move |siv: &mut cursive::Cursive| {
                flamegraph::show(block)
                    .or_else(|e| {
                        siv.add_layer(views::Dialog::info(e.to_string()));
                        return anyhow::Ok(());
                    })
                    .unwrap();
            }))
            .map_err(|_| anyhow!("Cannot send message to UI"))?;
    } else {
        flamegraph::open_in_speedscope(block).await?;
    }
    return Ok(());
}

async fn process_event(context: ContextArc, event: Event, need_clear: &mut bool) -> Result<()> {
    let cb_sink = context.lock().unwrap().cb_sink.clone();
    let clickhouse = context.lock().unwrap().clickhouse.clone();

    match event {
        Event::UpdateProcessList(filter, limit) => {
            let block = clickhouse.get_processlist(filter, limit).await?;
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.call_on_name_or_render_error(
                        "processes",
                        move |view: &mut views::OnEventView<view::ProcessesView>| {
                            return view.get_inner_mut().update(block);
                        },
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::UpdateSlowQueryLog(filter, start, end, limit) => {
            let block = clickhouse
                .get_slow_query_log(&filter, start, end, limit)
                .await?;
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.call_on_name_or_render_error(
                        "slow_query_log",
                        move |view: &mut views::OnEventView<view::ProcessesView>| {
                            return view.get_inner_mut().update(block);
                        },
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::UpdateLastQueryLog(filter, start, end, limit) => {
            let block = clickhouse
                .get_last_query_log(&filter, start, end, limit)
                .await?;
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.call_on_name_or_render_error(
                        "last_query_log",
                        move |view: &mut views::OnEventView<view::ProcessesView>| {
                            return view.get_inner_mut().update(block);
                        },
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::GetQueryTextLog(view_name, query_ids, start_microseconds, end_microseconds) => {
            let block = clickhouse
                .get_query_logs(&query_ids, start_microseconds, end_microseconds)
                .await?;
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.call_on_name_or_render_error(
                        view_name,
                        move |view: &mut view::TextLogView| {
                            return view.update(block);
                        },
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::ShowServerFlameGraph(tui, trace_type, start, end) => {
            let flamegraph_block = clickhouse
                .get_flamegraph(trace_type, None, Some(start), Some(end))
                .await?;
            render_flamegraph(tui, cb_sink, flamegraph_block).await?;
            *need_clear = true;
        }
        Event::ShowQueryFlameGraph(trace_type, tui, start, end, query_ids) => {
            let flamegraph_block = clickhouse
                .get_flamegraph(trace_type, Some(&query_ids), Some(start), end)
                .await?;
            render_flamegraph(tui, cb_sink, flamegraph_block).await?;
            *need_clear = true;
        }
        Event::ShowLiveQueryFlameGraph(tui, query_ids) => {
            let flamegraph_block = clickhouse.get_live_query_flamegraph(&query_ids).await?;
            render_flamegraph(tui, cb_sink, flamegraph_block).await?;
            *need_clear = true;
        }
        Event::ExplainPlanIndexes(database, query) => {
            let plan = clickhouse
                .explain_plan_indexes(database.as_str(), query.as_str())
                .await?
                .join("\n");
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(
                        views::Dialog::around(
                            views::LinearLayout::vertical()
                                .child(views::TextView::new("EXPLAIN PLAN indexes=1").center())
                                .child(views::DummyView.fixed_height(1))
                                .child(views::TextView::new(plan)),
                        )
                        .scrollable(),
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::ExecuteQuery(database, query) => {
            let stopwatch = Stopwatch::start_new();
            clickhouse
                .execute_query(database.as_str(), query.as_str())
                .await?;
            // TODO: print results?
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(views::Dialog::info(format!(
                        "Query executed ({} ms). Look results in 'Last queries'",
                        stopwatch.elapsed_ms(),
                    )));
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::ExplainSyntax(database, query, settings) => {
            let query = clickhouse
                .explain_syntax(database.as_str(), query.as_str(), &settings)
                .await?
                .join("\n");
            let query = highlight_sql(&query)?;
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(
                        views::Dialog::around(
                            views::LinearLayout::vertical()
                                .child(views::TextView::new("EXPLAIN SYNTAX").center())
                                .child(views::DummyView.fixed_height(1))
                                .child(views::TextView::new(query)),
                        )
                        .scrollable(),
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::ExplainPlan(database, query) => {
            let plan = clickhouse
                .explain_plan(database.as_str(), query.as_str())
                .await?
                .join("\n");
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(
                        views::Dialog::around(
                            views::LinearLayout::vertical()
                                .child(views::TextView::new("EXPLAIN PLAN").center())
                                .child(views::DummyView.fixed_height(1))
                                .child(views::TextView::new(plan)),
                        )
                        .scrollable(),
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::ExplainPipeline(database, query) => {
            let pipeline = clickhouse
                .explain_pipeline(database.as_str(), query.as_str())
                .await?
                .join("\n");
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(
                        views::Dialog::around(
                            views::LinearLayout::vertical()
                                .child(views::TextView::new("EXPLAIN PIPELINE").center())
                                .child(views::DummyView.fixed_height(1))
                                .child(views::TextView::new(pipeline)),
                        )
                        .scrollable(),
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::ExplainPipelineOpenGraphInBrowser(database, query) => {
            let pipeline = clickhouse
                .explain_pipeline_graph(database.as_str(), query.as_str())
                .await?
                .join("\n");
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    open_graph_in_browser(pipeline)
                        .or_else(|err| {
                            siv.add_layer(views::Dialog::info(err.to_string()));
                            return anyhow::Ok(());
                        })
                        .unwrap();
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::KillQuery(query_id) => {
            let ret = clickhouse.kill_query(query_id.as_str()).await;
            // NOTE: should we do this via cursive, to block the UI?
            let message;
            if let Err(err) = ret {
                message = err.to_string().clone();
            } else {
                message = format!("Query {} killed", query_id).to_string();
            }
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(views::Dialog::info(message));
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::UpdateSummary => {
            let block = clickhouse.get_summary().await;
            match block {
                Err(err) => {
                    let message = err.to_string().clone();
                    cb_sink
                        .send(Box::new(move |siv: &mut cursive::Cursive| {
                            siv.add_layer(views::Dialog::info(message));
                        }))
                        .map_err(|_| anyhow!("Cannot send message to UI"))?;
                }
                Ok(summary) => {
                    cb_sink
                        .send(Box::new(move |siv: &mut cursive::Cursive| {
                            siv.call_on_name("summary", move |view: &mut view::SummaryView| {
                                view.update(summary);
                            });
                        }))
                        .map_err(|_| anyhow!("Cannot send message to UI"))?;
                }
            }
        }
        Event::ViewQuery(view_name, query) => {
            let block = clickhouse.execute(query.as_str()).await?;
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    // TODO: update specific view (can we accept type somehow in the enum?)
                    siv.call_on_name_or_render_error(
                        view_name,
                        move |view: &mut view::QueryResultView| {
                            return view.update(block);
                        },
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
    }

    return Ok(());
}
