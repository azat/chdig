use crate::interpreter::{clickhouse::Columns, clickhouse::TraceType, flamegraph, ContextArc};
use crate::view::Navigation;
use crate::view::{self, utils};
use anyhow::{Error, Result};
use chrono::DateTime;
use chrono_tz::Tz;
// FIXME: "leaky abstractions"
use cursive::traits::*;
use cursive::views;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use stopwatch::Stopwatch;

#[derive(Debug, Clone)]
pub enum Event {
    UpdateProcessList,
    GetQueryTextLog(String, Option<DateTime<Tz>>),
    ShowServerFlameGraph(TraceType),
    ShowQueryFlameGraph(TraceType, Vec<String>),
    ShowLiveQueryFlameGraph(Vec<String>),
    UpdateSummary,
    KillQuery(String),
    ExplainPlan(String),
    ExplainPipeline(String),
    GetMergesList,
    GetReplicationQueueList,
    GetReplicatedFetchesList,
}

type ReceiverArc = Arc<Mutex<mpsc::Receiver<Event>>>;
type Sender = mpsc::Sender<Event>;

pub struct Worker {
    pub context: Option<ContextArc>,
    sender: Sender,
    receiver: ReceiverArc,
    thread: Option<thread::JoinHandle<()>>,
}

// TODO: can we simplify things with callbacks? (EnumValue(Type))
impl Worker {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel::<Event>();
        let receiver = Arc::new(Mutex::new(receiver));

        return Worker {
            context: None,
            sender,
            receiver,
            thread: None,
        };
    }

    pub fn start(&mut self, context: ContextArc) {
        let receiver = self.receiver.clone();
        self.thread = Some(std::thread::spawn(move || {
            start_tokio(context, receiver);
        }));
    }

    pub fn send(&mut self, event: Event) {
        self.sender.send(event).unwrap();
    }
}

#[tokio::main(flavor = "current_thread")]
async fn start_tokio(context: ContextArc, receiver: ReceiverArc) {
    let mut slow_processing = false;

    while let Ok(event) = receiver.lock().unwrap().recv() {
        let mut need_clear = false;
        let cb_sink = context.lock().unwrap().cb_sink.clone();
        let clickhouse = context.lock().unwrap().clickhouse.clone();
        let options = context.lock().unwrap().options.clone();

        let render_error = |err: &Error| {
            let message = err.to_string().clone();
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(views::Dialog::info(message));
                }))
                .unwrap();
        };

        let update_status = |message: &str| {
            let content = message.to_string();
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.set_statusbar_content(content.to_string());
                }))
                .unwrap();
        };

        // NOTE: rewrite to .unwrap_or_else() ?
        let check_block = |block_result: &Result<Columns>| -> bool {
            if let Err(err) = block_result {
                render_error(err);
                return false;
            }
            return true;
        };

        let processing_event = event.clone();
        let mut status = format!("Processing {:?}...", processing_event);
        if slow_processing {
            status.push_str(" (Processing takes too long, consider increasing --delay_interval)");
        }
        update_status(&status);
        let stopwatch = Stopwatch::start_new();

        match event {
            Event::UpdateProcessList => {
                let block = clickhouse
                    .get_processlist(!options.view.no_subqueries)
                    .await;
                if check_block(&block) {
                    cb_sink
                        .send(Box::new(move |siv: &mut cursive::Cursive| {
                            siv.call_on_name("processes", move |view: &mut view::ProcessesView| {
                                view.update(block.unwrap());
                            });
                        }))
                        .unwrap();
                }
            }
            Event::GetMergesList => {
                let block = clickhouse.get_merges().await;
                if check_block(&block) {
                    cb_sink
                        .send(Box::new(move |siv: &mut cursive::Cursive| {
                            siv.call_on_name("merges", move |view: &mut view::MergesView| {
                                view.update(block.unwrap());
                            });
                        }))
                        .unwrap();
                }
            }
            Event::GetReplicationQueueList => {
                let block = clickhouse.get_replication_queue().await;
                if check_block(&block) {
                    cb_sink
                        .send(Box::new(move |siv: &mut cursive::Cursive| {
                            siv.call_on_name(
                                "replication_queue",
                                move |view: &mut view::ReplicationQueueView| {
                                    view.update(block.unwrap());
                                },
                            );
                        }))
                        .unwrap();
                }
            }
            Event::GetReplicatedFetchesList => {
                let block = clickhouse.get_replicated_fetches().await;
                if check_block(&block) {
                    cb_sink
                        .send(Box::new(move |siv: &mut cursive::Cursive| {
                            siv.call_on_name(
                                "replication_fetches",
                                move |view: &mut view::ReplicatedFetchesView| {
                                    view.update(block.unwrap());
                                },
                            );
                        }))
                        .unwrap();
                }
            }
            Event::GetQueryTextLog(query_id, event_time_microseconds) => {
                let query_logs_block = clickhouse
                    .get_query_logs(query_id.as_str(), event_time_microseconds)
                    .await;
                if check_block(&query_logs_block) {
                    context.lock().unwrap().query_logs = Some(query_logs_block.unwrap());
                }
            }
            Event::ShowServerFlameGraph(trace_type) => {
                let flamegraph_block = clickhouse.get_flamegraph(trace_type, None).await;

                // NOTE: should we do this via cursive, to block the UI?
                if check_block(&flamegraph_block) {
                    flamegraph::show(flamegraph_block.unwrap())
                        .unwrap_or_else(|e| render_error(&e));
                    need_clear = true;
                }
            }
            Event::ShowQueryFlameGraph(trace_type, query_ids) => {
                let flamegraph_block = clickhouse
                    .get_flamegraph(trace_type, Some(&query_ids))
                    .await;
                // NOTE: should we do this via cursive, to block the UI?
                if check_block(&flamegraph_block) {
                    flamegraph::show(flamegraph_block.unwrap())
                        .unwrap_or_else(|e| render_error(&e));
                    need_clear = true;
                }
            }
            Event::ShowLiveQueryFlameGraph(query_ids) => {
                let flamegraph_block = clickhouse.get_live_query_flamegraph(&query_ids).await;
                // NOTE: should we do this via cursive, to block the UI?
                if check_block(&flamegraph_block) {
                    flamegraph::show(flamegraph_block.unwrap())
                        .unwrap_or_else(|e| render_error(&e));
                    need_clear = true;
                }
            }
            Event::ExplainPlan(query) => {
                let syntax = clickhouse
                    .explain_syntax(query.as_str())
                    .await
                    .unwrap()
                    .join("\n");
                let plan = clickhouse
                    .explain_plan(query.as_str())
                    .await
                    .unwrap()
                    .join("\n");
                cb_sink
                    .send(Box::new(move |siv: &mut cursive::Cursive| {
                        siv.add_layer(
                            views::Dialog::around(
                                views::LinearLayout::vertical()
                                    .child(views::TextView::new(
                                        utils::highlight_sql(&syntax).unwrap(),
                                    ))
                                    .child(views::DummyView.fixed_height(1))
                                    .child(views::TextView::new("EXPLAIN PLAN").center())
                                    .child(views::DummyView.fixed_height(1))
                                    .child(views::TextView::new(plan)),
                            )
                            .scrollable(),
                        );
                    }))
                    .unwrap();
            }
            Event::ExplainPipeline(query) => {
                let syntax = clickhouse
                    .explain_syntax(query.as_str())
                    .await
                    .unwrap()
                    .join("\n");
                let pipeline = clickhouse
                    .explain_pipeline(query.as_str())
                    .await
                    .unwrap()
                    .join("\n");
                cb_sink
                    .send(Box::new(move |siv: &mut cursive::Cursive| {
                        siv.add_layer(
                            views::Dialog::around(
                                views::LinearLayout::vertical()
                                    .child(views::TextView::new(
                                        utils::highlight_sql(&syntax).unwrap(),
                                    ))
                                    .child(views::DummyView.fixed_height(1))
                                    .child(views::TextView::new("EXPLAIN PIPELINE").center())
                                    .child(views::DummyView.fixed_height(1))
                                    .child(views::TextView::new(pipeline)),
                            )
                            .scrollable(),
                        );
                    }))
                    .unwrap();
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
                // TODO: move to status bar
                cb_sink
                    .send(Box::new(move |siv: &mut cursive::Cursive| {
                        siv.add_layer(views::Dialog::info(message));
                    }))
                    .unwrap();
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
                            .unwrap();
                    }
                    Ok(summary) => {
                        cb_sink
                            .send(Box::new(move |siv: &mut cursive::Cursive| {
                                siv.call_on_name("summary", move |view: &mut view::SummaryView| {
                                    view.update(summary);
                                });
                            }))
                            .unwrap();
                    }
                }
            }
        }

        update_status(&format!(
            "Processing {:?} took {} ms.",
            processing_event,
            stopwatch.elapsed_ms(),
        ));
        // It should not be reseted, since delay_interval should be set to the maximum service
        // query duration time.
        if stopwatch.elapsed() > context.lock().unwrap().options.view.delay_interval {
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
}
