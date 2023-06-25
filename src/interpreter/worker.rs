use crate::{
    interpreter::{clickhouse::Columns, clickhouse::TraceType, flamegraph, ContextArc},
    view::{self, Navigation},
};
use anyhow::{Error, Result};
use chdig::highlight_sql;
use chrono::DateTime;
use chrono_tz::Tz;
// FIXME: "leaky abstractions"
use cursive::traits::*;
use cursive::views;
use futures::channel::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use stopwatch::Stopwatch;

#[derive(Debug, Clone)]
pub enum Event {
    UpdateProcessList,
    UpdateSlowQueryLog,
    UpdateLastQueryLog,
    // ([query_ids], date)
    GetQueryTextLog(Vec<String>, Option<DateTime<Tz>>),
    ShowServerFlameGraph(TraceType),
    ShowQueryFlameGraph(TraceType, Vec<String>),
    // [query_ids]
    ShowLiveQueryFlameGraph(Vec<String>),
    UpdateSummary,
    // query_id
    KillQuery(String),
    // (database, query)
    ExplainPlan(String, String),
    // (database, query)
    ExplainPipeline(String, String),
    // (database, query)
    ExplainPlanIndexes(String, String),
    // TODO: support different types somehow
    // (view_name, query)
    ViewQuery(&'static str, String),
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
            context: None,
            sender,
            receiver,
            thread: None,
        };
    }

    pub fn start(&mut self, context: ContextArc) {
        let receiver = self.receiver.clone();
        let context = context.clone();
        self.thread = Some(std::thread::spawn(move || {
            start_tokio(context, receiver);
        }));
    }

    pub fn send(&mut self, event: Event) {
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
                            siv.call_on_name(
                                "processes",
                                move |view: &mut views::OnEventView<view::ProcessesView>| {
                                    view.get_inner_mut().update(block.unwrap());
                                },
                            );
                        }))
                        .unwrap();
                }
            }
            Event::UpdateSlowQueryLog => {
                let block = clickhouse
                    .get_slow_query_log(!options.view.no_subqueries)
                    .await;
                if check_block(&block) {
                    cb_sink
                        .send(Box::new(move |siv: &mut cursive::Cursive| {
                            siv.call_on_name(
                                "slow_query_log",
                                move |view: &mut views::OnEventView<view::ProcessesView>| {
                                    view.get_inner_mut().update(block.unwrap());
                                },
                            );
                        }))
                        .unwrap();
                }
            }
            Event::UpdateLastQueryLog => {
                let block = clickhouse
                    .get_last_query_log(!options.view.no_subqueries)
                    .await;
                if check_block(&block) {
                    cb_sink
                        .send(Box::new(move |siv: &mut cursive::Cursive| {
                            siv.call_on_name(
                                "last_query_log",
                                move |view: &mut views::OnEventView<view::ProcessesView>| {
                                    view.get_inner_mut().update(block.unwrap());
                                },
                            );
                        }))
                        .unwrap();
                }
            }
            Event::GetQueryTextLog(query_ids, event_time_microseconds) => {
                let block = clickhouse
                    .get_query_logs(&query_ids, event_time_microseconds)
                    .await;
                if check_block(&block) {
                    cb_sink
                        .send(Box::new(move |siv: &mut cursive::Cursive| {
                            siv.call_on_name("query_log", move |view: &mut view::TextLogView| {
                                view.update(block.unwrap());
                            });
                        }))
                        .unwrap();
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
            Event::ExplainPlanIndexes(database, query) => {
                let plan = clickhouse
                    .explain_plan_indexes(database.as_str(), query.as_str())
                    .await
                    .unwrap()
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
                    .unwrap();
            }
            Event::ExplainPlan(database, query) => {
                let syntax = clickhouse
                    .explain_syntax(database.as_str(), query.as_str())
                    .await
                    .unwrap()
                    .join("\n");
                let plan = clickhouse
                    .explain_plan(database.as_str(), query.as_str())
                    .await
                    .unwrap()
                    .join("\n");
                cb_sink
                    .send(Box::new(move |siv: &mut cursive::Cursive| {
                        siv.add_layer(
                            views::Dialog::around(
                                views::LinearLayout::vertical()
                                    .child(views::TextView::new(highlight_sql(&syntax).unwrap()))
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
            Event::ExplainPipeline(database, query) => {
                let syntax = clickhouse
                    .explain_syntax(database.as_str(), query.as_str())
                    .await
                    .unwrap()
                    .join("\n");
                let pipeline = clickhouse
                    .explain_pipeline(database.as_str(), query.as_str())
                    .await
                    .unwrap()
                    .join("\n");
                cb_sink
                    .send(Box::new(move |siv: &mut cursive::Cursive| {
                        siv.add_layer(
                            views::Dialog::around(
                                views::LinearLayout::vertical()
                                    .child(views::TextView::new(highlight_sql(&syntax).unwrap()))
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
            Event::ViewQuery(view_name, query) => {
                let block = clickhouse.execute(query.as_str()).await;
                if check_block(&block) {
                    cb_sink
                        .send(Box::new(move |siv: &mut cursive::Cursive| {
                            // TODO: update specific view (can we accept type somehow in the enum?)
                            siv.call_on_name(view_name, move |view: &mut view::QueryResultView| {
                                view.update(block.unwrap());
                            });
                        }))
                        .unwrap();
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

    log::info!("Event worker finished");
}
