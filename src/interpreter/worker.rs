use crate::interpreter::{clickhouse::Columns, clickhouse::TraceType, flamegraph, ContextArc};
use crate::view::{self, utils};
use anyhow::{Error, Result};
use chrono::DateTime;
use chrono_tz::Tz;
// FIXME: "leaky abstractions"
use cursive::traits::*;
use cursive::views;
use humantime::format_duration;
use size::{Base, SizeFormatter, Style};
use std::rc::Rc;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

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

        // NOTE: rewrite to .unwrap_or_else() ?
        let check_block = |block_result: &Result<Columns>| -> bool {
            if let Err(err) = block_result {
                render_error(err);
                return false;
            }
            return true;
        };

        match event {
            Event::UpdateProcessList => {
                let process_list_block = clickhouse
                    .get_processlist(!options.view.no_subqueries)
                    .await;
                if check_block(&process_list_block) {
                    context.lock().unwrap().processes = Some(process_list_block.unwrap());
                    cb_sink
                        .send(Box::new(move |siv: &mut cursive::Cursive| {
                            siv.call_on_name("processes", move |view: &mut view::ProcessesView| {
                                view.update_processes().unwrap();
                            });
                        }))
                        .unwrap();
                }
            }
            Event::GetMergesList => {
                let block = clickhouse.get_merges().await;
                if check_block(&block) {
                    context.lock().unwrap().merges = Some(block.unwrap());
                }
            }
            Event::GetReplicationQueueList => {
                let block = clickhouse.get_replication_queue().await;
                if check_block(&block) {
                    context.lock().unwrap().replication_queue = Some(block.unwrap());
                }
            }
            Event::GetReplicatedFetchesList => {
                let block = clickhouse.get_replicated_fetches().await;
                if check_block(&block) {
                    context.lock().unwrap().replicated_fetches = Some(block.unwrap());
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
                let summary_block = clickhouse.get_summary().await;
                match summary_block {
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
                                let fmt = Rc::new(
                                    SizeFormatter::new()
                                        .with_base(Base::Base2)
                                        .with_style(Style::Abbreviated),
                                );
                                let fmt_ref = fmt.as_ref();

                                if siv.find_name::<views::TextView>("mem").is_none() {
                                    return;
                                }

                                siv.call_on_name("mem", move |view: &mut views::TextView| {
                                    let mut description: Vec<String> = Vec::new();

                                    let mut add_description = |prefix: &str, value: u64| {
                                        if value > 100_000_000 {
                                            description.push(format!(
                                                "{}: {}",
                                                prefix,
                                                fmt_ref.format(value as i64)
                                            ));
                                        }
                                    };
                                    add_description("Tracked", summary.memory.tracked);
                                    add_description("Tables", summary.memory.tables);
                                    add_description("Caches", summary.memory.caches);
                                    add_description("Queries", summary.memory.processes);
                                    add_description("Merges", summary.memory.merges);
                                    add_description("Dictionaries", summary.memory.dictionaries);
                                    add_description("Indexes", summary.memory.primary_keys);

                                    view.set_content(format!(
                                        "{} / {} ({})",
                                        fmt_ref.format(summary.memory.resident as i64),
                                        fmt_ref.format(summary.memory.os_total as i64),
                                        description.join(", "),
                                    ));
                                })
                                .expect("No such view 'mem'");

                                siv.call_on_name("cpu", move |view: &mut views::TextView| {
                                    view.set_content(format!(
                                        "{} / {}",
                                        summary.cpu.user + summary.cpu.system,
                                        summary.cpu.count,
                                    ));
                                })
                                .expect("No such view 'cpu'");

                                siv.call_on_name("threads", move |view: &mut views::TextView| {
                                    let mut basic: Vec<String> = Vec::new();
                                    let mut add_basic = |prefix: &str, value: u64| {
                                        if value > 0 {
                                            basic.push(format!("{}: {}", prefix, value));
                                        }
                                    };
                                    add_basic("HTTP", summary.threads.http);
                                    add_basic("TCP", summary.threads.tcp);
                                    add_basic("Interserver", summary.threads.interserver);

                                    view.set_content(format!(
                                        "{} / {} ({})",
                                        summary.threads.os_runnable,
                                        summary.threads.os_total,
                                        basic.join(", "),
                                    ));
                                })
                                .expect("No such view 'threads'");

                                siv.call_on_name("pools", move |view: &mut views::TextView| {
                                    let mut pools: Vec<String> = Vec::new();
                                    let mut add_pool = |prefix: &str, value: u64| {
                                        if value > 0 {
                                            pools.push(format!("{}: {}", prefix, value));
                                        }
                                    };
                                    add_pool("Merges", summary.threads.pools.merges_mutations);
                                    add_pool("Fetches", summary.threads.pools.fetches);
                                    add_pool("Common", summary.threads.pools.common);
                                    add_pool("Moves", summary.threads.pools.moves);
                                    add_pool("Schedule", summary.threads.pools.schedule);
                                    add_pool("Buffer", summary.threads.pools.buffer_flush);
                                    add_pool("Distributed", summary.threads.pools.distributed);
                                    add_pool("Brokers", summary.threads.pools.message_broker);

                                    view.set_content(pools.join(", "));
                                })
                                .expect("No such view 'pools'");

                                siv.call_on_name("net_recv", move |view: &mut views::TextView| {
                                    view.set_content(
                                        fmt_ref.format(summary.network.receive_bytes as i64),
                                    );
                                })
                                .expect("No such view 'net_recv'");
                                siv.call_on_name("net_sent", move |view: &mut views::TextView| {
                                    view.set_content(
                                        fmt_ref.format(summary.network.send_bytes as i64),
                                    );
                                })
                                .expect("No such view 'net_sent'");

                                siv.call_on_name("disk_read", move |view: &mut views::TextView| {
                                    view.set_content(
                                        fmt_ref.format(summary.blkdev.read_bytes as i64),
                                    );
                                })
                                .expect("No such view 'disk_read'");
                                siv.call_on_name(
                                    "disk_write",
                                    move |view: &mut views::TextView| {
                                        view.set_content(
                                            fmt_ref.format(summary.blkdev.write_bytes as i64),
                                        );
                                    },
                                )
                                .expect("No such view 'disk_write'");

                                siv.call_on_name("uptime", move |view: &mut views::TextView| {
                                    view.set_content(
                                        format_duration(Duration::from_secs(summary.uptime.server))
                                            .to_string(),
                                    );
                                })
                                .expect("No such view 'uptime'");

                                siv.call_on_name("queries", move |view: &mut views::TextView| {
                                    view.set_content(summary.processes.to_string());
                                })
                                .expect("No such view 'queries'");
                            }))
                            .unwrap();
                    }
                }
            }
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
