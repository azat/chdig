use crate::{
    common::{RelativeDateTime, Stopwatch},
    interpreter::{
        ContextArc,
        clickhouse::{Columns, TextLogArguments, TraceType},
        flamegraph,
    },
    pastila,
    utils::{highlight_sql, share_graph},
    view::{self, Navigation},
};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Local};
// FIXME: "leaky abstractions"
use clickhouse_rs::errors::Error as ClickHouseError;
use cursive::traits::*;
use cursive::views;
use futures::channel::mpsc;
use std::collections::{HashMap, hash_map::Entry};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum Event {
    // [filter, limit]
    ProcessList(String, u64),
    // [filter, start, end, limit]
    SlowQueryLog(String, RelativeDateTime, RelativeDateTime, u64),
    // [filter, start, end, limit]
    LastQueryLog(String, RelativeDateTime, RelativeDateTime, u64),
    // (view_name, args)
    TextLog(&'static str, TextLogArguments),
    // [bool (true - show in TUI, false - share via pastila), type, start, end]
    ServerFlameGraph(bool, TraceType, DateTime<Local>, DateTime<Local>),
    // [bool (true - show in TUI, false - share via pastila)]
    JemallocFlameGraph(bool),
    // (type, bool (true - show in TUI, false - open in browser), start time, end time, [query_ids])
    QueryFlameGraph(
        TraceType,
        bool,
        DateTime<Local>,
        Option<DateTime<Local>>,
        Vec<String>,
    ),
    // [bool (true - show in TUI, false - open in browser), query_ids]
    LiveQueryFlameGraph(bool, Option<Vec<String>>),
    Summary,
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
    ExplainPipelineShareGraph(String, String),
    // (database, query)
    ExplainPlanIndexes(String, String),
    // (database, table)
    ShowCreateTable(String, String),
    // (view_name, query)
    SQLQuery(&'static str, String),
    // (log_name, database, table, start, end)
    BackgroundSchedulePoolLogs(
        Option<String>,
        String,
        String,
        RelativeDateTime,
        RelativeDateTime,
    ),
    // (database, table)
    TableParts(String, String),
    // (database, table)
    AsynchronousInserts(String, String),
    // (content to share via pastila)
    ShareLogs(String),
}

impl Event {
    fn enum_key(&self) -> String {
        match self {
            Event::ProcessList(..) => "ProcessList".to_string(),
            Event::SlowQueryLog(..) => "SlowQueryLog".to_string(),
            Event::LastQueryLog(..) => "LastQueryLog".to_string(),
            Event::TextLog(..) => "TextLog".to_string(),
            Event::ServerFlameGraph(..) => "ServerFlameGraph".to_string(),
            Event::JemallocFlameGraph(..) => "JemallocFlameGraph".to_string(),
            Event::QueryFlameGraph(..) => "QueryFlameGraph".to_string(),
            Event::LiveQueryFlameGraph(..) => "LiveQueryFlameGraph".to_string(),
            Event::Summary => "Summary".to_string(),
            Event::KillQuery(..) => "KillQuery".to_string(),
            Event::ExecuteQuery(..) => "ExecuteQuery".to_string(),
            Event::ExplainSyntax(..) => "ExplainSyntax".to_string(),
            Event::ExplainPlan(..) => "ExplainPlan".to_string(),
            Event::ExplainPipeline(..) => "ExplainPipeline".to_string(),
            Event::ExplainPipelineShareGraph(..) => "ExplainPipelineShareGraph".to_string(),
            Event::ExplainPlanIndexes(..) => "ExplainPlanIndexes".to_string(),
            Event::ShowCreateTable(..) => "ShowCreateTable".to_string(),
            Event::SQLQuery(view_name, _query) => format!("SQLQuery({})", view_name),
            Event::BackgroundSchedulePoolLogs(..) => "BackgroundSchedulePoolLogs".to_string(),
            Event::TableParts(..) => "TableParts".to_string(),
            Event::AsynchronousInserts(..) => "AsynchronousInserts".to_string(),
            Event::ShareLogs(..) => "ShareLogs".to_string(),
        }
    }
}

type ReceiverArc = Arc<Mutex<mpsc::Receiver<Event>>>;
type Sender = mpsc::Sender<Event>;

pub struct Worker {
    sender: Sender,
    sender_by_event: HashMap<String, Sender>,
    receiver: ReceiverArc,
    thread: Option<thread::JoinHandle<()>>,
    paused: bool,
}

// TODO: can we simplify things with callbacks? (EnumValue(Type))
impl Worker {
    pub fn new() -> Self {
        // Here the futures::channel::mpsc::channel is used over standard std::sync::mpsc::channel,
        // since standard does not allow to configure backlog (queue max size), while we uses
        // channel per distinct event (to avoid running multiple queries for the same view, since
        // it does not make sense), i.e. separate channel for Summary, separate for
        // UpdateProcessList and so on.
        //
        // Note, by default channel reserves slot for each sender [1].
        //
        //   [1]: https://github.com/rust-lang/futures-rs/issues/403
        let (sender, receiver) = mpsc::channel::<Event>(1);
        let receiver = Arc::new(Mutex::new(receiver));

        return Worker {
            sender,
            sender_by_event: HashMap::new(),
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

    // @force - ignore pause
    pub fn send(&mut self, force: bool, event: Event) {
        if !force && self.paused {
            return;
        }

        let entry = self.sender_by_event.entry(event.enum_key());
        let channel_created = matches!(&entry, Entry::Vacant(_));
        let sender = entry.or_insert(self.sender.clone());

        log::trace!(
            "Sending event: {:?} (channel created: {})",
            &event,
            channel_created
        );

        // Simply ignore errors (queue is full, likely update interval is too short)
        sender.try_send(event.clone()).unwrap_or_else(|e| {
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
    log::info!("Event worker started");

    loop {
        let event = match receiver.lock().unwrap().try_recv() {
            Ok(event) => event,
            // Channel closed.
            Err(mpsc::TryRecvError::Closed) => break,
            // No message available.
            Err(mpsc::TryRecvError::Empty) => {
                // Same as INPUT_POLL_DELAY_MS, but I hate such implementations, both should be fixed.
                thread::sleep(Duration::from_millis(30));
                continue;
            }
        };
        log::trace!("Got event: {:?}", event);

        let mut need_clear = false;
        let cb_sink = context.lock().unwrap().cb_sink.clone();
        let options = context.lock().unwrap().options.clone();

        let update_status = |message: &str| {
            let content = message.to_string();
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.set_statusbar_content(content);
                }))
                // Ignore errors on exit
                .unwrap_or_default();
        };

        update_status(&format!("Processing {}...", event.enum_key()));

        let stopwatch = Stopwatch::start_new();
        if let Err(err) = process_event(context.clone(), event.clone(), &mut need_clear).await {
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    let is_paused = siv
                        .user_data::<ContextArc>()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .worker
                        .is_paused();
                    if !is_paused {
                        siv.toggle_pause_updates(Some("due previous errors"));
                    }

                    const CLICKHOUSE_ERROR_CODE_ALL_CONNECTION_TRIES_FAILED: u32 = 279;
                    let has_cluster = siv
                        .user_data::<ContextArc>()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .options
                        .clickhouse
                        .cluster
                        .as_ref()
                        .is_some_and(|v| !v.is_empty());
                    if has_cluster
                        && let Some(ClickHouseError::Server(server_error)) =
                            &err.downcast_ref::<ClickHouseError>()
                        && server_error.code == CLICKHOUSE_ERROR_CODE_ALL_CONNECTION_TRIES_FAILED
                    {
                        siv.add_layer(views::Dialog::info(format!(
                            "{}\n(consider adding skip_unavailable_shards=1 to the connection URL)",
                            err
                        )));
                        return;
                    }

                    siv.add_layer(views::Dialog::info(err.to_string()));
                }))
                // Ignore errors on exit
                .unwrap_or_default();
        }
        let elapsed_ms = stopwatch.elapsed_ms();
        let mut completion_status =
            format!("Processing {} took {} ms.", event.enum_key(), elapsed_ms);

        // It should not be reset, since delay_interval should be set to the maximum service
        // query duration time.
        if stopwatch.elapsed() > options.view.delay_interval {
            completion_status.push_str(" (consider increasing --delay_interval)");
        }

        update_status(&completion_status);

        cb_sink
            .send(Box::new(move |siv: &mut cursive::Cursive| {
                if need_clear {
                    siv.complete_clear();
                }
                siv.on_event(cursive::event::Event::Refresh);
            }))
            // Ignore errors on exit
            .unwrap_or_default();
    }

    log::info!("Event worker finished");
}

async fn render_or_share_flamegraph(
    tui: bool,
    cb_sink: cursive::CbSink,
    block: Columns,
    pastila_clickhouse_host: String,
    pastila_url: String,
) -> Result<()> {
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
        let url = flamegraph::share(block, &pastila_clickhouse_host, &pastila_url).await?;

        let url_clone = url.clone();
        cb_sink
            .send(Box::new(move |siv: &mut cursive::Cursive| {
                siv.add_layer(
                    views::Dialog::text(format!("Flamegraph shared (encrypted):\n\n{}", url))
                        .title("Share Complete")
                        .button("Close", |siv| {
                            siv.pop_layer();
                        }),
                );
            }))
            .map_err(|_| anyhow!("Cannot send message to UI"))?;

        crate::utils::open_url_command(&url_clone).status()?;
    }
    return Ok(());
}

async fn process_event(context: ContextArc, event: Event, need_clear: &mut bool) -> Result<()> {
    let cb_sink = context.lock().unwrap().cb_sink.clone();
    let clickhouse = context.lock().unwrap().clickhouse.clone();
    let pastila_clickhouse_host = context
        .lock()
        .unwrap()
        .options
        .service
        .pastila_clickhouse_host
        .clone();
    let pastila_url = context.lock().unwrap().options.service.pastila_url.clone();
    let selected_host = context.lock().unwrap().selected_host.clone();

    match event {
        Event::ProcessList(filter, limit) => {
            let block = clickhouse
                .get_processlist(filter, limit, selected_host.as_ref())
                .await?;
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.call_on_name_or_render_error(
                        "processes",
                        move |view: &mut views::OnEventView<view::QueriesView>| {
                            return view.get_inner_mut().update(block);
                        },
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::SlowQueryLog(filter, start, end, limit) => {
            let block = clickhouse
                .get_slow_query_log(&filter, start, end, limit, selected_host.as_ref())
                .await?;
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.call_on_name_or_render_error(
                        "slow_query_log",
                        move |view: &mut views::OnEventView<view::QueriesView>| {
                            return view.get_inner_mut().update(block);
                        },
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::LastQueryLog(filter, start, end, limit) => {
            let block = clickhouse
                .get_last_query_log(&filter, start, end, limit, selected_host.as_ref())
                .await?;
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.call_on_name_or_render_error(
                        "last_query_log",
                        move |view: &mut views::OnEventView<view::QueriesView>| {
                            return view.get_inner_mut().update(block);
                        },
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::TextLog(view_name, args) => {
            let block = clickhouse.get_query_logs(&args).await?;
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
        Event::ServerFlameGraph(tui, trace_type, start, end) => {
            let flamegraph_block = clickhouse
                .get_flamegraph(
                    trace_type,
                    None,
                    Some(start),
                    Some(end),
                    selected_host.as_ref(),
                )
                .await?;
            render_or_share_flamegraph(
                tui,
                cb_sink,
                flamegraph_block,
                pastila_clickhouse_host,
                pastila_url,
            )
            .await?;
            *need_clear = true;
        }
        Event::JemallocFlameGraph(tui) => {
            let flamegraph_block = clickhouse
                .get_jemalloc_flamegraph(selected_host.as_ref())
                .await?;
            render_or_share_flamegraph(
                tui,
                cb_sink,
                flamegraph_block,
                pastila_clickhouse_host,
                pastila_url,
            )
            .await?;
            *need_clear = true;
        }
        Event::QueryFlameGraph(trace_type, tui, start, end, query_ids) => {
            let flamegraph_block = clickhouse
                .get_flamegraph(
                    trace_type,
                    Some(&query_ids),
                    Some(start),
                    end,
                    selected_host.as_ref(),
                )
                .await?;
            render_or_share_flamegraph(
                tui,
                cb_sink,
                flamegraph_block,
                pastila_clickhouse_host,
                pastila_url,
            )
            .await?;
            *need_clear = true;
        }
        Event::LiveQueryFlameGraph(tui, query_ids) => {
            let flamegraph_block = clickhouse
                .get_live_query_flamegraph(&query_ids, selected_host.as_ref())
                .await?;
            render_or_share_flamegraph(
                tui,
                cb_sink,
                flamegraph_block,
                pastila_clickhouse_host,
                pastila_url,
            )
            .await?;
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
        Event::ExplainPipelineShareGraph(database, query) => {
            let pipeline = clickhouse
                .explain_pipeline_graph(database.as_str(), query.as_str())
                .await?
                .join("\n");

            // Upload graph to pastila and open in browser
            match share_graph(pipeline, &pastila_clickhouse_host, &pastila_url).await {
                Ok(_) => {}
                Err(err) => {
                    let error_msg = err.to_string();
                    cb_sink
                        .send(Box::new(move |siv: &mut cursive::Cursive| {
                            siv.add_layer(views::Dialog::info(error_msg));
                        }))
                        .map_err(|_| anyhow!("Cannot send message to UI"))?;
                }
            }
        }
        Event::ShowCreateTable(database, table) => {
            let create_statement = clickhouse
                .show_create_table(database.as_str(), table.as_str())
                .await?;
            let create_statement = highlight_sql(&create_statement)?;
            let title = format!("CREATE TABLE {}.{}", database, table);
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(
                        views::Dialog::around(views::TextView::new(create_statement).scrollable())
                            .title(title),
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::KillQuery(query_id) => {
            let ret = clickhouse.kill_query(query_id.as_str()).await;
            // NOTE: should we do this via cursive, to block the UI?
            let message;
            if let Err(err) = ret {
                message = err.to_string();
            } else {
                message = format!("Query {} killed", query_id);
            }
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(views::Dialog::info(message));
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::Summary => {
            let block = clickhouse.get_summary(selected_host.as_ref()).await;
            match block {
                Err(err) => {
                    let message = err.to_string();
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
        Event::SQLQuery(view_name, query) => {
            let block = clickhouse.execute(query.as_str()).await?;
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    log::trace!(
                        "Updating {} (with block of {} rows)",
                        view_name,
                        block.row_count()
                    );
                    // TODO: update specific view (can we accept type somehow in the enum?)
                    siv.call_on_name_or_render_error(
                        view_name,
                        move |view: &mut views::OnEventView<view::SQLQueryView>| {
                            return view.get_inner_mut().update(block);
                        },
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::BackgroundSchedulePoolLogs(log_name, database, table, start, end) => {
            let query_ids = clickhouse
                .get_background_schedule_pool_query_ids(
                    log_name.clone(),
                    database.clone(),
                    table.clone(),
                    start.clone(),
                    end.clone(),
                    selected_host.as_ref(),
                )
                .await?;

            if query_ids.is_empty() {
                let error_msg = if let Some(log_name) = log_name {
                    format!(
                        "No entries for {} jobs (database: {}, table: {}, start: {}, end: {})",
                        log_name, database, table, start, end
                    )
                } else {
                    format!(
                        "No entries for {}.{} (start: {}, end: {})",
                        database, table, start, end
                    )
                };
                return Err(anyhow!(error_msg));
            }

            let title = if let Some(ref log_name) = log_name {
                format!("Logs for task: {}", log_name)
            } else {
                format!("Logs for tasks of {}.{}", database, table)
            };

            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    use cursive::view::Resizable;
                    let context = siv.user_data::<ContextArc>().unwrap().clone();
                    siv.add_layer(views::Dialog::around(
                        views::LinearLayout::vertical()
                            .child(views::TextView::new(title).center())
                            .child(views::DummyView.fixed_height(1))
                            .child(views::NamedView::new(
                                "background_schedule_pool_logs",
                                view::TextLogView::new(
                                    "background_schedule_pool_logs",
                                    context,
                                    TextLogArguments {
                                        query_ids: Some(query_ids),
                                        logger_names: None,
                                        hostname: None,
                                        message_filter: None,
                                        max_level: None,
                                        start: start.into(),
                                        end,
                                    },
                                ),
                            )),
                    ));
                    siv.focus_name("background_schedule_pool_logs").unwrap();
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::TableParts(database, table) => {
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    let context = siv.user_data::<ContextArc>().unwrap().clone();
                    crate::view::providers::table_parts::show_table_parts_dialog(
                        siv,
                        context,
                        Some(database),
                        Some(table),
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::AsynchronousInserts(database, table) => {
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    let context = siv.user_data::<ContextArc>().unwrap().clone();
                    crate::view::providers::asynchronous_inserts::show_asynchronous_inserts_dialog(
                        siv,
                        context,
                        Some(database),
                        Some(table),
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::ShareLogs(content) => {
            let url =
                pastila::upload_encrypted(&content, &pastila_clickhouse_host, &pastila_url).await?;

            let url_clone = url.clone();
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.pop_layer();
                    siv.add_layer(
                        views::Dialog::text(format!("Logs shared (encrypted):\n\n{}", url))
                            .title("Share Complete")
                            .button("Close", |siv| {
                                siv.pop_layer();
                            }),
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;

            crate::utils::open_url_command(&url_clone).status()?;
        }
    }

    return Ok(());
}
