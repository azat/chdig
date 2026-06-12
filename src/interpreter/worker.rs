use crate::{
    common::{RelativeDateTime, Stopwatch},
    interpreter::{
        ContextArc, Query,
        clickhouse::{
            ClickHouse, TextLogArguments, TraceType, parse_metric_log_block,
            parse_query_metric_log_block,
        },
        flamegraph,
        perfetto::PerfettoTraceBuilder,
    },
    pastila,
    utils::{highlight_sql, share_graph},
    view::{self, Navigation},
};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Local};
// FIXME: "leaky abstractions"
use clickhouse_rs::Block;
use clickhouse_rs::errors::Error as ClickHouseError;
use cursive::traits::*;
use cursive::views;
use futures::channel::{mpsc, oneshot};
use futures::{SinkExt, StreamExt};
use std::collections::{HashMap, hash_map::Entry};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

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
    // (type, start time, end time, [query_ids_a = before], [query_ids_b = after]).
    // Diff mode is TUI-only (color-coded via flamelens), no share path.
    QueryFlameGraphDiff(
        TraceType,
        DateTime<Local>,
        Option<DateTime<Local>>,
        Vec<String>,
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
    // (title, query returning (bucket UInt32, value Float64), number of buckets, time range label)
    ShowChart(String, String, u32, String),
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
    // (queries, query_ids, start, end)
    PerfettoExport(
        Vec<Query>,
        Vec<String>,
        DateTime<Local>,
        Option<DateTime<Local>>,
    ),
    // (start, end)
    ServerPerfettoExport(DateTime<Local>, DateTime<Local>),
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
            Event::QueryFlameGraphDiff(..) => "QueryFlameGraphDiff".to_string(),
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
            Event::ShowChart(title, ..) => format!("ShowChart({})", title),
            Event::BackgroundSchedulePoolLogs(..) => "BackgroundSchedulePoolLogs".to_string(),
            Event::TableParts(..) => "TableParts".to_string(),
            Event::AsynchronousInserts(..) => "AsynchronousInserts".to_string(),
            Event::ShareLogs(..) => "ShareLogs".to_string(),
            Event::PerfettoExport(..) => "PerfettoExport".to_string(),
            Event::ServerPerfettoExport(..) => "ServerPerfettoExport".to_string(),
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
    #[allow(clippy::new_without_default)]
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

        // Simply ignore errors (queue is full, likely update interval is too short).
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

        let debug_metrics = context.lock().unwrap().debug_metrics.clone();
        // RAII: decrements on scope exit, including panic or early return paths.
        let _in_flight = debug_metrics.track_in_flight();
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
        let elapsed = stopwatch.elapsed();
        debug_metrics.record_event(elapsed);
        let mut completion_status = format!(
            "Processing {} took {} ms.",
            event.enum_key(),
            elapsed.as_millis()
        );

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
    title: &'static str,
    data: String,
    pastila_clickhouse_host: String,
    pastila_url: String,
) -> Result<()> {
    if tui {
        cb_sink
            .send(Box::new(move |siv: &mut cursive::Cursive| {
                flamegraph::show(title, data)
                    .or_else(|e| {
                        siv.add_layer(views::Dialog::info(e.to_string()));
                        return anyhow::Ok(());
                    })
                    .unwrap();
            }))
            .map_err(|_| anyhow!("Cannot send message to UI"))?;
    } else {
        let url = flamegraph::share(data, &pastila_clickhouse_host, &pastila_url).await?;

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

use crate::interpreter::options::ChDigPerfettoConfig;

type ApplyBlock = Box<dyn FnOnce(&mut PerfettoTraceBuilder) + Send>;

// ClickHouse error codes (src/Common/ErrorCodes.cpp)
const UNKNOWN_TABLE: u32 = 60;
const CANNOT_EXTRACT_TABLE_STRUCTURE: u32 = 636;

// Runs one source's streaming fetch; fetch errors only skip this source
// (same tolerance as the old fetch_all-based code).
async fn stream_perfetto_source(name: &'static str, fetch: impl Future<Output = Result<()>>) {
    if let Err(e) = fetch.await {
        if let Some(ClickHouseError::Server(se)) = e.downcast_ref::<ClickHouseError>()
            && (se.code == UNKNOWN_TABLE || se.code == CANNOT_EXTRACT_TABLE_STRUCTURE)
        {
            log::debug!("Skipping {}: {}", name, e);
            return;
        }
        log::warn!("Failed to fetch {}: {}", name, e);
    }
}

// Per-block callback for the perfetto streaming fetches: defers applying the
// block to the builder by sending it into the bounded channel, serializing
// concurrent sources into the single consumer.
fn apply_via(
    mut tx: mpsc::Sender<ApplyBlock>,
    apply: impl Fn(&mut PerfettoTraceBuilder, Block) + Clone + Send + 'static,
) -> impl AsyncFnMut(Block) -> bool {
    async move |block| {
        let apply = apply.clone();
        tx.send(Box::new(move |builder| apply(builder, block)))
            .await
            .is_ok()
    }
}

// Streams query_log queries into the builder block-by-block (the full window
// of a server-wide export is hundreds of thousands of rows).
pub(crate) async fn stream_queries_into_perfetto_trace(
    clickhouse: &Arc<ClickHouse>,
    builder: &mut PerfettoTraceBuilder,
    query_ids: &Option<Vec<String>>,
    start: DateTime<Local>,
    end_time: DateTime<Local>,
) {
    let (tx, mut rx) = mpsc::channel::<ApplyBlock>(1);
    tokio::join!(
        stream_perfetto_source(
            "query_log queries",
            clickhouse.queries_for_perfetto(
                start,
                end_time,
                query_ids,
                apply_via(tx, |b, blk| {
                    let mut queries = Vec::with_capacity(blk.row_count());
                    for i in 0..blk.row_count() {
                        match Query::from_clickhouse_block(&blk, i, false) {
                            Ok(q) => queries.push(q),
                            Err(e) => {
                                log::warn!("Perfetto: failed to parse query row {}: {}", i, e)
                            }
                        }
                    }
                    b.add_queries(&queries);
                }),
            ),
        ),
        async {
            while let Some(apply) = rx.next().await {
                apply(builder);
            }
        },
    );
}

// Sources are fetched in parallel, but their blocks are applied to the
// builder by a single consumer through a bounded channel, so the peak memory
// is a few blocks instead of every result set at once (#242).
pub(crate) async fn fetch_and_populate_perfetto_trace(
    clickhouse: &Arc<ClickHouse>,
    builder: &mut PerfettoTraceBuilder,
    cfg: &ChDigPerfettoConfig,
    query_ids: Option<&[String]>,
    start: DateTime<Local>,
    end_time: DateTime<Local>,
) {
    let (tx, mut rx) = mpsc::channel::<ApplyBlock>(1);
    let tx_otel = tx.clone();
    let tx_counters = tx.clone();
    let tx_metrics = tx.clone();
    let tx_parts = tx.clone();
    let tx_threads = tx.clone();
    let tx_stacks = tx.clone();
    let tx_text = tx.clone();
    // The consumer finishes once every producer dropped its sender
    drop(tx);

    tokio::join!(
        async move {
            if cfg.opentelemetry_span_log {
                stream_perfetto_source(
                    "opentelemetry_span_log",
                    clickhouse.otel_spans_for_perfetto(
                        query_ids,
                        start,
                        end_time,
                        apply_via(tx_otel, |b, blk| b.add_otel_spans(&blk)),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.trace_log {
                stream_perfetto_source(
                    "trace_log counters",
                    clickhouse.trace_log_counters_for_perfetto(
                        query_ids,
                        start,
                        end_time,
                        apply_via(tx_counters, |b, blk| b.add_trace_log_counters(&blk)),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.query_metric_log {
                stream_perfetto_source(
                    "query_metric_log",
                    clickhouse.query_metric_log_for_perfetto(
                        query_ids,
                        start,
                        end_time,
                        apply_via(tx_metrics, |b, blk| {
                            b.add_query_metrics(&parse_query_metric_log_block(&blk))
                        }),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.part_log {
                stream_perfetto_source(
                    "part_log",
                    clickhouse.part_log_for_perfetto(
                        query_ids,
                        start,
                        end_time,
                        apply_via(tx_parts, |b, blk| b.add_part_log(&blk)),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.query_thread_log {
                stream_perfetto_source(
                    "query_thread_log",
                    clickhouse.query_thread_log_for_perfetto(
                        query_ids,
                        start,
                        end_time,
                        apply_via(tx_threads, |b, blk| b.add_query_thread_log(&blk)),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.trace_log {
                // Frames must be interned before samples reference them
                stream_perfetto_source(
                    "trace_log stack traces",
                    clickhouse.stack_traces_for_perfetto(
                        query_ids,
                        start,
                        end_time,
                        apply_via(tx_stacks.clone(), |b, blk| b.add_stack_frames(&blk)),
                    ),
                )
                .await;
                stream_perfetto_source(
                    "trace_log stack samples",
                    clickhouse.stack_trace_samples_for_perfetto(
                        query_ids,
                        start,
                        end_time,
                        apply_via(tx_stacks, |b, blk| b.add_stack_samples(&blk)),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.text_log {
                stream_perfetto_source(
                    "text_log",
                    clickhouse.text_log_for_perfetto(
                        query_ids,
                        start,
                        end_time,
                        apply_via(tx_text, |b, blk| b.add_text_logs(&blk)),
                    ),
                )
                .await;
            }
        },
        async {
            while let Some(apply) = rx.next().await {
                apply(builder);
            }
        },
    );
}

pub(crate) async fn fetch_server_perfetto_sources(
    clickhouse: &Arc<ClickHouse>,
    builder: &mut PerfettoTraceBuilder,
    cfg: &ChDigPerfettoConfig,
    start: DateTime<Local>,
    end_time: DateTime<Local>,
) {
    let (tx, mut rx) = mpsc::channel::<ApplyBlock>(1);
    let tx_metric = tx.clone();
    let tx_async_metric = tx.clone();
    let tx_async_insert = tx.clone();
    let tx_error = tx.clone();
    let tx_s3_queue = tx.clone();
    let tx_azure_queue = tx.clone();
    let tx_blob_storage = tx.clone();
    let tx_bg_pool = tx.clone();
    let tx_session = tx.clone();
    let tx_zk = tx.clone();
    drop(tx);

    tokio::join!(
        async move {
            if cfg.metric_log {
                stream_perfetto_source(
                    "metric_log",
                    clickhouse.metric_log_for_perfetto(
                        start,
                        end_time,
                        apply_via(tx_metric, |b, blk| {
                            b.add_metric_log(&parse_metric_log_block(&blk))
                        }),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.asynchronous_metric_log {
                stream_perfetto_source(
                    "asynchronous_metric_log",
                    clickhouse.asynchronous_metric_log_for_perfetto(
                        start,
                        end_time,
                        apply_via(tx_async_metric, |b, blk| {
                            b.add_asynchronous_metric_log(&blk)
                        }),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.asynchronous_insert_log {
                stream_perfetto_source(
                    "asynchronous_insert_log",
                    clickhouse.asynchronous_insert_log_for_perfetto(
                        start,
                        end_time,
                        apply_via(tx_async_insert, |b, blk| {
                            b.add_asynchronous_insert_log(&blk)
                        }),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.error_log {
                stream_perfetto_source(
                    "error_log",
                    clickhouse.error_log_for_perfetto(
                        start,
                        end_time,
                        apply_via(tx_error, |b, blk| b.add_error_log(&blk)),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.s3_queue_log {
                stream_perfetto_source(
                    "s3queue_log",
                    clickhouse.s3_queue_log_for_perfetto(
                        start,
                        end_time,
                        apply_via(tx_s3_queue, |b, blk| b.add_s3_queue_log(&blk)),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.azure_queue_log {
                stream_perfetto_source(
                    "azure_queue_log",
                    clickhouse.azure_queue_log_for_perfetto(
                        start,
                        end_time,
                        apply_via(tx_azure_queue, |b, blk| b.add_azure_queue_log(&blk)),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.blob_storage_log {
                stream_perfetto_source(
                    "blob_storage_log",
                    clickhouse.blob_storage_log_for_perfetto(
                        start,
                        end_time,
                        apply_via(tx_blob_storage, |b, blk| b.add_blob_storage_log(&blk)),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.background_schedule_pool_log {
                stream_perfetto_source(
                    "background_schedule_pool_log",
                    clickhouse.background_schedule_pool_log_for_perfetto(
                        start,
                        end_time,
                        apply_via(tx_bg_pool, |b, blk| b.add_background_pool_log(&blk)),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.session_log {
                stream_perfetto_source(
                    "session_log",
                    clickhouse.session_log_for_perfetto(
                        start,
                        end_time,
                        apply_via(tx_session, |b, blk| b.add_session_log(&blk)),
                    ),
                )
                .await;
            }
        },
        async move {
            if cfg.aggregated_zookeeper_log {
                stream_perfetto_source(
                    "aggregated_zookeeper_log",
                    clickhouse.aggregated_zookeeper_log_for_perfetto(
                        start,
                        end_time,
                        apply_via(tx_zk, |b, blk| b.add_aggregated_zookeeper_log(&blk)),
                    ),
                )
                .await;
            }
        },
        async {
            while let Some(apply) = rx.next().await {
                apply(builder);
            }
        },
    );
}

fn serve_perfetto_trace(
    context: ContextArc,
    cb_sink: cursive::CbSink,
    builder: PerfettoTraceBuilder,
) -> Result<()> {
    let trace_file = builder.build()?;
    let data_len = trace_file.size();
    log::info!("Saved trace ({} bytes)", data_len);

    let server = context.lock().unwrap().get_or_start_perfetto_server();
    server.set_trace_file(trace_file);
    let url = server.get_perfetto_url();

    let url_clone = url.clone();
    cb_sink
        .send(Box::new(move |siv: &mut cursive::Cursive| {
            siv.add_layer(
                views::Dialog::text(format!(
                    "Perfetto trace exported ({} bytes)\n\nOpening: {}",
                    data_len, url
                ))
                .title("Perfetto Export")
                .button("Close", |siv| {
                    siv.pop_layer();
                }),
            );
        }))
        .map_err(|_| anyhow!("Cannot send message to UI"))?;

    crate::utils::open_url_command(&url_clone).status()?;
    Ok(())
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
            let mut new_batch = true;
            clickhouse
                .get_query_logs(&args, async |block| {
                    let is_new_batch = std::mem::take(&mut new_batch);
                    let (ack_tx, ack_rx) = oneshot::channel::<bool>();
                    let sent = cb_sink.send(Box::new(move |siv: &mut cursive::Cursive| {
                        let ret = siv
                            .call_on_name(view_name, move |view: &mut view::TextLogView| {
                                view.update(block, is_new_batch)
                            });
                        let ok = match ret {
                            Some(Ok(())) => true,
                            Some(Err(err)) => {
                                siv.add_layer(views::Dialog::info(err.to_string()));
                                false
                            }
                            // The view is gone, stop the fetch
                            None => false,
                        };
                        ack_tx.send(ok).ok();
                    }));
                    // cb_sink is unbounded: wait for the UI to consume the block
                    // before pulling the next one, otherwise the whole result
                    // could pile up in the UI channel anyway.
                    sent.is_ok() && ack_rx.await.unwrap_or(false)
                })
                .await?;
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
                "Server",
                flamegraph::block_to_folded(&flamegraph_block),
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
                "jemalloc",
                flamegraph::block_to_folded(&flamegraph_block),
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
                "Query",
                flamegraph::block_to_folded(&flamegraph_block),
                pastila_clickhouse_host,
                pastila_url,
            )
            .await?;
            *need_clear = true;
        }
        Event::QueryFlameGraphDiff(trace_type, start, end, query_ids_a, query_ids_b) => {
            let (block_a, block_b) = tokio::try_join!(
                clickhouse.get_flamegraph(
                    trace_type.clone(),
                    Some(&query_ids_a),
                    Some(start),
                    end,
                    selected_host.as_ref(),
                ),
                clickhouse.get_flamegraph(
                    trace_type,
                    Some(&query_ids_b),
                    Some(start),
                    end,
                    selected_host.as_ref(),
                ),
            )?;
            let before = flamegraph::block_to_folded(&block_a);
            let after = flamegraph::block_to_folded(&block_b);
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    flamegraph::show_diff("Query diff", before, after)
                        .or_else(|e| {
                            siv.add_layer(views::Dialog::info(e.to_string()));
                            return anyhow::Ok(());
                        })
                        .unwrap();
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
            *need_clear = true;
        }
        Event::LiveQueryFlameGraph(tui, query_ids) => {
            let flamegraph_block = clickhouse
                .get_live_query_flamegraph(&query_ids, selected_host.as_ref())
                .await?;
            render_or_share_flamegraph(
                tui,
                cb_sink,
                "Query (live)",
                flamegraph::block_to_folded(&flamegraph_block),
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
            let start = Instant::now();
            let ret = clickhouse.kill_query(query_id.as_str()).await;
            let elapsed = start.elapsed();
            // NOTE: should we do this via cursive, to block the UI?
            let message;
            if let Err(err) = ret {
                message = format!("{} (elapsed: {:?})", err, elapsed);
            } else {
                message = format!("Query {} killed (elapsed: {:?})", query_id, elapsed);
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
        Event::ShowChart(title, query, buckets, range_label) => {
            let block = clickhouse.execute(query.as_str()).await?;
            let mut values = vec![0.0_f64; buckets as usize];
            for i in 0..block.row_count() {
                let bucket: u32 = block.get(i, "bucket")?;
                let value: f64 = block.get(i, "value")?;
                if let Some(v) = values.get_mut(bucket as usize) {
                    *v = value;
                }
            }
            let chart = crate::common::render_column_chart(&values, 16);
            cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(
                        views::Dialog::around(
                            views::LinearLayout::vertical()
                                .child(views::TextView::new(title).center())
                                .child(views::DummyView.fixed_height(1))
                                .child(views::TextView::new(chart))
                                .child(views::TextView::new(range_label).center()),
                        )
                        .scrollable(),
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
        Event::PerfettoExport(queries, query_ids, start, end) => {
            let perfetto_cfg = context.lock().unwrap().options.perfetto.clone();
            let end_time = end.unwrap_or_else(Local::now) + chrono::TimeDelta::seconds(1);
            let mut builder = PerfettoTraceBuilder::new_temp(
                perfetto_cfg.per_server,
                perfetto_cfg.text_log_android,
            )?;

            for q in &queries {
                log::info!(
                    "Perfetto query: id={} start_ns={} end_ns={} elapsed={}",
                    q.query_id,
                    q.query_start_time_microseconds
                        .timestamp_nanos_opt()
                        .unwrap_or(0),
                    q.query_end_time_microseconds
                        .timestamp_nanos_opt()
                        .unwrap_or(0),
                    q.elapsed,
                );
            }
            builder.add_queries(&queries);
            fetch_and_populate_perfetto_trace(
                &clickhouse,
                &mut builder,
                &perfetto_cfg,
                Some(&query_ids),
                start,
                end_time,
            )
            .await;
            serve_perfetto_trace(context.clone(), cb_sink, builder)?;
        }
        Event::ServerPerfettoExport(start, end) => {
            let perfetto_cfg = context.lock().unwrap().options.perfetto.clone();
            let end_time = end + chrono::TimeDelta::seconds(1);
            let mut builder = PerfettoTraceBuilder::new_temp(
                perfetto_cfg.per_server,
                perfetto_cfg.text_log_android,
            )?;
            stream_queries_into_perfetto_trace(&clickhouse, &mut builder, &None, start, end_time)
                .await;
            fetch_and_populate_perfetto_trace(
                &clickhouse,
                &mut builder,
                &perfetto_cfg,
                None,
                start,
                end_time,
            )
            .await;
            fetch_server_perfetto_sources(
                &clickhouse,
                &mut builder,
                &perfetto_cfg,
                start,
                end_time,
            )
            .await;
            serve_perfetto_trace(context.clone(), cb_sink, builder)?;
        }
    }

    return Ok(());
}
