use crate::{
    common::{RelativeDateTime, Stopwatch},
    interpreter::{
        ContextArc, Query,
        clickhouse::{
            ClickHouse, Columns, TextLogArguments, TraceType, parse_metric_log_block,
            parse_query_metric_log_block,
        },
        flamegraph,
        perfetto::PerfettoTraceBuilder,
    },
    pastila,
    tui::highlight_sql,
    utils::share_graph,
};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Local};
// FIXME: "leaky abstractions"
use clickhouse_rs::Block;
use clickhouse_rs::errors::Error as ClickHouseError;
use clickhouse_rs::types::Progress;

use crate::tui::views::queries_view::QueriesView;
use crate::tui::views::sql_query_view::SQLQueryView;
use crate::tui::views::summary_view::SummaryView;
use crate::tui::views::text_log_view::TextLogView;
use crate::tui::{
    App, Dialog, DummyView, Event as UiEvent, LinearLayout, NamedView, Navigation, OnEventView,
    Resizable, Scrollable, TextView, UiSink,
};
use futures::channel::{mpsc, oneshot};
use futures::future::{AbortHandle, Abortable, Aborted, LocalBoxFuture};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, SinkExt, StreamExt};
use size::{Base, SizeFormatter, Style};
use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// Events are traced with {:?} on every send/receive, so payloads that can be
// megabytes large must not leak their content into the Debug output.
#[derive(Clone)]
pub struct OpaquePayload<T>(pub T);

impl<T> From<T> for OpaquePayload<T> {
    fn from(content: T) -> Self {
        Self(content)
    }
}

impl std::fmt::Debug for OpaquePayload<String> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{} bytes>", self.0.len())
    }
}

impl std::fmt::Debug for OpaquePayload<Vec<Query>> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{} queries>", self.0.len())
    }
}

/// Feed of a live flamelens app (flamelens swaps the deposited graph in on
/// tick()), written by the worker directly, not via UiSink: the fullscreen
/// flamelens loop blocks the UI thread and never runs UiSink callbacks.
pub type FlamegraphSlot = Arc<Mutex<Option<flamelens::app::ParsedFlameGraph>>>;

impl std::fmt::Debug for OpaquePayload<FlamegraphSlot> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<flamegraph slot>")
    }
}

// How to (re-)fetch a flamegraph, for the periodic live updates
#[derive(Debug, Clone)]
pub enum FlamegraphSource {
    TraceLog {
        trace_type: TraceType,
        query_ids: Option<Vec<String>>,
        start: RelativeDateTime,
        end: RelativeDateTime,
    },
    StackTrace(Option<Vec<String>>),
    Jemalloc,
}

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
    ServerFlameGraph(bool, TraceType, RelativeDateTime, RelativeDateTime),
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
    // Periodic refresh of a live flamegraph: deposits the new graph into the
    // flamelens app's slot instead of opening a new view
    UpdateFlameGraph(FlamegraphSource, OpaquePayload<FlamegraphSlot>),
    Summary,
    // query_id
    KillQuery(String),
    // (database, query)
    ExecuteQuery(String, String),
    // (database, query, settings)
    ExplainSyntax(String, String, Arc<HashMap<String, String>>),
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
    // (view_name, query); the name is Arc<str> since dialog views get
    // per-filter generated names (not 'static)
    SQLQuery(Arc<str>, String),
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
    ShareLogs(OpaquePayload<String>),
    // (queries, query_ids, start, end)
    PerfettoExport(
        OpaquePayload<Vec<Query>>,
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
            // Per-view key: log views in several panes update concurrently,
            // one shared capacity-1 channel would drop their updates.
            Event::TextLog(view_name, ..) => format!("TextLog({})", view_name),
            Event::ServerFlameGraph(..) => "ServerFlameGraph".to_string(),
            Event::JemallocFlameGraph(..) => "JemallocFlameGraph".to_string(),
            Event::QueryFlameGraph(..) => "QueryFlameGraph".to_string(),
            Event::QueryFlameGraphDiff(..) => "QueryFlameGraphDiff".to_string(),
            Event::LiveQueryFlameGraph(..) => "LiveQueryFlameGraph".to_string(),
            Event::UpdateFlameGraph(..) => "UpdateFlameGraph".to_string(),
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

// A handle tying events to the view that requested them: the view and its
// update callback share one EventOwner via Arc, so once the view is dropped
// (replaced with another or recreated with new options) the last clone dies
// and cancels everything sent on its behalf - queued events are skipped and
// the in-flight ones are aborted. A forced send does the same for the owner's
// earlier events (see EventCanceller::supersede). Aborting drops the query
// future mid-stream, which marks the connection inconsistent, and the driver
// sends Cancel to the server on the next reuse of that connection (see
// BlockStream::drop and ClickhouseTransport::clear in clickhouse-rs).
pub struct EventOwner {
    id: u64,
    canceller: Arc<EventCanceller>,
}

impl Drop for EventOwner {
    fn drop(&mut self) {
        self.canceller.cancel(self.id);
    }
}

#[derive(Default)]
pub struct EventCanceller {
    inner: Mutex<EventCancellerInner>,
}

#[derive(Default)]
struct EventCancellerInner {
    next_owner_id: u64,
    next_token: u64,
    dead_owners: HashSet<u64>,
    // Bumped by supersede(); events tagged with an older epoch are stale and
    // must be skipped.
    epochs: HashMap<u64, u64>,
    // Events run concurrently, so an owner can have several in flight at
    // once: token -> (owner, handle).
    in_flight: HashMap<u64, (u64, AbortHandle)>,
}

impl EventCanceller {
    fn new_owner(self: &Arc<Self>) -> Arc<EventOwner> {
        let mut inner = self.inner.lock().unwrap();
        inner.next_owner_id += 1;
        return Arc::new(EventOwner {
            id: inner.next_owner_id,
            canceller: self.clone(),
        });
    }

    fn cancel(&self, owner: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.dead_owners.insert(owner);
        inner.epochs.remove(&owner);
        for (id, handle) in inner.in_flight.values() {
            if *id == owner {
                handle.abort();
            }
        }
    }

    // A forced send means the user asked for fresh data right now (explicit
    // refresh or changed parameters, e.g. the queries filter), so everything
    // sent earlier on this owner's behalf is stale: bump the epoch (queued
    // events tagged with an older one will be skipped in begin()) and abort
    // the in-flight one.
    fn supersede(&self, owner: u64) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        let epoch = inner.epochs.entry(owner).or_insert(0);
        *epoch += 1;
        let epoch = *epoch;
        for (id, handle) in inner.in_flight.values() {
            if *id == owner {
                handle.abort();
            }
        }
        return epoch;
    }

    fn epoch(&self, owner: u64) -> u64 {
        return self
            .inner
            .lock()
            .unwrap()
            .epochs
            .get(&owner)
            .copied()
            .unwrap_or(0);
    }

    // Registers the dequeued event as in-flight and returns a token to pass
    // to finish(); None means the owner is already dead (or the event was
    // superseded by a forced send) and the event must be skipped. Checking
    // and registering under one lock, so that cancel()/supersede() cannot
    // slip in between.
    fn begin(&self, owner: Option<(u64, u64)>, handle: AbortHandle) -> Option<u64> {
        let mut inner = self.inner.lock().unwrap();
        if let Some((id, epoch)) = owner {
            if inner.dead_owners.contains(&id) {
                return None;
            }
            if epoch < inner.epochs.get(&id).copied().unwrap_or(0) {
                return None;
            }
            inner.next_token += 1;
            let token = inner.next_token;
            inner.in_flight.insert(token, (id, handle));
            return Some(token);
        }
        // Ownerless events cannot be cancelled, nothing to track (real
        // tokens start from 1).
        return Some(0);
    }

    fn finish(&self, token: u64) {
        self.inner.lock().unwrap().in_flight.remove(&token);
    }
}

// (owner id, owner epoch at send time)
type SentOwner = (u64, u64);
type SentEvent = (Option<SentOwner>, Event);
type Receiver = mpsc::Receiver<SentEvent>;
type Sender = mpsc::Sender<SentEvent>;

pub struct Worker {
    sender: Sender,
    sender_by_event: HashMap<String, Sender>,
    receiver: Option<Receiver>,
    thread: Option<thread::JoinHandle<()>>,
    paused: bool,
    canceller: Arc<EventCanceller>,
}

// TODO: can we simplify things with callbacks? (EnumValue(Type))
impl Worker {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        // Here the futures::channel::mpsc::channel is used over standard std::sync::mpsc::channel,
        // since standard does not allow to configure backlog (queue max size), while we uses
        // channel per distinct event (as per-type backpressure - at most one queued message per
        // event type; not-running-the-same-query-concurrently is enforced in start_tokio()),
        // i.e. separate channel for Summary, separate for UpdateProcessList and so on.
        //
        // Note, by default channel reserves slot for each sender [1].
        //
        //   [1]: https://github.com/rust-lang/futures-rs/issues/403
        let (sender, receiver) = mpsc::channel::<SentEvent>(1);

        return Worker {
            sender,
            sender_by_event: HashMap::new(),
            receiver: Some(receiver),
            thread: None,
            paused: false,
            canceller: Arc::new(EventCanceller::default()),
        };
    }

    pub fn event_owner(&self) -> Arc<EventOwner> {
        return self.canceller.new_owner();
    }

    pub fn start(&mut self, context: ContextArc) {
        let receiver = self.receiver.take().expect("Worker already started");
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
        self.send_impl(None, force, event);
    }

    // Like send(), but ties the event to the view that requested it, so that
    // dropping the view cancels the event (see EventOwner).
    pub fn send_owned(&mut self, owner: &EventOwner, force: bool, event: Event) {
        self.send_impl(Some(owner.id), force, event);
    }

    fn send_impl(&mut self, owner: Option<u64>, force: bool, event: Event) {
        if !force && self.paused {
            return;
        }

        // A forced owned send supersedes the owner's earlier events: the user
        // asked for fresh data (or changed the parameters), so the stale
        // in-flight query is aborted and queued ones are skipped. Interval
        // updates (!force) must not supersede, otherwise a query slower than
        // the update interval would be restarted forever.
        let owner = owner.map(|id| {
            let epoch = if force {
                self.canceller.supersede(id)
            } else {
                self.canceller.epoch(id)
            };
            (id, epoch)
        });

        let entry = self.sender_by_event.entry(event.enum_key());
        let channel_created = matches!(&entry, Entry::Vacant(_));
        let sender = entry.or_insert(self.sender.clone());

        log::trace!(
            "Sending event: {:?} (channel created: {})",
            &event,
            channel_created
        );

        // Simply ignore errors (queue is full, likely update interval is too short).
        sender.try_send((owner, event.clone())).unwrap_or_else(|e| {
            log::error!(
                "Cannot send event {:?}: {} (too low --delay-interval?)",
                event,
                e
            )
        });
    }
}

#[tokio::main(flavor = "current_thread")]
async fn start_tokio(context: ContextArc, mut receiver: Receiver) {
    log::info!("Event worker started");

    // Names of the events running right now, to label the progress line (the
    // progress callback is global, so progress of concurrent queries cannot
    // be attributed to a specific one anyway) and to avoid running the same
    // event concurrently with itself.
    let running = Arc::new(Mutex::new(Vec::<String>::new()));
    {
        let (clickhouse, cb_sink) = {
            let context = context.lock().unwrap();
            (context.clickhouse.clone(), context.ui_sink.clone())
        };
        let running = running.clone();
        // The server sends Progress about every interactive_delay (100ms);
        // repainting the statusbar that often is pure overhead.
        let last_render = Mutex::new(None::<Instant>);
        clickhouse.set_progress_callback(Arc::new(move |progress| {
            {
                let mut last_render = last_render.lock().unwrap();
                if last_render.is_some_and(|at| at.elapsed() < Duration::from_millis(150)) {
                    return;
                }
                *last_render = Some(Instant::now());
            }
            let content = format!(
                "Processing {}... {}",
                running.lock().unwrap().join(", "),
                format_progress(progress)
            );
            cb_sink
                .send(Box::new(move |app: &mut App| {
                    app.set_statusbar_content(content);
                }))
                // Ignore errors on exit
                .unwrap_or_default();
        }));
    }

    let canceller = context.lock().unwrap().worker.canceller.clone();
    let cb_sink = context.lock().unwrap().ui_sink.clone();

    // Events are processed concurrently, so that a slow query in one view
    // does not stall the others, except that an event never runs
    // concurrently with itself (running the same view's query twice at once
    // makes no sense) - the latest same-key event waits in `deferred` (newest
    // wins, like the capacity-1 channel slot).
    let mut tasks = FuturesUnordered::<LocalBoxFuture<'static, String>>::new();
    let mut deferred = HashMap::<String, SentEvent>::new();

    loop {
        let (owner, event) = tokio::select! {
            Some(finished_key) = tasks.next(), if !tasks.is_empty() => {
                running.lock().unwrap().retain(|key| *key != finished_key);
                match deferred.remove(&finished_key) {
                    Some(sent_event) => sent_event,
                    None => continue,
                }
            }
            sent_event = receiver.next() => match sent_event {
                Some(sent_event) => sent_event,
                // Channel closed.
                None => break,
            }
        };
        log::trace!("Got event: {:?}", event);

        let key = event.enum_key();
        {
            let mut running = running.lock().unwrap();
            if running.contains(&key) {
                deferred.insert(key, (owner, event));
                continue;
            }
            running.push(key);
            // Render from the shared list (like the progress callback does),
            // so that concurrent events do not wipe each other off the
            // statusbar.
            update_statusbar(&cb_sink, &format!("Processing {}...", running.join(", ")));
        }
        tasks.push(Box::pin(run_event(
            context.clone(),
            canceller.clone(),
            owner,
            event,
        )));
    }

    log::info!("Event worker finished");
}

// Processes one event end-to-end (query, UI update, error reporting) and
// returns its enum_key() so that the caller can unblock same-key events.
async fn run_event(
    context: ContextArc,
    canceller: Arc<EventCanceller>,
    owner: Option<SentOwner>,
    event: Event,
) -> String {
    let key = event.enum_key();

    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    let Some(token) = canceller.begin(owner, abort_handle) else {
        log::debug!("Skipping event {:?} (view is gone or superseded)", event);
        return key;
    };

    let mut need_clear = false;
    let cb_sink = context.lock().unwrap().ui_sink.clone();
    let options = context.lock().unwrap().options.clone();

    let update_status = |message: &str| update_statusbar(&cb_sink, message);

    let debug_metrics = context.lock().unwrap().debug_metrics.clone();
    // RAII: decrements on scope exit, including panic or early return paths.
    let _in_flight = debug_metrics.track_in_flight();
    let stopwatch = Stopwatch::start_new();
    // catch_unwind: a panic (e.g. an unexpected column type from some server
    // version) would otherwise unwind the worker thread and the TUI would
    // keep running without ever updating again; degrade to an error dialog.
    let result = Abortable::new(
        std::panic::AssertUnwindSafe(process_event(
            context.clone(),
            event.clone(),
            &mut need_clear,
        ))
        .catch_unwind(),
        abort_registration,
    )
    .await
    .map(|result| {
        result.unwrap_or_else(|panic| {
            let message = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            Err(anyhow!(
                "Internal error while processing {}: {}",
                key,
                message
            ))
        })
    });
    canceller.finish(token);
    if let Err(Aborted) = result {
        log::debug!("Cancelled event {:?} (view is gone or superseded)", event);
        debug_metrics.record_event(stopwatch.elapsed());
        update_status(&format!("Cancelled {}", key));
        return key;
    }
    if let Ok(Err(err)) = result {
        cb_sink
            .send(Box::new(move |app: &mut App| {
                let is_paused = app
                    .user_data::<ContextArc>()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .worker
                    .is_paused();
                if !is_paused {
                    app.toggle_pause_updates(Some("due previous errors"));
                }

                const CLICKHOUSE_ERROR_CODE_ALL_CONNECTION_TRIES_FAILED: u32 = 279;
                let has_cluster = app
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
                    app.add_layer(Dialog::info(format!(
                        "{}\n(consider adding skip_unavailable_shards=1 to the connection URL)",
                        err
                    )));
                    return;
                }

                app.add_layer(Dialog::info(err.to_string()));
            }))
            // Ignore errors on exit
            .unwrap_or_default();
    }
    let elapsed = stopwatch.elapsed();
    debug_metrics.record_event(elapsed);
    let mut completion_status = format!("Processing {} took {} ms.", key, elapsed.as_millis());

    // It should not be reset, since delay_interval should be set to the maximum service
    // query duration time.
    if stopwatch.elapsed() > options.view.delay_interval {
        completion_status.push_str(" (consider increasing --delay_interval)");
    }

    update_status(&completion_status);

    cb_sink
        .send(Box::new(move |app: &mut App| {
            if need_clear {
                app.complete_clear();
            }
            app.on_event(UiEvent::Refresh);
        }))
        // Ignore errors on exit
        .unwrap_or_default();

    return key;
}

fn update_statusbar(cb_sink: &UiSink, message: &str) {
    let content = message.to_string();
    cb_sink
        .send(Box::new(move |app: &mut App| {
            app.set_statusbar_content(content);
        }))
        // Ignore errors on exit
        .unwrap_or_default();
}

async fn fetch_flamegraph(
    clickhouse: &ClickHouse,
    source: &FlamegraphSource,
    selected_host: Option<&String>,
) -> Result<Columns> {
    match source {
        FlamegraphSource::TraceLog {
            trace_type,
            query_ids,
            start,
            end,
        } => {
            clickhouse
                .get_flamegraph(
                    trace_type.clone(),
                    query_ids.as_deref(),
                    Some(start.clone().into()),
                    Some(end.clone().into()),
                    selected_host,
                )
                .await
        }
        FlamegraphSource::StackTrace(query_ids) => {
            clickhouse
                .get_live_query_flamegraph(query_ids, selected_host)
                .await
        }
        FlamegraphSource::Jemalloc => clickhouse.get_jemalloc_flamegraph(selected_host).await,
    }
}

async fn render_or_share_flamegraph(
    tui: bool,
    cb_sink: UiSink,
    title: String,
    data: String,
    pastila: pastila::PastilaConfig,
    live: Option<FlamegraphSource>,
) -> Result<()> {
    if tui {
        cb_sink
            .send(Box::new(move |app: &mut App| {
                if let Some(source) = live {
                    // Empty data is fine here: the updates will fill it in
                    app.show_flamelens(flamegraph::new_live_app(title, data), Some(source));
                } else {
                    match flamegraph::new_app(title, data) {
                        Ok(fl) => app.show_flamelens(fl, None),
                        Err(err) => app.add_layer(Dialog::info(err.to_string())),
                    }
                }
            }))
            .map_err(|_| anyhow!("Cannot send message to UI"))?;
    } else {
        let url = flamegraph::share(title, data, &pastila, |message| {
            update_statusbar(&cb_sink, message)
        })
        .await?;

        let url_clone = url.clone();
        cb_sink
            .send(Box::new(move |app: &mut App| {
                app.add_layer(
                    Dialog::text(format!("Flamegraph shared (encrypted):\n\n{}", url))
                        .title("Share Complete")
                        .button("Close", |app| {
                            app.pop_layer();
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
    cb_sink: UiSink,
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
        .send(Box::new(move |app: &mut App| {
            app.add_layer(
                Dialog::text(format!(
                    "Perfetto trace exported ({} bytes)\n\nOpening: {}",
                    data_len, url
                ))
                .title("Perfetto Export")
                .button("Close", |app| {
                    app.pop_layer();
                }),
            );
        }))
        .map_err(|_| anyhow!("Cannot send message to UI"))?;

    crate::utils::open_url_command(&url_clone).status()?;
    Ok(())
}

async fn process_event(context: ContextArc, event: Event, need_clear: &mut bool) -> Result<()> {
    let cb_sink = context.lock().unwrap().ui_sink.clone();
    let clickhouse = context.lock().unwrap().clickhouse.clone();
    let pastila = {
        let context = context.lock().unwrap();
        let service = &context.options.service;
        pastila::PastilaConfig {
            clickhouse_host: service.pastila_clickhouse_host.clone(),
            url: service.pastila_url.clone(),
            compress: service.pastila_compression,
        }
    };
    let selected_host = context.lock().unwrap().selected_host.clone();

    match event {
        Event::ProcessList(filter, limit) => {
            let block = clickhouse
                .get_processlist(filter, limit, selected_host.as_ref())
                .await?;
            cb_sink
                .send(Box::new(move |app: &mut App| {
                    app.call_on_name_or_render_error(
                        "processes",
                        move |view: &mut OnEventView<QueriesView>| {
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
                .send(Box::new(move |app: &mut App| {
                    app.call_on_name_or_render_error(
                        "slow_query_log",
                        move |view: &mut OnEventView<QueriesView>| {
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
                .send(Box::new(move |app: &mut App| {
                    app.call_on_name_or_render_error(
                        "last_query_log",
                        move |view: &mut OnEventView<QueriesView>| {
                            return view.get_inner_mut().update(block);
                        },
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::TextLog(view_name, args) => {
            let mut new_batch = true;
            let result = clickhouse
                .get_query_logs(&args, async |block| {
                    let is_new_batch = std::mem::take(&mut new_batch);
                    let (ack_tx, ack_rx) = oneshot::channel::<bool>();
                    let sent = cb_sink.send(Box::new(move |app: &mut App| {
                        let ret = app.call_on_name(view_name, move |view: &mut TextLogView| {
                            view.update(block, is_new_batch)
                        });
                        let ok = match ret {
                            Some(Ok(())) => true,
                            Some(Err(err)) => {
                                app.add_layer(Dialog::info(err.to_string()));
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
                .await;
            // Even a failed fetch ends the loading state
            // (the error will be reported separately)
            cb_sink
                .send(Box::new(move |app: &mut App| {
                    app.call_on_name(view_name, |view: &mut TextLogView| {
                        view.finish_loading();
                    });
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
            result?;
        }
        Event::ServerFlameGraph(tui, trace_type, start, end) => {
            let title = format!("ClickHouse Server {:?}", trace_type);
            // An end anchored to "now" (i.e. no explicit --end) makes the
            // window grow on every refresh
            let live = tui && end.get_date_time().is_none();
            let source = FlamegraphSource::TraceLog {
                trace_type,
                query_ids: None,
                start,
                end,
            };
            let flamegraph_block =
                fetch_flamegraph(&clickhouse, &source, selected_host.as_ref()).await?;
            render_or_share_flamegraph(
                tui,
                cb_sink,
                title,
                flamegraph::block_to_folded(&flamegraph_block),
                pastila.clone(),
                live.then_some(source),
            )
            .await?;
            *need_clear = true;
        }
        Event::JemallocFlameGraph(tui) => {
            let source = FlamegraphSource::Jemalloc;
            let flamegraph_block =
                fetch_flamegraph(&clickhouse, &source, selected_host.as_ref()).await?;
            render_or_share_flamegraph(
                tui,
                cb_sink,
                "ClickHouse Server jemalloc".to_string(),
                flamegraph::block_to_folded(&flamegraph_block),
                pastila.clone(),
                tui.then_some(source),
            )
            .await?;
            *need_clear = true;
        }
        Event::QueryFlameGraph(trace_type, tui, start, end, query_ids) => {
            let title = format!("ClickHouse Query {:?}", trace_type);
            // A query that is still running has no end time; keep refreshing
            // it (RelativeDateTime::from(None) resolves to now() every time)
            let live = tui && end.is_none();
            let source = FlamegraphSource::TraceLog {
                trace_type,
                query_ids: Some(query_ids),
                start: RelativeDateTime::from(start),
                end: RelativeDateTime::from(end),
            };
            let flamegraph_block =
                fetch_flamegraph(&clickhouse, &source, selected_host.as_ref()).await?;
            render_or_share_flamegraph(
                tui,
                cb_sink,
                title,
                flamegraph::block_to_folded(&flamegraph_block),
                pastila.clone(),
                live.then_some(source),
            )
            .await?;
            *need_clear = true;
        }
        Event::QueryFlameGraphDiff(trace_type, start, end, query_ids_a, query_ids_b) => {
            let title = format!("ClickHouse Query {:?} diff", trace_type);
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
                .send(Box::new(
                    move |app: &mut App| match flamegraph::new_diff_app(title, before, after) {
                        Ok(fl) => app.show_flamelens(fl, None),
                        Err(err) => app.add_layer(Dialog::info(err.to_string())),
                    },
                ))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
            *need_clear = true;
        }
        Event::LiveQueryFlameGraph(tui, query_ids) => {
            let title = if query_ids.is_some() {
                "ClickHouse Query (live)"
            } else {
                "ClickHouse Server (live)"
            };
            let source = FlamegraphSource::StackTrace(query_ids);
            let flamegraph_block =
                fetch_flamegraph(&clickhouse, &source, selected_host.as_ref()).await?;
            render_or_share_flamegraph(
                tui,
                cb_sink,
                title.to_string(),
                flamegraph::block_to_folded(&flamegraph_block),
                pastila.clone(),
                tui.then_some(source),
            )
            .await?;
            *need_clear = true;
        }
        Event::UpdateFlameGraph(source, slot) => {
            let tic = Instant::now();
            let flamegraph_block =
                fetch_flamegraph(&clickhouse, &source, selected_host.as_ref()).await?;
            let folded = flamegraph::block_to_folded(&flamegraph_block);
            // An empty result (e.g. trace_log not flushed yet, or the
            // stack_trace query finished) keeps the current graph on screen
            if !folded.trim().is_empty() {
                *slot.0.lock().unwrap() = Some(flamelens::app::ParsedFlameGraph {
                    flamegraph: flamelens::flame::FlameGraph::from_string(folded, true),
                    elapsed: tic.elapsed(),
                });
            }
        }
        Event::ExplainPlanIndexes(database, query) => {
            let plan = clickhouse
                .explain_plan_indexes(database.as_str(), query.as_str())
                .await?
                .join("\n");
            cb_sink
                .send(Box::new(move |app: &mut App| {
                    app.add_layer(
                        Dialog::around(
                            LinearLayout::vertical()
                                .child(TextView::new("EXPLAIN PLAN indexes=1").center())
                                .child(DummyView.fixed_height(1))
                                .child(TextView::new(plan)),
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
                .send(Box::new(move |app: &mut App| {
                    app.add_layer(Dialog::info(format!(
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
                .send(Box::new(move |app: &mut App| {
                    app.add_layer(
                        Dialog::around(
                            LinearLayout::vertical()
                                .child(TextView::new("EXPLAIN SYNTAX").center())
                                .child(DummyView.fixed_height(1))
                                .child(TextView::new(query)),
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
                .send(Box::new(move |app: &mut App| {
                    app.add_layer(
                        Dialog::around(
                            LinearLayout::vertical()
                                .child(TextView::new("EXPLAIN PLAN").center())
                                .child(DummyView.fixed_height(1))
                                .child(TextView::new(plan)),
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
                .send(Box::new(move |app: &mut App| {
                    app.add_layer(
                        Dialog::around(
                            LinearLayout::vertical()
                                .child(TextView::new("EXPLAIN PIPELINE").center())
                                .child(DummyView.fixed_height(1))
                                .child(TextView::new(pipeline)),
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
            match share_graph(pipeline, &pastila, |message| {
                update_statusbar(&cb_sink, message)
            })
            .await
            {
                Ok(_) => {}
                Err(err) => {
                    let error_msg = err.to_string();
                    cb_sink
                        .send(Box::new(move |app: &mut App| {
                            app.add_layer(Dialog::info(error_msg));
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
                .send(Box::new(move |app: &mut App| {
                    app.add_layer(
                        Dialog::around(TextView::new(create_statement).scrollable()).title(title),
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::KillQuery(query_id) => {
            let start = Instant::now();
            let ret = clickhouse.kill_query(query_id.as_str()).await;
            let elapsed = start.elapsed();
            // NOTE: should we do this via the UI, to block it?
            let message;
            if let Err(err) = ret {
                message = format!("{} (elapsed: {:?})", err, elapsed);
            } else {
                message = format!("Query {} killed (elapsed: {:?})", query_id, elapsed);
            }
            cb_sink
                .send(Box::new(move |app: &mut App| {
                    app.add_layer(Dialog::info(message));
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::Summary => {
            let block = clickhouse.get_summary(selected_host.as_ref()).await;
            match block {
                Err(err) => {
                    let message = err.to_string();
                    cb_sink
                        .send(Box::new(move |app: &mut App| {
                            app.add_layer(Dialog::info(message));
                        }))
                        .map_err(|_| anyhow!("Cannot send message to UI"))?;
                }
                Ok(summary) => {
                    cb_sink
                        .send(Box::new(move |app: &mut App| {
                            app.call_on_name("summary", move |view: &mut SummaryView| {
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
                .send(Box::new(move |app: &mut App| {
                    log::trace!(
                        "Updating {} (with block of {} rows)",
                        view_name,
                        block.row_count()
                    );
                    // TODO: update specific view (can we accept type somehow in the enum?)
                    app.call_on_name_or_render_error(
                        &view_name,
                        move |view: &mut OnEventView<SQLQueryView>| {
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
                .send(Box::new(move |app: &mut App| {
                    app.add_layer(
                        Dialog::around(
                            LinearLayout::vertical()
                                .child(TextView::new(title).center())
                                .child(DummyView.fixed_height(1))
                                .child(TextView::new(chart))
                                .child(TextView::new(range_label).center()),
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
                .send(Box::new(move |app: &mut App| {
                    let context = app.user_data::<ContextArc>().unwrap().clone();
                    app.add_layer(Dialog::around(
                        LinearLayout::vertical()
                            .child(TextView::new(title).center())
                            .child(DummyView.fixed_height(1))
                            .child(NamedView::new(
                                "background_schedule_pool_logs",
                                TextLogView::new(
                                    "background_schedule_pool_logs",
                                    context,
                                    TextLogArguments {
                                        query_ids: Some(query_ids),
                                        logger_names: None,
                                        hostname: None,
                                        message_filter: None,
                                        max_level: None,
                                        limit: None,
                                        start: start.into(),
                                        end,
                                    },
                                ),
                            )),
                    ));
                    app.focus_name("background_schedule_pool_logs");
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::TableParts(database, table) => {
            cb_sink
                .send(Box::new(move |app: &mut App| {
                    let context = app.user_data::<ContextArc>().unwrap().clone();
                    crate::tui::views::providers::table_parts::show_table_parts(
                        app,
                        context,
                        Some(database),
                        Some(table),
                        crate::tui::views::providers::Presentation::Dialog,
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::AsynchronousInserts(database, table) => {
            cb_sink
                .send(Box::new(move |app: &mut App| {
                    let context = app.user_data::<ContextArc>().unwrap().clone();
                    crate::tui::views::providers::asynchronous_inserts::show_asynchronous_inserts(
                        app,
                        context,
                        Some(database),
                        Some(table),
                        crate::tui::views::providers::Presentation::Dialog,
                    );
                }))
                .map_err(|_| anyhow!("Cannot send message to UI"))?;
        }
        Event::ShareLogs(content) => {
            // .terminal renders the ANSI colors (from LogViewBase::write_text)
            let url = pastila::upload_encrypted(&content.0, &pastila, ".terminal", |message| {
                update_statusbar(&cb_sink, message)
            })
            .await;
            // Remove the "Uploading logs..." dialog by name: the user may have
            // dismissed it already (its EventOwner aborts only a still-running
            // upload), and then a blind pop would remove an unrelated layer.
            cb_sink
                .send(Box::new(|app: &mut App| {
                    app.remove_layer_by_name("uploading_logs");
                }))
                .unwrap_or_default();
            let url = url?;

            let url_clone = url.clone();
            cb_sink
                .send(Box::new(move |app: &mut App| {
                    app.add_layer(
                        Dialog::text(format!("Logs shared (encrypted):\n\n{}", url))
                            .title("Share Complete")
                            .button("Close", |app| {
                                app.pop_layer();
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
                perfetto_cfg.stack_traces_by_thread,
                perfetto_cfg.compress,
            )?;

            for q in &queries.0 {
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
            builder.add_queries(&queries.0);
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
                perfetto_cfg.stack_traces_by_thread,
                perfetto_cfg.compress,
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

// clickhouse-client style: a bar with percentage when the server's total-rows
// estimate is known, plain counters otherwise.
fn format_progress(progress: &Progress) -> String {
    let fmt_bytes = SizeFormatter::new()
        .with_base(Base::Base2)
        .with_style(Style::Abbreviated);
    if progress.total_rows == 0 {
        return format!(
            "{} rows, {}",
            format_rows(progress.rows),
            fmt_bytes.format(progress.bytes as i64)
        );
    }
    let pct = ((progress.rows as f64 / progress.total_rows as f64) * 100.).min(100.) as usize;
    const BAR_WIDTH: usize = 10;
    let filled = pct * BAR_WIDTH / 100;
    return format!(
        "{}{} {}% ({} of {} rows, {})",
        "▓".repeat(filled),
        "░".repeat(BAR_WIDTH - filled),
        pct,
        format_rows(progress.rows),
        format_rows(progress.total_rows),
        fmt_bytes.format(progress.bytes as i64),
    );
}

fn format_rows(rows: u64) -> String {
    const SCALE: &[(f64, &str)] = &[(1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")];
    for &(factor, suffix) in SCALE {
        if rows as f64 >= factor {
            return format!("{:.2}{}", rows as f64 / factor, suffix);
        }
    }
    return rows.to_string();
}
