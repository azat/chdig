use anyhow::{Error, Result};
use chrono::{DateTime, Duration, Local};
use chrono_tz::Tz;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::sync::{Arc, Mutex};

use cursive::traits::{Nameable, Resizable};
use cursive::{
    event::{Callback, Event, EventResult},
    inner_getters,
    view::ViewWrapper,
    views::{self, Dialog, EditView, OnEventView},
    Cursive,
};
use size::{Base, SizeFormatter, Style};

use crate::interpreter::{
    clickhouse::Columns, clickhouse::TraceType, options::ViewOptions, BackgroundRunner, ContextArc,
    QueryProcess, WorkerEvent,
};
use crate::view::{ExtTableView, ProcessView, QueryResultView, TableViewItem, TextLogView};
use crate::wrap_impl_no_move;
use chdig::edit_query;

// Analog of mapFromArrays() in ClickHouse
fn map_from_arrays<K, V>(keys: Vec<K>, values: Vec<V>) -> HashMap<K, V>
where
    K: std::hash::Hash + std::cmp::Eq,
{
    let mut map = HashMap::new();
    for (k, v) in keys.into_iter().zip(values) {
        map.insert(k, v);
    }
    return map;
}
// count() OVER (PARTITION BY initial_query_id)
fn queries_count_subqueries(queries: &mut HashMap<String, QueryProcess>) {
    // <initial_query_id, count()>
    let mut subqueries = HashMap::<String, u64>::new();
    for v in queries.values_mut() {
        if let Some(c) = subqueries.get_mut(v.initial_query_id.as_str()) {
            *c += 1;
        } else {
            subqueries.insert(v.initial_query_id.clone(), 1);
        }
    }
    for v in queries.values_mut() {
        v.subqueries = subqueries[&v.initial_query_id];
    }
}
fn sum_map<K, V>(m1: &HashMap<K, V>, m2: &HashMap<K, V>) -> HashMap<K, V>
where
    K: std::hash::Hash + std::cmp::Eq + Clone,
    V: std::ops::AddAssign + Copy,
{
    let mut dst = m1.clone();
    for (k, v) in m2.iter() {
        if let Some(new_v) = dst.get_mut(k) {
            *new_v += *v;
        } else {
            dst.insert(k.clone(), *v);
        }
    }
    return dst;
}
// if(is_initial_query, (sumMap(ProfileEvents) OVER (PARTITION BY initial_query_id)), ProfileEvents)
fn queries_sum_profile_events(queries: &mut HashMap<String, QueryProcess>) {
    // <initial_query_id, sumMap(ProfileEvents)>
    let mut profile_events = HashMap::<String, HashMap<String, u64>>::new();
    for v in queries.values_mut() {
        if let Some(pe) = profile_events.get_mut(v.initial_query_id.as_str()) {
            *pe = sum_map(pe, &v.profile_events);
        } else {
            profile_events.insert(v.initial_query_id.clone(), v.profile_events.clone());
        }
    }
    for v in queries.values_mut() {
        if v.is_initial_query {
            v.profile_events = profile_events.remove(&v.initial_query_id).unwrap();
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum QueryProcessesColumn {
    Selection,
    HostName,
    SubQueries,
    Cpu,
    IOWait,
    CPUWait,
    User,
    Threads,
    Memory,
    DiskIO,
    IO,
    NetIO,
    Elapsed,
    QueryId,
    Query,
}
impl PartialEq<QueryProcess> for QueryProcess {
    fn eq(&self, other: &Self) -> bool {
        return self.query_id == other.query_id;
    }
}

impl TableViewItem<QueryProcessesColumn> for QueryProcess {
    fn to_column(&self, column: QueryProcessesColumn) -> String {
        let formatter = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);

        match column {
            QueryProcessesColumn::Selection => {
                if self.selection {
                    "x".to_string()
                } else {
                    " ".to_string()
                }
            }
            QueryProcessesColumn::HostName => self.host_name.to_string(),
            QueryProcessesColumn::SubQueries => {
                if self.is_initial_query {
                    return self.subqueries.to_string();
                } else {
                    return 1.to_string();
                }
            }
            QueryProcessesColumn::Cpu => format!("{:.1} %", self.cpu()),
            QueryProcessesColumn::IOWait => format!("{:.1} %", self.io_wait()),
            QueryProcessesColumn::CPUWait => format!("{:.1} %", self.cpu_wait()),
            QueryProcessesColumn::User => self.user.clone(),
            QueryProcessesColumn::Threads => self.threads.to_string(),
            QueryProcessesColumn::Memory => formatter.format(self.memory),
            QueryProcessesColumn::DiskIO => formatter.format(self.disk_io() as i64),
            QueryProcessesColumn::IO => formatter.format(self.io() as i64),
            QueryProcessesColumn::NetIO => formatter.format(self.net_io() as i64),
            QueryProcessesColumn::Elapsed => format!("{:.2}", self.elapsed),
            QueryProcessesColumn::QueryId => {
                if self.subqueries > 1 && self.is_initial_query {
                    return format!("-> {}", self.query_id);
                } else {
                    return self.query_id.clone();
                }
            }
            QueryProcessesColumn::Query => self.normalized_query.clone(),
        }
    }

    fn cmp(&self, other: &Self, column: QueryProcessesColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            QueryProcessesColumn::Selection => self.selection.cmp(&other.selection),
            QueryProcessesColumn::HostName => self.host_name.cmp(&other.host_name),
            QueryProcessesColumn::SubQueries => self.subqueries.cmp(&other.subqueries),
            QueryProcessesColumn::Cpu => self.cpu().total_cmp(&other.cpu()),
            QueryProcessesColumn::IOWait => self.io_wait().total_cmp(&other.io_wait()),
            QueryProcessesColumn::CPUWait => self.cpu_wait().total_cmp(&other.cpu_wait()),
            QueryProcessesColumn::User => self.user.cmp(&other.user),
            QueryProcessesColumn::Threads => self.threads.cmp(&other.threads),
            QueryProcessesColumn::Memory => self.memory.cmp(&other.memory),
            QueryProcessesColumn::DiskIO => self.disk_io().total_cmp(&other.disk_io()),
            QueryProcessesColumn::IO => self.io().total_cmp(&other.io()),
            QueryProcessesColumn::NetIO => self.net_io().total_cmp(&other.net_io()),
            QueryProcessesColumn::Elapsed => self.elapsed.total_cmp(&other.elapsed),
            QueryProcessesColumn::QueryId => self.query_id.cmp(&other.query_id),
            QueryProcessesColumn::Query => self.normalized_query.cmp(&other.normalized_query),
        }
    }
}

pub struct ProcessesView {
    context: ContextArc,
    table: ExtTableView<QueryProcess, QueryProcessesColumn>,
    items: HashMap<String, QueryProcess>,
    // For show only specific query
    query_id: Option<String>,
    // For multi selection
    selected_query_ids: HashSet<String>,
    has_selection_column: bool,
    options: ViewOptions,
    // Is this running processes, or queries from system.query_log?
    is_system_processes: bool,
    // Used to filter queries
    filter: Arc<Mutex<String>>,
    // Number of queries to render
    limit: Arc<Mutex<u64>>,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
}

#[derive(Debug, Clone)]
pub enum Type {
    ProcessList,
    SlowQueryLog,
    LastQueryLog,
}

impl ProcessesView {
    inner_getters!(self.table: ExtTableView<QueryProcess, QueryProcessesColumn>);

    pub fn update(&mut self, processes: Columns) -> Result<()> {
        let prev_items = take(&mut self.items);

        // Selected queries should be updated, since in the new query list it may not be exists
        // already
        let mut new_selected_query_ids = HashSet::new();

        // TODO: write some closure to extract the field with type propagation.
        for i in 0..processes.row_count() {
            let mut query_process = QueryProcess {
                selection: false,
                host_name: processes.get::<_, _>(i, "host_name")?,
                user: processes.get::<_, _>(i, "user")?,
                threads: processes.get::<Vec<u64>, _>(i, "thread_ids")?.len(),
                memory: processes.get::<_, _>(i, "peak_memory_usage")?,
                elapsed: processes.get::<_, _>(i, "elapsed")?,
                query_start_time_microseconds: processes
                    .get::<DateTime<Tz>, _>(i, "query_start_time_microseconds")?
                    .with_timezone(&Local),
                subqueries: 1, // See queries_count_subqueries()
                is_initial_query: processes.get::<u8, _>(i, "is_initial_query")? == 1,
                initial_query_id: processes.get::<_, _>(i, "initial_query_id")?,
                query_id: processes.get::<_, _>(i, "query_id")?,
                normalized_query: processes.get::<_, _>(i, "normalized_query")?,
                original_query: processes.get::<_, _>(i, "original_query")?,
                current_database: processes.get::<_, _>(i, "current_database")?,
                profile_events: map_from_arrays(
                    processes.get::<Vec<String>, _>(i, "ProfileEvents.Names")?,
                    processes.get::<Vec<u64>, _>(i, "ProfileEvents.Values")?,
                ),
                settings: map_from_arrays(
                    processes.get::<Vec<String>, _>(i, "Settings.Names")?,
                    processes.get::<Vec<String>, _>(i, "Settings.Values")?,
                ),

                prev_elapsed: None,
                prev_profile_events: None,

                running: self.is_system_processes,
            };

            // FIXME: Shrinking is slow, but without it memory consumption is too high, 100-200x
            // more! This is because by some reason the capacity inside clickhouse.rs is 4096,
            // which is ~100x more then we need for ProfileEvents (~40).
            query_process.profile_events.shrink_to_fit();
            query_process.settings.shrink_to_fit();

            if self.selected_query_ids.contains(&query_process.query_id) {
                new_selected_query_ids.insert(query_process.query_id.clone());
            }

            if let Some(prev_item) = prev_items.get(&query_process.query_id) {
                query_process.prev_elapsed = Some(prev_item.elapsed);
                query_process.prev_profile_events = Some(prev_item.profile_events.clone());
            }

            self.items
                .insert(query_process.query_id.clone(), query_process);
        }

        queries_count_subqueries(&mut self.items);
        if !self.options.no_subqueries {
            queries_sum_profile_events(&mut self.items);
        }

        self.selected_query_ids = new_selected_query_ids;
        self.update_view();

        return Ok(());
    }

    fn update_view(&mut self) {
        let mut items = Vec::new();
        if let Some(query_id) = &self.query_id {
            for query_process in self.items.values() {
                if query_process.initial_query_id == *query_id {
                    items.push(query_process.clone());
                }
            }
        } else {
            let mut query_ids = HashSet::new();
            for query_process in self.items.values() {
                query_ids.insert(&query_process.query_id);
            }

            for query_process in self.items.values() {
                if self.options.group_by {
                    // In case of grouping, do not show initial queries if they have initial query.
                    if !query_process.is_initial_query
                        && query_ids.contains(&query_process.initial_query_id)
                    {
                        continue;
                    }
                }
                items.push(query_process.clone());
            }
        }

        let inner_table = self.table.get_inner_mut().get_inner_mut();

        if !self.selected_query_ids.is_empty() {
            if !self.has_selection_column {
                inner_table.insert_column(0, QueryProcessesColumn::Selection, "v", |c| c.width(1));
                self.has_selection_column = true;
            }
            for item in &mut items {
                item.selection = self.selected_query_ids.contains(&item.query_id);
            }
        } else if self.has_selection_column {
            inner_table.remove_column(0);
            self.has_selection_column = false;
        }

        inner_table.set_items_stable(items);
    }

    fn show_flamegraph(&mut self, tui: bool, trace_type: Option<TraceType>) -> Result<()> {
        let (query_ids, min_query_start_microseconds, max_query_end_microseconds) =
            self.get_query_ids()?;
        let mut context_locked = self.context.lock().unwrap();
        if let Some(trace_type) = trace_type {
            context_locked.worker.send(WorkerEvent::ShowQueryFlameGraph(
                trace_type,
                tui,
                min_query_start_microseconds,
                max_query_end_microseconds,
                query_ids,
            ));
        } else {
            context_locked
                .worker
                .send(WorkerEvent::ShowLiveQueryFlameGraph(tui, query_ids));
        }

        return Ok(());
    }

    fn get_selected_query(&self) -> Result<QueryProcess> {
        let inner_table = self.table.get_inner().get_inner();
        let item_index = inner_table.item().ok_or(Error::msg("No query selected"))?;
        let item = inner_table
            .borrow_item(item_index)
            .ok_or(Error::msg("No such row anymore"))?;
        return Ok(item.clone());
    }

    fn get_query_ids(&self) -> Result<(Vec<String>, DateTime<Local>, Option<DateTime<Local>>)> {
        let selected_query = self.get_selected_query()?;
        let current_query_id = selected_query.query_id.clone();
        let mut min_query_start_microseconds = selected_query.query_start_time_microseconds;
        let mut max_query_end_microseconds = Option::<DateTime<Local>>::None;

        let mut query_ids = Vec::new();

        // In case of multi selection ignore current row, but otherwise current query_id should be
        // added since it may not be contained in self.items already.
        if self.selected_query_ids.is_empty() {
            query_ids.push(current_query_id.clone());
        }

        if !self.options.no_subqueries {
            if !self.selected_query_ids.is_empty() {
                for q in self.items.values() {
                    // NOTE: we have to look at both here, since selected_query_ids contains
                    // query_id not initial_query_id, while we are curious about both
                    if self.selected_query_ids.contains(&q.initial_query_id)
                        || self.selected_query_ids.contains(&q.query_id)
                    {
                        query_ids.push(q.query_id.clone());
                    }
                }
            } else if let Some(selected_query_id) = &self.query_id {
                for q in self.items.values() {
                    if q.initial_query_id == *selected_query_id {
                        query_ids.push(q.query_id.clone());
                    }
                }
            } else {
                for q in self.items.values() {
                    if q.initial_query_id == current_query_id {
                        query_ids.push(q.query_id.clone());
                    }
                }
            }
        } else {
            query_ids.extend(self.selected_query_ids.clone());
        }

        // Update min_query_start_microseconds/max_query_end_microseconds
        {
            let query_ids_set = HashSet::<&String>::from_iter(query_ids.iter());
            for q in self.items.values() {
                if !query_ids_set.contains(&q.query_id) {
                    continue;
                }
                if q.query_start_time_microseconds < min_query_start_microseconds {
                    min_query_start_microseconds = q.query_start_time_microseconds;
                }
                if !self.is_system_processes {
                    let query_end_time_microseconds = q
                        .query_start_time_microseconds
                        .checked_add_signed(
                            Duration::try_milliseconds((q.elapsed * 1e3) as i64).unwrap(),
                        )
                        .unwrap();

                    if let Some(max) = max_query_end_microseconds {
                        if query_end_time_microseconds < max {
                            max_query_end_microseconds = Some(query_end_time_microseconds);
                        }
                    } else {
                        max_query_end_microseconds = Some(query_end_time_microseconds);
                    }
                }
            }
        }

        return Ok((
            query_ids,
            min_query_start_microseconds,
            max_query_end_microseconds,
        ));
    }

    pub fn update_limit(&mut self, is_sub: bool) {
        let new_limit = if is_sub {
            self.limit.clone().lock().unwrap().saturating_sub(20)
        } else {
            self.limit.clone().lock().unwrap().saturating_add(20)
        };
        *self.limit.clone().lock().unwrap() = new_limit;
        log::debug!("Set limit to {}", new_limit);
    }

    pub fn new(
        context: ContextArc,
        processes_type: Type,
        view_name: &'static str,
    ) -> views::OnEventView<Self> {
        let delay = context.lock().unwrap().options.view.delay_interval;

        let is_system_processes = matches!(processes_type, Type::ProcessList);
        let filter = Arc::new(Mutex::new(String::new()));
        let limit = Arc::new(Mutex::new(if matches!(processes_type, Type::ProcessList) {
            10000
        } else {
            100_u64
        }));

        let update_callback_context = context.clone();
        let update_callback_filter = filter.clone();
        let update_callback_limit = limit.clone();
        let update_callback = move || {
            let mut context = update_callback_context.lock().unwrap();
            let filter = update_callback_filter.lock().unwrap().clone();
            let limit = *update_callback_limit.lock().unwrap();

            let start_time = context.options.view.start;
            let end_time = context.options.view.end;

            match processes_type {
                Type::ProcessList => context
                    .worker
                    .send(WorkerEvent::UpdateProcessList(filter, limit)),
                Type::SlowQueryLog => context.worker.send(WorkerEvent::UpdateSlowQueryLog(
                    filter, start_time, end_time, limit,
                )),
                Type::LastQueryLog => context.worker.send(WorkerEvent::UpdateLastQueryLog(
                    filter, start_time, end_time, limit,
                )),
            }
        };

        let mut table = ExtTableView::<QueryProcess, QueryProcessesColumn>::default();
        let inner_table = table.get_inner_mut().get_inner_mut();
        inner_table.add_column(QueryProcessesColumn::QueryId, "query_id", |c| c.width(12));
        inner_table.add_column(QueryProcessesColumn::Cpu, "cpu", |c| c.width(8));
        inner_table.add_column(QueryProcessesColumn::IOWait, "io_wait", |c| c.width(11));
        inner_table.add_column(QueryProcessesColumn::CPUWait, "cpu_wait", |c| c.width(12));
        inner_table.add_column(QueryProcessesColumn::User, "user", |c| c.width(8));
        inner_table.add_column(QueryProcessesColumn::Threads, "thr", |c| c.width(6));
        inner_table.add_column(QueryProcessesColumn::Memory, "mem", |c| c.width(6));
        inner_table.add_column(QueryProcessesColumn::DiskIO, "disk", |c| c.width(7));
        inner_table.add_column(QueryProcessesColumn::IO, "io", |c| c.width(7));
        inner_table.add_column(QueryProcessesColumn::NetIO, "net", |c| c.width(6));
        inner_table.add_column(QueryProcessesColumn::Elapsed, "elapsed", |c| c.width(11));
        inner_table.add_column(QueryProcessesColumn::Query, "query", |c| c);
        inner_table.set_on_submit(|siv, _row, _index| {
            siv.on_event(Event::Char('l'));
        });

        inner_table.sort_by(QueryProcessesColumn::Elapsed, Ordering::Greater);

        let view_options = context.lock().unwrap().options.view.clone();

        if !view_options.no_subqueries {
            inner_table.insert_column(0, QueryProcessesColumn::SubQueries, "Q#", |c| c.width(5));
        }
        if context.lock().unwrap().options.clickhouse.cluster.is_some() {
            inner_table.insert_column(0, QueryProcessesColumn::HostName, "host", |c| c.width(8));
        }

        let bg_runner_cv = context.lock().unwrap().background_runner_cv.clone();
        let mut bg_runner = BackgroundRunner::new(delay, bg_runner_cv);
        bg_runner.start(update_callback);

        let processes_view = ProcessesView {
            context: context.clone(),
            table,
            items: HashMap::new(),
            query_id: None,
            selected_query_ids: HashSet::new(),
            has_selection_column: false,
            options: view_options,
            is_system_processes,
            filter,
            limit,
            bg_runner,
        };

        // TODO:
        // - pause/disable the table if the foreground view had been changed
        // - space - multiquery selection (KILL, flamegraphs, logs, ...)
        let mut event_view = views::OnEventView::new(processes_view);

        let context_copy = context.clone();
        event_view.set_on_pre_event_inner(Event::Refresh, move |v, _| {
            let action_callback = context_copy.lock().unwrap().pending_view_callback.take();
            if let Some(action_callback) = action_callback {
                let result = action_callback.as_ref()(v);
                match result {
                    Err(err) => {
                        return Some(EventResult::with_cb_once(move |siv: &mut Cursive| {
                            siv.add_layer(Dialog::info(err.to_string()));
                        }));
                    }
                    Ok(event) => return event,
                }
            }
            return Some(EventResult::Ignored);
        });
        log::debug!("Adding views actions");
        let mut context = context.lock().unwrap();
        context.add_view_action(&mut event_view, "Select", ' ', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            let selected_query = v.get_selected_query()?;
            let query_id = selected_query.query_id.clone();

            if v.selected_query_ids.contains(&query_id) {
                v.selected_query_ids.remove(&query_id);
            } else {
                v.selected_query_ids.insert(query_id);
            }
            v.update_view();

            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(&mut event_view, "Show all queries", '-', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            v.query_id = None;
            v.update_view();
            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(&mut event_view, "Show queries on shards", '+', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            let selected_query = v.get_selected_query()?;
            let query_id = selected_query.query_id.clone();

            v.query_id = Some(query_id);
            v.update_view();

            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(&mut event_view, "Filter", '/', move |_v| {
            return Ok(Some(EventResult::Consumed(Some(Callback::from_fn(
                move |siv: &mut Cursive| {
                    let filter_cb = move |siv: &mut Cursive, text: &str| {
                        siv.call_on_name(view_name, |v: &mut OnEventView<ProcessesView>| {
                            let v = v.get_inner_mut();
                            log::info!("Set filter to '{}'", text);
                            *v.filter.lock().unwrap() = text.to_string();
                            // Trigger update
                            //
                            // NOTE: It will require first summary view and only after
                            // processes view, and this may be slow in case of cluster mode, and
                            // should be addressed.
                            v.bg_runner.schedule();
                        });
                        siv.pop_layer();
                    };
                    let view = OnEventView::new(EditView::new().on_submit(filter_cb).min_width(10));
                    siv.add_layer(view);
                },
            )))));
        });
        context.add_view_action(&mut event_view, "Query details", 'D', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            let selected_query = v.get_selected_query()?;
            v.context
                .lock()
                .unwrap()
                .cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(views::Dialog::around(
                        ProcessView::new(selected_query)
                            .with_name("process")
                            .min_size((70, 35)),
                    ));
                }))
                .unwrap();

            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(&mut event_view, "Query processors", 'P', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            // FIXME: after [1] we could simply use "initial_query_id"
            //
            //   [1]: https://github.com/ClickHouse/ClickHouse/pull/49777
            let (query_ids, min_query_start_microseconds, max_query_end_microseconds) = v.get_query_ids()?;
            let columns = vec![
                "name",
                "count() count",
                // TODO: support this units in QueryResultView
                "sum(elapsed_us)/1e6 elapsed_sec",
                "sum(input_wait_elapsed_us)/1e6 input_wait_sec",
                "sum(output_wait_elapsed_us)/1e6 output_wait_sec",
                "sum(input_rows) rows",
                "sum(input_bytes) bytes",
                "round(bytes/elapsed_sec,2)/1e6 bytes_per_sec",
            ];
            let sort_by = "elapsed_sec";
            let table = "system.processors_profile_log";
            let dbtable = v.context.lock().unwrap().clickhouse.get_table_name(table);
            let query = format!(
                r#"
                WITH
                    fromUnixTimestamp64Nano({}) AS start_time_,
                    fromUnixTimestamp64Nano({}) AS end_time_
                SELECT {}
                FROM {}
                WHERE
                        event_date >= toDate(start_time_) AND event_time >  toDateTime(start_time_) AND event_time_microseconds > start_time_
                    AND event_date <= toDate(end_time_)   AND event_time <= toDateTime(end_time_)   AND event_time_microseconds <= end_time_
                    AND query_id IN ('{}')
                GROUP BY name
                ORDER BY name ASC
                "#,
                min_query_start_microseconds
                    .timestamp_nanos_opt()
                    .ok_or(Error::msg("Invalid time"))?,
                max_query_end_microseconds
                    .unwrap_or(Local::now())
                    .timestamp_nanos_opt()
                    .ok_or(Error::msg("Invalid time"))?,
                columns.join(", "),
                dbtable,
                query_ids.join("','"),
            );

            let context_copy = v.context.clone();
            v.context
                .lock()
                .unwrap()
                .cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(views::Dialog::around(
                        views::LinearLayout::vertical()
                            .child(views::TextView::new("Processors:").center())
                            .child(views::DummyView.fixed_height(1))
                            .child(
                                QueryResultView::new(
                                    context_copy,
                                    table,
                                    sort_by,
                                    columns.clone(),
                                    1,
                                    query,
                                )
                                .unwrap_or_else(|_| panic!("Cannot get {}", table))
                                .with_name(table)
                                // TODO: autocalculate
                                .min_size((160, 40)),
                            ),
                    ));
                }))
                .unwrap();

            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(&mut event_view, "Query views", 'v', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            let (query_ids, min_query_start_microseconds, max_query_end_microseconds) = v.get_query_ids()?;
            let columns = vec!["view_name", "view_duration_ms"];
            let sort_by = "view_duration_ms";
            let table = "system.query_views_log";
            let dbtable = v.context.lock().unwrap().clickhouse.get_table_name(table);
            let query = format!(
                r#"
                WITH
                    fromUnixTimestamp64Nano({}) AS start_time_,
                    fromUnixTimestamp64Nano({}) AS end_time_
                SELECT {}
                FROM {}
                WHERE
                        event_date >= toDate(start_time_) AND event_time >  toDateTime(start_time_) AND event_time_microseconds > start_time_
                    AND event_date <= toDate(end_time_)   AND event_time <= toDateTime(end_time_)   AND event_time_microseconds <= end_time_
                    AND initial_query_id IN ('{}')
                ORDER BY view_duration_ms DESC
                "#,
                min_query_start_microseconds
                    .timestamp_nanos_opt()
                    .ok_or(Error::msg("Invalid time"))?,
                max_query_end_microseconds
                    .unwrap_or(Local::now())
                    .timestamp_nanos_opt()
                    .ok_or(Error::msg("Invalid time"))?,
                columns.join(", "),
                dbtable,
                query_ids.join("','"),
            );

            let context_copy = v.context.clone();
            v.context
                .lock()
                .unwrap()
                .cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(views::Dialog::around(
                        views::LinearLayout::vertical()
                            .child(views::TextView::new("Views:").center())
                            .child(views::DummyView.fixed_height(1))
                            .child(
                                QueryResultView::new(
                                    context_copy,
                                    table,
                                    sort_by,
                                    columns.clone(),
                                    1,
                                    query,
                                )
                                .unwrap_or_else(|_| panic!("Cannot get {}", table))
                                .with_name(table)
                                // TODO: autocalculate
                                .min_size((160, 40)),
                            ),
                    ));
                }))
                .unwrap();

            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(&mut event_view, "Show CPU flamegraph", 'C', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            v.show_flamegraph(true, Some(TraceType::CPU))?;
            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(&mut event_view, "Show Real flamegraph", 'R', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            v.show_flamegraph(true, Some(TraceType::Real))?;
            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(&mut event_view, "Show memory flamegraph", 'M', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            v.show_flamegraph(true, Some(TraceType::Memory))?;
            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(&mut event_view, "Show live flamegraph", 'L', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            v.show_flamegraph(true, None)?;
            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action_without_shortcut(
            &mut event_view,
            "Show CPU flamegraph in speedscope",
            |v| {
                let v = v.downcast_mut::<ProcessesView>().unwrap();
                v.show_flamegraph(false, Some(TraceType::CPU))?;
                return Ok(Some(EventResult::consumed()));
            },
        );
        context.add_view_action_without_shortcut(
            &mut event_view,
            "Show Real flamegraph in speedscope",
            |v| {
                let v = v.downcast_mut::<ProcessesView>().unwrap();
                v.show_flamegraph(false, Some(TraceType::Real))?;
                return Ok(Some(EventResult::consumed()));
            },
        );
        context.add_view_action_without_shortcut(
            &mut event_view,
            "Show memory flamegraph in speedscope",
            |v| {
                let v = v.downcast_mut::<ProcessesView>().unwrap();
                v.show_flamegraph(false, Some(TraceType::Memory))?;
                return Ok(Some(EventResult::consumed()));
            },
        );
        context.add_view_action_without_shortcut(
            &mut event_view,
            "Show live flamegraph in speedscope",
            |v| {
                let v = v.downcast_mut::<ProcessesView>().unwrap();
                v.show_flamegraph(false, None)?;
                return Ok(Some(EventResult::consumed()));
            },
        );
        context.add_view_action(
            &mut event_view,
            "Edit query and execute",
            Event::AltChar('E'),
            |v| {
                let v = v.downcast_mut::<ProcessesView>().unwrap();
                let selected_query = v.get_selected_query()?;
                let query = selected_query.original_query.clone();
                let database = selected_query.current_database.clone();
                let settings = selected_query.settings.clone();
                let mut context_locked = v.context.lock().unwrap();

                // TODO: prepend database
                let query = edit_query(&query, &settings)?;

                // TODO: add support for Log packets into clickhouse-rs and execute query with logging in place
                context_locked
                    .worker
                    .send(WorkerEvent::ExecuteQuery(database, query));

                return Ok(Some(EventResult::Consumed(Some(Callback::from_fn_once(
                    |siv| siv.clear(),
                )))));
            },
        );
        context.add_view_action(&mut event_view, "EXPLAIN SYNTAX", 's', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            let selected_query = v.get_selected_query()?;
            let query = selected_query.original_query.clone();
            let database = selected_query.current_database.clone();
            let settings = selected_query.settings.clone();
            let mut context_locked = v.context.lock().unwrap();
            context_locked
                .worker
                .send(WorkerEvent::ExplainSyntax(database, query, settings));

            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(&mut event_view, "EXPLAIN PLAN", 'e', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            let selected_query = v.get_selected_query()?;
            let query = selected_query.original_query.clone();
            let database = selected_query.current_database.clone();
            let mut context_locked = v.context.lock().unwrap();
            context_locked
                .worker
                .send(WorkerEvent::ExplainPlan(database, query));

            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(&mut event_view, "EXPLAIN PIPELINE", 'E', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            let selected_query = v.get_selected_query()?;
            let query = selected_query.original_query.clone();
            let database = selected_query.current_database.clone();
            let mut context_locked = v.context.lock().unwrap();
            context_locked
                .worker
                .send(WorkerEvent::ExplainPipeline(database, query));

            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(
            &mut event_view,
            "EXPLAIN PIPELINE graph=1 (open in browser)",
            'G',
            |v| {
                let v = v.downcast_mut::<ProcessesView>().unwrap();
                let selected_query = v.get_selected_query()?;
                let query = selected_query.original_query.clone();
                let database = selected_query.current_database.clone();
                let mut context_locked = v.context.lock().unwrap();
                context_locked
                    .worker
                    .send(WorkerEvent::ExplainPipelineOpenGraphInBrowser(
                        database, query,
                    ));

                return Ok(Some(EventResult::consumed()));
            },
        );
        context.add_view_action(&mut event_view, "EXPLAIN INDEXES", 'I', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            let selected_query = v.get_selected_query()?;
            let query = selected_query.original_query.clone();
            let database = selected_query.current_database.clone();
            let mut context_locked = v.context.lock().unwrap();
            context_locked
                .worker
                .send(WorkerEvent::ExplainPlanIndexes(database, query));

            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(&mut event_view, "KILL query", 'K', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            let selected_query = v.get_selected_query()?;
            let query_id = selected_query.query_id.clone();
            let context_copy = v.context.clone();
            v.context
                .lock()
                .unwrap()
                .cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(
                        views::Dialog::new()
                            .title(format!(
                                "Are you sure you want to KILL QUERY with query_id = {}",
                                query_id
                            ))
                            .button("Yes, I'm sure", move |s| {
                                context_copy
                                    .lock()
                                    .unwrap()
                                    .worker
                                    .send(WorkerEvent::KillQuery(query_id.clone()));
                                // TODO: wait for the KILL
                                s.pop_layer();
                            })
                            .button("Cancel", |s| {
                                s.pop_layer();
                            }),
                    );
                }))
                .unwrap();

            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(&mut event_view, "Show query logs", 'l', |v| {
            let v = v.downcast_mut::<ProcessesView>().unwrap();
            let (query_ids, min_query_start_microseconds, max_query_end_microseconds) =
                v.get_query_ids()?;
            let context_copy = v.context.clone();
            v.context
                .lock()
                .unwrap()
                .cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(views::Dialog::around(
                        views::LinearLayout::vertical()
                            .child(views::TextView::new("Logs:").center())
                            .child(views::DummyView.fixed_height(1))
                            .child(views::NamedView::new(
                                "query_log",
                                TextLogView::new(
                                    context_copy,
                                    min_query_start_microseconds,
                                    max_query_end_microseconds,
                                    query_ids,
                                ),
                            )),
                    ));
                    // FIXME: this should be done automatically (maybe due to lots of wrapping it
                    // does not work)
                    siv.focus_name("query_log").unwrap();
                }))
                .unwrap();

            return Ok(Some(EventResult::consumed()));
        });
        context.add_view_action(
            &mut event_view,
            "Increase number of queries to render to 20",
            '(',
            |v| {
                let v = v.downcast_mut::<ProcessesView>().unwrap();
                v.update_limit(true);
                v.bg_runner.schedule();
                return Ok(Some(EventResult::consumed()));
            },
        );
        context.add_view_action(
            &mut event_view,
            "Decrease number of queries to render to 20",
            ')',
            |v| {
                let v = v.downcast_mut::<ProcessesView>().unwrap();
                v.update_limit(false);
                v.bg_runner.schedule();
                return Ok(Some(EventResult::consumed()));
            },
        );
        return event_view;
    }
}

impl Drop for ProcessesView {
    fn drop(&mut self) {
        log::debug!("Removing views actions");
        self.context.lock().unwrap().view_actions.clear();
    }
}

// TODO: remove this extra wrapping
impl ViewWrapper for ProcessesView {
    wrap_impl_no_move!(self.table: ExtTableView<QueryProcess, QueryProcessesColumn>);
}
