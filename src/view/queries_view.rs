use anyhow::{Error, Result};
use chrono::{DateTime, Local, TimeDelta};
use cursive::view::Scrollable;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::sync::{Arc, Mutex};

use cursive::traits::{Nameable, Resizable};
use cursive::{
    Cursive,
    event::{Callback, Event, EventResult},
    inner_getters,
    view::ViewWrapper,
    views::{self, Dialog, OnEventView},
};
use size::{Base, SizeFormatter, Style};

use crate::common::RelativeDateTime;
use crate::view::show_bottom_prompt;
use crate::{
    interpreter::{
        BackgroundRunner, ContextArc, Query, TextLogArguments, WorkerEvent, clickhouse::Columns,
        clickhouse::TraceType, options::ViewOptions,
    },
    utils::{edit_query, find_common_hostname_prefix_and_suffix, get_query},
    view::table_view::TableView,
    view::{QueryView, SQLQueryView, TableViewItem, TextLogView},
    wrap_impl_no_move,
};

// ClickHouse may flush some system.* tables after system.query_log, likely it is only a precision
// error, so 1 second should be enough.
const QUERY_TIME_DRIFT_BUFFER_SECONDS: i64 = 1;

// count() OVER (PARTITION BY initial_query_id)
fn queries_count_subqueries(queries: &mut HashMap<String, Query>) {
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
fn queries_sum_profile_events(queries: &mut HashMap<String, Query>) {
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
pub enum QueriesColumn {
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
    QueryEnd,
    QueryId,
    Query,
}
impl PartialEq<Query> for Query {
    fn eq(&self, other: &Self) -> bool {
        return self.query_id == other.query_id;
    }
}

impl TableViewItem<QueriesColumn> for Query {
    fn to_column(&self, column: QueriesColumn) -> String {
        let formatter = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);

        match column {
            QueriesColumn::Selection => {
                if self.selection {
                    "x".to_string()
                } else {
                    " ".to_string()
                }
            }
            QueriesColumn::HostName => self.host_name.to_string(),
            QueriesColumn::SubQueries => {
                if self.is_initial_query {
                    return self.subqueries.to_string();
                } else {
                    return 1.to_string();
                }
            }
            QueriesColumn::Cpu => format!("{:.1} %", self.cpu()),
            QueriesColumn::IOWait => format!("{:.1} %", self.io_wait()),
            QueriesColumn::CPUWait => format!("{:.1} %", self.cpu_wait()),
            QueriesColumn::User => self.user.clone(),
            QueriesColumn::Threads => self.threads.to_string(),
            QueriesColumn::Memory => formatter.format(self.memory),
            QueriesColumn::DiskIO => formatter.format(self.disk_io() as i64),
            QueriesColumn::IO => formatter.format(self.io() as i64),
            QueriesColumn::NetIO => formatter.format(self.net_io() as i64),
            QueriesColumn::Elapsed => format!("{:.2}", self.elapsed),
            QueriesColumn::QueryEnd => format!("{}", self.query_end_time_microseconds),
            QueriesColumn::QueryId => {
                if self.subqueries > 1 && self.is_initial_query {
                    return format!("-> {}", self.query_id);
                } else {
                    return self.query_id.clone();
                }
            }
            QueriesColumn::Query => self.normalized_query.clone(),
        }
    }

    fn cmp(&self, other: &Self, column: QueriesColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            QueriesColumn::Selection => self.selection.cmp(&other.selection),
            QueriesColumn::HostName => self.host_name.cmp(&other.host_name),
            QueriesColumn::SubQueries => self.subqueries.cmp(&other.subqueries),
            QueriesColumn::Cpu => self.cpu().total_cmp(&other.cpu()),
            QueriesColumn::IOWait => self.io_wait().total_cmp(&other.io_wait()),
            QueriesColumn::CPUWait => self.cpu_wait().total_cmp(&other.cpu_wait()),
            QueriesColumn::User => self.user.cmp(&other.user),
            QueriesColumn::Threads => self.threads.cmp(&other.threads),
            QueriesColumn::Memory => self.memory.cmp(&other.memory),
            QueriesColumn::DiskIO => self.disk_io().total_cmp(&other.disk_io()),
            QueriesColumn::IO => self.io().total_cmp(&other.io()),
            QueriesColumn::NetIO => self.net_io().total_cmp(&other.net_io()),
            QueriesColumn::Elapsed => self.elapsed.total_cmp(&other.elapsed),
            QueriesColumn::QueryEnd => self
                .query_end_time_microseconds
                .cmp(&other.query_end_time_microseconds),
            QueriesColumn::QueryId => self.query_id.cmp(&other.query_id),
            QueriesColumn::Query => self.normalized_query.cmp(&other.normalized_query),
        }
    }
}

pub struct QueriesView {
    context: ContextArc,
    table: TableView<Query, QueriesColumn>,
    items: HashMap<String, Query>,
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
    // Keep clipboard alive so X11 clipboard manager can persist the data
    clipboard: Option<arboard::Clipboard>,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
}

#[derive(Debug, Clone)]
pub enum Type {
    ProcessList,
    SlowQueryLog,
    LastQueryLog,
}

impl QueriesView {
    inner_getters!(self.table: TableView<Query, QueriesColumn>);

    pub fn update(&mut self, processes: Columns) -> Result<()> {
        let prev_items = take(&mut self.items);

        // Selected queries should be updated, since in the new query list it may not be exists
        // already
        let mut new_selected_query_ids = HashSet::new();

        for i in 0..processes.row_count() {
            let mut query = Query::from_clickhouse_block(&processes, i, self.is_system_processes)?;

            if self.selected_query_ids.contains(&query.query_id) {
                new_selected_query_ids.insert(query.query_id.clone());
            }

            if let Some(prev_item) = prev_items.get(&query.query_id) {
                query.prev_elapsed = Some(prev_item.elapsed);
                query.prev_profile_events = Some(prev_item.profile_events.clone());
            }

            self.items.insert(query.query_id.clone(), query);
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
            for query in self.items.values() {
                if query.initial_query_id == *query_id {
                    items.push(query.clone());
                }
            }
        } else {
            let mut query_ids = HashSet::new();
            for query in self.items.values() {
                query_ids.insert(&query.query_id);
            }

            for query in self.items.values() {
                if self.options.group_by {
                    // In case of grouping, do not show initial queries if they have initial query.
                    if !query.is_initial_query && query_ids.contains(&query.initial_query_id) {
                        continue;
                    }
                }
                items.push(query.clone());
            }
        }

        // Strip common hostname prefix and suffix
        if !self.options.no_strip_hostname_suffix && items.len() > 1 {
            let (common_prefix, common_suffix) =
                find_common_hostname_prefix_and_suffix(items.iter().map(|q| q.host_name.as_str()));

            if !common_prefix.is_empty() || !common_suffix.is_empty() {
                for item in &mut items {
                    let mut hostname = item.host_name.as_str();

                    if !common_prefix.is_empty()
                        && let Some(stripped) = hostname.strip_prefix(&common_prefix)
                    {
                        hostname = stripped;
                    }

                    if !common_suffix.is_empty()
                        && let Some(stripped) = hostname.strip_suffix(&common_suffix)
                    {
                        hostname = stripped;
                    }

                    item.host_name = hostname.to_string();
                }
            }
        }

        if !self.selected_query_ids.is_empty() {
            if !self.has_selection_column {
                self.table
                    .insert_column(0, QueriesColumn::Selection, "v", |c| c.width(1));
                self.has_selection_column = true;
            }
            for item in &mut items {
                item.selection = self.selected_query_ids.contains(&item.query_id);
            }
        } else if self.has_selection_column {
            self.table.remove_column(0);
            self.has_selection_column = false;
        }

        self.table.set_items_stable(items);
    }

    fn show_flamegraph(&mut self, tui: bool, trace_type: Option<TraceType>) -> Result<()> {
        let (query_ids, min_query_start_microseconds, max_query_end_microseconds) =
            self.get_query_ids()?;
        let mut context_locked = self.context.lock().unwrap();
        if let Some(trace_type) = trace_type {
            context_locked.worker.send(
                true,
                WorkerEvent::QueryFlameGraph(
                    trace_type,
                    tui,
                    min_query_start_microseconds,
                    max_query_end_microseconds,
                    query_ids,
                ),
            );
        } else {
            context_locked
                .worker
                .send(true, WorkerEvent::LiveQueryFlameGraph(tui, Some(query_ids)));
        }

        return Ok(());
    }

    fn get_selected_query(&self) -> Result<Query> {
        let item_index = self.table.item().ok_or(Error::msg("No query selected"))?;
        let item = self
            .table
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
        } else {
            for q in self.items.values() {
                if q.initial_query_id == current_query_id {
                    query_ids.push(q.query_id.clone());
                }
            }
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
                    if let Some(max) = max_query_end_microseconds {
                        if q.query_end_time_microseconds > max {
                            max_query_end_microseconds = Some(q.query_end_time_microseconds);
                        }
                    } else {
                        max_query_end_microseconds = Some(q.query_end_time_microseconds);
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

    fn action_show_query_logs(&mut self) -> Result<Option<EventResult>> {
        let (query_ids, min_query_start_microseconds, max_query_end_microseconds) =
            self.get_query_ids()?;
        let context_copy = self.context.clone();
        self.context
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
                                "query_log",
                                context_copy,
                                TextLogArguments {
                                    query_ids: Some(query_ids),
                                    logger_names: None,
                                    hostname: None,
                                    message_filter: None,
                                    max_level: None,
                                    start: min_query_start_microseconds,
                                    end: RelativeDateTime::from(max_query_end_microseconds),
                                },
                            ),
                        )),
                ));
                siv.focus_name("query_log").unwrap();
            }))
            .unwrap();
        Ok(Some(EventResult::consumed()))
    }

    fn action_show_flamegraph(
        &mut self,
        tui: bool,
        trace_type: Option<TraceType>,
    ) -> Result<Option<EventResult>> {
        self.show_flamegraph(tui, trace_type)?;
        Ok(Some(EventResult::consumed()))
    }

    fn action_query_profile_events(&mut self) -> Result<Option<EventResult>> {
        // Check if multiple queries are selected
        if self.selected_query_ids.len() > 1 {
            // Get the queries for diff view
            let queries: Vec<Query> = self
                .items
                .values()
                .filter(|q| self.selected_query_ids.contains(&q.query_id))
                .cloned()
                .collect();

            if queries.is_empty() {
                return Err(Error::msg("No queries selected"));
            }

            self.context
                .lock()
                .unwrap()
                .cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(
                        views::Dialog::around(
                            QueryView::new_diff(queries, "process").min_size((120, 35)),
                        )
                        .title("Profile Events Diff"),
                    );
                }))
                .unwrap();
        } else {
            // Single query - show as before
            let selected_query = self.get_selected_query()?;
            self.context
                .lock()
                .unwrap()
                .cb_sink
                .send(Box::new(move |siv: &mut cursive::Cursive| {
                    siv.add_layer(views::Dialog::around(
                        QueryView::new(selected_query, "process").min_size((120, 35)),
                    ));
                }))
                .unwrap();
        }
        Ok(Some(EventResult::consumed()))
    }

    fn action_query_details(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        Ok(Some(EventResult::Consumed(Some(Callback::from_fn_once(
            move |siv| {
                siv.add_layer(views::Dialog::info(selected_query.to_string()).title("Details"));
            },
        )))))
    }

    fn action_edit_query_and_execute(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        let query = selected_query.original_query.clone();
        let database = selected_query.current_database.clone();
        let settings = selected_query.settings.clone();
        let mut context_locked = self.context.lock().unwrap();

        let query = edit_query(&query, &settings)?;
        context_locked
            .worker
            .send(true, WorkerEvent::ExecuteQuery(database, query));

        Ok(Some(EventResult::Consumed(Some(Callback::from_fn_once(
            |siv| siv.clear(),
        )))))
    }

    fn action_show_query(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        let query = selected_query.original_query.clone();
        let database = selected_query.current_database.clone();
        let settings = selected_query.settings.clone();

        let query = get_query(&query, &settings);
        let query = format!("USE {};\n{}", database, query);

        self.context
            .lock()
            .unwrap()
            .cb_sink
            .send(Box::new(move |siv: &mut cursive::Cursive| {
                siv.add_layer(views::Dialog::around(
                    views::LinearLayout::vertical()
                        .child(views::TextView::new("Query:").center())
                        .child(views::DummyView.fixed_height(1))
                        .child(views::TextView::new(query).scrollable()),
                ));
            }))
            .unwrap();

        Ok(Some(EventResult::consumed()))
    }

    fn action_copy_query(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        let query = selected_query.original_query.clone();

        match arboard::Clipboard::new() {
            Ok(mut clipboard) => {
                if let Err(e) = clipboard.set_text(query) {
                    return Ok(Some(EventResult::Consumed(Some(Callback::from_fn_once(
                        move |siv| {
                            siv.add_layer(Dialog::info(format!(
                                "Failed to copy to clipboard: {}",
                                e
                            )));
                        },
                    )))));
                }
                self.clipboard = Some(clipboard);
            }
            Err(e) => {
                return Ok(Some(EventResult::Consumed(Some(Callback::from_fn_once(
                    move |siv| {
                        siv.add_layer(Dialog::info(format!("Failed to access clipboard: {}", e)));
                    },
                )))));
            }
        }

        Ok(Some(EventResult::consumed()))
    }

    fn action_explain_syntax(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        let query = selected_query.original_query.clone();
        let database = selected_query.current_database.clone();
        let settings = selected_query.settings.clone();
        let mut context_locked = self.context.lock().unwrap();
        context_locked
            .worker
            .send(true, WorkerEvent::ExplainSyntax(database, query, settings));
        Ok(Some(EventResult::consumed()))
    }

    fn action_explain_plan(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        let query = selected_query.original_query.clone();
        let database = selected_query.current_database.clone();
        let mut context_locked = self.context.lock().unwrap();
        context_locked
            .worker
            .send(true, WorkerEvent::ExplainPlan(database, query));
        Ok(Some(EventResult::consumed()))
    }

    fn action_explain_pipeline(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        let query = selected_query.original_query.clone();
        let database = selected_query.current_database.clone();
        let mut context_locked = self.context.lock().unwrap();
        context_locked
            .worker
            .send(true, WorkerEvent::ExplainPipeline(database, query));
        Ok(Some(EventResult::consumed()))
    }

    fn action_select(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        let query_id = selected_query.query_id.clone();

        if self.selected_query_ids.contains(&query_id) {
            self.selected_query_ids.remove(&query_id);
        } else {
            self.selected_query_ids.insert(query_id);
        }
        self.update_view();

        Ok(Some(EventResult::consumed()))
    }

    fn action_show_all_queries(&mut self) -> Result<Option<EventResult>> {
        self.query_id = None;
        self.update_view();
        Ok(Some(EventResult::consumed()))
    }

    fn action_show_queries_on_shards(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        let query_id = selected_query.query_id.clone();

        self.query_id = Some(query_id);
        self.update_view();

        Ok(Some(EventResult::consumed()))
    }

    fn action_explain_indexes(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        let query = selected_query.original_query.clone();
        let database = selected_query.current_database.clone();
        let mut context_locked = self.context.lock().unwrap();
        context_locked
            .worker
            .send(true, WorkerEvent::ExplainPlanIndexes(database, query));
        Ok(Some(EventResult::consumed()))
    }

    fn action_explain_pipeline_graph(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        let query = selected_query.original_query.clone();
        let database = selected_query.current_database.clone();
        let mut context_locked = self.context.lock().unwrap();
        context_locked.worker.send(
            true,
            WorkerEvent::ExplainPipelineOpenGraphInBrowser(database, query),
        );
        Ok(Some(EventResult::consumed()))
    }

    fn action_kill_query(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        let query_id = selected_query.query_id.clone();
        let context_copy = self.context.clone();
        self.context
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
                                .send(true, WorkerEvent::KillQuery(query_id.clone()));
                            s.pop_layer();
                        })
                        .button("Cancel", |s| {
                            s.pop_layer();
                        }),
                );
            }))
            .unwrap();
        Ok(Some(EventResult::consumed()))
    }

    fn action_increase_limit(&mut self) -> Result<Option<EventResult>> {
        self.update_limit(true);
        self.bg_runner.schedule();
        Ok(Some(EventResult::consumed()))
    }

    fn action_decrease_limit(&mut self) -> Result<Option<EventResult>> {
        self.update_limit(false);
        self.bg_runner.schedule();
        Ok(Some(EventResult::consumed()))
    }

    fn action_query_processors(&mut self) -> Result<Option<EventResult>> {
        let (query_ids, min_query_start_microseconds, max_query_end_microseconds) =
            self.get_query_ids()?;
        let columns = vec![
            "name",
            "count() count",
            "sum(elapsed_us)/1e6 elapsed_sec",
            "sum(input_wait_elapsed_us)/1e6 input_wait_sec",
            "sum(output_wait_elapsed_us)/1e6 output_wait_sec",
            "sum(input_rows) rows",
            "sum(input_bytes) bytes",
            "round(bytes/elapsed_sec,2)/1e6 MB_per_sec",
        ];
        let sort_by = "elapsed_sec";
        let table = "processors_profile_log";
        let dbtable = self
            .context
            .lock()
            .unwrap()
            .clickhouse
            .get_table_name("system", table);

        let max_query_end_with_buffer = max_query_end_microseconds.unwrap_or(Local::now())
            + TimeDelta::seconds(QUERY_TIME_DRIFT_BUFFER_SECONDS);

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
            max_query_end_with_buffer
                .timestamp_nanos_opt()
                .ok_or(Error::msg("Invalid time"))?,
            columns.join(", "),
            dbtable,
            query_ids.join("','"),
        );

        let context_copy = self.context.clone();
        self.context
            .lock()
            .unwrap()
            .cb_sink
            .send(Box::new(move |siv: &mut cursive::Cursive| {
                siv.add_layer(views::Dialog::around(
                    views::LinearLayout::vertical()
                        .child(views::TextView::new("Processors:").center())
                        .child(views::DummyView.fixed_height(1))
                        .child(
                            SQLQueryView::new(
                                context_copy,
                                table,
                                sort_by,
                                columns.clone(),
                                vec!["name"],
                                query,
                            )
                            .unwrap_or_else(|_| panic!("Cannot get {}", table))
                            .with_name(table)
                            .min_size((160, 40)),
                        ),
                ));
            }))
            .unwrap();

        Ok(Some(EventResult::consumed()))
    }

    fn action_query_views(&mut self) -> Result<Option<EventResult>> {
        let (query_ids, min_query_start_microseconds, max_query_end_microseconds) =
            self.get_query_ids()?;
        let columns = vec!["view_name", "view_duration_ms"];
        let sort_by = "view_duration_ms";
        let table = "query_views_log";
        let dbtable = self
            .context
            .lock()
            .unwrap()
            .clickhouse
            .get_table_name("system", table);

        let max_query_end_with_buffer = max_query_end_microseconds.unwrap_or(Local::now())
            + TimeDelta::seconds(QUERY_TIME_DRIFT_BUFFER_SECONDS);

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
            max_query_end_with_buffer
                .timestamp_nanos_opt()
                .ok_or(Error::msg("Invalid time"))?,
            columns.join(", "),
            dbtable,
            query_ids.join("','"),
        );

        let context_copy = self.context.clone();
        self.context
            .lock()
            .unwrap()
            .cb_sink
            .send(Box::new(move |siv: &mut cursive::Cursive| {
                siv.add_layer(views::Dialog::around(
                    views::LinearLayout::vertical()
                        .child(views::TextView::new("Views:").center())
                        .child(views::DummyView.fixed_height(1))
                        .child(
                            SQLQueryView::new(
                                context_copy,
                                table,
                                sort_by,
                                columns.clone(),
                                vec!["view_name"],
                                query,
                            )
                            .unwrap_or_else(|_| panic!("Cannot get {}", table))
                            .with_name(table)
                            .min_size((160, 40)),
                        ),
                ));
            }))
            .unwrap();

        Ok(Some(EventResult::consumed()))
    }

    /// Ignore rustfmt max_width, otherwise callback actions looks ugly
    #[rustfmt::skip]
    pub fn new(
        context: ContextArc,
        processes_type: Type,
        view_name: &'static str,
    ) -> views::OnEventView<Self> {
        // Macro to simplify adding view actions
        macro_rules! add_action {
            // With shortcut and method arguments
            ($ctx:expr, $view:expr, $desc:expr, $shortcut:expr, $method:ident($($args:expr),*)) => {
                $ctx.add_view_action($view, $desc, $shortcut, |v| {
                    v.downcast_mut::<QueriesView>().unwrap().$method($($args),*)
                })
            };
            // Without shortcut but with method arguments
            ($ctx:expr, $view:expr, $desc:expr, $method:ident($($args:expr),*)) => {
                $ctx.add_view_action_without_shortcut($view, $desc, |v| {
                    v.downcast_mut::<QueriesView>().unwrap().$method($($args),*)
                })
            };
            // With shortcut (char or Event), no arguments
            ($ctx:expr, $view:expr, $desc:expr, $shortcut:expr, $method:ident) => {
                $ctx.add_view_action($view, $desc, $shortcut, |v| {
                    v.downcast_mut::<QueriesView>().unwrap().$method()
                })
            };
            // Without shortcut, no arguments
            ($ctx:expr, $view:expr, $desc:expr, $method:ident) => {
                $ctx.add_view_action_without_shortcut($view, $desc, |v| {
                    v.downcast_mut::<QueriesView>().unwrap().$method()
                })
            };
        }

        let delay = context.lock().unwrap().options.view.delay_interval;

        let is_system_processes = matches!(processes_type, Type::ProcessList);
        let filter = Arc::new(Mutex::new(String::new()));
        let limit = Arc::new(Mutex::new(if is_system_processes {
            10000
        } else {
            100_u64
        }));

        let update_callback_context = context.clone();
        let update_callback_filter = filter.clone();
        let update_callback_limit = limit.clone();
        let update_callback_process_type = processes_type.clone();
        let update_callback = move |force: bool| {
            let mut context = update_callback_context.lock().unwrap();
            let filter = update_callback_filter.lock().unwrap().clone();
            let limit = *update_callback_limit.lock().unwrap();

            let start_time = context.options.view.start.clone();
            let end_time = context.options.view.end.clone();

            match update_callback_process_type {
                Type::ProcessList => context
                    .worker
                    .send(force, WorkerEvent::ProcessList(filter, limit)),
                Type::SlowQueryLog => context.worker.send(
                    force,
                    WorkerEvent::SlowQueryLog(filter, start_time, end_time, limit),
                ),
                Type::LastQueryLog => context.worker.send(
                    force,
                    WorkerEvent::LastQueryLog(filter, start_time, end_time, limit),
                ),
            }
        };

        let mut table = TableView::<Query, QueriesColumn>::new();
        table.add_column(QueriesColumn::QueryId, "query_id", |c| c.width_min_max(8, 16));
        table.add_column(QueriesColumn::Cpu, "cpu", |c| c.width_min_max(3, 8));
        table.add_column(QueriesColumn::IOWait, "io_wait", |c| c.width_min_max(7, 11));
        table.add_column(QueriesColumn::CPUWait, "cpu_wait", |c| c.width_min_max(8, 12));
        table.add_column(QueriesColumn::User, "user", |c| c.width_min_max(4, 12));
        table.add_column(QueriesColumn::Threads, "thr", |c| c.width_min_max(3, 6));
        table.add_column(QueriesColumn::Memory, "mem", |c| c.width_min_max(3, 8));
        table.add_column(QueriesColumn::DiskIO, "disk", |c| c.width_min_max(4, 8));
        table.add_column(QueriesColumn::IO, "io", |c| c.width_min_max(2, 8));
        table.add_column(QueriesColumn::NetIO, "net", |c| c.width_min_max(3, 8));
        table.add_column(QueriesColumn::Elapsed, "elapsed", |c| c.width_min_max(7, 11));
        table.add_column(QueriesColumn::Query, "query", |c| c.width_min(20));
        table.set_on_submit(|siv, _row, _index| {
            let context = siv.user_data::<ContextArc>().unwrap().clone();
            let query_actions = context
                .lock()
                .unwrap()
                .view_actions
                .iter()
                .map(|x| &x.description)
                .cloned()
                .collect();

            crate::utils::fuzzy_actions(siv, query_actions, move |siv, action_text| {
                {
                    log::trace!("Triggering {:?} (from query row submit)", action_text);

                    let mut context = context.lock().unwrap();
                    if let Some(action) = context
                        .view_actions
                        .iter()
                        .find(|x| x.description.text == action_text)
                    {
                        context.pending_view_callback = Some(action.callback.clone());
                    }
                }
                siv.on_event(Event::Refresh);
            });
        });

        if matches!(processes_type, Type::LastQueryLog) {
            table.add_column(QueriesColumn::QueryEnd, "end", |c| c.width_min_max(19, 25));
            table.sort_by(QueriesColumn::QueryEnd, Ordering::Greater);
        } else {
            table.sort_by(QueriesColumn::Elapsed, Ordering::Greater);
        }

        let view_options = context.lock().unwrap().options.view.clone();

        if !view_options.no_subqueries {
            table.insert_column(0, QueriesColumn::SubQueries, "Q#", |c| c.width_min_max(2, 5));
        }

        // Only show hostname column when in cluster mode AND no host filter is active
        let (cluster, selected_host) = {
            let ctx = context.lock().unwrap();
            (ctx.options.clickhouse.cluster.is_some(), ctx.selected_host.clone())
        };
        if cluster && selected_host.is_none() {
            table.insert_column(0, QueriesColumn::HostName, "host", |c| c.width_min_max(4, 16));
        }

        let bg_runner_cv = context.lock().unwrap().background_runner_cv.clone();
        let bg_runner_force = context.lock().unwrap().background_runner_force.clone();
        let mut bg_runner = BackgroundRunner::new(delay, bg_runner_cv, bg_runner_force);
        bg_runner.start(update_callback);

        let processes_view = QueriesView {
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
            clipboard: None,
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

        //
        // NOTE: Place most common first
        //
        add_action!(context, &mut event_view, "Query logs", 'l', action_show_query_logs);
        add_action!(context, &mut event_view, "Query live flamegraph", 'L', action_show_flamegraph(true, None));
        add_action!(context, &mut event_view, "Query profile events", action_query_profile_events);
        add_action!(context, &mut event_view, "Query details", action_query_details);
        add_action!(context, &mut event_view, "Query CPU flamegraph", 'C', action_show_flamegraph(true, Some(TraceType::CPU)));
        add_action!(context, &mut event_view, "Query Real flamegraph", 'R', action_show_flamegraph(true, Some(TraceType::Real)));
        add_action!(context, &mut event_view, "Query memory flamegraph", 'M', action_show_flamegraph(true, Some(TraceType::Memory)));
        add_action!(context, &mut event_view, "Query memory sample flamegraph", action_show_flamegraph(true, Some(TraceType::MemorySample)));
        add_action!(context, &mut event_view, "Query jemalloc sample flamegraph", action_show_flamegraph(true, Some(TraceType::JemallocSample)));
        add_action!(context, &mut event_view, "Query MemoryAllocatedWithoutCheck flamegraph", action_show_flamegraph(true, Some(TraceType::MemoryAllocatedWithoutCheck)));
        add_action!(context, &mut event_view, "Query events flamegraph", action_show_flamegraph(true, Some(TraceType::ProfileEvents)));
        add_action!(context, &mut event_view, "Edit query and execute", Event::AltChar('E'), action_edit_query_and_execute);
        add_action!(context, &mut event_view, "Show query", 'S', action_show_query);
        add_action!(context, &mut event_view, "Copy query to clipboard", 'y', action_copy_query);
        add_action!(context, &mut event_view, "EXPLAIN SYNTAX", 's', action_explain_syntax);
        add_action!(context, &mut event_view, "EXPLAIN PLAN", 'e', action_explain_plan);
        add_action!(context, &mut event_view, "EXPLAIN PIPELINE", 'E', action_explain_pipeline);
        context.add_view_action(&mut event_view, "Filter", '/', move |_v| {
            return Ok(Some(EventResult::Consumed(Some(Callback::from_fn(
                move |siv: &mut Cursive| {
                    let filter_cb = move |siv: &mut Cursive, text: &str| {
                        siv.call_on_name(view_name, |v: &mut OnEventView<QueriesView>| {
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
                    show_bottom_prompt(siv, "/", filter_cb);
                },
            )))));
        });
        add_action!(context, &mut event_view, "Select", ' ', action_select);
        add_action!(context, &mut event_view, "Show all queries", '-', action_show_all_queries);
        // It is handy to use "Shift-" after "Shift+" to go back, instead of just "-"
        add_action!(context, &mut event_view, "Show all queries", '_', action_show_all_queries);
        add_action!(context, &mut event_view, "Show queries on shards", '+', action_show_queries_on_shards);
        add_action!(context, &mut event_view, "Query processors", 'P', action_query_processors);
        add_action!(context, &mut event_view, "Query views", 'v', action_query_views);
        add_action!(context, &mut event_view, "Share Query CPU flamegraph", action_show_flamegraph(false, Some(TraceType::CPU)));
        add_action!(context, &mut event_view, "Share Query Real flamegraph", action_show_flamegraph(false, Some(TraceType::Real)));
        add_action!(context, &mut event_view, "Share Query memory flamegraph", action_show_flamegraph(false, Some(TraceType::Memory)));
        add_action!(context, &mut event_view, "Share Query memory sample flamegraph", action_show_flamegraph(false, Some(TraceType::MemorySample)));
        add_action!(context, &mut event_view, "Share Query jemalloc sample flamegraph", action_show_flamegraph(false, Some(TraceType::JemallocSample)));
        add_action!(context, &mut event_view, "Share Query MemoryAllocatedWithoutCheck flamegraph", action_show_flamegraph(false, Some(TraceType::MemoryAllocatedWithoutCheck)));
        add_action!(context, &mut event_view, "Share Query events flamegraph", action_show_flamegraph(false, Some(TraceType::ProfileEvents)));
        add_action!(context, &mut event_view, "Share Query live flamegraph", action_show_flamegraph(false, None));
        add_action!(context, &mut event_view, "EXPLAIN INDEXES", 'I', action_explain_indexes);
        add_action!(context, &mut event_view, "EXPLAIN PIPELINE graph=1 (open in browser)", 'G', action_explain_pipeline_graph);
        add_action!(context, &mut event_view, "KILL query", 'K', action_kill_query);
        add_action!(context, &mut event_view, "Increase number of queries to render to 20", '(', action_increase_limit);
        add_action!(context, &mut event_view, "Decrease number of queries to render to 20", ')', action_decrease_limit);
        return event_view;
    }
}

impl Drop for QueriesView {
    fn drop(&mut self) {
        log::debug!("Removing views actions");
        self.context.lock().unwrap().view_actions.clear();
    }
}

// TODO: remove this extra wrapping
impl ViewWrapper for QueriesView {
    wrap_impl_no_move!(self.table: TableView<Query, QueriesColumn>);
}
