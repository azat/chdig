//
// Port of src/view/queries_view.rs onto the in-repo retained ratatui
// component framework (src/tui).
//

use anyhow::{Error, Result};
use chrono::{DateTime, Local, TimeDelta};
use ratatui::layout::{Rect, Size};
use size::{Base, SizeFormatter, Style as SizeStyle};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::sync::{Arc, Mutex};

use crate::common::RelativeDateTime;
use crate::interpreter::{
    BackgroundRunner, ContextArc, Query, TextLogArguments, WorkerEvent,
    clickhouse::{Columns, QueriesFilter, TraceType},
    options::ViewOptions,
};
use crate::tui::app::App;
use crate::tui::component::{Canvas, Component, DummyView, Nameable, NamedView, OnEventView};
use crate::tui::dialog::Dialog;
use crate::tui::event::{Event, EventResult};
use crate::tui::linear::LinearLayout;
use crate::tui::navigation::Navigation;
use crate::tui::prompt::show_bottom_prompt;
use crate::tui::resize::{Resizable, SizeConstraint};
use crate::tui::scroll::Scrollable;
use crate::tui::style::{Color, Modifier, Style, StyledString};
use crate::tui::text::TextView;
use crate::tui::views::query_view::QueryView;
use crate::tui::views::sql_query_view::{Row as QueryResultRow, SQLQueryView, Unit};
use crate::tui::views::table_view::{TableColumn, TableView, TableViewItem};
use crate::tui::views::text_log_view::TextLogView;
use crate::utils::{edit_query, find_common_hostname_prefix_and_suffix, get_query};

// ClickHouse may flush some system.* tables after system.query_log, likely it is only a precision
// error, so 1 second should be enough.
const QUERY_TIME_DRIFT_BUFFER_SECONDS: i64 = 1;

// count() OVER (PARTITION BY initial_query_id)
type QueryKey = (String, String); // (query_id, host_name)

fn query_key(q: &Query) -> QueryKey {
    (q.query_id.clone(), q.host_name.clone())
}

fn queries_count_subqueries(queries: &mut HashMap<QueryKey, Query>) {
    // <(initial_query_id, host_name), count()>
    let mut subqueries = HashMap::<(String, String), u64>::new();
    for v in queries.values() {
        *subqueries
            .entry((v.initial_query_id.clone(), v.host_name.clone()))
            .or_default() += 1;
    }
    for v in queries.values_mut() {
        v.subqueries = subqueries[&(v.initial_query_id.clone(), v.host_name.clone())];
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
// if(is_initial_query, (sumMap(ProfileEvents) OVER (PARTITION BY initial_query_id, host_name)), ProfileEvents)
fn queries_sum_profile_events(queries: &mut HashMap<QueryKey, Query>) {
    // <(initial_query_id, host_name), sumMap(ProfileEvents)>
    // Arc entries: a query without subqueries (the common case) shares its
    // map, only groups that actually aggregate allocate a summed copy.
    let mut profile_events = HashMap::<(String, String), Arc<HashMap<String, u64>>>::new();
    for v in queries.values() {
        let key = (v.initial_query_id.clone(), v.host_name.clone());
        if let Some(pe) = profile_events.get_mut(&key) {
            *pe = Arc::new(sum_map(pe, &v.profile_events));
        } else {
            profile_events.insert(key, Arc::clone(&v.profile_events));
        }
    }
    for v in queries.values_mut() {
        if v.is_initial_query
            && let Some(pe) = profile_events.get(&(v.initial_query_id.clone(), v.host_name.clone()))
        {
            v.profile_events = Arc::clone(pe);
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
    NormalizedQueryHash,
    IsCancelled,
    InitialUser,
    Database,
    LogComment,
    Exception,
    Query,
}

/// Stable label for each user-configurable queries column. Matches the header
/// strings passed to `TableView::add_column` so the settings dialog can show
/// exactly what the user sees in the table. `Selection` is excluded — it is
/// toggled implicitly when the user selects rows.
pub fn query_column_id(column: QueriesColumn) -> Option<&'static str> {
    Some(match column {
        QueriesColumn::Selection => return None,
        QueriesColumn::HostName => "host",
        QueriesColumn::SubQueries => "Q#",
        QueriesColumn::Cpu => "cpu",
        QueriesColumn::IOWait => "io_wait",
        QueriesColumn::CPUWait => "cpu_wait",
        QueriesColumn::User => "user",
        QueriesColumn::Threads => "thr",
        QueriesColumn::Memory => "mem",
        QueriesColumn::DiskIO => "disk",
        QueriesColumn::IO => "io",
        QueriesColumn::NetIO => "net",
        QueriesColumn::Elapsed => "elapsed",
        QueriesColumn::QueryEnd => "end",
        QueriesColumn::QueryId => "query_id",
        QueriesColumn::NormalizedQueryHash => "qhash",
        QueriesColumn::IsCancelled => "cancel",
        QueriesColumn::InitialUser => "init_user",
        QueriesColumn::Database => "db",
        QueriesColumn::LogComment => "log_comment",
        QueriesColumn::Exception => "exception",
        QueriesColumn::Query => "query",
    })
}

/// All user-configurable queries columns, in their natural display order.
pub const AVAILABLE_QUERY_COLUMNS: &[QueriesColumn] = &[
    QueriesColumn::HostName,
    QueriesColumn::SubQueries,
    QueriesColumn::QueryId,
    QueriesColumn::NormalizedQueryHash,
    QueriesColumn::Cpu,
    QueriesColumn::IOWait,
    QueriesColumn::CPUWait,
    QueriesColumn::User,
    QueriesColumn::Threads,
    QueriesColumn::Memory,
    QueriesColumn::DiskIO,
    QueriesColumn::IO,
    QueriesColumn::NetIO,
    QueriesColumn::Elapsed,
    QueriesColumn::IsCancelled,
    QueriesColumn::InitialUser,
    QueriesColumn::Database,
    QueriesColumn::LogComment,
    QueriesColumn::Exception,
    QueriesColumn::Query,
    QueriesColumn::QueryEnd,
];

fn is_query_column_visible(visible: &[String], label: &str) -> bool {
    visible.iter().any(|h| h == label)
}

/// Non-capturing closures coerce to `fn` pointers, so this stays a `const` slice.
pub type ColumnWidth = fn(TableColumn<QueriesColumn>) -> TableColumn<QueriesColumn>;

/// Main queries-view columns, in display order, paired with their width policy.
/// Excludes columns that have non-trivial placement: `HostName`/`SubQueries`
/// are prepended at index 0, `Selection` is toggled dynamically.
#[rustfmt::skip]
pub const QUERY_COLUMNS_WIDTH: &[(QueriesColumn, ColumnWidth)] = &[
    (QueriesColumn::QueryId,     |c| c.width_min_max(8, 16)),
    (QueriesColumn::NormalizedQueryHash, |c| c.width_min_max(5, 20)),
    (QueriesColumn::Cpu,         |c| c.width_min_max(3, 8)),
    (QueriesColumn::IOWait,      |c| c.width_min_max(7, 11)),
    (QueriesColumn::CPUWait,     |c| c.width_min_max(8, 12)),
    (QueriesColumn::User,        |c| c.width_min_max(4, 12)),
    (QueriesColumn::Threads,     |c| c.width_min_max(3, 6)),
    (QueriesColumn::Memory,      |c| c.width_min_max(3, 8)),
    (QueriesColumn::DiskIO,      |c| c.width_min_max(4, 8)),
    (QueriesColumn::IO,          |c| c.width_min_max(2, 8)),
    (QueriesColumn::NetIO,       |c| c.width_min_max(3, 8)),
    (QueriesColumn::Elapsed,     |c| c.width_min_max(7, 11)),
    (QueriesColumn::IsCancelled, |c| c.width_min_max(1, 6)),
    (QueriesColumn::InitialUser, |c| c.width_min_max(4, 16)),
    (QueriesColumn::Database,    |c| c.width_min_max(2, 16)),
    (QueriesColumn::LogComment,  |c| c.width_min_max(8, 32)),
    (QueriesColumn::Exception,   |c| c.width_min_max(8, 40)),
    (QueriesColumn::Query,       |c| c.width_min(20)),
    (QueriesColumn::QueryEnd,    |c| c.width_min_max(19, 25)),
];

impl PartialEq<Query> for Query {
    fn eq(&self, other: &Self) -> bool {
        return self.query_id == other.query_id && self.host_name == other.host_name;
    }
}

impl TableViewItem<QueriesColumn> for Query {
    fn to_column(&self, column: QueriesColumn) -> String {
        let formatter = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(SizeStyle::Abbreviated);

        match column {
            QueriesColumn::Selection => {
                if self.selection {
                    "x".to_string()
                } else {
                    " ".to_string()
                }
            }
            QueriesColumn::HostName => self
                .display_host_name
                .as_deref()
                .unwrap_or(&self.host_name)
                .to_string(),
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
            QueriesColumn::NormalizedQueryHash => self.normalized_query_hash.to_string(),
            QueriesColumn::IsCancelled => {
                if self.is_cancelled {
                    "x".to_string()
                } else {
                    " ".to_string()
                }
            }
            QueriesColumn::InitialUser => self.initial_user.clone(),
            QueriesColumn::Database => self.current_database.clone(),
            QueriesColumn::LogComment => self
                .settings
                .get("log_comment")
                .cloned()
                .unwrap_or_default(),
            QueriesColumn::Exception => self.exception.replace('\n', " "),
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
            QueriesColumn::NormalizedQueryHash => {
                self.normalized_query_hash.cmp(&other.normalized_query_hash)
            }
            QueriesColumn::IsCancelled => self.is_cancelled.cmp(&other.is_cancelled),
            QueriesColumn::InitialUser => self.initial_user.cmp(&other.initial_user),
            QueriesColumn::Database => self.current_database.cmp(&other.current_database),
            QueriesColumn::LogComment => self
                .settings
                .get("log_comment")
                .cmp(&other.settings.get("log_comment")),
            QueriesColumn::Exception => self.exception.cmp(&other.exception),
            QueriesColumn::Query => self.normalized_query.cmp(&other.normalized_query),
        }
    }

    fn to_column_styled(&self, column: QueriesColumn) -> StyledString {
        let text = self.to_column(column);
        // Cancelled is exception_code = 394 — keep it as the more specific case so
        // it stays yellow rather than blending into the generic red exception row.
        let color = if self.is_cancelled {
            Some(Color::Yellow)
        } else if !self.exception.is_empty() {
            Some(Color::Red)
        } else {
            None
        };
        if color.is_none() && !self.is_new {
            return StyledString::plain(text);
        }
        let mut style = Style::default();
        if let Some(color) = color {
            style = style.fg(color);
        }
        if self.is_new {
            style = style.add_modifier(Modifier::BOLD);
        }
        StyledString::styled(text, style)
    }
}

pub struct QueriesView {
    context: ContextArc,
    table: TableView<Query, QueriesColumn>,
    items: HashMap<QueryKey, Query>,
    // Suppresses is_new highlighting on the very first update().
    loaded: bool,
    // For show only specific query
    query_id: Option<String>,
    // For multi selection
    selected_query_ids: HashSet<QueryKey>,
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
    view_name: Arc<str>,

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
    pub fn update(&mut self, processes: Columns) -> Result<()> {
        let prev_items = take(&mut self.items);

        // Selected queries should be updated, since in the new query list it may not be exists
        // already
        let mut new_selected_query_ids = HashSet::new();

        for i in 0..processes.row_count() {
            let mut query = Query::from_clickhouse_block(&processes, i, self.is_system_processes)?;

            let key = query_key(&query);
            if self.selected_query_ids.contains(&key) {
                new_selected_query_ids.insert(key.clone());
            }

            if let Some(prev_item) = prev_items.get(&key) {
                query.prev_elapsed = Some(prev_item.elapsed);
                query.prev_profile_events = Some(prev_item.profile_events.clone());
            } else if self.loaded {
                query.is_new = true;
            }

            self.items.insert(key, query);
        }

        queries_count_subqueries(&mut self.items);
        if !self.options.no_subqueries {
            queries_sum_profile_events(&mut self.items);
        }

        self.selected_query_ids = new_selected_query_ids;
        self.loaded = true;
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

        // Compute stripped hostname for display (to_column uses display_host_name)
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

                    item.display_host_name = Some(hostname.to_string());
                }
            }
        }

        self.table.set_items_stable(items);
        self.sync_selection();
    }

    /// Sync the marker column and per-item selection flags with
    /// selected_query_ids on the items already in the table. Kept separate
    /// from update_view so that toggling a selection does not pay for a full
    /// item rebuild (deep-cloning every Query).
    fn sync_selection(&mut self) {
        if self.selected_query_ids.is_empty() {
            if self.has_selection_column {
                self.table.remove_column(0);
                self.has_selection_column = false;
            }
            return;
        }
        if !self.has_selection_column {
            self.table
                .insert_column(0, QueriesColumn::Selection, "v", |c| c.width(1));
            self.has_selection_column = true;
        }
        let ids = &self.selected_query_ids;
        for item in self.table.borrow_items_mut() {
            item.selection = ids.contains(&query_key(item));
        }
        // The flags feed the sort only for this column.
        if let Some((QueriesColumn::Selection, _)) = self.table.order() {
            self.table.sort();
        }
    }

    fn show_flamegraph(&mut self, trace_type: Option<TraceType>) -> Result<()> {
        let (query_ids, min_query_start_microseconds, max_query_end_microseconds) =
            self.get_query_ids()?;
        let mut context_locked = self.context.lock().unwrap();
        if let Some(trace_type) = trace_type {
            context_locked.worker.send(
                true,
                WorkerEvent::QueryFlameGraph(
                    trace_type,
                    min_query_start_microseconds,
                    max_query_end_microseconds,
                    query_ids,
                ),
            );
        } else {
            context_locked.worker.send(
                true,
                WorkerEvent::LiveQueryFlameGraph(Some(query_ids), None),
            );
        }

        return Ok(());
    }

    fn show_flamegraph_diff(&mut self, trace_type: TraceType) -> Result<()> {
        let (groups, min_query_start_microseconds, max_query_end_microseconds) =
            self.get_query_id_groups()?;
        if groups.len() != 2 {
            return Err(Error::msg(format!(
                "Flamegraph diff requires exactly 2 queries selected with <Space>, got {}",
                groups.len()
            )));
        }
        let mut groups_iter = groups.into_iter();
        let query_ids_a = groups_iter.next().unwrap();
        let query_ids_b = groups_iter.next().unwrap();
        let mut context_locked = self.context.lock().unwrap();
        context_locked.worker.send(
            true,
            WorkerEvent::QueryFlameGraphDiff(
                trace_type,
                min_query_start_microseconds,
                max_query_end_microseconds,
                query_ids_a,
                query_ids_b,
            ),
        );

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
                // (query_id, host_name) not (initial_query_id, host_name), while we are
                // curious about both
                let key = query_key(q);
                let initial_key = (q.initial_query_id.clone(), q.host_name.clone());
                if self.selected_query_ids.contains(&initial_key)
                    || self.selected_query_ids.contains(&key)
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

    /// Group selected queries by their initial_query_id so each logical distributed
    /// query becomes a single group of constituent query_ids. Preserves the selection
    /// order: the group whose initial_query_id first appears among the selected rows
    /// comes first.
    fn get_query_id_groups(
        &self,
    ) -> Result<(Vec<Vec<String>>, DateTime<Local>, Option<DateTime<Local>>)> {
        if self.selected_query_ids.len() < 2 {
            return Err(Error::msg(
                "Select at least 2 queries with <Space> to diff their flamegraphs",
            ));
        }

        // Dedup initial_query_ids for the selected rows, keeping insertion order so the
        // diff is deterministic (first-selected is "before", next "after").
        let mut initial_query_ids: Vec<String> = Vec::new();
        for q in self.items.values() {
            let key = query_key(q);
            let initial_key = (q.initial_query_id.clone(), q.host_name.clone());
            if (self.selected_query_ids.contains(&initial_key)
                || self.selected_query_ids.contains(&key))
                && !initial_query_ids.contains(&q.initial_query_id)
            {
                initial_query_ids.push(q.initial_query_id.clone());
            }
        }

        let mut min_start: Option<DateTime<Local>> = None;
        let mut max_end: Option<DateTime<Local>> = None;
        let mut groups: Vec<Vec<String>> = Vec::with_capacity(initial_query_ids.len());
        for iqid in &initial_query_ids {
            let mut group = Vec::new();
            for q in self.items.values() {
                if &q.initial_query_id != iqid {
                    continue;
                }
                group.push(q.query_id.clone());
                min_start = Some(match min_start {
                    Some(cur) => cur.min(q.query_start_time_microseconds),
                    None => q.query_start_time_microseconds,
                });
                if !self.is_system_processes {
                    max_end = Some(match max_end {
                        Some(cur) => cur.max(q.query_end_time_microseconds),
                        None => q.query_end_time_microseconds,
                    });
                }
            }
            if !group.is_empty() {
                groups.push(group);
            }
        }

        let min_start = min_start.ok_or_else(|| Error::msg("No queries matched selection"))?;
        return Ok((groups, min_start, max_end));
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
            .ui_sink
            .send(Box::new(move |app: &mut App| {
                app.present_logs(
                    "query_log",
                    "Logs:",
                    NamedView::new(
                        "query_log",
                        TextLogView::new(
                            "query_log",
                            context_copy,
                            TextLogArguments {
                                query_ids_subquery: None,
                                query_ids: Some(query_ids),
                                logger_names: None,
                                hostname: None,
                                message_filter: None,
                                max_level: None,
                                limit: None,
                                start: min_query_start_microseconds,
                                end: RelativeDateTime::from(max_query_end_microseconds),
                            },
                        ),
                    ),
                );
            }))
            .unwrap();
        Ok(Some(EventResult::consumed()))
    }

    fn action_show_flamegraph(
        &mut self,
        trace_type: Option<TraceType>,
    ) -> Result<Option<EventResult>> {
        self.show_flamegraph(trace_type)?;
        Ok(Some(EventResult::consumed()))
    }

    fn action_show_flamegraph_diff(
        &mut self,
        trace_type: TraceType,
    ) -> Result<Option<EventResult>> {
        self.show_flamegraph_diff(trace_type)?;
        Ok(Some(EventResult::consumed()))
    }

    fn action_query_profile_events(&mut self) -> Result<Option<EventResult>> {
        // Check if multiple queries are selected
        if self.selected_query_ids.len() > 1 {
            // Get the queries for diff view
            let queries: Vec<Query> = self
                .items
                .values()
                .filter(|q| self.selected_query_ids.contains(&query_key(q)))
                .cloned()
                .collect();

            if queries.is_empty() {
                return Err(Error::msg("No queries selected"));
            }

            self.context
                .lock()
                .unwrap()
                .ui_sink
                .send(Box::new(move |app: &mut App| {
                    app.add_layer(
                        Dialog::around(
                            QueryView::new_diff(queries, "process")
                                .resized(SizeConstraint::AtLeast(120), SizeConstraint::AtLeast(35)),
                        )
                        .title("Profile Events Diff"),
                    );
                }))
                .unwrap();
        } else {
            let selected_query = self.get_selected_query()?;
            let context = self.context.clone();
            self.context
                .lock()
                .unwrap()
                .ui_sink
                .send(Box::new(move |app: &mut App| {
                    app.add_layer(
                        Dialog::around(
                            QueryView::new(selected_query, "process", &context)
                                .resized(SizeConstraint::AtLeast(120), SizeConstraint::AtLeast(35)),
                        )
                        .title("Profile Events"),
                    );
                }))
                .unwrap();
        }
        Ok(Some(EventResult::consumed()))
    }

    fn action_query_details(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        Ok(Some(EventResult::with_cb_once(move |app: &mut App| {
            app.add_layer(Dialog::info(selected_query.to_string()).title("Details"));
        })))
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

        Ok(Some(EventResult::with_cb_once(|app: &mut App| {
            app.complete_clear()
        })))
    }

    fn action_show_query(&mut self) -> Result<Option<EventResult>> {
        let selected_query = self.get_selected_query()?;
        let query = selected_query.original_query.clone();
        let database = selected_query.current_database.clone();
        let settings = selected_query.settings.clone();

        let query = get_query(&query, &settings);
        let query = format!("USE {};\n{}", database, query);
        let query = crate::tui::highlight_sql(&query)
            .unwrap_or_else(|_| StyledString::plain(query.clone()));

        self.context
            .lock()
            .unwrap()
            .ui_sink
            .send(Box::new(move |app: &mut App| {
                app.add_layer(Dialog::around(
                    LinearLayout::vertical()
                        .child(TextView::new("Query:").center())
                        .child(DummyView.fixed_height(1))
                        .child(TextView::new(query).scrollable()),
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
                    return Ok(Some(EventResult::with_cb_once(move |app: &mut App| {
                        app.add_layer(Dialog::info(format!("Failed to copy to clipboard: {}", e)));
                    })));
                }
                self.clipboard = Some(clipboard);
            }
            Err(e) => {
                return Ok(Some(EventResult::with_cb_once(move |app: &mut App| {
                    app.add_layer(Dialog::info(format!("Failed to access clipboard: {}", e)));
                })));
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
        let key = query_key(&selected_query);

        if self.selected_query_ids.contains(&key) {
            self.selected_query_ids.remove(&key);
        } else {
            self.selected_query_ids.insert(key);
        }
        self.sync_selection();

        Ok(Some(EventResult::consumed()))
    }

    fn action_select_all(&mut self) -> Result<Option<EventResult>> {
        let keys: Vec<QueryKey> = self.table.borrow_items().iter().map(query_key).collect();

        // Toggle: if all visible rows are already selected, deselect them
        if !keys.is_empty() && keys.iter().all(|k| self.selected_query_ids.contains(k)) {
            for key in &keys {
                self.selected_query_ids.remove(key);
            }
        } else {
            self.selected_query_ids.extend(keys);
        }
        self.sync_selection();

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
            WorkerEvent::ExplainPipelineShareGraph(database, query),
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
            .ui_sink
            .send(Box::new(move |app: &mut App| {
                app.add_layer(
                    Dialog::new()
                        .title(format!(
                            "Are you sure you want to KILL QUERY with query_id = {}",
                            query_id
                        ))
                        .button("Yes, I'm sure", move |app| {
                            context_copy
                                .lock()
                                .unwrap()
                                .worker
                                .send(true, WorkerEvent::KillQuery(query_id.clone()));
                            app.pop_layer();
                        })
                        .button("Cancel", |app| {
                            app.pop_layer();
                        }),
                );
            }))
            .unwrap();
        Ok(Some(EventResult::consumed()))
    }

    fn action_export_perfetto(&mut self) -> Result<Option<EventResult>> {
        let (query_ids, min_query_start_microseconds, max_query_end_microseconds) =
            self.get_query_ids()?;

        let query_ids_set: HashSet<&String> = HashSet::from_iter(query_ids.iter());
        let queries: Vec<_> = self
            .items
            .values()
            .filter(|q| query_ids_set.contains(&q.query_id))
            .cloned()
            .collect();

        let mut context_locked = self.context.lock().unwrap();
        context_locked.worker.send(
            true,
            WorkerEvent::PerfettoExport(
                queries.into(),
                query_ids,
                min_query_start_microseconds,
                max_query_end_microseconds,
            ),
        );
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
            .get_log_table_name(table);

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
            .ui_sink
            .send(Box::new(move |app: &mut App| {
                app.add_layer(Dialog::around(
                    LinearLayout::vertical()
                        .child(TextView::new("Processors:").center())
                        .child(DummyView.fixed_height(1))
                        .child(
                            SQLQueryView::new(
                                context_copy,
                                table,
                                sort_by,
                                columns,
                                vec!["name"],
                                vec!["name"],
                                query,
                            )
                            .unwrap_or_else(|_| panic!("Cannot get {}", table))
                            .with_name(table)
                            .resized(SizeConstraint::AtLeast(160), SizeConstraint::AtLeast(40)),
                        ),
                ));
            }))
            .unwrap();

        Ok(Some(EventResult::consumed()))
    }

    /// SQL bounds of the selected queries: `fromUnixTimestamp64Nano()` of the
    /// earliest start, of the latest end plus the drift buffer (for the
    /// `event_time <= toDateTime()` filters, which truncate to seconds), and
    /// of the latest end itself (for bucketing); `now64(6)` for both ends
    /// while the queries are still running (processes view).
    fn query_ids_window(&self) -> Result<(Vec<String>, String, String, String)> {
        let (query_ids, min_start, max_end) = self.get_query_ids()?;
        let nanos = |dt: DateTime<Local>| -> Result<String> {
            Ok(format!(
                "fromUnixTimestamp64Nano({})",
                dt.timestamp_nanos_opt().ok_or(Error::msg("Invalid time"))?
            ))
        };
        let start = nanos(min_start)?;
        let (end_buffered, end) = match max_end {
            Some(end) => (
                nanos(end + TimeDelta::seconds(QUERY_TIME_DRIFT_BUFFER_SECONDS))?,
                nanos(end)?,
            ),
            None => ("now64(6)".to_string(), "now64(6)".to_string()),
        };
        Ok((query_ids, start, end_buffered, end))
    }

    fn action_query_metric_log(&mut self) -> Result<Option<EventResult>> {
        let (query_ids, start_sql, end_sql, bucket_end_sql) = self.query_ids_window()?;
        let (_, min_start, max_end) = self.get_query_ids()?;
        let dbtable = self
            .context
            .lock()
            .unwrap()
            .clickhouse
            .get_log_table_name("query_metric_log");
        let ids_filter = format!("query_id IN ('{}')", query_ids.join("','"));
        // One row per second at most, so a short query gets a bucket per second
        let span_seconds = (max_end.unwrap_or_else(Local::now) - min_start).num_seconds();
        let buckets = span_seconds.clamp(1, 16);

        // Same shape as the Metric log view: ProfileEvent_* are per-interval
        // deltas (summed), memory_usage/peak_memory_usage are gauges (averaged,
        // max per sparkline bucket); the buckets span the query lifetime
        // (without the drift buffer, which for a short query would push all
        // rows into the middle bucket).
        let query = format!(
            r#"
            WITH {start} AS start_, {end} AS end_, {bucket_end} AS bucket_end_
            SELECT
                name,
                value,
                max,
                dyn,
                if(arrayMax(heights_) <= 0,
                   repeat('▁', {buckets}),
                   arrayStringConcat(
                       arrayMap(
                           h -> ['▁','▂','▃','▄','▅','▆','▇','█'][toUInt32(least(8, greatest(1, ceil(h / arrayMax(heights_) * 8))))],
                           heights_),
                       '')) AS spark
            FROM
            (
                SELECT
                    pair_.1 AS name,
                    startsWith(name, 'ProfileEvent_') AS is_delta_,
                    if(is_delta_, sum(pair_.2), avg(pair_.2)) AS value,
                    max(pair_.2) AS max,
                    if(avg(pair_.2) != 0, stddevPop(pair_.2) / abs(avg(pair_.2)), 0) AS dyn,
                    if(is_delta_, sumMap(map(bucket_, pair_.2)), maxMap(map(bucket_, pair_.2))) AS m_,
                    arrayMap(i -> m_[toUInt16(i)], range({buckets})) AS heights_
                FROM
                (
                    SELECT
                        arrayJoin(arrayConcat(
                            CAST(tupleToNameValuePairs(tuple(COLUMNS('^ProfileEvent_'))), 'Array(Tuple(String, Float64))'),
                            [('memory_usage', toFloat64(memory_usage)), ('peak_memory_usage', toFloat64(peak_memory_usage))]
                        )) AS pair_,
                        toUInt16(least({buckets} - 1, floor((toUnixTimestamp64Micro(event_time_microseconds) - toUnixTimestamp64Micro(start_)) * {buckets} / greatest(1, toUnixTimestamp64Micro(bucket_end_) - toUnixTimestamp64Micro(start_))))) AS bucket_
                    FROM {dbtable}
                    WHERE
                        event_date BETWEEN toDate(start_) AND toDate(end_) AND
                        event_time BETWEEN toDateTime(start_) AND toDateTime(end_) AND
                        {ids_filter}
                )
                GROUP BY name
                HAVING max != 0
            )
            SETTINGS enable_named_columns_in_function_tuple=1
            "#,
            start = start_sql,
            end = end_sql,
            bucket_end = bucket_end_sql,
            buckets = buckets,
            dbtable = dbtable,
            ids_filter = ids_filter,
        );

        let view_name = "query_metric_log_dialog";
        let columns = vec!["name", "value", "max", "dyn", "spark"];
        let title = format!("Metric log: {}", query_ids.join(", "));
        let chart_start = RelativeDateTime::Absolute(min_start);
        let chart_end = max_end.map_or(RelativeDateTime::Now, |end| {
            RelativeDateTime::Absolute(end + TimeDelta::seconds(QUERY_TIME_DRIFT_BUFFER_SECONDS))
        });
        let context = self.context.clone();
        self.context
            .lock()
            .unwrap()
            .ui_sink
            .send(Box::new(move |app: &mut App| {
                let mut view = SQLQueryView::new(
                    context,
                    view_name,
                    "dyn",
                    columns,
                    vec!["name"],
                    vec!["name"],
                    query,
                )
                .unwrap_or_else(|_| panic!("Cannot create {}", view_name));
                view.get_inner_mut().set_title(title);
                view.get_inner_mut().set_on_submit(
                    move |app: &mut App, columns: Vec<&'static str>, row: QueryResultRow| {
                        let Some(name) = columns
                            .iter()
                            .zip(row.0.iter())
                            .find_map(|(c, r)| (*c == "name").then(|| r.to_string()))
                        else {
                            return;
                        };
                        crate::tui::views::providers::show_metric_chart_range(
                            app,
                            "query_metric_log",
                            format!("avg(`{}`)", name),
                            Some(ids_filter.clone()),
                            name,
                            chart_start.clone(),
                            chart_end.clone(),
                        );
                    },
                );
                app.add_layer(Dialog::around(
                    view.with_name(view_name)
                        .resized(SizeConstraint::AtLeast(120), SizeConstraint::AtLeast(35)),
                ));
            }))
            .unwrap();

        Ok(Some(EventResult::consumed()))
    }

    fn action_query_threads(&mut self) -> Result<Option<EventResult>> {
        let (query_ids, start_sql, end_sql, _) = self.query_ids_window()?;
        let dbtable = self
            .context
            .lock()
            .unwrap()
            .clickhouse
            .get_log_table_name("query_thread_log");
        let ids_filter = format!("query_id IN ('{}')", query_ids.join("','"));

        // One row per thread attachment (a pool thread re-attached to the same
        // query logs again), so the row identity includes its finish time.
        let columns = vec![
            "thread_name",
            "thread_id",
            "master",
            "finished",
            "elapsed",
            "cpu",
            "io_wait",
            "cpu_wait",
            "read_rows",
            "read_bytes",
            "written_rows",
            "written_bytes",
            "peak_mem",
            "query_id",
        ];
        let query = format!(
            r#"
            WITH {start} AS start_, {end} AS end_
            SELECT
                thread_name,
                toUInt32(thread_id) AS thread_id,
                toUInt32(master_thread_id) AS master,
                event_time AS finished,
                query_duration_ms AS elapsed,
                if(elapsed > 0, ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e3 / elapsed * 100, 0) AS cpu,
                if(elapsed > 0, ProfileEvents['OSIOWaitMicroseconds'] / 1e3 / elapsed * 100, 0) AS io_wait,
                if(elapsed > 0, ProfileEvents['OSCPUWaitMicroseconds'] / 1e3 / elapsed * 100, 0) AS cpu_wait,
                read_rows,
                read_bytes,
                written_rows,
                written_bytes,
                peak_memory_usage AS peak_mem,
                query_id
            FROM {dbtable}
            WHERE
                event_date BETWEEN toDate(start_) AND toDate(end_) AND
                event_time BETWEEN toDateTime(start_) AND toDateTime(end_) AND
                {ids_filter}
            ORDER BY cpu DESC
            "#,
            start = start_sql,
            end = end_sql,
            dbtable = dbtable,
            ids_filter = ids_filter,
        );

        let view_name = "query_thread_log_dialog";
        let title = format!("Threads: {}", query_ids.join(", "));
        let context = self.context.clone();
        let events_context = self.context.clone();
        self.context
            .lock()
            .unwrap()
            .ui_sink
            .send(Box::new(move |app: &mut App| {
                let mut view = SQLQueryView::new(
                    context,
                    view_name,
                    "cpu",
                    columns,
                    vec!["query_id", "thread_id", "finished"],
                    vec!["thread_name"],
                    query,
                )
                .unwrap_or_else(|_| panic!("Cannot create {}", view_name));
                {
                    let v = view.get_inner_mut();
                    v.set_title(title);
                    v.set_value_unit("elapsed", Unit::Milliseconds);
                    v.set_value_unit("read_rows", Unit::Count);
                    v.set_value_unit("written_rows", Unit::Count);
                    v.set_value_unit("read_bytes", Unit::Bytes);
                    v.set_value_unit("written_bytes", Unit::Bytes);
                    v.set_value_unit("peak_mem", Unit::Bytes);
                    v.set_on_submit(
                        move |app: &mut App, columns: Vec<&'static str>, row: QueryResultRow| {
                            show_thread_profile_events(
                                app,
                                events_context.clone(),
                                &dbtable,
                                &start_sql,
                                &end_sql,
                                columns,
                                row,
                            );
                        },
                    );
                }
                app.add_layer(Dialog::around(
                    view.with_name(view_name)
                        .resized(SizeConstraint::AtLeast(160), SizeConstraint::AtLeast(35)),
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
            .get_log_table_name(table);

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
            .ui_sink
            .send(Box::new(move |app: &mut App| {
                app.add_layer(Dialog::around(
                    LinearLayout::vertical()
                        .child(TextView::new("Views:").center())
                        .child(DummyView.fixed_height(1))
                        .child(
                            SQLQueryView::new(
                                context_copy,
                                table,
                                sort_by,
                                columns,
                                vec!["view_name"],
                                vec!["view_name"],
                                query,
                            )
                            .unwrap_or_else(|_| panic!("Cannot get {}", table))
                            .with_name(table)
                            .resized(SizeConstraint::AtLeast(160), SizeConstraint::AtLeast(40)),
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
        view_name: &str,
        title: &str,
    ) -> OnEventView<Self> {
        let view_name: Arc<str> = Arc::from(view_name);

        // Macro to simplify adding view actions
        macro_rules! add_action {
            // With shortcut and method arguments
            ($ctx:expr, $view:expr, $desc:expr, $shortcut:expr, $method:ident($($args:expr),*)) => {
                $ctx.add_view_action($view, view_name.clone(), $desc, $shortcut, |v| {
                    v.downcast_mut::<QueriesView>().unwrap().$method($($args),*)
                })
            };
            // Without shortcut but with method arguments
            ($ctx:expr, $view:expr, $desc:expr, $method:ident($($args:expr),*)) => {
                $ctx.add_view_action_without_shortcut($view, view_name.clone(), $desc, |v| {
                    v.downcast_mut::<QueriesView>().unwrap().$method($($args),*)
                })
            };
            // With shortcut (char or Event), no arguments
            ($ctx:expr, $view:expr, $desc:expr, $shortcut:expr, $method:ident) => {
                $ctx.add_view_action($view, view_name.clone(), $desc, $shortcut, |v| {
                    v.downcast_mut::<QueriesView>().unwrap().$method()
                })
            };
            // Without shortcut, no arguments
            ($ctx:expr, $view:expr, $desc:expr, $method:ident) => {
                $ctx.add_view_action_without_shortcut($view, view_name.clone(), $desc, |v| {
                    v.downcast_mut::<QueriesView>().unwrap().$method()
                })
            };
        }

        let delay = context.lock().unwrap().options.view.delay_interval;

        let is_system_processes = matches!(processes_type, Type::ProcessList);
        let filter = context.lock().unwrap().queries_filter(&view_name);
        let limit = context.lock().unwrap().queries_limit.clone();

        let event_owner = context.lock().unwrap().worker.event_owner();
        let update_callback_context = context.clone();
        let update_callback_filter = filter.clone();
        let update_callback_limit = limit.clone();
        let update_callback_process_type = processes_type.clone();
        let update_callback_view_name = view_name.clone();
        let update_callback = move |force: bool| {
            let view_name = &update_callback_view_name;
            let mut context = update_callback_context.lock().unwrap();
            let filter = QueriesFilter {
                like: update_callback_filter.lock().unwrap().clone(),
                query_kind: context.view_query_kind(view_name),
            };
            let limit = context.view_limit(view_name, *update_callback_limit.lock().unwrap());

            let (start_time, end_time) = context.view_interval(view_name);

            match update_callback_process_type {
                Type::ProcessList => context.worker.send_owned(
                    &event_owner,
                    force,
                    WorkerEvent::ProcessList(view_name.clone(), filter, limit),
                ),
                Type::SlowQueryLog => context.worker.send_owned(
                    &event_owner,
                    force,
                    WorkerEvent::SlowQueryLog(view_name.clone(), filter, start_time, end_time, limit),
                ),
                Type::LastQueryLog => context.worker.send_owned(
                    &event_owner,
                    force,
                    WorkerEvent::LastQueryLog(view_name.clone(), filter, start_time, end_time, limit),
                ),
            }
        };

        let enabled_cols = context.lock().unwrap().options.view.query_columns.clone();
        let visible = |col: QueriesColumn| -> bool {
            match query_column_id(col) {
                Some(label) => is_query_column_visible(&enabled_cols, label),
                None => true,
            }
        };

        let is_last_query_log = matches!(processes_type, Type::LastQueryLog);
        let mut table = TableView::<Query, QueriesColumn>::new();
        for &(col, width) in QUERY_COLUMNS_WIDTH {
            // QueryEnd is only useful for the LastQueryLog view.
            if col == QueriesColumn::QueryEnd && !is_last_query_log {
                continue;
            }
            if !visible(col) {
                continue;
            }
            let Some(label) = query_column_id(col) else {
                continue;
            };
            table.add_column(col, label, width);
        }
        // Keep the options in sync on column removal via middle mouse press,
        // so that the settings dialog (F3) shows the column as hidden
        table.set_on_remove_column(|app, col| {
            let Some(label) = query_column_id(col) else {
                return;
            };
            let context = app.user_data::<ContextArc>().unwrap().clone();
            context
                .lock()
                .unwrap()
                .options
                .view
                .query_columns
                .retain(|c| c != label);
        });
        let submit_view_name = view_name.clone();
        table.set_on_submit(move |app, _row, _index| {
            let context = app.user_data::<ContextArc>().unwrap().clone();
            let query_actions = context
                .lock()
                .unwrap()
                .view_actions
                .iter()
                .filter(|x| x.owner == submit_view_name)
                .map(|x| &x.description)
                .cloned()
                .collect();

            let view_name = submit_view_name.clone();
            crate::tui::fuzzy_actions(app, query_actions, move |app, action_text| {
                log::trace!("Triggering {:?} (from query row submit)", action_text);

                // Replay the action's event through the regular event flow
                // (the handler lives in this view's OnEventView).
                let event = context
                    .lock()
                    .unwrap()
                    .view_actions
                    .iter()
                    .find(|x| x.description.text == action_text && x.owner == view_name)
                    .map(|x| x.description.event.clone());
                if let Some(event) = event {
                    app.on_event(event);
                }
            });
        });

        let preferred_sort = if is_last_query_log {
            QueriesColumn::QueryEnd
        } else {
            QueriesColumn::Elapsed
        };

        let view_options = context.lock().unwrap().options.view.clone();

        if !view_options.no_subqueries && visible(QueriesColumn::SubQueries) {
            table.insert_column(0, QueriesColumn::SubQueries, "Q#", |c| c.width_min_max(2, 5));
        }

        // Only show hostname column when in cluster mode AND no host filter is active
        let (cluster, selected_host) = {
            let ctx = context.lock().unwrap();
            (ctx.options.clickhouse.cluster.is_some(), ctx.selected_host.clone())
        };
        if cluster && selected_host.is_none() && visible(QueriesColumn::HostName) {
            table.insert_column(0, QueriesColumn::HostName, "host", |c| c.width_min_max(4, 16));
        }

        // Apply sort: fall back to first registered column if the preferred one was hidden.
        let sort_target = if visible(preferred_sort) {
            Some(preferred_sort)
        } else {
            AVAILABLE_QUERY_COLUMNS.iter().copied().find(|c| visible(*c))
        };
        if let Some(col) = sort_target {
            table.sort_by(col, Ordering::Greater);
        }

        table.set_title(title);

        let bg_runner_cv = context.lock().unwrap().background_runner_cv.clone();
        let bg_runner_generation = context.lock().unwrap().background_runner_generation.clone();
        let mut bg_runner = BackgroundRunner::new(delay, bg_runner_cv, bg_runner_generation);
        bg_runner.start(update_callback);

        let processes_view = QueriesView {
            context: context.clone(),
            table,
            items: HashMap::new(),
            loaded: false,
            query_id: None,
            selected_query_ids: HashSet::new(),
            has_selection_column: false,
            options: view_options,
            is_system_processes,
            filter,
            limit,
            clipboard: None,
            view_name: view_name.clone(),
            bg_runner,
        };

        // TODO:
        // - pause/disable the table if the foreground view had been changed
        // - space - multiquery selection (KILL, flamegraphs, logs, ...)
        let mut event_view = OnEventView::new(processes_view);

        log::debug!("Adding views actions");
        let mut context = context.lock().unwrap();

        //
        // NOTE: Place most common first
        //
        add_action!(context, &mut event_view, "Query logs", 'l', action_show_query_logs);
        add_action!(context, &mut event_view, "Query live flamegraph", 'L', action_show_flamegraph(None));
        add_action!(context, &mut event_view, "Query profile events", action_query_profile_events);
        add_action!(context, &mut event_view, "Query details", action_query_details);
        add_action!(context, &mut event_view, "Query CPU flamegraph", action_show_flamegraph(Some(TraceType::CPU)));
        add_action!(context, &mut event_view, "Query Real flamegraph", action_show_flamegraph(Some(TraceType::Real)));
        add_action!(context, &mut event_view, "Query memory flamegraph", action_show_flamegraph(Some(TraceType::Memory)));
        add_action!(context, &mut event_view, "Query memory sample flamegraph", action_show_flamegraph(Some(TraceType::MemorySample)));
        add_action!(context, &mut event_view, "Query jemalloc sample flamegraph", action_show_flamegraph(Some(TraceType::JemallocSample)));
        add_action!(context, &mut event_view, "Query MemoryAllocatedWithoutCheck flamegraph", action_show_flamegraph(Some(TraceType::MemoryAllocatedWithoutCheck)));
        add_action!(context, &mut event_view, "Query events flamegraph", action_show_flamegraph(Some(TraceType::ProfileEvent)));
        add_action!(context, &mut event_view, "Export to Perfetto", action_export_perfetto);
        add_action!(context, &mut event_view, "Edit query and execute", Event::AltChar('E'), action_edit_query_and_execute);
        add_action!(context, &mut event_view, "Show query", 'S', action_show_query);
        add_action!(context, &mut event_view, "Copy query to clipboard", 'y', action_copy_query);
        add_action!(context, &mut event_view, "EXPLAIN SYNTAX", 's', action_explain_syntax);
        add_action!(context, &mut event_view, "EXPLAIN PLAN", 'e', action_explain_plan);
        add_action!(context, &mut event_view, "EXPLAIN PIPELINE", 'E', action_explain_pipeline);
        let filter_view_name = view_name.clone();
        context.add_view_action(&mut event_view, view_name.clone(), "Filter", '/', move |_v| {
            let view_name = filter_view_name.clone();
            return Ok(Some(EventResult::with_cb(move |app: &mut App| {
                let view_name = view_name.clone();
                let filter_cb = move |app: &mut App, text: &str| {
                    app.call_on_name(&view_name, |v: &mut OnEventView<QueriesView>| {
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
                    app.pop_layer();
                };
                show_bottom_prompt(app, "/", filter_cb);
            })));
        });
        add_action!(context, &mut event_view, "Select", ' ', action_select);
        add_action!(context, &mut event_view, "Select all (toggle)", 'A', action_select_all);
        add_action!(context, &mut event_view, "Show all queries", '-', action_show_all_queries);
        // It is handy to use "Shift-" after "Shift+" to go back, instead of just "-"
        add_action!(context, &mut event_view, "Show all queries", '_', action_show_all_queries);
        add_action!(context, &mut event_view, "Show queries on shards", '+', action_show_queries_on_shards);
        add_action!(context, &mut event_view, "Query processors", action_query_processors);
        add_action!(context, &mut event_view, "Query views", action_query_views);
        add_action!(context, &mut event_view, "Query metric log", action_query_metric_log);
        add_action!(context, &mut event_view, "Query threads", action_query_threads);
        add_action!(context, &mut event_view, "Query CPU flamegraph diff (select 2 with <Space>)", action_show_flamegraph_diff(TraceType::CPU));
        add_action!(context, &mut event_view, "Query Real flamegraph diff (select 2 with <Space>)", action_show_flamegraph_diff(TraceType::Real));
        add_action!(context, &mut event_view, "Query memory flamegraph diff (select 2 with <Space>)", action_show_flamegraph_diff(TraceType::Memory));
        add_action!(context, &mut event_view, "Query memory sample flamegraph diff (select 2 with <Space>)", action_show_flamegraph_diff(TraceType::MemorySample));
        add_action!(context, &mut event_view, "Query jemalloc sample flamegraph diff (select 2 with <Space>)", action_show_flamegraph_diff(TraceType::JemallocSample));
        add_action!(context, &mut event_view, "Query MemoryAllocatedWithoutCheck flamegraph diff (select 2 with <Space>)", action_show_flamegraph_diff(TraceType::MemoryAllocatedWithoutCheck));
        add_action!(context, &mut event_view, "Query events flamegraph diff (select 2 with <Space>)", action_show_flamegraph_diff(TraceType::ProfileEvent));
        add_action!(context, &mut event_view, "EXPLAIN INDEXES", 'I', action_explain_indexes);
        add_action!(context, &mut event_view, "EXPLAIN PIPELINE graph=1 (share)", 'G', action_explain_pipeline_graph);
        add_action!(context, &mut event_view, "KILL query", 'K', action_kill_query);
        add_action!(context, &mut event_view, "Increase number of queries to render to 20", '(', action_increase_limit);
        add_action!(context, &mut event_view, "Decrease number of queries to render to 20", ')', action_decrease_limit);
        return event_view;
    }
}

/// ProfileEvents of one thread of the query (summed over its attachments).
fn show_thread_profile_events(
    app: &mut App,
    context: ContextArc,
    dbtable: &str,
    start_sql: &str,
    end_sql: &str,
    columns: Vec<&'static str>,
    row: QueryResultRow,
) {
    let field = |name: &str| {
        columns
            .iter()
            .zip(row.0.iter())
            .find_map(|(c, r)| (*c == name).then(|| r.to_string()))
    };
    let (Some(query_id), Some(thread_id)) = (field("query_id"), field("thread_id")) else {
        return;
    };
    let Ok(thread_id) = thread_id.parse::<u32>() else {
        return;
    };
    let query = format!(
        r#"
        WITH {start} AS start_, {end} AS end_
        SELECT event_name_ AS name, sum(event_value_) AS value
        FROM {dbtable}
        ARRAY JOIN mapKeys(ProfileEvents) AS event_name_, mapValues(ProfileEvents) AS event_value_
        WHERE
            event_date BETWEEN toDate(start_) AND toDate(end_) AND
            event_time BETWEEN toDateTime(start_) AND toDateTime(end_) AND
            query_id = '{query_id}' AND thread_id = {thread_id} AND event_value_ != 0
        GROUP BY name
        ORDER BY value DESC
        "#,
        start = start_sql,
        end = end_sql,
        dbtable = dbtable,
        query_id = query_id.replace('\\', "\\\\").replace('\'', "\\'"),
        thread_id = thread_id,
    );
    let view_name = "query_thread_profile_events";
    let mut view = SQLQueryView::new(
        context,
        view_name,
        "value",
        vec!["name", "value"],
        vec!["name"],
        vec!["name"],
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));
    view.get_inner_mut()
        .set_title(format!("Thread {} profile events", thread_id));
    app.add_layer(Dialog::around(
        view.with_name(view_name)
            .resized(SizeConstraint::AtLeast(80), SizeConstraint::AtLeast(30)),
    ));
}

impl Drop for QueriesView {
    fn drop(&mut self) {
        log::debug!("Removing {} view actions", self.view_name);
        // Only own actions: with panes the replacement view is constructed
        // (and registers its actions) before this view is dropped.
        self.context
            .lock()
            .unwrap()
            .view_actions
            .retain(|a| a.owner != self.view_name);
    }
}

impl Component for QueriesView {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        self.table.draw(canvas, area, focused);
    }

    fn required_size(&mut self, max: Size) -> Size {
        self.table.required_size(max)
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        self.table.on_event(event)
    }

    fn take_focus(&mut self) -> bool {
        true
    }

    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        f(&mut self.table);
    }
}
