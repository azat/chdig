// Port of src/view/query_view.rs onto the in-repo ratatui component
// framework (src/tui). Callers arrive with the queries view port.

use crate::interpreter::{BackgroundRunner, ContextArc, EventOwner, Query, WorkerEvent};
use crate::tui::app::App;
use crate::tui::component::{Canvas, Component, Nameable, NamedView, OnEventView};
use crate::tui::event::{Event, EventResult};
use crate::tui::style::{Color, StyledString};
use crate::tui::views::table_view::{TableView, TableViewItem};
use humantime::format_duration;
use ratatui::layout::{Rect, Size};
use size::{Base, SizeFormatter, Style as SizeStyle};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum QueryDetailsColumn {
    Name,
    Current,
    Rate,
    // Dynamic columns for diff view: Q1, Q2, ..., QN
    QueryValue(usize),
}
#[derive(Clone, Debug)]
pub struct QueryProcessDetails {
    name: String,
    current: u64,
    rate: f64,
    // Flag to indicate if this is a diff value that should be highlighted
    is_diff: bool,
    // Values from multiple queries (for diff view)
    query_values: Vec<u64>,
}

impl PartialEq<QueryProcessDetails> for QueryProcessDetails {
    fn eq(&self, other: &Self) -> bool {
        return *self.name == other.name;
    }
}

// TODO:
// - colored print
// - implement loadavg like with moving average
impl QueryProcessDetails {
    fn format_value(&self, value: u64) -> String {
        let fmt_bytes = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(SizeStyle::Abbreviated);
        let fmt_rows = SizeFormatter::new()
            .with_base(Base::Base10)
            .with_style(SizeStyle::Abbreviated);

        if self.name.contains("Microseconds") {
            format!("{}", format_duration(Duration::from_micros(value)))
        } else if self.name.contains("Millisecond") {
            format!("{}", format_duration(Duration::from_millis(value)))
        } else if self.name.contains("Ns") || self.name.contains("Nanoseconds") {
            format!("{}", format_duration(Duration::from_nanos(value)))
        } else if self.name.contains("Bytes") || self.name.contains("Chars") {
            fmt_bytes.format(value as i64)
        } else if value > 1_000 {
            fmt_rows.format(value as i64)
        } else {
            value.to_string()
        }
    }

    // Time events are displayed in normalized units, so they should be compared in normalized
    // units as well (nanoseconds), otherwise i.e. FooNanoseconds will be sorted above
    // BarMicroseconds simply due to bigger raw value.
    fn unit_multiplier(&self) -> u64 {
        if self.name.contains("Microseconds") {
            1_000
        } else if self.name.contains("Millisecond") {
            1_000_000
        } else {
            1
        }
    }

    fn normalized_value(&self, value: u64) -> u64 {
        value.saturating_mul(self.unit_multiplier())
    }

    fn format_rate(&self, rate: f64) -> String {
        let fmt_bytes = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(SizeStyle::Abbreviated);
        let fmt_rows = SizeFormatter::new()
            .with_base(Base::Base10)
            .with_style(SizeStyle::Abbreviated);

        if self.name.contains("Microseconds") {
            format!("{}/s", format_duration(Duration::from_micros(rate as u64)))
        } else if self.name.contains("Millisecond") {
            format!("{}/s", format_duration(Duration::from_millis(rate as u64)))
        } else if self.name.contains("Ns") || self.name.contains("Nanoseconds") {
            format!("{}/s", format_duration(Duration::from_nanos(rate as u64)))
        } else if self.name.contains("Bytes") || self.name.contains("Chars") {
            fmt_bytes.format(rate as i64) + "/s"
        } else if rate > 1e3 {
            fmt_rows.format(rate as i64) + "/s"
        } else {
            format!("{:.2}", rate)
        }
    }
}

impl TableViewItem<QueryDetailsColumn> for QueryProcessDetails {
    fn to_column(&self, column: QueryDetailsColumn) -> String {
        match column {
            QueryDetailsColumn::Name => self.name.clone(),
            QueryDetailsColumn::QueryValue(idx) => {
                if idx < self.query_values.len() {
                    self.format_value(self.query_values[idx])
                } else {
                    String::new()
                }
            }
            QueryDetailsColumn::Current => self.format_value(self.current),
            QueryDetailsColumn::Rate => self.format_rate(self.rate),
        }
    }

    fn cmp(&self, other: &Self, column: QueryDetailsColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            QueryDetailsColumn::Name => self.name.cmp(&other.name),
            QueryDetailsColumn::Current => self
                .normalized_value(self.current)
                .cmp(&other.normalized_value(other.current)),
            QueryDetailsColumn::Rate => (self.rate * self.unit_multiplier() as f64)
                .total_cmp(&(other.rate * other.unit_multiplier() as f64)),
            QueryDetailsColumn::QueryValue(idx) => {
                let self_val = self.query_values.get(idx).copied().unwrap_or(0);
                let other_val = other.query_values.get(idx).copied().unwrap_or(0);
                self.normalized_value(self_val)
                    .cmp(&other.normalized_value(other_val))
            }
        }
    }

    fn to_column_styled(&self, column: QueryDetailsColumn) -> StyledString {
        let text = self.to_column(column);

        // Highlight based on different conditions
        let should_highlight_miss =
            matches!(column, QueryDetailsColumn::Name) && self.name.to_lowercase().contains("miss");

        // For diff view, highlight QueryValue columns where values differ
        let should_highlight_diff = if self.is_diff {
            if let QueryDetailsColumn::QueryValue(idx) = column {
                // Check if this value differs from others
                if let Some(&current_val) = self.query_values.get(idx) {
                    // Check if any other value is different
                    self.query_values.iter().any(|&v| v != current_val)
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if should_highlight_miss {
            StyledString::styled(text, Color::Red)
        } else if should_highlight_diff {
            StyledString::styled(text, Color::Green)
        } else {
            StyledString::plain(text)
        }
    }
}

pub struct QueryView {
    table: TableView<QueryProcessDetails, QueryDetailsColumn>,
    all_items: Vec<QueryProcessDetails>,
    filter: Arc<Mutex<String>>,
    /// The followed running query, None for finished queries and diffs.
    tracked: Option<Query>,
    /// Initial queries show the sum over their subqueries, as the queries view does.
    sum_subqueries: bool,
    /// Periodic QueryProfileEvents requests; dropping it (with the popup, or
    /// once the query is gone) stops the runner and cancels the in-flight one.
    live: Option<(BackgroundRunner, Arc<EventOwner>)>,
}

fn title_for(query: &Query, finished: bool) -> String {
    let elapsed = format_duration(Duration::from_secs(query.elapsed as u64));
    if finished {
        format!("{} (finished, {})", query.query_id, elapsed)
    } else {
        format!("{} (running, {})", query.query_id, elapsed)
    }
}

/// Per-second rate of a counter: over the last refresh interval for a running
/// query with a previous snapshot (like top), the query-lifetime average otherwise.
fn event_rate(query: &Query, name: &str, current: u64) -> f64 {
    if query.running
        && let (Some(prev_events), Some(prev_elapsed)) =
            (&query.prev_profile_events, query.prev_elapsed)
    {
        let interval = query.elapsed - prev_elapsed;
        if interval > 0. {
            let prev = prev_events.get(name).copied().unwrap_or(0);
            // Initial queries carry the sum over their subqueries, which shrinks
            // when a subquery finishes, hence saturating.
            return current.saturating_sub(prev) as f64 / interval;
        }
    }
    if query.elapsed > 0. {
        current as f64 / query.elapsed
    } else {
        0.
    }
}

fn build_items(queries: &[Query]) -> Vec<QueryProcessDetails> {
    let is_diff_view = queries.len() > 1;

    let mut all_event_names = std::collections::HashSet::new();
    for query in queries {
        for name in query.profile_events.keys() {
            all_event_names.insert(name.clone());
        }
    }

    let mut items = Vec::new();

    // Add query duration as a special profile event (only in diff view)
    if is_diff_view {
        let mut query_values = Vec::new();
        let mut max_duration = 0_u64;

        for query in queries {
            // Convert elapsed seconds to microseconds for consistency with other time metrics
            let duration_us = (query.elapsed * 1_000_000.0) as u64;
            query_values.push(duration_us);
            max_duration = max_duration.max(duration_us);
        }

        items.push(QueryProcessDetails {
            name: "QueryDurationMicroseconds".to_string(),
            current: max_duration,
            rate: 0.0, // Rate doesn't make sense for query duration
            is_diff: is_diff_view,
            query_values,
        });
    }

    for event_name in all_event_names {
        let mut query_values = Vec::new();
        let mut max_value = 0_u64;

        for query in queries {
            let value = query.profile_events.get(&event_name).copied().unwrap_or(0);
            query_values.push(value);
            max_value = max_value.max(value);
        }

        let rate = queries
            .first()
            .map_or(0., |q| event_rate(q, &event_name, max_value));

        items.push(QueryProcessDetails {
            name: event_name,
            current: max_value,
            rate,
            is_diff: is_diff_view,
            query_values,
        });
    }

    items
}

impl QueryView {
    /// Apply a get_process() result (the query and its subqueries). An empty
    /// result means the query is gone from system.processes: the last
    /// snapshot stays on screen and the updates stop.
    pub fn update(&mut self, query_id: &str, rows: Vec<Query>) {
        let Some(prev) = self.tracked.as_ref().filter(|q| q.query_id == query_id) else {
            return;
        };
        let Some(mut query) = rows.iter().find(|q| q.query_id == query_id).cloned() else {
            self.live = None;
            self.table.set_title(title_for(prev, true));
            self.tracked = None;
            return;
        };
        if self.sum_subqueries && query.is_initial_query {
            let mut sum = HashMap::new();
            for row in &rows {
                for (k, v) in row.profile_events.iter() {
                    *sum.entry(k.clone()).or_insert(0) += *v;
                }
            }
            query.profile_events = Arc::new(sum);
        }
        query.prev_elapsed = Some(prev.elapsed);
        query.prev_profile_events = Some(prev.profile_events.clone());

        self.table.set_title(title_for(&query, false));
        self.all_items = build_items(std::slice::from_ref(&query));
        self.apply_filter();
        self.tracked = Some(query);
    }

    fn apply_filter(&mut self) {
        let filter_text = self.filter.lock().unwrap().clone();
        let filter_lower = filter_text.to_lowercase();

        let filtered_items: Vec<QueryProcessDetails> = if filter_text.is_empty() {
            self.all_items.clone()
        } else {
            self.all_items
                .iter()
                .filter(|item| item.name.to_lowercase().contains(&filter_lower))
                .cloned()
                .collect()
        };

        self.table.set_items_stable(filtered_items);
    }

    pub fn new(
        query: Query,
        view_name: &'static str,
        context: &ContextArc,
    ) -> NamedView<OnEventView<Self>> {
        Self::new_internal(vec![query], view_name, Some(context))
    }

    pub fn new_diff(queries: Vec<Query>, view_name: &'static str) -> NamedView<OnEventView<Self>> {
        Self::new_internal(queries, view_name, None)
    }

    /// Periodic refresh of `query` (the first run is skipped: the caller's
    /// snapshot is fresh).
    fn start_live(query: &Query, context: &ContextArc) -> (BackgroundRunner, Arc<EventOwner>) {
        let (delay, cv, generation, owner) = {
            let ctx = context.lock().unwrap();
            (
                ctx.options.view.delay_interval,
                ctx.background_runner_cv.clone(),
                ctx.background_runner_generation.clone(),
                ctx.worker.event_owner(),
            )
        };
        let mut runner = BackgroundRunner::new(delay, cv, generation);
        let cb_context = context.clone();
        let cb_owner = owner.clone();
        let query_id = query.query_id.clone();
        let host_name = query.host_name.clone();
        let first = std::sync::atomic::AtomicBool::new(true);
        runner.start(move |force| {
            if first.swap(false, std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            cb_context.lock().unwrap().worker.send_owned(
                &cb_owner,
                force,
                WorkerEvent::QueryProfileEvents(query_id.clone(), host_name.clone()),
            );
        });
        (runner, owner)
    }

    fn new_internal(
        queries: Vec<Query>,
        view_name: &'static str,
        context: Option<&ContextArc>,
    ) -> NamedView<OnEventView<Self>> {
        let mut table = TableView::<QueryProcessDetails, QueryDetailsColumn>::new();
        table.add_column(QueryDetailsColumn::Name, "Name", |c| c.width_min(20));

        let is_diff_view = queries.len() > 1;

        if is_diff_view {
            // Add a column for each query
            for idx in 0..queries.len() {
                let col_name = if queries.len() <= 10 {
                    format!("q{}", idx + 1)
                } else {
                    format!("q{:02}", idx + 1)
                };
                table.add_column(QueryDetailsColumn::QueryValue(idx), &col_name, |c| {
                    c.width_min_max(7, 12)
                });
            }
        } else {
            table.add_column(QueryDetailsColumn::Current, "Current", |c| {
                c.width_min_max(7, 12)
            });
            table.add_column(QueryDetailsColumn::Rate, "Per second rate", |c| {
                c.width_min_max(16, 20)
            });
        }

        let items = build_items(&queries);
        table.set_items(items.clone());

        table.sort_by(QueryDetailsColumn::Current, Ordering::Greater);
        table.set_selected_row(0);

        let filter = Arc::new(Mutex::new(String::new()));

        let (tracked, live) = match (queries.as_slice(), context) {
            ([query], Some(context)) => {
                table.set_title(title_for(query, !query.running));
                if query.running {
                    (Some(query.clone()), Some(Self::start_live(query, context)))
                } else {
                    (None, None)
                }
            }
            _ => (None, None),
        };
        let sum_subqueries = context.is_some_and(|c| !c.lock().unwrap().options.view.no_subqueries);

        let view = QueryView {
            table,
            all_items: items,
            filter: filter.clone(),
            tracked,
            sum_subqueries,
            live,
        };

        let event_view = OnEventView::new(view).on_event('/', move |app: &mut App| {
            let filter_cb = move |app: &mut App, text: &str| {
                app.call_on_name(view_name, |event_view: &mut OnEventView<QueryView>| {
                    let query_view = event_view.get_inner_mut();
                    *query_view.filter.lock().unwrap() = text.to_string();
                    query_view.apply_filter();
                });
                app.pop_layer();
            };

            crate::tui::show_bottom_prompt(app, "/", filter_cb);
        });

        return event_view.with_name(view_name);
    }
}

impl Component for QueryView {
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
        self.table.take_focus()
    }

    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        f(&mut self.table);
    }

    fn focus_name(&mut self, name: &str) -> bool {
        self.table.focus_name(name)
    }
}
