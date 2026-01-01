use crate::interpreter::Query;
use crate::view::TableViewItem;
use crate::view::table_view::TableView;
use cursive::theme::{BaseColor, Color, ColorStyle};
use cursive::traits::Nameable;
use cursive::utils::markup::StyledString;
use cursive::views::{NamedView, OnEventView};
use cursive::{Cursive, view::ViewWrapper, wrap_impl};
use humantime::format_duration;
use size::{Base, SizeFormatter, Style};
use std::cmp::Ordering;
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
// - human print
// - colored print
// - auto refresh
// - implement loadavg like with moving average
impl QueryProcessDetails {
    fn format_value(&self, value: u64) -> String {
        let fmt_bytes = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);
        let fmt_rows = SizeFormatter::new()
            .with_base(Base::Base10)
            .with_style(Style::Abbreviated);

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

    fn format_rate(&self, rate: f64) -> String {
        let fmt_bytes = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);
        let fmt_rows = SizeFormatter::new()
            .with_base(Base::Base10)
            .with_style(Style::Abbreviated);

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
            QueryDetailsColumn::Current => self.current.cmp(&other.current),
            QueryDetailsColumn::Rate => self.rate.total_cmp(&other.rate),
            QueryDetailsColumn::QueryValue(idx) => {
                let self_val = self.query_values.get(idx).copied().unwrap_or(0);
                let other_val = other.query_values.get(idx).copied().unwrap_or(0);
                self_val.cmp(&other_val)
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
            let mut styled = StyledString::new();
            styled.append_styled(
                text,
                ColorStyle::new(Color::Dark(BaseColor::Red), Color::TerminalDefault),
            );
            styled
        } else if should_highlight_diff {
            let mut styled = StyledString::new();
            styled.append_styled(
                text,
                ColorStyle::new(Color::Dark(BaseColor::Green), Color::TerminalDefault),
            );
            styled
        } else {
            StyledString::plain(text)
        }
    }
}

pub struct QueryView {
    table: TableView<QueryProcessDetails, QueryDetailsColumn>,
    all_items: Vec<QueryProcessDetails>,
    filter: Arc<Mutex<String>>,
}

impl QueryView {
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

    pub fn new(query: Query, view_name: &'static str) -> NamedView<OnEventView<Self>> {
        Self::new_internal(vec![query], view_name)
    }

    pub fn new_diff(queries: Vec<Query>, view_name: &'static str) -> NamedView<OnEventView<Self>> {
        Self::new_internal(queries, view_name)
    }

    fn new_internal(queries: Vec<Query>, view_name: &'static str) -> NamedView<OnEventView<Self>> {
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

        // Collect all profile event names
        let mut all_event_names = std::collections::HashSet::new();
        for query in &queries {
            for name in query.profile_events.keys() {
                all_event_names.insert(name.clone());
            }
        }

        let mut items = Vec::new();

        // Add query duration as a special profile event (only in diff view)
        if is_diff_view {
            let mut query_values = Vec::new();
            let mut max_duration = 0_u64;

            for query in &queries {
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

        // Add all other profile events
        for event_name in all_event_names {
            let mut query_values = Vec::new();
            let mut max_value = 0_u64;

            for query in &queries {
                let value = query.profile_events.get(&event_name).copied().unwrap_or(0);
                query_values.push(value);
                max_value = max_value.max(value);
            }

            let rate = if !queries.is_empty() {
                max_value as f64 / queries[0].elapsed
            } else {
                0.0
            };

            items.push(QueryProcessDetails {
                name: event_name,
                current: max_value,
                rate,
                is_diff: is_diff_view,
                query_values,
            });
        }
        table.set_items(items.clone());

        table.sort_by(QueryDetailsColumn::Current, Ordering::Greater);
        table.set_selected_row(0);

        let filter = Arc::new(Mutex::new(String::new()));

        let view = QueryView {
            table,
            all_items: items,
            filter: filter.clone(),
        };

        let event_view = OnEventView::new(view).on_event('/', move |siv: &mut Cursive| {
            let filter_cb = move |siv: &mut Cursive, text: &str| {
                siv.call_on_name(view_name, |v: &mut NamedView<OnEventView<QueryView>>| {
                    let mut event_view = v.get_mut();
                    let query_view = event_view.get_inner_mut();
                    *query_view.filter.lock().unwrap() = text.to_string();
                    query_view.apply_filter();
                });
                siv.pop_layer();
            };

            crate::view::show_bottom_prompt(siv, "/", filter_cb);
        });

        return event_view.with_name(view_name);
    }
}

impl ViewWrapper for QueryView {
    wrap_impl!(self.table: TableView<QueryProcessDetails, QueryDetailsColumn>);
}
