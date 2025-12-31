use crate::interpreter::Query;
use crate::view::TableViewItem;
use crate::view::table_view::TableView;
use cursive::traits::Nameable;
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
}
#[derive(Clone, Debug)]
pub struct QueryProcessDetails {
    name: String,
    current: u64,
    rate: f64,
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
impl TableViewItem<QueryDetailsColumn> for QueryProcessDetails {
    fn to_column(&self, column: QueryDetailsColumn) -> String {
        let fmt_bytes = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);
        // FIXME: more humanable size formatter for non-bytes like
        let fmt_rows = SizeFormatter::new()
            .with_base(Base::Base10)
            .with_style(Style::Abbreviated);

        match column {
            QueryDetailsColumn::Name => self.name.clone(),
            QueryDetailsColumn::Current => {
                if self.name.contains("Microseconds") {
                    return format!("{}", format_duration(Duration::from_micros(self.current)));
                }
                if self.name.contains("Millisecond") {
                    return format!("{}", format_duration(Duration::from_millis(self.current)));
                }
                if self.name.contains("Ns") {
                    return format!("{}", format_duration(Duration::from_nanos(self.current)));
                }
                if self.name.contains("Bytes") || self.name.contains("Chars") {
                    return fmt_bytes.format(self.current as i64);
                }
                if self.current > 1_000 {
                    return fmt_rows.format(self.current as i64);
                }
                return self.current.to_string();
            }
            QueryDetailsColumn::Rate => {
                if self.name.contains("Microseconds") {
                    return format!(
                        "{}/s",
                        format_duration(Duration::from_micros(self.rate as u64))
                    );
                }
                if self.name.contains("Millisecond") {
                    return format!(
                        "{}/s",
                        format_duration(Duration::from_millis(self.rate as u64))
                    );
                }
                if self.name.contains("Ns") {
                    return format!(
                        "{}/s",
                        format_duration(Duration::from_nanos(self.rate as u64))
                    );
                }
                if self.name.contains("Bytes") || self.name.contains("Chars") {
                    return fmt_bytes.format(self.rate as i64) + "/s";
                }
                if self.rate > 1e3 {
                    return fmt_rows.format(self.rate as i64) + "/s";
                }
                return format!("{:.2}", self.rate);
            }
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
        let mut table = TableView::<QueryProcessDetails, QueryDetailsColumn>::new();
        table.add_column(QueryDetailsColumn::Name, "Name", |c| c.width_min(20));
        table.add_column(QueryDetailsColumn::Current, "Current", |c| {
            return c.width_min_max(7, 12);
        });
        table.add_column(QueryDetailsColumn::Rate, "Per second rate", |c| {
            c.width_min_max(16, 20)
        });

        let mut items = Vec::new();
        for pe in query.profile_events {
            items.push(QueryProcessDetails {
                name: pe.0,
                current: pe.1,
                rate: pe.1 as f64 / query.elapsed,
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
