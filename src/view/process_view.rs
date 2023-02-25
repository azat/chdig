use crate::interpreter::QueryProcess;
use cursive::{
    direction::Direction,
    event::{Event, EventResult},
    vec::Vec2,
    view::{CannotFocus, View},
    Printer, Rect,
};
use cursive_table_view::{TableView, TableViewItem};
use humantime::format_duration;
use size::{Base, SizeFormatter, Style};
use std::cmp::Ordering;
use std::time::Duration;

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum QueryProcessDetailsColumn {
    Name,
    Current,
    Rate,
}
#[derive(Clone, Debug)]
struct QueryProcessDetails {
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
impl TableViewItem<QueryProcessDetailsColumn> for QueryProcessDetails {
    fn to_column(&self, column: QueryProcessDetailsColumn) -> String {
        let fmt_bytes = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);
        // FIXME: more humanable size formatter for non-bytes like
        let fmt_rows = SizeFormatter::new()
            .with_base(Base::Base10)
            .with_style(Style::Abbreviated);

        match column {
            QueryProcessDetailsColumn::Name => self.name.clone(),
            QueryProcessDetailsColumn::Current => {
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
            QueryProcessDetailsColumn::Rate => {
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

    fn cmp(&self, other: &Self, column: QueryProcessDetailsColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            QueryProcessDetailsColumn::Name => self.name.cmp(&other.name),
            QueryProcessDetailsColumn::Current => self.current.cmp(&other.current),
            QueryProcessDetailsColumn::Rate => self.rate.total_cmp(&other.rate),
        }
    }
}

pub struct ProcessView {
    table: TableView<QueryProcessDetails, QueryProcessDetailsColumn>,
}

impl ProcessView {
    pub fn new(query_process: QueryProcess) -> Self {
        let mut table = TableView::<QueryProcessDetails, QueryProcessDetailsColumn>::new()
            .column(QueryProcessDetailsColumn::Name, "Name", |c| c.width(30))
            .column(QueryProcessDetailsColumn::Current, "Current", |c| {
                return c.width(12);
            })
            .column(QueryProcessDetailsColumn::Rate, "Per second rate", |c| {
                c.width(18)
            });

        let mut items = Vec::new();
        for pe in query_process.profile_events {
            items.push(QueryProcessDetails {
                name: pe.0,
                current: pe.1,
                rate: pe.1 as f64 / query_process.elapsed,
            });
        }
        table.set_items(items);

        table.sort_by(QueryProcessDetailsColumn::Current, Ordering::Greater);
        table.set_selected_row(0);

        return ProcessView { table };
    }
}

impl View for ProcessView {
    fn draw(&self, printer: &Printer) {
        self.table.draw(printer);
    }

    fn layout(&mut self, size: Vec2) {
        self.table.layout(size);
    }

    fn take_focus(&mut self, direction: Direction) -> Result<EventResult, CannotFocus> {
        return self.table.take_focus(direction);
    }

    fn on_event(&mut self, event: Event) -> EventResult {
        return self.table.on_event(event);
    }

    fn important_area(&self, size: Vec2) -> Rect {
        return self.table.important_area(size);
    }
}
