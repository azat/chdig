use crate::interpreter::QueryProcess;
use crate::view::{ExtTableView, TableViewItem};
use cursive::{view::ViewWrapper, wrap_impl};
use humantime::format_duration;
use size::{Base, SizeFormatter, Style};
use std::cmp::Ordering;
use std::time::Duration;

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum QueryProcessDetailsColumn {
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
    table: ExtTableView<QueryProcessDetails, QueryProcessDetailsColumn>,
}

impl ProcessView {
    pub fn new(query_process: QueryProcess) -> Self {
        let mut table = ExtTableView::<QueryProcessDetails, QueryProcessDetailsColumn>::default();
        let inner_table = table.get_inner_mut();
        inner_table.add_column(QueryProcessDetailsColumn::Name, "Name", |c| c.width(30));
        inner_table.add_column(QueryProcessDetailsColumn::Current, "Current", |c| {
            return c.width(12);
        });
        inner_table.add_column(QueryProcessDetailsColumn::Rate, "Per second rate", |c| {
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
        inner_table.set_items(items);

        inner_table.sort_by(QueryProcessDetailsColumn::Current, Ordering::Greater);
        inner_table.set_selected_row(0);

        return ProcessView { table };
    }
}

impl ViewWrapper for ProcessView {
    wrap_impl!(self.table: ExtTableView<QueryProcessDetails, QueryProcessDetailsColumn>);
}
