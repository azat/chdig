use std::cmp::Ordering;

use anyhow::Result;

use crate::interpreter::{clickhouse::Columns, ContextArc, WorkerEvent};
use crate::view::{TableViewItem, UpdatingTableView};
use crate::wrap_impl_no_move;
use cursive::view::ViewWrapper;
use size::{Base, SizeFormatter, Style};

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum ReplicatedFetchesColumn {
    HostName,
    Database,
    Table,
    ResultPart,
    Elapsed,
    Progress,
    TotalSizeBytesCompressed,
    BytesReadCompressed,
}

#[derive(Clone, Debug)]
pub struct FetchEntry {
    pub host_name: String,
    pub database: String,
    pub table: String,
    pub result_part_name: String,
    pub elapsed: f64,
    pub progress: f64,
    pub total_size_bytes_compressed: u64,
    pub bytes_read_compressed: u64,
}
impl PartialEq<FetchEntry> for FetchEntry {
    fn eq(&self, other: &Self) -> bool {
        return self.host_name == other.host_name
            && self.database == other.database
            && self.table == other.table
            && self.result_part_name == other.result_part_name;
    }
}

impl TableViewItem<ReplicatedFetchesColumn> for FetchEntry {
    fn to_column(&self, column: ReplicatedFetchesColumn) -> String {
        let fmt_bytes = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);

        match column {
            ReplicatedFetchesColumn::HostName => self.host_name.clone(),
            ReplicatedFetchesColumn::Database => self.database.clone(),
            ReplicatedFetchesColumn::Table => self.table.clone(),
            ReplicatedFetchesColumn::ResultPart => self.result_part_name.clone(),
            ReplicatedFetchesColumn::Elapsed => format!("{:.2}", self.elapsed),
            ReplicatedFetchesColumn::Progress => format!("{:.2}", self.progress),
            ReplicatedFetchesColumn::TotalSizeBytesCompressed => {
                fmt_bytes.format(self.total_size_bytes_compressed as i64)
            }
            ReplicatedFetchesColumn::BytesReadCompressed => {
                fmt_bytes.format(self.bytes_read_compressed as i64)
            }
        }
    }

    fn cmp(&self, other: &Self, column: ReplicatedFetchesColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            ReplicatedFetchesColumn::HostName => self.host_name.cmp(&other.host_name),
            ReplicatedFetchesColumn::Database => self.database.cmp(&other.database),
            ReplicatedFetchesColumn::Table => self.table.cmp(&other.table),
            ReplicatedFetchesColumn::ResultPart => {
                self.result_part_name.cmp(&other.result_part_name)
            }
            ReplicatedFetchesColumn::Elapsed => self.elapsed.total_cmp(&other.elapsed),
            ReplicatedFetchesColumn::Progress => self.progress.total_cmp(&other.progress),
            ReplicatedFetchesColumn::TotalSizeBytesCompressed => self
                .total_size_bytes_compressed
                .cmp(&other.total_size_bytes_compressed),
            ReplicatedFetchesColumn::BytesReadCompressed => {
                self.bytes_read_compressed.cmp(&other.bytes_read_compressed)
            }
        }
    }
}

pub struct ReplicatedFetchesView {
    context: ContextArc,
    table: UpdatingTableView<FetchEntry, ReplicatedFetchesColumn>,
}

impl ReplicatedFetchesView {
    pub fn update(self: &mut Self, rows: Columns) {
        let mut items = Vec::new();

        for i in 0..rows.row_count() {
            items.push(FetchEntry {
                host_name: rows.get::<_, _>(i, "host_name").expect("host_name"),
                database: rows.get::<_, _>(i, "database").expect("database"),
                table: rows.get::<_, _>(i, "table").expect("table"),
                result_part_name: rows
                    .get::<_, _>(i, "result_part_name")
                    .expect("result_part_name"),
                elapsed: rows.get::<_, _>(i, "elapsed").expect("elapsed"),
                progress: rows.get::<_, _>(i, "progress").expect("progress"),
                total_size_bytes_compressed: rows
                    .get::<_, _>(i, "total_size_bytes_compressed")
                    .expect("total_size_bytes_compressed"),
                bytes_read_compressed: rows
                    .get::<_, _>(i, "bytes_read_compressed")
                    .expect("bytes_read_compressed"),
            });
        }

        if self.table.get_inner().is_empty() {
            self.table.get_inner_mut().set_items_stable(items);
            self.table.get_inner_mut().set_selected_row(0);
        } else {
            self.table.get_inner_mut().set_items_stable(items);
        }
    }

    pub fn new(context: ContextArc) -> Result<Self> {
        let delay = context.lock().unwrap().options.view.delay_interval;

        let update_callback_context = context.clone();
        let update_callback = move || {
            if let Ok(mut context_locked) = update_callback_context.try_lock() {
                context_locked
                    .worker
                    .send(WorkerEvent::GetReplicatedFetchesList);
            }
        };

        let mut table =
            UpdatingTableView::<FetchEntry, ReplicatedFetchesColumn>::new(delay, update_callback)
                .column(ReplicatedFetchesColumn::Database, "Database", |c| c)
                .column(ReplicatedFetchesColumn::Table, "Table", |c| c)
                .column(ReplicatedFetchesColumn::ResultPart, "Part", |c| c)
                .column(ReplicatedFetchesColumn::Elapsed, "Elapsed", |c| c)
                .column(ReplicatedFetchesColumn::Progress, "Progress", |c| c)
                .column(
                    ReplicatedFetchesColumn::TotalSizeBytesCompressed,
                    "Total",
                    |c| c,
                )
                .column(ReplicatedFetchesColumn::BytesReadCompressed, "Read", |c| c);
        // TODO: on_submit - show logs from system.text_log for this fetch

        table.sort_by(ReplicatedFetchesColumn::Elapsed, Ordering::Greater);

        if context.lock().unwrap().options.clickhouse.cluster.is_some() {
            table.insert_column(0, ReplicatedFetchesColumn::HostName, "HOST", |c| c.width(8));
        }

        let view = ReplicatedFetchesView { context, table };
        view.context
            .lock()
            .unwrap()
            .worker
            .send(WorkerEvent::GetReplicatedFetchesList);
        return Ok(view);
    }
}

impl ViewWrapper for ReplicatedFetchesView {
    wrap_impl_no_move!(self.table: UpdatingTableView<FetchEntry, ReplicatedFetchesColumn>);
}
