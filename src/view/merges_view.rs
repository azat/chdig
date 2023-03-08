use std::cmp::Ordering;

use anyhow::Result;
use size::{Base, SizeFormatter, Style};

use crate::interpreter::{clickhouse::Columns, BackgroundRunner, ContextArc, WorkerEvent};
use crate::view::{ExtTableView, TableViewItem};
use crate::wrap_impl_no_move;
use cursive::view::ViewWrapper;

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum MergesColumn {
    HostName,
    Database,
    Table,
    ResultPart,
    Elapsed,
    Progress,
    NumParts,
    IsMutation,
    Size,
    RowsRead,
    RowsWritten,
    Memory,
}

#[derive(Clone, Debug)]
pub struct Merge {
    pub host_name: String,
    pub database: String,
    pub table: String,
    pub result_part_name: String,
    pub elapsed: f64,
    pub progress: f64,
    pub num_parts: u64,
    pub is_mutation: bool,
    pub size: u64,
    pub rows_read: u64,
    pub rows_written: u64,
    pub memory: u64,
}

impl PartialEq<Merge> for Merge {
    fn eq(&self, other: &Self) -> bool {
        return self.host_name == other.host_name
            && self.database == other.database
            && self.table == other.table
            && self.result_part_name == other.result_part_name;
    }
}

impl TableViewItem<MergesColumn> for Merge {
    fn to_column(&self, column: MergesColumn) -> String {
        let fmt_bytes = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);
        let fmt_rows = SizeFormatter::new()
            .with_base(Base::Base10)
            .with_style(Style::Abbreviated);

        match column {
            MergesColumn::HostName => self.host_name.clone(),
            MergesColumn::Database => self.database.clone(),
            MergesColumn::Table => self.table.clone(),
            MergesColumn::ResultPart => self.result_part_name.clone(),
            MergesColumn::Elapsed => format!("{:.2}", self.elapsed),
            MergesColumn::Progress => format!("{:.2}", self.progress),
            MergesColumn::NumParts => self.num_parts.to_string(),
            MergesColumn::IsMutation => self.is_mutation.to_string(),
            MergesColumn::Size => fmt_rows.format(self.size as i64),
            MergesColumn::RowsRead => fmt_rows.format(self.rows_read as i64),
            MergesColumn::RowsWritten => fmt_rows.format(self.rows_written as i64),
            MergesColumn::Memory => fmt_bytes.format(self.memory as i64),
        }
    }

    fn cmp(&self, other: &Self, column: MergesColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            MergesColumn::HostName => self.host_name.cmp(&other.host_name),
            MergesColumn::Database => self.database.cmp(&other.database),
            MergesColumn::Table => self.table.cmp(&other.table),
            MergesColumn::ResultPart => self.result_part_name.cmp(&other.result_part_name),
            MergesColumn::Elapsed => self.elapsed.total_cmp(&other.elapsed),
            MergesColumn::Progress => self.progress.total_cmp(&other.progress),
            MergesColumn::NumParts => self.num_parts.cmp(&other.num_parts),
            MergesColumn::IsMutation => self.is_mutation.cmp(&other.is_mutation),
            MergesColumn::Size => self.size.cmp(&other.size),
            MergesColumn::RowsRead => self.rows_read.cmp(&other.rows_read),
            MergesColumn::RowsWritten => self.rows_written.cmp(&other.rows_written),
            MergesColumn::Memory => self.memory.cmp(&other.memory),
        }
    }
}

pub struct MergesView {
    table: ExtTableView<Merge, MergesColumn>,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
}

impl MergesView {
    pub fn update(self: &mut Self, rows: Columns) {
        let mut items = Vec::new();

        for i in 0..rows.row_count() {
            items.push(Merge {
                host_name: rows.get::<_, _>(i, "host_name").expect("host_name"),
                database: rows.get::<_, _>(i, "database").expect("database"),
                table: rows.get::<_, _>(i, "table").expect("table"),
                result_part_name: rows
                    .get::<_, _>(i, "result_part_name")
                    .expect("result_part_name"),
                elapsed: rows.get::<_, _>(i, "elapsed").expect("elapsed"),
                progress: rows.get::<_, _>(i, "progress").expect("progress"),
                num_parts: rows.get::<_, _>(i, "num_parts").expect("num_parts"),
                is_mutation: rows.get::<u8, _>(i, "is_mutation").expect("is_mutation") == 1,
                size: rows
                    .get::<_, _>(i, "total_size_bytes_compressed")
                    .expect("total_size_bytes_compressed"),
                rows_read: rows.get::<_, _>(i, "rows_read").expect("rows_read"),
                rows_written: rows.get::<_, _>(i, "rows_written").expect("rows_written"),
                memory: rows.get::<_, _>(i, "memory_usage").expect("memory_usage"),
            });
        }

        let inner_table = self.table.get_inner_mut();
        if inner_table.is_empty() {
            inner_table.set_items_stable(items);
            inner_table.set_selected_row(0);
        } else {
            inner_table.set_items_stable(items);
        }
    }

    pub fn new(context: ContextArc) -> Result<Self> {
        let delay = context.lock().unwrap().options.view.delay_interval;

        let update_callback_context = context.clone();
        let update_callback = move || {
            if let Ok(mut context_locked) = update_callback_context.try_lock() {
                context_locked.worker.send(WorkerEvent::GetMergesList);
            }
        };

        let mut table = ExtTableView::<Merge, MergesColumn>::default();
        let inner_table = table.get_inner_mut();
        inner_table.add_column(MergesColumn::Database, "Database", |c| {
            return c.ordering(Ordering::Less);
        });
        inner_table.add_column(MergesColumn::Table, "Table", |c| {
            return c.ordering(Ordering::Less);
        });
        inner_table.add_column(MergesColumn::ResultPart, "Part", |c| c);
        inner_table.add_column(MergesColumn::Elapsed, "Elapsed", |c| c);
        inner_table.add_column(MergesColumn::Progress, "Progress", |c| c);
        inner_table.add_column(MergesColumn::NumParts, "Parts", |c| c);
        inner_table.add_column(MergesColumn::IsMutation, "Mutation", |c| c);
        inner_table.add_column(MergesColumn::Size, "Size", |c| c);
        inner_table.add_column(MergesColumn::RowsRead, "Read", |c| c);
        inner_table.add_column(MergesColumn::RowsWritten, "Written", |c| c);
        inner_table.add_column(MergesColumn::Memory, "Memory", |c| c);
        // TODO: on_submit - show logs from system.text_log for this merge

        inner_table.sort_by(MergesColumn::Elapsed, Ordering::Greater);

        if context.lock().unwrap().options.clickhouse.cluster.is_some() {
            inner_table.insert_column(0, MergesColumn::HostName, "HOST", |c| c.width(8));
        }

        let mut bg_runner = BackgroundRunner::new(delay);
        bg_runner.start(update_callback);

        let view = MergesView { table, bg_runner };
        return Ok(view);
    }
}

impl ViewWrapper for MergesView {
    wrap_impl_no_move!(self.table: ExtTableView<Merge, MergesColumn>);
}
