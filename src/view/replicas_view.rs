use std::cmp::Ordering;

use anyhow::Result;
use chrono::DateTime;
use chrono_tz::Tz;

use crate::interpreter::{clickhouse::Columns, BackgroundRunner, ContextArc, WorkerEvent};
use crate::view::{ExtTableView, TableViewItem};
use crate::wrap_impl_no_move;
use cursive::view::ViewWrapper;

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum ReplicasColumn {
    HostName,
    Database,
    Table,
    IsReadOnly,
    PartsToCheck,
    QueueSize,
    AbsoluteDelay,
    LastQueueUpdate,
}

#[derive(Clone, Debug)]
pub struct ReplicaEntry {
    pub host_name: String,
    pub database: String,
    pub table: String,
    pub replica_path: String,
    pub is_readonly: bool,
    pub parts_to_check: u32,
    pub queue_size: u32,
    pub absolute_delay: u64,
    pub last_queue_update: DateTime<Tz>,
}
impl PartialEq<ReplicaEntry> for ReplicaEntry {
    fn eq(&self, other: &Self) -> bool {
        return self.host_name == other.host_name && self.replica_path == other.replica_path;
    }
}

impl TableViewItem<ReplicasColumn> for ReplicaEntry {
    fn to_column(&self, column: ReplicasColumn) -> String {
        match column {
            ReplicasColumn::HostName => self.host_name.clone(),
            ReplicasColumn::Database => self.database.clone(),
            ReplicasColumn::Table => self.table.clone(),
            ReplicasColumn::IsReadOnly => self.is_readonly.to_string(),
            ReplicasColumn::PartsToCheck => self.parts_to_check.to_string(),
            ReplicasColumn::QueueSize => self.queue_size.to_string(),
            ReplicasColumn::AbsoluteDelay => self.absolute_delay.to_string(),
            ReplicasColumn::LastQueueUpdate => self.last_queue_update.to_string(),
        }
    }

    fn cmp(&self, other: &Self, column: ReplicasColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            ReplicasColumn::HostName => self.host_name.cmp(&other.host_name),
            ReplicasColumn::Database => self.database.cmp(&other.database),
            ReplicasColumn::Table => self.table.cmp(&other.table),
            ReplicasColumn::IsReadOnly => self.is_readonly.cmp(&other.is_readonly),
            ReplicasColumn::PartsToCheck => self.parts_to_check.cmp(&other.parts_to_check),
            ReplicasColumn::QueueSize => self.queue_size.cmp(&other.queue_size),
            ReplicasColumn::AbsoluteDelay => self.absolute_delay.cmp(&other.absolute_delay),
            ReplicasColumn::LastQueueUpdate => self.last_queue_update.cmp(&other.last_queue_update),
        }
    }
}

pub struct ReplicasView {
    table: ExtTableView<ReplicaEntry, ReplicasColumn>,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
}

impl ReplicasView {
    pub fn update(self: &mut Self, rows: Columns) {
        let mut items = Vec::new();

        for i in 0..rows.row_count() {
            items.push(ReplicaEntry {
                host_name: rows.get::<_, _>(i, "host_name").expect("host_name"),
                database: rows.get::<_, _>(i, "database").expect("database"),
                table: rows.get::<_, _>(i, "table").expect("table"),
                replica_path: rows.get::<_, _>(i, "replica_path").expect("replica_path"),
                is_readonly: rows.get::<u8, _>(i, "is_readonly").expect("is_readonly") == 1,
                parts_to_check: rows
                    .get::<_, _>(i, "parts_to_check")
                    .expect("parts_to_check"),
                queue_size: rows.get::<_, _>(i, "queue_size").expect("queue_size"),
                absolute_delay: rows
                    .get::<_, _>(i, "absolute_delay")
                    .expect("absolute_delay"),
                last_queue_update: rows
                    .get::<_, _>(i, "last_queue_update")
                    .expect("last_queue_update"),
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
                context_locked.worker.send(WorkerEvent::GetReplicasList);
            }
        };

        let mut table = ExtTableView::<ReplicaEntry, ReplicasColumn>::default();
        let inner_table = table.get_inner_mut();
        inner_table.add_column(ReplicasColumn::Database, "Database", |c| c);
        inner_table.add_column(ReplicasColumn::Table, "Table", |c| c);
        inner_table.add_column(ReplicasColumn::IsReadOnly, "Read only", |c| c);
        inner_table.add_column(ReplicasColumn::PartsToCheck, "Parts to check", |c| c);
        inner_table.add_column(ReplicasColumn::QueueSize, "Queue", |c| c);
        inner_table.add_column(ReplicasColumn::AbsoluteDelay, "Delay", |c| c);
        inner_table.add_column(ReplicasColumn::LastQueueUpdate, "Last queue update", |c| c);

        // TODO: multiple sort by IsReadOnly and QueueSize
        inner_table.sort_by(ReplicasColumn::QueueSize, Ordering::Greater);

        if context.lock().unwrap().options.clickhouse.cluster.is_some() {
            inner_table.insert_column(0, ReplicasColumn::HostName, "HOST", |c| c.width(8));
        }

        let mut bg_runner = BackgroundRunner::new(delay);
        bg_runner.start(update_callback);

        let view = ReplicasView { table, bg_runner };
        return Ok(view);
    }
}

impl ViewWrapper for ReplicasView {
    wrap_impl_no_move!(self.table: ExtTableView<ReplicaEntry, ReplicasColumn>);
}
