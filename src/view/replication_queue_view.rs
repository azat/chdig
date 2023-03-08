use std::cmp::Ordering;

use anyhow::Result;
use chrono::DateTime;
use chrono_tz::Tz;

use crate::interpreter::{clickhouse::Columns, BackgroundRunner, ContextArc, WorkerEvent};
use crate::view::{ExtTableView, TableViewItem};
use crate::wrap_impl_no_move;
use cursive::view::ViewWrapper;

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum ReplicationQueueColumn {
    HostName,
    Database,
    Table,
    CreateTime,
    NewPartName,
    IsCurrentlyExecuting,
    NumTries,
    LastException,
    NumPostponed,
    PostponeReason,
}

#[derive(Clone, Debug)]
pub struct ReplicationQueueEntry {
    pub host_name: String,
    pub database: String,
    pub table: String,
    pub create_time: DateTime<Tz>,
    pub new_part_name: String,
    pub is_currently_executing: bool,
    pub num_tries: u32,
    pub last_exception: String,
    pub num_postponed: u32,
    pub postpone_reason: String,
}
impl PartialEq<ReplicationQueueEntry> for ReplicationQueueEntry {
    fn eq(&self, other: &Self) -> bool {
        return self.host_name == other.host_name
            && self.database == other.database
            && self.table == other.table
            && self.new_part_name == other.new_part_name;
    }
}

impl TableViewItem<ReplicationQueueColumn> for ReplicationQueueEntry {
    fn to_column(&self, column: ReplicationQueueColumn) -> String {
        match column {
            ReplicationQueueColumn::HostName => self.host_name.clone(),
            ReplicationQueueColumn::Database => self.database.clone(),
            ReplicationQueueColumn::Table => self.table.clone(),
            ReplicationQueueColumn::CreateTime => self.create_time.to_string(),
            ReplicationQueueColumn::NewPartName => self.new_part_name.clone(),
            ReplicationQueueColumn::IsCurrentlyExecuting => self.is_currently_executing.to_string(),
            ReplicationQueueColumn::NumTries => self.num_tries.to_string(),
            ReplicationQueueColumn::LastException => self.last_exception.clone(),
            ReplicationQueueColumn::NumPostponed => self.num_postponed.to_string(),
            ReplicationQueueColumn::PostponeReason => self.postpone_reason.clone(),
        }
    }

    fn cmp(&self, other: &Self, column: ReplicationQueueColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            ReplicationQueueColumn::HostName => self.host_name.cmp(&other.host_name),
            ReplicationQueueColumn::Database => self.database.cmp(&other.database),
            ReplicationQueueColumn::Table => self.table.cmp(&other.table),
            ReplicationQueueColumn::CreateTime => self.create_time.cmp(&other.create_time),
            ReplicationQueueColumn::NewPartName => self.new_part_name.cmp(&other.new_part_name),
            ReplicationQueueColumn::IsCurrentlyExecuting => self
                .is_currently_executing
                .cmp(&other.is_currently_executing),
            ReplicationQueueColumn::NumTries => self.num_tries.cmp(&other.num_tries),
            ReplicationQueueColumn::LastException => self.last_exception.cmp(&other.last_exception),
            ReplicationQueueColumn::NumPostponed => self.num_postponed.cmp(&other.num_postponed),
            ReplicationQueueColumn::PostponeReason => {
                self.postpone_reason.cmp(&other.postpone_reason)
            }
        }
    }
}

pub struct ReplicationQueueView {
    table: ExtTableView<ReplicationQueueEntry, ReplicationQueueColumn>,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
}

impl ReplicationQueueView {
    pub fn update(self: &mut Self, rows: Columns) {
        let mut items = Vec::new();

        for i in 0..rows.row_count() {
            items.push(ReplicationQueueEntry {
                host_name: rows.get::<_, _>(i, "host_name").expect("host_name"),
                database: rows.get::<_, _>(i, "database").expect("database"),
                table: rows.get::<_, _>(i, "table").expect("table"),
                create_time: rows.get::<_, _>(i, "create_time").expect("create_time"),
                new_part_name: rows.get::<_, _>(i, "new_part_name").expect("new_part_name"),
                is_currently_executing: rows
                    .get::<u8, _>(i, "is_currently_executing")
                    .expect("is_currently_executing")
                    == 1,
                num_tries: rows.get::<_, _>(i, "num_tries").expect("num_tries"),
                last_exception: rows
                    .get::<_, _>(i, "last_exception")
                    .expect("last_exception"),
                num_postponed: rows.get::<_, _>(i, "num_postponed").expect("num_postponed"),
                postpone_reason: rows
                    .get::<_, _>(i, "postpone_reason")
                    .expect("postpone_reason"),
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
                context_locked
                    .worker
                    .send(WorkerEvent::GetReplicationQueueList);
            }
        };

        let mut table = ExtTableView::<ReplicationQueueEntry, ReplicationQueueColumn>::default();
        let inner_table = table.get_inner_mut();
        inner_table.add_column(ReplicationQueueColumn::Database, "Database", |c| c);
        inner_table.add_column(ReplicationQueueColumn::Table, "Table", |c| c);
        inner_table.add_column(ReplicationQueueColumn::CreateTime, "Created", |c| c);
        inner_table.add_column(ReplicationQueueColumn::NewPartName, "NewPart", |c| c);
        inner_table.add_column(
            ReplicationQueueColumn::IsCurrentlyExecuting,
            "Running",
            |c| c,
        );
        inner_table.add_column(ReplicationQueueColumn::NumTries, "Tries", |c| c);
        inner_table.add_column(ReplicationQueueColumn::LastException, "Error", |c| c);
        inner_table.add_column(ReplicationQueueColumn::NumPostponed, "Postponed", |c| c);
        inner_table.add_column(ReplicationQueueColumn::PostponeReason, "Reason", |c| c);
        // TODO: on_submit - show logs from system.text_log for this replication queue entry

        inner_table.sort_by(ReplicationQueueColumn::NumTries, Ordering::Greater);

        if context.lock().unwrap().options.clickhouse.cluster.is_some() {
            inner_table.insert_column(0, ReplicationQueueColumn::HostName, "HOST", |c| c.width(8));
        }

        let mut bg_runner = BackgroundRunner::new(delay);
        bg_runner.start(update_callback);

        let view = ReplicationQueueView { table, bg_runner };
        return Ok(view);
    }
}

impl ViewWrapper for ReplicationQueueView {
    wrap_impl_no_move!(self.table: ExtTableView<ReplicationQueueEntry, ReplicationQueueColumn>);
}
