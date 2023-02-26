use std::cmp::Ordering;
use std::thread;

use anyhow::Result;
use chrono::DateTime;
use chrono_tz::Tz;
use cursive::{
    direction::Direction,
    event::{Event, EventResult, Key},
    vec::Vec2,
    view::{CannotFocus, View},
    Printer, Rect,
};
use cursive_table_view::{TableView, TableViewItem};

use crate::interpreter::{ContextArc, WorkerEvent};

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum ReplicationQueueColumn {
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
pub struct ReplicationEntry {
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
impl PartialEq<ReplicationEntry> for ReplicationEntry {
    fn eq(&self, other: &Self) -> bool {
        return self.database == other.database
            && self.table == other.table
            && self.new_part_name == other.new_part_name;
    }
}

impl TableViewItem<ReplicationQueueColumn> for ReplicationEntry {
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
    context: ContextArc,
    table: TableView<ReplicationEntry, ReplicationQueueColumn>,
    last_size: Vec2,

    thread: Option<thread::JoinHandle<()>>,
}

impl ReplicationQueueView {
    fn update(self: &mut Self) -> Result<()> {
        let context_locked = self.context.try_lock();
        if let Err(_) = context_locked {
            return Ok(());
        }

        let mut new_items = context_locked.unwrap().replication_queue.clone();
        let mut items = Vec::new();
        if let Some(rows) = new_items.as_mut() {
            for i in 0..rows.row_count() {
                items.push(ReplicationEntry {
                    host_name: rows.get::<String, _>(i, "host_name").expect("host_name"),
                    database: rows.get::<String, _>(i, "database").expect("database"),
                    table: rows.get::<String, _>(i, "table").expect("table"),
                    create_time: rows
                        .get::<DateTime<Tz>, _>(i, "create_time")
                        .expect("create_time"),
                    new_part_name: rows
                        .get::<String, _>(i, "new_part_name")
                        .expect("new_part_name"),
                    is_currently_executing: rows
                        .get::<u8, _>(i, "is_currently_executing")
                        .expect("is_currently_executing")
                        == 1,
                    num_tries: rows.get::<u32, _>(i, "num_tries").expect("num_tries"),
                    last_exception: rows
                        .get::<String, _>(i, "last_exception")
                        .expect("last_exception"),
                    num_postponed: rows
                        .get::<u32, _>(i, "num_postponed")
                        .expect("num_postponed"),
                    postpone_reason: rows
                        .get::<String, _>(i, "postpone_reason")
                        .expect("postpone_reason"),
                });
            }
        }

        self.table.set_items_stable(items);

        return Ok(());
    }

    pub fn start(&mut self) {
        let context_copy = self.context.clone();
        let delay = self.context.lock().unwrap().options.view.delay_interval;
        // FIXME: more common way to do periodic job
        self.thread = Some(std::thread::spawn(move || loop {
            // Do not try to do anything if there is contention,
            // since likely means that there is some query already in progress.
            if let Ok(mut context_locked) = context_copy.try_lock() {
                context_locked
                    .worker
                    .send(WorkerEvent::GetReplicationQueueList);
            }
            thread::sleep(delay);
        }));
    }

    pub fn new(context: ContextArc) -> Result<Self> {
        let mut table = TableView::<ReplicationEntry, ReplicationQueueColumn>::new()
            .column(ReplicationQueueColumn::Database, "Database", |c| c)
            .column(ReplicationQueueColumn::Table, "Table", |c| c)
            .column(ReplicationQueueColumn::CreateTime, "Created", |c| c)
            .column(ReplicationQueueColumn::NewPartName, "NewPart", |c| c)
            .column(
                ReplicationQueueColumn::IsCurrentlyExecuting,
                "Running",
                |c| c,
            )
            .column(ReplicationQueueColumn::NumTries, "Tries", |c| c)
            .column(ReplicationQueueColumn::LastException, "Error", |c| c)
            .column(ReplicationQueueColumn::NumPostponed, "Postponed", |c| c)
            .column(ReplicationQueueColumn::PostponeReason, "Reason", |c| c);
        // TODO: on_submit - show logs from system.text_log for this replication queue entry

        if context.lock().unwrap().options.clickhouse.cluster.is_some() {
            table.insert_column(0, ReplicationQueueColumn::HostName, "HOST", |c| c.width(8));
        }

        // TODO: add loader until it is loading
        let mut view = ReplicationQueueView {
            context,
            table,
            last_size: Vec2 { x: 1, y: 1 },
            thread: None,
        };
        view.context
            .lock()
            .unwrap()
            .worker
            .send(WorkerEvent::GetReplicationQueueList);
        view.start();
        return Ok(view);
    }
}

impl View for ReplicationQueueView {
    fn draw(&self, printer: &Printer) {
        self.table.draw(printer);
    }

    fn layout(&mut self, size: Vec2) {
        self.last_size = size;

        assert!(self.last_size.y > 2);
        // header and borders
        self.last_size.y -= 2;

        self.table.layout(size);
    }

    fn take_focus(&mut self, direction: Direction) -> Result<EventResult, CannotFocus> {
        return self.table.take_focus(direction);
    }

    // TODO:
    // - pause/disable the table if the foreground view had been changed
    fn on_event(&mut self, event: Event) -> EventResult {
        match event {
            // Table actions
            Event::Refresh => self.update().unwrap(),
            // Basic bindings (TODO: add a wrapper for table with the actions below)
            Event::Char('k') => return self.table.on_event(Event::Key(Key::Up)),
            Event::Char('j') => return self.table.on_event(Event::Key(Key::Down)),
            // cursive_table_view scrolls only 10 rows, rebind to scroll the whole page
            Event::Key(Key::PageUp) => {
                let row = self.table.row().unwrap_or_default();
                let height = self.last_size.y;
                let new_row = if row > height { row - height + 1 } else { 0 };
                self.table.set_selected_row(new_row);
                return EventResult::Consumed(None);
            }
            Event::Key(Key::PageDown) => {
                let row = self.table.row().unwrap_or_default();
                let len = self.table.len();
                let height = self.last_size.y;
                let new_row = if len - row > height {
                    row + height - 1
                } else if len > 0 {
                    len - 1
                } else {
                    0
                };
                self.table.set_selected_row(new_row);
                return EventResult::Consumed(None);
            }
            _ => {}
        }
        return self.table.on_event(event);
    }

    fn important_area(&self, size: Vec2) -> Rect {
        return self.table.important_area(size);
    }
}
