use std::cmp::Ordering;
use std::sync::{Arc, Condvar, Mutex};
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

use crate::interpreter::{clickhouse::Columns, ContextArc, WorkerEvent};

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum ReplicasColumn {
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
pub struct ReplicationEntry {
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
impl PartialEq<ReplicationEntry> for ReplicationEntry {
    fn eq(&self, other: &Self) -> bool {
        // TODO: add host_name for each comparator
        return self.replica_path == other.replica_path;
    }
}

impl TableViewItem<ReplicasColumn> for ReplicationEntry {
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
    context: ContextArc,
    table: TableView<ReplicationEntry, ReplicasColumn>,
    last_size: Vec2,

    thread: Option<thread::JoinHandle<()>>,
    cv: Arc<(Mutex<bool>, Condvar)>,
}

impl Drop for ReplicasView {
    fn drop(&mut self) {
        *self.cv.0.lock().unwrap() = true;
        self.cv.1.notify_one();
        self.thread.take().unwrap().join().unwrap();
    }
}

impl ReplicasView {
    pub fn update(self: &mut Self, rows: Columns) {
        let mut items = Vec::new();

        for i in 0..rows.row_count() {
            items.push(ReplicationEntry {
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

        if self.table.is_empty() {
            self.table.set_items_stable(items);
            self.table.set_selected_row(0);
        } else {
            self.table.set_items_stable(items);
        }
    }

    pub fn start(&mut self) {
        let context_copy = self.context.clone();
        let delay = self.context.lock().unwrap().options.view.delay_interval;
        let cv = self.cv.clone();
        // FIXME: more common way to do periodic job
        self.thread = Some(std::thread::spawn(move || loop {
            // Do not try to do anything if there is contention,
            // since likely means that there is some query already in progress.
            if let Ok(mut context_locked) = context_copy.try_lock() {
                context_locked.worker.send(WorkerEvent::GetReplicasList);
            }
            let result = cv.1.wait_timeout(cv.0.lock().unwrap(), delay).unwrap();
            let exit = *result.0;
            if exit {
                break;
            }
        }));
    }

    pub fn new(context: ContextArc) -> Result<Self> {
        let mut table = TableView::<ReplicationEntry, ReplicasColumn>::new()
            .column(ReplicasColumn::Database, "Database", |c| c)
            .column(ReplicasColumn::Table, "Table", |c| c)
            .column(ReplicasColumn::IsReadOnly, "Read only", |c| c)
            .column(ReplicasColumn::PartsToCheck, "Parts to check", |c| c)
            .column(ReplicasColumn::QueueSize, "Queue", |c| c)
            .column(ReplicasColumn::AbsoluteDelay, "Delay", |c| c)
            .column(ReplicasColumn::LastQueueUpdate, "Last queue update", |c| c);

        // TODO: multiple sort by IsReadOnly and QueueSize
        table.sort_by(ReplicasColumn::QueueSize, Ordering::Greater);

        if context.lock().unwrap().options.clickhouse.cluster.is_some() {
            table.insert_column(0, ReplicasColumn::HostName, "HOST", |c| c.width(8));
        }

        // TODO: add loader until it is loading
        let mut view = ReplicasView {
            context,
            table,
            last_size: Vec2 { x: 1, y: 1 },
            thread: None,
            cv: Arc::new((Mutex::new(false), Condvar::new())),
        };
        view.context
            .lock()
            .unwrap()
            .worker
            .send(WorkerEvent::GetReplicasList);
        view.start();
        return Ok(view);
    }
}

impl View for ReplicasView {
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
