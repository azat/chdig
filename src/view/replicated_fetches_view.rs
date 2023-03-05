use std::cmp::Ordering;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use anyhow::Result;
use cursive::{
    direction::Direction,
    event::{Event, EventResult, Key},
    vec::Vec2,
    view::{CannotFocus, View},
    Printer, Rect,
};
use cursive_table_view::{TableView, TableViewItem};
use size::{Base, SizeFormatter, Style};

use crate::interpreter::{clickhouse::Columns, ContextArc, WorkerEvent};

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum ReplicatedFetchesColumn {
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
        return self.database == other.database
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
    table: TableView<FetchEntry, ReplicatedFetchesColumn>,
    last_size: Vec2,

    thread: Option<thread::JoinHandle<()>>,
    cv: Arc<(Mutex<bool>, Condvar)>,
}

impl Drop for ReplicatedFetchesView {
    fn drop(&mut self) {
        *self.cv.0.lock().unwrap() = true;
        self.cv.1.notify_one();
        self.thread.take().unwrap().join().unwrap();
    }
}

impl ReplicatedFetchesView {
    pub fn update(self: &mut Self, rows: Columns) {
        let mut items = Vec::new();

        for i in 0..rows.row_count() {
            items.push(FetchEntry {
                host_name: rows.get::<String, _>(i, "host_name").expect("host_name"),
                database: rows.get::<String, _>(i, "database").expect("database"),
                table: rows.get::<String, _>(i, "table").expect("table"),
                result_part_name: rows
                    .get::<String, _>(i, "result_part_name")
                    .expect("result_part_name"),
                elapsed: rows.get::<f64, _>(i, "elapsed").expect("elapsed"),
                progress: rows.get::<f64, _>(i, "progress").expect("progress"),
                total_size_bytes_compressed: rows
                    .get::<u64, _>(i, "total_size_bytes_compressed")
                    .expect("total_size_bytes_compressed"),
                bytes_read_compressed: rows
                    .get::<u64, _>(i, "bytes_read_compressed")
                    .expect("bytes_read_compressed"),
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
                context_locked
                    .worker
                    .send(WorkerEvent::GetReplicatedFetchesList);
            }
            let result = cv.1.wait_timeout(cv.0.lock().unwrap(), delay).unwrap();
            let exit = *result.0;
            if exit {
                break;
            }
        }));
    }

    pub fn new(context: ContextArc) -> Result<Self> {
        let mut table = TableView::<FetchEntry, ReplicatedFetchesColumn>::new()
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

        // TODO: add loader until it is loading
        let mut view = ReplicatedFetchesView {
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
            .send(WorkerEvent::GetReplicatedFetchesList);
        view.start();
        return Ok(view);
    }
}

impl View for ReplicatedFetchesView {
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
