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
enum MergesColumn {
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
    context: ContextArc,
    table: TableView<Merge, MergesColumn>,
    last_size: Vec2,

    thread: Option<thread::JoinHandle<()>>,
    cv: Arc<(Mutex<bool>, Condvar)>,
}

impl Drop for MergesView {
    fn drop(&mut self) {
        *self.cv.0.lock().unwrap() = true;
        self.cv.1.notify_one();
        self.thread.take().unwrap().join().unwrap();
    }
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
                context_locked.worker.send(WorkerEvent::GetMergesList);
            }
            let result = cv.1.wait_timeout(cv.0.lock().unwrap(), delay).unwrap();
            let exit = *result.0;
            if exit {
                break;
            }
        }));
    }

    pub fn new(context: ContextArc) -> Result<Self> {
        let mut table = TableView::<Merge, MergesColumn>::new()
            .column(MergesColumn::Database, "Database", |c| {
                return c.ordering(Ordering::Less);
            })
            .column(MergesColumn::Table, "Table", |c| {
                return c.ordering(Ordering::Less);
            })
            .column(MergesColumn::ResultPart, "Part", |c| c)
            .column(MergesColumn::Elapsed, "Elapsed", |c| c)
            .column(MergesColumn::Progress, "Progress", |c| c)
            .column(MergesColumn::NumParts, "Parts", |c| c)
            .column(MergesColumn::IsMutation, "Mutation", |c| c)
            .column(MergesColumn::Size, "Size", |c| c)
            .column(MergesColumn::RowsRead, "Read", |c| c)
            .column(MergesColumn::RowsWritten, "Written", |c| c)
            .column(MergesColumn::Memory, "Memory", |c| c);
        // TODO: on_submit - show logs from system.text_log for this merge

        table.sort_by(MergesColumn::Elapsed, Ordering::Greater);

        if context.lock().unwrap().options.clickhouse.cluster.is_some() {
            table.insert_column(0, MergesColumn::HostName, "HOST", |c| c.width(8));
        }

        // TODO: add loader until it is loading
        let mut view = MergesView {
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
            .send(WorkerEvent::GetMergesList);
        view.start();
        return Ok(view);
    }
}

impl View for MergesView {
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
