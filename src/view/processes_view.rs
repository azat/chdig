use std::cmp::Ordering;
use std::thread;

use anyhow::Result;
use cursive::{
    direction::Direction,
    event::{Event, EventResult, Key},
    vec::Vec2,
    view::{CannotFocus, View},
    views, Cursive, Printer, Rect,
};
use cursive_table_view::{TableView, TableViewItem};
use size::{Base, SizeFormatter, Style};

use crate::interpreter::{ContextArc, WorkerEvent};
use crate::view;

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum QueryProcessBasicColumn {
    Cpu,
    User,
    Threads,
    Memory,
    DiskIO,
    NetIO,
    Elapsed,
    QueryId,
    Query,
}
#[derive(Clone, Debug)]
struct QueryProcess {
    cpu: u64,
    user: String,
    threads: usize,
    memory: i64,
    disk_io: u64,
    net_io: u64,
    elapsed: f64,
    query_id: String,
    query: String,
}
impl QueryProcess {
    fn get_cpu(&self) -> f64 {
        return (self.cpu as f64) / 1e6 / self.elapsed * 100.;
    }
}
impl PartialEq<QueryProcess> for QueryProcess {
    fn eq(&self, other: &Self) -> bool {
        return *self.query_id == other.query_id;
    }
}

impl TableViewItem<QueryProcessBasicColumn> for QueryProcess {
    fn to_column(&self, column: QueryProcessBasicColumn) -> String {
        let formatter = SizeFormatter::new()
            .with_base(Base::Base10)
            .with_style(Style::Abbreviated);

        match column {
            QueryProcessBasicColumn::Cpu => format!("{:.1} %", self.get_cpu()),
            QueryProcessBasicColumn::User => self.user.to_string(),
            QueryProcessBasicColumn::Threads => self.threads.to_string(),
            QueryProcessBasicColumn::Memory => formatter.format(self.memory),
            QueryProcessBasicColumn::DiskIO => formatter.format(self.disk_io as i64),
            QueryProcessBasicColumn::NetIO => formatter.format(self.net_io as i64),
            QueryProcessBasicColumn::Elapsed => format!("{:.2}", self.elapsed),
            QueryProcessBasicColumn::QueryId => self.query_id.clone(),
            QueryProcessBasicColumn::Query => self.query.clone(),
        }
    }

    fn cmp(&self, other: &Self, column: QueryProcessBasicColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            QueryProcessBasicColumn::Cpu => self.get_cpu().total_cmp(&other.get_cpu()),
            QueryProcessBasicColumn::User => self.user.cmp(&other.user),
            QueryProcessBasicColumn::Threads => self.threads.cmp(&other.threads),
            QueryProcessBasicColumn::Memory => self.memory.cmp(&other.memory),
            QueryProcessBasicColumn::DiskIO => self.disk_io.cmp(&other.disk_io),
            QueryProcessBasicColumn::NetIO => self.net_io.cmp(&other.net_io),
            QueryProcessBasicColumn::Elapsed => self.elapsed.total_cmp(&other.elapsed),
            QueryProcessBasicColumn::QueryId => self.query_id.cmp(&other.query_id),
            QueryProcessBasicColumn::Query => self.query.cmp(&other.query),
        }
    }
}

pub struct ProcessesView {
    context: ContextArc,
    table: TableView<QueryProcess, QueryProcessBasicColumn>,

    thread: Option<thread::JoinHandle<()>>,
}

impl ProcessesView {
    fn update_processes(self: &mut Self) -> Result<()> {
        let mut new_items = self.context.lock().unwrap().processes.clone();

        let mut items = Vec::new();
        if let Some(processes) = new_items.as_mut() {
            for i in 0..processes.row_count() {
                items.push(QueryProcess {
                    cpu: processes.get::<u64, _>(i, "cpu")?,
                    user: processes.get::<String, _>(i, "user")?,
                    threads: processes.get::<Vec<u64>, _>(i, "thread_ids")?.len(),
                    memory: processes.get::<i64, _>(i, "peak_memory_usage")?,
                    disk_io: processes.get::<u64, _>(i, "disk_io")?,
                    net_io: processes.get::<u64, _>(i, "net_io")?,
                    elapsed: processes.get::<f64, _>(i, "elapsed")?,
                    query_id: processes.get::<String, _>(i, "query_id")?,
                    query: processes.get::<String, _>(i, "query")?,
                });
            }
        }

        self.table.set_items_stable(items);

        return Ok(());
    }

    pub fn start(&mut self) {
        let context_copy = self.context.clone();
        let delay = self.context.lock().unwrap().options.view.delay_interval;
        self.thread = Some(std::thread::spawn(move || loop {
            // Do not try to do anything if there is contention,
            // since likely means that there is some query already in progress.
            if let Ok(mut context_locked) = context_copy.try_lock() {
                context_locked.worker.send(WorkerEvent::UpdateProcessList);
            }
            thread::sleep(delay);
        }));
    }

    pub fn new(context: ContextArc) -> Result<Self> {
        let mut table = TableView::<QueryProcess, QueryProcessBasicColumn>::new()
            .column(QueryProcessBasicColumn::Cpu, "CPU", |c| c.width(6))
            .column(QueryProcessBasicColumn::User, "USER", |c| c.width(10))
            .column(QueryProcessBasicColumn::Threads, "TH", |c| c.width(6))
            .column(QueryProcessBasicColumn::Memory, "MEM", |c| c.width(6))
            .column(QueryProcessBasicColumn::DiskIO, "DISK", |c| c.width(7))
            .column(QueryProcessBasicColumn::NetIO, "NET", |c| c.width(6))
            .column(QueryProcessBasicColumn::Elapsed, "Elapsed", |c| c.width(11))
            .column(QueryProcessBasicColumn::QueryId, "QueryId", |c| c.width(10))
            .column(QueryProcessBasicColumn::Query, "Query", |c| c)
            .on_submit(|siv: &mut Cursive, /* row */ _: usize, index: usize| {
                let (context, query_id, query) = siv
                    .call_on_name(
                        // TODO: use const for this name and move it here?
                        "processes",
                        move |view: &mut ProcessesView| {
                            let item = view.table.borrow_item(index).unwrap();
                            return (
                                view.context.clone(),
                                item.query_id.clone(),
                                item.query.clone(),
                            );
                        },
                    )
                    .expect("No such view 'processes'");

                // TODO: add loader until it is loading
                siv.add_layer(
                    views::Dialog::around(
                        views::ScrollView::new(views::NamedView::new(
                            "query_log",
                            view::TextLogView::new(context, query_id),
                        ))
                        .scroll_x(true),
                    )
                    // TODO: wrap lines
                    .title(query),
                );
            });

        table.sort_by(QueryProcessBasicColumn::Cpu, Ordering::Greater);

        // TODO: add loader until it is loading
        let mut view = ProcessesView {
            context,
            table,
            thread: None,
        };
        view.context
            .lock()
            .unwrap()
            .worker
            .send(WorkerEvent::UpdateProcessList);
        view.start();
        return Ok(view);
    }
}

impl View for ProcessesView {
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
        match event {
            // Tools
            Event::Char('f') => {
                let mut context_locked = self.context.lock().unwrap();
                let item_index = self.table.item().unwrap();
                let query_id = self.table.borrow_item(item_index).unwrap().query_id.clone();
                context_locked
                    .worker
                    .send(WorkerEvent::ShowQueryFlameGraph(query_id));
            }
            // Actions
            Event::Refresh => self.update_processes().unwrap(),
            // Basic bindings
            Event::Char('k') => return self.table.on_event(Event::Key(Key::Up)),
            Event::Char('j') => return self.table.on_event(Event::Key(Key::Down)),
            // TODO: PgDown/PgUP to scroll the screen, not only 10 items
            _ => {}
        }
        return self.table.on_event(event);
    }

    fn important_area(&self, size: Vec2) -> Rect {
        return self.table.important_area(size);
    }
}
