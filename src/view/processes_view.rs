use std::cmp::Ordering;
use std::thread;

use anyhow::Result;
use cursive::traits::Resizable;
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
    HostName,
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
    host_name: String,
    cpu: u64,
    user: String,
    threads: usize,
    memory: i64,
    disk_io: u64,
    net_io: u64,
    elapsed: f64,
    has_initial_query: bool,
    is_initial_query: bool,
    initial_query_id: String,
    query_id: String,
    normalized_query: String,
    original_query: String,
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
            QueryProcessBasicColumn::HostName => self.host_name.to_string(),
            QueryProcessBasicColumn::Cpu => format!("{:.1} %", self.get_cpu()),
            QueryProcessBasicColumn::User => {
                if self.is_initial_query {
                    return self.user.to_string();
                } else if self.initial_query_id.is_empty() {
                    return self.user.to_string();
                } else {
                    return self.initial_query_id.to_string();
                }
            }
            QueryProcessBasicColumn::Threads => self.threads.to_string(),
            QueryProcessBasicColumn::Memory => formatter.format(self.memory),
            QueryProcessBasicColumn::DiskIO => formatter.format(self.disk_io as i64),
            QueryProcessBasicColumn::NetIO => formatter.format(self.net_io as i64),
            QueryProcessBasicColumn::Elapsed => format!("{:.2}", self.elapsed),
            QueryProcessBasicColumn::QueryId => {
                if self.is_initial_query {
                    return self.query_id.clone();
                } else if self.initial_query_id.is_empty() {
                    return self.query_id.clone();
                } else if self.has_initial_query {
                    return format!(" + {}", self.query_id);
                } else {
                    return format!("*{}", self.query_id);
                }
            }
            QueryProcessBasicColumn::Query => self.normalized_query.clone(),
        }
    }

    fn cmp(&self, other: &Self, column: QueryProcessBasicColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            QueryProcessBasicColumn::HostName => self.host_name.cmp(&other.host_name),
            QueryProcessBasicColumn::Cpu => self.get_cpu().total_cmp(&other.get_cpu()),
            QueryProcessBasicColumn::User => self.user.cmp(&other.user),
            QueryProcessBasicColumn::Threads => self.threads.cmp(&other.threads),
            QueryProcessBasicColumn::Memory => self.memory.cmp(&other.memory),
            QueryProcessBasicColumn::DiskIO => self.disk_io.cmp(&other.disk_io),
            QueryProcessBasicColumn::NetIO => self.net_io.cmp(&other.net_io),
            QueryProcessBasicColumn::Elapsed => self.elapsed.total_cmp(&other.elapsed),
            QueryProcessBasicColumn::QueryId => {
                // Group by initial_query_id
                let ordering = self.initial_query_id.cmp(&other.initial_query_id);
                if ordering == Ordering::Equal {
                    // Reverse order by is_initial_query, since we want to show initial query
                    // first.
                    let ordering = other.is_initial_query.cmp(&self.is_initial_query);
                    if ordering == Ordering::Equal {
                        return self.query_id.cmp(&other.query_id);
                    }
                    return ordering;
                }
                return ordering;
            }
            QueryProcessBasicColumn::Query => self.normalized_query.cmp(&other.normalized_query),
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
                    host_name: processes.get::<String, _>(i, "host_name")?,
                    cpu: processes.get::<u64, _>(i, "cpu")?,
                    user: processes.get::<String, _>(i, "user")?,
                    threads: processes.get::<Vec<u64>, _>(i, "thread_ids")?.len(),
                    memory: processes.get::<i64, _>(i, "peak_memory_usage")?,
                    disk_io: processes.get::<u64, _>(i, "disk_io")?,
                    net_io: processes.get::<u64, _>(i, "net_io")?,
                    elapsed: processes.get::<f64, _>(i, "elapsed")?,
                    has_initial_query: processes.get::<u8, _>(i, "has_initial_query")? == 1,
                    is_initial_query: processes.get::<u8, _>(i, "is_initial_query")? == 1,
                    initial_query_id: processes.get::<String, _>(i, "initial_query_id")?,
                    query_id: processes.get::<String, _>(i, "query_id")?,
                    normalized_query: processes.get::<String, _>(i, "normalized_query")?,
                    original_query: processes.get::<String, _>(i, "original_query")?,
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
                // FIXME: leaky abstraction
                context_locked.worker.send(WorkerEvent::UpdateSummary);
            }
            thread::sleep(delay);
        }));
    }

    pub fn new(context: ContextArc) -> Result<Self> {
        let mut table = TableView::<QueryProcess, QueryProcessBasicColumn>::new()
            .column(QueryProcessBasicColumn::QueryId, "QueryId", |c| {
                return c.ordering(Ordering::Less).width(10);
            })
            .column(QueryProcessBasicColumn::Cpu, "CPU", |c| {
                return c.ordering(Ordering::Greater).width(6);
            })
            .column(QueryProcessBasicColumn::User, "USER", |c| c.width(10))
            .column(QueryProcessBasicColumn::Threads, "TH", |c| c.width(6))
            .column(QueryProcessBasicColumn::Memory, "MEM", |c| c.width(6))
            .column(QueryProcessBasicColumn::DiskIO, "DISK", |c| c.width(7))
            .column(QueryProcessBasicColumn::NetIO, "NET", |c| c.width(6))
            .column(QueryProcessBasicColumn::Elapsed, "Elapsed", |c| c.width(11))
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
                                item.original_query.clone(),
                            );
                        },
                    )
                    .expect("No such view 'processes'");

                // TODO: add loader until it is loading
                siv.add_layer(views::Dialog::around(
                    views::LinearLayout::vertical()
                        .child(views::TextView::new(query))
                        .child(views::DummyView.fixed_height(1))
                        .child(views::TextView::new("Logs:").center())
                        .child(views::DummyView.fixed_height(1))
                        .child(
                            views::ScrollView::new(views::NamedView::new(
                                "query_log",
                                view::TextLogView::new(context, query_id),
                            ))
                            .scroll_x(true),
                        ),
                ));
            });

        if context.lock().unwrap().options.clickhouse.cluster.is_some() {
            table.insert_column(0, QueryProcessBasicColumn::HostName, "HOST", |c| c.width(8));
        }

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
