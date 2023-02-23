use std::cmp::Ordering;
use std::rc::Rc;
use std::thread;

use anyhow::Result;
use cursive::traits::Resizable;
use cursive::{
    direction::Direction,
    event::{Event, EventResult, Key},
    menu,
    vec::Vec2,
    view::{CannotFocus, View},
    views, Cursive, Printer, Rect,
};
use cursive_table_view::{TableView, TableViewItem};
use size::{Base, SizeFormatter, Style};

use crate::interpreter::{clickhouse::TraceType, ContextArc, WorkerEvent};
use crate::view;
use crate::view::utils;

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
    last_size: Vec2,

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
        // FIXME: more common way to do periodic job
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
            .on_submit(|siv: &mut Cursive, _row: usize, _index: usize| {
                siv.add_layer(views::MenuPopup::new(Rc::new(
                    menu::Tree::new()
                        .leaf("Show query logs  (l)", |s| s.on_event(Event::Char('l')))
                        .leaf("CPU flamegraph   (C)", |s| s.on_event(Event::Char('C')))
                        .leaf("Real flamegraph  (R)", |s| s.on_event(Event::Char('R')))
                        .leaf("Memory flamegraph(M)", |s| s.on_event(Event::Char('M')))
                        .leaf("Live flamegraph  (L)", |s| s.on_event(Event::Char('L')))
                        .leaf("EXPLAIN PLAN     (e)", |s| s.on_event(Event::Char('e')))
                        .leaf("EXPLAIN PIPELINE (E)", |s| s.on_event(Event::Char('E')))
                        .leaf("Kill this query  (K)", |s| s.on_event(Event::Char('K'))),
                )));
            });

        if context.lock().unwrap().options.clickhouse.cluster.is_some() {
            table.insert_column(0, QueryProcessBasicColumn::HostName, "HOST", |c| c.width(8));
        }

        // TODO: add loader until it is loading
        let mut view = ProcessesView {
            context,
            table,
            last_size: Vec2 { x: 1, y: 1 },
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
        self.last_size = size;

        assert!(self.last_size.y > 2);
        // header and borders
        self.last_size.y -= 2;

        self.table.layout(size);
    }

    fn take_focus(&mut self, direction: Direction) -> Result<EventResult, CannotFocus> {
        return self.table.take_focus(direction);
    }

    fn on_event(&mut self, event: Event) -> EventResult {
        match event {
            // Query actions
            Event::Char('C') => {
                let mut context_locked = self.context.lock().unwrap();
                let item_index = self.table.item().unwrap();
                let query_id = self.table.borrow_item(item_index).unwrap().query_id.clone();
                context_locked
                    .worker
                    .send(WorkerEvent::ShowQueryFlameGraph(TraceType::CPU, query_id));
            }
            // TODO: reduce copy-paste
            Event::Char('R') => {
                let mut context_locked = self.context.lock().unwrap();
                let item_index = self.table.item().unwrap();
                let query_id = self.table.borrow_item(item_index).unwrap().query_id.clone();
                context_locked
                    .worker
                    .send(WorkerEvent::ShowQueryFlameGraph(TraceType::Real, query_id));
            }
            Event::Char('M') => {
                let mut context_locked = self.context.lock().unwrap();
                let item_index = self.table.item().unwrap();
                let query_id = self.table.borrow_item(item_index).unwrap().query_id.clone();
                context_locked.worker.send(WorkerEvent::ShowQueryFlameGraph(
                    TraceType::Memory,
                    query_id,
                ));
            }
            Event::Char('L') => {
                let mut context_locked = self.context.lock().unwrap();
                let item_index = self.table.item().unwrap();
                let query_id = self.table.borrow_item(item_index).unwrap().query_id.clone();
                context_locked
                    .worker
                    .send(WorkerEvent::ShowLiveQueryFlameGraph(query_id));
            }
            Event::Char('e') => {
                let mut context_locked = self.context.lock().unwrap();
                let item_index = self.table.item().unwrap();
                let query = self
                    .table
                    .borrow_item(item_index)
                    .unwrap()
                    .original_query
                    .clone();
                context_locked.worker.send(WorkerEvent::ExplainPlan(query));
            }
            Event::Char('E') => {
                let mut context_locked = self.context.lock().unwrap();
                let item_index = self.table.item().unwrap();
                let query = self
                    .table
                    .borrow_item(item_index)
                    .unwrap()
                    .original_query
                    .clone();
                context_locked
                    .worker
                    .send(WorkerEvent::ExplainPipeline(query));
            }
            Event::Char('K') => {
                let item_index = self.table.item().unwrap();
                let query_id = self.table.borrow_item(item_index).unwrap().query_id.clone();
                let context_copy = self.context.clone();

                self.context
                    .lock()
                    .unwrap()
                    .cb_sink
                    .send(Box::new(move |siv: &mut cursive::Cursive| {
                        siv.add_layer(
                            views::Dialog::new()
                                .title(&format!(
                                    "Are you sure you want to KILL QUERY with query_id = {}",
                                    query_id
                                ))
                                .button("Yes, I'm sure", move |s| {
                                    context_copy
                                        .lock()
                                        .unwrap()
                                        .worker
                                        .send(WorkerEvent::KillQuery(query_id.clone()));
                                    // TODO: wait for the KILL
                                    s.pop_layer();
                                })
                                .button("Cancel", |s| {
                                    s.pop_layer();
                                }),
                        );
                    }))
                    .unwrap();
            }
            Event::Char('l') => {
                let item_index = self.table.item().unwrap();
                let item = self.table.borrow_item(item_index).unwrap();
                let query_id = item.query_id.clone();
                let original_query = item.original_query.clone();
                let context_copy = self.context.clone();

                self.context
                    .lock()
                    .unwrap()
                    .cb_sink
                    .send(Box::new(move |siv: &mut cursive::Cursive| {
                        // TODO: add loader until it is loading
                        siv.add_layer(views::Dialog::around(
                            views::LinearLayout::vertical()
                                .child(views::TextView::new(
                                    utils::highlight_sql(&original_query).unwrap(),
                                ))
                                .child(views::DummyView.fixed_height(1))
                                .child(views::TextView::new("Logs:").center())
                                .child(views::DummyView.fixed_height(1))
                                .child(
                                    views::ScrollView::new(views::NamedView::new(
                                        "query_log",
                                        view::TextLogView::new(context_copy, query_id.clone()),
                                    ))
                                    .scroll_x(true),
                                ),
                        ));
                    }))
                    .unwrap();
            }
            // Table actions
            Event::Refresh => self.update_processes().unwrap(),
            // Basic bindings
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
