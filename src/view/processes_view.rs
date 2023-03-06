use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::take;
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use anyhow::Result;
use cursive::traits::{Nameable, Resizable};
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

use crate::interpreter::{
    clickhouse::Columns, clickhouse::TraceType, options::ViewOptions, ContextArc, QueryProcess,
    WorkerEvent,
};
use crate::view;
use crate::view::utils;

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum QueryProcessesColumn {
    HostName,
    SubQueries,
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
impl PartialEq<QueryProcess> for QueryProcess {
    fn eq(&self, other: &Self) -> bool {
        return self.query_id == other.query_id;
    }
}

impl TableViewItem<QueryProcessesColumn> for QueryProcess {
    fn to_column(&self, column: QueryProcessesColumn) -> String {
        let formatter = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);

        match column {
            QueryProcessesColumn::HostName => self.host_name.to_string(),
            QueryProcessesColumn::SubQueries => {
                if self.is_initial_query {
                    return self.subqueries.to_string();
                } else {
                    return 1.to_string();
                }
            }
            QueryProcessesColumn::Cpu => format!("{:.1} %", self.cpu()),
            QueryProcessesColumn::User => self.user.clone(),
            QueryProcessesColumn::Threads => self.threads.to_string(),
            QueryProcessesColumn::Memory => formatter.format(self.memory),
            QueryProcessesColumn::DiskIO => formatter.format(self.disk_io() as i64),
            QueryProcessesColumn::NetIO => formatter.format(self.net_io() as i64),
            QueryProcessesColumn::Elapsed => format!("{:.2}", self.elapsed),
            QueryProcessesColumn::QueryId => {
                if self.subqueries > 0 && self.is_initial_query {
                    return format!("-> {}", self.query_id);
                } else {
                    return self.query_id.clone();
                }
            }
            QueryProcessesColumn::Query => self.normalized_query.clone(),
        }
    }

    fn cmp(&self, other: &Self, column: QueryProcessesColumn) -> Ordering
    where
        Self: Sized,
    {
        match column {
            QueryProcessesColumn::HostName => self.host_name.cmp(&other.host_name),
            QueryProcessesColumn::SubQueries => self.subqueries.cmp(&other.subqueries),
            QueryProcessesColumn::Cpu => self.cpu().total_cmp(&other.cpu()),
            QueryProcessesColumn::User => self.user.cmp(&other.user),
            QueryProcessesColumn::Threads => self.threads.cmp(&other.threads),
            QueryProcessesColumn::Memory => self.memory.cmp(&other.memory),
            QueryProcessesColumn::DiskIO => self.disk_io().total_cmp(&other.disk_io()),
            QueryProcessesColumn::NetIO => self.net_io().total_cmp(&other.net_io()),
            QueryProcessesColumn::Elapsed => self.elapsed.total_cmp(&other.elapsed),
            QueryProcessesColumn::QueryId => self.query_id.cmp(&other.query_id),
            QueryProcessesColumn::Query => self.normalized_query.cmp(&other.normalized_query),
        }
    }
}

pub struct ProcessesView {
    context: ContextArc,
    table: TableView<QueryProcess, QueryProcessesColumn>,
    last_size: Vec2,
    items: HashMap<String, QueryProcess>,
    query_id: Option<String>,
    options: ViewOptions,

    thread: Option<thread::JoinHandle<()>>,
    cv: Arc<(Mutex<bool>, Condvar)>,
}

impl Drop for ProcessesView {
    fn drop(&mut self) {
        log::debug!("Stopping updates of processes");
        *self.cv.0.lock().unwrap() = true;
        self.cv.1.notify_one();
        self.thread.take().unwrap().join().unwrap();
        log::debug!("Updates of processes stopped");
    }
}

impl ProcessesView {
    pub fn update(self: &mut Self, processes: Columns) {
        let prev_items = take(&mut self.items);

        // TODO: write some closure to extract the field with type propagation.
        for i in 0..processes.row_count() {
            let mut query_process = QueryProcess {
                host_name: processes.get::<_, _>(i, "host_name").expect("host_name"),
                user: processes.get::<_, _>(i, "user").expect("user"),
                threads: processes
                    .get::<Vec<u64>, _>(i, "thread_ids")
                    .expect("thread_ids")
                    .len(),
                memory: processes
                    .get::<_, _>(i, "peak_memory_usage")
                    .expect("peak_memory_usage"),
                elapsed: processes.get::<_, _>(i, "elapsed").expect("elapsed"),
                subqueries: processes.get::<_, _>(i, "subqueries").expect("subqueries"),
                is_initial_query: processes
                    .get::<u8, _>(i, "is_initial_query")
                    .expect("is_initial_query")
                    == 1,
                initial_query_id: processes
                    .get::<_, _>(i, "initial_query_id")
                    .expect("initial_query_id"),
                query_id: processes.get::<_, _>(i, "query_id").expect("query_id"),
                normalized_query: processes
                    .get::<_, _>(i, "normalized_query")
                    .expect("normalizeQuery"),
                original_query: processes
                    .get::<_, _>(i, "original_query")
                    .expect("original_query"),
                profile_events: processes
                    .get::<_, _>(i, "ProfileEvents")
                    .expect("ProfileEvents"),

                prev_elapsed: None,
                prev_profile_events: None,
            };

            if let Some(prev_item) = prev_items.get(&query_process.query_id) {
                query_process.prev_elapsed = Some(prev_item.elapsed);
                query_process.prev_profile_events = Some(prev_item.profile_events.clone());
            }

            self.items
                .insert(query_process.query_id.clone(), query_process);
        }

        self.update_table();
    }

    fn update_table(self: &mut Self) {
        let mut table_items = Vec::new();
        if let Some(query_id) = &self.query_id {
            for (_, query_process) in &self.items {
                if query_process.initial_query_id == *query_id {
                    table_items.push(query_process.clone());
                }
            }
        } else {
            for (_, query_process) in &self.items {
                if !self.options.no_group_by && !query_process.is_initial_query {
                    continue;
                }
                table_items.push(query_process.clone());
            }
        }
        if self.table.is_empty() {
            self.table.set_items_stable(table_items);
            // NOTE: this is not a good solution since in this case we cannot select always first
            // row if user did not select anything...
            self.table.set_selected_row(0);
        } else {
            self.table.set_items_stable(table_items);
        }
    }

    fn show_flamegraph(self: &mut Self, trace_type: Option<TraceType>) -> EventResult {
        if self.table.item().is_none() {
            return EventResult::Ignored;
        }

        let mut context_locked = self.context.lock().unwrap();
        let item_index = self.table.item().unwrap();
        let query_id = self.table.borrow_item(item_index).unwrap().query_id.clone();

        let mut query_ids = Vec::new();

        query_ids.push(query_id.clone());
        if !self.options.no_subqueries {
            for (_, query_process) in &self.items {
                if query_process.initial_query_id == *query_id {
                    query_ids.push(query_process.query_id.clone());
                }
            }
        }

        if let Some(trace_type) = trace_type {
            context_locked
                .worker
                .send(WorkerEvent::ShowQueryFlameGraph(trace_type, query_ids));
        } else {
            context_locked
                .worker
                .send(WorkerEvent::ShowLiveQueryFlameGraph(query_ids));
        }

        return EventResult::Consumed(None);
    }

    pub fn start(&mut self) {
        let context_copy = self.context.clone();
        let delay = self.options.delay_interval;
        let cv = self.cv.clone();
        // FIXME: more common way to do periodic job
        self.thread = Some(std::thread::spawn(move || loop {
            // Do not try to do anything if there is contention,
            // since likely means that there is some query already in progress.
            if let Ok(mut context_locked) = context_copy.try_lock() {
                // FIXME: we should not send any requests for updates if there is some update in
                // progress (and not only this but updates for any queries)
                context_locked.worker.send(WorkerEvent::UpdateProcessList);
                // FIXME: leaky abstraction
                context_locked.worker.send(WorkerEvent::UpdateSummary);
            }
            let result = cv.1.wait_timeout(cv.0.lock().unwrap(), delay).unwrap();
            let exit = *result.0;
            if exit {
                break;
            }
        }));
    }

    pub fn new(context: ContextArc) -> Result<Self> {
        let mut table = TableView::<QueryProcess, QueryProcessesColumn>::new()
            .column(QueryProcessesColumn::QueryId, "QueryId", |c| c.width(10))
            .column(QueryProcessesColumn::Cpu, "CPU", |c| c.width(8))
            .column(QueryProcessesColumn::User, "USER", |c| c.width(10))
            .column(QueryProcessesColumn::Threads, "TH", |c| c.width(6))
            .column(QueryProcessesColumn::Memory, "MEM", |c| c.width(6))
            .column(QueryProcessesColumn::DiskIO, "DISK", |c| c.width(7))
            .column(QueryProcessesColumn::NetIO, "NET", |c| c.width(6))
            .column(QueryProcessesColumn::Elapsed, "Elapsed", |c| c.width(11))
            .column(QueryProcessesColumn::Query, "Query", |c| c)
            .on_submit(|siv: &mut Cursive, _row: usize, _index: usize| {
                siv.add_layer(views::MenuPopup::new(Rc::new(
                    menu::Tree::new()
                        // NOTE: Keep it in sync with:
                        // - show_help_dialog()
                        // - fuzzy_shortcuts()
                        // - "Actions" menu
                        //
                        // NOTE: should not overlaps with global shortcuts (add_global_callback())
                        .leaf("Queries on shards(+)", |s| s.on_event(Event::Char('+')))
                        .leaf("Show query logs  (l)", |s| s.on_event(Event::Char('l')))
                        .leaf("Query details    (D)", |s| s.on_event(Event::Char('D')))
                        .leaf("CPU flamegraph   (C)", |s| s.on_event(Event::Char('C')))
                        .leaf("Real flamegraph  (R)", |s| s.on_event(Event::Char('R')))
                        .leaf("Memory flamegraph(M)", |s| s.on_event(Event::Char('M')))
                        .leaf("Live flamegraph  (L)", |s| s.on_event(Event::Char('L')))
                        .leaf("EXPLAIN PLAN     (e)", |s| s.on_event(Event::Char('e')))
                        .leaf("EXPLAIN PIPELINE (E)", |s| s.on_event(Event::Char('E')))
                        .leaf("Kill this query  (K)", |s| s.on_event(Event::Char('K'))),
                )));
            });

        table.sort_by(QueryProcessesColumn::Elapsed, Ordering::Greater);

        let view_options = context.lock().unwrap().options.view.clone();

        if !view_options.no_subqueries {
            table.insert_column(0, QueryProcessesColumn::SubQueries, "Q#", |c| c.width(5));
        }
        if context.lock().unwrap().options.clickhouse.cluster.is_some() {
            table.insert_column(0, QueryProcessesColumn::HostName, "HOST", |c| c.width(8));
        }

        // TODO: add loader until it is loading
        let mut view = ProcessesView {
            context,
            table,
            last_size: Vec2 { x: 1, y: 1 },
            items: HashMap::new(),
            query_id: None,
            options: view_options,
            thread: None,
            cv: Arc::new((Mutex::new(false), Condvar::new())),
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

    // TODO:
    // - pause/disable the table if the foreground view had been changed
    // - space - multiquery selection (KILL, flamegraphs, logs, ...)
    fn on_event(&mut self, event: Event) -> EventResult {
        match event {
            // Query actions
            Event::Char('-') => {
                self.query_id = None;
                self.update_table();
            }
            Event::Char('+') => {
                if self.table.item().is_none() {
                    return EventResult::Ignored;
                }

                let item_index = self.table.item().unwrap();
                let query_id = self.table.borrow_item(item_index).unwrap().query_id.clone();

                self.query_id = Some(query_id);
                self.update_table();
            }
            Event::Char('D') => {
                if self.table.item().is_none() {
                    return EventResult::Ignored;
                }

                let item_index = self.table.item().unwrap();
                let row = self.table.borrow_item(item_index).unwrap().clone();

                self.context
                    .lock()
                    .unwrap()
                    .cb_sink
                    .send(Box::new(move |siv: &mut cursive::Cursive| {
                        siv.add_layer(views::Dialog::around(
                            view::ProcessView::new(row)
                                .with_name("process")
                                .min_size((70, 35)),
                        ));
                    }))
                    .unwrap();
            }
            Event::Char('C') => {
                return self.show_flamegraph(Some(TraceType::CPU));
            }
            Event::Char('R') => {
                return self.show_flamegraph(Some(TraceType::Real));
            }
            Event::Char('M') => {
                return self.show_flamegraph(Some(TraceType::Memory));
            }
            Event::Char('L') => {
                return self.show_flamegraph(None);
            }
            Event::Char('e') => {
                if self.table.item().is_none() {
                    return EventResult::Ignored;
                }

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
                if self.table.item().is_none() {
                    return EventResult::Ignored;
                }

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
                if self.table.item().is_none() {
                    return EventResult::Ignored;
                }

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
                if self.table.item().is_none() {
                    return EventResult::Ignored;
                }

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
