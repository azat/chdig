use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::take;
use std::rc::Rc;

use anyhow::Result;
use cursive::traits::{Nameable, Resizable};
use cursive::{
    event::{Event, EventResult},
    inner_getters, menu,
    view::ViewWrapper,
    views, Cursive,
};
use size::{Base, SizeFormatter, Style};

use crate::interpreter::{
    clickhouse::Columns, clickhouse::TraceType, options::ViewOptions, BackgroundRunner, ContextArc,
    QueryProcess, WorkerEvent,
};
use crate::view::utils;
use crate::view::{ExtTableView, ProcessView, TableViewItem, TextLogView};
use crate::wrap_impl_no_move;

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum QueryProcessesColumn {
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
    table: ExtTableView<QueryProcess, QueryProcessesColumn>,
    items: HashMap<String, QueryProcess>,
    query_id: Option<String>,
    options: ViewOptions,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
}

impl ProcessesView {
    inner_getters!(self.table: ExtTableView<QueryProcess, QueryProcessesColumn>);

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

        self.update_view();
    }

    fn update_view(self: &mut Self) {
        let mut items = Vec::new();
        if let Some(query_id) = &self.query_id {
            for (_, query_process) in &self.items {
                if query_process.initial_query_id == *query_id {
                    items.push(query_process.clone());
                }
            }
        } else {
            for (_, query_process) in &self.items {
                if self.options.group_by && !query_process.is_initial_query {
                    continue;
                }
                items.push(query_process.clone());
            }
        }

        let inner_table = self.table.get_inner_mut();
        if inner_table.is_empty() {
            inner_table.set_items_stable(items);
            // NOTE: this is not a good solution since in this case we cannot select always first
            // row if user did not select anything...
            inner_table.set_selected_row(0);
        } else {
            inner_table.set_items_stable(items);
        }
    }

    fn show_flamegraph(self: &mut Self, trace_type: Option<TraceType>) -> EventResult {
        let inner_table = self.table.get_inner_mut();

        if inner_table.item().is_none() {
            return EventResult::Ignored;
        }

        let mut context_locked = self.context.lock().unwrap();
        let item_index = inner_table.item().unwrap();
        let query_id = inner_table
            .borrow_item(item_index)
            .unwrap()
            .query_id
            .clone();

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

    pub fn new(context: ContextArc) -> Result<Self> {
        let delay = context.lock().unwrap().options.view.delay_interval;

        let update_callback_context = context.clone();
        let update_callback = move || {
            if let Ok(mut context_locked) = update_callback_context.try_lock() {
                context_locked.worker.send(WorkerEvent::UpdateProcessList);
                context_locked.worker.send(WorkerEvent::UpdateSummary);
            }
        };

        let mut table = ExtTableView::<QueryProcess, QueryProcessesColumn>::default();
        let inner_table = table.get_inner_mut();
        inner_table.add_column(QueryProcessesColumn::QueryId, "QueryId", |c| c.width(10));
        inner_table.add_column(QueryProcessesColumn::Cpu, "CPU", |c| c.width(8));
        inner_table.add_column(QueryProcessesColumn::User, "USER", |c| c.width(10));
        inner_table.add_column(QueryProcessesColumn::Threads, "TH", |c| c.width(6));
        inner_table.add_column(QueryProcessesColumn::Memory, "MEM", |c| c.width(6));
        inner_table.add_column(QueryProcessesColumn::DiskIO, "DISK", |c| c.width(7));
        inner_table.add_column(QueryProcessesColumn::NetIO, "NET", |c| c.width(6));
        inner_table.add_column(QueryProcessesColumn::Elapsed, "Elapsed", |c| c.width(11));
        inner_table.add_column(QueryProcessesColumn::Query, "Query", |c| c);
        inner_table.set_on_submit(|siv: &mut Cursive, _row: usize, _index: usize| {
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

        inner_table.sort_by(QueryProcessesColumn::Elapsed, Ordering::Greater);

        let view_options = context.lock().unwrap().options.view.clone();

        if !view_options.no_subqueries {
            inner_table.insert_column(0, QueryProcessesColumn::SubQueries, "Q#", |c| c.width(5));
        }
        if context.lock().unwrap().options.clickhouse.cluster.is_some() {
            inner_table.insert_column(0, QueryProcessesColumn::HostName, "HOST", |c| c.width(8));
        }

        let mut bg_runner = BackgroundRunner::new(delay);
        bg_runner.start(update_callback);

        let view = ProcessesView {
            context,
            table,
            items: HashMap::new(),
            query_id: None,
            options: view_options,
            bg_runner,
        };
        return Ok(view);
    }
}

impl ViewWrapper for ProcessesView {
    wrap_impl_no_move!(self.table: ExtTableView<QueryProcess, QueryProcessesColumn>);

    // TODO:
    // - pause/disable the table if the foreground view had been changed
    // - space - multiquery selection (KILL, flamegraphs, logs, ...)
    fn wrap_on_event(&mut self, event: Event) -> EventResult {
        let inner_table = self.table.get_inner_mut();

        match event {
            // Query actions
            Event::Char('-') => {
                self.query_id = None;
                self.update_view();
            }
            Event::Char('+') => {
                if inner_table.item().is_none() {
                    return EventResult::Ignored;
                }

                let item_index = inner_table.item().unwrap();
                let query_id = inner_table
                    .borrow_item(item_index)
                    .unwrap()
                    .query_id
                    .clone();

                self.query_id = Some(query_id);
                self.update_view();
            }
            Event::Char('D') => {
                if inner_table.item().is_none() {
                    return EventResult::Ignored;
                }

                let item_index = inner_table.item().unwrap();
                let row = inner_table.borrow_item(item_index).unwrap().clone();

                self.context
                    .lock()
                    .unwrap()
                    .cb_sink
                    .send(Box::new(move |siv: &mut cursive::Cursive| {
                        siv.add_layer(views::Dialog::around(
                            ProcessView::new(row)
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
                if inner_table.item().is_none() {
                    return EventResult::Ignored;
                }

                let mut context_locked = self.context.lock().unwrap();
                let item_index = inner_table.item().unwrap();
                let query = inner_table
                    .borrow_item(item_index)
                    .unwrap()
                    .original_query
                    .clone();
                context_locked.worker.send(WorkerEvent::ExplainPlan(query));
            }
            Event::Char('E') => {
                if inner_table.item().is_none() {
                    return EventResult::Ignored;
                }

                let mut context_locked = self.context.lock().unwrap();
                let item_index = inner_table.item().unwrap();
                let query = inner_table
                    .borrow_item(item_index)
                    .unwrap()
                    .original_query
                    .clone();
                context_locked
                    .worker
                    .send(WorkerEvent::ExplainPipeline(query));
            }
            Event::Char('K') => {
                if inner_table.item().is_none() {
                    return EventResult::Ignored;
                }

                let item_index = inner_table.item().unwrap();
                let query_id = inner_table
                    .borrow_item(item_index)
                    .unwrap()
                    .query_id
                    .clone();
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
                if inner_table.item().is_none() {
                    return EventResult::Ignored;
                }

                let item_index = inner_table.item().unwrap();
                let item = inner_table.borrow_item(item_index).unwrap();
                let query_id = item.query_id.clone();
                let original_query = item.original_query.clone();
                let context_copy = self.context.clone();

                self.context
                    .lock()
                    .unwrap()
                    .cb_sink
                    .send(Box::new(move |siv: &mut cursive::Cursive| {
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
                                        TextLogView::new(context_copy, query_id.clone()),
                                    ))
                                    .scroll_x(true),
                                ),
                        ));
                    }))
                    .unwrap();
            }
            _ => {}
        }
        return self.get_inner_mut().wrap_on_event(event);
    }
}
