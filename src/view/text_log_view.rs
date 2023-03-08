use cursive::view::ViewWrapper;

use crate::interpreter::{clickhouse::Columns, BackgroundRunner, ContextArc, WorkerEvent};
use crate::view::{LogEntry, LogView};
use crate::wrap_impl_no_move;

pub struct TextLogView {
    inner_view: LogView,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
}

impl TextLogView {
    pub fn new(context: ContextArc, query_id: String) -> Self {
        let delay = context.lock().unwrap().options.view.delay_interval;

        let update_query_id = query_id.clone();
        let update_callback_context = context.clone();
        let update_callback = move || {
            if let Ok(mut context_locked) = update_callback_context.try_lock() {
                context_locked
                    .worker
                    .send(WorkerEvent::GetQueryTextLog(update_query_id.clone(), None));
            }
        };

        let mut bg_runner = BackgroundRunner::new(delay);
        bg_runner.start(update_callback);

        let view = TextLogView {
            inner_view: LogView::new(),
            bg_runner,
        };
        context
            .lock()
            .unwrap()
            .worker
            .send(WorkerEvent::GetQueryTextLog(query_id, None));
        return view;
    }

    pub fn update(self: &mut Self, logs: Columns) {
        // FIXME: now we can make incremental logs
        self.inner_view.logs.clear();

        for i in 0..logs.row_count() {
            self.inner_view.logs.push(LogEntry {
                level: logs.get::<String, _>(i, "level").unwrap(),
                message: logs.get::<String, _>(i, "message").unwrap(),
                event_time: logs.get::<String, _>(i, "event_time").unwrap(),
            });
        }
    }
}

impl ViewWrapper for TextLogView {
    wrap_impl_no_move!(self.inner_view: LogView);
}
