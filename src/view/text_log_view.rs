use std::sync::{Arc, Mutex};

use chrono::DateTime;
use chrono_tz::Tz;
use cursive::view::ViewWrapper;

use crate::interpreter::{clickhouse::Columns, BackgroundRunner, ContextArc, WorkerEvent};
use crate::view::{LogEntry, LogView};
use crate::wrap_impl_no_move;

pub type DateTimeArc = Arc<Mutex<Option<DateTime<Tz>>>>;

pub struct TextLogView {
    inner_view: LogView,
    last_event_time_microseconds: DateTimeArc,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
}

impl TextLogView {
    pub fn new(context: ContextArc, query_id: String) -> Self {
        let last_event_time_microseconds = Arc::new(Mutex::new(None));

        let delay = context.lock().unwrap().options.view.delay_interval;

        let update_query_id = query_id.clone();
        let update_last_event_time_microseconds = last_event_time_microseconds.clone();
        let update_callback_context = context.clone();
        let update_callback = move || {
            update_callback_context
                .lock()
                .unwrap()
                .worker
                .send(WorkerEvent::GetQueryTextLog(
                    update_query_id.clone(),
                    *update_last_event_time_microseconds.lock().unwrap(),
                ));
        };

        let mut bg_runner = BackgroundRunner::new(delay);
        bg_runner.start(update_callback);

        let view = TextLogView {
            inner_view: LogView::new(),
            last_event_time_microseconds,
            bg_runner,
        };
        return view;
    }

    pub fn update(self: &mut Self, logs: Columns) {
        let mut last_event_time_microseconds = self.last_event_time_microseconds.lock().unwrap();

        for i in 0..logs.row_count() {
            let log_entry = LogEntry {
                level: logs.get::<_, _>(i, "level").unwrap(),
                message: logs.get::<_, _>(i, "message").unwrap(),
                event_time: logs.get::<_, _>(i, "event_time").unwrap(),
                event_time_microseconds: logs.get::<_, _>(i, "event_time_microseconds").unwrap(),
            };

            if last_event_time_microseconds.is_none() {
                *last_event_time_microseconds = Some(log_entry.event_time_microseconds);
            } else if last_event_time_microseconds.unwrap() < log_entry.event_time_microseconds {
                *last_event_time_microseconds = Some(log_entry.event_time_microseconds);
            }

            self.inner_view.logs.push(log_entry);
        }
    }
}

impl ViewWrapper for TextLogView {
    wrap_impl_no_move!(self.inner_view: LogView);
}
