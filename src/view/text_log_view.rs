use anyhow::Result;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration};
use chrono_tz::Tz;
use cursive::view::ViewWrapper;

use crate::interpreter::{clickhouse::Columns, BackgroundRunner, ContextArc, WorkerEvent};
use crate::view::{LogEntry, LogView};
use crate::wrap_impl_no_move;

pub type DateTime64 = DateTime<Tz>;
pub type DateTimeArc = Arc<Mutex<DateTime64>>;

pub struct TextLogView {
    inner_view: LogView,
    last_event_time_microseconds: DateTimeArc,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
}

impl TextLogView {
    pub fn new(
        context: ContextArc,
        min_query_start_microseconds: DateTime64,
        query_ids: Vec<String>,
    ) -> Self {
        // subtract one second since we have a common expression for the query start time and the
        // last available log and it is strict comparison
        //
        // NOTE: 1 second is not enough
        let min_query_start_microseconds = min_query_start_microseconds
            .checked_sub_signed(Duration::seconds(10))
            .unwrap();
        let last_event_time_microseconds = Arc::new(Mutex::new(min_query_start_microseconds));

        let delay = context.lock().unwrap().options.view.delay_interval;

        let update_query_ids = query_ids.clone();
        let update_last_event_time_microseconds = last_event_time_microseconds.clone();
        let update_callback_context = context.clone();
        let update_callback = move || {
            update_callback_context
                .lock()
                .unwrap()
                .worker
                .send(WorkerEvent::GetQueryTextLog(
                    update_query_ids.clone(),
                    *update_last_event_time_microseconds.lock().unwrap(),
                ));
        };

        let mut bg_runner = BackgroundRunner::new(delay);
        bg_runner.start(update_callback);

        let is_cluster = context.lock().unwrap().options.clickhouse.cluster.is_some();
        let view = TextLogView {
            inner_view: LogView::new(is_cluster),
            last_event_time_microseconds,
            bg_runner,
        };
        return view;
    }

    pub fn update(self: &mut Self, logs: Columns) -> Result<()> {
        let mut last_event_time_microseconds = self.last_event_time_microseconds.lock().unwrap();

        for i in 0..logs.row_count() {
            // TODO: add host for cluster mode
            let log_entry = LogEntry {
                level: logs.get::<_, _>(i, "level")?,
                message: logs.get::<_, _>(i, "message")?,
                event_time: logs.get::<_, _>(i, "event_time")?,
                event_time_microseconds: logs.get::<_, _>(i, "event_time_microseconds")?,
                host_name: logs.get::<_, _>(i, "host_name")?,
            };

            if *last_event_time_microseconds < log_entry.event_time_microseconds {
                *last_event_time_microseconds = log_entry.event_time_microseconds;
            }

            self.inner_view.push_logs(log_entry);
        }

        return Ok(());
    }
}

impl ViewWrapper for TextLogView {
    wrap_impl_no_move!(self.inner_view: LogView);
}
