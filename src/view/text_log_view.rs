use anyhow::Result;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Local};
use chrono_tz::Tz;
use cursive::view::ViewWrapper;

use crate::interpreter::{clickhouse::Columns, BackgroundRunner, ContextArc, WorkerEvent};
use crate::view::{LogEntry, LogView};
use crate::wrap_impl_no_move;

pub type DateTime64 = DateTime<Local>;
pub type DateTimeArc = Arc<Mutex<DateTime64>>;

pub struct TextLogView {
    inner_view: LogView,
    last_event_time_microseconds: DateTimeArc,

    #[allow(unused)]
    bg_runner: Option<BackgroundRunner>,
}

// flush_interval_milliseconds for each *_log table from the config.xml/yml
const FLUSH_INTERVAL_MILLISECONDS: i64 = 7500;

impl TextLogView {
    pub fn new(
        view_name: &'static str,
        context: ContextArc,
        min_query_start_microseconds: DateTime64,
        max_query_end_microseconds: Option<DateTime64>,
        query_ids: Option<Vec<String>>,
    ) -> Self {
        let flush_interval_milliseconds =
            Duration::try_milliseconds(FLUSH_INTERVAL_MILLISECONDS).unwrap();
        let query_start_microseconds = min_query_start_microseconds;
        let last_event_time_microseconds = Arc::new(Mutex::new(query_start_microseconds));

        let delay = context.lock().unwrap().options.view.delay_interval;

        let mut bg_runner = None;
        // Start pulling only if the query did not finished, i.e. we don't know the end time.
        // (but respect the FLUSH_INTERVAL_MILLISECONDS)
        let now = Local::now();
        if max_query_end_microseconds.is_some()
            && (now - max_query_end_microseconds.unwrap()) >= flush_interval_milliseconds
        {
            context
                .lock()
                .unwrap()
                .worker
                .send(WorkerEvent::GetQueryTextLog(
                    view_name,
                    query_ids.clone(),
                    query_start_microseconds,
                    max_query_end_microseconds,
                ));
        } else {
            let update_query_ids = query_ids.clone();
            let update_last_event_time_microseconds = last_event_time_microseconds.clone();
            let update_callback_context = context.clone();
            let update_callback =
                move || {
                    update_callback_context.lock().unwrap().worker.send(
                        WorkerEvent::GetQueryTextLog(
                            view_name,
                            update_query_ids.clone(),
                            *update_last_event_time_microseconds.lock().unwrap(),
                            max_query_end_microseconds,
                        ),
                    );
                };

            let bg_runner_cv = context.lock().unwrap().background_runner_cv.clone();
            let mut created_bg_runner = BackgroundRunner::new(delay, bg_runner_cv);
            created_bg_runner.start(update_callback);
            bg_runner = Some(created_bg_runner);
        }

        let is_cluster = context.lock().unwrap().options.clickhouse.cluster.is_some();
        let wrap = context.lock().unwrap().options.view.wrap;
        let view = TextLogView {
            inner_view: LogView::new(is_cluster, wrap),
            last_event_time_microseconds,
            bg_runner,
        };
        return view;
    }

    pub fn update(&mut self, logs_block: Columns) -> Result<()> {
        let mut last_event_time_microseconds = self.last_event_time_microseconds.lock().unwrap();

        let mut logs = Vec::<LogEntry>::new();
        for i in 0..logs_block.row_count() {
            let log_entry = LogEntry {
                host_name: logs_block.get::<_, _>(i, "host_name")?,
                event_time: logs_block
                    .get::<DateTime<Tz>, _>(i, "event_time")?
                    .with_timezone(&Local),
                event_time_microseconds: logs_block
                    .get::<DateTime<Tz>, _>(i, "event_time_microseconds")?
                    .with_timezone(&Local),
                thread_id: logs_block.get::<_, _>(i, "thread_id")?,
                level: logs_block.get::<_, _>(i, "level")?,
                message: logs_block.get::<_, _>(i, "message")?,
            };

            if *last_event_time_microseconds < log_entry.event_time_microseconds {
                *last_event_time_microseconds = log_entry.event_time_microseconds;
            }

            logs.push(log_entry);
        }

        self.inner_view.push_logs(&mut logs);

        return Ok(());
    }
}

impl ViewWrapper for TextLogView {
    wrap_impl_no_move!(self.inner_view: LogView);
}
