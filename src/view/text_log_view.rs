use anyhow::Result;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Local};
use chrono_tz::Tz;
use cursive::view::ViewWrapper;

use crate::common::RelativeDateTime;
use crate::interpreter::{
    BackgroundRunner, ContextArc, TextLogArguments, WorkerEvent, clickhouse::Columns,
};
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
    pub fn new(view_name: &'static str, context: ContextArc, args: TextLogArguments) -> Self {
        let flush_interval_milliseconds =
            Duration::try_milliseconds(FLUSH_INTERVAL_MILLISECONDS).unwrap();
        let start = args.start;
        let end = args.end.clone();
        let query_ids = args.query_ids.clone();
        let logger_names = args.logger_names.clone();
        let message_filter = args.message_filter.clone();
        let max_level = args.max_level.clone();
        let last_event_time_microseconds = Arc::new(Mutex::new(start));

        let delay = context.lock().unwrap().options.view.delay_interval;

        let mut bg_runner = None;
        // Start pulling only if the query did not finished, i.e. we don't know the end time.
        // (but respect the FLUSH_INTERVAL_MILLISECONDS)
        let now = Local::now();
        if logger_names.is_none()
            && let Some(mut end) = end.get_date_time()
            && ((now - end) >= flush_interval_milliseconds || query_ids.is_none())
        {
            // It is possible to have messages in the system.text_log, whose
            // event_time_microseconds > max(event_time_microseconds) from system.query_log
            // But let's consider that 3 seconds is enough.
            if query_ids.is_some() {
                end += Duration::try_seconds(3).unwrap();
            }
            context.lock().unwrap().worker.send(
                true,
                WorkerEvent::TextLog(
                    view_name,
                    TextLogArguments {
                        query_ids: query_ids.clone(),
                        logger_names: None,
                        message_filter: message_filter.clone(),
                        max_level: max_level.clone(),
                        start,
                        end: RelativeDateTime::from(end),
                    },
                ),
            );
        } else {
            let update_query_ids = query_ids.clone();
            let update_logger_names = logger_names.clone();
            let update_message_filter = message_filter.clone();
            let update_max_level = max_level.clone();
            let update_last_event_time_microseconds = last_event_time_microseconds.clone();
            let update_callback_context = context.clone();

            let is_first_invocation = Arc::new(Mutex::new(true));
            let update_callback = move |force: bool| {
                let mut is_first = is_first_invocation.lock().unwrap();
                let effective_force = if *is_first {
                    *is_first = false;
                    true
                } else {
                    force
                };

                update_callback_context.lock().unwrap().worker.send(
                    effective_force,
                    WorkerEvent::TextLog(
                        view_name,
                        TextLogArguments {
                            query_ids: update_query_ids.clone(),
                            logger_names: update_logger_names.clone(),
                            message_filter: update_message_filter.clone(),
                            max_level: update_max_level.clone(),
                            start: *update_last_event_time_microseconds.lock().unwrap(),
                            end: end.clone(),
                        },
                    ),
                );
            };

            let bg_runner_cv = context.lock().unwrap().background_runner_cv.clone();
            let bg_runner_force = context.lock().unwrap().background_runner_force.clone();
            let mut created_bg_runner = BackgroundRunner::new(delay, bg_runner_cv, bg_runner_force);
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
                event_time_microseconds: logs_block
                    .get::<DateTime<Tz>, _>(i, "event_time_microseconds")?
                    .with_timezone(&Local),
                thread_id: logs_block.get::<_, _>(i, "thread_id")?,
                level: logs_block.get::<_, _>(i, "level")?,
                message: logs_block.get::<_, _>(i, "message")?,
                query_id: logs_block.get::<_, _>(i, "query_id").ok(),
                logger_name: logs_block.get::<_, _>(i, "logger_name").ok(),
            };

            if *last_event_time_microseconds < log_entry.event_time_microseconds {
                *last_event_time_microseconds = log_entry.event_time_microseconds;
            }

            logs.push(log_entry);
        }

        self.inner_view.push_logs(&logs);

        return Ok(());
    }
}

impl ViewWrapper for TextLogView {
    wrap_impl_no_move!(self.inner_view: LogView);
}
