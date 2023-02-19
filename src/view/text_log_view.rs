use chrono::DateTime;
use chrono_tz::Tz;
use cursive::view::View;
use cursive::Printer;
use cursive::{theme, Vec2};
use std::cmp::max;
use std::thread;

use crate::interpreter::{ContextArc, WorkerEvent};

pub struct TextLogView {
    context: ContextArc,
    query_id: String,
    thread: Option<thread::JoinHandle<()>>,
}

impl TextLogView {
    pub fn new(context: ContextArc, query_id: String) -> Self {
        let mut view = TextLogView {
            context: context.clone(),
            query_id: query_id.clone(),
            thread: None,
        };
        context
            .lock()
            .unwrap()
            .worker
            .send(WorkerEvent::GetQueryTextLog(query_id, None));
        view.start();
        return view;
    }

    pub fn start(&mut self) {
        let context_copy = self.context.clone();
        let delay = self.context.lock().unwrap().options.view.delay_interval;
        let query_id = self.query_id.clone();

        // FIXME: more common way to do periodic job
        self.thread = Some(std::thread::spawn(move || loop {
            // Do not try to do anything if there is contention,
            // since likely means that there is some query already in progress.
            if let Ok(mut context_locked) = context_copy.try_lock() {
                let mut event_time_microseconds = None;
                if let Some(logs) = context_locked.query_logs.as_mut() {
                    for i in 0..logs.row_count() {
                        let current = logs
                            .get::<DateTime<Tz>, _>(i, "event_time_microseconds")
                            .expect("event_time_microseconds");
                        if event_time_microseconds.is_none() {
                            event_time_microseconds = Some(current);
                        } else {
                            if current > event_time_microseconds.unwrap() {
                                event_time_microseconds = Some(current);
                            }
                        }
                    }
                }

                // FIXME: Append to the view table is not implemented, hence no need to apply
                // filter for text_log
                event_time_microseconds = None;

                context_locked.worker.send(WorkerEvent::GetQueryTextLog(
                    query_id.clone(),
                    event_time_microseconds,
                ));
            }
            thread::sleep(delay);
        }));
    }
}

impl View for TextLogView {
    fn draw(&self, printer: &Printer) {
        let mut context_locked = self.context.lock().unwrap();
        if let Some(logs) = context_locked.query_logs.as_mut() {
            for i in 0..logs.row_count() {
                let level = logs.get::<String, _>(i, "level").unwrap();
                // TODO: add logger_name
                let message = logs.get::<String, _>(i, "message").unwrap();
                let event_time = logs.get::<String, _>(i, "event_time").unwrap();

                printer.print(
                    (0, i),
                    //             "Information  "
                    //             ^^^^^^^^^^^^^^^
                    &format!("{} | [             ] {}", event_time, message),
                );
                // TODO:
                // - better coloring
                // - use the same color schema as ClickHouse (not only for level)
                let color = match level.as_str() {
                    "Fatal" => theme::BaseColor::Red.dark(),
                    "Critical" => theme::BaseColor::Red.dark(),
                    "Error" => theme::BaseColor::Red.dark(),
                    "Warning" => theme::BaseColor::Blue.dark(),
                    "Notice" => theme::BaseColor::Yellow.dark(),
                    "Information" => theme::BaseColor::Blue.dark(),
                    "Debug" => theme::BaseColor::White.dark(),
                    "Trace" => theme::BaseColor::White.dark(),
                    "Test" => theme::BaseColor::White.dark(),
                    _ => panic!("Unknown level {}", level),
                };
                printer.with_color(color.into(), |printer| {
                    let time_width = "1970-01-01 00:00:00 | [ ".len();
                    printer.print((time_width, i), &format!("{} ", level))
                });
            }
        }
    }

    fn required_size(&mut self, _constraint: Vec2) -> Vec2 {
        let mut context_locked = self.context.lock().unwrap();
        if let Some(logs) = context_locked.query_logs.as_mut() {
            let level_width = " Information ".len();
            let time_width = "1970-01-01 00:00:00 ".len();

            // The longest line sets the width
            let mut max_width = 0;
            for i in 0..logs.row_count() {
                let message = logs.get::<String, _>(i, "message").unwrap();
                max_width = max(max_width, message.len());
            }

            let h = logs.row_count();

            return Vec2::new(max_width + level_width + time_width, h);
        }
        return Vec2::new(0, 0);
    }
}
