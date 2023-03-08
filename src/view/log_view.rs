use cursive::view::View;
use cursive::Printer;
use cursive::{theme, Vec2};
use std::cmp::max;

pub struct LogEntry {
    pub level: String,
    pub message: String,
    // TODO: use chrono::DateTime<Tz>
    pub event_time: String,
    // TODO: add logger_name
}

#[derive(Default)]
pub struct LogView {
    pub logs: Vec<LogEntry>,
}

impl LogView {
    pub fn new() -> Self {
        return LogView::default();
    }
}

impl View for LogView {
    fn draw(&self, printer: &Printer) {
        for (i, log) in self.logs.iter().enumerate() {
            printer.print(
                (0, i),
                //             "Information  "
                //             ^^^^^^^^^^^^^^^
                &format!("{} | [             ] {}", log.event_time, log.message),
            );
            // TODO:
            // - better coloring
            // - use the same color schema as ClickHouse (not only for level)
            let color = match log.level.as_str() {
                "Fatal" => theme::BaseColor::Red.dark(),
                "Critical" => theme::BaseColor::Red.dark(),
                "Error" => theme::BaseColor::Red.dark(),
                "Warning" => theme::BaseColor::Blue.dark(),
                "Notice" => theme::BaseColor::Yellow.dark(),
                "Information" => theme::BaseColor::Blue.dark(),
                "Debug" => theme::BaseColor::White.dark(),
                "Trace" => theme::BaseColor::White.dark(),
                "Test" => theme::BaseColor::White.dark(),
                _ => panic!("Unknown level {}", log.level),
            };
            printer.with_color(color.into(), |printer| {
                let time_width = "1970-01-01 00:00:00 | [ ".len();
                printer.print((time_width, i), &format!("{} ", log.level))
            });
        }
    }

    fn required_size(&mut self, _constraint: Vec2) -> Vec2 {
        let level_width = " Information ".len();
        let time_width = "1970-01-01 00:00:00 ".len();
        let mut max_width = 0;

        // The longest line sets the width
        for log in &self.logs {
            max_width = max(max_width, log.message.len());
        }
        let h = self.logs.len();

        return Vec2::new(max_width + level_width + time_width, h);
    }
}
