use chrono::DateTime;
use chrono_tz::Tz;
use cursive::{
    event::{Callback, EventResult},
    theme::{BaseColor, Color},
    utils::markup::StyledString,
    view::{Nameable, Resizable, View},
    views::{EditView, NamedView, OnEventView},
    Cursive, Printer, Vec2,
};
use std::cmp::max;

fn get_level_color(level: &str) -> Color {
    // TODO:
    // - better coloring
    // - use the same color schema as ClickHouse (not only for level)
    match level {
        // NOTE: not all terminals support dark()
        "Fatal" => return BaseColor::Red.light(),
        "Critical" => return BaseColor::Red.light(),
        "Error" => return BaseColor::Red.light(),
        "Warning" => return BaseColor::Blue.light(),
        "Notice" => return BaseColor::Yellow.light(),
        "Information" => return BaseColor::Blue.light(),
        "Debug" => return BaseColor::White.light(),
        "Trace" => return BaseColor::White.light(),
        "Test" => return BaseColor::White.light(),
        _ => panic!("Unknown level {}", level),
    };
}

pub struct LogEntry {
    pub level: String,
    pub message: String,
    pub event_time: DateTime<Tz>,
    pub event_time_microseconds: DateTime<Tz>,
    pub host_name: String,
    // NOTE:
    // - logger_name maybe a bit overwhelming
}

#[derive(Default)]
pub struct LogView {
    pub logs: Vec<LogEntry>,
    search_term: String,
    cluster: bool,
}

impl LogView {
    pub fn new(cluster: bool) -> OnEventView<NamedView<LogView>> {
        let log_view = LogView {
            logs: Vec::new(),
            search_term: String::new(),
            cluster,
        };
        let named = log_view.with_name("logs");
        // TODO: 'n' - next result
        let event_view = OnEventView::new(named).on_event_inner('/', |_, _| {
            let search_prompt = Callback::from_fn(|siv| {
                let find = |siv: &mut Cursive, text: &str| {
                    siv.call_on_name("logs", |v: &mut LogView| {
                        v.search_term = text.to_string();
                    });
                    siv.pop_layer();
                };
                let view = OnEventView::new(EditView::new().on_submit(find).min_width(10));
                siv.add_layer(view);
            });
            return Some(EventResult::Consumed(Some(search_prompt)));
        });
        return event_view;
    }
}

impl View for LogView {
    fn draw(&self, printer: &Printer) {
        for (i, log) in self.logs.iter().enumerate() {
            let mut line = StyledString::new();

            if self.cluster {
                line.append_plain(&format!("[{}] ", log.host_name));
            }

            line.append_plain(&format!("{} <", log.event_time.format("%Y-%m-%d %H:%M:%S")));
            line.append_styled(log.level.as_str(), get_level_color(log.level.as_str()));
            line.append_plain("> ");
            if !self.search_term.is_empty() && log.message.contains(&self.search_term) {
                // TODO:
                // - better highlight
                // - scroll to this line
                line.append_styled(log.message.as_str(), BaseColor::Red.dark());
            } else {
                line.append_plain(log.message.as_str());
            }

            printer.print_styled((0, i), &line);
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
