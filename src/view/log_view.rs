use chrono::DateTime;
use chrono_tz::Tz;
use cursive::{
    event::{Callback, EventResult, Key},
    theme::{BaseColor, Color},
    utils::markup::StyledString,
    view::{Nameable, Resizable, ScrollStrategy, Scrollable, View, ViewWrapper},
    views::{EditView, NamedView, OnEventView, ScrollView},
    wrap_impl, Cursive, Printer, Vec2,
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

pub struct LogViewBase {
    pub logs: Vec<LogEntry>,
    search_term: String,
    matched_line: Option<usize>,
    cluster: bool,
}

pub struct LogView {
    inner_view: OnEventView<NamedView<ScrollView<LogViewBase>>>,
}

impl LogView {
    pub fn new(cluster: bool) -> Self {
        let v = LogViewBase {
            logs: Vec::new(),
            search_term: String::new(),
            matched_line: None,
            cluster,
        };
        let v = v
            .scrollable()
            .scroll_strategy(ScrollStrategy::StickToBottom)
            .scroll_x(true);
        // NOTE: we cannot pass mutable ref to view in search_prompt callback, sigh.
        let v = v.with_name("logs");

        let reset_search =
            |v: &mut NamedView<ScrollView<LogViewBase>>, _: &_| -> Option<EventResult> {
                let mut base = v.get_mut();
                let base = base.get_inner_mut();
                base.matched_line = None;
                base.search_term.clear();
                return None;
            };

        let v = OnEventView::new(v)
            // TODO: scroll the whole page
            .on_pre_event_inner(Key::PageUp, reset_search)
            .on_pre_event_inner(Key::PageDown, reset_search)
            .on_pre_event_inner(Key::Up, reset_search)
            .on_pre_event_inner(Key::Down, reset_search)
            .on_pre_event_inner('j', reset_search)
            .on_pre_event_inner('k', reset_search)
            .on_event_inner('/', |_, _| {
                let search_prompt = Callback::from_fn(|siv| {
                    let find = |siv: &mut Cursive, text: &str| {
                        siv.call_on_name("logs", |v: &mut ScrollView<LogViewBase>| {
                            let base = v.get_inner_mut();

                            base.search_term = text.to_string();
                            base.matched_line = None;

                            if !text.is_empty() {
                                for (i, log) in base.logs.iter().enumerate() {
                                    if log.message.contains(text) {
                                        base.matched_line = Some(i);
                                        break;
                                    }
                                }
                            }

                            log::trace!(
                                "search_term: {}, matched_line: {:?}",
                                &text,
                                base.matched_line,
                            );
                        });
                        siv.pop_layer();
                    };
                    let view = OnEventView::new(EditView::new().on_submit(find).min_width(10));
                    siv.add_layer(view);
                });
                return Some(EventResult::Consumed(Some(search_prompt)));
            })
            .on_event_inner('n', |v, _| {
                let mut base = v.get_mut();
                let base = base.get_inner_mut();

                if base.search_term.is_empty() {
                    return Some(EventResult::consumed());
                }

                let matched_line = base.matched_line.unwrap() + 1;
                for (i, log) in base.logs.iter().enumerate().skip(matched_line) {
                    if log.message.contains(&base.search_term) {
                        base.matched_line = Some(i);
                        break;
                    }
                }

                log::trace!(
                    "search_term: {}, matched_line: {:?} (next)",
                    &base.search_term,
                    base.matched_line,
                );
                return Some(EventResult::consumed());
            });

        let log_view = LogView { inner_view: v };
        return log_view;
    }

    pub fn push_logs(&mut self, entry: LogEntry) {
        self.inner_view
            .get_inner_mut()
            .get_mut()
            .get_inner_mut()
            .logs
            .push(entry);
    }
}

impl View for LogViewBase {
    fn draw(&self, printer: &Printer) {
        // TODO: re-render only last lines, otherwise it is too CPU costly, since cursive re-render
        // each 0.2 sec
        for (i, log) in self.logs.iter().enumerate() {
            let mut line = StyledString::new();

            if self.cluster {
                line.append_plain(&format!("[{}] ", log.host_name));
            }

            line.append_plain(&format!("{} <", log.event_time.format("%Y-%m-%d %H:%M:%S")));
            line.append_styled(log.level.as_str(), get_level_color(log.level.as_str()));
            line.append_plain("> ");
            if self.matched_line.is_some() && i == self.matched_line.unwrap() {
                // TODO: better highlight (only the phrase itself, not the whole line?)
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
        let mut host_width = 0;
        let mut message_width = 0;

        for log in &self.logs {
            message_width = max(message_width, log.message.len());
            if self.cluster {
                host_width = max(host_width, log.host_name.len() + 3 /* [{} ] */);
            }
        }
        let h = self.logs.len();

        return Vec2::new(message_width + host_width + level_width + time_width, h);
    }

    fn needs_relayout(&self) -> bool {
        return false;
    }
}

impl ViewWrapper for LogView {
    wrap_impl!(self.inner_view: OnEventView<NamedView<ScrollView<LogViewBase>>>);

    // Scroll to the search phrase
    fn wrap_layout(&mut self, size: Vec2) {
        self.with_view_mut(|v| v.layout(size));

        self.inner_view.get_inner_mut().with_view_mut(|v| {
            let matched_line = v.get_inner_mut().matched_line;
            if let Some(matched_line) = matched_line {
                v.set_offset((0, matched_line));
            }
        });
    }
}
