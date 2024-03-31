use chrono::{DateTime, Local};
use cursive::{
    event::{Callback, Event, EventResult, Key},
    theme::{BaseColor, Color},
    utils::{
        lines::spans::{LinesIterator, Row},
        markup::StyledString,
    },
    view::{scroll, Nameable, Resizable, ScrollStrategy, Scrollable, SizeCache, View, ViewWrapper},
    views::{EditView, NamedView, OnEventView, ScrollView},
    wrap_impl, Cursive, Printer, Vec2, XY,
};
use unicode_width::UnicodeWidthStr;

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
    pub host_name: String,
    pub event_time: DateTime<Local>,
    pub event_time_microseconds: DateTime<Local>,
    pub thread_id: u64,
    pub level: String,
    pub message: String,
    // NOTE:
    // - logger_name maybe a bit overwhelming
}

impl LogEntry {
    fn to_styled_string(&self, cluster: bool, highlight: bool) -> StyledString {
        let mut line = StyledString::new();

        if cluster {
            line.append_plain(&format!("[{}] ", self.host_name));
        }

        line.append_plain(&format!(
            "{} [ {} ] <",
            self.event_time.format("%Y-%m-%d %H:%M:%S"),
            self.thread_id
        ));
        line.append_styled(self.level.as_str(), get_level_color(self.level.as_str()));
        line.append_plain("> ");
        if highlight {
            // TODO: better highlight (only the phrase itself, not the whole line?)
            line.append_styled(self.message.as_str(), BaseColor::Red.dark());
        } else {
            line.append_plain(self.message.as_str());
        }
        return line;
    }
}

pub struct LogViewBase {
    logs: Vec<LogEntry>,
    content: StyledString,
    rows: Vec<Row>,

    size_cache: Option<XY<SizeCache>>,
    width: Option<usize>,

    search_term: String,
    matched_line: Option<usize>,

    cluster: bool,
}

impl LogViewBase {
    fn search_forward(&mut self) -> Option<EventResult> {
        self.size_cache = None;

        if self.search_term.is_empty() {
            return Some(EventResult::consumed());
        }

        let matched_line = self.matched_line.map(|x| x + 1).unwrap_or_default();
        for (i, log) in self.logs.iter().enumerate().skip(matched_line) {
            if log.message.contains(&self.search_term) {
                self.matched_line = Some(i);
                break;
            }
        }

        log::trace!(
            "search_term: {}, matched_line: {:?} (next)",
            &self.search_term,
            self.matched_line,
        );
        return Some(EventResult::consumed());
    }

    fn search_backward(&mut self) -> Option<EventResult> {
        self.size_cache = None;

        if self.search_term.is_empty() {
            return Some(EventResult::consumed());
        }

        let line = self.matched_line.unwrap_or_default();
        for i in (0..line).rev().chain((line..self.logs.len()).rev()) {
            if self.logs[i].message.contains(&self.search_term) {
                self.matched_line = Some(i);
                break;
            }
        }

        log::trace!(
            "search_term: {}, matched_line: {:?} ({}..0][{}..{}] (prev)",
            &self.search_term,
            self.matched_line,
            line,
            self.logs.len(),
            line,
        );
        return Some(EventResult::consumed());
    }

    fn is_cache_valid(&self, size: Vec2) -> bool {
        match self.size_cache {
            None => false,
            Some(ref last) => last.x.accept(size.x) && last.y.accept(size.y),
        }
    }

    fn compute_content_and_rows(&mut self, size: Vec2) {
        if self.is_cache_valid(size) {
            return;
        }

        self.size_cache = None;

        if size.x == 0 {
            // Nothing we can do at this point.
            return;
        }

        log::trace!("Updating cache");

        self.content = StyledString::new();
        self.logs.iter().enumerate().for_each(|(i, row)| {
            let highlight = self.matched_line == Some(i);
            let line = row.to_styled_string(self.cluster, highlight);
            self.content.append(line);
            self.content.append("\n");
        });
        self.rows = LinesIterator::new(&self.content, size.x).collect();

        // Desired width
        self.width = if self.rows.iter().any(|row| row.is_wrapped) {
            // If any rows are wrapped, then require the full width.
            Some(size.x)
        } else {
            self.rows.iter().map(|row| row.width).max()
        };

        // The entire "virtual" size (includes all rows)
        let my_size = Vec2::new(self.width.unwrap_or(0), self.rows.len());

        // Build a fresh cache.
        self.size_cache = Some(SizeCache::build(my_size, size));
    }
}

pub struct LogView {
    inner_view: OnEventView<NamedView<ScrollView<LogViewBase>>>,
}

impl LogView {
    pub fn new(cluster: bool) -> Self {
        let v = LogViewBase {
            logs: Vec::new(),
            content: StyledString::new(),
            rows: Vec::new(),

            size_cache: None,
            width: None,

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

        let scroll_page =
            move |v: &mut NamedView<ScrollView<LogViewBase>>, e: &Event| -> Option<EventResult> {
                return Some(scroll::on_event(
                    &mut *v.get_mut(),
                    e.clone(),
                    |s: &mut ScrollView<LogViewBase>, e| s.on_event(e),
                    |s, si| s.important_area(si),
                ));
            };

        let reset_search =
            move |v: &mut NamedView<ScrollView<LogViewBase>>, e: &Event| -> Option<EventResult> {
                {
                    let mut base = v.get_mut();
                    let base = base.get_inner_mut();
                    base.matched_line = None;
                    base.search_term.clear();
                }
                return scroll_page(v, e);
            };

        let search_prompt_impl = |siv: &mut Cursive, forward: bool| {
            let find = move |siv: &mut Cursive, text: &str| {
                siv.call_on_name("logs", |v: &mut ScrollView<LogViewBase>| {
                    let base = v.get_inner_mut();

                    base.search_term = text.to_string();
                    base.matched_line = None;

                    if forward {
                        base.search_forward();
                    } else {
                        base.search_backward();
                    }
                });
                siv.pop_layer();
            };
            let view = OnEventView::new(EditView::new().on_submit(find).min_width(10));
            siv.add_layer(view);
        };
        let search_prompt_forward = move |siv: &mut Cursive| {
            search_prompt_impl(siv, /* forward= */ true);
        };
        let search_prompt_backward = move |siv: &mut Cursive| {
            search_prompt_impl(siv, /* forward= */ false);
        };

        let v = OnEventView::new(v)
            .on_pre_event_inner(Key::PageUp, reset_search)
            .on_pre_event_inner(Key::PageDown, reset_search)
            .on_pre_event_inner(Key::Left, reset_search)
            .on_pre_event_inner(Key::Right, reset_search)
            .on_pre_event_inner(Key::Up, reset_search)
            .on_pre_event_inner(Key::Down, reset_search)
            .on_pre_event_inner('j', reset_search)
            .on_pre_event_inner('k', reset_search)
            .on_event_inner('/', move |_, _| {
                return Some(EventResult::Consumed(Some(Callback::from_fn(
                    search_prompt_forward,
                ))));
            })
            .on_event_inner('?', move |_, _| {
                return Some(EventResult::Consumed(Some(Callback::from_fn(
                    search_prompt_backward,
                ))));
            })
            .on_event_inner('n', move |v, _| {
                let mut base = v.get_mut();
                let base = base.get_inner_mut();
                return base.search_forward();
            })
            .on_event_inner('N', move |v, _| {
                let mut base = v.get_mut();
                let base = base.get_inner_mut();
                return base.search_backward();
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
        self.inner_view
            .get_inner_mut()
            .get_mut()
            .get_inner_mut()
            .size_cache = None;
    }
}

impl View for LogViewBase {
    fn draw(&self, printer: &Printer<'_, '_>) {
        let printer = &printer.offset((0, 0));

        for (y, row) in self
            .rows
            .iter()
            .enumerate()
            .skip(printer.content_offset.y)
            .take(printer.output_size.y)
        {
            let mut x = 0;
            for span in row.resolve_stream(&self.content) {
                printer.with_style(*span.attr, |printer| {
                    printer.print((x, y), span.content);
                    x += span.content.width();
                });
            }
        }
    }

    fn needs_relayout(&self) -> bool {
        self.size_cache.is_none()
    }

    fn required_size(&mut self, size: Vec2) -> Vec2 {
        self.compute_content_and_rows(size);
        Vec2::new(self.width.unwrap_or(0), self.rows.len())
    }

    fn layout(&mut self, size: Vec2) {
        self.compute_content_and_rows(size);
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
