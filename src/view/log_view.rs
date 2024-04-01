use chrono::{DateTime, Local};
use cursive::{
    event::{Callback, Event, EventResult, Key},
    theme::{BaseColor, Color, ColorStyle},
    utils::{
        lines::spans::{LinesIterator, Row},
        markup::StyledString,
    },
    view::{scroll, Nameable, Resizable, ScrollStrategy, View, ViewWrapper},
    views::{EditView, NamedView, OnEventView},
    wrap_impl, Cursive, Printer, Vec2,
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
    fn to_styled_string(&self, cluster: bool) -> StyledString {
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
        line.append_plain(self.message.as_str());
        return line;
    }
}

#[derive(Default)]
pub struct LogViewBase {
    logs: Vec<LogEntry>,
    content: StyledString,
    rows: Option<Vec<Row>>,
    max_width: usize,

    cached_size: Vec2,

    needs_relayout: bool,
    update_content: bool,
    scroll_core: scroll::Core,

    search_direction_forward: bool,
    search_term: String,
    matched_row: Option<usize>,

    cluster: bool,
    wrap: bool,
}

cursive::impl_scroller!(LogViewBase::scroll_core);

impl LogViewBase {
    fn update_search_forward(&mut self) -> Option<EventResult> {
        if self.search_term.is_empty() {
            return Some(EventResult::consumed());
        }

        let matched_row = self.matched_row.map(|x| x + 1).unwrap_or_default();
        if let Some(rows) = self.rows.as_ref() {
            for i in (matched_row..rows.len())
                // wrap search from the beginning
                .chain(0..matched_row)
            {
                let mut matched = false;
                for span in rows[i].resolve_stream(&self.content) {
                    if span.content.contains(&self.search_term) {
                        self.matched_row = Some(i);
                        matched = true;
                        break;
                    }
                }
                if matched {
                    break;
                }
            }
        }

        log::trace!(
            "search_term: {}, matched_row: {:?} (forward-search)",
            &self.search_term,
            self.matched_row,
        );
        return Some(EventResult::consumed());
    }

    fn update_search_reverse(&mut self) -> Option<EventResult> {
        if self.search_term.is_empty() {
            return Some(EventResult::consumed());
        }

        let matched_row = self.matched_row.unwrap_or_default();
        if let Some(rows) = self.rows.as_ref() {
            for i in (0..matched_row)
                .rev()
                // wrap search to the beginning
                .chain((matched_row..rows.len()).rev())
            {
                let mut matched = false;
                for span in rows[i].resolve_stream(&self.content) {
                    if span.content.contains(&self.search_term) {
                        self.matched_row = Some(i);
                        matched = true;
                        break;
                    }
                }
                if matched {
                    break;
                }
            }
        }

        log::trace!(
            "search_term: {}, matched_row: {:?} (reverse-search)",
            &self.search_term,
            self.matched_row,
        );
        return Some(EventResult::consumed());
    }

    fn update_search(&mut self) -> Option<EventResult> {
        // In case of resize we can have less rows then before,
        // so reset the matched_row for this scenario to avoid out-of-bound access.
        if let Some(rows) = self.rows.as_ref() {
            if rows.len() < self.matched_row.unwrap_or_default() {
                self.matched_row = None;
            }
        }
        if self.search_direction_forward {
            return self.update_search_forward();
        } else {
            return self.update_search_reverse();
        }
    }

    fn push_logs(&mut self, logs: &mut Vec<LogEntry>) {
        let new_rows = logs.len();
        self.logs.append(logs);
        log::trace!("Add {} log entries (total {})", new_rows, self.logs.len());

        // Increment content update
        for log in self.logs.iter().skip(self.logs.len() - new_rows) {
            let mut line = log.to_styled_string(self.cluster);
            line.append("\n");

            self.content.append(line.clone());
        }

        self.needs_relayout = true;
        self.compute_rows();
    }

    fn compute_rows(&mut self) {
        let size = if self.wrap {
            self.cached_size
        } else {
            Vec2::max_value()
        };
        // NOTE: incremental update is not possible (since the references in the rows to the
        // content will be wrong)
        let mut max_width = 0;
        let rows = LinesIterator::new(&self.content, size.x)
            .map(|x| {
                max_width = usize::max(max_width, x.width);
                return x;
            })
            .collect::<Vec<Row>>();
        log::trace!(
            "Updating rows cache (size: {:?}, wrap: {}, width: {}, rows: {}, inner size: {:?}, last size: {:?})",
            size,
            self.wrap,
            max_width,
            rows.len(),
            self.scroll_core.inner_size(),
            self.scroll_core.last_available_size()
        );
        self.rows = Some(rows);
        self.max_width = max_width;
        // NOTE: works incorrectly after screen resize
        self.update_search();
    }

    fn rows_are_valid(&mut self, size: Vec2) -> bool {
        if self.update_content || self.needs_relayout {
            return false;
        }
        if self.wrap && self.cached_size != size {
            return false;
        }
        return true;
    }

    fn layout_content(&mut self, size: Vec2) {
        if !self.rows_are_valid(size) {
            log::trace!("Size changed: {:?} -> {:?}", self.cached_size, size);
            self.cached_size = size;
            self.compute_rows();
        }
        self.needs_relayout = false;
        self.update_content = false;
    }

    fn content_required_size(&mut self, mut req: Vec2) -> Vec2 {
        let rows = self.rows.as_ref().map_or(0, |r| r.len());
        req.y = rows;
        req.x = usize::max(req.x, self.max_width);
        return req;
    }

    fn draw_content(&self, printer: &Printer<'_, '_>) {
        if let Some(rows) = &self.rows {
            for (y, row) in rows
                .iter()
                .enumerate()
                .skip(printer.content_offset.y)
                .take(printer.output_size.y)
            {
                let row_style = if Some(y) == self.matched_row {
                    ColorStyle::highlight()
                } else {
                    ColorStyle::primary()
                };
                printer.with_style(row_style, |printer| {
                    let mut x = 0;
                    for span in row.resolve_stream(&self.content) {
                        printer.with_style(*span.attr, |printer| {
                            printer.print((x, y), span.content);
                            x += span.content.width();
                        });
                    }
                });
            }
        }
    }
}

pub struct LogView {
    inner_view: OnEventView<NamedView<LogViewBase>>,
}

impl LogView {
    pub fn new(cluster: bool, wrap: bool) -> Self {
        let mut v = LogViewBase {
            needs_relayout: true,
            cluster,
            wrap,
            ..Default::default()
        };
        v.scroll_core
            .set_scroll_strategy(ScrollStrategy::StickToBottom);
        v.scroll_core.set_scroll_x(true);
        if !wrap {
            v.scroll_core.set_scroll_y(true);
        }
        // NOTE: we cannot pass mutable ref to view in search_prompt callback, sigh.
        let v = v.with_name("logs");

        let scroll_page = move |v: &mut NamedView<LogViewBase>, e: &Event| -> Option<EventResult> {
            return Some(scroll::on_event(
                &mut *v.get_mut(),
                e.clone(),
                |s: &mut LogViewBase, e| s.on_event(e),
                |s, si| s.important_area(si),
            ));
        };

        let reset_search =
            move |v: &mut NamedView<LogViewBase>, e: &Event| -> Option<EventResult> {
                {
                    let mut base = v.get_mut();
                    // TODO: highlight next matched row instead of resetting search
                    base.matched_row = None;
                    base.search_term.clear();
                }
                return scroll_page(v, e);
            };

        let search_prompt_impl = |siv: &mut Cursive, forward: bool| {
            let find = move |siv: &mut Cursive, text: &str| {
                siv.call_on_name("logs", |base: &mut LogViewBase| {
                    base.search_term = text.to_string();
                    base.matched_row = None;

                    base.search_direction_forward = forward;
                    base.update_search();
                });
                siv.pop_layer();
            };
            let view = OnEventView::new(EditView::new().on_submit(find).min_width(10));
            siv.add_layer(view);
        };
        let search_prompt_forward = move |siv: &mut Cursive| {
            search_prompt_impl(siv, /* forward= */ true);
        };
        let search_prompt_reverse = move |siv: &mut Cursive| {
            search_prompt_impl(siv, /* forward= */ false);
        };

        let v = OnEventView::new(v)
            .on_pre_event_inner(Key::PageUp, reset_search)
            .on_pre_event_inner(Key::PageDown, reset_search)
            .on_pre_event_inner(Key::Left, reset_search)
            .on_pre_event_inner(Key::Right, reset_search)
            .on_pre_event_inner(Key::Up, reset_search)
            .on_pre_event_inner(Key::Down, reset_search)
            .on_pre_event_inner('j', move |v, _| reset_search(v, &Event::Key(Key::Down)))
            .on_pre_event_inner('k', move |v, _| reset_search(v, &Event::Key(Key::Up)))
            .on_pre_event_inner('g', move |v, _| reset_search(v, &Event::Key(Key::Home)))
            .on_pre_event_inner('G', move |v, _| reset_search(v, &Event::Key(Key::End)))
            .on_event_inner('/', move |_, _| {
                return Some(EventResult::Consumed(Some(Callback::from_fn(
                    search_prompt_forward,
                ))));
            })
            .on_event_inner('?', move |_, _| {
                return Some(EventResult::Consumed(Some(Callback::from_fn(
                    search_prompt_reverse,
                ))));
            })
            .on_event_inner('n', move |v, _| {
                let mut base = v.get_mut();
                base.search_direction_forward = true;
                return base.update_search_forward();
            })
            .on_event_inner('N', move |v, _| {
                let mut base = v.get_mut();
                base.search_direction_forward = false;
                return base.update_search_reverse();
            });

        let log_view = LogView { inner_view: v };
        return log_view;
    }

    pub fn push_logs(&mut self, logs: &mut Vec<LogEntry>) {
        self.inner_view.get_inner_mut().get_mut().push_logs(logs);
    }
}

impl View for LogViewBase {
    fn draw(&self, printer: &Printer<'_, '_>) {
        scroll::draw(self, printer, Self::draw_content);
    }

    fn layout(&mut self, size: Vec2) {
        scroll::layout(
            self,
            size.saturating_sub((0, 0)),
            self.needs_relayout,
            Self::layout_content,
            Self::content_required_size,
        );

        if let Some(matched_row) = self.matched_row {
            self.scroll_core.set_offset((0, matched_row));
        }
    }
}

impl ViewWrapper for LogView {
    wrap_impl!(self.inner_view: OnEventView<NamedView<LogViewBase>>);

    fn wrap_required_size(&mut self, req: Vec2) -> Vec2 {
        return self
            .inner_view
            .get_inner_mut()
            .get_mut()
            .content_required_size(req);
    }
}
