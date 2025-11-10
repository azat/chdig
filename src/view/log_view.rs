use anyhow::{Error, Result};
use chrono::{DateTime, Datelike, Local, Timelike};
use cursive::{
    Cursive, Printer, Vec2,
    event::{Callback, Event, EventResult, Key},
    theme::{Color, ColorStyle, Style},
    utils::{
        lines::spans::{LinesIterator, Row},
        markup::StyledString,
    },
    view::{Nameable, Resizable, ScrollStrategy, View, ViewWrapper, scroll},
    views::{Dialog, EditView, NamedView, OnEventView},
    wrap_impl,
};
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use unicode_width::UnicodeWidthStr;

// Hash-based color function matching ClickHouse's setColor from terminalColors.cpp
// Uses YCbCr color space with constant brightness (y=128) for better readability
fn hash_to_color(hash: u64) -> Color {
    let y = 128u8;
    let cb = ((hash >> 8) & 0xFF) as u8;
    let cr = (hash & 0xFF) as u8;

    // YCbCr to RGB conversion (ITU-R BT.601)
    // R = Y + 1.402 * (Cr - 128)
    // G = Y - 0.344136 * (Cb - 128) - 0.714136 * (Cr - 128)
    // B = Y + 1.772 * (Cb - 128)

    let cb_offset = cb as i32 - 128;
    let cr_offset = cr as i32 - 128;

    let r = (y as i32 + (1402 * cr_offset) / 1000).clamp(0, 255) as u8;
    let g = (y as i32 - (344 * cb_offset) / 1000 - (714 * cr_offset) / 1000).clamp(0, 255) as u8;
    let b = (y as i32 + (1772 * cb_offset) / 1000).clamp(0, 255) as u8;

    Color::Rgb(r, g, b)
}

// Color for log priority level matching ClickHouse's setColorForLogPriority from terminalColors.cpp
fn get_level_color(level: &str) -> Color {
    match level {
        // Fatal: \033[1;41m (bold + red background) - using bright red
        "Fatal" => Color::Rgb(255, 85, 85),
        // Critical: \033[7;31m (reverse video + red) - using bright red
        "Critical" => Color::Rgb(255, 85, 85),
        // Error: \033[1;31m (bold red) - bright red
        "Error" => Color::Rgb(255, 85, 85),
        // Warning: \033[0;31m (red) - normal red
        "Warning" => Color::Rgb(255, 0, 0),
        // Notice: \033[0;33m (yellow) - normal yellow
        "Notice" => Color::Rgb(255, 255, 0),
        // Information: \033[1m (bold) - using default terminal color (light gray)
        "Information" => Color::Rgb(192, 192, 192),
        // Debug: no color - default terminal color
        "Debug" => Color::TerminalDefault,
        // Trace: \033[2m (dim) - dark gray
        "Trace" => Color::Rgb(128, 128, 128),
        // Test: no specific color in ClickHouse
        "Test" => Color::TerminalDefault,
        _ => Color::TerminalDefault,
    }
}

// Hash function similar to ClickHouse's intHash64
fn int_hash_64(value: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn string_hash(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

pub struct LogEntry {
    pub host_name: String,
    pub event_time_microseconds: DateTime<Local>,
    pub thread_id: u64,
    pub level: String,
    pub message: String,
    pub query_id: Option<String>,
    pub logger_name: Option<String>,
}

impl LogEntry {
    fn to_styled_string(&self, cluster: bool) -> StyledString {
        let mut line = StyledString::new();

        if cluster {
            line.append_plain(format!("[{}] ", self.host_name));
        }

        // Format timestamp with microseconds matching ClickHouse format: YYYY.MM.DD HH:MM:SS.microseconds
        let dt = self.event_time_microseconds;
        let microseconds = dt.timestamp_subsec_micros();
        let timestamp = format!(
            "{:04}.{:02}.{:02} {:02}:{:02}:{:02}.{:06}",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
            microseconds
        );
        line.append_plain(format!("{} ", timestamp));

        // Thread ID with hash-based coloring: [ thread_id ]
        line.append_plain("[ ");
        let thread_hash = int_hash_64(self.thread_id);
        let thread_color = hash_to_color(thread_hash);
        line.append_styled(format!("{}", self.thread_id), thread_color);
        line.append_plain(" ] ");

        // Query ID with hash-based coloring: {query_id}
        // ClickHouse writes query_id even if empty for log parser convenience
        line.append_plain("{");
        let query_id_str = self.query_id.as_deref().unwrap_or("");
        if !query_id_str.is_empty() {
            let query_hash = string_hash(query_id_str);
            let query_color = hash_to_color(query_hash);
            line.append_styled(query_id_str, query_color);
        }
        line.append_plain("} ");

        // Priority level with color: <level>
        line.append_plain("<");
        let level_color = get_level_color(self.level.as_str());
        line.append_styled(self.level.as_str(), level_color);
        line.append_plain("> ");

        // Logger name (source) with hash-based coloring: source:
        if let Some(logger_name) = &self.logger_name {
            let logger_hash = string_hash(logger_name);
            let logger_color = hash_to_color(logger_hash);
            line.append_styled(logger_name, logger_color);
            line.append_plain(": ");
        }

        // Message
        line.append_plain(self.message.as_str());
        return line;
    }
}

#[derive(Default)]
pub struct LogViewBase {
    content: StyledString,
    rows: Option<Vec<Row>>,
    max_width: usize,

    content_size_with_wrap: Vec2,
    // Size without respecting wrap, since with wrap width is equal to the longest line
    screen_size_without_wrap: Vec2,

    needs_relayout: bool,
    update_content: bool,
    scroll_core: scroll::Core,

    search_direction_forward: bool,
    search_term: String,
    matched_row: Option<usize>,
    matched_col: Option<usize>,
    skip_scroll: bool,

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
                let mut x = 0;
                for span in rows[i].resolve_stream(&self.content) {
                    if let Some(pos) = span.content.find(&self.search_term) {
                        self.matched_row = Some(i);
                        self.matched_col = Some(x + pos);
                        matched = true;
                        break;
                    }
                    x += span.content.width();
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
                let mut x = 0;
                for span in rows[i].resolve_stream(&self.content) {
                    if let Some(pos) = span.content.find(&self.search_term) {
                        self.matched_row = Some(i);
                        self.matched_col = Some(x + pos);
                        matched = true;
                        break;
                    }
                    x += span.content.width();
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
        if let Some(rows) = self.rows.as_ref()
            && rows.len() < self.matched_row.unwrap_or_default()
        {
            self.matched_row = None;
        }
        if self.search_direction_forward {
            return self.update_search_forward();
        } else {
            return self.update_search_reverse();
        }
    }

    fn set_options(&mut self, options: &str) -> Result<()> {
        if options.is_empty() {
        } else if options == "S" {
            self.wrap = !self.wrap;
            log::trace!("Toggle wrap mode, switched to {}", self.wrap);
        } else {
            return Err(Error::msg(format!("Invalid options: {}", options)));
        }
        return Ok(());
    }

    fn push_logs(&mut self, logs: &[LogEntry]) {
        log::trace!("Add {} log entries", logs.len());

        // Increment content update
        for log in logs.iter() {
            let mut line = log.to_styled_string(self.cluster);
            line.append("\n");

            self.content.append(line.clone());
        }

        self.needs_relayout = true;
        self.compute_rows();
    }

    fn compute_rows(&mut self) {
        let width = if self.wrap {
            // For scrolling we need to subtract some padding
            self.screen_size_without_wrap.x.saturating_sub(2)
        } else {
            usize::MAX
        };
        // NOTE: incremental update is not possible (since the references in the rows to the
        // content will be wrong)
        let mut max_width = 0;
        let rows = LinesIterator::new(&self.content, width)
            .map(|x| {
                max_width = usize::max(max_width, x.width);
                return x;
            })
            .collect::<Vec<Row>>();
        log::trace!(
            "Updating rows cache (width: {:?}, wrap: {}, max width: {}, rows: {}, inner size: {:?}, last size: {:?})",
            width,
            self.wrap,
            max_width,
            rows.len(),
            self.scroll_core.inner_size(),
            self.scroll_core.last_available_size()
        );
        self.rows = Some(rows);
        self.max_width = max_width;

        self.update_search();
        // Show the horizontal scrolling
        self.needs_relayout = true;
    }

    fn rows_are_valid(&mut self, size: Vec2) -> bool {
        if self.update_content || self.needs_relayout {
            return false;
        }
        if self.wrap && self.content_size_with_wrap != size {
            return false;
        }
        return true;
    }

    fn layout_content(&mut self, size: Vec2) {
        if !self.rows_are_valid(size) {
            log::trace!(
                "Size changed: content_size={:?}, screen_size={:?}, size={:?}",
                self.content_size_with_wrap,
                self.screen_size_without_wrap,
                size
            );
            self.content_size_with_wrap = size;
            self.compute_rows();

            self.scroll_core.set_scroll_x(!self.wrap);
        }
        self.needs_relayout = false;
        self.update_content = false;
    }

    fn inner_required_size(&mut self, mut req: Vec2) -> Vec2 {
        self.screen_size_without_wrap = req;

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
                let mut x = 0;
                for span in row.resolve_stream(&self.content) {
                    // Check if this row matches and if the span contains the search term
                    if Some(y) == self.matched_row && span.content.contains(&self.search_term) {
                        let content = span.content;
                        let search_term = &self.search_term;
                        let mut last_pos = 0;

                        for (match_start, _) in content.match_indices(search_term) {
                            // Print text before match with normal style
                            if match_start > last_pos {
                                let before = &content[last_pos..match_start];
                                printer.with_style(*span.attr, |printer| {
                                    printer.print((x, y), before);
                                });
                                x += before.width();
                            }

                            // Use the same highlight theme as less(1):
                            // - Always use black as text color
                            // - Use original text color as background
                            // - For no-style use white as background
                            let matched = &content[match_start..match_start + search_term.len()];
                            let bg_color = if *span.attr == Style::default() {
                                Color::Rgb(255, 255, 255).into()
                            } else {
                                span.attr.color.front
                            };
                            let inverted_style = ColorStyle::new(Color::Rgb(0, 0, 0), bg_color);
                            printer.with_style(inverted_style, |printer| {
                                printer.print((x, y), matched);
                            });
                            x += matched.width();

                            last_pos = match_start + search_term.len();
                        }

                        // Print remaining text after last match
                        if last_pos < content.len() {
                            let after = &content[last_pos..];
                            printer.with_style(*span.attr, |printer| {
                                printer.print((x, y), after);
                            });
                            x += after.width();
                        }
                    } else {
                        // No match in this span or row, print normally
                        printer.with_style(*span.attr, |printer| {
                            printer.print((x, y), span.content);
                            x += span.content.width();
                        });
                    }
                }
            }
        }
    }

    // Write plain text content from the styled string directly to a writer
    fn write_plain_text<W: Write>(&self, writer: &mut W) -> Result<()> {
        if let Some(rows) = &self.rows.as_ref() {
            for row in rows.iter() {
                for span in row.resolve_stream(&self.content) {
                    writer.write_all(span.content.as_bytes())?;
                }
                writer.write_all(b"\n")?;
            }
        }
        Ok(())
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
        v.scroll_core.set_scroll_x(!wrap);
        v.scroll_core.set_scroll_y(true);
        // NOTE: we cannot pass mutable ref to view in search_prompt callback, sigh.
        let v = v.with_name("logs");

        let scroll = move |v: &mut NamedView<LogViewBase>, e: &Event| -> Option<EventResult> {
            v.get_mut().skip_scroll = true;
            return Some(scroll::on_event(
                &mut *v.get_mut(),
                e.clone(),
                |s: &mut LogViewBase, e| s.on_event(e),
                |s, si| s.important_area(si),
            ));
        };

        let show_options = |siv: &mut Cursive| {
            let options = move |siv: &mut Cursive, text: &str| {
                let status = siv.call_on_name("logs", |base: &mut LogViewBase| {
                    let status = base.set_options(text);
                    base.compute_rows();
                    return status;
                });
                siv.pop_layer();
                if let Some(Err(err)) = status {
                    siv.add_layer(Dialog::info(err.to_string()));
                }
            };
            let view = OnEventView::new(EditView::new().on_submit(options).min_width(10));
            siv.add_layer(view);
        };

        let search_prompt_impl = |siv: &mut Cursive, forward: bool| {
            let find = move |siv: &mut Cursive, text: &str| {
                siv.call_on_name("logs", |base: &mut LogViewBase| {
                    base.search_term = text.to_string();
                    base.matched_row = None;
                    base.matched_col = None;
                    base.skip_scroll = false;

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

        let show_save_prompt = |siv: &mut Cursive| {
            let save_file_impl = |siv: &mut Cursive| {
                let file_path = siv
                    .call_on_name("save_file_path", |view: &mut EditView| {
                        view.get_content().to_string()
                    })
                    .unwrap();
                siv.pop_layer();

                if file_path.trim().is_empty() {
                    siv.add_layer(Dialog::info("File path cannot be empty"));
                    return;
                }

                let result = siv.call_on_name("logs", |base: &mut LogViewBase| -> Result<()> {
                    let mut file = fs::File::create(&file_path)?;
                    base.write_plain_text(&mut file)?;
                    Ok(())
                });

                match result {
                    Some(Ok(_)) => {
                        siv.add_layer(Dialog::info(format!("Logs saved to: {}", file_path)));
                    }
                    Some(Err(err)) => {
                        siv.add_layer(Dialog::info(format!("Error saving file: {}", err)));
                    }
                    None => {
                        siv.add_layer(Dialog::info("Error: Could not access log content"));
                    }
                }
            };

            let save_file_for_submit = {
                move |siv: &mut Cursive, _: &str| {
                    save_file_impl(siv);
                }
            };
            let view = EditView::new()
                .on_submit(save_file_for_submit)
                .with_name("save_file_path")
                .min_width(40);
            siv.add_layer(
                Dialog::around(view)
                    .title("Save logs to file")
                    .button("Save", save_file_impl)
                    .button("Cancel", |siv: &mut Cursive| {
                        siv.pop_layer();
                    }),
            );
        };

        let v = OnEventView::new(v)
            .on_pre_event_inner(Key::PageUp, scroll)
            .on_pre_event_inner(Key::PageDown, scroll)
            .on_pre_event_inner(Key::Left, scroll)
            .on_pre_event_inner(Key::Right, scroll)
            .on_pre_event_inner(Key::Up, scroll)
            .on_pre_event_inner(Key::Down, scroll)
            .on_pre_event_inner('j', move |v, _| scroll(v, &Event::Key(Key::Down)))
            .on_pre_event_inner('k', move |v, _| scroll(v, &Event::Key(Key::Up)))
            .on_pre_event_inner('g', move |v, _| scroll(v, &Event::Key(Key::Home)))
            .on_pre_event_inner('G', move |v, _| scroll(v, &Event::Key(Key::End)))
            .on_event_inner('-', move |_, _| {
                return Some(EventResult::Consumed(Some(Callback::from_fn(show_options))));
            })
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
            })
            .on_event_inner('s', move |_, _| {
                return Some(EventResult::Consumed(Some(Callback::from_fn(
                    show_save_prompt,
                ))));
            });

        let log_view = LogView { inner_view: v };
        return log_view;
    }

    pub fn push_logs(&mut self, logs: &[LogEntry]) {
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
            Self::inner_required_size,
        );

        if self.skip_scroll {
            self.skip_scroll = false;
        } else if let Some(matched_row) = self.matched_row {
            let x_offset = self.matched_col.unwrap_or(0);
            self.scroll_core.set_offset((x_offset, matched_row));
        }
    }
}

impl ViewWrapper for LogView {
    wrap_impl!(self.inner_view: OnEventView<NamedView<LogViewBase>>);

    fn wrap_required_size(&mut self, mut req: Vec2) -> Vec2 {
        req = self
            .inner_view
            .get_inner_mut()
            .get_mut()
            .inner_required_size(req);
        // For scrollbars
        req.x += 1;
        req.y += 1;
        return req;
    }
}
