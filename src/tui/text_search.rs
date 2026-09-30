use ratatui::layout::{Rect, Size};
use ratatui::text::{Line, Span};
use regex::Regex;

use super::app::App;
use super::component::{Canvas, Component, Nameable, NamedView};
use super::dialog::Dialog;
use super::event::{Event, EventResult, Key, MouseEvent};
use super::prompt::show_bottom_prompt;
use super::scroll::{draw_scrollbar_v, keep_row_visible};
use super::style::{Row, StyledString, invert_style, print_line, row_width, wrap_styled};

/// Read-only styled text with less-style search (`/`, `?`, `n`, `N`) and
/// scrolling. It wraps and scrolls itself instead of using ScrollView: the
/// search needs the display-row mapping that wrapping produces.
pub struct TextSearchView {
    content: StyledString,
    /// Own name, to reach the view from the search prompt callback
    name: String,
    /// `content` wrapped to `width`
    rows: Vec<Row>,
    width: u16,
    /// Width of the last draw: row indices (`matched_row`) are relative to it
    draw_width: u16,
    offset: usize,
    viewport: usize,
    regex: Option<Regex>,
    matched_row: Option<usize>,
    /// Set by a search, honored by the next draw (the viewport height is
    /// known only then)
    scroll_to_match: bool,
}

impl TextSearchView {
    pub fn new(name: &str, content: impl Into<StyledString>) -> NamedView<Self> {
        let view = Self {
            content: content.into(),
            name: name.to_string(),
            rows: Vec::new(),
            width: 0,
            draw_width: 0,
            offset: 0,
            viewport: 1,
            regex: None,
            matched_row: None,
            scroll_to_match: false,
        };
        view.with_name(name)
    }

    fn wrap(&mut self, width: u16) {
        let width = width.max(1);
        if self.width == width {
            return;
        }
        self.width = width;
        self.rows = wrap_styled(&self.content, width as usize);
    }

    fn max_offset(&self) -> usize {
        self.rows.len().saturating_sub(self.viewport)
    }

    fn scroll_by(&mut self, delta: i64) -> EventResult {
        let offset = (self.offset as i64 + delta).clamp(0, self.max_offset() as i64) as usize;
        if offset == self.offset {
            return EventResult::Ignored;
        }
        self.offset = offset;
        EventResult::consumed()
    }

    /// First row matching the search at or after (before) `from`.
    fn find_row(&self, from: usize, forward: bool) -> Option<usize> {
        let re = self.regex.as_ref()?;
        let matches = |row: &Row| {
            let text: String = row.iter().map(|span| span.content.as_ref()).collect();
            re.is_match(&text)
        };
        if forward {
            (from..self.rows.len()).find(|&row| matches(&self.rows[row]))
        } else {
            (0..=from.min(self.rows.len().saturating_sub(1)))
                .rev()
                .find(|&row| matches(&self.rows[row]))
        }
    }

    fn set_search(&mut self, regex: Regex, forward: bool) -> bool {
        self.regex = Some(regex);
        self.matched_row = self.find_row(self.offset, forward);
        self.scroll_to_match = self.matched_row.is_some();
        self.matched_row.is_some()
    }

    /// Jumps to the next (previous) match, wrapping around.
    fn step(&mut self, forward: bool) -> bool {
        if self.regex.is_none() || self.rows.is_empty() {
            return false;
        }
        let current = self.matched_row.unwrap_or(self.offset);
        let next = if forward {
            self.find_row(current + 1, true)
                .or_else(|| self.find_row(0, true))
        } else {
            current
                .checked_sub(1)
                .and_then(|from| self.find_row(from, false))
                .or_else(|| self.find_row(self.rows.len() - 1, false))
        };
        if next.is_none() {
            return false;
        }
        self.matched_row = next;
        self.scroll_to_match = true;
        true
    }

    fn prompt(&self, forward: bool) -> EventResult {
        let name = self.name.clone();
        EventResult::with_cb(move |app: &mut App| {
            let name = name.clone();
            let prefix = if forward { "/" } else { "?" };
            show_bottom_prompt(app, prefix, move |app: &mut App, text: &str| {
                let regex = match Regex::new(text) {
                    Ok(regex) => regex,
                    Err(err) => {
                        app.pop_layer();
                        app.add_layer(Dialog::info(format!("Invalid regex: {}", err)));
                        return;
                    }
                };
                let found = app.call_on_name(&name, |view: &mut TextSearchView| {
                    view.set_search(regex, forward)
                });
                app.pop_layer();
                if found == Some(false) {
                    app.add_layer(Dialog::info("Pattern not found"));
                }
            });
        })
    }

    /// The row with the matched parts highlighted (less(1) theme).
    fn render_row(&self, row: &Row) -> Line<'static> {
        let Some(ref re) = self.regex else {
            return Line::from(row.clone());
        };
        let text: String = row.iter().map(|span| span.content.as_ref()).collect();
        let ranges = re
            .find_iter(&text)
            .filter(|m| !m.is_empty())
            .map(|m| (m.start(), m.end()))
            .collect::<Vec<_>>();
        if ranges.is_empty() {
            return Line::from(row.clone());
        }

        let mut spans = Vec::new();
        let mut span_start = 0;
        for span in row {
            let span_end = span_start + span.content.len();
            let mut pos = span_start;
            for &(start, end) in &ranges {
                if end <= pos || start >= span_end {
                    continue;
                }
                let (start, end) = (start.max(pos), end.min(span_end));
                if start > pos {
                    spans.push(Span::styled(text[pos..start].to_string(), span.style));
                }
                spans.push(Span::styled(
                    text[start..end].to_string(),
                    invert_style(span.style),
                ));
                pos = end;
            }
            if pos < span_end {
                spans.push(Span::styled(text[pos..span_end].to_string(), span.style));
            }
            span_start = span_end;
        }
        Line::from(spans)
    }
}

impl Component for TextSearchView {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, _focused: bool) {
        self.viewport = area.height.max(1) as usize;
        self.wrap(area.width);
        // Narrowing only adds rows, so the scrollbar decision does not flap.
        let scrollbar = self.rows.len() > self.viewport;
        if scrollbar {
            self.wrap(area.width.saturating_sub(1));
        }
        // required_size() wraps at its own width, so only a change between
        // draws invalidates the row indices.
        if self.draw_width != self.width {
            self.draw_width = self.width;
            self.matched_row = None;
        }

        if self.scroll_to_match {
            self.scroll_to_match = false;
            if let Some(row) = self.matched_row {
                self.offset = keep_row_visible(self.offset, row, self.viewport);
            }
        }
        self.offset = self.offset.min(self.max_offset());

        for (y, row) in self
            .rows
            .iter()
            .skip(self.offset)
            .take(self.viewport)
            .enumerate()
        {
            print_line(
                canvas.buf,
                area.x,
                area.y + y as u16,
                area,
                &self.render_row(row),
            );
        }

        if scrollbar {
            draw_scrollbar_v(
                canvas.buf,
                area.right() - 1,
                area.y,
                self.rows.len(),
                self.viewport,
                self.offset,
            );
        }
    }

    fn required_size(&mut self, max: Size) -> Size {
        self.wrap(max.width);
        let width = self.rows.iter().map(row_width).max().unwrap_or(0) as u16;
        if self.rows.len() > max.height as usize {
            // One column goes to the scrollbar
            return Size::new(width.saturating_add(1).min(max.width).max(1), max.height);
        }
        Size::new(
            width.min(max.width).max(1),
            (self.rows.len() as u16).max(1).min(max.height),
        )
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        let page = self.viewport.max(1) as i64;
        match event {
            Event::Char('/') => self.prompt(true),
            Event::Char('?') => self.prompt(false),
            Event::Char('n') | Event::Char('N') => {
                let forward = *event == Event::Char('n');
                if self.step(forward) {
                    EventResult::consumed()
                } else if self.regex.is_some() {
                    EventResult::with_cb_once(|app: &mut App| {
                        app.add_layer(Dialog::info("Pattern not found"));
                    })
                } else {
                    EventResult::Ignored
                }
            }
            Event::Key(Key::Up) => self.scroll_by(-1),
            Event::Key(Key::Down) => self.scroll_by(1),
            Event::Key(Key::PageUp) => self.scroll_by(-page),
            Event::Key(Key::PageDown) => self.scroll_by(page),
            Event::Key(Key::Home) => self.scroll_by(i64::MIN / 2),
            Event::Key(Key::End) => self.scroll_by(i64::MAX / 2),
            Event::Mouse {
                event: MouseEvent::WheelUp,
                ..
            } => self.scroll_by(-3),
            Event::Mouse {
                event: MouseEvent::WheelDown,
                ..
            } => self.scroll_by(3),
            _ => EventResult::Ignored,
        }
    }

    fn take_focus(&mut self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn draw(view: &mut TextSearchView, width: u16, height: u16) {
        let area = Rect::new(0, 0, width, height);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        let mut canvas = Canvas {
            buf: &mut buf,
            cursor: None,
        };
        view.draw(&mut canvas, area, true);
    }

    fn view(lines: usize) -> NamedView<TextSearchView> {
        let content = (0..lines)
            .map(|i| format!("line{}", i))
            .collect::<Vec<_>>()
            .join("\n");
        TextSearchView::new("t", content)
    }

    #[test]
    fn test_scroll() {
        let mut named = view(10);
        let view = named.get_mut();
        draw(view, 10, 3);

        assert!(view.on_event(&Event::Key(Key::PageDown)).is_consumed());
        assert_eq!(view.offset, 3);
        assert!(view.on_event(&Event::Key(Key::End)).is_consumed());
        assert_eq!(view.offset, 7);
        // Already at the bottom
        assert!(!view.on_event(&Event::Key(Key::Down)).is_consumed());
        assert!(view.on_event(&Event::Key(Key::Home)).is_consumed());
        assert_eq!(view.offset, 0);
    }

    #[test]
    fn test_search_scrolls_to_the_match() {
        let mut named = view(10);
        let view = named.get_mut();
        draw(view, 10, 3);

        assert!(view.set_search(Regex::new("line8").unwrap(), true));
        draw(view, 10, 3);
        assert_eq!(view.matched_row, Some(8));
        assert_eq!(view.offset, 6);

        assert!(!view.set_search(Regex::new("nothing").unwrap(), true));
    }

    #[test]
    fn test_search_wraps_around() {
        let mut named = view(10);
        let view = named.get_mut();
        draw(view, 10, 3);

        assert!(view.set_search(Regex::new("line1").unwrap(), true));
        assert_eq!(view.matched_row, Some(1));
        // line1 -> line10 does not exist, so back to line1
        assert!(view.step(true));
        assert_eq!(view.matched_row, Some(1));
        assert!(view.step(false));
        assert_eq!(view.matched_row, Some(1));
    }

    /// The row indices the search works with are those of the wrapped text.
    #[test]
    fn test_search_over_wrapped_rows() {
        let mut named = TextSearchView::new("t", "aaaabbbbcccc\nzz");
        let view = named.get_mut();
        draw(view, 4, 10);

        assert_eq!(view.rows.len(), 4);
        assert!(view.set_search(Regex::new("cccc").unwrap(), true));
        assert_eq!(view.matched_row, Some(2));
    }
}
