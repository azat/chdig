use ratatui::layout::{Rect, Size};
use std::sync::Arc;

use super::app::App;
use super::component::{Canvas, Component};
use super::event::{Event, EventResult, Key};
use super::style::{Modifier, Style, print_str, str_width};

type OnEdit = Arc<dyn Fn(&mut App, &str, usize) + Send + Sync>;
type OnSubmit = Arc<dyn Fn(&mut App, &str) + Send + Sync>;

/// Single-line text input.
pub struct EditView {
    content: String,
    /// Byte offset of the cursor within `content`.
    cursor: usize,
    /// Leftmost visible byte offset (horizontal scrolling).
    offset: usize,
    last_width: u16,
    style: Style,
    on_edit: Option<OnEdit>,
    on_submit: Option<OnSubmit>,
}

impl Default for EditView {
    fn default() -> Self {
        Self::new()
    }
}

impl EditView {
    pub fn new() -> Self {
        Self {
            content: String::new(),
            cursor: 0,
            offset: 0,
            last_width: 0,
            style: Style::default().add_modifier(Modifier::REVERSED),
            on_edit: None,
            on_submit: None,
        }
    }

    pub fn content(mut self, content: impl Into<String>) -> Self {
        let cb = self.set_content(content);
        let _ = cb;
        self
    }

    pub fn style(mut self, style: impl Into<Style>) -> Self {
        self.style = style.into();
        self
    }

    pub fn on_edit<F>(mut self, cb: F) -> Self
    where
        F: Fn(&mut App, &str, usize) + Send + Sync + 'static,
    {
        self.on_edit = Some(Arc::new(cb));
        self
    }

    pub fn on_submit<F>(mut self, cb: F) -> Self
    where
        F: Fn(&mut App, &str) + Send + Sync + 'static,
    {
        self.on_submit = Some(Arc::new(cb));
        self
    }

    pub fn get_content(&self) -> String {
        self.content.clone()
    }

    pub fn get_cursor(&self) -> usize {
        self.cursor
    }

    pub fn set_cursor(&mut self, cursor: usize) {
        self.cursor = cursor.min(self.content.len());
    }

    /// Returns the callback notifying `on_edit` (run it against the App).
    #[must_use = "the returned callback must be run to notify on_edit"]
    pub fn set_content(&mut self, content: impl Into<String>) -> super::event::Callback {
        self.content = content.into();
        self.cursor = self.content.len();
        self.make_edit_cb()
    }

    fn make_edit_cb(&self) -> super::event::Callback {
        let cb = self.on_edit.clone();
        let content = self.content.clone();
        let cursor = self.cursor;
        Arc::new(move |app| {
            if let Some(cb) = &cb {
                cb(app, &content, cursor);
            }
        })
    }

    fn prev_char_boundary(&self) -> usize {
        self.content[..self.cursor]
            .char_indices()
            .next_back()
            .map(|(i, _)| i)
            .unwrap_or(0)
    }

    fn next_char_boundary(&self) -> usize {
        self.content[self.cursor..]
            .chars()
            .next()
            .map(|c| self.cursor + c.len_utf8())
            .unwrap_or(self.cursor)
    }

    /// Start of the whitespace-delimited word before the cursor
    /// (readline unix-word-rubout).
    fn prev_space_word_start(&self) -> usize {
        let trimmed = self.content[..self.cursor].trim_end();
        trimmed
            .char_indices()
            .rev()
            .find(|(_, c)| c.is_whitespace())
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0)
    }

    /// Start of the alphanumeric word before the cursor (readline backward-word).
    fn prev_word_start(&self) -> usize {
        let mut in_word = false;
        for (i, c) in self.content[..self.cursor].char_indices().rev() {
            if c.is_alphanumeric() {
                in_word = true;
            } else if in_word {
                return i + c.len_utf8();
            }
        }
        0
    }

    /// End of the alphanumeric word after the cursor (readline forward-word).
    fn next_word_end(&self) -> usize {
        let mut in_word = false;
        for (i, c) in self.content[self.cursor..].char_indices() {
            if c.is_alphanumeric() {
                in_word = true;
            } else if in_word {
                return self.cursor + i;
            }
        }
        self.content.len()
    }

    fn keep_cursor_visible(&mut self) {
        let width = self.last_width.max(1) as usize;
        if self.cursor < self.offset {
            self.offset = self.cursor;
            return;
        }
        while str_width(&self.content[self.offset..self.cursor]) >= width {
            self.offset = self.content[self.offset..]
                .chars()
                .next()
                .map(|c| self.offset + c.len_utf8())
                .unwrap_or(self.content.len());
        }
    }
}

impl Component for EditView {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        self.last_width = area.width;
        // Fill the whole line so the input field is visible even when empty.
        for x in area.left()..area.right() {
            if let Some(cell) = canvas.buf.cell_mut((x, area.top())) {
                cell.set_symbol(" ").set_style(self.style);
            }
        }
        self.keep_cursor_visible();
        print_str(
            canvas.buf,
            area.x,
            area.y,
            area,
            &self.content[self.offset..],
            self.style,
        );
        if focused {
            let cx = area.x + str_width(&self.content[self.offset..self.cursor]) as u16;
            canvas.cursor = Some((cx.min(area.right().saturating_sub(1)), area.y));
        }
    }

    fn required_size(&mut self, max: Size) -> Size {
        // Grow with content, like cursive's EditView with auto width.
        let width = (str_width(&self.content) as u16 + 1).clamp(1, max.width);
        Size::new(width, 1)
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Char(c) => {
                self.content.insert(self.cursor, *c);
                self.cursor += c.len_utf8();
            }
            Event::Key(Key::Backspace) => {
                if self.cursor > 0 {
                    let prev = self.prev_char_boundary();
                    self.content.replace_range(prev..self.cursor, "");
                    self.cursor = prev;
                } else {
                    return EventResult::consumed();
                }
            }
            Event::Key(Key::Del) => {
                if self.cursor < self.content.len() {
                    let next = self.next_char_boundary();
                    self.content.replace_range(self.cursor..next, "");
                } else {
                    return EventResult::consumed();
                }
            }
            Event::Key(Key::Left) => {
                self.cursor = self.prev_char_boundary();
                return EventResult::consumed();
            }
            Event::Key(Key::Right) => {
                self.cursor = self.next_char_boundary();
                return EventResult::consumed();
            }
            Event::Key(Key::Home) | Event::CtrlChar('a') => {
                self.cursor = 0;
                return EventResult::consumed();
            }
            Event::Key(Key::End) | Event::CtrlChar('e') => {
                self.cursor = self.content.len();
                return EventResult::consumed();
            }
            Event::AltChar('b') | Event::Ctrl(Key::Left) => {
                self.cursor = self.prev_word_start();
                return EventResult::consumed();
            }
            Event::AltChar('f') | Event::Ctrl(Key::Right) => {
                self.cursor = self.next_word_end();
                return EventResult::consumed();
            }
            Event::CtrlChar('w') => {
                let start = self.prev_space_word_start();
                if start >= self.cursor {
                    return EventResult::consumed();
                }
                self.content.replace_range(start..self.cursor, "");
                self.cursor = start;
            }
            Event::CtrlChar('u') => {
                if self.cursor == 0 {
                    return EventResult::consumed();
                }
                self.content.replace_range(..self.cursor, "");
                self.cursor = 0;
            }
            Event::CtrlChar('k') => {
                if self.cursor == self.content.len() {
                    return EventResult::consumed();
                }
                self.content.truncate(self.cursor);
            }
            Event::Key(Key::Enter) => {
                if let Some(cb) = self.on_submit.clone() {
                    let content = self.content.clone();
                    return EventResult::with_cb(move |app| cb(app, &content));
                }
                return EventResult::Ignored;
            }
            _ => return EventResult::Ignored,
        }
        EventResult::Consumed(Some(self.make_edit_cb()))
    }

    fn take_focus(&mut self) -> bool {
        true
    }
}
