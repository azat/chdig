use ratatui::layout::{Rect, Size};
use std::sync::Arc;

use super::app::App;
use super::component::{Canvas, Component};
use super::event::{Event, EventResult, Key, MouseEvent};
use super::style::{StyledString, highlight, highlight_inactive, print_line};

type OnSubmit<T> = Arc<dyn Fn(&mut App, &T) + Send + Sync>;

/// Vertical list with one selected item.
pub struct SelectView<T = String> {
    items: Vec<(StyledString, T)>,
    selected: usize,
    scroll_offset: usize,
    autojump: bool,
    last_area: Rect,
    on_submit: Option<OnSubmit<T>>,
}

impl<T: Send + Sync + 'static> Default for SelectView<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Sync + 'static> SelectView<T> {
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            selected: 0,
            scroll_offset: 0,
            autojump: false,
            last_area: Rect::default(),
            on_submit: None,
        }
    }

    pub fn autojump(mut self) -> Self {
        self.autojump = true;
        self
    }

    pub fn on_submit<F>(mut self, cb: F) -> Self
    where
        F: Fn(&mut App, &T) + Send + Sync + 'static,
    {
        self.on_submit = Some(Arc::new(cb));
        self
    }

    pub fn set_on_submit<F>(&mut self, cb: F)
    where
        F: Fn(&mut App, &T) + Send + Sync + 'static,
    {
        self.on_submit = Some(Arc::new(cb));
    }

    pub fn add_item(&mut self, label: impl Into<StyledString>, value: T) {
        self.items.push((label.into(), value));
    }

    pub fn item(mut self, label: impl Into<StyledString>, value: T) -> Self {
        self.add_item(label, value);
        self
    }

    pub fn clear(&mut self) {
        self.items.clear();
        self.selected = 0;
        self.scroll_offset = 0;
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn selection(&self) -> Option<&T> {
        self.items.get(self.selected).map(|(_, v)| v)
    }

    pub fn set_selection(&mut self, index: usize) {
        self.selected = index.min(self.items.len().saturating_sub(1));
    }

    pub fn select_up(&mut self, n: usize) {
        self.selected = self.selected.saturating_sub(n);
    }

    pub fn select_down(&mut self, n: usize) {
        if !self.items.is_empty() {
            self.selected = (self.selected + n).min(self.items.len() - 1);
        }
    }

    fn keep_selection_visible(&mut self) {
        let height = self.last_area.height.max(1) as usize;
        if self.selected < self.scroll_offset {
            self.scroll_offset = self.selected;
        } else if self.selected >= self.scroll_offset + height {
            self.scroll_offset = self.selected + 1 - height;
        }
    }
}

impl<T: Clone + Send + Sync + 'static> SelectView<T> {
    fn submit(&self) -> EventResult {
        let (Some(cb), Some((_, value))) = (self.on_submit.clone(), self.items.get(self.selected))
        else {
            return EventResult::consumed();
        };
        let value = value.clone();
        EventResult::with_cb(move |app| cb(app, &value))
    }
}

impl SelectView<String> {
    pub fn add_item_str(&mut self, label: impl Into<String>) {
        let label = label.into();
        self.add_item(StyledString::plain(label.clone()), label);
    }
}

impl<T: Clone + Send + Sync + 'static> Component for SelectView<T> {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        self.last_area = area;
        self.keep_selection_visible();
        for (row, idx) in (self.scroll_offset
            ..self
                .items
                .len()
                .min(self.scroll_offset + area.height as usize))
            .enumerate()
        {
            let y = area.top() + row as u16;
            let (label, _) = &self.items[idx];
            let mut line = label.first_line();
            if idx == self.selected {
                let style = if focused {
                    highlight()
                } else {
                    highlight_inactive()
                };
                line = line.style(style);
                // Highlight the full row, not only the text.
                for x in area.left()..area.right() {
                    if let Some(cell) = canvas.buf.cell_mut((x, y)) {
                        cell.set_symbol(" ").set_style(style);
                    }
                }
            }
            print_line(canvas.buf, area.x, y, area, &line);
        }
    }

    fn required_size(&mut self, max: Size) -> Size {
        let width = self.items.iter().map(|(l, _)| l.width()).max().unwrap_or(0) as u16;
        let height = self.items.len() as u16;
        Size::new(width.clamp(1, max.width), height.clamp(1, max.height))
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(Key::Up) => self.select_up(1),
            Event::Key(Key::Down) => self.select_down(1),
            Event::Key(Key::PageUp) => self.select_up(10),
            Event::Key(Key::PageDown) => self.select_down(10),
            Event::Key(Key::Home) => self.selected = 0,
            Event::Key(Key::End) => self.selected = self.items.len().saturating_sub(1),
            Event::Key(Key::Enter) => return self.submit(),
            Event::Char(c) if self.autojump => {
                let c = c.to_lowercase().next().unwrap_or(*c);
                let start = (self.selected + 1) % self.items.len().max(1);
                let found = (0..self.items.len())
                    .map(|i| (start + i) % self.items.len())
                    .find(|&i| {
                        self.items[i]
                            .0
                            .source()
                            .chars()
                            .next()
                            .map(|f| f.to_lowercase().next() == Some(c))
                            .unwrap_or(false)
                    });
                match found {
                    Some(i) => self.selected = i,
                    None => return EventResult::Ignored,
                }
            }
            Event::Mouse {
                position,
                event: MouseEvent::Press(super::event::MouseButton::Left),
            } => {
                if !self.last_area.contains(*position) {
                    return EventResult::Ignored;
                }
                let idx = self.scroll_offset + (position.y - self.last_area.y) as usize;
                if idx >= self.items.len() {
                    return EventResult::Ignored;
                }
                if idx == self.selected {
                    return self.submit();
                }
                self.selected = idx;
            }
            Event::Mouse {
                event: MouseEvent::WheelUp,
                ..
            } => self.select_up(1),
            Event::Mouse {
                event: MouseEvent::WheelDown,
                ..
            } => self.select_down(1),
            _ => return EventResult::Ignored,
        }
        EventResult::consumed()
    }

    fn take_focus(&mut self) -> bool {
        true
    }
}
