use ratatui::layout::{Position, Rect, Size};
use ratatui::widgets::{Block, Borders, Widget};
use std::sync::Arc;

use super::app::App;
use super::component::{Boxed, Canvas, Component};
use super::event::{Callback, Event, EventResult, Key, MouseButton, MouseEvent};
use super::style::{StyledString, highlight, print_str, str_width};
use super::text::TextView;

struct Button {
    label: String,
    callback: Callback,
    last_rect: Rect,
}

enum Focus {
    Content,
    Button(usize),
}

/// Bordered modal with a content view and a bottom row of buttons.
pub struct Dialog {
    title: String,
    content: Boxed,
    buttons: Vec<Button>,
    focus: Focus,
}

impl Dialog {
    pub fn new() -> Self {
        Self::around(super::component::DummyView)
    }

    pub fn around<V: Component + 'static>(content: V) -> Self {
        Self {
            title: String::new(),
            content: Boxed::new(content),
            buttons: Vec::new(),
            focus: Focus::Content,
        }
    }

    pub fn text(text: impl Into<StyledString>) -> Self {
        Self::around(TextView::new(text))
    }

    /// Message dialog with a dismissing "Ok" button.
    pub fn info(text: impl Into<StyledString>) -> Self {
        Self::text(text).button("Ok", |app| {
            app.pop_layer();
        })
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = title.into();
        self
    }

    pub fn content<V: Component + 'static>(mut self, content: V) -> Self {
        self.content = Boxed::new(content);
        self
    }

    pub fn button<F>(mut self, label: impl Into<String>, cb: F) -> Self
    where
        F: Fn(&mut App) + Send + Sync + 'static,
    {
        self.buttons.push(Button {
            label: label.into(),
            callback: Arc::new(cb),
            last_rect: Rect::default(),
        });
        if self.buttons.len() == 1 && !self.content.take_focus() {
            self.focus = Focus::Button(0);
        }
        self
    }

    fn buttons_height(&self) -> u16 {
        if self.buttons.is_empty() { 0 } else { 2 }
    }

    fn activate_button(&self, index: usize) -> EventResult {
        let cb = self.buttons[index].callback.clone();
        EventResult::Consumed(Some(cb))
    }

    fn content_area(area: Rect, buttons_height: u16) -> Rect {
        // 1 cell border + 1 cell horizontal padding.
        let inner = Rect {
            x: area.x + 2,
            y: area.y + 1,
            width: area.width.saturating_sub(4),
            height: area.height.saturating_sub(2),
        };
        Rect {
            height: inner.height.saturating_sub(buttons_height),
            ..inner
        }
    }
}

impl Default for Dialog {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for Dialog {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        let block = Block::new().borders(Borders::ALL).title(self.title.clone());
        block.render(area, canvas.buf);

        let content_area = Self::content_area(area, self.buttons_height());
        if content_area.width > 0 && content_area.height > 0 {
            self.content.draw(
                canvas,
                content_area,
                focused && matches!(self.focus, Focus::Content),
            );
        }

        if !self.buttons.is_empty() {
            let y = area.bottom().saturating_sub(2);
            let total: u16 = self
                .buttons
                .iter()
                .map(|b| str_width(&b.label) as u16 + 4)
                .sum();
            let mut x = area.right().saturating_sub(2 + total);
            for (i, button) in self.buttons.iter_mut().enumerate() {
                let label = format!("<{}>", button.label);
                let style = if focused && matches!(self.focus, Focus::Button(f) if f == i) {
                    highlight()
                } else {
                    Default::default()
                };
                let width = print_str(canvas.buf, x, y, area, &label, style);
                button.last_rect = Rect::new(x, y, width, 1);
                x = x.saturating_add(width + 2);
            }
        }
    }

    fn required_size(&mut self, max: Size) -> Size {
        let chrome_x = 4;
        let chrome_y = 2 + self.buttons_height();
        let inner_max = Size::new(
            max.width.saturating_sub(chrome_x),
            max.height.saturating_sub(chrome_y),
        );
        let content = self.content.required_size(inner_max);
        let buttons: u16 = self
            .buttons
            .iter()
            .map(|b| str_width(&b.label) as u16 + 4)
            .sum();
        let title = str_width(&self.title) as u16 + 4;
        let width = content.width.max(buttons).max(title) + chrome_x;
        let height = content.height + chrome_y;
        Size::new(width.min(max.width), height.min(max.height))
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        if let Event::Mouse {
            position,
            event: MouseEvent::Press(MouseButton::Left),
        } = event
        {
            let pos = Position::new(position.x, position.y);
            for i in 0..self.buttons.len() {
                if self.buttons[i].last_rect.contains(pos) {
                    self.focus = Focus::Button(i);
                    return self.activate_button(i);
                }
            }
        }

        match self.focus {
            Focus::Content => {
                let result = self.content.on_event(event);
                if result.is_consumed() {
                    return result;
                }
                match event {
                    Event::Key(Key::Tab) | Event::Key(Key::Down) if !self.buttons.is_empty() => {
                        self.focus = Focus::Button(0);
                        EventResult::consumed()
                    }
                    _ => EventResult::Ignored,
                }
            }
            Focus::Button(i) => match event {
                Event::Key(Key::Enter) => self.activate_button(i),
                Event::Key(Key::Left) if i > 0 => {
                    self.focus = Focus::Button(i - 1);
                    EventResult::consumed()
                }
                Event::Key(Key::Right) if i + 1 < self.buttons.len() => {
                    self.focus = Focus::Button(i + 1);
                    EventResult::consumed()
                }
                Event::Key(Key::Tab) => {
                    if i + 1 < self.buttons.len() {
                        self.focus = Focus::Button(i + 1);
                    } else if self.content.take_focus() {
                        self.focus = Focus::Content;
                    } else {
                        self.focus = Focus::Button(0);
                    }
                    EventResult::consumed()
                }
                Event::Key(Key::Up) if self.content.take_focus() => {
                    self.focus = Focus::Content;
                    EventResult::consumed()
                }
                _ => {
                    // Unhandled keys still go to the content (e.g. scrolling
                    // a dialog while a button is focused).
                    self.content.on_event(event)
                }
            },
        }
    }

    fn take_focus(&mut self) -> bool {
        true
    }

    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        f(&mut self.content);
    }

    fn focus_name(&mut self, name: &str) -> bool {
        if self.content.focus_name(name) {
            self.focus = Focus::Content;
            return true;
        }
        false
    }
}
