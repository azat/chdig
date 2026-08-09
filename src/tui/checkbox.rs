use ratatui::layout::{Rect, Size};

use super::component::{Canvas, Component};
use super::event::{Event, EventResult, Key, MouseButton, MouseEvent};
use super::style::{Style, highlight, print_str};

/// `[ ]`/`[x]` toggle (cursive's Checkbox). The label is composed by callers.
#[derive(Default)]
pub struct Checkbox {
    checked: bool,
    last_area: Rect,
}

impl Checkbox {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn checked(mut self, checked: bool) -> Self {
        self.checked = checked;
        self
    }

    pub fn set_checked(&mut self, checked: bool) {
        self.checked = checked;
    }

    pub fn is_checked(&self) -> bool {
        self.checked
    }

    fn toggle(&mut self) {
        self.checked = !self.checked;
    }
}

impl Component for Checkbox {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        self.last_area = area;
        let style = if focused {
            highlight()
        } else {
            Style::default()
        };
        let symbol = if self.checked { "[x]" } else { "[ ]" };
        print_str(canvas.buf, area.x, area.y, area, symbol, style);
    }

    fn required_size(&mut self, _max: Size) -> Size {
        Size::new(3, 1)
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Char(' ') | Event::Key(Key::Enter) => {
                self.toggle();
                EventResult::consumed()
            }
            Event::Mouse {
                position,
                event: MouseEvent::Press(MouseButton::Left),
            } if self.last_area.contains(*position) => {
                self.toggle();
                EventResult::consumed()
            }
            _ => EventResult::Ignored,
        }
    }

    fn take_focus(&mut self) -> bool {
        true
    }
}
