use ratatui::buffer::Buffer;
use ratatui::layout::{Rect, Size};

use super::component::{Canvas, Component};
use super::event::{Event, EventResult, Key, MouseEvent};

/// Scrollable wrapper: renders the inner component at its full required size
/// into an offscreen buffer and blits the visible window. Suits bounded
/// content (dialogs, settings); large virtual views (tables, logs) implement
/// their own row-based scrolling instead.
pub struct ScrollView<V> {
    inner: V,
    offset_x: u16,
    offset_y: u16,
    scroll_x: bool,
    content: Size,
    viewport: Size,
}

impl<V: Component> ScrollView<V> {
    pub fn new(inner: V) -> Self {
        Self {
            inner,
            offset_x: 0,
            offset_y: 0,
            scroll_x: false,
            content: Size::default(),
            viewport: Size::default(),
        }
    }

    pub fn scroll_x(mut self, enabled: bool) -> Self {
        self.scroll_x = enabled;
        self
    }

    pub fn get_inner_mut(&mut self) -> &mut V {
        &mut self.inner
    }

    fn max_offset_y(&self) -> u16 {
        self.content.height.saturating_sub(self.viewport.height)
    }

    fn max_offset_x(&self) -> u16 {
        self.content.width.saturating_sub(self.viewport.width)
    }

    fn scroll_by(&mut self, dy: i32, dx: i32) -> EventResult {
        let new_y = (self.offset_y as i32 + dy).clamp(0, self.max_offset_y() as i32) as u16;
        let new_x = (self.offset_x as i32 + dx).clamp(0, self.max_offset_x() as i32) as u16;
        if new_y == self.offset_y && new_x == self.offset_x {
            return EventResult::Ignored;
        }
        self.offset_y = new_y;
        self.offset_x = new_x;
        EventResult::consumed()
    }
}

impl<V: Component + 'static> Component for ScrollView<V> {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        self.viewport = Size::new(area.width, area.height);
        let content_max = Size::new(
            if self.scroll_x {
                u16::MAX / 2
            } else {
                area.width
            },
            u16::MAX / 2,
        );
        let mut content = self.inner.required_size(content_max);
        content.width = content.width.max(area.width.min(1));
        self.content = content;

        let scrollable =
            content.height > area.height || (self.scroll_x && content.width > area.width);
        if !scrollable {
            self.offset_x = 0;
            self.offset_y = 0;
            self.inner.draw(canvas, area, focused);
            return;
        }

        self.offset_y = self.offset_y.min(self.max_offset_y());
        self.offset_x = self.offset_x.min(self.max_offset_x());

        let full = Rect::new(0, 0, content.width.max(area.width), content.height);
        let mut offscreen = Buffer::empty(full);
        let mut sub = Canvas {
            buf: &mut offscreen,
            cursor: None,
        };
        self.inner.draw(&mut sub, full, focused);

        let scrollbar = content.height > area.height;
        let visible_width = area.width.saturating_sub(scrollbar as u16);
        for y in 0..area.height {
            for x in 0..visible_width {
                let src = (x + self.offset_x, y + self.offset_y);
                if src.0 < full.width && src.1 < full.height {
                    let cell = offscreen[src].clone();
                    if let Some(dst) = canvas.buf.cell_mut((area.x + x, area.y + y)) {
                        *dst = cell;
                    }
                }
            }
        }

        if scrollbar {
            let x = area.right() - 1;
            let thumb_height =
                ((area.height as u32 * area.height as u32) / content.height as u32).max(1) as u16;
            let denom = self.max_offset_y() as u32;
            let thumb_top = if denom == 0 {
                0
            } else {
                (self.offset_y as u32 * (area.height - thumb_height) as u32 / denom) as u16
            };
            for y in 0..area.height {
                let symbol = if y >= thumb_top && y < thumb_top + thumb_height {
                    "▓"
                } else {
                    "░"
                };
                if let Some(cell) = canvas.buf.cell_mut((x, area.y + y)) {
                    cell.set_symbol(symbol);
                }
            }
        }
    }

    fn required_size(&mut self, max: Size) -> Size {
        let content_max = Size::new(
            if self.scroll_x {
                u16::MAX / 2
            } else {
                max.width
            },
            u16::MAX / 2,
        );
        let content = self.inner.required_size(content_max);
        Size::new(content.width.min(max.width), content.height.min(max.height))
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        let inner_result = self.inner.on_event(event);
        if inner_result.is_consumed() {
            return inner_result;
        }
        let page = self.viewport.height.max(1) as i32;
        match event {
            Event::Key(Key::Up) => self.scroll_by(-1, 0),
            Event::Key(Key::Down) => self.scroll_by(1, 0),
            Event::Key(Key::Left) if self.scroll_x => self.scroll_by(0, -2),
            Event::Key(Key::Right) if self.scroll_x => self.scroll_by(0, 2),
            Event::Key(Key::PageUp) => self.scroll_by(-page, 0),
            Event::Key(Key::PageDown) => self.scroll_by(page, 0),
            Event::Key(Key::Home) => self.scroll_by(i32::MIN / 2, 0),
            Event::Key(Key::End) => self.scroll_by(i32::MAX / 2, 0),
            Event::Mouse {
                event: MouseEvent::WheelUp,
                ..
            } => self.scroll_by(-3, 0),
            Event::Mouse {
                event: MouseEvent::WheelDown,
                ..
            } => self.scroll_by(3, 0),
            _ => EventResult::Ignored,
        }
    }

    fn take_focus(&mut self) -> bool {
        // Scrollable content must be reachable even if the inner view
        // (e.g. plain text) refuses focus.
        true
    }

    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        f(&mut self.inner);
    }

    fn focus_name(&mut self, name: &str) -> bool {
        self.inner.focus_name(name)
    }
}

pub trait Scrollable: Component + Sized {
    fn scrollable(self) -> ScrollView<Self> {
        ScrollView::new(self)
    }
}

impl<V: Component + Sized> Scrollable for V {}
