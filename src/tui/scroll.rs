use ratatui::buffer::Buffer;
use ratatui::layout::{Rect, Size};

/// Clamp `offset` so that `row` is inside a `viewport`-tall window.
pub fn keep_row_visible(offset: usize, row: usize, viewport: usize) -> usize {
    let viewport = viewport.max(1);
    if row < offset {
        row
    } else if row >= offset + viewport {
        row + 1 - viewport
    } else {
        offset
    }
}

/// Thumb (start, length) of a scrollbar for `viewport` visible cells out of
/// `content`, scrolled to `offset`.
pub fn scrollbar_thumb(content: usize, viewport: usize, offset: usize) -> (usize, usize) {
    let len = ((viewport * viewport) / content.max(1)).max(1);
    let denom = content.saturating_sub(viewport);
    let top = if denom == 0 {
        0
    } else {
        offset * viewport.saturating_sub(len) / denom
    };
    (top, len)
}

/// Vertical scrollbar track at column `x` starting at row `y0`.
pub fn draw_scrollbar_v(
    buf: &mut Buffer,
    x: u16,
    y0: u16,
    content: usize,
    viewport: usize,
    offset: usize,
) {
    let (top, len) = scrollbar_thumb(content, viewport, offset);
    for row in 0..viewport {
        let symbol = if row >= top && row < top + len {
            "▓"
        } else {
            "░"
        };
        if let Some(cell) = buf.cell_mut((x, y0 + row as u16)) {
            cell.set_symbol(symbol);
        }
    }
}

/// Horizontal scrollbar track at row `y` starting at column `x0`.
pub fn draw_scrollbar_h(
    buf: &mut Buffer,
    y: u16,
    x0: u16,
    content: usize,
    viewport: usize,
    offset: usize,
) {
    let (left, len) = scrollbar_thumb(content, viewport, offset);
    for col in 0..viewport {
        let symbol = if col >= left && col < left + len {
            "▓"
        } else {
            "░"
        };
        if let Some(cell) = buf.cell_mut((x0 + col as u16, y)) {
            cell.set_symbol(symbol);
        }
    }
}

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
            draw_scrollbar_v(
                canvas.buf,
                area.right() - 1,
                area.y,
                content.height as usize,
                area.height as usize,
                self.offset_y as usize,
            );
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
