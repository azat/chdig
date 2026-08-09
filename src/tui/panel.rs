use ratatui::layout::{Rect, Size};
use ratatui::widgets::{Block, Borders, Widget};

use super::component::{Canvas, Component};
use super::event::{Event, EventResult};

/// Bordered box with an optional title.
pub struct Panel<V> {
    inner: V,
    title: String,
}

impl<V: Component> Panel<V> {
    pub fn new(inner: V) -> Self {
        Self {
            inner,
            title: String::new(),
        }
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = title.into();
        self
    }

    pub fn get_inner_mut(&mut self) -> &mut V {
        &mut self.inner
    }

    fn inner_area(area: Rect) -> Rect {
        Rect {
            x: area.x + 1,
            y: area.y + 1,
            width: area.width.saturating_sub(2),
            height: area.height.saturating_sub(2),
        }
    }
}

impl<V: Component + 'static> Component for Panel<V> {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        Block::new()
            .borders(Borders::ALL)
            .title(self.title.clone())
            .render(area, canvas.buf);
        let inner = Self::inner_area(area);
        if inner.width > 0 && inner.height > 0 {
            self.inner.draw(canvas, inner, focused);
        }
    }

    fn required_size(&mut self, max: Size) -> Size {
        let inner = self.inner.required_size(Size::new(
            max.width.saturating_sub(2),
            max.height.saturating_sub(2),
        ));
        Size::new(
            (inner.width + 2).min(max.width),
            (inner.height + 2).min(max.height),
        )
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        self.inner.on_event(event)
    }

    fn take_focus(&mut self) -> bool {
        self.inner.take_focus()
    }

    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        f(&mut self.inner);
    }

    fn focus_name(&mut self, name: &str) -> bool {
        self.inner.focus_name(name)
    }
}
