use ratatui::layout::{Rect, Size};

use super::component::{Canvas, Component};
use super::event::{Event, EventResult};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizeConstraint {
    /// Let the child decide.
    Free,
    Fixed(u16),
    /// Take all the available space.
    Full,
    AtMost(u16),
    AtLeast(u16),
}

impl SizeConstraint {
    fn result(self, child: u16, available: u16) -> u16 {
        match self {
            SizeConstraint::Free => child,
            SizeConstraint::Fixed(v) => v.min(available),
            SizeConstraint::Full => available,
            SizeConstraint::AtMost(v) => child.min(v).min(available),
            SizeConstraint::AtLeast(v) => child.max(v).min(available),
        }
    }

    fn available(self, available: u16) -> u16 {
        match self {
            SizeConstraint::Fixed(v) | SizeConstraint::AtMost(v) => v.min(available),
            _ => available,
        }
    }
}

pub struct ResizedView<V> {
    width: SizeConstraint,
    height: SizeConstraint,
    view: V,
}

impl<V: Component> ResizedView<V> {
    pub fn new(width: SizeConstraint, height: SizeConstraint, view: V) -> Self {
        Self {
            width,
            height,
            view,
        }
    }

    pub fn get_inner_mut(&mut self) -> &mut V {
        &mut self.view
    }
}

impl<V: Component + 'static> Component for ResizedView<V> {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        self.view.draw(canvas, area, focused);
    }

    fn required_size(&mut self, max: Size) -> Size {
        let inner_max = Size::new(
            self.width.available(max.width),
            self.height.available(max.height),
        );
        let child = self.view.required_size(inner_max);
        Size::new(
            self.width.result(child.width, max.width),
            self.height.result(child.height, max.height),
        )
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        self.view.on_event(event)
    }

    fn take_focus(&mut self) -> bool {
        self.view.take_focus()
    }

    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        f(&mut self.view);
    }

    fn focus_name(&mut self, name: &str) -> bool {
        self.view.focus_name(name)
    }
}

pub trait Resizable: Component + Sized {
    fn resized(self, width: SizeConstraint, height: SizeConstraint) -> ResizedView<Self> {
        ResizedView::new(width, height, self)
    }

    fn full_width(self) -> ResizedView<Self> {
        self.resized(SizeConstraint::Full, SizeConstraint::Free)
    }

    fn full_height(self) -> ResizedView<Self> {
        self.resized(SizeConstraint::Free, SizeConstraint::Full)
    }

    fn full_screen(self) -> ResizedView<Self> {
        self.resized(SizeConstraint::Full, SizeConstraint::Full)
    }

    fn fixed_width(self, width: u16) -> ResizedView<Self> {
        self.resized(SizeConstraint::Fixed(width), SizeConstraint::Free)
    }

    fn fixed_height(self, height: u16) -> ResizedView<Self> {
        self.resized(SizeConstraint::Free, SizeConstraint::Fixed(height))
    }

    fn fixed_size(self, size: (u16, u16)) -> ResizedView<Self> {
        self.resized(SizeConstraint::Fixed(size.0), SizeConstraint::Fixed(size.1))
    }

    fn max_width(self, width: u16) -> ResizedView<Self> {
        self.resized(SizeConstraint::AtMost(width), SizeConstraint::Free)
    }

    fn max_height(self, height: u16) -> ResizedView<Self> {
        self.resized(SizeConstraint::Free, SizeConstraint::AtMost(height))
    }

    fn min_width(self, width: u16) -> ResizedView<Self> {
        self.resized(SizeConstraint::AtLeast(width), SizeConstraint::Free)
    }
}

impl<V: Component + Sized> Resizable for V {}
