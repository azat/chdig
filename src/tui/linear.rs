use ratatui::layout::{Position, Rect, Size};

use super::component::{Boxed, Canvas, Component};
use super::event::{Event, EventResult, Key};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Orientation {
    Horizontal,
    Vertical,
}

struct Child {
    view: Boxed,
    last_rect: Rect,
}

/// Sequential container (cursive's LinearLayout): children are sized by
/// their `required_size`, the leftover goes to children that asked for
/// everything available (`full_width`/`full_screen` wrappers, tables, panes).
pub struct LinearLayout {
    orientation: Orientation,
    children: Vec<Child>,
    focus: usize,
}

impl LinearLayout {
    pub fn new(orientation: Orientation) -> Self {
        Self {
            orientation,
            children: Vec::new(),
            focus: 0,
        }
    }

    pub fn horizontal() -> Self {
        Self::new(Orientation::Horizontal)
    }

    pub fn vertical() -> Self {
        Self::new(Orientation::Vertical)
    }

    pub fn child<V: Component + 'static>(mut self, view: V) -> Self {
        self.add_child(view);
        self
    }

    pub fn add_child<V: Component + 'static>(&mut self, view: V) {
        self.children.push(Child {
            view: Boxed::new(view),
            last_rect: Rect::default(),
        });
    }

    pub fn remove_child(&mut self, index: usize) -> Option<Boxed> {
        if index >= self.children.len() {
            return None;
        }
        let child = self.children.remove(index);
        self.focus = self.focus.min(self.children.len().saturating_sub(1));
        Some(child.view)
    }

    pub fn len(&self) -> usize {
        self.children.len()
    }

    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    /// Split `total` between children: everyone gets its required size,
    /// greedy children (required == available) share the leftover.
    fn layout(&mut self, area: Rect) -> Vec<Rect> {
        let total = match self.orientation {
            Orientation::Horizontal => area.width,
            Orientation::Vertical => area.height,
        };
        let cross = match self.orientation {
            Orientation::Horizontal => area.height,
            Orientation::Vertical => area.width,
        };

        let mut sizes = Vec::with_capacity(self.children.len());
        let mut greedy = Vec::new();
        let mut remaining = total;
        for (i, child) in self.children.iter_mut().enumerate() {
            let max = match self.orientation {
                Orientation::Horizontal => Size::new(remaining, cross),
                Orientation::Vertical => Size::new(cross, remaining),
            };
            let req = child.view.required_size(max);
            let want = match self.orientation {
                Orientation::Horizontal => req.width,
                Orientation::Vertical => req.height,
            };
            if want >= remaining && remaining > 0 {
                greedy.push(i);
                // Greedy children are sized after the fixed ones; reserve
                // nothing yet so later fixed children still fit.
                sizes.push(0);
            } else {
                sizes.push(want);
                remaining = remaining.saturating_sub(want);
            }
        }
        if !greedy.is_empty() {
            let share = remaining / greedy.len() as u16;
            let mut extra = remaining % greedy.len() as u16;
            for &i in &greedy {
                sizes[i] = share + if extra > 0 { 1 } else { 0 };
                extra = extra.saturating_sub(1);
            }
        }

        let mut rects = Vec::with_capacity(self.children.len());
        let mut pos = 0u16;
        for &size in &sizes {
            let rect = match self.orientation {
                Orientation::Horizontal => Rect::new(
                    area.x + pos,
                    area.y,
                    size.min(area.width.saturating_sub(pos)),
                    area.height,
                ),
                Orientation::Vertical => Rect::new(
                    area.x,
                    area.y + pos,
                    area.width,
                    size.min(area.height.saturating_sub(pos)),
                ),
            };
            rects.push(rect);
            pos = pos.saturating_add(size);
        }
        rects
    }

    fn focusable_from(&mut self, start: usize, forward: bool) -> Option<usize> {
        if forward {
            (start..self.children.len()).find(|&i| self.children[i].view.take_focus())
        } else {
            (0..=start)
                .rev()
                .find(|&i| self.children[i].view.take_focus())
        }
    }

    fn move_focus(&mut self, forward: bool) -> EventResult {
        let next = if forward {
            self.focusable_from(self.focus + 1, true)
        } else if self.focus > 0 {
            self.focusable_from(self.focus - 1, false)
        } else {
            None
        };
        match next {
            Some(i) => {
                self.focus = i;
                EventResult::consumed()
            }
            None => EventResult::Ignored,
        }
    }
}

impl Component for LinearLayout {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        let rects = self.layout(area);
        let focus = self.focus;
        for (i, (child, rect)) in self.children.iter_mut().zip(rects).enumerate() {
            child.last_rect = rect;
            if rect.width > 0 && rect.height > 0 {
                child.view.draw(canvas, rect, focused && i == focus);
            }
        }
    }

    fn required_size(&mut self, max: Size) -> Size {
        let mut main = 0u16;
        let mut cross = 0u16;
        for child in &mut self.children {
            let remaining = match self.orientation {
                Orientation::Horizontal => Size::new(max.width.saturating_sub(main), max.height),
                Orientation::Vertical => Size::new(max.width, max.height.saturating_sub(main)),
            };
            let req = child.view.required_size(remaining);
            match self.orientation {
                Orientation::Horizontal => {
                    main = main.saturating_add(req.width);
                    cross = cross.max(req.height);
                }
                Orientation::Vertical => {
                    main = main.saturating_add(req.height);
                    cross = cross.max(req.width);
                }
            }
        }
        match self.orientation {
            Orientation::Horizontal => Size::new(main.min(max.width), cross.min(max.height)),
            Orientation::Vertical => Size::new(cross.min(max.width), main.min(max.height)),
        }
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        if self.children.is_empty() {
            return EventResult::Ignored;
        }

        // Mouse events are routed positionally, not by focus.
        if let Event::Mouse { position, .. } = event {
            let pos = Position::new(position.x, position.y);
            for i in 0..self.children.len() {
                if self.children[i].last_rect.contains(pos) {
                    let result = self.children[i].view.on_event(event);
                    if result.is_consumed() && self.children[i].view.take_focus() {
                        self.focus = i;
                    }
                    return result;
                }
            }
            return EventResult::Ignored;
        }

        let focus = self.focus.min(self.children.len() - 1);
        self.focus = focus;
        let result = self.children[focus].view.on_event(event);
        if result.is_consumed() {
            return result;
        }

        match (self.orientation, event) {
            (Orientation::Horizontal, Event::Key(Key::Left)) => self.move_focus(false),
            (Orientation::Horizontal, Event::Key(Key::Right)) => self.move_focus(true),
            (Orientation::Vertical, Event::Key(Key::Up)) => self.move_focus(false),
            (Orientation::Vertical, Event::Key(Key::Down)) => self.move_focus(true),
            (_, Event::Key(Key::Tab)) => self.move_focus(true),
            _ => EventResult::Ignored,
        }
    }

    fn take_focus(&mut self) -> bool {
        for i in 0..self.children.len() {
            if self.children[i].view.take_focus() {
                if !self.children[self.focus].view.take_focus() {
                    self.focus = i;
                }
                return true;
            }
        }
        false
    }

    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        for child in &mut self.children {
            f(&mut child.view);
        }
    }

    fn focus_name(&mut self, name: &str) -> bool {
        for i in 0..self.children.len() {
            if self.children[i].view.focus_name(name) {
                self.focus = i;
                return true;
            }
        }
        false
    }
}
