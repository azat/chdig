use ratatui::layout::{Rect, Size};

use super::component::{Boxed, Canvas, Component};
use super::event::{Event, EventResult, Key, MouseButton, MouseEvent};
use super::style::{Modifier, Style, highlight, highlight_inactive, print_str, str_width};

/// Spaces around a title in the list
const TITLE_PADDING: usize = 2;
const LIST_SEPARATOR: &str = "│";

struct Tab {
    title: String,
    /// None: a group header in the list (not selectable)
    view: Option<Boxed>,
    /// Cells of the title in the list (mouse target), from the last draw
    title_rect: Rect,
}

/// Tabbed container: a list of the titles on the left and the active tab's
/// content to the right of it. All tabs stay in the component tree, so name
/// lookups reach the hidden ones too and `focus_name` switches to the tab
/// holding the target. The list may contain group headers (`add_group`),
/// which are skipped when switching.
///
/// Keys: Alt+Up/Down and Alt+1..9 switch tabs from anywhere inside;
/// Up/Down do the same while the list is focused (Right/Tab/Enter move into
/// the content, Left/Shift-Tab out of it back to the list).
pub struct Tabs {
    tabs: Vec<Tab>,
    active: usize,
    /// Focus is on the list rather than on the active content
    list_focused: bool,
    /// First title shown (the list scrolls to keep the active one visible)
    list_offset: usize,
    list_rect: Rect,
    content_rect: Rect,
}

impl Default for Tabs {
    fn default() -> Self {
        Self::new()
    }
}

impl Tabs {
    pub fn new() -> Self {
        Self {
            tabs: Vec::new(),
            active: 0,
            list_focused: false,
            list_offset: 0,
            list_rect: Rect::default(),
            content_rect: Rect::default(),
        }
    }

    pub fn tab<V: Component + 'static>(mut self, title: impl Into<String>, view: V) -> Self {
        self.add_tab(title, view);
        self
    }

    pub fn add_tab<V: Component + 'static>(&mut self, title: impl Into<String>, view: V) {
        // The active index always points at a real tab (not at a group)
        if !self.has_view(self.active) {
            self.active = self.tabs.len();
        }
        self.tabs.push(Tab {
            title: title.into(),
            view: Some(Boxed::new(view)),
            title_rect: Rect::default(),
        });
    }

    /// A header in the list: the following tabs belong to it.
    pub fn add_group(&mut self, title: impl Into<String>) {
        self.tabs.push(Tab {
            title: title.into(),
            view: None,
            title_rect: Rect::default(),
        });
    }

    /// Starts with the title list focused instead of the active content.
    pub fn focus_list(mut self) -> Self {
        self.list_focused = true;
        self
    }

    pub fn active(&self) -> usize {
        self.active
    }

    pub fn set_active(&mut self, index: usize) {
        if self.has_view(index) {
            self.active = index;
        }
    }

    pub fn active_title(&self) -> Option<&str> {
        self.tabs.get(self.active).map(|tab| tab.title.as_str())
    }

    fn has_view(&self, index: usize) -> bool {
        self.tabs.get(index).is_some_and(|tab| tab.view.is_some())
    }

    fn active_view(&mut self) -> Option<&mut Boxed> {
        self.tabs.get_mut(self.active)?.view.as_mut()
    }

    /// Width of the title list (without the separator)
    fn list_width(&self) -> u16 {
        let width = self
            .tabs
            .iter()
            .map(|tab| str_width(&tab.title))
            .max()
            .unwrap_or(0)
            + TITLE_PADDING;
        width.min(u16::MAX as usize) as u16
    }

    /// Moves to the next real tab in `delta`'s direction (wrapping, skipping
    /// the groups).
    fn switch(&mut self, delta: isize) -> EventResult {
        let len = self.tabs.len() as isize;
        let mut index = self.active as isize;
        for _ in 0..len {
            index = (index + delta).rem_euclid(len);
            if self.has_view(index as usize) {
                self.active = index as usize;
                break;
            }
        }
        EventResult::consumed()
    }

    fn content_takes_focus(&mut self) -> bool {
        self.active_view().is_some_and(|view| view.take_focus())
    }

    fn on_list_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(Key::Up) => self.switch(-1),
            Event::Key(Key::Down) => self.switch(1),
            Event::Key(Key::Tab) | Event::Key(Key::Right) | Event::Key(Key::Enter) => {
                if self.content_takes_focus() {
                    self.list_focused = false;
                    EventResult::consumed()
                } else {
                    EventResult::Ignored
                }
            }
            _ => EventResult::Ignored,
        }
    }

    fn on_mouse_event(&mut self, event: &Event) -> EventResult {
        let Event::Mouse {
            position,
            event: mouse,
        } = event
        else {
            unreachable!()
        };
        if self.list_rect.contains(*position) {
            match mouse {
                MouseEvent::Press(MouseButton::Left) => {
                    if let Some(i) = self
                        .tabs
                        .iter()
                        .position(|tab| tab.view.is_some() && tab.title_rect.contains(*position))
                    {
                        self.active = i;
                        self.list_focused = true;
                        return EventResult::consumed();
                    }
                }
                MouseEvent::WheelUp => return self.switch(-1),
                MouseEvent::WheelDown => return self.switch(1),
                _ => {}
            }
            return EventResult::Ignored;
        }
        let Some(view) = self.active_view() else {
            return EventResult::Ignored;
        };
        let result = view.on_event(event);
        if result.is_consumed() && self.content_takes_focus() {
            self.list_focused = false;
        }
        result
    }
}

impl Component for Tabs {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        if area.height == 0 || area.width == 0 {
            return;
        }
        let list_width = self.list_width().min(area.width);
        self.list_rect = Rect::new(area.x, area.y, list_width, area.height);
        let active_style = if focused && self.list_focused {
            highlight()
        } else {
            highlight_inactive()
        };
        let rows = area.height as usize;
        if self.active < self.list_offset {
            self.list_offset = self.active;
        } else if self.active >= self.list_offset + rows {
            self.list_offset = self.active + 1 - rows;
        }
        for (i, tab) in self.tabs.iter_mut().enumerate() {
            if i < self.list_offset || i >= self.list_offset + rows {
                tab.title_rect = Rect::default();
                continue;
            }
            let y = area.y + (i - self.list_offset) as u16;
            let style = if tab.view.is_none() {
                Style::default().add_modifier(Modifier::BOLD)
            } else if i == self.active {
                active_style
            } else {
                Style::default()
            };
            // Padded to the list width, so that the highlight is a full bar
            let label = format!(" {:<width$}", tab.title, width = list_width as usize - 1);
            let width = print_str(canvas.buf, area.x, y, self.list_rect, &label, style);
            tab.title_rect = Rect::new(area.x, y, width, if width > 0 { 1 } else { 0 });
        }
        let separator_x = area.x.saturating_add(list_width);
        for y in area.top()..area.bottom() {
            print_str(
                canvas.buf,
                separator_x,
                y,
                area,
                LIST_SEPARATOR,
                Style::default(),
            );
        }

        let content_x = separator_x.saturating_add(1);
        self.content_rect = Rect::new(
            content_x,
            area.y,
            area.right().saturating_sub(content_x),
            area.height,
        );
        let content_rect = self.content_rect;
        let list_focused = self.list_focused;
        if content_rect.width > 0
            && let Some(view) = self.active_view()
        {
            view.draw(canvas, content_rect, focused && !list_focused);
        }
    }

    fn required_size(&mut self, max: Size) -> Size {
        let list_width = self.list_width().saturating_add(1);
        // Sized by the largest tab, so that switching does not resize the
        // enclosing dialog
        let content_max = Size::new(max.width.saturating_sub(list_width), max.height);
        let mut content = Size::new(0, 0);
        for view in self.tabs.iter_mut().filter_map(|tab| tab.view.as_mut()) {
            let size = view.required_size(content_max);
            content.width = content.width.max(size.width);
            content.height = content.height.max(size.height);
        }
        // The list scrolls, so it does not add to the height
        Size::new(
            list_width.saturating_add(content.width).min(max.width),
            content.height.max(1).min(max.height),
        )
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        if !self.has_view(self.active) {
            return EventResult::Ignored;
        }
        match event {
            Event::Alt(Key::Up) => return self.switch(-1),
            Event::Alt(Key::Down) => return self.switch(1),
            Event::AltChar(c) if c.is_ascii_digit() => {
                let index = (*c as usize).wrapping_sub('1' as usize);
                if !self.has_view(index) {
                    return EventResult::Ignored;
                }
                self.active = index;
                return EventResult::consumed();
            }
            Event::Mouse { .. } => return self.on_mouse_event(event),
            _ => {}
        }

        if !self.content_takes_focus() {
            self.list_focused = true;
        }
        if self.list_focused {
            return self.on_list_event(event);
        }

        let result = self.active_view().unwrap().on_event(event);
        if result.is_consumed() {
            return result;
        }
        match event {
            Event::Key(Key::Left) | Event::Key(Key::BackTab) => {
                self.list_focused = true;
                EventResult::consumed()
            }
            _ => EventResult::Ignored,
        }
    }

    fn take_focus(&mut self) -> bool {
        self.tabs.iter().any(|tab| tab.view.is_some())
    }

    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        for view in self.tabs.iter_mut().filter_map(|tab| tab.view.as_mut()) {
            f(view);
        }
    }

    fn focus_name(&mut self, name: &str) -> bool {
        for i in 0..self.tabs.len() {
            if let Some(view) = &mut self.tabs[i].view
                && view.focus_name(name)
            {
                self.active = i;
                self.list_focused = false;
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::checkbox::Checkbox;
    use crate::tui::component::{Nameable, call_on_name};
    use crate::tui::text::TextView;
    use ratatui::buffer::Buffer;
    use ratatui::layout::Position;

    fn tabs() -> Tabs {
        Tabs::new()
            .tab("First", Checkbox::new().with_name("first"))
            .tab("Second", Checkbox::new().with_name("second"))
            .tab("Third", TextView::new("read only"))
    }

    fn draw(tabs: &mut Tabs, focused: bool) -> Buffer {
        let area = Rect::new(0, 0, 40, 5);
        let mut buf = Buffer::empty(area);
        let mut canvas = Canvas {
            buf: &mut buf,
            cursor: None,
        };
        tabs.draw(&mut canvas, area, focused);
        buf
    }

    fn row(buf: &Buffer, y: u16) -> String {
        (0..buf.area.width)
            .map(|x| buf[(x, y)].symbol().to_string())
            .collect()
    }

    #[test]
    fn switch_with_alt_keys() {
        let mut tabs = tabs();
        assert!(tabs.on_event(&Event::Alt(Key::Down)).is_consumed());
        assert_eq!(tabs.active(), 1);
        assert!(tabs.on_event(&Event::Alt(Key::Up)).is_consumed());
        assert!(tabs.on_event(&Event::Alt(Key::Up)).is_consumed());
        assert_eq!(tabs.active(), 2, "wraps around");
        assert!(tabs.on_event(&Event::AltChar('2')).is_consumed());
        assert_eq!(tabs.active(), 1);
        assert!(!tabs.on_event(&Event::AltChar('9')).is_consumed());
        assert_eq!(tabs.active(), 1);
    }

    #[test]
    fn list_focus_cycle() {
        let mut tabs = tabs();
        // Content first: a checkbox toggles on space
        assert!(tabs.on_event(&Event::Char(' ')).is_consumed());
        assert_eq!(
            call_on_name(&mut tabs, "first", |c: &mut Checkbox| c.is_checked()),
            Some(true)
        );
        // Shift-Tab leaves the content for the list, where Down switches
        assert!(tabs.on_event(&Event::Key(Key::BackTab)).is_consumed());
        assert!(tabs.list_focused);
        assert!(tabs.on_event(&Event::Key(Key::Down)).is_consumed());
        assert_eq!(tabs.active(), 1);
        assert!(!tabs.on_event(&Event::Char(' ')).is_consumed());
        // Tab goes back into the content
        assert!(tabs.on_event(&Event::Key(Key::Tab)).is_consumed());
        assert!(!tabs.list_focused);
        assert!(tabs.on_event(&Event::Char(' ')).is_consumed());
        assert_eq!(
            call_on_name(&mut tabs, "second", |c: &mut Checkbox| c.is_checked()),
            Some(true)
        );
        // A tab without focusable content keeps the list focused, and Tab
        // bubbles up
        tabs.set_active(2);
        assert!(!tabs.on_event(&Event::Key(Key::Tab)).is_consumed());
        assert!(tabs.list_focused);
        assert!(!tabs.on_event(&Event::Key(Key::BackTab)).is_consumed());
    }

    #[test]
    fn hidden_tabs_stay_reachable() {
        let mut tabs = tabs();
        assert_eq!(
            call_on_name(&mut tabs, "second", |c: &mut Checkbox| c.is_checked()),
            Some(false)
        );
        assert!(tabs.focus_name("second"));
        assert_eq!(tabs.active(), 1);
        assert!(!tabs.list_focused);
        assert!(!tabs.focus_name("nosuch"));
    }

    #[test]
    fn mouse_selects_title() {
        let mut tabs = tabs();
        let buf = draw(&mut tabs, true);
        // The list is as wide as the longest title plus the padding, the
        // content starts after the separator
        assert_eq!(row(&buf, 0).trim_end(), " First  │[ ]");
        assert_eq!(row(&buf, 1).trim_end(), " Second │");
        assert_eq!(row(&buf, 4).trim_end(), "        │");
        let second = tabs.tabs[1].title_rect;
        let click = Event::Mouse {
            position: Position::new(second.x + 1, second.y),
            event: MouseEvent::Press(MouseButton::Left),
        };
        assert!(tabs.on_event(&click).is_consumed());
        assert_eq!(tabs.active(), 1);
        assert!(tabs.list_focused);
    }

    #[test]
    fn groups_are_skipped() {
        let mut tabs = Tabs::new();
        tabs.add_group("Group");
        tabs.add_tab("  a", Checkbox::new().with_name("a"));
        tabs.add_tab("  b", Checkbox::new());
        assert_eq!(tabs.active(), 1, "the first real tab is active");
        assert!(tabs.on_event(&Event::Alt(Key::Down)).is_consumed());
        assert_eq!(tabs.active(), 2);
        assert!(tabs.on_event(&Event::Alt(Key::Down)).is_consumed());
        assert_eq!(tabs.active(), 1, "wraps over the group");
        tabs.set_active(0);
        assert_eq!(tabs.active(), 1);
        assert!(!tabs.on_event(&Event::AltChar('1')).is_consumed());
        let buf = draw(&mut tabs, true);
        assert_eq!(row(&buf, 0).trim_end(), " Group │[ ]");
        assert_eq!(row(&buf, 1).trim_end(), "   a   │");
        let group = tabs.tabs[0].title_rect;
        let click = Event::Mouse {
            position: Position::new(group.x + 1, group.y),
            event: MouseEvent::Press(MouseButton::Left),
        };
        assert!(!tabs.on_event(&click).is_consumed());
        assert!(tabs.focus_name("a"));
    }

    #[test]
    fn list_scrolls_to_the_active_tab() {
        let mut tabs = tabs();
        let area = Rect::new(0, 0, 20, 2);
        let draw_rows = |tabs: &mut Tabs| {
            let mut buf = Buffer::empty(area);
            let mut canvas = Canvas {
                buf: &mut buf,
                cursor: None,
            };
            tabs.draw(&mut canvas, area, true);
            (row(&buf, 0), row(&buf, 1))
        };
        let (first, second) = draw_rows(&mut tabs);
        assert!(first.starts_with(" First "));
        assert!(second.starts_with(" Second "));
        tabs.set_active(2);
        let (first, second) = draw_rows(&mut tabs);
        assert!(first.starts_with(" Second "));
        assert!(second.starts_with(" Third "));
        // The hidden title is no mouse target
        assert_eq!(tabs.tabs[0].title_rect, Rect::default());
        tabs.set_active(0);
        let (first, _) = draw_rows(&mut tabs);
        assert!(first.starts_with(" First "));
    }

    #[test]
    fn sized_by_the_largest_tab() {
        let mut tabs = Tabs::new()
            .tab("A", TextView::new("x"))
            .tab("B", TextView::new("a much longer line\nsecond"));
        let size = tabs.required_size(Size::new(80, 24));
        // list ("B" + padding) + separator + widest content; two lines of
        // content beat two titles
        assert_eq!(size, Size::new(3 + 1 + 18, 2));
    }
}
