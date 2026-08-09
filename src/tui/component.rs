use ratatui::buffer::Buffer;
use ratatui::layout::{Rect, Size};
use std::any::Any;

use super::event::{Event, EventResult};

/// Draw target: the frame buffer plus the cursor position requested by the
/// focused editable widget (bubbles up to the terminal).
pub struct Canvas<'a> {
    pub buf: &'a mut Buffer,
    pub cursor: Option<(u16, u16)>,
}

pub trait AsAny {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T: Any> AsAny for T {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Retained UI element rendered with ratatui.
///
/// Unlike idiomatic immediate-mode ratatui, components own their state and
/// form a tree; the tree is searchable by name (`call_on_name`) so background
/// workers can mutate views without holding references to them.
pub trait Component: AsAny + Send + 'static {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool);

    /// Preferred size within `max` (used for dialog/layout sizing).
    fn required_size(&mut self, max: Size) -> Size {
        max
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        let _ = event;
        EventResult::Ignored
    }

    /// Whether the component accepts focus.
    fn take_focus(&mut self) -> bool {
        false
    }

    /// Visit direct children (drives name lookup and focus traversal).
    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        let _ = f;
    }

    fn name(&self) -> Option<&str> {
        None
    }

    /// Move focus to the descendant named `name`. Containers adjust their
    /// focus index along the path. Returns true when found.
    fn focus_name(&mut self, name: &str) -> bool {
        if self.name() == Some(name) {
            return true;
        }
        let mut found = false;
        self.for_each_child(&mut |child| {
            if !found {
                found = child.focus_name(name);
            }
        });
        found
    }
}

impl dyn Component {
    pub fn downcast_mut<T: Component>(&mut self) -> Option<&mut T> {
        self.as_any_mut().downcast_mut::<T>()
    }
}

/// Depth-first traversal calling `f` on every component named `name`.
/// A name carried by a wrapper (NamedView) resolves to its content as well,
/// so downcasts to the inner view type succeed (cursive semantics).
pub fn call_on_any(root: &mut dyn Component, name: &str, f: &mut dyn FnMut(&mut dyn Component)) {
    if root.name() == Some(name) {
        f(root);
        root.for_each_child(&mut |child| f(child));
    }
    root.for_each_child(&mut |child| call_on_any(child, name, f));
}

/// Find the component named `name` of type `V` and run `cb` on it.
pub fn call_on_name<V: Component, F, R>(root: &mut dyn Component, name: &str, cb: F) -> Option<R>
where
    F: FnOnce(&mut V) -> R,
{
    let mut cb = Some(cb);
    let mut result = None;
    call_on_any(root, name, &mut |comp| {
        if let Some(v) = comp.downcast_mut::<V>()
            && let Some(cb) = cb.take()
        {
            result = Some(cb(v));
        }
    });
    result
}

/// A component with an attached name for tree lookups.
pub struct NamedView<V> {
    name: String,
    view: V,
}

impl<V: Component> NamedView<V> {
    pub fn new(name: impl Into<String>, view: V) -> Self {
        Self {
            name: name.into(),
            view,
        }
    }

    pub fn get_mut(&mut self) -> &mut V {
        &mut self.view
    }
}

impl<V: Component> Component for NamedView<V> {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        self.view.draw(canvas, area, focused);
    }

    fn required_size(&mut self, max: Size) -> Size {
        self.view.required_size(max)
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

    fn name(&self) -> Option<&str> {
        Some(&self.name)
    }

    fn focus_name(&mut self, name: &str) -> bool {
        self.name == name || self.view.focus_name(name)
    }
}

pub trait Nameable: Component + Sized {
    fn with_name(self, name: impl Into<String>) -> NamedView<Self> {
        NamedView::new(name, self)
    }
}

impl<V: Component + Sized> Nameable for V {}

/// Type-erased component (containers with heterogeneous children).
pub struct Boxed(pub Box<dyn Component>);

impl Boxed {
    pub fn new<V: Component + 'static>(view: V) -> Self {
        Boxed(Box::new(view))
    }
}

impl Component for Boxed {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        self.0.draw(canvas, area, focused);
    }

    fn required_size(&mut self, max: Size) -> Size {
        self.0.required_size(max)
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        self.0.on_event(event)
    }

    fn take_focus(&mut self) -> bool {
        self.0.take_focus()
    }

    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        f(self.0.as_mut());
    }

    fn focus_name(&mut self, name: &str) -> bool {
        self.0.focus_name(name)
    }
}

/// Empty spacer.
#[derive(Default)]
pub struct DummyView;

impl Component for DummyView {
    fn draw(&mut self, _canvas: &mut Canvas<'_>, _area: Rect, _focused: bool) {}

    fn required_size(&mut self, _max: Size) -> Size {
        Size::new(1, 1)
    }
}

type PlainHandler = std::sync::Arc<dyn Fn(&mut super::app::App) + Send + Sync>;
type InnerHandler<V> = Box<dyn Fn(&mut V, &Event) -> Option<EventResult> + Send + Sync>;

/// Wrapper attaching extra event handlers to a view (cursive's OnEventView).
pub struct OnEventView<V> {
    inner: V,
    pre: Vec<(Event, PlainHandler)>,
    pre_inner: Vec<(Event, InnerHandler<V>)>,
    post: Vec<(Event, PlainHandler)>,
    post_inner: Vec<(Event, InnerHandler<V>)>,
}

impl<V: Component> OnEventView<V> {
    pub fn new(inner: V) -> Self {
        Self {
            inner,
            pre: Vec::new(),
            pre_inner: Vec::new(),
            post: Vec::new(),
            post_inner: Vec::new(),
        }
    }

    pub fn get_inner_mut(&mut self) -> &mut V {
        &mut self.inner
    }

    /// Consumed before the inner view sees the event.
    pub fn on_pre_event<E, F>(mut self, event: E, cb: F) -> Self
    where
        E: Into<Event>,
        F: Fn(&mut super::app::App) + Send + Sync + 'static,
    {
        self.pre.push((event.into(), std::sync::Arc::new(cb)));
        self
    }

    /// Runs before the inner view; consumed only if the handler returns Some.
    pub fn on_pre_event_inner<E, F>(mut self, event: E, cb: F) -> Self
    where
        E: Into<Event>,
        F: Fn(&mut V, &Event) -> Option<EventResult> + Send + Sync + 'static,
    {
        self.pre_inner.push((event.into(), Box::new(cb)));
        self
    }

    /// Consumed if the inner view ignored the event.
    pub fn on_event<E, F>(mut self, event: E, cb: F) -> Self
    where
        E: Into<Event>,
        F: Fn(&mut super::app::App) + Send + Sync + 'static,
    {
        self.post.push((event.into(), std::sync::Arc::new(cb)));
        self
    }

    /// Runs if the inner view ignored the event.
    pub fn on_event_inner<E, F>(mut self, event: E, cb: F) -> Self
    where
        E: Into<Event>,
        F: Fn(&mut V, &Event) -> Option<EventResult> + Send + Sync + 'static,
    {
        self.post_inner.push((event.into(), Box::new(cb)));
        self
    }

    pub fn set_on_event_inner<E, F>(&mut self, event: E, cb: F)
    where
        E: Into<Event>,
        F: Fn(&mut V, &Event) -> Option<EventResult> + Send + Sync + 'static,
    {
        self.post_inner.push((event.into(), Box::new(cb)));
    }

    pub fn set_on_pre_event_inner<E, F>(&mut self, event: E, cb: F)
    where
        E: Into<Event>,
        F: Fn(&mut V, &Event) -> Option<EventResult> + Send + Sync + 'static,
    {
        self.pre_inner.push((event.into(), Box::new(cb)));
    }
}

impl<V: Component + 'static> Component for OnEventView<V> {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        self.inner.draw(canvas, area, focused);
    }

    fn required_size(&mut self, max: Size) -> Size {
        self.inner.required_size(max)
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        for (trigger, cb) in &self.pre {
            if trigger == event {
                let cb = cb.clone();
                return EventResult::Consumed(Some(std::sync::Arc::new(move |app| cb(app))));
            }
        }
        for (trigger, cb) in &self.pre_inner {
            if trigger == event
                && let Some(result) = cb(&mut self.inner, event)
            {
                return result;
            }
        }

        let result = self.inner.on_event(event);
        if result.is_consumed() {
            return result;
        }

        for (trigger, cb) in &self.post_inner {
            if trigger == event
                && let Some(result) = cb(&mut self.inner, event)
            {
                return result;
            }
        }
        for (trigger, cb) in &self.post {
            if trigger == event {
                let cb = cb.clone();
                return EventResult::Consumed(Some(std::sync::Arc::new(move |app| cb(app))));
            }
        }
        EventResult::Ignored
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
