use anyhow::Result;
use ratatui::Frame;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Position, Rect, Size};
use ratatui::widgets::{Clear, Widget};
use std::any::Any;
use std::io::Stdout;
use std::time::Duration;

use super::component::{Boxed, Canvas, Component};
use super::event::{Callback, Event, EventResult};

/// Callback executed on the UI thread against the App (replaces
/// cursive::CbSink for background workers).
pub type UiCallback = Box<dyn FnOnce(&mut App) + Send>;
pub type UiSink = crossbeam_channel::Sender<UiCallback>;

pub enum LayerPosition {
    Center,
    FullScreen,
    At(u16, u16),
}

struct Layer {
    view: Boxed,
    position: LayerPosition,
}

pub struct App {
    root: Option<Boxed>,
    layers: Vec<Layer>,
    global_callbacks: Vec<(Event, Callback)>,
    cb_sink: UiSink,
    cb_source: crossbeam_channel::Receiver<UiCallback>,
    user_data: Option<Box<dyn Any>>,
    screen_size: Size,
    needs_clear: bool,
    running: bool,
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

impl App {
    pub fn new() -> Self {
        let (cb_sink, cb_source) = crossbeam_channel::unbounded();
        Self {
            root: None,
            layers: Vec::new(),
            global_callbacks: Vec::new(),
            cb_sink,
            cb_source,
            user_data: None,
            screen_size: Size::default(),
            needs_clear: false,
            running: true,
        }
    }

    pub fn cb_sink(&self) -> &UiSink {
        &self.cb_sink
    }

    pub fn quit(&mut self) {
        self.running = false;
    }

    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Force a full terminal repaint on the next draw (after an external
    /// program used the screen).
    pub fn complete_clear(&mut self) {
        self.needs_clear = true;
    }

    pub fn set_user_data<T: Any>(&mut self, data: T) {
        self.user_data = Some(Box::new(data));
    }

    pub fn user_data<T: Any>(&mut self) -> Option<&mut T> {
        self.user_data.as_mut()?.downcast_mut()
    }

    pub fn screen_size(&self) -> Size {
        self.screen_size
    }

    pub fn add_global_callback<E, F>(&mut self, event: E, cb: F)
    where
        E: Into<Event>,
        F: Fn(&mut App) + Send + Sync + 'static,
    {
        self.global_callbacks
            .push((event.into(), std::sync::Arc::new(cb)));
    }

    /// The base fullscreen view (the first `add_fullscreen_layer`).
    pub fn set_root<V: Component + 'static>(&mut self, view: V) {
        self.root = Some(Boxed::new(view));
    }

    fn push_layer(&mut self, view: Boxed, position: LayerPosition) {
        let mut view = view;
        // Focus the new layer's first focusable widget, otherwise containers
        // keep focus on their first (often non-focusable) child and key
        // presses leak to the global shortcuts.
        view.take_focus();
        self.layers.push(Layer { view, position });
    }

    pub fn add_layer<V: Component + 'static>(&mut self, view: V) {
        self.push_layer(Boxed::new(view), LayerPosition::Center);
    }

    pub fn add_fullscreen_layer<V: Component + 'static>(&mut self, view: V) {
        if self.root.is_none() {
            self.set_root(view);
        } else {
            self.push_layer(Boxed::new(view), LayerPosition::FullScreen);
        }
    }

    pub fn add_layer_at<V: Component + 'static>(&mut self, x: u16, y: u16, view: V) {
        self.push_layer(Boxed::new(view), LayerPosition::At(x, y));
    }

    pub fn pop_layer(&mut self) -> Option<Boxed> {
        self.layers.pop().map(|l| l.view)
    }

    /// Number of layers including the root (cursive's screen().len()).
    pub fn screen_len(&self) -> usize {
        self.layers.len() + self.root.is_some() as usize
    }

    /// Remove the topmost layer containing a view named `name`.
    /// Returns false if no such layer exists.
    pub fn remove_layer_by_name(&mut self, name: &str) -> bool {
        for i in (0..self.layers.len()).rev() {
            let mut found = false;
            super::component::call_on_any(self.layers[i].view.0.as_mut(), name, &mut |_| {
                found = true;
            });
            if found {
                self.layers.remove(i);
                return true;
            }
        }
        false
    }

    pub fn call_on_name<V: Component, F, R>(&mut self, name: &str, cb: F) -> Option<R>
    where
        F: FnOnce(&mut V) -> R,
    {
        let mut cb = Some(cb);
        let mut result = None;
        {
            let mut visit = |comp: &mut dyn Component| {
                if let Some(v) = comp.downcast_mut::<V>()
                    && let Some(cb) = cb.take()
                {
                    result = Some(cb(v));
                }
            };
            for layer in self.layers.iter_mut().rev() {
                super::component::call_on_any(layer.view.0.as_mut(), name, &mut visit);
            }
            if let Some(root) = &mut self.root {
                super::component::call_on_any(root.0.as_mut(), name, &mut visit);
            }
        }
        result
    }

    /// Move focus to the named view. Returns false when not found.
    pub fn focus_name(&mut self, name: &str) -> bool {
        for layer in self.layers.iter_mut().rev() {
            if layer.view.focus_name(name) {
                return true;
            }
        }
        if let Some(root) = &mut self.root {
            return root.focus_name(name);
        }
        false
    }

    pub fn has_view(&mut self, name: &str) -> bool {
        self.focus_name(name)
    }

    pub fn draw(&mut self, frame: &mut Frame<'_>) {
        let area = frame.area();
        self.screen_size = Size::new(area.width, area.height);
        let mut canvas = Canvas {
            buf: frame.buffer_mut(),
            cursor: None,
        };

        let layers_len = self.layers.len();
        if let Some(root) = &mut self.root {
            root.draw(&mut canvas, area, layers_len == 0);
        }
        for i in 0..layers_len {
            let focused = i + 1 == layers_len;
            let layer = &mut self.layers[i];
            let rect = match layer.position {
                LayerPosition::FullScreen => area,
                LayerPosition::Center => {
                    let max =
                        Size::new(area.width.saturating_sub(2), area.height.saturating_sub(2));
                    let size = layer.view.required_size(max);
                    Rect::new(
                        area.x + (area.width.saturating_sub(size.width)) / 2,
                        area.y + (area.height.saturating_sub(size.height)) / 2,
                        size.width,
                        size.height,
                    )
                }
                LayerPosition::At(x, y) => {
                    let size = layer.view.required_size(Size::new(
                        area.width.saturating_sub(x),
                        area.height.saturating_sub(y),
                    ));
                    Rect::new(x, y, size.width, size.height)
                }
            };
            if !matches!(layer.position, LayerPosition::At(..)) {
                Clear.render(rect, canvas.buf);
            }
            layer.view.draw(&mut canvas, rect, focused);
        }

        if let Some((x, y)) = canvas.cursor {
            frame.set_cursor_position(Position::new(x, y));
        }
    }

    pub fn on_event(&mut self, event: Event) {
        let result = if let Some(layer) = self.layers.last_mut() {
            layer.view.on_event(&event)
        } else if let Some(root) = &mut self.root {
            root.on_event(&event)
        } else {
            EventResult::Ignored
        };

        match result {
            EventResult::Consumed(Some(cb)) => cb(self),
            EventResult::Consumed(None) => {}
            EventResult::Ignored => {
                let callbacks: Vec<Callback> = self
                    .global_callbacks
                    .iter()
                    .filter(|(trigger, _)| *trigger == event)
                    .map(|(_, cb)| cb.clone())
                    .collect();
                for cb in callbacks {
                    cb(self);
                }
            }
        }
    }

    /// Drain pending worker callbacks (public for headless test harnesses).
    pub fn process_callbacks(&mut self) {
        while let Ok(cb) = self.cb_source.try_recv() {
            cb(self);
        }
    }

    /// Main loop: draw, poll input, drain worker callbacks.
    ///
    /// Input is polled with a timeout instead of a reader thread so that
    /// nested full-screen apps (flamelens) can take over `event::read()`.
    pub fn run(
        &mut self,
        terminal: &mut ratatui::Terminal<CrosstermBackend<Stdout>>,
    ) -> Result<()> {
        // The draw diff starts from an all-blank back buffer: blank cells are
        // never emitted, so without an explicit clear the previous terminal
        // content shows through.
        terminal.clear()?;
        while self.running {
            self.process_callbacks();
            if !self.running {
                break;
            }
            if self.needs_clear {
                self.needs_clear = false;
                terminal.clear()?;
            }
            terminal.draw(|frame| self.draw(frame))?;

            if crossterm::event::poll(Duration::from_millis(30))? {
                // Drain the whole burst before redrawing.
                loop {
                    if let Some(event) = Event::from_crossterm(crossterm::event::read()?) {
                        self.on_event(event);
                    }
                    if self.needs_clear || !crossterm::event::poll(Duration::ZERO)? {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Terminal setup/teardown for the main app (raw mode + alternate screen).
pub struct TerminalGuard;

impl TerminalGuard {
    pub fn enter() -> Result<Self> {
        crossterm::terminal::enable_raw_mode()?;
        crossterm::execute!(
            std::io::stdout(),
            crossterm::terminal::EnterAlternateScreen,
            crossterm::event::EnableMouseCapture,
            crossterm::cursor::Hide,
        )?;
        Ok(TerminalGuard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = crossterm::execute!(
            std::io::stdout(),
            crossterm::event::DisableMouseCapture,
            crossterm::style::ResetColor,
            crossterm::cursor::Show,
            crossterm::terminal::LeaveAlternateScreen,
        );
        let _ = crossterm::terminal::disable_raw_mode();
    }
}
