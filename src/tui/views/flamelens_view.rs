use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::layout::Rect;

use std::sync::Arc;

use crate::interpreter::{BackgroundRunner, ContextArc, EventOwner, options::ChDigViews};
use crate::tui::app::App;
use crate::tui::component::{Canvas, Component};
use crate::tui::event::{Event, EventResult, Key};
use crate::tui::navigation::Navigation;

/// Embeds the flamelens viewer as a pane (view.flamelens_pane), as opposed to
/// the fullscreen terminal takeover of `interpreter::flamegraph::show`.
pub struct FlamelensView {
    fl: flamelens::app::App,
    // Keeps the periodic live updates running; dropping it with the pane
    // stops the runner thread and cancels the in-flight query (EventOwner).
    #[allow(unused)]
    live: Option<(BackgroundRunner, Arc<EventOwner>)>,
}

impl FlamelensView {
    pub fn new(fl: flamelens::app::App, live: Option<(BackgroundRunner, Arc<EventOwner>)>) -> Self {
        Self { fl, live }
    }
}

fn key_code(key: Key) -> Option<KeyCode> {
    Some(match key {
        Key::Enter => KeyCode::Enter,
        Key::Tab => KeyCode::Tab,
        Key::Backspace => KeyCode::Backspace,
        Key::Esc => KeyCode::Esc,
        Key::Left => KeyCode::Left,
        Key::Right => KeyCode::Right,
        Key::Up => KeyCode::Up,
        Key::Down => KeyCode::Down,
        Key::Ins => KeyCode::Insert,
        Key::Del => KeyCode::Delete,
        Key::Home => KeyCode::Home,
        Key::End => KeyCode::End,
        Key::PageUp => KeyCode::PageUp,
        Key::PageDown => KeyCode::PageDown,
        // F-keys stay with chdig (menus and global shortcuts)
        _ => return None,
    })
}

fn to_key_event(event: &Event) -> Option<KeyEvent> {
    match event {
        Event::Char(c) => Some(KeyEvent::new(KeyCode::Char(*c), KeyModifiers::NONE)),
        Event::Key(k) => Some(KeyEvent::new(key_code(*k)?, KeyModifiers::NONE)),
        Event::Shift(k) => Some(KeyEvent::new(key_code(*k)?, KeyModifiers::SHIFT)),
        // Ctrl/Alt combos stay with chdig (pane navigation/resize/zoom)
        _ => None,
    }
}

/// The flamelens pane can end up being the lone pane (its neighbors were
/// closed while it was open), which the Mux cannot remove: restore the
/// current chdig view instead.
fn close_pane(app: &mut App) {
    if app.close_pane() {
        return;
    }
    let context = app.user_data::<ContextArc>().unwrap().clone();
    let provider = {
        let ctx = context.lock().unwrap();
        let current_view = ctx
            .current_view
            .or(ctx.options.start_view())
            .unwrap_or(ChDigViews::Queries);
        ctx.view_registry.get_by_view_type(current_view)
    };
    app.drop_main_view();
    provider.show(app, context);
}

impl Component for FlamelensView {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        // Swaps in a pending live update, if any
        self.fl.tick();
        let cursor = flamelens::ui::render_in_area(&mut self.fl, area, canvas.buf);
        if focused && cursor.is_some() {
            canvas.cursor = cursor;
        }
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        // In live mode r/R force a refresh instead of flamelens's reset
        // (Esc covers that); not while typing into the search buffer.
        if matches!(event, Event::Char('r' | 'R'))
            && self.fl.input_buffer.is_none()
            && let Some((runner, _)) = self.live.as_mut()
        {
            runner.schedule();
            return EventResult::consumed();
        }
        // flamelens always consumes Esc (unzoom); with nothing to unwind let
        // it bubble up to the global "close pane" action.
        if *event == Event::Key(Key::Esc)
            && self.fl.input_buffer.is_none()
            && self.fl.flamegraph_state().zoom.is_none()
        {
            return EventResult::Ignored;
        }
        let Some(key) = to_key_event(event) else {
            return EventResult::Ignored;
        };
        // Handler errors abort the viewer, as in the fullscreen event loop
        if flamelens::handler::handle_key_events(key, &mut self.fl).is_err() {
            self.fl.quit();
        }
        if !self.fl.running {
            return EventResult::with_cb_once(close_pane);
        }
        EventResult::consumed()
    }

    fn take_focus(&mut self) -> bool {
        true
    }
}
