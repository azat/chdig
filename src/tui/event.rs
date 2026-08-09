use ratatui::layout::Position;
use std::sync::{Arc, Mutex};

use super::app::App;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Key {
    Enter,
    Tab,
    Backspace,
    Esc,
    Left,
    Right,
    Up,
    Down,
    Ins,
    Del,
    Home,
    End,
    PageUp,
    PageDown,
    F1,
    F2,
    F3,
    F4,
    F5,
    F6,
    F7,
    F8,
    F9,
    F10,
    F11,
    F12,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MouseButton {
    Left,
    Middle,
    Right,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MouseEvent {
    Press(MouseButton),
    Hold(MouseButton),
    Release(MouseButton),
    WheelUp,
    WheelDown,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Event {
    Char(char),
    CtrlChar(char),
    AltChar(char),
    Key(Key),
    Shift(Key),
    Ctrl(Key),
    Alt(Key),
    Mouse {
        position: Position,
        event: MouseEvent,
    },
    /// Synthetic event used to deliver pending view callbacks and force repaints.
    Refresh,
    WindowResize,
    /// Never produced by the terminal; used for actions without a shortcut.
    Unknown(Vec<u8>),
}

impl From<char> for Event {
    fn from(c: char) -> Self {
        Event::Char(c)
    }
}

impl From<Key> for Event {
    fn from(k: Key) -> Self {
        Event::Key(k)
    }
}

fn convert_key_code(code: crossterm::event::KeyCode) -> Option<Key> {
    use crossterm::event::KeyCode;
    Some(match code {
        KeyCode::Enter => Key::Enter,
        KeyCode::Tab | KeyCode::BackTab => Key::Tab,
        KeyCode::Backspace => Key::Backspace,
        KeyCode::Esc => Key::Esc,
        KeyCode::Left => Key::Left,
        KeyCode::Right => Key::Right,
        KeyCode::Up => Key::Up,
        KeyCode::Down => Key::Down,
        KeyCode::Insert => Key::Ins,
        KeyCode::Delete => Key::Del,
        KeyCode::Home => Key::Home,
        KeyCode::End => Key::End,
        KeyCode::PageUp => Key::PageUp,
        KeyCode::PageDown => Key::PageDown,
        KeyCode::F(1) => Key::F1,
        KeyCode::F(2) => Key::F2,
        KeyCode::F(3) => Key::F3,
        KeyCode::F(4) => Key::F4,
        KeyCode::F(5) => Key::F5,
        KeyCode::F(6) => Key::F6,
        KeyCode::F(7) => Key::F7,
        KeyCode::F(8) => Key::F8,
        KeyCode::F(9) => Key::F9,
        KeyCode::F(10) => Key::F10,
        KeyCode::F(11) => Key::F11,
        KeyCode::F(12) => Key::F12,
        _ => return None,
    })
}

impl Event {
    pub fn from_crossterm(event: crossterm::event::Event) -> Option<Event> {
        use crossterm::event::{Event as CEvent, KeyCode, KeyEventKind, KeyModifiers};

        match event {
            CEvent::Key(key) => {
                if key.kind == KeyEventKind::Release {
                    return None;
                }
                let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
                let alt = key.modifiers.contains(KeyModifiers::ALT);
                if let KeyCode::Char(c) = key.code {
                    return Some(match (ctrl, alt) {
                        (true, _) => Event::CtrlChar(c),
                        (false, true) => Event::AltChar(c),
                        (false, false) => Event::Char(c),
                    });
                }
                let k = convert_key_code(key.code)?;
                Some(if ctrl {
                    Event::Ctrl(k)
                } else if alt {
                    Event::Alt(k)
                } else if key.modifiers.contains(KeyModifiers::SHIFT) {
                    Event::Shift(k)
                } else {
                    Event::Key(k)
                })
            }
            CEvent::Mouse(m) => {
                use crossterm::event::{MouseButton as CButton, MouseEventKind};
                let button = |b: CButton| match b {
                    CButton::Left => MouseButton::Left,
                    CButton::Middle => MouseButton::Middle,
                    CButton::Right => MouseButton::Right,
                };
                let event = match m.kind {
                    MouseEventKind::Down(b) => MouseEvent::Press(button(b)),
                    MouseEventKind::Drag(b) => MouseEvent::Hold(button(b)),
                    MouseEventKind::Up(b) => MouseEvent::Release(button(b)),
                    MouseEventKind::ScrollUp => MouseEvent::WheelUp,
                    MouseEventKind::ScrollDown => MouseEvent::WheelDown,
                    _ => return None,
                };
                Some(Event::Mouse {
                    position: Position::new(m.column, m.row),
                    event,
                })
            }
            CEvent::Resize(..) => Some(Event::WindowResize),
            _ => None,
        }
    }
}

pub type Callback = Arc<dyn Fn(&mut App) + Send + Sync>;

pub enum EventResult {
    Ignored,
    Consumed(Option<Callback>),
}

impl EventResult {
    pub fn consumed() -> Self {
        EventResult::Consumed(None)
    }

    pub fn is_consumed(&self) -> bool {
        matches!(self, EventResult::Consumed(_))
    }

    pub fn with_cb<F>(f: F) -> Self
    where
        F: Fn(&mut App) + Send + Sync + 'static,
    {
        EventResult::Consumed(Some(Arc::new(f)))
    }

    pub fn with_cb_once<F>(f: F) -> Self
    where
        F: FnOnce(&mut App) + Send + 'static,
    {
        let f = Mutex::new(Some(f));
        EventResult::Consumed(Some(Arc::new(move |app| {
            if let Some(f) = f.lock().unwrap().take() {
                f(app);
            }
        })))
    }

    /// Chain two results: consumed wins, callbacks of both are kept.
    pub fn and(self, other: Self) -> Self {
        match (self, other) {
            (EventResult::Ignored, other) => other,
            (this, EventResult::Ignored) => this,
            (EventResult::Consumed(a), EventResult::Consumed(b)) => match (a, b) {
                (None, b) => EventResult::Consumed(b),
                (a, None) => EventResult::Consumed(a),
                (Some(a), Some(b)) => EventResult::Consumed(Some(Arc::new(move |app| {
                    a(app);
                    b(app);
                }))),
            },
        }
    }
}
