//! Replacement for cursive-flexi-logger-view: a flexi_logger LogWriter that
//! appends formatted records into a shared ring buffer, plus a DebugConsole
//! component rendering it (toggled as a fullscreen layer).

use flexi_logger::writers::LogWriter;
use flexi_logger::{DeferredNow, FormatFunction};
use log::Record;
use ratatui::layout::{Position, Rect, Size};
use std::collections::VecDeque;
use std::sync::Mutex;
use unicode_width::UnicodeWidthChar;

use super::app::App;
use super::component::{Canvas, Component, Nameable};
use super::dialog::Dialog;
use super::event::{Event, EventResult, Key, MouseEvent};
use super::style::{Color, Modifier, Style, print_str};

const MAX_LINES: usize = 10_000;

static LOGS: Mutex<VecDeque<(log::Level, String)>> = Mutex::new(VecDeque::new());

const DEBUG_CONSOLE_VIEW_NAME: &str = "flexi_logger_debug_console";

struct DebugConsoleWriter {
    format: FormatFunction,
}

impl LogWriter for DebugConsoleWriter {
    fn write(&self, now: &mut DeferredNow, record: &Record<'_>) -> std::io::Result<()> {
        let mut buffer = Vec::new();
        (self.format)(&mut buffer, now, record)?;
        let text = String::from_utf8_lossy(&buffer);

        let mut logs = LOGS.lock().unwrap();
        // Multi-line messages become one buffer entry per line, so scrolling
        // and the ring capacity operate on display lines.
        for line in text.lines() {
            if logs.len() >= MAX_LINES {
                logs.pop_front();
            }
            logs.push_back((record.level(), line.to_string()));
        }
        Ok(())
    }

    fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn format(&mut self, format: FormatFunction) {
        self.format = format;
    }
}

/// Log writer for `Logger::log_to_writer()` (colors are applied at draw time,
/// so the non-colored `with_thread` format is used).
pub fn log_writer() -> Box<dyn LogWriter> {
    Box::new(DebugConsoleWriter {
        format: flexi_logger::with_thread,
    })
}

fn level_style(level: log::Level) -> Style {
    match level {
        log::Level::Error => Style::default().fg(Color::Red),
        log::Level::Warn => Style::default().fg(Color::Yellow),
        log::Level::Info => Style::default(),
        log::Level::Debug | log::Level::Trace => Style::default().add_modifier(Modifier::DIM),
    }
}

/// Scrollable view over the shared log ring buffer, following the tail until
/// the user scrolls away (scrolling back to the bottom re-enables following).
#[derive(Default)]
pub struct DebugConsole {
    offset_y: usize,
    offset_x: usize,
    stick_to_bottom: bool,
    last_area: Rect,
}

impl DebugConsole {
    pub fn new() -> Self {
        Self {
            stick_to_bottom: true,
            ..Default::default()
        }
    }

    fn max_offset_y(&self) -> usize {
        let lines = LOGS.lock().unwrap().len();
        lines.saturating_sub(self.last_area.height as usize)
    }

    fn scroll_by(&mut self, delta: isize) {
        let max = self.max_offset_y();
        if self.stick_to_bottom {
            self.offset_y = max;
        }
        self.offset_y = self.offset_y.saturating_add_signed(delta).min(max);
        self.stick_to_bottom = self.offset_y >= max;
    }
}

impl Component for DebugConsole {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, _focused: bool) {
        self.last_area = area;
        let logs = LOGS.lock().unwrap();
        let height = area.height as usize;
        let max_offset = logs.len().saturating_sub(height);
        if self.stick_to_bottom {
            self.offset_y = max_offset;
        } else {
            self.offset_y = self.offset_y.min(max_offset);
        }

        for (i, (level, line)) in logs.iter().skip(self.offset_y).take(height).enumerate() {
            // Horizontal scrolling: skip offset_x display columns
            let mut skip = self.offset_x;
            let visible: String = line
                .chars()
                .skip_while(|ch| {
                    if skip == 0 {
                        return false;
                    }
                    skip = skip.saturating_sub(ch.width().unwrap_or(0));
                    true
                })
                .collect();
            print_str(
                canvas.buf,
                area.x,
                area.y + i as u16,
                area,
                &visible,
                level_style(*level),
            );
        }
    }

    fn required_size(&mut self, max: Size) -> Size {
        max
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        let page = self.last_area.height.max(1) as isize;
        match event {
            Event::Key(Key::Up) => self.scroll_by(-1),
            Event::Key(Key::Down) => self.scroll_by(1),
            Event::Key(Key::PageUp) => self.scroll_by(-page),
            Event::Key(Key::PageDown) => self.scroll_by(page),
            Event::Key(Key::Home) => {
                self.offset_y = 0;
                self.offset_x = 0;
                self.stick_to_bottom = false;
            }
            Event::Key(Key::End) => {
                self.stick_to_bottom = true;
            }
            Event::Key(Key::Left) => {
                self.offset_x = self.offset_x.saturating_sub(8);
            }
            Event::Key(Key::Right) => {
                self.offset_x += 8;
            }
            Event::Mouse {
                position,
                event: mouse,
            } if self
                .last_area
                .contains(Position::new(position.x, position.y)) =>
            {
                match mouse {
                    MouseEvent::WheelUp => self.scroll_by(-3),
                    MouseEvent::WheelDown => self.scroll_by(3),
                    _ => return EventResult::Ignored,
                }
            }
            _ => return EventResult::Ignored,
        }
        EventResult::consumed()
    }

    fn take_focus(&mut self) -> bool {
        true
    }
}

/// Show the debug console as a fullscreen layer, or remove it if it is
/// already shown (bind to a single toggle key).
pub fn toggle_debug_console(app: &mut App) {
    if app.remove_layer_by_name(DEBUG_CONSOLE_VIEW_NAME) {
        return;
    }
    app.add_fullscreen_layer(
        Dialog::around(DebugConsole::new().with_name(DEBUG_CONSOLE_VIEW_NAME))
            .title("Debug console"),
    );
}
