use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span, Text};
use unicode_width::UnicodeWidthStr;

pub use ratatui::style::{Color, Modifier, Style};

/// Highlight of the focused selection (cursive: Light(Cyan) over black text).
pub fn highlight() -> Style {
    Style::new().bg(Color::LightCyan).fg(Color::Black)
}

/// Highlight of the selection in an unfocused view.
pub fn highlight_inactive() -> Style {
    Style::new().bg(Color::Blue).fg(Color::Black)
}

/// Owned styled text with cursive-like append API (multi-line aware).
#[derive(Clone, Default)]
pub struct StyledString {
    text: Text<'static>,
}

impl StyledString {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn plain<S: Into<String>>(s: S) -> Self {
        let mut this = Self::new();
        this.append_plain(s);
        this
    }

    pub fn styled<S: Into<String>>(s: S, style: impl Into<Style>) -> Self {
        let mut this = Self::new();
        this.append_styled(s, style);
        this
    }

    pub fn append_plain<S: Into<String>>(&mut self, s: S) {
        self.append_styled(s, Style::default());
    }

    pub fn append_styled<S: Into<String>>(&mut self, s: S, style: impl Into<Style>) {
        let s = s.into();
        let style = style.into();
        let mut lines = s.split('\n');
        if let Some(first) = lines.next() {
            if self.text.lines.is_empty() {
                self.text.lines.push(Line::default());
            }
            if !first.is_empty() {
                self.text
                    .lines
                    .last_mut()
                    .unwrap()
                    .push_span(Span::styled(first.to_string(), style));
            }
        }
        for line in lines {
            let mut new_line = Line::default();
            if !line.is_empty() {
                new_line.push_span(Span::styled(line.to_string(), style));
            }
            self.text.lines.push(new_line);
        }
    }

    pub fn append(&mut self, other: StyledString) {
        let mut lines = other.text.lines.into_iter();
        if let Some(first) = lines.next() {
            if self.text.lines.is_empty() {
                self.text.lines.push(Line::default());
            }
            let last = self.text.lines.last_mut().unwrap();
            for span in first.spans {
                last.push_span(span);
            }
        }
        self.text.lines.extend(lines);
    }

    /// Raw text without styling.
    pub fn source(&self) -> String {
        self.text
            .lines
            .iter()
            .map(|l| {
                l.spans
                    .iter()
                    .map(|s| s.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn is_empty(&self) -> bool {
        self.text.lines.is_empty()
            || (self.text.lines.len() == 1 && self.text.lines[0].width() == 0)
    }

    /// Width of the widest line.
    pub fn width(&self) -> usize {
        self.text.lines.iter().map(|l| l.width()).max().unwrap_or(0)
    }

    pub fn height(&self) -> usize {
        self.text.lines.len().max(1)
    }

    pub fn as_text(&self) -> &Text<'static> {
        &self.text
    }

    pub fn into_text(self) -> Text<'static> {
        self.text
    }

    /// First line of the text (styled strings used in single-line contexts).
    pub fn first_line(&self) -> Line<'static> {
        self.text.lines.first().cloned().unwrap_or_default()
    }
}

impl From<&str> for StyledString {
    fn from(s: &str) -> Self {
        StyledString::plain(s)
    }
}

impl From<String> for StyledString {
    fn from(s: String) -> Self {
        StyledString::plain(s)
    }
}

impl From<Text<'static>> for StyledString {
    fn from(text: Text<'static>) -> Self {
        StyledString { text }
    }
}

/// Print a single-line styled string at (x, y), clipped to `area`.
/// Returns the printed width.
pub fn print_line(buf: &mut Buffer, x: u16, y: u16, area: Rect, line: &Line<'_>) -> u16 {
    if y < area.top() || y >= area.bottom() || x >= area.right() {
        return 0;
    }
    let max_width = area.right() - x;
    let (_, printed) = buf.set_line(x, y, line, max_width);
    printed - x
}

/// Print a plain string with a style at (x, y), clipped to `area`.
pub fn print_str(buf: &mut Buffer, x: u16, y: u16, area: Rect, s: &str, style: Style) -> u16 {
    if y < area.top() || y >= area.bottom() || x >= area.right() {
        return 0;
    }
    let max_width = (area.right() - x) as usize;
    let (_, printed) = buf.set_stringn(x, y, s, max_width, style);
    printed - x
}

/// Display width of a string.
pub fn str_width(s: &str) -> usize {
    UnicodeWidthStr::width(s)
}
