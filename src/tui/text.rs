use ratatui::layout::{Alignment, Rect, Size};
use ratatui::widgets::{Paragraph, Widget, Wrap};

use super::component::{Canvas, Component};
use super::style::{Style, StyledString};

/// Styled text block with wrapping (cursive's TextView).
#[derive(Default)]
pub struct TextView {
    content: StyledString,
    align: Alignment,
    style: Style,
    wrap: bool,
}

impl TextView {
    pub fn new(content: impl Into<StyledString>) -> Self {
        Self {
            content: content.into(),
            align: Alignment::Left,
            style: Style::default(),
            wrap: true,
        }
    }

    pub fn empty() -> Self {
        Self::new("")
    }

    pub fn center(mut self) -> Self {
        self.align = Alignment::Center;
        self
    }

    pub fn style(mut self, style: impl Into<Style>) -> Self {
        self.style = style.into();
        self
    }

    pub fn no_wrap(mut self) -> Self {
        self.wrap = false;
        self
    }

    pub fn set_content(&mut self, content: impl Into<StyledString>) {
        self.content = content.into();
    }

    pub fn append(&mut self, content: impl Into<StyledString>) {
        self.content.append(content.into());
    }

    pub fn get_content(&self) -> &StyledString {
        &self.content
    }

    fn paragraph(&self) -> Paragraph<'_> {
        let mut p = Paragraph::new(self.content.as_text().clone())
            .alignment(self.align)
            .style(self.style);
        if self.wrap {
            p = p.wrap(Wrap { trim: false });
        }
        p
    }
}

impl Component for TextView {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, _focused: bool) {
        self.paragraph().render(area, canvas.buf);
    }

    fn required_size(&mut self, max: Size) -> Size {
        let width = (self.content.width() as u16).min(max.width);
        let height = if self.wrap && width > 0 {
            self.paragraph().line_count(width) as u16
        } else {
            self.content.height() as u16
        };
        Size::new(width, height.min(max.height).max(1))
    }
}
