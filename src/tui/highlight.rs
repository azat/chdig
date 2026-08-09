use anyhow::{Context, Result};
use syntect::easy::HighlightLines;
use syntect::highlighting::{FontStyle, ThemeSet};
use syntect::parsing::SyntaxSet;

use super::style::{Color, Modifier, Style, StyledString};

fn convert_style(style: syntect::highlighting::Style) -> Style {
    let fg = style.foreground;
    let mut result = Style::default().fg(Color::Rgb(fg.r, fg.g, fg.b));
    if style.font_style.contains(FontStyle::BOLD) {
        result = result.add_modifier(Modifier::BOLD);
    }
    if style.font_style.contains(FontStyle::ITALIC) {
        result = result.add_modifier(Modifier::ITALIC);
    }
    if style.font_style.contains(FontStyle::UNDERLINE) {
        result = result.add_modifier(Modifier::UNDERLINED);
    }
    result
}

/// SQL syntax highlighting (replaces cursive-syntect).
pub fn highlight_sql(text: &str) -> Result<StyledString> {
    let syntax_set = SyntaxSet::load_defaults_newlines();
    let ts = ThemeSet::load_defaults();
    let mut highlighter = HighlightLines::new(
        syntax_set
            .find_syntax_by_token("sql")
            .context("Cannot load SQL syntax")?,
        &ts.themes["base16-ocean.dark"],
    );

    let mut result = StyledString::new();
    for line in text.split_inclusive('\n') {
        let regions = highlighter
            .highlight_line(line, &syntax_set)
            .context("Cannot highlight query")?;
        for (style, part) in regions {
            // NOTE: background is dropped on purpose (terminal default looks
            // better than the theme background).
            result.append_styled(part, convert_style(style));
        }
    }
    Ok(result)
}
