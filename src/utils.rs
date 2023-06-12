use crate::ActionDescription;
use anyhow::{Context, Result};
use cursive::utils::markup::StyledString;
use cursive_syntect;
use skim::prelude::*;
use syntect::{highlighting::ThemeSet, parsing::SyntaxSet};

impl SkimItem for ActionDescription {
    fn text(&self) -> Cow<str> {
        return Cow::Borrowed(&self.text);
    }
}

// TODO: render from the bottom
pub fn fuzzy_actions(actions: Vec<ActionDescription>) -> Option<String> {
    let options = SkimOptionsBuilder::default()
        .height(Some("30%"))
        .build()
        .unwrap();

    let (tx, rx): (SkimItemSender, SkimItemReceiver) = unbounded();
    actions
        .iter()
        .map(|i| tx.send(Arc::new(i.clone())).unwrap())
        // TODO: can this be written better?
        // NOTE: len() optimizes map() out?
        .last();
    drop(tx);

    let out = Skim::run_with(&options, Some(rx));
    // FIXME:
    // - skim breaks resizing
    // - skim + tfg hung

    if out.is_none() {
        return None;
    }

    let out = out.unwrap();
    if out.is_abort {
        return None;
    }

    let selected_items = out.selected_items;
    if selected_items.is_empty() {
        return None;
    }

    // TODO: cast SkimItem to ActionDescription
    return Some(selected_items[0].text().into());
}

pub fn highlight_sql(text: &String) -> Result<StyledString> {
    let syntax_set = SyntaxSet::load_defaults_newlines();
    let ts = ThemeSet::load_defaults();
    let mut highlighter = syntect::easy::HighlightLines::new(
        syntax_set
            .find_syntax_by_token("sql")
            .context("Cannot load SQL syntax")?,
        &ts.themes["base16-ocean.dark"],
    );
    // NOTE: parse() does not interpret syntect::highlighting::Color::a (alpha/tranparency)
    return cursive_syntect::parse(text, &mut highlighter, &syntax_set)
        .context("Cannot highlight query");
}
