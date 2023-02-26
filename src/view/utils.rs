use anyhow::{Context, Result};
use cursive::event::{Event, Key};
use cursive::menu;
use cursive::theme::{BaseColor, Color, PaletteColor, Theme};
use cursive::utils::markup::StyledString;
use cursive::views::Dialog;
use cursive::Cursive;
use cursive_syntect;
use skim::prelude::*;
use syntect::{highlighting::ThemeSet, parsing::SyntaxSet};

use crate::interpreter::ContextArc;
use crate::view::Navigation;

// FIXME: most of this can be reimplemented as trait for cursive

#[derive(Debug, Clone)]
struct ShortcutItem {
    text: String,
    event: Event,
}
impl SkimItem for ShortcutItem {
    fn text(&self) -> Cow<str> {
        return Cow::Borrowed(&self.text);
    }

    fn preview(&self, _context: PreviewContext) -> ItemPreview {
        return ItemPreview::Text(self.text.to_owned());
    }
}

// TODO: use the same color schema as in htop/csysdig
pub fn make_cursive_theme_from_therminal(siv: &Cursive) -> Theme {
    let mut theme = siv.current_theme().clone();
    theme.palette[PaletteColor::Background] = Color::TerminalDefault;
    theme.palette[PaletteColor::View] = Color::TerminalDefault;
    theme.palette[PaletteColor::Primary] = Color::TerminalDefault;
    theme.palette[PaletteColor::Highlight] = Color::Light(BaseColor::Cyan);
    theme.palette[PaletteColor::HighlightText] = Color::Dark(BaseColor::Black);
    theme.shadow = false;
    return theme;
}

pub fn pop_ui(siv: &mut cursive::Cursive) {
    if siv.screen_mut().len() == 1 {
        siv.quit();
    } else {
        siv.pop_layer();
    }
}

// TODO: more enhanced help (like in htop(1))
pub fn show_help_dialog(siv: &mut cursive::Cursive) {
    siv.add_layer(Dialog::info(
        r#"
    chdig - v0.001

    General shortcuts:

    F1          - show help
    Enter       - show query logs (from system.text_log)
    Up/Down/j/k - navigate through the queries
    ~           - chdig debug console
    q/Backspace - go back

    Query actions:

    l           - Show query logs
    D           - Query details
    C           - CPU flamegraph
    R           - Real flamegraph
    M           - Memory flamegraph
    L           - Live flamegraph
    e           - EXPLAIN PLAN
    E           - EXPLAIN PIPELINE
    K           - Kill this query (requires confirmation)

    Global server actions:

    F           - server flamegraph
                               "#,
    ));
}

pub fn add_menu(siv: &mut cursive::Cursive) {
    // TODO: color F<N> differently
    siv.menubar()
        .add_subtree(
            "F2: Views",
            menu::Tree::new().leaf("Processes", |s| {
                let context = s.user_data::<ContextArc>().unwrap().clone();
                s.show_clickhouse_processes(context);
            }),
        )
        .add_subtree(
            "F8: Actions",
            menu::Tree::new()
                .leaf("Show query logs  (l)", |s| s.on_event(Event::Char('l')))
                .leaf("Query details    (D)", |s| s.on_event(Event::Char('D')))
                .leaf("CPU flamegraph   (C)", |s| s.on_event(Event::Char('C')))
                .leaf("Real flamegraph  (R)", |s| s.on_event(Event::Char('R')))
                .leaf("Memory flamegraph(M)", |s| s.on_event(Event::Char('M')))
                .leaf("Live flamegraph  (L)", |s| s.on_event(Event::Char('L')))
                .leaf("EXPLAIN PLAN     (e)", |s| s.on_event(Event::Char('e')))
                .leaf("EXPLAIN PIPELINE (E)", |s| s.on_event(Event::Char('E')))
                .leaf("Kill this query  (K)", |s| s.on_event(Event::Char('K'))),
        )
        .add_leaf("F1: Help", |s| s.on_event(Event::Key(Key::F1)));

    siv.set_autohide_menu(false);
    siv.add_global_callback(Key::F2, |s| s.select_menubar());

    // TODO: simply use skim for actions?
}

fn fuzzy_shortcuts(siv: &mut cursive::Cursive) {
    let actions = vec![
        ShortcutItem {
            text: "Show query logs  (l)".to_string(),
            event: Event::Char('l'),
        },
        ShortcutItem {
            text: "Query details    (D)".to_string(),
            event: Event::Char('D'),
        },
        ShortcutItem {
            text: "CPU server flamegraph (F)".to_string(),
            event: Event::Char('F'),
        },
        ShortcutItem {
            text: "CPU flamegraph   (C)".to_string(),
            event: Event::Char('C'),
        },
        ShortcutItem {
            text: "Real flamegraph  (R)".to_string(),
            event: Event::Char('R'),
        },
        ShortcutItem {
            text: "Memory flamegraph(M)".to_string(),
            event: Event::Char('M'),
        },
        ShortcutItem {
            text: "EXPLAIN PLAN     (e)".to_string(),
            event: Event::Char('e'),
        },
        ShortcutItem {
            text: "EXPLAIN PIPELINE (E)".to_string(),
            event: Event::Char('E'),
        },
        ShortcutItem {
            text: "Kill this query  (K)".to_string(),
            event: Event::Char('K'),
        },
    ];

    let options = SkimOptionsBuilder::default()
        .height(Some("10%"))
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

    let selected_items = Skim::run_with(&options, Some(rx))
        .map(|out| out.selected_items)
        .unwrap_or_else(|| Vec::new());
    if selected_items.is_empty() {
        // FIXME: proper clear
        siv.on_event(Event::Refresh);
        return;
    }

    // TODO: cast SkimItem to ShortcutItem
    let skim_item = &selected_items[0];
    let shortcut_item = actions.iter().find(|&x| x.text == skim_item.text());
    if let Some(item) = shortcut_item {
        siv.on_event(item.event.clone());
    }
}
pub fn add_fuzzy_shortcuts(siv: &mut cursive::Cursive) {
    siv.add_global_callback(Event::CtrlChar('p'), |s| fuzzy_shortcuts(s));
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
