use anyhow::{Context, Result};
use cursive::event::{Event, Key};
use cursive::menu;
use cursive::theme::{BaseColor, Color, Effect, PaletteColor, Theme};
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
pub struct ShortcutItem {
    pub text: &'static str,
    pub event: Event,
}
impl ShortcutItem {
    pub fn event_string(&self) -> String {
        match self.event {
            Event::Char(c) => {
                return c.to_string();
            }
            Event::Key(k) => {
                return format!("{:?}", k);
            }
            _ => panic!("{:?} is not supported", self.event),
        }
    }
    fn preview_styled(&self) -> StyledString {
        let mut text = StyledString::default();
        text.append_styled(format!("{:>10}", self.event_string()), Effect::Bold);
        text.append_plain(format!(" - {}\n", self.text));
        return text;
    }
}
impl SkimItem for ShortcutItem {
    fn text(&self) -> Cow<str> {
        return Cow::Borrowed(&self.text);
    }
}

// NOTE: should not overlaps with global shortcuts (add_global_callback())
pub static QUERY_SHORTCUTS: &'static [ShortcutItem] = &[
    ShortcutItem {
        text: "Queries on shards",
        event: Event::Char('+'),
    },
    ShortcutItem {
        text: "Show query logs",
        event: Event::Char('l'),
    },
    ShortcutItem {
        text: "Query details",
        event: Event::Char('D'),
    },
    ShortcutItem {
        text: "Query processors",
        event: Event::Char('P'),
    },
    ShortcutItem {
        text: "Query views",
        event: Event::Char('v'),
    },
    ShortcutItem {
        text: "CPU flamegraph",
        event: Event::Char('C'),
    },
    ShortcutItem {
        text: "Real flamegraph",
        event: Event::Char('R'),
    },
    ShortcutItem {
        text: "Memory flamegraph",
        event: Event::Char('M'),
    },
    ShortcutItem {
        text: "Live flamegraph",
        event: Event::Char('L'),
    },
    ShortcutItem {
        text: "EXPLAIN PLAN",
        event: Event::Char('e'),
    },
    ShortcutItem {
        text: "EXPLAIN PIPELINE",
        event: Event::Char('E'),
    },
    ShortcutItem {
        text: "Kill this query",
        event: Event::Char('K'),
    },
];
static GENERAL_SHORTCUTS: &'static [ShortcutItem] = &[
    ShortcutItem {
        text: "Show help",
        event: Event::Key(Key::F1),
    },
    ShortcutItem {
        text: "Show actions for current item",
        event: Event::Key(Key::Enter),
    },
    ShortcutItem {
        text: "chdig debug console",
        event: Event::Char('~'),
    },
    ShortcutItem {
        text: "Back/Quit",
        event: Event::Char('q'),
    },
    ShortcutItem {
        text: "Back/Quit",
        event: Event::Key(Key::Backspace),
    },
    ShortcutItem {
        text: "Fuzzy actions",
        event: Event::Char('P'),
    },
];
static SERVER_SHORTCUTS: &'static [ShortcutItem] = &[ShortcutItem {
    text: "CPU server flamegraph",
    event: Event::Char('F'),
}];

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
    // - main view
    // - statusbar
    if siv.screen_mut().len() == 2 {
        siv.quit();
    } else {
        siv.pop_layer();
    }
}

pub fn show_help_dialog(siv: &mut cursive::Cursive) {
    let mut text = StyledString::default();

    text.append_styled(
        format!("chdig v{version}\n", version = env!("CARGO_PKG_VERSION")),
        Effect::Bold,
    );

    text.append_styled("\nGeneral shortcuts:\n\n", Effect::Bold);
    for shortcut in GENERAL_SHORTCUTS.iter() {
        text.append(shortcut.preview_styled());
    }

    text.append_styled("\nQuery actions:\n\n", Effect::Bold);
    for shortcut in QUERY_SHORTCUTS.iter() {
        text.append(shortcut.preview_styled());
    }

    text.append_styled("\nGlobal server actions:\n\n", Effect::Bold);
    for shortcut in SERVER_SHORTCUTS.iter() {
        text.append(shortcut.preview_styled());
    }

    text.append_plain(format!(
        "\nIssues and suggestions: {homepage}/issues",
        homepage = env!("CARGO_PKG_HOMEPAGE")
    ));

    siv.add_layer(Dialog::info(text));
}

pub fn add_menu(siv: &mut cursive::Cursive) {
    let mut actions = menu::Tree::new();
    for shortcut in QUERY_SHORTCUTS.iter() {
        actions.add_leaf(shortcut.text, |s| s.on_event(shortcut.event.clone()));
    }

    // TODO: color F<N> differently
    siv.menubar()
        .add_subtree(
            "F2: Views",
            menu::Tree::new()
                .leaf("Processes", |s| {
                    let context = s.user_data::<ContextArc>().unwrap().clone();
                    s.show_clickhouse_processes(context);
                })
                .leaf("Slow queries", |s| {
                    let context = s.user_data::<ContextArc>().unwrap().clone();
                    s.show_clickhouse_slow_query_log(context);
                })
                .leaf("Last queries", |s| {
                    let context = s.user_data::<ContextArc>().unwrap().clone();
                    s.show_clickhouse_last_query_log(context);
                })
                .leaf("Merges", |s| {
                    let context = s.user_data::<ContextArc>().unwrap().clone();
                    s.show_clickhouse_merges(context);
                })
                .leaf("Mutations", |s| {
                    let context = s.user_data::<ContextArc>().unwrap().clone();
                    s.show_clickhouse_mutations(context);
                })
                .leaf("Replication queue", |s| {
                    let context = s.user_data::<ContextArc>().unwrap().clone();
                    s.show_clickhouse_replication_queue(context);
                })
                .leaf("Fetches", |s| {
                    let context = s.user_data::<ContextArc>().unwrap().clone();
                    s.show_clickhouse_replicated_fetches(context);
                })
                .leaf("Replicas", |s| {
                    let context = s.user_data::<ContextArc>().unwrap().clone();
                    s.show_clickhouse_replicas(context);
                })
                .leaf("Backups", |s| {
                    let context = s.user_data::<ContextArc>().unwrap().clone();
                    s.show_clickhouse_backups(context);
                })
                .leaf("Errors", |s| {
                    let context = s.user_data::<ContextArc>().unwrap().clone();
                    s.show_clickhouse_errors(context);
                }),
        )
        .add_subtree("F8: Actions", actions)
        .add_leaf("F1: Help", |s| s.on_event(Event::Key(Key::F1)));

    siv.set_autohide_menu(false);
    siv.add_global_callback(Key::F2, |s| s.select_menubar());
}

fn fuzzy_shortcuts(siv: &mut cursive::Cursive) {
    let options = SkimOptionsBuilder::default()
        .height(Some("10%"))
        .build()
        .unwrap();

    let get_actions = || {
        GENERAL_SHORTCUTS
            .iter()
            .chain(QUERY_SHORTCUTS.iter())
            .chain(SERVER_SHORTCUTS.iter())
    };

    let (tx, rx): (SkimItemSender, SkimItemReceiver) = unbounded();
    get_actions()
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
    let shortcut_item = get_actions().find(|&x| x.text == skim_item.text());
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
