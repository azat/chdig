use cursive::{
    event::{Event, Key},
    theme::Effect,
    utils::markup::StyledString,
};

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
    pub fn preview_styled(&self) -> StyledString {
        let mut text = StyledString::default();
        text.append_styled(format!("{:>10}", self.event_string()), Effect::Bold);
        text.append_plain(format!(" - {}\n", self.text));
        return text;
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
pub static GENERAL_SHORTCUTS: &'static [ShortcutItem] = &[
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
pub static SERVER_SHORTCUTS: &'static [ShortcutItem] = &[ShortcutItem {
    text: "CPU server flamegraph",
    event: Event::Char('F'),
}];
