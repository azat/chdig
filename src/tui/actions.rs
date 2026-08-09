use anyhow::Result;
use std::sync::Arc;

use super::app::App;
use super::component::Component;
use super::event::{Event, EventResult};
use super::style::{Modifier, Style, StyledString};

#[derive(Clone)]
pub struct ActionDescription {
    pub text: &'static str,
    pub event: Event,
}

impl ActionDescription {
    pub fn event_string(&self) -> String {
        match &self.event {
            Event::Char(c) => {
                // - It is hard to understand that nothing is a space
                // - And it overlaps with no shortcut actions
                if *c == ' ' {
                    "<Space>".to_string()
                } else {
                    c.to_string()
                }
            }
            Event::CtrlChar(c) => format!("Ctrl+{}", c),
            Event::AltChar(c) => format!("Alt+{}", c),
            Event::Key(k) => format!("{:?}", k),
            Event::Unknown(_) => "".to_string(),
            _ => panic!("{:?} is not supported", self.event),
        }
    }

    pub fn preview_styled(&self) -> StyledString {
        let mut text = StyledString::new();
        text.append_styled(
            format!("{:>10}", self.event_string()),
            Style::default().add_modifier(Modifier::BOLD),
        );
        text.append_plain(format!(" - {}\n", self.text));
        text
    }
}

pub type GlobalActionCallback = Arc<dyn Fn(&mut App) + Send + Sync>;

pub struct GlobalAction {
    pub description: ActionDescription,
    pub callback: GlobalActionCallback,
}

pub type ViewActionCallback =
    Arc<dyn Fn(&mut dyn Component) -> Result<Option<EventResult>> + Send + Sync>;

pub struct ViewAction {
    /// Name of the view the action belongs to (actions of several live views
    /// can coexist, each view drops only its own).
    pub owner: &'static str,
    pub description: ActionDescription,
    pub callback: ViewActionCallback,
}
